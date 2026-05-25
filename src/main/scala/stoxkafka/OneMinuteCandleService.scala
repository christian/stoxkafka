package stoxkafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import java.time.{Duration, Instant, ZoneOffset}
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters.*
import scala.util.Try

object OneMinuteCandleService:
  private val bootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
  private val inputTopic = sys.env.getOrElse("KAFKA_INPUT_TOPIC", "market.trades.raw")
  private val outputTopic = sys.env.getOrElse("KAFKA_OUTPUT_TOPIC", "market.candles.1m")
  private val groupId = sys.env.getOrElse("KAFKA_GROUP_ID", "one-minute-candle-service")
  private val closeGrace = Duration.ofSeconds(sys.env.get("CANDLE_CLOSE_GRACE_SECONDS").flatMap(_.toLongOption).getOrElse(2L))
  private val timeframe = "1m"

  final case class Tick(
      exchange: String,
      symbol: String,
      currency: Option[String],
      price: BigDecimal,
      size: BigDecimal,
      cumulativeVolume: Option[BigDecimal],
      timestamp: Instant
  )

  final case class CandleState(
      exchange: String,
      symbol: String,
      currency: Option[String],
      openTime: Instant,
      closeTime: Instant,
      open: BigDecimal,
      high: BigDecimal,
      low: BigDecimal,
      close: BigDecimal,
      volume: BigDecimal,
      tickCount: Long,
      lastCumulativeVolume: Option[BigDecimal]
  ):
    def update(tick: Tick): CandleState =
      val volumeIncrement = tick.cumulativeVolume
        .zip(lastCumulativeVolume)
        .map((current, previous) => (current - previous).max(BigDecimal(0)))
        .orElse(if tick.size > 0 then Some(tick.size) else None)
        .getOrElse(BigDecimal(0))

      copy(
        high = high.max(tick.price),
        low = low.min(tick.price),
        close = tick.price,
        volume = volume + volumeIncrement,
        tickCount = tickCount + 1,
        lastCumulativeVolume = tick.cumulativeVolume.orElse(lastCumulativeVolume)
      )

  def main(args: Array[String]): Unit =
    val consumer = KafkaConsumer[String, String](consumerProperties())
    val producer = KafkaProducer[String, String](producerProperties())
    val candles = scala.collection.mutable.Map.empty[String, CandleState]

    consumer.subscribe(Collections.singletonList(inputTopic))

    sys.addShutdownHook {
      consumer.wakeup()
    }

    println(
      s"building $timeframe candles input=$inputTopic output=$outputTopic group=$groupId bootstrapServers=$bootstrapServers"
    )

    try
      while true do
        val records = consumer.poll(Duration.ofMillis(500))

        records.asScala.foreach { record =>
          parseTick(record.value(), Option(record.key()), Instant.ofEpochMilli(record.timestamp())) match
            case Some(tick) =>
              val candleKey = s"${tick.exchange}:${tick.symbol}:$timeframe"
              val tickOpenTime = minuteStart(tick.timestamp)

              candles.get(candleKey) match
                case Some(existing) if tickOpenTime.isBefore(existing.openTime) =>
                  println(s"skipping late tick key=$candleKey tickTime=${tick.timestamp} currentOpen=${existing.openTime}")

                case Some(existing) if tickOpenTime == existing.openTime =>
                  val updated = existing.update(tick)
                  candles.update(candleKey, updated)
                  publishCandle(producer, candleKey, updated, isFinal = false)

                case Some(existing) =>
                  publishCandle(producer, candleKey, existing, isFinal = true)
                  val next = newCandle(tick, tickOpenTime)
                  candles.update(candleKey, next)
                  publishCandle(producer, candleKey, next, isFinal = false)

                case None =>
                  val next = newCandle(tick, tickOpenTime)
                  candles.update(candleKey, next)
                  publishCandle(producer, candleKey, next, isFinal = false)

            case None =>
              println(s"skipping unparsable tick offset=${record.offset()} key=${record.key()} value=${record.value()}")
        }

        closeDueCandles(producer, candles, Instant.now())
        producer.flush()
    catch
      case _: org.apache.kafka.common.errors.WakeupException =>
        println("candle service stopping")
    finally
      candles.foreach { case (key, candle) =>
        publishCandle(producer, key, candle, isFinal = true)
      }
      producer.flush()
      consumer.close()
      producer.close()

  private def consumerProperties(): Properties =
    val props = Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.deserializer", classOf[StringDeserializer].getName)
    props.put("value.deserializer", classOf[StringDeserializer].getName)
    props.put("group.id", groupId)
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "true")
    props

  private def producerProperties(): Properties =
    val props = Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("acks", "all")
    props

  private def parseTick(value: String, recordKey: Option[String], fallbackTimestamp: Instant): Option[Tick] =
    Try(ujson.read(value)).toOption.flatMap { json =>
      val exchange = stringField(json, "exchange")
        .orElse(recordKey.flatMap(_.split(":").headOption))
        .getOrElse("unknown")

      val symbol = stringField(json, "symbol")
        .orElse(recordKey.flatMap(_.split(":").lift(1)))

      val price = decimalField(json, "last")
        .orElse(midpoint(json))
        .orElse(decimalField(json, "close"))

      for
        symbolValue <- symbol
        priceValue <- price
      yield Tick(
        exchange = exchange,
        symbol = symbolValue,
        currency = stringField(json, "currency"),
        price = priceValue,
        size = decimalField(json, "lastSize").getOrElse(BigDecimal(0)),
        cumulativeVolume = decimalField(json, "volume"),
        timestamp = instantField(json, "sourceTimestamp")
          .orElse(instantField(json, "publishedAt"))
          .getOrElse(fallbackTimestamp)
      )
    }

  private def stringField(json: ujson.Value, name: String): Option[String] =
    json.obj.get(name).collect { case ujson.Str(value) if value.nonEmpty => value }

  private def decimalField(json: ujson.Value, name: String): Option[BigDecimal] =
    json.obj.get(name).flatMap {
      case ujson.Num(value) if !value.isNaN => Some(BigDecimal(value))
      case ujson.Str(value) if value.nonEmpty => Try(BigDecimal(value)).toOption
      case _ => None
    }

  private def instantField(json: ujson.Value, name: String): Option[Instant] =
    stringField(json, name).flatMap(value => Try(Instant.parse(value)).toOption)

  private def midpoint(json: ujson.Value): Option[BigDecimal] =
    for
      bid <- decimalField(json, "bid")
      ask <- decimalField(json, "ask")
      if bid > 0 && ask > 0
    yield (bid + ask) / 2

  private def minuteStart(instant: Instant): Instant =
    instant.atZone(ZoneOffset.UTC)
      .withSecond(0)
      .withNano(0)
      .toInstant

  private def newCandle(tick: Tick, openTime: Instant): CandleState =
    CandleState(
      exchange = tick.exchange,
      symbol = tick.symbol,
      currency = tick.currency,
      openTime = openTime,
      closeTime = openTime.plusSeconds(60).minusMillis(1),
      open = tick.price,
      high = tick.price,
      low = tick.price,
      close = tick.price,
      volume = BigDecimal(0),
      tickCount = 1,
      lastCumulativeVolume = tick.cumulativeVolume
    )

  private def publishCandle(
      producer: KafkaProducer[String, String],
      key: String,
      candle: CandleState,
      isFinal: Boolean
  ): Unit =
    val payload = ujson.Obj(
      "event" -> (if isFinal then "candle.closed" else "candle.updated"),
      "exchange" -> candle.exchange,
      "symbol" -> candle.symbol,
      "currency" -> candle.currency.getOrElse(""),
      "timeframe" -> timeframe,
      "openTime" -> candle.openTime.toString,
      "closeTime" -> candle.closeTime.toString,
      "open" -> candle.open.toString,
      "high" -> candle.high.toString,
      "low" -> candle.low.toString,
      "close" -> candle.close.toString,
      "volume" -> candle.volume.toString,
      "tickCount" -> candle.tickCount,
      "isFinal" -> isFinal,
      "publishedAt" -> Instant.now().toString
    )

    producer.send(ProducerRecord[String, String](outputTopic, key, ujson.write(payload)))
    println(
      s"published key=$key final=$isFinal open=${candle.open} high=${candle.high} low=${candle.low} close=${candle.close} volume=${candle.volume}"
    )

  private def closeDueCandles(
      producer: KafkaProducer[String, String],
      candles: scala.collection.mutable.Map[String, CandleState],
      now: Instant
  ): Unit =
    val dueKeys = candles.collect {
      case (key, candle) if !now.isBefore(candle.closeTime.plusMillis(1).plus(closeGrace)) => key
    }.toList

    dueKeys.foreach { key =>
      candles.remove(key).foreach { candle =>
        publishCandle(producer, key, candle, isFinal = true)
      }
    }
