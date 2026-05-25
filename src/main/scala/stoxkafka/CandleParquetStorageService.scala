package stoxkafka

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import java.nio.file.{Files, Paths}
import java.time.{Duration, Instant, LocalDate, ZoneOffset}
import java.util.{Collections, Properties, UUID}
import scala.jdk.CollectionConverters.*
import scala.util.Try

object CandleParquetStorageService:
  private val bootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
  private val inputTopic = sys.env.getOrElse("KAFKA_INPUT_TOPIC", "market.candles.1m")
  private val groupId = sys.env.getOrElse("KAFKA_GROUP_ID", "candle-parquet-storage")
  private val outputDir = Paths.get(sys.env.getOrElse("CANDLE_PARQUET_DIR", "data/candles"))
  private val finalOnly = sys.env.get("STORE_FINAL_ONLY").forall(_.toBooleanOption.getOrElse(true))

  private val schema: Schema = Schema.Parser().parse(
    """
      |{
      |  "type": "record",
      |  "name": "Candle",
      |  "namespace": "stoxkafka",
      |  "fields": [
      |    {"name": "event", "type": "string"},
      |    {"name": "exchange", "type": "string"},
      |    {"name": "symbol", "type": "string"},
      |    {"name": "currency", "type": ["null", "string"], "default": null},
      |    {"name": "timeframe", "type": "string"},
      |    {"name": "openTime", "type": "string"},
      |    {"name": "closeTime", "type": "string"},
      |    {"name": "tradingDate", "type": "string"},
      |    {"name": "open", "type": "string"},
      |    {"name": "high", "type": "string"},
      |    {"name": "low", "type": "string"},
      |    {"name": "close", "type": "string"},
      |    {"name": "volume", "type": "string"},
      |    {"name": "tickCount", "type": "long"},
      |    {"name": "isFinal", "type": "boolean"},
      |    {"name": "publishedAt", "type": "string"},
      |    {"name": "storedAt", "type": "string"},
      |    {"name": "kafkaKey", "type": ["null", "string"], "default": null},
      |    {"name": "kafkaTopic", "type": "string"},
      |    {"name": "kafkaPartition", "type": "int"},
      |    {"name": "kafkaOffset", "type": "long"}
      |  ]
      |}
      |""".stripMargin
  )

  final case class Candle(
      event: String,
      exchange: String,
      symbol: String,
      currency: Option[String],
      timeframe: String,
      openTime: Instant,
      closeTime: Instant,
      open: String,
      high: String,
      low: String,
      close: String,
      volume: String,
      tickCount: Long,
      isFinal: Boolean,
      publishedAt: String
  ):
    def tradingDate: LocalDate = openTime.atZone(ZoneOffset.UTC).toLocalDate

  final case class PartitionKey(exchange: String, symbol: String, timeframe: String, tradingDate: LocalDate)

  final case class WriterHandle(writer: ParquetWriter[GenericRecord], path: java.nio.file.Path):
    def close(): Unit = writer.close()

  def main(args: Array[String]): Unit =
    Files.createDirectories(outputDir)

    val consumer = KafkaConsumer[String, String](consumerProperties())
    val writers = scala.collection.mutable.Map.empty[PartitionKey, WriterHandle]

    consumer.subscribe(Collections.singletonList(inputTopic))

    sys.addShutdownHook {
      consumer.wakeup()
    }

    println(
      s"storing candles input=$inputTopic group=$groupId outputDir=${outputDir.toAbsolutePath} finalOnly=$finalOnly"
    )

    try
      while true do
        val records = consumer.poll(Duration.ofMillis(1000))

        records.asScala.foreach { record =>
          parseCandle(record.value()) match
            case Some(candle) if !finalOnly || candle.isFinal =>
              val key = PartitionKey(candle.exchange, candle.symbol, candle.timeframe, candle.tradingDate)
              val handle = writers.getOrElseUpdate(key, createWriter(key))
              handle.writer.write(toRecord(candle, Option(record.key()), record.topic(), record.partition(), record.offset()))
              println(
                s"stored key=${record.key()} final=${candle.isFinal} symbol=${candle.symbol} openTime=${candle.openTime} path=${handle.path}"
              )

            case Some(candle) =>
              println(s"skipping non-final candle symbol=${candle.symbol} openTime=${candle.openTime}")

            case None =>
              println(s"skipping unparsable candle offset=${record.offset()} key=${record.key()} value=${record.value()}")
        }
    catch
      case _: org.apache.kafka.common.errors.WakeupException =>
        println("parquet storage service stopping")
    finally
      writers.values.foreach(_.close())
      consumer.close()

  private def consumerProperties(): Properties =
    val props = Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.deserializer", classOf[StringDeserializer].getName)
    props.put("value.deserializer", classOf[StringDeserializer].getName)
    props.put("group.id", groupId)
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "true")
    props

  private def parseCandle(value: String): Option[Candle] =
    Try(ujson.read(value)).toOption.flatMap { json =>
      for
        exchange <- stringField(json, "exchange")
        symbol <- stringField(json, "symbol")
        timeframe <- stringField(json, "timeframe")
        openTime <- instantField(json, "openTime")
        closeTime <- instantField(json, "closeTime")
        open <- stringOrNumberField(json, "open")
        high <- stringOrNumberField(json, "high")
        low <- stringOrNumberField(json, "low")
        close <- stringOrNumberField(json, "close")
      yield Candle(
        event = stringField(json, "event").getOrElse("candle"),
        exchange = exchange,
        symbol = symbol,
        currency = stringField(json, "currency").filter(_.nonEmpty),
        timeframe = timeframe,
        openTime = openTime,
        closeTime = closeTime,
        open = open,
        high = high,
        low = low,
        close = close,
        volume = stringOrNumberField(json, "volume").getOrElse("0"),
        tickCount = longField(json, "tickCount").getOrElse(0L),
        isFinal = booleanField(json, "isFinal").getOrElse(false),
        publishedAt = stringField(json, "publishedAt").getOrElse("")
      )
    }

  private def createWriter(key: PartitionKey): WriterHandle =
    val dir = outputDir
      .resolve(s"exchange=${safePathSegment(key.exchange)}")
      .resolve(s"symbol=${safePathSegment(key.symbol)}")
      .resolve(s"timeframe=${safePathSegment(key.timeframe)}")
      .resolve(s"date=${key.tradingDate}")

    Files.createDirectories(dir)
    val path = dir.resolve(s"part-${UUID.randomUUID()}.parquet")

    val writer = AvroParquetWriter
      .builder[GenericRecord](Path(path.toUri))
      .withSchema(schema)
      .withConf(Configuration())
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    WriterHandle(writer, path)

  private def toRecord(
      candle: Candle,
      kafkaKey: Option[String],
      topic: String,
      partition: Int,
      offset: Long
  ): GenericRecord =
    val record = GenericData.Record(schema)
    record.put("event", candle.event)
    record.put("exchange", candle.exchange)
    record.put("symbol", candle.symbol)
    record.put("currency", candle.currency.orNull)
    record.put("timeframe", candle.timeframe)
    record.put("openTime", candle.openTime.toString)
    record.put("closeTime", candle.closeTime.toString)
    record.put("tradingDate", candle.tradingDate.toString)
    record.put("open", candle.open)
    record.put("high", candle.high)
    record.put("low", candle.low)
    record.put("close", candle.close)
    record.put("volume", candle.volume)
    record.put("tickCount", candle.tickCount)
    record.put("isFinal", candle.isFinal)
    record.put("publishedAt", candle.publishedAt)
    record.put("storedAt", Instant.now().toString)
    record.put("kafkaKey", kafkaKey.orNull)
    record.put("kafkaTopic", topic)
    record.put("kafkaPartition", partition)
    record.put("kafkaOffset", offset)
    record

  private def stringField(json: ujson.Value, name: String): Option[String] =
    json.obj.get(name).collect { case ujson.Str(value) => value }

  private def stringOrNumberField(json: ujson.Value, name: String): Option[String] =
    json.obj.get(name).flatMap {
      case ujson.Str(value) => Some(value)
      case ujson.Num(value) if !value.isNaN => Some(BigDecimal(value).toString)
      case _ => None
    }

  private def longField(json: ujson.Value, name: String): Option[Long] =
    json.obj.get(name).flatMap {
      case ujson.Num(value) if !value.isNaN => Some(value.toLong)
      case ujson.Str(value) => value.toLongOption
      case _ => None
    }

  private def booleanField(json: ujson.Value, name: String): Option[Boolean] =
    json.obj.get(name).collect { case ujson.Bool(value) => value }

  private def instantField(json: ujson.Value, name: String): Option[Instant] =
    stringField(json, name).flatMap(value => Try(Instant.parse(value)).toOption)

  private def safePathSegment(value: String): String =
    value.replaceAll("[^A-Za-z0-9._=-]", "_")

