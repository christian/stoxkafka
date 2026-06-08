package stoxkafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import java.nio.file.{Files, Paths}
import java.sql.{Connection, DriverManager}
import java.time.{Duration, Instant, LocalDate, ZoneOffset}
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters.*
import scala.util.Try

object CandleSqliteStorageService:
  private val bootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
  private val inputTopic = sys.env.getOrElse("KAFKA_INPUT_TOPIC", "market.candles.1m")
  private val groupId = sys.env.getOrElse("KAFKA_GROUP_ID", "candle-sqlite-storage")
  private val dbPath = Paths.get(sys.env.getOrElse("CANDLE_SQLITE_DB", "data/live/candles.db"))
  private val finalOnly = sys.env.get("STORE_FINAL_ONLY").exists(_.toBooleanOption.getOrElse(false))

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

  def main(args: Array[String]): Unit =
    Files.createDirectories(dbPath.getParent)

    Class.forName("org.sqlite.JDBC")

    val consumer = KafkaConsumer[String, String](consumerProperties())
    val connection = DriverManager.getConnection(s"jdbc:sqlite:${dbPath.toAbsolutePath}")
    initDatabase(connection)

    consumer.subscribe(Collections.singletonList(inputTopic))

    sys.addShutdownHook {
      consumer.wakeup()
    }

    println(s"storing live candles input=$inputTopic group=$groupId db=${dbPath.toAbsolutePath} finalOnly=$finalOnly")

    try
      while true do
        val records = consumer.poll(Duration.ofMillis(1000))

        records.asScala.foreach { record =>
          parseCandle(record.value()) match
            case Some(candle) if !finalOnly || candle.isFinal =>
              upsertCandle(connection, candle, Option(record.key()), record.topic(), record.partition(), record.offset())
              println(s"stored sqlite key=${record.key()} final=${candle.isFinal} symbol=${candle.symbol} openTime=${candle.openTime}")

            case Some(candle) =>
              println(s"skipping non-final candle symbol=${candle.symbol} openTime=${candle.openTime}")

            case None =>
              println(s"skipping unparsable candle offset=${record.offset()} key=${record.key()} value=${record.value()}")
        }
    catch
      case _: org.apache.kafka.common.errors.WakeupException =>
        println("sqlite storage service stopping")
    finally
      connection.close()
      consumer.close()

  private def consumerProperties(): Properties =
    val props = Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.deserializer", classOf[StringDeserializer].getName)
    props.put("value.deserializer", classOf[StringDeserializer].getName)
    props.put("group.id", groupId)
    props.put("auto.offset.reset", "latest")
    props.put("enable.auto.commit", "true")
    props

  private def initDatabase(connection: Connection): Unit =
    val sql =
      """
        |CREATE TABLE IF NOT EXISTS candles (
        |  trading_date TEXT NOT NULL,
        |  exchange TEXT NOT NULL,
        |  symbol TEXT NOT NULL,
        |  timeframe TEXT NOT NULL,
        |  open_time TEXT NOT NULL,
        |  close_time TEXT NOT NULL,
        |  event TEXT NOT NULL,
        |  currency TEXT,
        |  open TEXT NOT NULL,
        |  high TEXT NOT NULL,
        |  low TEXT NOT NULL,
        |  close TEXT NOT NULL,
        |  volume TEXT NOT NULL,
        |  tick_count INTEGER NOT NULL,
        |  is_final INTEGER NOT NULL,
        |  published_at TEXT NOT NULL,
        |  stored_at TEXT NOT NULL,
        |  kafka_key TEXT,
        |  kafka_topic TEXT NOT NULL,
        |  kafka_partition INTEGER NOT NULL,
        |  kafka_offset INTEGER NOT NULL,
        |  PRIMARY KEY (trading_date, exchange, symbol, timeframe, open_time)
        |);
        |CREATE INDEX IF NOT EXISTS idx_candles_lookup
        |  ON candles (trading_date, exchange, symbol, timeframe, open_time);
        |""".stripMargin

    val statement = connection.createStatement()
    try statement.executeUpdate(sql)
    finally statement.close()

  private def upsertCandle(
      connection: Connection,
      candle: Candle,
      kafkaKey: Option[String],
      topic: String,
      partition: Int,
      offset: Long
  ): Unit =
    val sql =
      """
        |INSERT INTO candles (
        |  trading_date, exchange, symbol, timeframe, open_time, close_time,
        |  event, currency, open, high, low, close, volume, tick_count, is_final,
        |  published_at, stored_at, kafka_key, kafka_topic, kafka_partition, kafka_offset
        |) VALUES (
        |  ?, ?, ?, ?, ?, ?,
        |  ?, ?, ?, ?, ?, ?, ?, ?, ?,
        |  ?, ?, ?, ?, ?, ?
        |)
        |ON CONFLICT(trading_date, exchange, symbol, timeframe, open_time) DO UPDATE SET
        |  close_time = excluded.close_time,
        |  event = excluded.event,
        |  currency = excluded.currency,
        |  open = excluded.open,
        |  high = excluded.high,
        |  low = excluded.low,
        |  close = excluded.close,
        |  volume = excluded.volume,
        |  tick_count = excluded.tick_count,
        |  is_final = excluded.is_final,
        |  published_at = excluded.published_at,
        |  stored_at = excluded.stored_at,
        |  kafka_key = excluded.kafka_key,
        |  kafka_topic = excluded.kafka_topic,
        |  kafka_partition = excluded.kafka_partition,
        |  kafka_offset = excluded.kafka_offset
        |""".stripMargin

    val prepared = connection.prepareStatement(sql)
    try
      prepared.setString(1, candle.tradingDate.toString)
      prepared.setString(2, candle.exchange)
      prepared.setString(3, candle.symbol)
      prepared.setString(4, candle.timeframe)
      prepared.setString(5, candle.openTime.toString)
      prepared.setString(6, candle.closeTime.toString)
      prepared.setString(7, candle.event)
      prepared.setString(8, candle.currency.orNull)
      prepared.setString(9, candle.open)
      prepared.setString(10, candle.high)
      prepared.setString(11, candle.low)
      prepared.setString(12, candle.close)
      prepared.setString(13, candle.volume)
      prepared.setLong(14, candle.tickCount)
      prepared.setInt(15, if candle.isFinal then 1 else 0)
      prepared.setString(16, candle.publishedAt)
      prepared.setString(17, Instant.now().toString)
      prepared.setString(18, kafkaKey.orNull)
      prepared.setString(19, topic)
      prepared.setInt(20, partition)
      prepared.setLong(21, offset)
      prepared.executeUpdate()
    finally prepared.close()

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
