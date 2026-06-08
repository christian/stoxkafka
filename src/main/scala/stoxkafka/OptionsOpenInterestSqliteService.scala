package stoxkafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import java.nio.file.{Files, Paths}
import java.sql.{Connection, DriverManager}
import java.time.{Duration, Instant, LocalDate, ZoneOffset}
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters.*
import scala.util.Try

object OptionsOpenInterestSqliteService:
  private val bootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
  private val inputTopic = sys.env.getOrElse("KAFKA_INPUT_TOPIC", "market.options.oi.snapshot")
  private val groupId = sys.env.getOrElse("KAFKA_GROUP_ID", "options-open-interest-sqlite")
  private val dbPath = Paths.get(sys.env.getOrElse("OPTIONS_OI_SQLITE_DB", "data/live/options_oi.db"))

  final case class OptionOiSnapshot(
      event: String,
      exchange: String,
      source: String,
      snapshotAt: Instant,
      underlyingSymbol: String,
      underlyingCurrency: Option[String],
      underlyingExchange: String,
      underlyingPrimaryExchange: String,
      underlyingConId: Option[Long],
      underlyingPrice: Option[BigDecimal],
      symbol: String,
      currency: Option[String],
      optionExchange: Option[String],
      chainExchange: Option[String],
      tradingClass: Option[String],
      multiplier: Option[String],
      conId: Option[Long],
      expiry: String,
      strike: BigDecimal,
      right: String,
      optionOpenInterest: Option[Long],
      callOpenInterest: Option[Long],
      putOpenInterest: Option[Long],
      bid: Option[BigDecimal],
      ask: Option[BigDecimal],
      last: Option[BigDecimal],
      close: Option[BigDecimal],
      volume: Option[Long],
      openInterest: Option[Long],
      underlyingMid: Option[BigDecimal],
      sourceTimestamp: Option[Instant],
      publishedAt: Instant
  ):
    def tradingDate: LocalDate = snapshotAt.atZone(ZoneOffset.UTC).toLocalDate

  final case class DailyRatioRow(
      tradingDate: LocalDate,
      snapshotAt: Instant,
      exchange: String,
      source: String,
      underlyingSymbol: String,
      underlyingCurrency: Option[String],
      underlyingExchange: String,
      underlyingPrimaryExchange: String,
      underlyingConId: Option[Long],
      underlyingPrice: Option[BigDecimal],
      totalPutOpenInterest: Long,
      totalCallOpenInterest: Long,
      putCallRatio: Option[BigDecimal],
      contractCount: Long,
      previousTradingDate: Option[LocalDate],
      previousPutOpenInterest: Option[Long],
      previousCallOpenInterest: Option[Long],
      previousPutCallRatio: Option[BigDecimal],
      putOpenInterestChange: Option[Long],
      callOpenInterestChange: Option[Long],
      ratioChange: Option[BigDecimal],
      ratioPctChange: Option[BigDecimal],
      lastRecordedAt: Instant
  )

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

    println(s"storing options open interest input=$inputTopic group=$groupId db=${dbPath.toAbsolutePath}")

    try
      while true do
        val records = consumer.poll(Duration.ofMillis(1000))

        records.asScala.foreach { record =>
          parseSnapshot(record.value()) match
            case Some(snapshot) =>
              upsertContractSnapshot(connection, snapshot, Option(record.key()), record.topic(), record.partition(), record.offset())
              val aggregate = recomputeDailyRatio(connection, snapshot)
              upsertDailyRatio(connection, aggregate, Option(record.key()), record.topic(), record.partition(), record.offset())
              println(
                s"stored options snapshot symbol=${snapshot.underlyingSymbol} expiry=${snapshot.expiry} right=${snapshot.right} " +
                  s"snapshotAt=${snapshot.snapshotAt} put=${snapshot.putOpenInterest} call=${snapshot.callOpenInterest}"
              )

            case None =>
              println(s"skipping unparsable options snapshot offset=${record.offset()} key=${record.key()} value=${record.value()}")
        }
    catch
      case _: org.apache.kafka.common.errors.WakeupException =>
        println("options open interest storage service stopping")
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
        |CREATE TABLE IF NOT EXISTS option_contract_snapshots (
        |  kafka_topic TEXT NOT NULL,
        |  kafka_partition INTEGER NOT NULL,
        |  kafka_offset INTEGER NOT NULL,
        |  trading_date TEXT NOT NULL,
        |  snapshot_at TEXT NOT NULL,
        |  exchange TEXT NOT NULL,
        |  source TEXT NOT NULL,
        |  underlying_symbol TEXT NOT NULL,
        |  underlying_currency TEXT,
        |  underlying_exchange TEXT NOT NULL,
        |  underlying_primary_exchange TEXT NOT NULL,
        |  underlying_con_id INTEGER,
        |  underlying_price TEXT,
        |  symbol TEXT NOT NULL,
        |  currency TEXT,
        |  option_exchange TEXT,
        |  chain_exchange TEXT,
        |  trading_class TEXT,
        |  multiplier TEXT,
        |  con_id INTEGER,
        |  expiry TEXT NOT NULL,
        |  strike TEXT NOT NULL,
        |  right TEXT NOT NULL,
        |  option_open_interest INTEGER,
        |  call_open_interest INTEGER,
        |  put_open_interest INTEGER,
        |  bid TEXT,
        |  ask TEXT,
        |  last TEXT,
        |  close TEXT,
        |  volume INTEGER,
        |  open_interest INTEGER,
        |  underlying_mid TEXT,
        |  event TEXT NOT NULL,
        |  source_timestamp TEXT,
        |  published_at TEXT NOT NULL,
        |  stored_at TEXT NOT NULL,
        |  kafka_key TEXT,
        |  PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
        |);
        |
        |CREATE INDEX IF NOT EXISTS idx_option_contract_snapshots_lookup
        |  ON option_contract_snapshots (trading_date, underlying_symbol, underlying_exchange, underlying_primary_exchange, snapshot_at);
        |
        |CREATE TABLE IF NOT EXISTS option_daily_ratios (
        |  trading_date TEXT NOT NULL,
        |  snapshot_at TEXT NOT NULL,
        |  exchange TEXT NOT NULL,
        |  source TEXT NOT NULL,
        |  underlying_symbol TEXT NOT NULL,
        |  underlying_currency TEXT,
        |  underlying_exchange TEXT NOT NULL,
        |  underlying_primary_exchange TEXT NOT NULL,
        |  underlying_con_id INTEGER,
        |  underlying_price TEXT,
        |  total_put_open_interest INTEGER NOT NULL,
        |  total_call_open_interest INTEGER NOT NULL,
        |  put_call_ratio TEXT,
        |  contract_count INTEGER NOT NULL,
        |  previous_trading_date TEXT,
        |  previous_put_open_interest INTEGER,
        |  previous_call_open_interest INTEGER,
        |  previous_put_call_ratio TEXT,
        |  put_open_interest_change INTEGER,
        |  call_open_interest_change INTEGER,
        |  ratio_change TEXT,
        |  ratio_pct_change TEXT,
        |  last_recorded_at TEXT NOT NULL,
        |  stored_at TEXT NOT NULL,
        |  kafka_key TEXT,
        |  kafka_topic TEXT NOT NULL,
        |  kafka_partition INTEGER NOT NULL,
        |  kafka_offset INTEGER NOT NULL,
        |  PRIMARY KEY (trading_date, underlying_symbol, underlying_exchange, underlying_primary_exchange)
        |);
        |
        |CREATE INDEX IF NOT EXISTS idx_option_daily_ratios_lookup
        |  ON option_daily_ratios (underlying_symbol, trading_date);
        |""".stripMargin

    val statement = connection.createStatement()
    try statement.executeUpdate(sql)
    finally statement.close()

  private def upsertContractSnapshot(
      connection: Connection,
      snapshot: OptionOiSnapshot,
      kafkaKey: Option[String],
      topic: String,
      partition: Int,
      offset: Long
  ): Unit =
    val sql =
      """
        |INSERT INTO option_contract_snapshots (
        |  kafka_topic, kafka_partition, kafka_offset, trading_date, snapshot_at,
        |  exchange, source, underlying_symbol, underlying_currency, underlying_exchange,
        |  underlying_primary_exchange, underlying_con_id, underlying_price, symbol, currency,
        |  option_exchange, chain_exchange, trading_class, multiplier, con_id,
        |  expiry, strike, right, option_open_interest, call_open_interest,
        |  put_open_interest, bid, ask, last, close, volume,
        |  open_interest, underlying_mid, event, source_timestamp, published_at,
        |  stored_at, kafka_key
        |) VALUES (
        |  ?, ?, ?, ?, ?,
        |  ?, ?, ?, ?, ?,
        |  ?, ?, ?, ?, ?,
        |  ?, ?, ?, ?, ?,
        |  ?, ?, ?, ?, ?,
        |  ?, ?, ?, ?, ?,
        |  ?, ?, ?, ?, ?,
        |  ?, ?, ?
        |)
        |ON CONFLICT(kafka_topic, kafka_partition, kafka_offset) DO UPDATE SET
        |  trading_date = excluded.trading_date,
        |  snapshot_at = excluded.snapshot_at,
        |  exchange = excluded.exchange,
        |  source = excluded.source,
        |  underlying_symbol = excluded.underlying_symbol,
        |  underlying_currency = excluded.underlying_currency,
        |  underlying_exchange = excluded.underlying_exchange,
        |  underlying_primary_exchange = excluded.underlying_primary_exchange,
        |  underlying_con_id = excluded.underlying_con_id,
        |  underlying_price = excluded.underlying_price,
        |  symbol = excluded.symbol,
        |  currency = excluded.currency,
        |  option_exchange = excluded.option_exchange,
        |  chain_exchange = excluded.chain_exchange,
        |  trading_class = excluded.trading_class,
        |  multiplier = excluded.multiplier,
        |  con_id = excluded.con_id,
        |  expiry = excluded.expiry,
        |  strike = excluded.strike,
        |  right = excluded.right,
        |  option_open_interest = excluded.option_open_interest,
        |  call_open_interest = excluded.call_open_interest,
        |  put_open_interest = excluded.put_open_interest,
        |  bid = excluded.bid,
        |  ask = excluded.ask,
        |  last = excluded.last,
        |  close = excluded.close,
        |  volume = excluded.volume,
        |  open_interest = excluded.open_interest,
        |  underlying_mid = excluded.underlying_mid,
        |  event = excluded.event,
        |  source_timestamp = excluded.source_timestamp,
        |  published_at = excluded.published_at,
        |  stored_at = excluded.stored_at,
        |  kafka_key = excluded.kafka_key
        |""".stripMargin

    val prepared = connection.prepareStatement(sql)
    try
      prepared.setString(1, topic)
      prepared.setInt(2, partition)
      prepared.setLong(3, offset)
      prepared.setString(4, snapshot.tradingDate.toString)
      prepared.setString(5, snapshot.snapshotAt.toString)
      prepared.setString(6, snapshot.exchange)
      prepared.setString(7, snapshot.source)
      prepared.setString(8, snapshot.underlyingSymbol)
      prepared.setString(9, snapshot.underlyingCurrency.orNull)
      prepared.setString(10, snapshot.underlyingExchange)
      prepared.setString(11, snapshot.underlyingPrimaryExchange)
      snapshot.underlyingConId match
        case Some(value) => prepared.setLong(12, value)
        case None => prepared.setNull(12, java.sql.Types.BIGINT)
      prepared.setString(13, snapshot.underlyingPrice.map(_.toString).orNull)
      prepared.setString(14, snapshot.symbol)
      prepared.setString(15, snapshot.currency.orNull)
      prepared.setString(16, snapshot.optionExchange.orNull)
      prepared.setString(17, snapshot.chainExchange.orNull)
      prepared.setString(18, snapshot.tradingClass.orNull)
      prepared.setString(19, snapshot.multiplier.orNull)
      snapshot.conId match
        case Some(value) => prepared.setLong(20, value)
        case None => prepared.setNull(20, java.sql.Types.BIGINT)
      prepared.setString(21, snapshot.expiry)
      prepared.setString(22, snapshot.strike.toString)
      prepared.setString(23, snapshot.right)
      snapshot.optionOpenInterest match
        case Some(value) => prepared.setLong(24, value)
        case None => prepared.setNull(24, java.sql.Types.BIGINT)
      snapshot.callOpenInterest match
        case Some(value) => prepared.setLong(25, value)
        case None => prepared.setNull(25, java.sql.Types.BIGINT)
      snapshot.putOpenInterest match
        case Some(value) => prepared.setLong(26, value)
        case None => prepared.setNull(26, java.sql.Types.BIGINT)
      prepared.setString(27, snapshot.bid.map(_.toString).orNull)
      prepared.setString(28, snapshot.ask.map(_.toString).orNull)
      prepared.setString(29, snapshot.last.map(_.toString).orNull)
      prepared.setString(30, snapshot.close.map(_.toString).orNull)
      snapshot.volume match
        case Some(value) => prepared.setLong(31, value)
        case None => prepared.setNull(31, java.sql.Types.BIGINT)
      snapshot.openInterest match
        case Some(value) => prepared.setLong(32, value)
        case None => prepared.setNull(32, java.sql.Types.BIGINT)
      prepared.setString(33, snapshot.underlyingMid.map(_.toString).orNull)
      prepared.setString(34, snapshot.event)
      prepared.setString(35, snapshot.sourceTimestamp.map(_.toString).orNull)
      prepared.setString(36, snapshot.publishedAt.toString)
      prepared.setString(37, Instant.now().toString)
      prepared.setString(38, kafkaKey.orNull)
      prepared.executeUpdate()
    finally prepared.close()

  private def recomputeDailyRatio(connection: Connection, snapshot: OptionOiSnapshot): DailyRatioRow =
    val aggregateSql =
      """
        |SELECT
        |  COALESCE(SUM(CASE WHEN right = 'P' THEN COALESCE(option_open_interest, 0) ELSE 0 END), 0) AS put_open_interest,
        |  COALESCE(SUM(CASE WHEN right = 'C' THEN COALESCE(option_open_interest, 0) ELSE 0 END), 0) AS call_open_interest,
        |  COUNT(*) AS contract_count
        |FROM option_contract_snapshots
        |WHERE snapshot_at = ? AND underlying_symbol = ? AND underlying_exchange = ? AND underlying_primary_exchange = ?
        |""".stripMargin

    val aggregatePrepared = connection.prepareStatement(aggregateSql)
    try
      aggregatePrepared.setString(1, snapshot.snapshotAt.toString)
      aggregatePrepared.setString(2, snapshot.underlyingSymbol)
      aggregatePrepared.setString(3, snapshot.underlyingExchange)
      aggregatePrepared.setString(4, snapshot.underlyingPrimaryExchange)

      val aggregateResult = aggregatePrepared.executeQuery()
      val totals =
        if aggregateResult.next() then
          (
            aggregateResult.getLong("put_open_interest"),
            aggregateResult.getLong("call_open_interest"),
            aggregateResult.getLong("contract_count")
          )
        else
          (0L, 0L, 0L)
      aggregateResult.close()

      val (totalPutOpenInterest, totalCallOpenInterest, contractCount) = totals
      val putCallRatio =
        if totalCallOpenInterest == 0 then None
        else Some(BigDecimal(totalPutOpenInterest) / BigDecimal(totalCallOpenInterest))

      val previousSql =
        """
          |SELECT
          |  trading_date,
          |  total_put_open_interest,
          |  total_call_open_interest,
          |  put_call_ratio
          |FROM option_daily_ratios
          |WHERE underlying_symbol = ? AND underlying_exchange = ? AND underlying_primary_exchange = ? AND trading_date < ?
          |ORDER BY trading_date DESC
          |LIMIT 1
          |""".stripMargin

      val previousPrepared = connection.prepareStatement(previousSql)
      try
        previousPrepared.setString(1, snapshot.underlyingSymbol)
        previousPrepared.setString(2, snapshot.underlyingExchange)
        previousPrepared.setString(3, snapshot.underlyingPrimaryExchange)
        previousPrepared.setString(4, snapshot.tradingDate.toString)

        val previousResult = previousPrepared.executeQuery()
        val previousRow =
          if previousResult.next() then
            Some(
              (
                LocalDate.parse(previousResult.getString("trading_date")),
                previousResult.getLong("total_put_open_interest"),
                previousResult.getLong("total_call_open_interest"),
                Option(previousResult.getString("put_call_ratio")).flatMap(parseBigDecimal)
              )
            )
          else
            None
        previousResult.close()

        val previousTradingDate = previousRow.map(_._1)
        val previousPutOpenInterest = previousRow.map(_._2)
        val previousCallOpenInterest = previousRow.map(_._3)
        val previousPutCallRatio = previousRow.flatMap(_._4)
        val putOpenInterestChange = previousPutOpenInterest.map(totalPutOpenInterest - _)
        val callOpenInterestChange = previousCallOpenInterest.map(totalCallOpenInterest - _)
        val ratioChange = for
          current <- putCallRatio
          previous <- previousPutCallRatio
        yield current - previous
        val ratioPctChange = for
          current <- putCallRatio
          previous <- previousPutCallRatio
          if previous != 0
        yield (current / previous) - BigDecimal(1)

        DailyRatioRow(
          tradingDate = snapshot.tradingDate,
          snapshotAt = snapshot.snapshotAt,
          exchange = snapshot.exchange,
          source = snapshot.source,
          underlyingSymbol = snapshot.underlyingSymbol,
          underlyingCurrency = snapshot.underlyingCurrency,
          underlyingExchange = snapshot.underlyingExchange,
          underlyingPrimaryExchange = snapshot.underlyingPrimaryExchange,
          underlyingConId = snapshot.underlyingConId,
          underlyingPrice = snapshot.underlyingPrice,
          totalPutOpenInterest = totalPutOpenInterest,
          totalCallOpenInterest = totalCallOpenInterest,
          putCallRatio = putCallRatio,
          contractCount = contractCount,
          previousTradingDate = previousTradingDate,
          previousPutOpenInterest = previousPutOpenInterest,
          previousCallOpenInterest = previousCallOpenInterest,
          previousPutCallRatio = previousPutCallRatio,
          putOpenInterestChange = putOpenInterestChange,
          callOpenInterestChange = callOpenInterestChange,
          ratioChange = ratioChange,
          ratioPctChange = ratioPctChange,
          lastRecordedAt = snapshot.publishedAt
        )
      finally previousPrepared.close()
    finally aggregatePrepared.close()

  private def upsertDailyRatio(
      connection: Connection,
      row: DailyRatioRow,
      kafkaKey: Option[String],
      topic: String,
      partition: Int,
      offset: Long
  ): Unit =
    val sql =
      """
        |INSERT INTO option_daily_ratios (
        |  trading_date, snapshot_at, exchange, source, underlying_symbol, underlying_currency,
        |  underlying_exchange, underlying_primary_exchange, underlying_con_id, underlying_price,
        |  total_put_open_interest, total_call_open_interest, put_call_ratio, contract_count,
        |  previous_trading_date, previous_put_open_interest, previous_call_open_interest,
        |  previous_put_call_ratio, put_open_interest_change, call_open_interest_change,
        |  ratio_change, ratio_pct_change, last_recorded_at, stored_at,
        |  kafka_key, kafka_topic, kafka_partition, kafka_offset
        |) VALUES (
        |  ?, ?, ?, ?, ?, ?,
        |  ?, ?, ?, ?,
        |  ?, ?, ?, ?,
        |  ?, ?, ?,
        |  ?, ?, ?,
        |  ?, ?, ?, ?,
        |  ?, ?, ?, ?
        |)
        |ON CONFLICT(trading_date, underlying_symbol, underlying_exchange, underlying_primary_exchange) DO UPDATE SET
        |  snapshot_at = excluded.snapshot_at,
        |  exchange = excluded.exchange,
        |  source = excluded.source,
        |  underlying_currency = excluded.underlying_currency,
        |  underlying_con_id = excluded.underlying_con_id,
        |  underlying_price = excluded.underlying_price,
        |  total_put_open_interest = excluded.total_put_open_interest,
        |  total_call_open_interest = excluded.total_call_open_interest,
        |  put_call_ratio = excluded.put_call_ratio,
        |  contract_count = excluded.contract_count,
        |  previous_trading_date = excluded.previous_trading_date,
        |  previous_put_open_interest = excluded.previous_put_open_interest,
        |  previous_call_open_interest = excluded.previous_call_open_interest,
        |  previous_put_call_ratio = excluded.previous_put_call_ratio,
        |  put_open_interest_change = excluded.put_open_interest_change,
        |  call_open_interest_change = excluded.call_open_interest_change,
        |  ratio_change = excluded.ratio_change,
        |  ratio_pct_change = excluded.ratio_pct_change,
        |  last_recorded_at = excluded.last_recorded_at,
        |  stored_at = excluded.stored_at,
        |  kafka_key = excluded.kafka_key,
        |  kafka_topic = excluded.kafka_topic,
        |  kafka_partition = excluded.kafka_partition,
        |  kafka_offset = excluded.kafka_offset
        |""".stripMargin

    val prepared = connection.prepareStatement(sql)
    try
      prepared.setString(1, row.tradingDate.toString)
      prepared.setString(2, row.snapshotAt.toString)
      prepared.setString(3, row.exchange)
      prepared.setString(4, row.source)
      prepared.setString(5, row.underlyingSymbol)
      prepared.setString(6, row.underlyingCurrency.orNull)
      prepared.setString(7, row.underlyingExchange)
      prepared.setString(8, row.underlyingPrimaryExchange)
      row.underlyingConId match
        case Some(value) => prepared.setLong(9, value)
        case None => prepared.setNull(9, java.sql.Types.BIGINT)
      prepared.setString(10, row.underlyingPrice.map(_.toString).orNull)
      prepared.setLong(11, row.totalPutOpenInterest)
      prepared.setLong(12, row.totalCallOpenInterest)
      prepared.setString(13, row.putCallRatio.map(_.toString).orNull)
      prepared.setLong(14, row.contractCount)
      prepared.setString(15, row.previousTradingDate.map(_.toString).orNull)
      row.previousPutOpenInterest match
        case Some(value) => prepared.setLong(16, value)
        case None => prepared.setNull(16, java.sql.Types.BIGINT)
      row.previousCallOpenInterest match
        case Some(value) => prepared.setLong(17, value)
        case None => prepared.setNull(17, java.sql.Types.BIGINT)
      prepared.setString(18, row.previousPutCallRatio.map(_.toString).orNull)
      row.putOpenInterestChange match
        case Some(value) => prepared.setLong(19, value)
        case None => prepared.setNull(19, java.sql.Types.BIGINT)
      row.callOpenInterestChange match
        case Some(value) => prepared.setLong(20, value)
        case None => prepared.setNull(20, java.sql.Types.BIGINT)
      prepared.setString(21, row.ratioChange.map(_.toString).orNull)
      prepared.setString(22, row.ratioPctChange.map(_.toString).orNull)
      prepared.setString(23, row.lastRecordedAt.toString)
      prepared.setString(24, Instant.now().toString)
      prepared.setString(25, kafkaKey.orNull)
      prepared.setString(26, topic)
      prepared.setInt(27, partition)
      prepared.setLong(28, offset)
      prepared.executeUpdate()
    finally prepared.close()

  private def parseSnapshot(value: String): Option[OptionOiSnapshot] =
    Try(ujson.read(value)).toOption.flatMap { json =>
      for
        exchange <- stringField(json, "exchange")
        source <- stringField(json, "source")
        snapshotAt <- instantField(json, "snapshotAt")
        underlyingSymbol <- stringField(json, "underlyingSymbol")
        underlyingExchange <- stringField(json, "underlyingExchange")
        underlyingPrimaryExchange = stringField(json, "underlyingPrimaryExchange").getOrElse("")
        symbol <- stringField(json, "symbol")
        expiry <- stringField(json, "expiry")
        strike <- bigDecimalField(json, "strike")
        right <- stringField(json, "right")
      yield OptionOiSnapshot(
        event = stringField(json, "event").getOrElse("ibkr.option_oi_snapshot"),
        exchange = exchange,
        source = source,
        snapshotAt = snapshotAt,
        underlyingSymbol = underlyingSymbol,
        underlyingCurrency = stringField(json, "underlyingCurrency").filter(_.nonEmpty),
        underlyingExchange = underlyingExchange,
        underlyingPrimaryExchange = underlyingPrimaryExchange,
        underlyingConId = longField(json, "underlyingConId"),
        underlyingPrice = bigDecimalField(json, "underlyingPrice"),
        symbol = symbol,
        currency = stringField(json, "currency").filter(_.nonEmpty),
        optionExchange = stringField(json, "optionExchange").filter(_.nonEmpty),
        chainExchange = stringField(json, "chainExchange").filter(_.nonEmpty),
        tradingClass = stringField(json, "tradingClass").filter(_.nonEmpty),
        multiplier = stringField(json, "multiplier").filter(_.nonEmpty),
        conId = longField(json, "conId"),
        expiry = expiry,
        strike = strike,
        right = right.toUpperCase,
        optionOpenInterest = longField(json, "optionOpenInterest"),
        callOpenInterest = longField(json, "callOpenInterest"),
        putOpenInterest = longField(json, "putOpenInterest"),
        bid = bigDecimalField(json, "bid"),
        ask = bigDecimalField(json, "ask"),
        last = bigDecimalField(json, "last"),
        close = bigDecimalField(json, "close"),
        volume = longField(json, "volume"),
        openInterest = longField(json, "openInterest"),
        underlyingMid = bigDecimalField(json, "underlyingMid"),
        sourceTimestamp = instantField(json, "sourceTimestamp"),
        publishedAt = instantField(json, "publishedAt").getOrElse(snapshotAt)
      )
    }

  private def parseBigDecimal(value: String): Option[BigDecimal] =
    Try(BigDecimal(value)).toOption

  private def stringField(json: ujson.Value, name: String): Option[String] =
    json.obj.get(name).collect { case ujson.Str(value) if value.nonEmpty => value }

  private def longField(json: ujson.Value, name: String): Option[Long] =
    json.obj.get(name).flatMap {
      case ujson.Num(value) if !value.isNaN => Some(value.toLong)
      case ujson.Str(value) if value.nonEmpty => value.toLongOption
      case _ => None
    }

  private def bigDecimalField(json: ujson.Value, name: String): Option[BigDecimal] =
    json.obj.get(name).flatMap {
      case ujson.Num(value) if !value.isNaN => Some(BigDecimal(value))
      case ujson.Str(value) if value.nonEmpty => parseBigDecimal(value)
      case _ => None
    }

  private def instantField(json: ujson.Value, name: String): Option[Instant] =
    stringField(json, name).flatMap(value => Try(Instant.parse(value)).toOption)
