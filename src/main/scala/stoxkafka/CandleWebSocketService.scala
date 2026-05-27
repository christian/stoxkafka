package stoxkafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.headers.`Access-Control-Allow-Origin`
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpHeader}
import org.apache.pekko.http.scaladsl.model.ws.{Message, TextMessage}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.stream.{OverflowStrategy, QueueOfferResult}
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source, SourceQueueWithComplete}

import java.nio.file.Files
import java.nio.file.Paths
import java.sql.{Connection, DriverManager}
import java.time.Duration
import java.time.{Instant, LocalDate, ZoneOffset}
import java.util.{Collections, Properties, UUID}
import java.util.concurrent.{ConcurrentHashMap, Executors}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

object CandleWebSocketService:
  private val bootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
  private val inputTopic = sys.env.getOrElse("KAFKA_INPUT_TOPIC", "market.candles.1m")
  private val groupId = sys.env.getOrElse("KAFKA_GROUP_ID", "candle-websocket-service")
  private val host = sys.env.getOrElse("WEBSOCKET_HOST", "0.0.0.0")
  private val port = sys.env.get("WEBSOCKET_PORT").flatMap(_.toIntOption).getOrElse(9000)
  private val sqlitePath = Paths.get(sys.env.getOrElse("CANDLE_SQLITE_DB", "data/live/candles.db"))

  final case class Client(
      id: String,
      queue: SourceQueueWithComplete[Message],
      symbol: Option[String],
      timeframe: Option[String]
  )

  def main(args: Array[String]): Unit =
    given system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "stox-kafka-websocket")
    given ec: ExecutionContext = system.executionContext

    val clients = ConcurrentHashMap[String, Client]()
    val consumer = KafkaConsumer[String, String](consumerProperties())
    val consumerExecutor = Executors.newSingleThreadExecutor()
    Files.createDirectories(sqlitePath.getParent)
    Class.forName("org.sqlite.JDBC")
    val dbUrl = s"jdbc:sqlite:${sqlitePath.toAbsolutePath}"

    sys.addShutdownHook {
      Try(consumer.wakeup())
      consumerExecutor.shutdownNow()
      system.terminate()
    }

    consumerExecutor.submit(new Runnable:
      override def run(): Unit =
        consumeCandles(consumer, clients)
    )

    val route =
      path("health") {
        complete(StatusCodes.OK -> "ok")
      } ~
        path("api" / "candles") {
          parameters("symbol", "timeframe".?, "date".?) { (symbol, timeframe, date) =>
            complete {
              val resolvedDate = date.flatMap(value => Try(LocalDate.parse(value)).toOption).getOrElse(LocalDate.now(ZoneOffset.UTC))
              val resolvedTimeframe = timeframe.getOrElse("1m")
              val candles = loadCandlesFromSqlite(dbUrl, symbol, resolvedTimeframe, resolvedDate)
              val payload = ujson.write(ujson.Arr.from(candles.map { candle =>
                ujson.Obj(
                  "event" -> candle.event,
                  "exchange" -> candle.exchange,
                  "symbol" -> candle.symbol,
                  "currency" -> candle.currency.getOrElse(""),
                  "timeframe" -> candle.timeframe,
                  "openTime" -> candle.openTime.toString,
                  "closeTime" -> candle.closeTime.toString,
                  "open" -> candle.open,
                  "high" -> candle.high,
                  "low" -> candle.low,
                  "close" -> candle.close,
                  "volume" -> candle.volume,
                  "tickCount" -> candle.tickCount,
                  "isFinal" -> candle.isFinal,
                  "publishedAt" -> candle.publishedAt
                )
              }))
              val headers = List[HttpHeader](`Access-Control-Allow-Origin`.*)
              org.apache.pekko.http.scaladsl.model.HttpResponse(
                entity = HttpEntity(ContentTypes.`application/json`, payload),
                headers = headers
              )
            }
          }
        } ~
        path("ws" / "candles") {
          parameters("symbol".?, "timeframe".?) { (symbol, timeframe) =>
            handleWebSocketMessages(webSocketFlow(clients, symbol, timeframe))
          }
        }

    Http()
      .newServerAt(host, port)
      .bind(route)
      .onComplete {
        case Success(binding) =>
          val address = binding.localAddress
          println(s"candle websocket service listening http://${address.getHostString}:${address.getPort}")
          println(s"websocket path ws://localhost:$port/ws/candles?symbol=ASML&timeframe=1m")

        case Failure(error) =>
          println(s"failed to bind websocket service: ${error.getMessage}")
          system.terminate()
      }

    scala.concurrent.Await.result(system.whenTerminated, scala.concurrent.duration.Duration.Inf)

  private def webSocketFlow(
      clients: ConcurrentHashMap[String, Client],
      symbol: Option[String],
      timeframe: Option[String]
  )(using system: ActorSystem[Nothing]): Flow[Message, Message, ?] =
    val clientId = UUID.randomUUID().toString
    val normalizedSymbol = symbol.map(_.trim.toUpperCase).filter(_.nonEmpty)
    val normalizedTimeframe = timeframe.map(_.trim).filter(_.nonEmpty)

    val sink = Sink.foreach[Message] {
      case TextMessage.Strict("ping") =>
        ()
      case TextMessage.Strict(text) =>
        println(s"ignored client message client=$clientId text=$text")
      case TextMessage.Streamed(stream) =>
        stream.runWith(Sink.ignore)
      case _ =>
        ()
    }

    val source = Source
      .queue[Message](256, OverflowStrategy.dropHead)
      .mapMaterializedValue { queue =>
        clients.put(clientId, Client(clientId, queue, normalizedSymbol, normalizedTimeframe))
        println(s"websocket client connected id=$clientId symbol=${normalizedSymbol.getOrElse("*")} timeframe=${normalizedTimeframe.getOrElse("*")}")
        queue.offer(TextMessage.Strict(welcomeMessage(normalizedSymbol, normalizedTimeframe)))
        queue
      }
      .watchTermination() { (queue, done) =>
        given ExecutionContext = system.executionContext
        done.onComplete { _ =>
          clients.remove(clientId)
          queue.complete()
          println(s"websocket client disconnected id=$clientId")
        }
        queue
      }

    Flow.fromSinkAndSourceCoupled(sink, source)

  private def consumeCandles(
      consumer: KafkaConsumer[String, String],
      clients: ConcurrentHashMap[String, Client]
  ): Unit =
    consumer.subscribe(Collections.singletonList(inputTopic))
    println(s"websocket candle consumer input=$inputTopic group=$groupId bootstrapServers=$bootstrapServers")

    try
      while true do
        val records = consumer.poll(Duration.ofMillis(500))
        records.asScala.foreach { record =>
          val value = record.value()
          val parsed = parseRoutingFields(value)

          clients.values().asScala.foreach { client =>
            if shouldSend(client, parsed) then
              client.queue.offer(TextMessage.Strict(value)).onComplete {
                case Success(QueueOfferResult.Enqueued) => ()
                case Success(QueueOfferResult.Dropped) =>
                  println(s"dropped candle for websocket client=${client.id}")
                case Success(QueueOfferResult.Failure(error)) =>
                  println(s"failed websocket offer client=${client.id} error=${error.getMessage}")
                  clients.remove(client.id)
                case Success(QueueOfferResult.QueueClosed) =>
                  clients.remove(client.id)
                case Failure(error) =>
                  println(s"failed websocket offer future client=${client.id} error=${error.getMessage}")
                  clients.remove(client.id)
              }(using ExecutionContext.global)
          }
        }
    catch
      case _: org.apache.kafka.common.errors.WakeupException =>
        println("websocket candle consumer stopping")
    finally
      consumer.close()

  private def shouldSend(client: Client, fields: Option[(String, String)]): Boolean =
    fields match
      case Some((symbol, timeframe)) =>
        client.symbol.forall(_ == symbol.toUpperCase) &&
          client.timeframe.forall(_ == timeframe)
      case None =>
        client.symbol.isEmpty && client.timeframe.isEmpty

  private def parseRoutingFields(value: String): Option[(String, String)] =
    Try(ujson.read(value)).toOption.flatMap { json =>
      for
        symbol <- stringField(json, "symbol")
        timeframe <- stringField(json, "timeframe")
      yield (symbol, timeframe)
    }

  private def stringField(json: ujson.Value, name: String): Option[String] =
    json.obj.get(name).collect { case ujson.Str(value) if value.nonEmpty => value }

  private def welcomeMessage(symbol: Option[String], timeframe: Option[String]): String =
    ujson.write(
      ujson.Obj(
        "event" -> "websocket.connected",
        "symbol" -> symbol.getOrElse("*"),
        "timeframe" -> timeframe.getOrElse("*"),
        "topic" -> inputTopic
      )
    )

  private def consumerProperties(): Properties =
    val props = Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.deserializer", classOf[StringDeserializer].getName)
    props.put("value.deserializer", classOf[StringDeserializer].getName)
    props.put("group.id", groupId)
    props.put("auto.offset.reset", "latest")
    props.put("enable.auto.commit", "true")
    props

  private final case class HistoryCandle(
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
  )

  private def loadCandlesFromSqlite(
      dbUrl: String,
      symbol: String,
      timeframe: String,
      date: LocalDate
  ): Seq[HistoryCandle] =
    val sql =
      """
        |SELECT event, exchange, symbol, currency, timeframe, open_time, close_time,
        |       open, high, low, close, volume, tick_count, is_final, published_at
        |FROM candles
        |WHERE trading_date = ? AND symbol = ? AND timeframe = ?
        |ORDER BY open_time ASC
        |""".stripMargin

    val connection = DriverManager.getConnection(dbUrl)
    try
      val prepared = connection.prepareStatement(sql)
      try
        prepared.setString(1, date.toString)
        prepared.setString(2, symbol)
        prepared.setString(3, timeframe)

        val resultSet = prepared.executeQuery()
        try
          val rows = scala.collection.mutable.ArrayBuffer.empty[HistoryCandle]
          while resultSet.next() do
            rows += HistoryCandle(
              event = resultSet.getString("event"),
              exchange = resultSet.getString("exchange"),
              symbol = resultSet.getString("symbol"),
              currency = Option(resultSet.getString("currency")).filter(_.nonEmpty),
              timeframe = resultSet.getString("timeframe"),
              openTime = Instant.parse(resultSet.getString("open_time")),
              closeTime = Instant.parse(resultSet.getString("close_time")),
              open = resultSet.getString("open"),
              high = resultSet.getString("high"),
              low = resultSet.getString("low"),
              close = resultSet.getString("close"),
              volume = resultSet.getString("volume"),
              tickCount = resultSet.getLong("tick_count"),
              isFinal = resultSet.getInt("is_final") == 1,
              publishedAt = resultSet.getString("published_at")
            )
          rows.toSeq
        finally
          resultSet.close()
      finally
        prepared.close()
    finally
      connection.close()
