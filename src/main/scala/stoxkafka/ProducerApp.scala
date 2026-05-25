package stoxkafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import java.time.Instant
import java.util.Properties
import scala.concurrent.duration.DurationInt

object ProducerApp:
  private val bootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
  private val topic = sys.env.getOrElse("KAFKA_TOPIC", "learning-events")

  def main(args: Array[String]): Unit =
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("acks", "all")

    val producer = KafkaProducer[String, String](props)

    try
      val count = args.headOption.flatMap(_.toIntOption).getOrElse(10)

      (1 to count).foreach { n =>
        val key = s"user-${n % 3}"
        val value = s"""{"event":"page_view","count":$n,"at":"${Instant.now()}"}"""
        val record = ProducerRecord[String, String](topic, key, value)
        val metadata: RecordMetadata = producer.send(record).get()

        println(
          s"sent key=$key value=$value topic=${metadata.topic()} partition=${metadata.partition()} offset=${metadata.offset()}"
        )

        Thread.sleep(500.millis.toMillis)
      }
    finally
      producer.close()

