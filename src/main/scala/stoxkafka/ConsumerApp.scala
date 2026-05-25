package stoxkafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util.{Collections, Properties}

object ConsumerApp:
  private val bootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
  private val topic = sys.env.getOrElse("KAFKA_TOPIC", "learning-events")
  private val groupId = sys.env.getOrElse("KAFKA_GROUP_ID", "learning-consumer")

  def main(args: Array[String]): Unit =
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.deserializer", classOf[StringDeserializer].getName)
    props.put("value.deserializer", classOf[StringDeserializer].getName)
    props.put("group.id", groupId)
    props.put("auto.offset.reset", "earliest")
    props.put("enable.auto.commit", "true")

    val consumer = KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList(topic))

    sys.addShutdownHook {
      consumer.wakeup()
    }

    println(s"listening topic=$topic group=$groupId bootstrapServers=$bootstrapServers")

    try
      while true do
        val records = consumer.poll(Duration.ofMillis(1000))
        records.forEach { record =>
          println(
            s"received key=${record.key()} value=${record.value()} partition=${record.partition()} offset=${record.offset()}"
          )
        }
    catch
      case _: org.apache.kafka.common.errors.WakeupException =>
        println("consumer stopped")
    finally
      consumer.close()

