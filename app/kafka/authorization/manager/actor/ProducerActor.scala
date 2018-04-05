package kafka.authorization.manager.actor

import java.util.Properties
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Try

/**
  * Created by xhuang on 30/06/2017.
  */
class ProducerActor(bootstrapServer: String, timeoutMillis: Long) extends Actor with ActorLogging {

  import ProducerActor._

  private var producer: Option[KafkaProducer[String, String]] = None


  override def receive: Receive = {
    case r: ProducerRecord[String, String] =>
      sender ! Try{producer.get.send(r).get(timeoutMillis, TimeUnit.MILLISECONDS)}
  }


  override def preStart() = {
    super.preStart()
    initProducer()
  }

  override def postStop() = {
    super.postStop()
    closeProducer()
  }


  private def initProducer() = {
    val config = new Properties
    config.put("client.id", clientId) //这里需要修改为自己的client id。可以自己起一个，只要能用于区分不同的client即可
    config.put("bootstrap.servers", bootstrapServer)
    config.put("acks", "all")
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    producer = Some(new KafkaProducer(config))
  }

  private def closeProducer() = Try{producer.foreach(_.close)}
}

object ProducerActor{
  val clientId = "kafka-authorization-manager-producer"
}
