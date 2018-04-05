package kafka.authorization.manager.actor

import akka.actor.{Actor, ActorLogging}
import kafka.authorization.manager.model.request._
import kafka.authorization.manager.utils.ConsumerCreator
import kafka.authorization.manager.utils.ConsumerOps._
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.util.{Failure, Success, Try}

/**
  * Created by xhuang on 25/04/2017.
  */
class MessageActor(bootstrapServers: String, timeout: Long) extends Actor with ActorLogging {

  private var consumer = ConsumerCreator.newStandAloneConsumer(bootstrapServers)

  override def preStart(): Unit = {
    super.preStart()
    consumer = ConsumerCreator.newStandAloneConsumer(bootstrapServers)
  }

  override def postStop(): Unit = {
    super.postStop()
    consumer.close()
    log.info("MessageActor is stopped.")
  }
  private implicit def toMessage(record: ConsumerRecord[Array[Byte], Array[Byte]]): Message = Message(record.key(), record.value(), record.timestamp(), record.partition(), record.offset())

  override def receive: Receive = {

    case r: MessageRequest =>
      val originSender = sender()
      Try{consumer.message(r.tp, r.offset, timeout)} match {
        case Success(record) => originSender ! MessageResponse(r, Success(record))
        case f: Failure[_] => originSender ! MessageResponse(r, Failure(f.exception))
      }

    case r@MessagesRequest(id, tp, beginOffset, maxCount) => {
      val originSender = sender()
      Try{consumer.messages(tp, beginOffset, maxCount, timeout)} match {
        case Success(records) => originSender ! MessagesResponse(r, Success(records.map(record => toMessage(record))))
        case f: Failure[_] => originSender ! MessagesResponse(r, Failure(f.exception))
      }
    }
  }
}
