package kafka.authorization.manager.model.request

import java.sql.Timestamp

import kafka.authorization.manager.model.TopicPartition
import kafka.authorization.manager.utils.TextEncoder.encodeUTF8

import scala.util.Try

/**
  * Created by xhuang on 25/04/2017.
  */
case class MessageRequest(id: Long, tp: TopicPartition, offset: Long) extends KafkaActorRequest
//这里的ts是这个Message本身的timestamp
case class MessageResponse(request: MessageRequest, result: Try[Message]) extends KafkaActorResponse

case class Message(key: Array[Byte], value: Array[Byte], ts: Long, partition: Int, offset: Long)
case class StringMessage(key: String, value: String, ts: String, partition: Int, offset: Long)

object StringMessage {
  def apply(message: Message): StringMessage = StringMessage(
    key = encodeUTF8(message.key),
    value = encodeUTF8(message.value),
    ts = new Timestamp(message.ts).toString,
    partition = message.partition,
    offset = message.offset)
}



case class MessagesRequest(id: Long, tp: TopicPartition, beginOffset: Long, maxCount: Int) extends KafkaActorRequest
case class MessagesResponse(request: MessagesRequest, result: Try[Seq[Message]])
