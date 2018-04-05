package kafka.authorization.manager.model.request

import kafka.authorization.manager.model.TopicPartition

import scala.util.Try


sealed trait OffsetRequest extends KafkaActorRequest

/**
  * 返回一个offset，它所指向消息的timestamp是第一个大于或等于 @param ts 的消息
  *
  * @param topicPartition
  * @param ts
  */
case class FindPartitionOffsetRequest(id:Long, topicPartition: TopicPartition, ts: Long) extends  OffsetRequest

case class FindPartitionOffsetResponse(request: FindPartitionOffsetRequest, offsetBehindTs: Try[OffsetBehindTs]) extends KafkaActorResponse

//找到一个topic的各个分区在大于等于某个timestamp的第一条消息的offset
case class FindTopicOffsetRequest(id: Long, topic: String, ts: Long) extends OffsetRequest

case class FindTopicOffsetResponse(request: FindTopicOffsetRequest, partitionOffset: Try[Map[Int, OffsetBehindTs]]) extends KafkaActorResponse

case class FindPartitionOffsetDiffRequest(id: Long, tp: TopicPartition, from: Long, to: Long) extends OffsetRequest {
  require(from < to)
}

case class FindPartitionOffsetDiffResponse(request: FindPartitionOffsetDiffRequest, start: OffsetBehindTs, end: OffsetBehindTs) extends KafkaActorResponse

case class FindTopicOffsetDiffRequest(id: Long, topic: String, from: Long, to: Long) extends OffsetRequest {
  require(from < to)
}

case class FindTopicOffsetDiffResponse(request: FindTopicOffsetDiffRequest, start: Map[Int, OffsetBehindTs], end: Map[Int, OffsetBehindTs]) extends KafkaActorResponse {

  require(start.keySet &~ end.keySet isEmpty)

  def sumDiff: Long = end.values.map(_.offset).sum - start.values.map(_.offset).sum

  def partitionDiff(p: Int): Long = end(p).offset - start(p).offset
}

/**
  * 在某个timestamp之后的第一条消息的offset。
  * 注意，这条消息可能是不存在的，比如此ts在当前partition最后一条消息的ts之后的情况，或者当前partition没有数据的情况。
  * 这点与org.apache.kafka.common.OffsetAndTimestamp不同。OffsetAndTimestamp一定是某条消息真实的offset和这条消息真实的timestamp``
  *
  * @param offset
  * @param ts
  */
case class OffsetBehindTs(offset: Long, ts: Long)

