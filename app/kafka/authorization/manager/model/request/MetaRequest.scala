package kafka.authorization.manager.model.request

import kafka.authorization.manager.model.TopicPartition
import org.apache.kafka.common.{Node, PartitionInfo}

import scala.util.Try

/**
  * Created by xhuang on 27/04/2017.
  */
trait MetadataRequest extends KafkaActorRequest

trait MetadataResponse extends KafkaActorResponse

case class DescribeTopicRequest(id: Long, topic: String) extends MetadataRequest

case class GetPartitionRequest(id: Long, topic: String) extends MetadataRequest

case class GetPartitionResponse(request: GetPartitionRequest, partitionNumber: Try[Int]) extends MetadataResponse

case class DescribeTopicResponse(request: DescribeTopicRequest, result: Try[TopicMeta]) extends MetadataResponse

case class TopicMeta(name: String,
                     partitionInfo: List[PartitionInfo],
                     beginOffsets: Map[TopicPartition, Long],
                     endOffsets: Map[TopicPartition, Long])

//private final Node leader;
//private final Node[] replicas;
//private final Node[] inSyncReplicas;

case class PartitionedTopicMetadata(name: String, partitions: Map[Int, PartitionMetadata])

object PartitionedTopicMetadata {
  def apply(topicMeta: TopicMeta): PartitionedTopicMetadata = {
    val partitionNumber = topicMeta.partitionInfo.size
    val topicName = topicMeta.name
    val allPartitions = Set((0 until partitionNumber) map {
      TopicPartition(topicName, _)
    }: _*)
    require(topicMeta.beginOffsets.keySet == allPartitions)
    require(topicMeta.endOffsets.keySet == allPartitions)

    PartitionedTopicMetadata(topicName,
      topicMeta.partitionInfo.map { info =>
        (info.partition(), {
          val tp = TopicPartition(topicName, info.partition())
          val beginOffset = topicMeta.beginOffsets(tp)
          val endOffset = topicMeta.endOffsets(tp)
          PartitionMetadata(info.partition(), info.leader(), info.replicas(), info.inSyncReplicas(), beginOffset, endOffset)
        })
      }.toMap
    )
  }
}


case class PartitionMetadata(id: Int, leader: Node, replicas: Array[Node], inSyncReplicas: Array[Node], beginOffset: Long, endOffset: Long)

case class TopicExistRequest(id: Long, topic: String) extends KafkaActorRequest

case class TopicExistResponse(request: TopicExistRequest, existed: Boolean)


sealed trait ConsumerGroupRequest extends KafkaActorRequest

case class ListConsumerGroupRequest(id: Long) extends ConsumerGroupRequest

case class ListConsumerGroupResponse(request: ListConsumerGroupRequest.type, groups: Try[List[String]])

case class DescribeConsumerGroupRequest(id: Long, group: String) extends ConsumerGroupRequest

case class DescribeConsumerGroupResponse(request: DescribeConsumerGroupRequest, description: Try[String])

case class WatchConsumerGroupRequest(id: Long, group: String) extends ConsumerGroupRequest

case class StopWatchingConsumerGroupRequest(id: Long, group: String) extends ConsumerGroupRequest
