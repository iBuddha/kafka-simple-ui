package kafka.authorization.manager.actor

import akka.actor.{ActorLogging, Terminated}
import akka.event.LoggingReceive
import kafka.authorization.manager.model.TopicPartition
import kafka.authorization.manager.model.exception.{NoSuchTopicException, RequestExecutionException}
import kafka.authorization.manager.model.request._
import kafka.authorization.manager.utils.ConsumerCreator
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * Created by xhuang on 21/04/2017.
  */
class MetadataActor(private val bootstrapServers: String) extends KafkaClientActor with ActorLogging {

  var consumer: KafkaConsumer[Array[Byte], Array[Byte]] = null

  override def preStart(): Unit = {
    super.preStart()
    consumer = ConsumerCreator.newStandAloneConsumer(bootstrapServers)
    log.info("MetadataActor started")
  }


  override def postStop(): Unit = {
    super.postStop()
    consumer.close()
    log.info("MetadataActor is stopped.")
  }

  override def receive: Receive = LoggingReceive {
    case r@DescribeTopicRequest(id, topic) => sender ! DescribeTopicResponse(r, Try {
      val allTopics = consumer.listTopics()
      if (!allTopics.containsKey(topic)) throw new NoSuchTopicException(topic)
      val partitionInfo = allTopics.get(topic).toList
      val partitions = partitionInfo.map(p => new org.apache.kafka.common.TopicPartition(r.topic, p.partition()))
      val beginOffsets = mapAsScalaMap(consumer.beginningOffsets(partitions)).map {
        case (tp, offset) => (TopicPartition(tp.topic(), tp.partition()), offset.toLong)
      }.toMap

      val endOffsets = mapAsScalaMap(consumer.endOffsets(partitions)).map {
        case (tp, offset) => (TopicPartition(tp.topic(), tp.partition()), offset.toLong)
      }.toMap
      TopicMeta(topic, partitionInfo, beginOffsets, endOffsets)
    })

    case r@GetPartitionRequest(id, topic) => sender ! GetPartitionResponse(r, Try{
      Option(consumer.partitionsFor(topic)).map(_.size()).getOrElse(throw new NoSuchTopicException(topic))
    })

    case r@TopicExistRequest(id, topic) => {
      sender ! TopicExistResponse(r, consumer.listTopics().containsKey(topic))
    }
    case r: ListTopicsRequest => sender ! ListTopicResponse(r, Try{consumer.listTopics().map(_._1).toList})
  }
}

