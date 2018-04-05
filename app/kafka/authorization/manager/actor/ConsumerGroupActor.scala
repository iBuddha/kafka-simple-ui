package kafka.authorization.manager.actor

import java.util.Properties

import akka.actor.{Actor, ActorLogging}
import akka.event.LoggingReceive
import kafka.admin.AdminClient
import kafka.admin.ConsumerGroupCommand.LogEndOffsetResult
import kafka.authorization.manager.model._
import kafka.authorization.manager.model.request.{DescribeConsumerGroupRequest, ListConsumerGroupRequest, ListConsumerGroupResponse}
import kafka.authorization.manager.utils.SimpleAdminClient
import kafka.common.TopicAndPartition
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Created by xhuang on 28/04/2017.
  * TODO: 需要把admin功能搞出来, 建立一个AdminActor
  *
  * 获取consumer group的信息
  */
class ConsumerGroupActor(bootstrapServers: String) extends Actor with ActorLogging {

  var adminClient: SimpleAdminClient = null
  var outerConsumer: Option[KafkaConsumer[String, String]] = None

  override def receive: Receive = LoggingReceive {
    case r@ListConsumerGroupRequest => sender !
      ListConsumerGroupResponse(r, Try {
        adminClient.listAllGroupsFlattened().map(x => x.groupId)
      })
    case DescribeConsumerGroupRequest(id, targetGroup) =>
      log.debug(s"getting description for $targetGroup")
      sender ! Try{getGroupDescription(targetGroup)}
    case request => log.error(s"received unknown request $request")
  }


  override def preStart(): Unit = {
    super.preStart()
    adminClient = createAdminClient()
    log.info("consumer group actor started")
  }


  override def postStop(): Unit = {
    super.postStop()
    if (adminClient != null)
      adminClient.close()
    closeConsumer()
    log.info("ConsumerGroupActor stopped.")
  }

  private def createAdminClient(): SimpleAdminClient = {
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    SimpleAdminClient.create(props)
  }


  private def getGroupDescription(group: String): GroupDescription = {
    val description = adminClient.describeConsumerGroup(group)
    description.state match {
      case "Stable" => try {
        val consumerSummaries = description.consumers
          rebuildConsumer(group)
          val consumer = getConsumer()
          val allDescriptions = scala.collection.mutable.Map.empty[TopicAndPartition, Option[PartitionDescription]]
          consumerSummaries.foreach { consumerSummary =>
            val topicPartitions = consumerSummary.assignment.map(tp => TopicAndPartition(tp.topic, tp.partition))
            val partitionOffsets = topicPartitions.flatMap { topicPartition =>
              Option(consumer.committed(new TopicPartition(topicPartition.topic, topicPartition.partition))).map { offsetAndMetadata => topicPartition -> offsetAndMetadata.offset
              }
            }.toMap

            val topicPartitionDescriptions = getTopicPartitionDescription(group,
              topicPartitions,
              partitionOffsets.get,
              _ => Some(PartitionOwner(consumerSummary.memberId, consumerSummary.clientId, consumerSummary.clientHost))
            )
            allDescriptions ++= topicPartitionDescriptions
          }
          GroupDescription(group, "Stable", allDescriptions.toMap)
        } finally {
          closeConsumer()
        }
      case other => GroupDescription(group, other, Map.empty)

    }
  }

  /**
    *
    * @return 如果value为None，说明找不到关于这个TopicPartition的信息，可能是因为这个partition不存在
    */
  private def getTopicPartitionDescription(group: String,
                                           topicPartitions: Seq[TopicAndPartition],
                                           getPartitionOffset: TopicAndPartition => Option[Long],
                                           getOwner: TopicAndPartition => Option[PartitionOwner]): Map[TopicAndPartition, Option[PartitionDescription]] = {
    val result = scala.collection.mutable.Map.empty[TopicAndPartition, Option[PartitionDescription]]
    topicPartitions.sortBy { case topicPartition => topicPartition.partition }
      .foreach { topicPartition =>
        result.put(topicPartition,
          getPartitionDescription(group, topicPartition.topic, topicPartition.partition, getPartitionOffset(topicPartition),
            getOwner(topicPartition)))
      }
    result.toMap
  }

  protected def describeTopicPartition(group: String, topicPartitions: Seq[TopicAndPartition],
                                       getPartitionOffset: TopicAndPartition => Option[Long],
                                       getOwner: TopicAndPartition => Option[String],
                                       builder: StringBuilder): Unit = {
    topicPartitions
      .sortBy { case topicPartition => topicPartition.partition }
      .foreach { topicPartition =>
        describePartition(group, topicPartition.topic, topicPartition.partition, getPartitionOffset(topicPartition),
          getOwner(topicPartition), builder)
      }
  }


  protected def getDescribeHeader(): String = {
    "%-30s %-30s %-10s %-15s %-15s %-15s %s".format("GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "OWNER")
  }

  private def describePartition(group: String,
                                topic: String,
                                partition: Int,
                                offsetOpt: Option[Long],
                                ownerOpt: Option[String],
                                builder: StringBuilder): String = {
    def addLeo(logEndOffset: Option[Long]): Unit = {
      val lag = offsetOpt.filter(_ != -1).flatMap(offset => logEndOffset.map(_ - offset))
      builder.append("%-30s %-30s %-10s %-15s %-15s %-15s %s".format(group, topic, partition, offsetOpt.getOrElse("unknown"), logEndOffset.getOrElse("unknown"), lag.getOrElse("unknown"), ownerOpt.getOrElse("none")))
        .append("\n")
    }

    getLogEndOffset(topic, partition) match {
      case LogEndOffsetResult.LogEndOffset(logEndOffset) => addLeo(Some(logEndOffset))
      case LogEndOffsetResult.Unknown => addLeo(None)
      case LogEndOffsetResult.Ignore =>
    }
    builder.append("\n").toString
  }


  /**
    *
    * @return 如果没有这个topic, partition, 会返回None. currentOffset, leo和log都可能为None，表示unknown
    */
  private def getPartitionDescription(group: String,
                                      topic: String,
                                      partition: Int,
                                      committedOffsetOpt: Option[Long],
                                      ownerOpt: Option[PartitionOwner]): Option[PartitionDescription] = {
    def addLeo(logEndOffset: Option[Long]): PartitionDescription = {
      val lag = committedOffsetOpt.filter(_ != -1).flatMap(offset => logEndOffset.map(_ - offset))
      PartitionDescription(group, topic, partition, committedOffsetOpt, logEndOffset, lag, ownerOpt)
    }

    getLogEndOffset(topic, partition) match {
      case LogEndOffsetResult.LogEndOffset(logEndOffset) => Some(addLeo(Some(logEndOffset)))
      case LogEndOffsetResult.Unknown => Some(addLeo(None))
      case LogEndOffsetResult.Ignore => None
    }
  }

  protected def getLogEndOffset(topic: String, partition: Int): LogEndOffsetResult = {
    val consumer = getConsumer()
    val topicPartition = new TopicPartition(topic, partition)
    consumer.assign(List(topicPartition).asJava)
    consumer.seekToEnd(List(topicPartition).asJava)
    val logEndOffset = consumer.position(topicPartition)
    LogEndOffsetResult.LogEndOffset(logEndOffset)
  }

  private def getConsumer() = {
    outerConsumer.get
  }

  private def closeConsumer() = Try {
    if (outerConsumer.isDefined) {
      log.info("close consumer")
      outerConsumer.get.close()
      outerConsumer = None
    }
  }


  private def rebuildConsumer(groupId: String) = {
    closeConsumer()
    val properties = new Properties()
    val deserializer = (new StringDeserializer).getClass.getName
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)

    outerConsumer = Some(new KafkaConsumer(properties))
  }
}
