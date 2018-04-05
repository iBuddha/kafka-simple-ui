package kafka.authorization.manager.utils

import java.io.{Closeable, IOException}
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.ActorRef
import kafka.authorization.manager.utils.client.ExclusiveAssignor.Callback
import kafka.authorization.manager.utils.client.{AdminHelper, SimpleKafkaConsumer}
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.common.{Node, TopicPartition}
import org.slf4j.LoggerFactory
import akka.pattern._
import akka.util.Timeout
import com.sun.xml.internal.bind.v2.runtime.Coordinator
import kafka.authorization.manager.model.request.{FindTopicOffsetRequest, FindTopicOffsetResponse}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{JoinGroupResponse, LeaveGroupResponse}

import scala.collection.JavaConversions
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

/**
  * Created by xhuang on 05/07/2017.
  */
class ConsumerGroupManager(bootStrapServers: String) extends Closeable {

  private val logger = LoggerFactory.getLogger(classOf[ConsumerGroupManager])

  private val adminClient: SimpleAdminClient = SimpleAdminClient.createSimplePlaintext(bootStrapServers)

  logger.info("ConsumerGroupManager started")

  private[utils] def getMembersAndTopics(groupId: String, coordinatorOpts: Option[Node]) = {
    val groupSummary = adminClient.describeConsumerGroup(groupId, coordinatorOpts)
    val subscribedTopics = groupSummary.consumers.flatMap(_.assignment.map(_.topic()))
    val members = groupSummary.consumers.map(_.memberId)
    (members, subscribedTopics)
  }

  private[utils] def forceLeave(coordinator: Node, groupId: String, members: List[String]) = {
    members.foreach(adminClient.forceLeave(coordinator, _, groupId))
  }

  def clearCurrentMembers(groupId: String) = {
    val coordinator = adminClient.findCoordinator(groupId)
    val members = getMembersAndTopics(groupId, Some(coordinator))._1
    if (!members.isEmpty) {
      forceLeave(coordinator, groupId, members)
      logger.info(s"cleared current members for $groupId")
    }
  }

  private def clearCurrentMembers(coordinator: Node, groupId: String, exclude: Option[String]) = {
    val (members, _) = getMembersAndTopics(groupId, Some(coordinator))
    val memberExcludeSelf = members.filter(exclude.isEmpty || !_.startsWith(exclude.get))
    forceLeave(coordinator, groupId, memberExcludeSelf)
  }

  def becomeLeader(groupId: String, maxRetries: Int): Unit = {
    val consumer = createConsumer(groupId)
    becomeLeader(consumer, groupId, maxRetries)
    consumer.close()
  }

  private def becomeLeader(consumer: SimpleKafkaConsumer[Array[Byte], Array[Byte]], groupId: String, maxRetries: Int): Unit = {
    var retryNumber = 0
    val coordinatorNode = adminClient.findCoordinator(groupId)
    val topics = getMembersAndTopics(groupId, Some(coordinatorNode))._2
    if (topics.isEmpty) {
      logger.debug(s"group: $groupId current has no subscribed topic")
      return
    }
    consumer.subscribe(JavaConversions.seqAsJavaList(topics))
    val assignedAll = new AtomicBoolean(false)
    consumer.setExclusiveAssignorCallback(new Callback {
      override def onSuccess(): Unit = assignedAll.set(true)
    })

    while (!assignedAll.get() && retryNumber < maxRetries) {
      clearCurrentMembers(coordinatorNode, groupId, Some(ConsumerGroupManager.magicConsumerId))
      consumer.poll(5000)
      retryNumber = retryNumber + 1
    }
    printCurrentAssignment(consumer)
  }

  def triggerRebalance(groupId: String): Unit = {
    logger.info(s"triggering rebalance for $groupId")
    val joinGroupRequest = AdminHelper.consumerJoinGroupRequest(ConsumerGroupManager.magicTopic, groupId)
    val coordinator = adminClient.findCoordinator(groupId)
    val responseStruct = adminClient.send(coordinator, ApiKeys.JOIN_GROUP, joinGroupRequest)
    val response = new JoinGroupResponse(responseStruct)
    if (response.errorCode() == Errors.NONE.code()) {

      val memberId = response.memberId()
      val leaveGroupResponses = new LeaveGroupResponse(adminClient.forceLeave(coordinator, memberId, groupId))
      if (leaveGroupResponses.errorCode() == Errors.NONE.code()) {
        logger.info(s"successfully leave group $groupId")
      } else {
        logger.warn("failed to leave group when trigger rebalance, because: " + Errors.forCode(leaveGroupResponses.errorCode()).message())
      }
    } else {
      val exception = Errors.forCode(response.errorCode()).exception()
      logger.error("failed to join group", exception)
      throw exception
    }
  }

  /**
    * 把一个consumer group的所有成员的进度重置到某个时间点
    *
    * @param groupId
    * @param ts
    * @param maxRetries 会比实际的重试次数要大。这并非一个绝对值。
    */
  def forceReset(offsetLookupActor: ActorRef, groupId: String, ts: Long, maxRetries: Int)(implicit executionContext: ExecutionContext): Boolean = {
    logger.info(s"resetting offset for $groupId to $ts")
    val groupSummary = adminClient.describeConsumerGroup(groupId)
    val topics = groupSummary.subscribedTopics
    if (topics.isEmpty)
      throw new IllegalStateException(s"group $groupId currently subscribed no topic")
    val offsetToCommit = getOffsetsBehindTs(offsetLookupActor, topics, ts, 10000)
    val consumer = createConsumer(groupId)
    try {
      forceCommit(consumer, groupId, topics, maxRetries, offsetToCommit)
      true
    } finally {
      consumer.close()
    }
  }

  def forceReset(offsetLookupActor: ActorRef, groupId: String, topic: String, ts: Long, maxRetries: Int)(implicit executionContext: ExecutionContext): Boolean = {
    logger.info(s"resetting offset for $groupId  $topic to $ts")
    val groupSummary = adminClient.describeConsumerGroup(groupId)
    val subscribed = groupSummary.subscribedTopics

    if (!subscribed.contains(topic) &&
      !(groupSummary.state.equals(ConsumerGroupState.Dead) || groupSummary.state.equals(ConsumerGroupState.Empty)))
      throw new IllegalStateException(s"Consumer group's state is ${groupSummary.state}, but topic to be reset isn't subscribed, so can't reset offset for this consumer group")

    val offsetToCommit = getOffsetsBehindTs(offsetLookupActor, Seq(topic), ts, 10000)
    val consumer = createConsumer(groupId)
    try {
      forceCommit(consumer, groupId, subscribed :+ topic, maxRetries, offsetToCommit)
      true
    } finally {
      consumer.close()
    }
  }

  private def forceCommit(consumer: SimpleKafkaConsumer[_, _], groupId: String, topics: Seq[String], maxRetries: Int, toCommit: Map[TopicPartition, OffsetAndMetadata], coordinatorOpt: Option[Node] = None) = {
    consumer.subscribe(JavaConversions.seqAsJavaList(topics))
    val assignedAll = new AtomicBoolean(false)
    consumer.setExclusiveAssignorCallback(new Callback {
      override def onSuccess(): Unit = assignedAll.set(true)
    })
    var currentRetries = 0
    val coordinatorNode = coordinatorOpt.getOrElse(adminClient.findCoordinator(groupId))
    while (!assignedAll.get() && currentRetries < maxRetries) {
      logger.info(s"trying to reset offset for $groupId, retry count $currentRetries  ....")
      clearCurrentMembers(coordinatorNode, groupId, Some(ConsumerGroupManager.magicConsumerId))
      consumer.poll(5000)
      printCurrentAssignment(consumer)
      currentRetries = currentRetries + 1
    }
    if (currentRetries >= maxRetries)
      throw new RuntimeException(s"retry exhausted when getting leadership of $groupId")
    val javaOffsetToCommit = JavaConversions.mapAsJavaMap(toCommit)
    consumer.commitSync(javaOffsetToCommit)
    logger.info(s"successfully committed offset for $groupId: $toCommit")
    consumer.unsubscribe()
  }

  private def getOffsetsBehindTs(offsetLookupActor: ActorRef, topics: Seq[String], timestamp: Long, timeoutMillis: Long)(implicit executionContext: ExecutionContext) = {
    require(!topics.isEmpty)
    implicit val timeout = Timeout(timeoutMillis millis)
    val responseFuture = Future.sequence(topics.map { topic =>
      (offsetLookupActor ? new FindTopicOffsetRequest(-1, topic, timestamp)).mapTo[FindTopicOffsetResponse]
    })
    val responses = Await.result(responseFuture, timeout.duration)
    val failures = responses.filter(_.partitionOffset.isFailure)
    failures.foreach(f => logger.error("", f.partitionOffset.failed.get))
    if (!failures.isEmpty)
      throw new IOException("Failed to retrieve offsets for " + failures.map(_.request.topic).mkString(","))
    val offsetToCommit = responses.flatMap { response =>
      val topic = response.request.topic
      val partitionAndOffsets = response.partitionOffset.get
      partitionAndOffsets.map {
        case (partitionId, offsetBehindTs) => new TopicPartition(topic, partitionId) -> new OffsetAndMetadata(offsetBehindTs.offset)
      }
    }.toMap
    offsetToCommit
  }

  private def printCurrentAssignment(consumer: SimpleKafkaConsumer[_, _]): Unit = {
    val iter = consumer.assignment().iterator()
    while (iter.hasNext) {
      val tp = iter.next()
      println(tp)
    }
  }


  def close(): Unit = {
    if (adminClient != null)
      adminClient.close()
    logger.info("ConsumerGroupManager closed")
  }

  private def createConsumer(groupId: String) = {
    val props = new Properties
    props.put("bootstrap.servers", bootStrapServers)
    props.put("group.id", groupId)
    props.put("enable.auto.commit", "false")
    props.put("session.timeout.ms", "30000")
    props.put("client.id", ConsumerGroupManager.magicConsumerId)
    props.put("auto.offset.reset", "latest")

    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "kafka.authorization.manager.utils.client.ExclusiveAssignor, org.apache.kafka.clients.consumer.RangeAssignor, org.apache.kafka.clients.consumer.RoundRobinAssignor")
    //    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "kafka.authorization.manager.utils.client.ExclusiveAssignor")

    new SimpleKafkaConsumer[Array[Byte], Array[Byte]](props)
  }


}

object ConsumerGroupManager {
  val magicConsumerId = "consumer-manager-1e3ed23"
  val magicTopic = "a34b1_do_not_write"
}