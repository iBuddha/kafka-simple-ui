package kafka.authorization.manager.utils

/**
  * Created by xhuang on 04/07/2017.
  */

import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.clients._
import org.apache.kafka.clients.consumer.internals.{ConsumerNetworkClient, ConsumerProtocol, RequestFuture}
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.common.errors.DisconnectException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.Selector
import org.apache.kafka.common.protocol.types.Struct
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.{SystemTime, Time, Utils}
import org.apache.kafka.common.{Cluster, KafkaException, Node, TopicPartition}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class SimpleAdminClient(val time: Time,
                        val requestTimeoutMs: Int,
                        val client: ConsumerNetworkClient,
                        val bootstrapBrokers: List[Node]) {
  val logger = LoggerFactory.getLogger(classOf[SimpleAdminClient])

  def send(target: Node,
           api: ApiKeys,
           request: AbstractRequest): Struct = {
    var future: RequestFuture[ClientResponse] = null

    future = client.send(target, api, request)
    client.poll(future)

    if (future.succeeded())
      future.value().responseBody()
    else
      throw future.exception()
  }

  private def sendAnyNode(api: ApiKeys, request: AbstractRequest): Struct = {
    bootstrapBrokers.foreach {
      case broker =>
        try {
          return send(broker, api, request)
        } catch {
          case e: Exception =>
            logger.debug(s"Request $api failed against node $broker", e)
        }
    }
    throw new RuntimeException(s"Request $api failed on brokers $bootstrapBrokers")
  }

  def findCoordinator(groupId: String): Node = {
    val request = new GroupCoordinatorRequest(groupId)
    val responseBody = sendAnyNode(ApiKeys.GROUP_COORDINATOR, request)
    val response = new GroupCoordinatorResponse(responseBody)
    Errors.forCode(response.errorCode()).maybeThrow()
    response.node()
  }

  def forceLeave(coordinator: Node, memberId: String, groupId: String) = {
    logger.info(s"forcing group member: $memberId to leave group: $groupId ")
    send(coordinator, ApiKeys.LEAVE_GROUP, new LeaveGroupRequest(groupId, memberId))
  }

  def listGroups(node: Node): List[GroupOverview] = {
    val responseBody = send(node, ApiKeys.LIST_GROUPS, new ListGroupsRequest())
    val response = new ListGroupsResponse(responseBody)
    Errors.forCode(response.errorCode()).maybeThrow()
    response.groups().map(group => GroupOverview(group.groupId(), group.protocolType())).toList
  }

  private def findAllBrokers(): List[Node] = {
    val request = new MetadataRequest(List[String]())
    val responseBody = sendAnyNode(ApiKeys.METADATA, request)
    val response = new MetadataResponse(responseBody)
    val errors = response.errors()
    if (!errors.isEmpty)
      logger.debug(s"Metadata request contained errors: $errors")
    response.cluster().nodes().asScala.toList
  }

  def listAllGroups(): Map[Node, List[GroupOverview]] = {
    findAllBrokers.map {
      case broker =>
        broker -> {
          try {
            listGroups(broker)
          } catch {
            case e: Exception =>
              logger.debug(s"Failed to find groups from broker $broker", e)
              List[GroupOverview]()
          }
        }
    }.toMap
  }

  def listAllConsumerGroups(): Map[Node, List[GroupOverview]] = {
    listAllGroups().mapValues { groups =>
      groups.filter(_.protocolType == ConsumerProtocol.PROTOCOL_TYPE)
    }
  }

  def listAllGroupsFlattened(): List[GroupOverview] = {
    listAllGroups.values.flatten.toList
  }

  def listAllConsumerGroupsFlattened(): List[GroupOverview] = {
    listAllGroupsFlattened.filter(_.protocolType == ConsumerProtocol.PROTOCOL_TYPE)
  }

  def describeGroup(groupId: String, coordinatorOpts: Option[Node] = None): GroupSummary = {
    val coordinator =  coordinatorOpts.getOrElse(findCoordinator(groupId))
    val responseBody = send(coordinator, ApiKeys.DESCRIBE_GROUPS, new DescribeGroupsRequest(List(groupId).asJava))
    val response = new DescribeGroupsResponse(responseBody)
    val metadata = response.groups().get(groupId)
    if (metadata == null)
      throw new KafkaException(s"Response from broker contained no metadata for group $groupId")

    Errors.forCode(metadata.errorCode()).maybeThrow()
    val members = metadata.members().map { member =>
      val metadata = Utils.readBytes(member.memberMetadata())
      val assignment = Utils.readBytes(member.memberAssignment())
      MemberSummary(member.memberId(), member.clientId(), member.clientHost(), metadata, assignment)
    }.toList
    GroupSummary(metadata.state(), metadata.protocolType(), metadata.protocol(), members)
  }

  case class ConsumerSummary(memberId: String,
                             clientId: String,
                             clientHost: String,
                             assignment: List[TopicPartition])

  case class StateAndConsumers(state: String, consumers: List[ConsumerSummary]) {
    def members = consumers.map(_.memberId)

    def subscribedTopics = consumers.flatMap(_.assignment.map(_.topic()))
  }

  def describeConsumerGroup(groupId: String, coordinatorOpt: Option[Node] = None): StateAndConsumers = {
    val group = if(coordinatorOpt.isEmpty) describeGroup(groupId) else describeGroup(groupId, coordinatorOpt)
    if (group.state != ConsumerGroupState.Dead && group.protocolType != ConsumerProtocol.PROTOCOL_TYPE)
      throw new IllegalArgumentException(s"Group $groupId with protocol type '${group.protocolType}' is not a valid consumer group")

    if (group.state == ConsumerGroupState.Stable) {
      StateAndConsumers(ConsumerGroupState.Stable, group.members.map { member =>
        val assignment = ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(member.assignment))
        new ConsumerSummary(member.memberId, member.clientId, member.clientHost, assignment.partitions().asScala.toList)
      })
    } else {
      StateAndConsumers(group.state, List.empty)
    }
  }

  def close() {
    client.close()
  }

}

object ConsumerGroupState{
  val PreparingRebalance = "PreparingRebalance"
  val AwaitingSync = "AwaitingSync"
  val Stable = "Stable"
  val Dead = "Dead"
  val Empty = "Empty"
}

object SimpleAdminClient {
  val DefaultConnectionMaxIdleMs = 9 * 60 * 1000
  val DefaultRequestTimeoutMs = 5000
  val DefaultMaxInFlightRequestsPerConnection = 100
  val DefaultReconnectBackoffMs = 50
  val DefaultSendBufferBytes = 128 * 1024
  val DefaultReceiveBufferBytes = 32 * 1024
  val DefaultRetryBackoffMs = 100
  val AdminClientIdSequence = new AtomicInteger(1)
  val AdminConfigDef = {
    val config = new ConfigDef()
      .define(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
        Type.LIST,
        Importance.HIGH,
        CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
      .define(
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
        ConfigDef.Type.STRING,
        CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
        ConfigDef.Importance.MEDIUM,
        CommonClientConfigs.SECURITY_PROTOCOL_DOC)
      .withClientSslSupport()
      .withClientSaslSupport()
    config
  }

  class AdminConfig(originals: Map[_, _]) extends AbstractConfig(AdminConfigDef, originals, false)

  def createSimplePlaintext(brokerUrl: String): SimpleAdminClient = {
    val config = Map(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG -> brokerUrl)
    create(new AdminConfig(config))
  }

  def create(props: Properties): SimpleAdminClient = create(props.asScala.toMap)

  def create(props: Map[String, _]): SimpleAdminClient = create(new AdminConfig(props))

  def create(config: AdminConfig): SimpleAdminClient = {
    val time = new SystemTime
    val metrics = new Metrics(time)
    val metadata = new Metadata
    val channelBuilder = ClientUtils.createChannelBuilder(config.values())

    val brokerUrls = config.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
    val brokerAddresses = ClientUtils.parseAndValidateAddresses(brokerUrls)
    val bootstrapCluster = Cluster.bootstrap(brokerAddresses)
    metadata.update(bootstrapCluster, 0)

    val selector = new Selector(
      DefaultConnectionMaxIdleMs,
      metrics,
      time,
      "admin",
      channelBuilder)

    val networkClient = new NetworkClient(
      selector,
      metadata,
      "admin-" + AdminClientIdSequence.getAndIncrement(),
      DefaultMaxInFlightRequestsPerConnection,
      DefaultReconnectBackoffMs,
      DefaultSendBufferBytes,
      DefaultReceiveBufferBytes,
      DefaultRequestTimeoutMs,
      time)

    val highLevelClient = new ConsumerNetworkClient(
      networkClient,
      metadata,
      time,
      DefaultRetryBackoffMs,
      DefaultRequestTimeoutMs)

    new SimpleAdminClient(
      time,
      DefaultRequestTimeoutMs,
      highLevelClient,
      bootstrapCluster.nodes().asScala.toList)
  }
}
