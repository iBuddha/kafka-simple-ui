package kafka.authorization.manager.utils.client

import java.nio.ByteBuffer
import java.util
import java.util.{ArrayList, List}

import kafka.server.{Defaults, KafkaConfig}
import org.apache.kafka.clients.consumer.RangeAssignor
import org.apache.kafka.clients.consumer.RoundRobinAssignor
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.requests.JoinGroupRequest

/**
  * Created by xhuang on 07/07/2017.
  */
object AdminHelper {
  def protocolMetadata(topics: Set[String]): util.List[JoinGroupRequest.ProtocolMetadata] = {
    val metadataList = new util.ArrayList[JoinGroupRequest.ProtocolMetadata]
    import scala.collection.JavaConversions._
    val rangeAssignor = new RangeAssignor
    val roundRobinAssignor = new RoundRobinAssignor
    val assignors = Set(rangeAssignor, roundRobinAssignor)
    for (assignor <- assignors) {
      val subscription = assignor.subscription(topics)
      val metadata = ConsumerProtocol.serializeSubscription(subscription)
      metadataList.add(new JoinGroupRequest.ProtocolMetadata(assignor.name, metadata))
    }
    metadataList
  }

  def consumerJoinGroupRequest(topic: String, groupId: String) =  {
    val protocolType = ConsumerProtocol.PROTOCOL_TYPE
    val sessionTimeout = Defaults.GroupMinSessionTimeoutMs
    val rebalanceTimeout = 1000
    val memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID
    new JoinGroupRequest(groupId,
      sessionTimeout,
      rebalanceTimeout,
      memberId,
      protocolType,
      protocolMetadata(Set(topic)))
  }



}
