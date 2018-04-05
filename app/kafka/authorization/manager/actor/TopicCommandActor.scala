package kafka.authorization.manager.actor

import akka.actor.{Actor, ActorLogging}
import com.typesafe.config.ConfigFactory
import kafka.admin.{AdminUtils, BrokerMetadata}
import kafka.authorization.manager.model.NewTopicWithAssignment
import kafka.authorization.manager.model.form.NewTopic
import kafka.authorization.manager.model.request.GetBrokerMetadatas
import kafka.server.KafkaConfig
import kafka.utils.ZkUtils
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException
import org.apache.kafka.common.security.JaasUtils

import scala.util.Try

/**
  * Created by xhuang on 01/07/2017.
  */
class TopicCommandActor(zkConnect: String) extends Actor with ActorLogging {

  private var zkUtils: Option[ZkUtils] = None

  override def receive: Receive = {
    case GetBrokerMetadatas => sender ! Try {
      AdminUtils.getBrokerMetadatas(zkUtils.get)
    }
    case NewTopic(name, partitions, replicas) =>
      AdminUtils.createTopic(zkUtils.get, name, partitions, replicas)
    case NewTopicWithAssignment(topicName, partitionNumber, replicationFactor, assignment) =>  sender ! Try {
      val currentBrokes = AdminUtils.getBrokerMetadatas(zkUtils.get).map(_.id).toSet
      if(!assignment.forall(currentBrokes.contains))
        throw new InvalidReplicaAssignmentException("not all user assigned brokers are alive")
      val replicaAssignment = AdminUtils.assignReplicasToBrokers(assignment.map(BrokerMetadata(_, None)).toSeq, partitionNumber, replicationFactor)
      AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils.get, topicName, replicaAssignment)
    }
    case other => log.error(s"got unknown message: $other.toString")
  }

  override def preStart(): Unit = {
    super.preStart()
    zkUtils.foreach(_.close)
    zkUtils = Some(ZkUtils(zkConnect,
      30000,
      30000,
      JaasUtils.isZkSecurityEnabled()))
  }

  override def postStop(): Unit = {
    super.postStop()
    zkUtils.foreach(_.close())
    zkUtils = None
  }
}
