package kafka.authorization.manager.actor

import akka.actor.{Actor, ActorLogging, ActorRef}
import kafka.authorization.manager.utils.{ConsumerGroupManager, SimpleAdminClient}

import scala.util.Try

/**
  * Created by xhuang on 05/07/2017.
  */
class AdminActor(bootstrapServers: String, offsetLookupActorRef: ActorRef) extends Actor with ActorLogging {
  import AdminActor._

  private var adminClient: SimpleAdminClient = null
  private var consumerManager: ConsumerGroupManager = null

  override def preStart(): Unit = {
    super.preStart()
    adminClient = SimpleAdminClient.createSimplePlaintext(bootstrapServers)
    consumerManager = new ConsumerGroupManager(bootstrapServers)
  }

  override def postStop(): Unit = {
    super.postStop()
    if(adminClient != null)
      adminClient.close
    if(consumerManager != null)
      consumerManager.close()
  }

  //   def forceReset(offsetLookupActor: ActorRef, groupId: String, topic: String, ts: Long, maxRetries: Int)(implicit executionContext: ExecutionContext): Boolean =

  override def receive = {
    case TriggerRebalance(groupId) =>
      sender ! Try{consumerManager.triggerRebalance(groupId)}
    case ResetGroupOffsets(groupId, ts) =>
      log.info(s"resetting offset for group: $groupId")
      sender ! Try{consumerManager.forceReset(offsetLookupActorRef, groupId, ts, maxRetry)(context.dispatcher)}
    case ResetGroupTopicOffset(groupId, topic, ts) =>
      log.info(s"resetting offset for topic: $topic of group: $groupId")
      sender ! Try{consumerManager.forceReset(offsetLookupActorRef, groupId, topic, ts, maxRetry)(context.dispatcher)}
  }
}

object AdminActor {
  case class TriggerRebalance(groupId: String)
  case class ResetGroupOffsets(groupId: String, ts: Long)
  case class ResetGroupTopicOffset(groupId: String, topic: String, ts: Long)
  val maxRetry = 10
}
