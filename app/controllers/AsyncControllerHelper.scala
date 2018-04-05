package controllers

import akka.actor.{ActorSystem, OneForOneStrategy, Props}
import akka.actor.SupervisorStrategy.Restart
import akka.pattern.{Backoff, BackoffSupervisor}
import com.typesafe.config.ConfigFactory
import kafka.authorization.manager.model.request.OffsetBehindTs
import kafka.server.KafkaConfig
import kafka.utils.ZkUtils
import org.apache.kafka.common.security.JaasUtils

import scala.concurrent.duration._

/**
  * Created by xhuang on 28/06/2017.
  */
object AsyncControllerHelper {

  private val zkConnect = ConfigFactory.load().getString(KafkaConfig.ZkConnectProp)

  private val zkUtils = ZkUtils(zkConnect, 30000, 30000, JaasUtils.isZkSecurityEnabled())

  /**
    * 顶级actor的重试策略。
    * 使用这个策略，顶级的actor会被无限重启
    *
    * @param childProps
    * @param childName
    * @return
    */
  private def topLevelActorBackoffOptions(childProps: Props, childName: String) =
    Backoff.onFailure(
      childProps,
      childName = childName,
      minBackoff = 3.seconds,
      maxBackoff = 60.seconds,
      randomFactor = 0.2 // adds 20% "noise" to vary the intervals slightly
    ).withAutoReset(30 seconds)
      //        .withManualReset
      .withSupervisorStrategy(OneForOneStrategy() {
      case e: Exception => Restart
    })

  def supervisorActor(childProps: Props, childName: String)(implicit actorSystem: ActorSystem) = {
    val supervisorProps = BackoffSupervisor.props(topLevelActorBackoffOptions(childProps, childName))
    val supervisorName = s"$childName-supervisor"
    actorSystem.actorOf(supervisorProps, supervisorName)
  }


  def offsetDiff(beginOffsets: Map[Int, OffsetBehindTs], endOffsets: Map[Int, OffsetBehindTs]): Map[Int, (OffsetBehindTs, OffsetBehindTs)] = {
    require(beginOffsets.keySet == endOffsets.keySet)
    beginOffsets.keySet.map { id =>
      val begin = beginOffsets(id)
      val end = endOffsets(id)
      (id, (begin, end))
    }.toMap
  }
}
