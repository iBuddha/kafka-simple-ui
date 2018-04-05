package kafka.authorization.manager.actor

import java.net.InetAddress

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.typesafe.config.ConfigFactory
import kafka.network.RequestChannel.Session
import kafka.security.auth._
import org.apache.kafka.common.security.auth.KafkaPrincipal

/**
  * Created by xhuang on 21/06/2017.
  */
class AuthorizerActor extends Actor with ActorLogging {

  import AuthorizerActor._

  private var authorizer: Option[SimpleAclAuthorizer] = None

  override def preStart(): Unit = {
    super.preStart()
    initAuthorizer()
  }

  override def postStop(): Unit = {
    log.warning("stopping AuthorizerActor")
    try {
      authorizer.foreach(_.close())
    } catch {
      case e: Exception => log.error(e, "failed to close SimpleAclAuthorizer")
    }
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    log.error(reason, "AuthorizerActor restarted because of " + message.map(_.toString).getOrElse(""))
  }


  override def receive: Receive = {
    case req: AuthorizationRequest => {
      processRequest(req, sender(), authorizer.get)
    }
    case other => log.warning(s"received unknown message $other")
  }

  private def initAuthorizer() = {
    authorizer = Some(new SimpleAclAuthorizer)
    authorizer.foreach(_.configure(getConfig()))
  }


  private def processRequest(req: AuthorizationRequest, sender: ActorRef, authorizer: SimpleAclAuthorizer) = {
    req match {
      case AddAclsReq(acls, resource) => {
        authorizer.addAcls(acls, resource)
        log.info(s"added $acls for $resource")
        sender ! SuccessfullyAdded
      }

      case RemoveAclsReq(acls, resource) => {
        authorizer.removeAcls(acls, resource) match {
          case true =>
            log.info(s"Successfully removed $acls for $resource")
            sender ! SuccessfullyRemoved
          case false =>
            log.info(s"Failed to remove $acls for $resource")
            sender ! FailedToRemove("failed to remove")
        }
      }

      case RemoveAllAclsReq(resource) => {
        authorizer.removeAcls(resource) match {
          case true =>
            log.info(s"Successfully removed all acls for $resource")
            sender ! SuccessfullyRemoved
          case false =>
            log.info(s"Failed to remove all acls for $resource")
            sender ! FailedToRemove("failed to remove")
        }
      }

      case GetResourceAclsReq(resource) => {
        val acls = authorizer.getAcls(resource)
        sender ! AclsForResource(acls, resource)
      }

      case GetPrincipalAclsReq(principal) => {
        val acls = authorizer.getAcls(principal)
        sender ! AclsOfPrincipal(acls, principal)
      }
      case GetAllAclsReq => sender ! AllAcls(authorizer.getAcls())

      case CheckAclMatchReq(op, res, principal, host) => {
        val session = Session(principal, InetAddress.getByName(host))
        val result = authorizer.authorize(session, op, res)
        sender ! AclMatchResult(result)
      }
    }
  }
}

object AuthorizerActor {
  def getConfig(): java.util.Map[String, String] = {
    val configs = new java.util.HashMap[String, String]()
    val iter = ConfigFactory.load().entrySet().iterator()
    while (iter.hasNext) {
      val config = iter.next()
      configs.put(config.getKey, config.getValue.unwrapped().toString)
    }
    configs
  }
}

sealed trait AuthorizationRequest

case class AddAclsReq(acls: Set[Acl], resource: Resource) extends AuthorizationRequest

case class RemoveAclsReq(acls: Set[Acl], resource: Resource) extends AuthorizationRequest

case class RemoveAllAclsReq(resource: Resource) extends AuthorizationRequest

case class GetResourceAclsReq(resource: Resource) extends AuthorizationRequest

case class GetPrincipalAclsReq(principal: KafkaPrincipal) extends AuthorizationRequest

case object GetAllAclsReq extends AuthorizationRequest

case class CheckAclMatchReq(operation: Operation,
                            resource: Resource,
                            principal: KafkaPrincipal,
                            host: String) extends AuthorizationRequest


sealed trait AuthorizationResponse

case object SuccessfullyAdded extends AuthorizationResponse

case object SuccessfullyRemoved extends AuthorizationResponse
case class FailedToRemove(msg: String) extends AuthorizationResponse

case class AclsForResource(acls: Set[Acl], resource: Resource) extends AuthorizationResponse

case class AclsOfPrincipal(acls: Map[Resource, Set[Acl]], principal: KafkaPrincipal) extends AuthorizationResponse

case class AllAcls(acls: Map[Resource, Set[Acl]]) extends AuthorizationResponse

case class AclMatchResult(matches: Boolean) extends AuthorizationResponse

