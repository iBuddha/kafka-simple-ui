package unit

import kafka.authorization.manager.actor.AuthorizerActor
import kafka.security.auth.{Acl, SimpleAclAuthorizer, _}
import kafka.server.KafkaConfig
import org.apache.curator.test.TestingServer
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfterEach, FunSuite}
/**
  * Created by xhuang on 21/06/2017.
  */
class TestAuthorizerActor extends FunSuite with BeforeAndAfterEach {

  private[this] val testServer = new TestingServer(false)

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    testServer.start()
  }

  override protected def afterEach(): Unit = {
    testServer.stop()
    super.afterEach()
  }

  test("SimpleAclAuthorizer is configured using typesafe ConfigFactory") {
    val config = AuthorizerActor.getConfig()
    assert(config.get(KafkaConfig.ZkConnectProp) != null)
    assert(config.get(SimpleAclAuthorizer.SuperUsersProp).equals("User:kafka"))
    assert(config.get(SimpleAclAuthorizer.AllowEveryoneIfNoAclIsFoundProp).equals("true"))
    addTestZkConnect(config)
    var authorizer: SimpleAclAuthorizer = null
    try {
      authorizer = new SimpleAclAuthorizer
      authorizer.configure(config)
    } finally {
      if(authorizer != null)
        authorizer.close()
    }
  }

//  case class Acl(principal: KafkaPrincipal, permissionType: PermissionType, host: String, operation: Operation) {

  test("read after write") {
    tryWithAuthorizer((authorizer) => {
      val acl = Acl(bob, Allow, hostA, Read)
      authorizer.addAcls(Set(acl), topicA)
      authorizer.getAcls(topicA) should equal(Set(acl))
    })
  }



  private val bob = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Bob")
  private val alice = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice")
  private val anonymous = KafkaPrincipal.ANONYMOUS

  private val hostA = "192.168.23.11"
  private val hostB = "192.168.24.12"

  private val topicA = topicResource("A")
  private val topicB = topicResource("B")

  private def topicResource(name: String) = new Resource(Topic, name)
  private def groupResource(name: String) = new Resource(Group, name)


  private def tryWithAuthorizer(funs: (SimpleAclAuthorizer) => Unit): Unit = {
    var authorizer: Option[SimpleAclAuthorizer] = None
    try{
      val config = AuthorizerActor.getConfig()
      addTestZkConnect(config)
      val aclAuthorizer = new SimpleAclAuthorizer
      authorizer = Some(aclAuthorizer)
      aclAuthorizer.configure(config)
      funs(aclAuthorizer)
    } finally{
      authorizer.foreach(a => a.close())
    }
  }

  private def addTestZkConnect(config: java.util.Map[String, String]) = {
    config.put(KafkaConfig.ZkConnectProp, s"localhost:${testServer.getPort}")
  }

}
