package unit

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import kafka.admin.BrokerMetadata
import kafka.authorization.manager.actor.TopicCommandActor
import kafka.authorization.manager.model.request.GetBrokerMetadatas
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.util.Success

/**
  * Created by xhuang on 01/07/2017.
  */
class TestTopicCommandActor() extends TestKit(ActorSystem("MySpec")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  var broker: Option[SeededBroker] = None

  override def beforeAll(): Unit = {
    super.beforeAll()
    broker = Some(new SeededBroker())
  }

  override def afterAll(): Unit = {
    super.afterAll()
    broker.foreach(_.shutdown())
    TestKit.shutdownActorSystem(system)
  }

  "An TopicCommandActor " must {
    "send back broker metadata " in {
      val topicCommandActor = system.actorOf(Props(new TopicCommandActor(broker.get.getZookeeperConnectionString)))
      topicCommandActor ! GetBrokerMetadatas
      expectMsg(Success(Seq(BrokerMetadata(0, None))))
//      val meta = receiveOne(3 seconds).asInstanceOf[Try[Seq[BrokerMetadata]]]
//      println(meta)
    }
  }


//  override def afterAll {
//    TestKit.shutdownActorSystem(system)
//  }
//
//  "An Echo actor" must {
//
//    "send back messages unchanged" in {
//      val echo = system.actorOf(TestActors.echoActorProps)
//      echo ! "hello world"
//      expectMsg("hello world")
//    }
//
//  }
}
