package unit

import akka.actor.{ActorSystem, Props}
import kafka.authorization.manager.actor.OffsetLookupActor
import kafka.authorization.manager.utils.ConsumerGroupManager
import org.scalatest.FunSuite

import scala.concurrent.Await

/**
  * Created by xhuang on 05/07/2017.
  */
class GroupManagerTest extends FunSuite {
  test("SimpleKafkaConsumer consume") {
    val bootstrapServers = "localhost:9092"
    val consumerGroupManager = new ConsumerGroupManager(bootstrapServers)
    consumerGroupManager.becomeLeader("kafka-examples", 5)
    consumerGroupManager.close()
  }

  test("forceReset") {
    val bootstrapServers = "localhost:9092"
    val delayHours = 24
    val resetTS = System.currentTimeMillis() - delayHours * 60 * 60 * 1000

    val consumerGroupManager = new ConsumerGroupManager(bootstrapServers)
    val actorSystem = ActorSystem("test-system")
    val offsetLookupActor = actorSystem.actorOf(Props(new OffsetLookupActor(bootstrapServers)))
    consumerGroupManager.forceReset(offsetLookupActor, "kafka-examples", resetTS, 25)(actorSystem.dispatcher)
    val terminateFuture = actorSystem.terminate()
    import scala.concurrent.duration._
    Await.ready(terminateFuture, 5 seconds)
    consumerGroupManager.close()
  }
}
