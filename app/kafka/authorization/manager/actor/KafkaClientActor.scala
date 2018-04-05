package kafka.authorization.manager.actor

import akka.actor.{Actor, Status}

/**
  * Created by xhuang on 25/04/2017.
  */
trait KafkaClientActor extends Actor {
  protected[this] def maybeResponseException[T](block: => T) = {
    try {
      sender ! block
    } catch {
      case e: Exception => sender ! Status.Failure(e)
    }
  }
}
