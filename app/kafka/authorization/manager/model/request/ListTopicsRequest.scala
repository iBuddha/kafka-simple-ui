package kafka.authorization.manager.model.request

import scala.util.Try

/**
  * Created by xhuang on 28/04/2017.
  */
case class ListTopicsRequest(id: Long) extends KafkaActorRequest
case class ListTopicResponse(request: ListTopicsRequest, result: Try[List[String]]) extends KafkaActorResponse

