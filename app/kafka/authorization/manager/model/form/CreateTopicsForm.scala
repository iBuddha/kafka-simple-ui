package kafka.authorization.manager.model.form

import controllers.routes

/**
  * Created by xhuang on 01/07/2017.
  */
case class CreateTopicsForm(topics: List[NewTopic])

case class NewTopic(name: String, partitions: Int, replicas: Int)

object CreateTopicsForm {

  import play.api.data._
  import play.api.data.Forms._

  val newTopicMapping = mapping(
    "name" -> nonEmptyText,
    "partitions" -> number,
    "replicas" -> number
  )(NewTopic.apply)(NewTopic.unapply)


  val form = Form(
    mapping(
      "topics" -> list(newTopicMapping))(CreateTopicsForm.apply)(CreateTopicsForm.unapply)
  )

  val postUrl = routes.AsyncController.createTopicsPost
}
