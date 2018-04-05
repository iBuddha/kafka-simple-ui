package kafka.authorization.manager.model.form

import controllers.routes

/**
  * Created by xhuang on 28/06/2017.
  */
case class TopicPartitionOffsetForm(topic: String, partition: Int, offset: Long)
object TopicPartitionOffsetForm {
    import play.api.data._
    import play.api.data.Forms._
    val form = Form (
      mapping(
        "topic" -> nonEmptyText,
        "partition" -> number,
        "offset" -> longNumber
      )(TopicPartitionOffsetForm.apply)(TopicPartitionOffsetForm.unapply)
    )

    val getMessageByTopicPartitionOffsetUrl = routes.AsyncController.getMessageByTopicPartitionOffsetPost()
}

//case class TopicForm(topic: String)
//object TopicForm {
//  import play.api.data._
//  import play.api.data.Forms._
//  val form = Form (
//    mapping(
//      "topic" -> nonEmptyText
//    )(TopicForm.apply)(TopicForm.unapply)
//  )
//
//  val describeTopicPostUrl = routes.AsyncController.describeTopicPost()
//}
//
