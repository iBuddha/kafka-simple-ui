package kafka.authorization.manager.model.form

import controllers.routes
import org.joda.time.{DateTime, DateTimeZone}


/**
  * Created by xhuang on 28/06/2017.
  */
case class SelectTopicOffsetForm(topic: String, time: DateTime)

object SelectTopicOffsetForm {
  import play.api.data._
  import play.api.data.Forms._
  val form = Form (
    mapping(
      "topic" -> nonEmptyText,
      "time" -> jodaDate("yyyy-MM-dd HH:mm:ss.SSS")
    )(SelectTopicOffsetForm.apply)(SelectTopicOffsetForm.unapply)
  )
  val postUrl = routes.AsyncController.topicOffsetPost()
}