package kafka.authorization.manager.model.form

import controllers.routes
import org.joda.time.DateTime


/**
  * Created by xhuang on 28/06/2017.
  */
case class SelectTopicOffsetDiffForm(topic: String, from: DateTime, to: DateTime)
object SelectTopicOffsetDiffForm {
  import play.api.data._
  import play.api.data.Forms._
  val form = Form (
    mapping(
      "topic" -> nonEmptyText,
      "from" -> jodaDate("yyyy-MM-dd hh:mm:ss,SSS"),
      "to" -> jodaDate("yyyy-MM-dd hh:mm:ss,SSS")
    )(SelectTopicOffsetDiffForm.apply)(SelectTopicOffsetDiffForm.unapply)
  )
  val postUrl = routes.AsyncController.topicOffsetDiffPost()
}
