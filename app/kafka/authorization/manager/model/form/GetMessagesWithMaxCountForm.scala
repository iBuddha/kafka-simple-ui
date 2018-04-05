package kafka.authorization.manager.model.form

import java.util.TimeZone

import controllers.routes
import org.joda.time.{DateTime, DateTimeZone}

/**
  * Created by xhuang on 29/06/2017.
  */
case class GetMessagesWithMaxCountForm(topic: String, time: DateTime, count: Int)

object GetMessagesWithMaxCountForm {
  import play.api.data._
  import play.api.data.Forms._
  val form = Form (
    mapping(
      "topic" -> nonEmptyText,
      "time" -> jodaDate("yyyy-MM-dd HH:mm:ss,SSS"),
      "count" -> number(1, 100)
    )(GetMessagesWithMaxCountForm.apply)(GetMessagesWithMaxCountForm.unapply)
  )
  val postUrl = routes.AsyncController.messagesWithCountPost()
}

