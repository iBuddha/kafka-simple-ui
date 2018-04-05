package kafka.authorization.manager.model.form

import controllers.routes

/**
  * Created by xhuang on 29/06/2017.
  */
case class ProduceForm(topic: String, partition: Int, key: String, value: String)

object ProduceForm {
  import play.api.data._
  import play.api.data.Forms._
  val form = Form (
    mapping(
      "topic" -> nonEmptyText,
      "partition" -> number,
      "key" -> text,
      "value" -> text
    )(ProduceForm.apply)(ProduceForm.unapply)
  )
  val postUrl = routes.AsyncController.producePost()
}

