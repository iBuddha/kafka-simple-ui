package kafka.authorization.manager.model.form

import controllers.routes

/**
  * Created by xhuang on 28/06/2017.
  */
case class ConsumerGroupForm(group: String)

object ConsumerGroupForm {

  import play.api.data._
  import play.api.data.Forms._

  val form = Form(
    mapping(
      "group" -> nonEmptyText
    )(ConsumerGroupForm.apply)(ConsumerGroupForm.unapply)
  )

  def getForm(group: String) = form.fill(ConsumerGroupForm(group))

  val describeGroupPostUrl = routes.AsyncController.describeGroupPost
  val triggerRebalancePostUrl = routes.AsyncController.rebalancePost()
}