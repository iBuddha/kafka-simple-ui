package kafka.authorization.manager.model.form

import org.apache.kafka.common.security.auth.KafkaPrincipal

/**
  * Created by xhuang on 26/06/2017.
  */
case class SelectPrincipalForm(name: String) {
  def toKafkaPrincipal(): KafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, name)
}

object SelectPrincipalForm {
  import play.api.data._
  import play.api.data.Forms._
  val form = Form (
    mapping(
      "principal" -> nonEmptyText
    )(SelectPrincipalForm.apply)(SelectPrincipalForm.unapply)
  )
}
