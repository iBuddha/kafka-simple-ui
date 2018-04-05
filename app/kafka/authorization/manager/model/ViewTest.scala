package kafka.authorization.manager.model

import controllers.AsyncController
import kafka.authorization.manager.model.form.AclRecordForm
import play.api.i18n.Lang
import views.html.helper


/**
  * Created by xhuang on 23/06/2017.
  *
  * 用于构造一个能使用IDE的自动提示功能的环境。因为直接在html里写的话，没有语法检查和自动提示
  */
class ViewTest(controller: AsyncController) {

  implicit val messages = controller.messagesApi.preferred(Seq(Lang.defaultLang))

  def testSelect() = {
    helper.select(
      field = AclRecordForm.form("permissionType"),
      options = Seq(
        "Allow" -> "Allow",
        "Deny" -> "Deny"
      ),
      '_default -> "Allow"
    )
  }
}
