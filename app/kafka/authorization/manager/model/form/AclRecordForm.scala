package kafka.authorization.manager.model.form


import kafka.security.auth._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import play.api.data.Form
import play.api.data.Forms.mapping

/**
  * Created by xhuang on 22/06/2017.
  * case class Acl(principal: KafkaPrincipal, permissionType: PermissionType, host: String, operation: Operation)
  * sealed trait Operation extends BaseEnum
case object Read extends Operation { val name = "Read" }
case object Write extends Operation { val name = "Write" }
case object Create extends Operation { val name = "Create" }
case object Delete extends Operation { val name = "Delete" }
case object Alter extends Operation { val name = "Alter" }
case object Describe extends Operation { val name = "Describe" }
case object ClusterAction extends Operation { val name = "ClusterAction" }
case object All extends Operation { val name = "All" }
  */
case class AclRecordForm(principal: String, permissionType: String, host: String, operation: String, resourceType: String, resourceName: String) {
  def acl: Acl = {
    val kafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, principal)
    val kafkaPermissionType = permissionType match {
      case "Allow" => Allow
      case "Deny" => Deny
    }
    val allOperations = Operation.values.map(o => (o.name, o)).toMap
    val kafkaOperation = allOperations.get(operation).get
    Acl(kafkaPrincipal, kafkaPermissionType, host, kafkaOperation)
  }

  def resource: Resource = {
    resourceType match {
      case "topic" => Resource(Topic, resourceName)
      case "group" => Resource(Group, resourceName)
      case "cluster" => Resource.ClusterResource
    }
  }

//  val userForm = Form(
//    mapping(
//      "name" -> text,
//      "age" -> number
//    )(UserData.apply)(UserData.unapply)
//  )
}

object AclRecordForm {
  import play.api.data._
  import play.api.data.Forms._
  val form = Form (
    mapping(
      "principal" -> nonEmptyText,
      "permissionType" -> nonEmptyText,
      "host" -> nonEmptyText,
      "operation" -> nonEmptyText,
      "resourceType" -> nonEmptyText,
      "resourceName" -> text
    )(AclRecordForm.apply)(AclRecordForm.unapply)
  )
}