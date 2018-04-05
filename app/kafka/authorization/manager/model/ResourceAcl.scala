package kafka.authorization.manager.model

import kafka.security.auth._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import play.api.libs.json._
import play.api.libs.functional.syntax._

/**
  * Created by xhuang on 26/06/2017.
  */
case class ResourceAcl(resource: Resource, acl: Acl) {
  def toJson() = {
    import AuthJsonUtils.resourceAclWrites
    Json.toJson(this)(resourceAclWrites).toString()
  }
}

object AuthJsonUtils {
  implicit val resourceTypeWrites = new Writes[ResourceType] {
    override def writes(o: ResourceType): JsValue = Json.obj(
      "name" -> o.name
    )
  }

  implicit val resourceTypeReads: Reads[ResourceType] = {
    (JsPath \ "name").read[String](Reads.StringReads).map(s => ResourceType.fromString(s))
  }

  implicit val resourceWrites = new Writes[Resource] {
    override def writes(o: Resource): JsValue = Json.obj(
      "resourceType" -> o.resourceType,
      "name" -> o.name
    )
  }

  implicit val resourceReads = (
    (JsPath \ "resourceType").read[ResourceType] and
      (JsPath \ "name").read[String](Reads.StringReads)
    ) (Resource.apply _)

  def toJson(acl: Acl): String = {
    kafka.utils.Json.encode(Acl.toJsonCompatibleMap(Set(acl)))
  }

  implicit val principalWrites = new Writes[KafkaPrincipal] {
    override def writes(o: KafkaPrincipal): JsValue = Json.obj(
      "principalType" -> o.getPrincipalType,
      "name" -> o.getName
    )
  }

  implicit val principalReads = (
    (JsPath \ "principalType").read[String] and
      (JsPath \ "name").read[String]
    ) ((t, n) => new KafkaPrincipal(t, n))


  //  public KafkaPrincipal(String principalType, String name) {
  //case object Allow extends PermissionType {
  //  val name = "Allow"
  //}
  //
  //  case object Deny extends PermissionType {
  //    val name = "Deny"
  //  }
  //case object Read extends Operation { val name = "Read" }


  implicit val permissionTypeWrites = new Writes[PermissionType] {
    override def writes(o: PermissionType): JsValue = Json.obj(
      "name" -> o.name
    )
  }

  implicit val permissionTypeReads =
    ((JsPath \ "name").read[String](Reads.StringReads)) map (s => PermissionType.fromString(s))

  implicit val operationWrites = new Writes[Operation] {
    override def writes(o: Operation): JsValue = Json.obj(
      "name" -> o.name
    )
  }

  implicit val operationReads =
    (JsPath \ "name").read[String].map(s => Operation.fromString(s))

  //  case class Acl(principal: KafkaPrincipal, permissionType: PermissionType, host: String, operation: Operation)

  implicit val aclWrites = new Writes[Acl] {
    override def writes(o: Acl): JsValue = Json.obj(
      "principal" -> o.principal,
      "permissionType" -> o.permissionType,
      "host" -> o.host,
      "operation" -> o.operation
    )
  }

  implicit val aclReads = (
    (JsPath \ "principal").read[KafkaPrincipal] and
      (JsPath \ "permissionType").read[PermissionType] and
      (JsPath \ "host").read[String] and
      (JsPath \ "operation").read[Operation]
    ) (Acl.apply _)

  implicit val resourceAclWrites = new Writes[ResourceAcl] {
    override def writes(o: ResourceAcl): JsValue = Json.obj(
      "resource" -> o.resource,
      "acl" -> o.acl
    )
  }

  implicit val resourceAclReads = (
    (JsPath \ "resource").read[Resource] and
      (JsPath \ "acl").read[Acl]
    ) (ResourceAcl.apply _)
}
