package kafka.authorization.manager.model.form

import kafka.security.auth._

/**
  * Created by xhuang on 26/06/2017.
  */
case class SelectResourceForm(resourceName: String, resourceType: String) {
  def toResource(): Resource = Resource(kafkaResourceType, resourceName)

  def kafkaResourceType: ResourceType = resourceType match {
    case "Topic" => Topic
    case "Cluster" => Cluster
    case "Group" => Group
  }
}

object SelectResourceForm {
  import play.api.data._
  import play.api.data.Forms._
  val form = Form (
    mapping(
      "resourceName" -> nonEmptyText,
      "resourceType" -> nonEmptyText
    )(SelectResourceForm.apply)(SelectResourceForm.unapply)
  )

  def apply(res: Resource): SelectResourceForm =  SelectResourceForm(res.name, res.resourceType.name)

  def getForm(res: Resource): Form[SelectResourceForm] = form.fill(SelectResourceForm(res))
}