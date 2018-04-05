package unit

import kafka.authorization.manager.model.ResourceAcl
import kafka.security.auth._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.scalatest.FunSuite
import play.api.libs.json.Json

/**
  * Created by xhuang on 27/06/2017.
  */
class TestJsonSerde extends FunSuite {
  import kafka.authorization.manager.model.AuthJsonUtils._

  test {"ResourceType to json"} {
    assert(Json.toJson(Topic).toString().equals("{\"name\":\"Topic\"}"))
  }

  test {"json to ResourceType"} {
    val json = "{\"name\":\"Topic\"}"
    val resourceType = Json.parse(json).as[ResourceType]
    assertResult(Topic)(resourceType)
  }

  test{"Resource to json"} {
    val r = Resource(Topic, "topicName")
    assert(Json.toJson(r).toString() == "{\"resourceType\":{\"name\":\"Topic\"},\"name\":\"topicName\"}")
  }

  test{"json to Resource"} {
    val json = "{\"resourceType\":{\"name\":\"Topic\"},\"name\":\"topicName\"}"
    assertResult(Resource(Topic, "topicName"))(Json.parse(json).as[Resource])
  }

  test{"Acl to json"} {
    val acl = Acl(KafkaPrincipal.ANONYMOUS, Allow, "192.168.1.1", Read)
    val expected = "{\"principal\":{\"principalType\":\"User\",\"name\":\"ANONYMOUS\"},\"permissionType\":{\"name\":\"Allow\"},\"host\":\"192.168.1.1\",\"operation\":{\"name\":\"Read\"}}"
    assertResult(expected)(Json.toJson(acl).toString())
  }

  test{"json to Acl"} {
    val json = "{\"principal\":{\"principalType\":\"User\",\"name\":\"ANONYMOUS\"},\"permissionType\":{\"name\":\"Allow\"},\"host\":\"192.168.1.1\",\"operation\":{\"name\":\"Read\"}}"
    val result = Json.parse(json).as[Acl]
    val expected = Acl(KafkaPrincipal.ANONYMOUS, Allow, "192.168.1.1", Read)
    assertResult(expected)(result)
  }

  test{"AclAndResource to Json"} {
    val acl =  Acl(KafkaPrincipal.ANONYMOUS, Allow, "192.168.1.1", Read)
    val resource = Resource(Topic, "topicName")
    val aclAndResource = ResourceAcl(resource, acl)
//    val expected = "{\"[acl\":{\"principal\":{\"principalType\":\"User\",\"name\":\"ANONYMOUS\"},\"permissionType\":{\"name\":\"Allow\"},\"host\":\"192.168.1.1\",\"operation\":{\"name\":\"Read\"}},\"resource\":{\"resourceType\":{\"name\":\"Topic\"},\"name\":\"topicName\"]}}"
    println(Json.toJson(aclAndResource).toString)
  }

  test("json to AclAndResouce"){
    val json =  "{\"acl\":{\"principal\":{\"principalType\":\"User\",\"name\":\"ANONYMOUS\"},\"permissionType\":{\"name\":\"Allow\"},\"host\":\"192.168.1.1\",\"operation\":{\"name\":\"Read\"}},\"resource\":{\"resourceType\":{\"name\":\"Topic\"},\"name\":\"topicName\"}}"
    val result = Json.parse(json).as[ResourceAcl]
    val expected = {
      val acl =  Acl(KafkaPrincipal.ANONYMOUS, Allow, "192.168.1.1", Read)
      val resource = Resource(Topic, "topicName")
      ResourceAcl(resource, acl)
    }
    assertResult(expected)(result)
  }

  test("json to ResourceAndAcl") {
    val json = "{\"resource\":{\"resourceType\":{\"name\":\"Topic\"},\"name\":\"testTopic\"},\"acl\":{\"principal\":{\"principalType\":\"User\",\"name\":\"tom\"},\"permissionType\":{\"name\":\"Allow\"},\"host\":\"0.0.0.0\",\"operation\":{\"name\":\"Read\"}}}"

    val result = Json.parse(json)

    val record = Json.parse(json).as[ResourceAcl]

    println("")

  }
}
