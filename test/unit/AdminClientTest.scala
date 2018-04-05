package unit

import kafka.authorization.manager.utils.SimpleAdminClient
import kafka.authorization.manager.utils.client.AdminHelper
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{JoinGroupRequest, JoinGroupResponse, LeaveGroupResponse}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * Created by xhuang on 04/07/2017.
  */
class AdminClientTest extends FunSuite with BeforeAndAfterAll{
  var adminClient: SimpleAdminClient = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    adminClient = SimpleAdminClient.createSimplePlaintext("localhost:9092")
  }

  test("admin client trigger rebalance") {
    adminClient.triggerRebalance("kafka-examples")
  }

//  public JoinGroupRequest(String groupId,
//    int sessionTimeout,
//    int rebalanceTimeout,
//    String memberId,
//    String protocolType,
//    List<ProtocolMetadata> groupProtocols) {
//    this(1, groupId, sessionTimeout, rebalanceTimeout, memberId, protocolType, groupProtocols);
//  }


  test("AdminUtils send JoinGroupRequest") {
    val groupId = "kafka-examples"
    val joinGroupRequest: JoinGroupRequest = AdminHelper.consumerJoinGroupRequest("topic23xd232", groupId)
    val adminClient = SimpleAdminClient.createSimplePlaintext("localhost:9092")
    val coordinator =adminClient.findCoordinator(groupId)
    val responseStruct = adminClient.send(coordinator, ApiKeys.JOIN_GROUP, joinGroupRequest)
    val response = new JoinGroupResponse(responseStruct)
    if(response.errorCode() == Errors.NONE.code()){
      val memberId = response.memberId()
      val leaveGroupRespones = new LeaveGroupResponse(adminClient.forceLeave(coordinator, memberId, groupId))
      if(leaveGroupRespones.errorCode() == Errors.NONE.code()){
        println("successfully leave group")
      } else{
        println(Errors.forCode(leaveGroupRespones.errorCode()).message())
      }
    }
    adminClient.close()


  }

  override def afterAll(): Unit = {
    super.afterAll()
    adminClient.close()
  }
}
