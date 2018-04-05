package kafka.authorization.manager.utils.client

/**
  * Created by xhuang on 05/07/2017.
  */

class LeaderIdHolder {

  @volatile private var leaderId: Option[String] = None

  def setLeaderId(id: String) = {
    leaderId = Some(id)
  }

  def getLeaderId(): Option[String] = leaderId
}
