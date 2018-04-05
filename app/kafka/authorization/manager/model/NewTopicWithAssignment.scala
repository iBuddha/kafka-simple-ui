package kafka.authorization.manager.model

/**
  * Created by xhuang on 04/07/2017.
  */
case class NewTopicWithAssignment(topicName: String, partitionNumber: Int, replicationFactor: Int, assignment: Set[Int])
