package kafka.authorization.manager.model

import kafka.common.TopicAndPartition

/**
  * Created by xhuang on 28/06/2017.
  */

/**
  *
  * @param group
  * @param topic
  * @param partition
  * @param currentOffset 如果是None，表示尚未commit过
  * @param logEndOffset 如果是None，表示没有这个partition
  * @param lag 如果是None,表示无法计算lag
  * @param owner 如果是None，表示找不到owner
  */
case class PartitionDescription(group: String, topic: String, partition: Int, currentOffset: Option[Long], logEndOffset: Option[Long], lag: Option[Long], owner: Option[PartitionOwner])

case class PartitionOwner(memberId: String, clientId: String, host: String)

case class GroupDescription(groupId: String, state: String, partitionDescriptions: Map[TopicAndPartition, Option[PartitionDescription]])
