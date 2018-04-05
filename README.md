# 一个简单的UI,实现以下功能
## Topic
### Message and Offset
* Get Latest Message 查看某个topic最新的消息
* Get Offsets 搜索某个时间点对应的offset
* Get Offsets Diff 计算两个时间点之间的消息条数
* Get Messages 获取某个时间点开始的N条信息
* Get Message 根据(topic, partition, offset)的组合获取消息
* Produce 发送消息到topic

### Manage
* Describe Topic 查看topic的元数据
* Add Topics 批量创建topic
* Add Topics 批量创建topic
* Create Topic with broker assignment 创建topic并指定使用的broker

## Consumer Group
* Describe Consumer Group 查看consumer group的信息
* Trigger Rebalance 触发rebalance
* Reset Consumer Group Progress 将Consumer Group的消费进度重置到某个时间
* Reset Consumer Group Progress for single topic 将Consumer Group订阅的某个topic的消费进度重置到某个时间

## ACLs
* Add ACL
* Find Resource ACLs
* Find Principal ACLs
* List All ACLs

# 编译部署
适用于Kafka 0.10.1以上版本

这是一个Play Framework程序。

编译

    $sbt clean dist

然后在target/universal下会有kafka-simple-ui-1.0-SNAPSHOT.zip. 解压即可使用

部署

参见[play文档](https://www.playframework.com/documentation/2.5.x/Deploying)

例如：

    $./bin/kafka-simple-ui -Dconfig.file=conf/application.conf -Dhttp.port=8080