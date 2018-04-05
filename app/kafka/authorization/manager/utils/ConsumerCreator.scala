package kafka.authorization.manager.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.ByteArrayDeserializer

/**
  * Created by xhuang on 21/04/2017.
  *
  * 用来创建consumer实例
  */
class ConsumerCreator[K, V](keyDeserializerClass: Class[_],
                            valueDeserializerClass: Class[_],
                            groupID: String,
                            boostrpServers: String) {

  private val properties: Properties = new Properties()
  properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass.getCanonicalName)
  properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass.getCanonicalName)
  properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID)
  properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrpServers)

  def set(key: String, value: String) = {
    properties.setProperty(key, value)
    this
  }

  def build(): KafkaConsumer[K, V] = {
    new KafkaConsumer[K, V](properties)
  }

}

object ConsumerCreator {
  private val magicGroupId = "kafka-tools-1x3d"

  /**
    * 用于执行各种metadata操作的consumer, 例如：
    * 1.根据timestamp查找offset
    * 2.获取metadata
    *
    * @return
    */
  def newStandAloneConsumer(bootstrapServers: String): KafkaConsumer[Array[Byte], Array[Byte]] = {
    new ConsumerCreator[Array[Byte], Array[Byte]](classOf[ByteArrayDeserializer], classOf[ByteArrayDeserializer], magicGroupId, bootstrapServers)
      .set(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      .set(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, "1000")
      .build()
  }
}

