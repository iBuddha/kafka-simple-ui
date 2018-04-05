package unit

import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.authorization.manager.utils.client.ExclusiveAssignor.Callback
import kafka.authorization.manager.utils.{ConsumerGroupManager, SimpleAdminClient}
import kafka.authorization.manager.utils.client.SimpleKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.scalatest.FunSuite

/**
  * Created by xhuang on 05/07/2017.
  */
class SimpleConsumerTest extends FunSuite {
  test("SimpleKafkaConsumer consume") {
    val props = new Properties
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "kafka-examples")
    props.put("enable.auto.commit", "false")

    props.put("session.timeout.ms", "30000")
    props.put("client.id", "mac")
    props.put("auto.offset.reset", "latest")

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "kafka.authorization.manager.utils.client.ExclusiveAssignor, org.apache.kafka.clients.consumer.RangeAssignor, org.apache.kafka.clients.consumer.RoundRobinAssignor")

    val consumer = new SimpleKafkaConsumer[String, String](props)

    consumer.subscribe(util.Arrays.asList("kafka-ui-test"))

    val groupManager = new ConsumerGroupManager("localhost:9092")

    val assignedAll = new AtomicBoolean(false)
    consumer.setExclusiveAssignorCallback(new Callback {
      override def onSuccess(): Unit = assignedAll.set(true)
    })
    var i = 1
    while (i < 5) {
      if (!assignedAll.get())
        groupManager.clearCurrentMembers("kafka-examples")
      val records = consumer.poll(3000)
      println("poll returned, with records size: " + records.count())
      i = i + 1
    }

    consumer.close()
    groupManager.close()

  }

  test("SimpleKafkaConsumer join group") {
    val props = new Properties
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "kafka-examples")
    props.put("enable.auto.commit", "false")

    props.put("session.timeout.ms", "30000")
    props.put("client.id", "mac")
    props.put("auto.offset.reset", "latest")

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = new SimpleKafkaConsumer[String, String](props)

    consumer.subscribe(util.Arrays.asList("topic-not-existed-xx1x"))

    val groupManager = new ConsumerGroupManager("localhost:9092")

    val assignedAll = new AtomicBoolean(false)
    consumer.setExclusiveAssignorCallback(new Callback {
      override def onSuccess(): Unit = assignedAll.set(true)
    })
    var i = 1
    while (i < 5) {
      val records = consumer.poll(3000)
      i = i + 1
    }

    consumer.close()
    groupManager.close()

  }
}
