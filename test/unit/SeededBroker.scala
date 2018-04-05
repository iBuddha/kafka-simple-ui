package unit

/**
  * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
  * See accompanying LICENSE file.
  */

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import kafka.admin.AdminUtils
import kafka.consumer._
import kafka.utils.ZkUtils
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.test.TestingServer
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.security.JaasUtils
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * modified from Kafka Manager of Yahoo
  *
  */
class SeededBroker private (seedTopic: String, partitions: Int, createTopic: Boolean) {

  def this(seedTopic: String, partition: Int) = this(seedTopic, partition, true)

  def this() = this(null, 0, false)

  private[this] val maxRetry = 100
  private[this] val testingServer = getTestingServer
  private[this] val zookeeperConnectionString: String = testingServer.getConnectString
  private[this] val retryPolicy: ExponentialBackoffRetry = new ExponentialBackoffRetry(1000, 3)
  private[this] final val zookeeper: CuratorFramework =
    CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy)
  zookeeper.start()
  private[this] val broker = new KafkaTestBroker(zookeeper, zookeeperConnectionString)

  if (createTopic) {
    val zkUtils = ZkUtils(zookeeperConnectionString, 30000, 30000,
      JaasUtils.isZkSecurityEnabled())

    //seed with table
    AdminUtils.createTopic(zkUtils, seedTopic, partitions, 1)
  }

  private def getTestingServer: TestingServer = {
    var count = 0
    while (count < maxRetry) {
      val port = SeededBroker.nextPortNum()
      val result = initTestingServer(port)
      if (result.isSuccess)
        return result.get
      count += 1
    }
    throw new RuntimeException("Failed to create testing server using curator!")
  }

  private def initTestingServer(port: Int): Try[TestingServer] = {
    Try(new TestingServer(port, true))
  }

  def getBrokerConnectionString = broker.getBrokerConnectionString

  def getZookeeperConnectionString = testingServer.getConnectString

  def shutdown(): Unit = {
    Try(broker.shutdown())
    Try {
      if (zookeeper.getState == CuratorFrameworkState.STARTED) {
        zookeeper.close()
      }
    }
    Try(testingServer.close())
  }

  def getNewConsumer: NewKafkaManagedConsumer = {
    new NewKafkaManagedConsumer(seedTopic, "test-new-consumer", getBrokerConnectionString)
  }

}

object SeededBroker {
  val portNum = new AtomicInteger(10000)

  def nextPortNum(): Int = portNum.incrementAndGet()
}

case class NewKafkaManagedConsumer(topic: String,
                                   groupId: String,
                                   brokerConnect: String,
                                   pollMillis: Int = 100,
                                   readFromStartOfStream: Boolean = true) {
  val logger = LoggerFactory.getLogger(classOf[NewKafkaManagedConsumer])

  val props = new Properties()
  props.put("bootstrap.servers", brokerConnect)
  props.put("group.id", groupId)
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  props.put("session.timeout.ms", "30000")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("auto.offset.reset", if (readFromStartOfStream) "earliest" else "latest")

  val consumer = new KafkaConsumer[String, String](props)

  val filterSpec = new Whitelist(topic)

  logger.info("setup:start topic=%s for broker=%s and groupId=%s".format(topic, brokerConnect, groupId))
  consumer.subscribe(java.util.Arrays.asList(topic))
  logger.info("setup:complete topic=%s for zk=%s and groupId=%s".format(topic, brokerConnect, groupId))

  def read(write: (String) => Unit) = {
    import collection.JavaConverters._
    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(pollMillis)
      for (record <- records.asScala) {
        write(record.value())
      }
    }
  }

  def close() {
    consumer.close()
  }
}