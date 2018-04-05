package kafka.authorization.manager.utils

import java.util

import kafka.authorization.manager.model.TopicPartition
import kafka.authorization.manager.model.exception.{NoSuchTopicException, RequestExecutionException}
import kafka.authorization.manager.utils.Converters._
import kafka.authorization.manager.model.request.OffsetBehindTs
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, KafkaConsumer, OffsetAndTimestamp}

import scala.collection.JavaConverters.{asJavaCollectionConverter, asScalaIteratorConverter, mapAsScalaMapConverter}
import scala.collection.mutable

/**
  * Created by xhuang on 23/04/2017.
  */
class ConsumerOps[K, V](val consumer: Consumer[K, V]) {
  type ApacheTopicPartition = org.apache.kafka.common.TopicPartition

  def assign(topicPartition: TopicPartition): Unit = {
    consumer.assign(util.Arrays.asList(topicPartition))
  }

  //  implicit def assign(topicPartition: KafkaTopicPartition) = {
  //    consumer.assign(util.Arrays.asList(topicPartition))
  //  }

  /**
    * 在调用KafkaConsumer的offsetsForTimes方法时，consumer不必被assign要查找其offset的TopicPartition
    *
    * @param tp
    * @param timestamp
    * @return 如果找到了offset就返回Some，否则返回None
    */
  def offsetBehindTime(tp: TopicPartition, timestamp: Long): OffsetBehindTs = {
    val tpAndTs = new util.HashMap[ApacheTopicPartition, java.lang.Long]()
    tpAndTs.put(tp, timestamp)
    val offsets: java.util.Map[ApacheTopicPartition, OffsetAndTimestamp] = consumer.offsetsForTimes(tpAndTs)
    Option(offsets.get(tp))
      .map(e => OffsetBehindTs(e.offset(), timestamp))
      .getOrElse {
        val endOffset = consumer.endOffsets(tpAndTs.keySet()).get(tp)
        OffsetBehindTs(endOffset, timestamp)
      }
  }

  def offsetBehindTime(topic: String, timestamp: Long): Map[Int, OffsetBehindTs] = {
    //防止当auto create topic为true时调用partitionsFor(topic)时自动创建topic
    if (!consumer.listTopics().containsKey(topic))
      throw new NoSuchTopicException(topic)
    val partitionsMaybeNull = consumer.partitionsFor(topic)
    val partitions = partitionsMaybeNull.iterator()
    val tpAndTs = new util.HashMap[ApacheTopicPartition, java.lang.Long]
    partitions.asScala.foreach(p => tpAndTs.put(new ApacheTopicPartition(topic, p.partition()), timestamp))
    val offsets = consumer.offsetsForTimes(tpAndTs)
    if (offsets.containsValue(null)) {
      val partitionsWithNullResult = offsets.asScala.filter(e => e._2 == null).map(_._1.partition())
      val toFind = partitionsWithNullResult.map(p => new ApacheTopicPartition(topic, p)).toList.asJavaCollection
      val endOffsets = consumer.endOffsets(toFind).asScala
      endOffsets.foreach {
        case (tp, offset) => offsets.put(new ApacheTopicPartition(tp.topic(), tp.partition()), new OffsetAndTimestamp(offset, timestamp))
      }
    }
    var result = mutable.Map.empty[Int, OffsetBehindTs]
    mapAsScalaMapConverter(offsets).asScala.foreach {
      case (tp, o) => result += tp.partition() -> OffsetBehindTs(o.offset(), timestamp)
    }
    result.toMap
  }

  def earliestOffset(tp: TopicPartition): Long =
    consumer.beginningOffsets(util.Arrays.asList(tp)).get(Converters.toApacheTopicPartition(tp))

  /**
    * 注意，这里返回的offset是下一条要写入的消息的offset。而不是当前已经在log里的最后一条消息的offset
    */
  def latestOffset(tp: TopicPartition): Long =
    consumer.endOffsets(util.Arrays.asList(tp)).get(toApacheTopicPartition(tp))


  def message(tp: TopicPartition, offset: Long, timeout: Long): ConsumerRecord[Array[Byte], Array[Byte]] = {
    val consumer = this.consumer.asInstanceOf[KafkaConsumer[Array[Byte], Array[Byte]]]
    val smallest = earliestOffset(tp)
    val largest = latestOffset(tp) - 1 //当前在日志里的最大offset
    if (offset < smallest)
      throw new RequestExecutionException(s"offset $offset is smaller than currently smallest offset $smallest")
    else if (offset > largest)
      throw new RequestExecutionException(s"offset $offset is bigger than currently largest offset $largest")
    else {
      consumer.assign(util.Arrays.asList(tp))
      consumer.seek(tp, offset)
      var message: Option[ConsumerRecord[Array[Byte], Array[Byte]]] = None
      //loop with timeout
      val endTime = System.currentTimeMillis() + timeout
      while (message.isEmpty && endTime > System.currentTimeMillis()) {
        val records = consumer.poll(Math.max(0, endTime - System.currentTimeMillis()))
        records.iterator().asScala.find(r => r.offset() == offset) match {
          case Some(record) => message = Some(record)
          case None =>
        }
      }
      message match {
        case Some(record) => record
        case None => throw new RequestExecutionException(s"can't retrieve message within $timeout")
      }
    }
  }

  def messages(tp: TopicPartition, beginOffset: Long, maxCount: Int, timeout: Long): List[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    val consumer = this.consumer.asInstanceOf[KafkaConsumer[Array[Byte], Array[Byte]]]
    val smallest = earliestOffset(tp)
    val largest = latestOffset(tp) - 1 //当前在日志里的最大offset
    if (beginOffset + maxCount - 1 < smallest)
      throw new RequestExecutionException(s"currently smallest offset is $smallest, but you are fetching message begin with offset $beginOffset and with count $maxCount")
    else if (beginOffset > largest)
      throw new RequestExecutionException(s"offset $beginOffset is bigger than currently largest offset $largest")
    else {
      consumer.assign(util.Arrays.asList(tp))
      consumer.seek(tp, beginOffset)
      val maxOffset = beginOffset + maxCount - 1
      val targetOffset = Math.min(largest, maxOffset) //达到这个offset就可以返回了

      val result = scala.collection.mutable.MutableList.empty[ConsumerRecord[Array[Byte], Array[Byte]]]
      //loop with timeout
      val endTime = System.currentTimeMillis() + timeout
      var currentMaxOffset = -1L
      while (currentMaxOffset < targetOffset && endTime > System.currentTimeMillis()) {
        val records = consumer.poll(Math.max(0, endTime - System.currentTimeMillis()))
        val qualified = records.iterator().asScala.filter { r => r.offset() > currentMaxOffset && r.offset <= maxOffset }
        result ++= qualified
        currentMaxOffset = result.last.offset()
      }
      if (result.isEmpty)
        throw new RequestExecutionException(s"can't retrieve message within $timeout")
      else
        result.toList
    }
  }

  def exists(topic: String): Boolean = consumer.listTopics().containsKey(topic)

  def exists(tp: TopicPartition): Boolean =
    exists(tp.topic) match {
      case false => false
      case true => consumer.partitionsFor(tp.topic).iterator().asScala.exists(p => p.partition() == tp.partition)
    }
}

object ConsumerOps {
  implicit def apply[K, V](consumer: KafkaConsumer[K, V]) = new ConsumerOps[K, V](consumer)
}
