package kafka.authorization.manager.actor

import akka.actor.{ActorLogging, Terminated}
import akka.event.LoggingReceive
import kafka.authorization.manager.model.request._
import kafka.authorization.manager.utils.ConsumerCreator
import kafka.authorization.manager.utils.ConsumerOps._
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.util.Try

/**
  * Created by xhuang on 21/04/2017.
  */
class OffsetLookupActor(private val bootstrapServers: String) extends KafkaClientActor with ActorLogging {

  var consumer: KafkaConsumer[Array[Byte], Array[Byte]] = null

  override def preStart(): Unit = {
    super.preStart()
    consumer = ConsumerCreator.newStandAloneConsumer(bootstrapServers)
  }


  override def postStop(): Unit = {
    super.postStop()
    consumer.close()
    log.info("OffsetLookupActor is stopped.")
  }

  override def receive: Receive = LoggingReceive {
    case request: FindPartitionOffsetRequest => {
      val result = Try{consumer.offsetBehindTime(request.topicPartition, request.ts)}
      sender ! FindPartitionOffsetResponse(request, result)
    }

    case request: FindTopicOffsetRequest => {
      val result = Try{consumer.offsetBehindTime(request.topic, request.ts)}
      sender ! FindTopicOffsetResponse(request, result)
    }

    case request: FindPartitionOffsetDiffRequest => maybeResponseException {
      val startOffset: OffsetBehindTs = consumer.offsetBehindTime(request.tp, request.from)
      val endOffset: OffsetBehindTs = consumer.offsetBehindTime(request.tp, request.to)
      FindPartitionOffsetDiffResponse(request, startOffset, endOffset)
    }

    case request: FindTopicOffsetDiffRequest => maybeResponseException {
      val startOffsets = consumer.offsetBehindTime(request.topic, request.from)
      val endOffsets = consumer.offsetBehindTime(request.topic, request.to)
      FindTopicOffsetDiffResponse(request, startOffsets, endOffsets)
    }

    case Terminated => {
      log.info("OffsetLookupActor is terminating.")
      consumer.close()
    }
  }
}

