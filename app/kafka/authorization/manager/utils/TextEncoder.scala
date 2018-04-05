package kafka.authorization.manager.utils

import java.nio.charset.{Charset, StandardCharsets}
import java.sql.Timestamp
import java.util.Base64

import org.joda.time.DateTime

/**
  * Created by xhuang on 26/04/2017.
  *
  * 用于从byte[]转成文本。由于Kafka里的消息可能非文本，所以这个工具应该以best effort的方式来解析非文本的消息
  */
object TextEncoder {
  def encodeByCharset(message: Array[Byte], utf: String): String = new String(message, Charset.forName(utf))

  def encodeUTF8(message: Array[Byte]): String =
    if(message == null) null else new String(message, StandardCharsets.UTF_8)

  def encodeBase64(message: Array[Byte]): String = Base64.getEncoder.encodeToString(message)

//  def encodeAvro(message: Array[Byte], schema: String): String = {
//    val parser = new Schema.Parser
//    val parsedSchema  = parser.parse(schema)
//    val binaryInjection = GenericAvroCodecs.toBinary[GenericRecord](parsedSchema)
//    val jsonInjection = GenericAvroCodecs.toJson[GenericRecord](parsedSchema)
//    val record = binaryInjection.invert(message).get
//    jsonInjection(record)
//  }
  def tsToString(ts: Long): String = new DateTime(ts).toString
}
