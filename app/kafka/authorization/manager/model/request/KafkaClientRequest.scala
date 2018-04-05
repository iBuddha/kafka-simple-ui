package kafka.authorization.manager.model.request

trait KafkaActorRequest{
  //用于区分不同的请求。当前，它应该是一个递增序列
  def id: Long
}

trait KafkaActorResponse {
  val request: KafkaActorRequest
}