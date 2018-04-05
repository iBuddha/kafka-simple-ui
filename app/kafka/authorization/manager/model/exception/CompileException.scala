package kafka.authorization.manager.model.exception

/**
  * Created by xhuang on 25/04/2017.
  */
class CompileException(s: String, cause: Throwable) extends Exception(s, cause) {
  def this(s: String) =this(s, null)
}
