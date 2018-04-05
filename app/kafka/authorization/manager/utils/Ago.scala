package kafka.authorization.manager.utils

/**
  * Created by xhuang on 25/04/2017.
  */
class Ago(time: Int) {
  def secondsAgo(): Long = System.currentTimeMillis() - time * 1000L

  def minutesAgo(): Long = System.currentTimeMillis() - time * 60 * 1000L

  def hoursAgo(): Long = System.currentTimeMillis() - time * 60 * 60 * 1000L

  def secondsAgo(from: Long) = from - time * 1000

  def minutesAgo(from: Long) = from - time * 60 * 1000L

  def hoursAgo(from: Long) = from - time * 60 * 60 * 1000L
}

object Ago {
  implicit def apply(time: Int): Ago = {
    new Ago(time)
  }
}
