package com.newmotion.akka.rabbitmq

import scala.util._
import akka.actor.Actor
import akka.event.LoggingAdapter
import com.rabbitmq.client.{ ShutdownListener, ShutdownSignalException }
import java.io.IOException
import java.util.concurrent.TimeoutException

/**
 * @author Yaroslav Klymko
 */
trait RabbitMqActor extends Actor with ShutdownListener {
  def log: LoggingAdapter

  def shutdownCompleted(cause: ShutdownSignalException): Unit = {
    log.info("[MQ][A] shutdownCompleted cause={}", cause)
    self ! AmqpShutdownSignal(cause)
  }

  def close(x: AutoCloseable): Unit = Try {
    x.close()
  } match {
    case Success(_) =>
      log.info("[MQ][A] Closed success!")
    case Failure(throwable) =>
      throwable match {
        case why: com.rabbitmq.client.AlreadyClosedException =>
          log.warning("[MQ][A] Close result: {}", why)
        case why: Throwable =>
          log.error("[MQ][A] Exception on close {}", why)
      }
  }

  def safe[T](f: => T): Option[T] = Try {
    f
  } match {
    case Success(result)                     => Some(result)
    case Failure(_: IOException)             => None
    case Failure(_: ShutdownSignalException) => None
    case Failure(_: TimeoutException)        => None
    case Failure(throwable)                  => throw throwable
  }
}

sealed trait ShutdownSignal
case class AmqpShutdownSignal(cause: ShutdownSignalException) extends ShutdownSignal {
  def appliesTo(x: AnyRef): Boolean = cause.getReference eq x
}
case object ParentShutdownSignal extends ShutdownSignal
