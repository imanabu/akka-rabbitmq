package com.newmotion.akka.rabbitmq

import akka.actor.{ Props, ActorRef, FSM }
import collection.immutable.Queue
import ConnectionActor.ProvideChannel
import scala.annotation.tailrec
import scala.util.control.NonFatal

/**
 * @author Yaroslav Klymko
 */
object ChannelActor {
  sealed trait State
  case object Disconnected extends State
  case object Connected extends State

  case object GetState

  private[rabbitmq] sealed trait Data
  private[rabbitmq] case class InMemory(queue: Queue[OnChannel] = Queue()) extends Data
  private[rabbitmq] case class Connected(channel: Channel) extends Data

  def props(setupChannel: (Channel, ActorRef) => Any = (_, _) => ()): Props = {
    // Props(classOf[ChannelActor], setupChannel)
    Props(new ChannelActor(setupChannel))
  }

  private[rabbitmq] case class Retrying(retries: Int, onChannel: OnChannel) extends OnChannel {
    def apply(channel: Channel): Any = onChannel(channel)
  }
}

class ChannelActor(setupChannel: (Channel, ActorRef) => Any)
  extends RabbitMqActor
  with FSM[ChannelActor.State, ChannelActor.Data] {

  import ChannelActor._

  startWith(Disconnected, InMemory())

  private sealed trait ProcessingResult {}
  private case class ProcessSuccess(m: Any) extends ProcessingResult
  private case class ProcessFailureRetry(onChannel: Retrying) extends ProcessingResult
  private case object ProcessFailureDrop extends ProcessingResult

  private def header(state: ChannelActor.State, msg: Any) = s"in $state received $msg:"

  private def safeWithRetry(channel: Channel, fn: OnChannel): ProcessingResult = {
    safe(fn(channel)) match {
      case Some(r) =>
        ProcessSuccess(r)

      case None if channel.isOpen =>
        // If the function failed but the channel is still open, we know that the
        // problem was with f, and not the channel state. Therefore we do *not* retry f
        // in this case because its failure might be due to some inherent problem with f
        // itself, and in that case a whole application might get stuck in a retry loop.
        ProcessFailureDrop

      case None =>
        // The channel closed but the actor state is Connected: There is a small window
        // between a disconnect, sending an ShutdownSignal, and processing that signal.
        // Just because our ChannelMessage was processed in this window does not mean we
        // should ignore the intent of dropChannelAndRequestNewChannel (because there was,
        // in fact, no channel)
        fn match {
          case Retrying(retries, _) if retries == 0 =>
            ProcessFailureDrop
          case Retrying(retries, onChannel) =>
            ProcessFailureRetry(Retrying(retries - 1, onChannel))
          case _ =>
            ProcessFailureRetry(Retrying(3, fn))
        }
    }
  }

  when(Disconnected) {
    case Event(channel: Channel, InMemory(queue)) =>
      val state = "Disconnected"
      @tailrec
      def loop(qs: Queue[OnChannel]): State = qs.headOption match {
        case None => goto(Connected) using Connected(channel)
        case Some(onChannel) =>
          val res = safeWithRetry(channel, onChannel)
          log.warning(
            "[MQ][A][{}]  {} queued message {} resulted in {}",
            state, header(Disconnected, channel), onChannel, res)
          res match {
            case ProcessSuccess(_) => loop(qs.tail)
            case ProcessFailureRetry(retry) =>
              dropChannelAndRequestNewChannel(channel)
              stay() using InMemory(retry +: qs.tail)
            case ProcessFailureDrop =>
              log.warning(
                "[MQ][A][{}]  {} stopped retrying message {}",
                state, header(Disconnected, channel), onChannel)
              dropChannelAndRequestNewChannel(channel)
              stay() using InMemory(qs.tail)
          }
      }

      if (setup(channel)) {
        if (queue.nonEmpty) log.warning(
          "[MQ][A][{}] {} processing {} queued messages {}", state,
          header(Disconnected, channel), queue.size, queue.mkString("\n", "\n", ""))
        loop(queue)
      } else {
        log.warning(s"[MQ][A][$state] Dropping and Requesting New Channel")
        dropChannelAndRequestNewChannel(channel)
        stay()
      }

    case Event(msg @ ChannelMessage(onChannel, dropIfNoChannel), InMemory(queue)) =>
      if (dropIfNoChannel) {
        log.error(
          "[MQ][A] No channel! {} dropping message {}",
          header(Disconnected, msg), onChannel)
        stay()
      } else {
        log.debug("[MQ][A]  {} queueing message {}", header(Disconnected, msg), onChannel)
        stay() using InMemory(queue enqueue onChannel)
      }

    case Event(_: ShutdownSignal, _) => stay()
  }

  when(Connected) {
    case Event(channel: Channel, Connected(_)) =>
      log.warning("[MQ][A]  {} closing unexpected channel {}", header(Connected, channel), channel)
      close(channel)
      stay()

    case Event(msg: ShutdownSignal, Connected(channel)) =>
      (msg match {
        case ParentShutdownSignal =>
          Some(dropChannel _) // The parent is responsible for providing the new channel.
        case amqpSignal: AmqpShutdownSignal if amqpSignal.appliesTo(channel) =>
          Some(dropChannelAndRequestNewChannel _)
        case _ => None
      }).fold(stay()) { shutdownAction =>
        log.debug("[MQ][A]  {} shutdown Channel", header(Connected, msg))
        shutdownAction(channel)
        goto(Disconnected) using InMemory()
      }

    case Event(msg @ ChannelMessage(onChannel, _), Connected(channel)) =>
      val res = safeWithRetry(channel, onChannel)
      log.debug("[MQ][A]  {} received channel message resulted in {}", header(Connected, msg), res)
      res match {
        case ProcessSuccess(_) => stay()
        case ProcessFailureRetry(retry) if !msg.dropIfNoChannel =>
          log.warning("[MQ][A] Failed sending message but let's retry.")
          dropChannelAndRequestNewChannel(channel)
          goto(Disconnected) using InMemory(Queue(retry))
        case _ =>
          if (!msg.dropIfNoChannel) {
            log.warning(
              "[MQ][A]  {} Requesting a new channel and retry from " +
              "memory" +
              " {}",
              header(Connected, msg), onChannel)
          }
          dropChannelAndRequestNewChannel(channel)
          goto(Disconnected) using InMemory()
      }
  }

  whenUnhandled {
    case Event(GetState, _) =>
      sender() ! stateName
      stay()
  }

  onTransition {
    case Disconnected -> Connected => log.info(
      "[MQ][A]  {} transition to connected", self.path)
    case Connected -> Disconnected =>
      log.warning("[MQ][A]  {} transition to disconnected", self.path)
  }

  onTermination {
    case StopEvent(_, Connected, Connected(channel)) =>
      log.debug("[MQ][A]  {} closing channel {}", self.path, channel)
      close(channel)
  }

  initialize()

  private def setup(channel: Channel): Boolean = {
    channel.addShutdownListener(this)
    log.info("[MQ][A]  {} setting up new channel {}", self.path, channel)
    try {
      safe(setupChannel(channel, self)).isDefined
    } catch {
      case NonFatal(throwable) =>
        log.error("[MQ][A]  {} setup channel callback error {}", self.path,
          channel)
        close(channel)
        throw throwable
    }
  }

  private def dropChannelAndRequestNewChannel(broken: Channel): Unit = {
    dropChannel(broken)
    askForChannel()
  }

  private def dropChannel(brokenChannel: Channel): Unit = {
    log.debug("[MQ][A]  {} closing broken channel {}", self.path, brokenChannel)
    close(brokenChannel)
  }

  private def askForChannel(): Unit = {
    log.debug("[MQ][A]  {} asking for new channel", self.path)
    connectionActor ! ProvideChannel
  }

  private[rabbitmq] def connectionActor = context.parent

  @scala.throws[Exception](classOf[Exception])
  override def postRestart(reason: Throwable): Unit = {
    log.error(s"{} child restarted with exception {}, reason: {}", self.path,
      reason, reason.getMessage)
    super.postRestart(reason)
    askForChannel()
  }
}
