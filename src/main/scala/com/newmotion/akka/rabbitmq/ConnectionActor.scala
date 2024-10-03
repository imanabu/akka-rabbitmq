package com.newmotion.akka.rabbitmq

import akka.actor.{ ActorRef, DeadLetter, FSM, Props }

import concurrent.duration._
import scala.concurrent.{ ExecutionContext, ExecutionContextExecutor, Future, blocking }
import scala.util.Success
import scala.util.control.NonFatal

/**
 * @author Yaroslav Klymko
 */
object ConnectionActor {
  sealed trait State
  case object Disconnected extends State
  case object Connected extends State

  case object GetState

  private[rabbitmq] sealed trait Data
  private[rabbitmq] case object NoConnection extends Data
  private[rabbitmq] case class Connected(conn: Connection) extends Data

  sealed trait Message
  case object ProvideChannel extends Message
  case object Connect extends Message
  case class Reconnect(oldConnection: Connection) extends Message
  case class NewConnection(connection: Connection) extends Message
  case class SetupChildren(refs: Iterable[ActorRef]) extends Message

  final val DefaultDispatcherId = "akka-rabbitmq.default-connection-dispatcher"

  // For binary compatibility reasons, this version of props is still here
  def props(
    factory: ConnectionFactory,
    reconnectionDelay: FiniteDuration,
    setupConnection: (Connection, ActorRef) => Any): Props =
    props(factory, reconnectionDelay, setupConnection, DefaultDispatcherId)

  def props(
    factory: ConnectionFactory,
    reconnectionDelay: FiniteDuration = 10.seconds,
    setupConnection: (Connection, ActorRef) => Any = (_, _) => (),
    dispatcher: String = DefaultDispatcherId): Props =
    Props(new ConnectionActor(factory, reconnectionDelay, setupConnection))
      .withDispatcher(dispatcher)
}

class ConnectionActor(
  factory: ConnectionFactory,
  reconnectionDelay: FiniteDuration,
  setupConnection: (Connection, ActorRef) => Any) extends RabbitMqActor
  with FSM[ConnectionActor.State, ConnectionActor.Data] {

  import ConnectionActor._

  implicit val executionContext: ExecutionContext = context.dispatcher

  context.system.eventStream.subscribe(self, classOf[DeadLetter])

  val reconnectTimer = "reconnect"

  startWith(Disconnected, NoConnection)

  private def header(state: ConnectionActor.State, msg: Any) = s"${self.path} in $state received $msg:"

  when(Disconnected) {
    case Event(Connect, _) =>
      setup().onComplete {
        case Success(Some(connection)) =>
          log.info(s"[MQ][A] Success - New Connection Created $connection")
          self ! NewConnection(connection)
        case _ =>
          log.error(
            "{} can't connect to {}, retrying in {}",
            header(Disconnected, Connect), factory.uri, reconnectionDelay)
          startSingleTimer(reconnectTimer, Connect, reconnectionDelay)

      }
      stay()

    case Event(msg @ NewConnection(connection), _) =>
      log.info("[MQ][A][Disconnected] {} setup {} children", header(Disconnected, msg),
        children.size)
      self ! SetupChildren(children)
      goto(Connected) using Connected(connection)

    case Event(msg @ CreateChannel(props, name), _) =>
      val child = newChild(props, name)
      log.info("[MQ][A][Disconnected]  {} creating child {} in disconnected " +
        "state", header(Disconnected, msg), child)
      stay() replying ChannelCreated(child)

    case Event(_: AmqpShutdownSignal, _) => stay()

    case Event(_: Reconnect, _)          => stay()

    case Event(ProvideChannel, _) =>
      log.info("[MQ][A][Disconnected] {} Providing a channel for {} in " +
        "disconnected " +
        "state", header(Disconnected, ProvideChannel), sender())
      stay()
  }

  when(Connected) {
    case Event(SetupChildren(refs), Connected(connection)) =>
      setupChildren(connection, refs).onComplete {
        case Success(true) =>
          log.info("[MQ][A][Connected]  {} setup children success", self.path)
        case _ =>
          log.error("[MQ][A][Connected] {} setup children failed", self.path)
          self ! Reconnect(connection)
      }
      stay()

    case Event(msg @ Reconnect(oldConnection), Connected(connection)) =>
      // Check the connection id to guard against Reconnect messages
      // still queued in the mailbox during the previous connection.
      if (oldConnection.getId == connection.getId) {
        log.info("[MQ][A]  {} reconnect to the same ID {}", header(
          Connected,
          msg), factory.uri)
        reconnect(connection, msg)
        goto(Disconnected) using NoConnection
      } else {
        log.debug(
          "[MQ][A]  {} already reconnected to {}",
          header(Connected, msg), factory.uri)
        stay()
      }

    case Event(ProvideChannel, Connected(connection)) =>
      provideChannel(connection, sender(), ProvideChannel)
      stay()

    case Event(msg @ CreateChannel(props, name), Connected(connection)) =>
      val child = newChild(props, name)
      provideChannel(connection, child, msg)
      stay() replying ChannelCreated(child)

    case Event(msg @ AmqpShutdownSignal(cause), Connected(connection)) =>
      // It is important that we check if a shutdown signal pertains to the current connection.
      if (msg.appliesTo(connection)) {
        log.info("[MQ][A]  {} shutdown (initiated by app {})", header(
          Connected,
          msg), cause.isInitiatedByApplication)
        reconnect(connection, msg)
        goto(Disconnected) using NoConnection
      } else stay()
  }

  whenUnhandled {
    case Event(GetState, _) =>
      sender() ! stateName
      stay()

    case Event(msg @ DeadLetter(channel: Channel, `self`, child), _) =>
      log.info("[MQ][A]  {} closing channel {} of child {}", header(stateName,
        msg), channel, child)
      close(channel)
      stay()

    case Event(_: DeadLetter, _) =>
      stay()
  }

  onTransition {
    case Connected -> Disconnected => log.warning("[MQ][A]  {} lost connection to {}", self.path, factory.uri)
    case Disconnected -> Connected => log.info("[MQ][A]  {} connected to {}", self.path, factory.uri)
  }

  onTermination {
    case StopEvent(_, Connected, Connected(connection)) =>
      log.info("[MQ][A]  closing connection to {}", factory.uri)
      close(connection)
  }

  initialize()

  private def reconnect(connection: Connection, msg: Any): Unit = {
    def dropConnectionAndNotifyChildren(): Unit = {
      log.info("[MQ][A]  {} closing broken connection {}", header(
        Connected,
        msg), connection)
      close(connection)

      log.info("[MQ][A]  {} sending shutdown signal to {} children", header(Connected, msg), children.size)
      children.foreach(_ ! ParentShutdownSignal)
    }

    dropConnectionAndNotifyChildren()
    log.info("[MQ][A]  {} reconnecting to {} in {}", header(Connected, msg), factory.uri, reconnectionDelay)
    startSingleTimer(reconnectTimer, Connect, reconnectionDelay)
  }

  /**
   * As connection recovery at this level does not play well
   * with [[http://www.rabbitmq.com/api-guide.html#recovery native recovery]]
   * factory settings are changed to disable it even if it was enabled
   * to ensure correctness of operations.
   */
  private def setup(): Future[Option[Connection]] =
    Future {
      blocking {
        factory.setAutomaticRecoveryEnabled(false)
        log.debug("[MQ][A] setup() {} creating new connection", self.path)
        safe(factory.newConnection()).flatMap { connection =>
          cancelTimer(reconnectTimer)
          connection.addShutdownListener(this)
          log.debug("[MQ][A] setup() {} setting up new connection {}", self
            .path, connection)
          try {
            safe(setupConnection(connection, self)).map(_ => connection)
          } catch {
            case NonFatal(throwable) =>
              log.debug("[MQ][A] setup()  {} setup connection callback error " +
                "{}", self.path, connection)
              close(connection)
              throw throwable
          }
        }
      }
    }

  private def setupChildren(connection: Connection, refs: Iterable[ActorRef]): Future[Boolean] =
    Future {
      blocking {
        refs.foldLeft(true) {
          case (success, child) =>
            success && (safeCreateChannel(connection) match {
              case None => false
              case Some(channel) =>
                child ! channel
                true
            })
        }
      }
    }

  private def provideChannel(connection: Connection, sender: ActorRef, msg: Any): Unit =
    Future(blocking(safeCreateChannel(connection))).onComplete {
      case Success(Some(channel)) =>
        log.info("[MQ][A] {} channel acquired", header(Connected, msg))
        sender ! channel
      case _ =>
        log.error("[MQ][A] {} no channel acquired. ", header(Connected, msg))
        self ! Reconnect(connection)
    }

  private def safeCreateChannel(connection: Connection): Option[Channel] =
    safe(connection.createChannel()).flatMap { channel =>
      if (channel == null) {
        log.info("[MQ][A] {} no channels available on connection {}", self.path, connection)
      }
      Option(channel)
    }

  private[rabbitmq] def children = context.children

  private[rabbitmq] def newChild(props: Props, name: Option[String]) = name match {
    case Some(x) => context.actorOf(props, x)
    case None    => context.actorOf(props)
  }

  override def preStart(): Unit = {
    self ! Connect
  }
}
