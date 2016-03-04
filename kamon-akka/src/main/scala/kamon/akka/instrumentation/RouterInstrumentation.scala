package akka.kamon.instrumentation

import akka.actor.{ Cell, Props, ActorRef, ActorSystem }
import akka.dispatch.{ Envelope, MessageDispatcher }
import akka.routing.RoutedActorCell
import kamon.Kamon
import kamon.akka.RouterMetrics
import kamon.metric.Entity
import kamon.util.RelativeNanoTimestamp
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation._

trait RouterInstrumentation {
  def processMessage(pjp: ProceedingJoinPoint): AnyRef
  def processFailure(failure: Throwable): Unit
  def cleanup(): Unit

  def routeeAdded(): Unit
  def routeeRemoved(): Unit
}

object RouterInstrumentation {

  def createRouterInstrumentation(cell: Cell): RouterInstrumentation = {
    val cellInfo = CellInfo.cellInfoFor(cell, cell.system, cell.self, cell.parent)
    def routerMetrics = Kamon.metrics.entity(RouterMetrics, cellInfo.entity)

    if (cellInfo.isTracked)
      new MetricsOnlyRouterInstrumentation(cellInfo.entity, routerMetrics)
    else NoOpRouterInstrumentation
  }
}

object NoOpRouterInstrumentation extends RouterInstrumentation {
  def processMessage(pjp: ProceedingJoinPoint): AnyRef = pjp.proceed()
  def processFailure(failure: Throwable): Unit = {}
  def routeeAdded(): Unit = {}
  def routeeRemoved(): Unit = {}
  def cleanup(): Unit = {}
}

class MetricsOnlyRouterInstrumentation(entity: Entity, routerMetrics: RouterMetrics) extends RouterInstrumentation {

  def processMessage(pjp: ProceedingJoinPoint): AnyRef = {
    val timestampBeforeProcessing = RelativeNanoTimestamp.now

    try {
      pjp.proceed()
    } finally {
      val timestampAfterProcessing = RelativeNanoTimestamp.now
      val routingTime = timestampAfterProcessing - timestampBeforeProcessing

      routerMetrics.routingTime.record(routingTime.nanos)
    }
  }

  def processFailure(failure: Throwable): Unit = {}
  def routeeAdded(): Unit = {}
  def routeeRemoved(): Unit = {}
  def cleanup(): Unit = {}
}

@Aspect
class RoutedActorCellInstrumentation {

  def routerInstrumentation(cell: Cell): RouterInstrumentation =
    cell.asInstanceOf[RouterInstrumentationAware].routerInstrumentation

  @Pointcut("execution(akka.routing.RoutedActorCell.new(..)) && this(cell) && args(system, ref, props, dispatcher, routeeProps, supervisor)")
  def routedActorCellCreation(cell: RoutedActorCell, system: ActorSystem, ref: ActorRef, props: Props, dispatcher: MessageDispatcher, routeeProps: Props, supervisor: ActorRef): Unit = {}

  @After("routedActorCellCreation(cell, system, ref, props, dispatcher, routeeProps, supervisor)")
  def afterRoutedActorCellCreation(cell: RoutedActorCell, system: ActorSystem, ref: ActorRef, props: Props, dispatcher: MessageDispatcher, routeeProps: Props, supervisor: ActorRef): Unit = {
    cell.asInstanceOf[RouterInstrumentationAware].setRouterInstrumentation(
      RouterInstrumentation.createRouterInstrumentation(cell))
  }

  @Pointcut("execution(* akka.routing.RoutedActorCell.sendMessage(*)) && this(cell) && args(envelope)")
  def sendMessageInRouterActorCell(cell: RoutedActorCell, envelope: Envelope) = {}

  @Around("sendMessageInRouterActorCell(cell, envelope)")
  def aroundSendMessageInRouterActorCell(pjp: ProceedingJoinPoint, cell: RoutedActorCell, envelope: Envelope): Any = {
    routerInstrumentation(cell).processMessage(pjp)
  }
}

trait RouterInstrumentationAware {
  def routerInstrumentation: RouterInstrumentation
  def setRouterInstrumentation(ai: RouterInstrumentation): Unit
}

object RouterInstrumentationAware {
  def apply(): RouterInstrumentationAware = new RouterInstrumentationAware {
    private var _ri: RouterInstrumentation = _

    def setRouterInstrumentation(ai: RouterInstrumentation): Unit = _ri = ai
    def routerInstrumentation: RouterInstrumentation = _ri
  }
}

@Aspect
class MetricsIntoRouterCellsMixin {

  @DeclareMixin("akka.routing.RoutedActorCell")
  def mixinActorCellMetricsToRoutedActorCell: RouterInstrumentationAware = RouterInstrumentationAware()

}