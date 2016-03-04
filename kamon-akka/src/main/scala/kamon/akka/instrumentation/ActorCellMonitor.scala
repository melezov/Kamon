package akka.kamon.instrumentation

import akka.actor.{ Cell, ActorRef, ActorSystem }
import akka.kamon.instrumentation.ActorCellMonitors.{ TrackedRoutee, ContextPropagation, TrackedActor }
import kamon.Kamon
import kamon.akka.TraceContextPropagationSettings.{ Always, MonitoredActorsOnly, Off }
import kamon.akka.{ AkkaExtension, RouterMetrics, ActorMetrics }
import kamon.metric.Entity
import kamon.trace.{ TraceContext, EmptyTraceContext, Tracer }
import kamon.util.RelativeNanoTimestamp
import org.aspectj.lang.ProceedingJoinPoint

trait ActorCellMonitor {
  def captureEnvelopeContext(): EnvelopeContext = EnvelopeContext(captureTimestamp, captureTraceContext)
  def processMessage(pjp: ProceedingJoinPoint, envelopeContext: EnvelopeContext): AnyRef = pjp.proceed()
  def processFailure(failure: Throwable): Unit = {}
  def cleanup(): Unit = {}

  protected def captureTimestamp: RelativeNanoTimestamp = RelativeNanoTimestamp.zero
  protected def captureTraceContext: TraceContext = EmptyTraceContext
}

object ActorCellMonitor {

  def createActorInstrumentation(cell: Cell, system: ActorSystem, ref: ActorRef, parent: ActorRef): ActorCellMonitor = {
    val cellInfo = CellInfo.cellInfoFor(cell, system, ref, parent)

    if (cellInfo.isRouter)
      ActorCellMonitors.NoOp
    else {
      if (cellInfo.isRoutee)
        createRouteeMonitor(cellInfo)
      else
        createRegularActorMonitor(cellInfo)
    }
  }

  def createRegularActorMonitor(cellInfo: CellInfo): ActorCellMonitor = {
    def actorMetrics = Kamon.metrics.entity(ActorMetrics, cellInfo.entity)

    AkkaExtension.traceContextPropagation match {
      case Off if cellInfo.isTracked                 ⇒ new TrackedActor(cellInfo.entity, actorMetrics)
      case Off                                       ⇒ ActorCellMonitors.NoOp
      case MonitoredActorsOnly if cellInfo.isTracked ⇒ new TrackedActor(cellInfo.entity, actorMetrics) with ContextPropagation
      case MonitoredActorsOnly                       ⇒ ActorCellMonitors.NoOp
      case Always if cellInfo.isTracked              ⇒ new TrackedActor(cellInfo.entity, actorMetrics) with ContextPropagation
      case Always                                    ⇒ ActorCellMonitors.ContextPropagationOnly
    }
  }

  def createRouteeMonitor(cellInfo: CellInfo): ActorCellMonitor = {
    def routerMetrics = Kamon.metrics.entity(RouterMetrics, cellInfo.entity)

    AkkaExtension.traceContextPropagation match {
      case Off if cellInfo.isTracked                 ⇒ new TrackedRoutee(cellInfo.entity, routerMetrics)
      case Off                                       ⇒ ActorCellMonitors.NoOp
      case MonitoredActorsOnly if cellInfo.isTracked ⇒ new TrackedRoutee(cellInfo.entity, routerMetrics) with ContextPropagation
      case MonitoredActorsOnly                       ⇒ ActorCellMonitors.NoOp
      case Always if cellInfo.isTracked              ⇒ new TrackedRoutee(cellInfo.entity, routerMetrics) with ContextPropagation
      case Always                                    ⇒ ActorCellMonitors.ContextPropagationOnly
    }
  }
}

object ActorCellMonitors {
  val NoOp = new ActorCellMonitor {}
  val ContextPropagationOnly = new ActorCellMonitor with ContextPropagation

  trait ContextPropagation extends ActorCellMonitor {
    override protected def captureTraceContext: TraceContext = Tracer.currentContext

    override def processMessage(pjp: ProceedingJoinPoint, envelopeContext: EnvelopeContext): AnyRef = {
      Tracer.withContext(envelopeContext.context) {
        super.processMessage(pjp, envelopeContext)
      }
    }
  }

  class TrackedActor(entity: Entity, actorMetrics: ActorMetrics) extends ActorCellMonitor {
    override def captureEnvelopeContext(): EnvelopeContext = {
      actorMetrics.mailboxSize.increment()
      super.captureEnvelopeContext()
    }

    override def processMessage(pjp: ProceedingJoinPoint, envelopeContext: EnvelopeContext): AnyRef = {
      val timestampBeforeProcessing = RelativeNanoTimestamp.now

      try {
        super.processMessage(pjp, envelopeContext)

      } finally {
        val timestampAfterProcessing = RelativeNanoTimestamp.now
        val timeInMailbox = timestampBeforeProcessing - envelopeContext.nanoTime
        val processingTime = timestampAfterProcessing - timestampBeforeProcessing

        actorMetrics.processingTime.record(processingTime.nanos)
        actorMetrics.timeInMailbox.record(timeInMailbox.nanos)
        actorMetrics.mailboxSize.decrement()
      }
    }

    override def processFailure(failure: Throwable): Unit = actorMetrics.errors.increment()
    override def captureTimestamp: RelativeNanoTimestamp = RelativeNanoTimestamp.now
    override def cleanup(): Unit = Kamon.metrics.removeEntity(entity)
  }

  class TrackedRoutee(entity: Entity, routerMetrics: RouterMetrics) extends ActorCellMonitor {

    override def processMessage(pjp: ProceedingJoinPoint, envelopeContext: EnvelopeContext): AnyRef = {
      val timestampBeforeProcessing = RelativeNanoTimestamp.now

      try {
        super.processMessage(pjp, envelopeContext)

      } finally {
        val timestampAfterProcessing = RelativeNanoTimestamp.now
        val timeInMailbox = timestampBeforeProcessing - envelopeContext.nanoTime
        val processingTime = timestampAfterProcessing - timestampBeforeProcessing

        routerMetrics.processingTime.record(processingTime.nanos)
        routerMetrics.timeInMailbox.record(timeInMailbox.nanos)
      }
    }

    override def processFailure(failure: Throwable): Unit = routerMetrics.errors.increment()
    override def captureTimestamp: RelativeNanoTimestamp = RelativeNanoTimestamp.now
    override def cleanup(): Unit = Kamon.metrics.removeEntity(entity)
  }
}