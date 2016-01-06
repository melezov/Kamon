package akka.kamon.instrumentation

import kamon.akka.RouterMetrics
import kamon.trace.{ EmptyTraceContext, TraceContext }
import kamon.util.NanoTimestamp
import org.aspectj.lang.annotation.{ DeclareMixin, Aspect }

case class EnvelopeContext(nanoTime: NanoTimestamp, context: TraceContext, router: Option[RouterMetrics])

object EnvelopeContext {
  val Empty = EnvelopeContext(new NanoTimestamp(0L), EmptyTraceContext, None)
}

trait InstrumentedEnvelope {
  def envelopeContext(): EnvelopeContext
  def setEnvelopeContext(envelopeContext: EnvelopeContext): Unit
}

object InstrumentedEnvelope {
  def apply(): InstrumentedEnvelope = new InstrumentedEnvelope {
    var envelopeContext: EnvelopeContext = _

    def setEnvelopeContext(envelopeContext: EnvelopeContext): Unit =
      this.envelopeContext = envelopeContext
  }
}

@Aspect
class EnvelopeContextIntoEnvelopeMixin {

  @DeclareMixin("akka.dispatch.Envelope")
  def mixinInstrumentationToEnvelope: InstrumentedEnvelope = InstrumentedEnvelope()
}