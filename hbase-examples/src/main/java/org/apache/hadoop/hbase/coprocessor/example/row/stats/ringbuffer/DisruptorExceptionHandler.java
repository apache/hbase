package org.apache.hadoop.hbase.coprocessor.example.row.stats.ringbuffer;

import com.lmax.disruptor.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class DisruptorExceptionHandler implements ExceptionHandler<RingBufferEnvelope> {

  private static final Logger LOG = LoggerFactory.getLogger(
    DisruptorExceptionHandler.class
  );

  @Override
  public void handleEventException(Throwable e, long sequence, RingBufferEnvelope event) {
    if (event != null) {
      LOG.error(
        "Unable to persist event={} with sequence={}",
        event.getPayload(),
        sequence,
        e
      );
    } else {
      LOG.error("Event with sequence={} was null", sequence, e);
    }
  }

  @Override
  public void handleOnStartException(Throwable e) {
    LOG.error("Disruptor onStartException", e);
  }

  @Override
  public void handleOnShutdownException(Throwable e) {
    LOG.error("Disruptor onShutdownException", e);
  }
}
