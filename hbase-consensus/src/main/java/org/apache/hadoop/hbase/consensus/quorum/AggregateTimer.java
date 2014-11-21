package org.apache.hadoop.hbase.consensus.quorum;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;

/**
 * This is a Timer that can work with multiple ConstituentTimer objects, and
 * invoke their appropriate callbacks when required.
 *
 * The main benefit over RepeatingTimer is to not have to use a separate thread
 * for each timer.
 */
public class AggregateTimer {
  public static final Log LOG = LogFactory.getLog(AggregateTimer.class);
  private ScheduledExecutorService executor;
  Set<ConstituentTimer> timers;

  public class TimerEvent implements Runnable {
    final ConstituentTimer timer;
    private boolean cancelled = false;

    public TimerEvent(ConstituentTimer timer) {
      this.timer = timer;
    }

    public synchronized void cancel() {
      cancelled = true;
    }

    @Override
    public synchronized void run() {
      try {
        if (cancelled || timer.isStopped()) {
          return;
        }

        timer.onTimeOut();
        if (!timer.isStopped()) {
          schedule(this);
        }
      } catch (Exception e) {
        LOG.error("Timer caught an unknown exception ", e);
        throw e;
      }
    }
  }

  public AggregateTimer() {
    this.timers = new ConcurrentSkipListSet<>();
    this.executor = Executors.newSingleThreadScheduledExecutor(
      new DaemonThreadFactory("aggregate-timer"));
  }

  public ConstituentTimer createTimer(
    String timerName, final long delay, final TimeUnit unit,
    final TimeoutEventHandler callback) {
    ConstituentTimer timer =
      new ConstituentTimer(this, timerName, delay, unit, callback);
    submitNewTimerEvent(timer);

    return timer;
  }

  public TimerEvent submitNewTimerEvent(final ConstituentTimer timer) {
    if (!timer.isStopped()) {
      TimerEvent event = new TimerEvent(timer);
      schedule(event);
      return event;
    }
    return null;
  }

  void schedule(TimerEvent event) {
    executor.schedule(event,
      event.timer.getDelayMillis() + event.timer.getBackOffInterval(),
      TimeUnit.MILLISECONDS);
  }

  public void shutdown() {
    executor.shutdown();
  }

  public void shutdownNow() {
    executor.shutdownNow();
  }
}
