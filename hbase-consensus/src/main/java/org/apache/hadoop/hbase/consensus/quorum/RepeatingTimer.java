package org.apache.hadoop.hbase.consensus.quorum;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.hbase.util.DaemonThreadFactory;

/**
 * A single threaded Timer implementation. In production we use the
 * AggregateTimer + ConsitutentTimer combination.
 */
public class RepeatingTimer implements Timer, Runnable {
  private volatile long delayMillis;
  private volatile long backOffInterval;
  private TimeoutEventHandler callBack;
  private ExecutorService executor;

  private volatile boolean isStopped = true;
  private volatile boolean isTaskSubmitted = false;
  private volatile boolean shouldReset = false;

  public RepeatingTimer(final String name, final long delay, TimeUnit unit,
                           final TimeoutEventHandler callback) {
    this.delayMillis = TimeUnit.MILLISECONDS.convert(delay, unit);
    this.callBack = callback;
    this.executor = Executors.newSingleThreadExecutor(
            new DaemonThreadFactory(name + "-timer"));
    this.backOffInterval = 0;
  }

  @Override
  public synchronized void start() {
    isStopped = false;
    if (!isTaskSubmitted) {
      executor.submit(this);
      isTaskSubmitted = true;
    }
  }

  @Override
  public synchronized void stop() {
    if (isTaskSubmitted) {
      isStopped = true;
      this.notifyAll();
    }
  }

  @Override
  public synchronized void reset() {
    backOffInterval = 0;
    if (isTaskSubmitted) {
      shouldReset = true;
      this.notifyAll();
    }
  }

  @Override
  public synchronized void shutdown() {
    executor.shutdown();
    stop();
  }

  @Override public synchronized void backoff(long backOffTime, TimeUnit units) {
    backOffInterval = TimeUnit.MILLISECONDS.convert(backOffTime, units);
  }

  @Override
  public synchronized void run() {
    try {
      while (!isStopped) {
        wait(delayMillis + backOffInterval);
        if (!isStopped) {  // The timer might have been stopped.
          if (!shouldReset) {
            callBack.onTimeout();
          } else {
            shouldReset = false;
          }
        }
      }
    } catch (InterruptedException ex) {
      // This should not happen under normal circumstances. If the waiting
      // thread is interrupted, assume something bad happened and let the
      // task complete.
    } finally {
      isTaskSubmitted = false;
    }
  }

  public synchronized void setDelay(final long delay, TimeUnit unit) {
    delayMillis = TimeUnit.MILLISECONDS.convert(delay, unit);
  }
}
