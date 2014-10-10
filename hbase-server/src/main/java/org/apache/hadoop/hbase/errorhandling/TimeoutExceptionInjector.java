/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.errorhandling;

import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Time a given process/operation and report a failure if the elapsed time exceeds the max allowed
 * time.
 * <p>
 * The timer won't start tracking time until calling {@link #start()}. If {@link #complete()} or
 * {@link #trigger()} is called before {@link #start()}, calls to {@link #start()} will fail.
 */
@InterfaceAudience.Private
public class TimeoutExceptionInjector {

  private static final Log LOG = LogFactory.getLog(TimeoutExceptionInjector.class);

  private final long maxTime;
  private volatile boolean complete;
  private final Timer timer;
  private final TimerTask timerTask;
  private long start = -1;

  /**
   * Create a generic timer for a task/process.
   * @param listener listener to notify if the process times out
   * @param maxTime max allowed running time for the process. Timer starts on calls to
   *          {@link #start()}
   */
  public TimeoutExceptionInjector(final ForeignExceptionListener listener, final long maxTime) {
    this.maxTime = maxTime;
    timer = new Timer();
    timerTask = new TimerTask() {
      @Override
      public void run() {
        // ensure we don't run this task multiple times
        synchronized (this) {
          // quick exit if we already marked the task complete
          if (TimeoutExceptionInjector.this.complete) return;
          // mark the task is run, to avoid repeats
          TimeoutExceptionInjector.this.complete = true;
        }
        long end = EnvironmentEdgeManager.currentTime();
        TimeoutException tee =  new TimeoutException(
            "Timeout caused Foreign Exception", start, end, maxTime);
        String source = "timer-" + timer;
        listener.receive(new ForeignException(source, tee));
      }
    };
  }

  public long getMaxTime() {
    return maxTime;
  }

  /**
   * For all time forward, do not throw an error because the process has completed.
   */
  public void complete() {
    synchronized (this.timerTask) {
      if (this.complete) {
        LOG.warn("Timer already marked completed, ignoring!");
        return;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Marking timer as complete - no error notifications will be received for " +
          "this timer.");
      }
      this.complete = true;
    }
    this.timer.cancel();
  }

  /**
   * Start a timer to fail a process if it takes longer than the expected time to complete.
   * <p>
   * Non-blocking.
   * @throws IllegalStateException if the timer has already been marked done via {@link #complete()}
   *           or {@link #trigger()}
   */
  public synchronized void start() throws IllegalStateException {
    if (this.start >= 0) {
      LOG.warn("Timer already started, can't be started again. Ignoring second request.");
      return;
    }
    LOG.debug("Scheduling process timer to run in: " + maxTime + " ms");
    timer.schedule(timerTask, maxTime);
    this.start = EnvironmentEdgeManager.currentTime();
  }

  /**
   * Trigger the timer immediately.
   * <p>
   * Exposed for testing.
   */
  public void trigger() {
    synchronized (timerTask) {
      if (this.complete) {
        LOG.warn("Timer already completed, not triggering.");
        return;
      }
      LOG.debug("Triggering timer immediately!");
      this.timer.cancel();
      this.timerTask.run();
    }
  }
}
