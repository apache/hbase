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
package org.apache.hadoop.hbase.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.util.List;
import java.util.Map;

/**
 * A wrapper for a runnable for a group of actions for a single regionserver.
 * <p>
 * This can be used to build up the actions that should be taken and then
 * </p>
 * <p>
 * This class exists to simulate using a ScheduledExecutorService with just a regular
 * ExecutorService and Runnables. It is used for legacy reasons in the the client; this could
 * only be removed if we change the expectations in HTable around the pool the client is able to
 * pass in and even if we deprecate the current APIs would require keeping this class around
 * for the interim to bridge between the legacy ExecutorServices and the scheduled pool.
 * </p>
 */
@InterfaceAudience.Private
public class DelayingRunner<T> implements Runnable {
  private static final Log LOG = LogFactory.getLog(DelayingRunner.class);

  private final Object sleepLock = new Object();
  private boolean triggerWake = false;
  private long sleepTime;
  private MultiAction<T> actions = new MultiAction<T>();
  private Runnable runnable;

  public DelayingRunner(long sleepTime, Map.Entry<byte[], List<Action<T>>> e) {
    this.sleepTime = sleepTime;
    add(e);
  }

  public void setRunner(Runnable runner) {
    this.runnable = runner;
  }

  @Override
  public void run() {
    if (!sleep()) {
      LOG.warn(
          "Interrupted while sleeping for expected sleep time " + sleepTime + " ms");
    }
    //TODO maybe we should consider switching to a listenableFuture for the actual callable and
    // then handling the results/errors as callbacks. That way we can decrement outstanding tasks
    // even if we get interrupted here, but for now, we still need to run so we decrement the
    // outstanding tasks
    this.runnable.run();
  }

  /**
   * Sleep for an expected amount of time.
   * <p>
   * This is nearly a copy of what the Sleeper does, but with the ability to know if you
   * got interrupted while sleeping.
   * </p>
   *
   * @return <tt>true</tt> if the sleep completely entirely successfully,
   * but otherwise <tt>false</tt> if the sleep was interrupted.
   */
  private boolean sleep() {
    long now = EnvironmentEdgeManager.currentTime();
    long startTime = now;
    long waitTime = sleepTime;
    while (waitTime > 0) {
      long woke = -1;
      try {
        synchronized (sleepLock) {
          if (triggerWake) break;
          sleepLock.wait(waitTime);
        }
        woke = EnvironmentEdgeManager.currentTime();
      } catch (InterruptedException iex) {
        return false;
      }
      // Recalculate waitTime.
      woke = (woke == -1) ? EnvironmentEdgeManager.currentTime() : woke;
      waitTime = waitTime - (woke - startTime);
    }
    return true;
  }

  public void add(Map.Entry<byte[], List<Action<T>>> e) {
    actions.add(e.getKey(), e.getValue());
  }

  public MultiAction<T> getActions() {
    return actions;
  }

  public long getSleepTime() {
    return sleepTime;
  }
}