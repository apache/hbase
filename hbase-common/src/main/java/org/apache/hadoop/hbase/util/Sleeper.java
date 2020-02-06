/**
 *
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
package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.Stoppable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sleeper for current thread.
 * Sleeps for passed period.  Also checks passed boolean and if interrupted,
 * will return if the flag is set (rather than go back to sleep until its
 * sleep time is up).
 */
@InterfaceAudience.Private
public class Sleeper {
  private static final Logger LOG = LoggerFactory.getLogger(Sleeper.class);
  private final int period;
  private final Stoppable stopper;
  private static final long MINIMAL_DELTA_FOR_LOGGING = 10000;

  private final Object sleepLock = new Object();
  private boolean triggerWake = false;

  /**
   * @param sleep sleep time in milliseconds
   * @param stopper When {@link Stoppable#isStopped()} is true, this thread will
   *    cleanup and exit cleanly.
   */
  public Sleeper(final int sleep, final Stoppable stopper) {
    this.period = sleep;
    this.stopper = stopper;
  }

  /**
   * If currently asleep, stops sleeping; if not asleep, will skip the next
   * sleep cycle.
   */
  public void skipSleepCycle() {
    synchronized (sleepLock) {
      triggerWake = true;
      sleepLock.notifyAll();
    }
  }

  /**
   * Sleep for period.
   */
  public void sleep() {
    sleep(this.period);
  }

  public void sleep(long sleepTime) {
    if (this.stopper.isStopped()) {
      return;
    }
    long now = System.currentTimeMillis();
    long currentSleepTime = sleepTime;
    while (currentSleepTime > 0) {
      long woke = -1;
      try {
        synchronized (sleepLock) {
          if (triggerWake) {
            break;
          }

          sleepLock.wait(currentSleepTime);
        }
        woke = System.currentTimeMillis();
        long slept = woke - now;
        if (slept - this.period > MINIMAL_DELTA_FOR_LOGGING) {
          LOG.warn("We slept {}ms instead of {}ms, this is likely due to a long " +
              "garbage collecting pause and it's usually bad, see " +
              "http://hbase.apache.org/book.html#trouble.rs.runtime.zkexpired", slept, this.period);
        }
      } catch(InterruptedException iex) {
        // We we interrupted because we're meant to stop?  If not, just
        // continue ignoring the interruption
        if (this.stopper.isStopped()) {
          return;
        }
      }
      // Recalculate waitTime.
      woke = (woke == -1)? System.currentTimeMillis(): woke;
      currentSleepTime = this.period - (woke - now);
    }
    synchronized(sleepLock) {
      triggerWake = false;
    }
  }

  /**
   * @return the sleep period in milliseconds
   */
  public final int getPeriod() {
    return period;
  }
}
