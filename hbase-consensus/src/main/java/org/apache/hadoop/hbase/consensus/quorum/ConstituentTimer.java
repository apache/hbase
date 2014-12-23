package org.apache.hadoop.hbase.consensus.quorum;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.TimeUnit;

/**
 * This is a timer which is a part of an AggregateTimer, which holds several
 * such timers, and invokes the callbacks on the child timers as required.
 *
 * The timer retains the behavior of the regular timer, and the user need not
 * know that this timer doesn't own an exclusive thread.
 */
public class ConstituentTimer implements Timer {
  public static final Log LOG = LogFactory.getLog(ConstituentTimer.class);
  private volatile long delayMillis;
  private volatile long backOffInterval;

  private volatile boolean isStopped = true;

  AggregateTimer aggregateTimer;
  TimeoutEventHandler callback;
  AggregateTimer.TimerEvent timerEvent;

  String timerName;

  /**
   * @param aggregateTimer The AggregateTimer object to use.
   * @param delay Delay between the timeouts
   * @param timeUnit The time unit of the delay
   * @param callback The callback to register
   */
  public ConstituentTimer(AggregateTimer aggregateTimer,
                          String timerName,
                          long delay,
                          TimeUnit timeUnit,
                          TimeoutEventHandler callback) {
    this.aggregateTimer = aggregateTimer;
    this.callback = callback;
    this.delayMillis = TimeUnit.MILLISECONDS.convert(delay, timeUnit);
    this.backOffInterval = 0;
    this.timerName = timerName;
  }

  @Override
  public synchronized void start() {
    if (isStopped) {
      isStopped = false;
      timerEvent = aggregateTimer.submitNewTimerEvent(this);
      backOffInterval = 0;
    }
  }

  @Override
  public synchronized void stop() {
    if (!isStopped) {
      isStopped = true;
      timerEvent.cancel();
      timerEvent = null;
    }
  }

  @Override
  public synchronized void reset() {
    if (!isStopped) {
      // Reset happens by proactively removing and inserting the timer event
      // again.
      timerEvent.cancel();
      timerEvent = aggregateTimer.submitNewTimerEvent(this);
      backOffInterval = 0;
    }
  }

  @Override
  public synchronized void shutdown() {
    stop();
  }

  @Override
  public synchronized void backoff(long backOffTime, TimeUnit units) {
    backOffInterval = TimeUnit.MILLISECONDS.convert(backOffTime, units);
  }

  @Override
  public synchronized void setDelay(long delay, TimeUnit unit) {
    delayMillis = TimeUnit.MILLISECONDS.convert(delay, unit);
  }

  public void onTimeOut() {
    callback.onTimeout();
  }

  public long getDelayMillis() {
    return delayMillis;
  }

  public long getBackOffInterval() {
    return backOffInterval;
  }

  public boolean isStopped() {
    return isStopped;
  }

  public String getTimerName() {
    return timerName;
  }
}
