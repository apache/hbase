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
package org.apache.hadoop.hbase.regionserver.regionreplication;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hbase.thirdparty.io.netty.util.HashedWheelTimer;
import org.apache.hbase.thirdparty.io.netty.util.Timeout;

/**
 * A helper class for requesting flush on a given region.
 * <p/>
 * In general, we do not want to trigger flush too frequently for a region, so here we will add
 * something like a rate control, i.e, the interval of the two flush request should not be too
 * small.
 */
@InterfaceAudience.Private
class RegionReplicationFlushRequester {

  /**
   * The timer for executing delayed flush request task.
   * <p/>
   * It will be shared across all the instances {@link RegionReplicationFlushRequester}. Created on
   * demand to save one extra thread as not every user uses region replication.
   */
  private static volatile HashedWheelTimer TIMER;

  /**
   * The minimum interval between two flush requests
   */
  public static final String MIN_INTERVAL_SECS =
    "hbase.region.read-replica.sink.flush.min-interval.secs";

  public static final int MIN_INTERVAL_SECS_DEFAULT = 30;

  private final Runnable flushRequester;

  private final long minIntervalSecs;

  private long lastRequestNanos;

  private long pendingFlushRequestSequenceId;

  private long lastFlushedSequenceId;

  private Timeout pendingFlushRequest;

  RegionReplicationFlushRequester(Configuration conf, Runnable flushRequester) {
    this.flushRequester = flushRequester;
    this.minIntervalSecs = conf.getInt(MIN_INTERVAL_SECS, MIN_INTERVAL_SECS_DEFAULT);
  }

  private static HashedWheelTimer getTimer() {
    HashedWheelTimer timer = TIMER;
    if (timer != null) {
      return timer;
    }
    synchronized (RegionReplicationFlushRequester.class) {
      timer = TIMER;
      if (timer != null) {
        return timer;
      }
      timer = new HashedWheelTimer(
        new ThreadFactoryBuilder().setNameFormat("RegionReplicationFlushRequester-Timer-pool-%d")
          .setDaemon(true).setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build(),
        500, TimeUnit.MILLISECONDS);
      TIMER = timer;
    }
    return timer;
  }

  private void request() {
    flushRequester.run();
    lastRequestNanos = System.nanoTime();
  }

  private synchronized void flush(Timeout timeout) {
    pendingFlushRequest = null;
    if (pendingFlushRequestSequenceId >= lastFlushedSequenceId) {
      request();
    }
  }

  /**
   * Request a flush for the given region.
   * <p/>
   * The sequence id of the edit which we fail to replicate. A flush must happen after this sequence
   * id to recover the failure.
   */
  synchronized void requestFlush(long sequenceId) {
    // if there is already a flush task, just reuse it.
    if (pendingFlushRequest != null) {
      pendingFlushRequestSequenceId = Math.max(sequenceId, pendingFlushRequestSequenceId);
      return;
    }
    // check last flush time
    long elapsedSecs = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - lastRequestNanos);
    if (elapsedSecs >= minIntervalSecs) {
      request();
      return;
    }
    // schedule a timer task
    HashedWheelTimer timer = getTimer();
    pendingFlushRequest =
      timer.newTimeout(this::flush, minIntervalSecs - elapsedSecs, TimeUnit.SECONDS);
  }

  /**
   * Record that we have already finished a flush with the given {@code sequenceId}.
   * <p/>
   * We can cancel the pending flush request if the failed sequence id is less than the given
   * {@code sequenceId}.
   */
  synchronized void recordFlush(long sequenceId) {
    this.lastFlushedSequenceId = sequenceId;
    // cancel the pending flush request if it is necessary, i.e, we have already finished a flush
    // with higher sequence id.
    if (sequenceId > pendingFlushRequestSequenceId && pendingFlushRequest != null) {
      pendingFlushRequest.cancel();
      pendingFlushRequest = null;
    }
  }
}
