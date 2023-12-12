/*
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
package org.apache.hadoop.hbase.client.backoff;

import static org.apache.hadoop.hbase.client.ConnectionUtils.SLEEP_DELTA_NS;

import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseServerException;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class HBaseServerExceptionPauseManager {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseServerExceptionPauseManager.class);

  private final long pauseNs;
  private final long pauseNsForServerOverloaded;
  private final long timeoutNs;

  public HBaseServerExceptionPauseManager(long pauseNs, long pauseNsForServerOverloaded,
    long timeoutNs) {
    this.pauseNs = pauseNs;
    this.pauseNsForServerOverloaded = pauseNsForServerOverloaded;
    this.timeoutNs = timeoutNs;
  }

  public OptionalLong getPauseNsFromException(Throwable error, int tries, long startNs) {
    return getPauseNsFromException(error, tries, startNs, null);
  }

  /**
   * Returns the nanos, if any, for which the client should wait
   * @param error The exception from the server
   * @param tries The current retry count
   * @return The time, in nanos, to pause. If empty then pausing would exceed our timeout, so we
   *         should throw now
   */
  public OptionalLong getPauseNsFromException(Throwable error, int tries, long startNs,
    ScanMetrics scanMetrics) {
    long remainingTimeNs = remainingTimeNs(startNs) - SLEEP_DELTA_NS;
    if (error instanceof RpcThrottlingException) {
      RpcThrottlingException rpcThrottlingException = (RpcThrottlingException) error;
      long waitInterval = rpcThrottlingException.getWaitInterval();
      if (scanMetrics != null) {
        scanMetrics.throttleTime.addAndGet(waitInterval);
      }
      long expectedSleepNs = TimeUnit.MILLISECONDS.toNanos(waitInterval);
      if (timeoutNs > 0 && expectedSleepNs > remainingTimeNs) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("RpcThrottlingException suggested pause of {}ns which would exceed "
            + "the timeout. We should throw instead.", expectedSleepNs, rpcThrottlingException);
        }
        return OptionalLong.empty();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sleeping for {}ns after catching RpcThrottlingException", expectedSleepNs,
          rpcThrottlingException);
      }
      return OptionalLong.of(expectedSleepNs);
    } else {
      if (HBaseServerException.isServerOverloaded(error)) {
        OptionalLong expectedSleepNs =
          getRelativePauseTime(pauseNsForServerOverloaded, remainingTimeNs, tries);
        if (scanMetrics != null && expectedSleepNs.isPresent()) {
          scanMetrics.throttleTime
            .addAndGet(TimeUnit.NANOSECONDS.toMillis(expectedSleepNs.getAsLong()));
        }
        return expectedSleepNs;
      } else {
        return getRelativePauseTime(pauseNs, remainingTimeNs, tries);
      }
    }
  }

  private OptionalLong getRelativePauseTime(long basePause, long remainingTimeNs, int tries) {
    long expectedSleepNs = ConnectionUtils.getPauseTime(basePause, tries - 1);
    if (timeoutNs > 0) {
      if (remainingTimeNs <= 0) {
        return OptionalLong.empty();
      }
      expectedSleepNs = Math.min(remainingTimeNs, expectedSleepNs);
    }
    return OptionalLong.of(expectedSleepNs);
  }

  public long remainingTimeNs(long startNs) {
    return timeoutNs - (System.nanoTime() - startNs);
  }

}
