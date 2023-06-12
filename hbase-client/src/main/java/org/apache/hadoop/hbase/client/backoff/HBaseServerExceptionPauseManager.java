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

import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseServerException;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class HBaseServerExceptionPauseManager {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseServerExceptionPauseManager.class);

  private final long pauseNs;
  private final long pauseNsForServerOverloaded;

  public HBaseServerExceptionPauseManager(long pauseNs, long pauseNsForServerOverloaded) {
    this.pauseNs = pauseNs;
    this.pauseNsForServerOverloaded = pauseNsForServerOverloaded;
  }

  /**
   * Returns the nanos, if any, for which the client should wait
   * @param error           The exception from the server
   * @param remainingTimeNs The remaining nanos before timeout
   * @return The time, in nanos, to pause. If empty then pausing would exceed our timeout, so we
   *         should throw now
   */
  public OptionalLong getPauseNsFromException(Throwable error, long remainingTimeNs) {
    long expectedSleepNs;
    if (error instanceof RpcThrottlingException) {
      RpcThrottlingException rpcThrottlingException = (RpcThrottlingException) error;
      expectedSleepNs = TimeUnit.MILLISECONDS.toNanos(rpcThrottlingException.getWaitInterval());
      if (expectedSleepNs > remainingTimeNs) {
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
    } else {
      expectedSleepNs =
        HBaseServerException.isServerOverloaded(error) ? pauseNsForServerOverloaded : pauseNs;
    }
    return OptionalLong.of(expectedSleepNs);
  }

}
