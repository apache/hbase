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

  private HBaseServerExceptionPauseManager() {}

  public static OptionalLong getPauseNsFromException(Throwable error, long pauseNs,
    long pauseNsForServerOverloaded, long remainingTimeNs) {
    long expectedSleepNs;
    if (error instanceof RpcThrottlingException) {
      RpcThrottlingException rpcThrottlingException = (RpcThrottlingException) error;
      expectedSleepNs = TimeUnit.MILLISECONDS.toNanos(rpcThrottlingException.getWaitInterval());
      if (expectedSleepNs > remainingTimeNs) {
        return OptionalLong.empty();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sleeping for {}ms after catching RpcThrottlingException", expectedSleepNs,
          rpcThrottlingException);
      }
    } else {
      expectedSleepNs =
        HBaseServerException.isServerOverloaded(error) ? pauseNsForServerOverloaded : pauseNs;
    }
    return OptionalLong.of(expectedSleepNs);
  }

}
