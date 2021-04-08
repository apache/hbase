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

package org.apache.hadoop.hbase.quotas;

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Internal class used to check and consume quota if exceed throttle quota is enabled. Exceed
 * throttle quota means, user can over consume user/namespace/table quota if region server has
 * additional available quota because other users don't consume at the same time.
 *
 * There are some limits when enable exceed throttle quota:
 * 1. Must set at least one read and one write region server throttle quota;
 * 2. All region server throttle quotas must be in seconds time unit. Because once previous requests
 * exceed their quota and consume region server quota, quota in other time units may be refilled in
 * a long time, this may affect later requests.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ExceedOperationQuota extends DefaultOperationQuota {
  private static final Logger LOG = LoggerFactory.getLogger(ExceedOperationQuota.class);
  private QuotaLimiter regionServerLimiter;

  public ExceedOperationQuota(final Configuration conf, QuotaLimiter regionServerLimiter,
      final QuotaLimiter... limiters) {
    super(conf, limiters);
    this.regionServerLimiter = regionServerLimiter;
  }

  @Override
  public void checkQuota(int numWrites, int numReads, int numScans) throws RpcThrottlingException {
    if (regionServerLimiter.isBypass()) {
      // If region server limiter is bypass, which means no region server quota is set, check and
      // throttle by all other quotas. In this condition, exceed throttle quota will not work.
      LOG.warn("Exceed throttle quota is enabled but no region server quotas found");
      super.checkQuota(numWrites, numReads, numScans);
    } else {
      // 1. Update estimate quota which will be consumed
      updateEstimateConsumeQuota(numWrites, numReads, numScans);
      // 2. Check if region server limiter is enough. If not, throw RpcThrottlingException.
      regionServerLimiter.checkQuota(numWrites, writeConsumed, numReads + numScans, readConsumed,
        writeCapacityUnitConsumed, readCapacityUnitConsumed);
      // 3. Check if other limiters are enough. If not, exceed other limiters because region server
      // limiter is enough.
      boolean exceed = false;
      try {
        super.checkQuota(numWrites, numReads, numScans);
      } catch (RpcThrottlingException e) {
        exceed = true;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Read/Write requests num exceeds quota: writes:{} reads:{} scan:{}, "
              + "try use region server quota",
            numWrites, numReads, numScans);
        }
      }
      // 4. Region server limiter is enough and grab estimated consume quota.
      readAvailable = Math.max(readAvailable, regionServerLimiter.getReadAvailable());
      regionServerLimiter.grabQuota(numWrites, writeConsumed, numReads + numScans, readConsumed,
        writeCapacityUnitConsumed, writeCapacityUnitConsumed);
      if (exceed) {
        // 5. Other quota limiter is exceeded and has not been grabbed (because throw
        // RpcThrottlingException in Step 3), so grab it.
        for (final QuotaLimiter limiter : limiters) {
          limiter.grabQuota(numWrites, writeConsumed, numReads + numScans, readConsumed,
            writeCapacityUnitConsumed, writeCapacityUnitConsumed);
        }
      }
    }
  }

  @Override
  public void close() {
    super.close();
    if (writeDiff != 0) {
      regionServerLimiter.consumeWrite(writeDiff, writeCapacityUnitDiff);
    }
    if (readDiff != 0) {
      regionServerLimiter.consumeRead(readDiff, readCapacityUnitDiff);
    }
  }
}
