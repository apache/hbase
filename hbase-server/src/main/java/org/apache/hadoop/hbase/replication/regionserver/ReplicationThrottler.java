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
package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Per-peer per-node throttling controller for replication: enabled if
 * bandwidth > 0, a cycle = 100ms, by throttling we guarantee data pushed
 * to peer within each cycle won't exceed 'bandwidth' bytes
 */
@InterfaceAudience.Private
public class ReplicationThrottler {
  private final boolean enabled;
  private final double bandwidth;
  private long cyclePushSize;
  private long cycleStartTick;

  /**
   * ReplicationThrottler constructor
   * If bandwidth less than 1, throttling is disabled
   * @param bandwidth per cycle(100ms)
   */
  public ReplicationThrottler(final double bandwidth) {
    this.bandwidth = bandwidth;
    this.enabled = this.bandwidth > 0;
    if (this.enabled) {
      this.cyclePushSize = 0;
      this.cycleStartTick = EnvironmentEdgeManager.currentTimeMillis();
    }
  }

  /**
   * If throttling is enabled
   * @return true if throttling is enabled
   */
  public boolean isEnabled() {
    return this.enabled;
  }

  /**
   * Get how long the caller should sleep according to the current size and
   * current cycle's total push size and start tick, return the sleep interval
   * for throttling control.
   * @param size is the size of edits to be pushed
   * @return sleep interval for throttling control
   */
  public long getNextSleepInterval(final int size) {
    if (!this.enabled) {
      return 0;
    }

    long sleepTicks = 0;
    long now = EnvironmentEdgeManager.currentTimeMillis();
    // 1. if cyclePushSize exceeds bandwidth, we need to sleep some
    //    following cycles to amortize, this case can occur when a single push
    //    exceeds the bandwidth
    if ((double)this.cyclePushSize > bandwidth) {
      double cycles = Math.ceil((double)this.cyclePushSize / bandwidth);
      long shouldTillTo = this.cycleStartTick + (long)(cycles * 100);
      if (shouldTillTo > now) {
        sleepTicks = shouldTillTo - now;
      } else {
        // no reset in shipEdits since no sleep, so we need to reset cycleStartTick here!
        this.cycleStartTick = now;
      }
      this.cyclePushSize = 0;
    } else {
      long nextCycleTick = this.cycleStartTick + 100;  //a cycle is 100ms
      if (now >= nextCycleTick) {
        // 2. switch to next cycle if the current cycle has passed
        this.cycleStartTick = now;
        this.cyclePushSize = 0;
      } else if (this.cyclePushSize > 0 &&
          (double)(this.cyclePushSize + size) >= bandwidth) {
        // 3. delay the push to next cycle if exceeds throttling bandwidth.
        //    enforcing cyclePushSize > 0 to avoid the unnecessary sleep for case
        //    where a cycle's first push size(currentSize) > bandwidth
        sleepTicks = nextCycleTick - now;
        this.cyclePushSize = 0;
      }
    }
    return sleepTicks;
  }

  /**
   * Add current size to the current cycle's total push size
   * @param size is the current size added to the current cycle's
   * total push size
   */
  public void addPushSize(final int size) {
    if (this.enabled) {
      this.cyclePushSize += size;
    }
  }

  /**
   * Reset the cycle start tick to NOW
   */
  public void resetStartTick() {
    if (this.enabled) {
      this.cycleStartTick = EnvironmentEdgeManager.currentTimeMillis();
    }
  }
}
