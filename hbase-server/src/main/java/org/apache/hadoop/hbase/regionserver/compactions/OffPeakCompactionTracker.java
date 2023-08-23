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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class used to track off-peak compactions. Off-peak compaction counter
 * is global for the entire server.
 */
@InterfaceAudience.Private
public class OffPeakCompactionTracker implements ConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(OffPeakCompactionTracker.class);

  public static final String HBASE_OFFPEAK_COMPACTION_MAX_SIZE_KEY = "hbase.offpeak.compaction.max.size";

  /**
   * Max number of off peak compactions.
   * Please lock compactionCountLock before modifying.
   */
  private volatile static int maxCompactionSize = 1;

  /**
   * Number of off peak compactions either in the compaction queue or
   * happening now. Please lock compactionCountLock before modifying.
   */
  private static int numOutstanding = 0;

  /**
   * Lock object for maxCompactionSize
   */
  private static final Object compactionCountLock = new Object();

  private static final AtomicBoolean registered = new AtomicBoolean(false);

  public OffPeakCompactionTracker() {
  }

  public boolean isUnRegistered() {
    return registered.compareAndSet(false, true);
  }

  /**
   * Tries making the compaction off-peak.
   * @return Whether the compaction can be made off-peak.
   */
  public boolean tryStartOffPeakRequest() {
    synchronized(compactionCountLock) {
      if (numOutstanding < maxCompactionSize) {
        numOutstanding++;
        return true;
      }
    }
    return false;
  }

  /**
   * The current compaction finished, so reset the off peak compactions count
   * if this was an off peak compaction.
   */
  public void endOffPeakRequest() {
    int newValueToLog = -1;
    synchronized (compactionCountLock) {
      newValueToLog = --numOutstanding;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Compaction done, numOutstandingOffPeakCompactions is now {}" , newValueToLog);
    }
  }


  public void setMaxCompactionSize(Configuration conf) {
    setMaxCompactionSize(conf.getInt(HBASE_OFFPEAK_COMPACTION_MAX_SIZE_KEY, -1));
  }

  private boolean setMaxCompactionSize(int targetCompactionSize) {

    if (targetCompactionSize < 1) {
      LOG.info("Off-peak compaction max size must >= 1, current set {}", targetCompactionSize);
      return false;
    }

    synchronized (compactionCountLock) {
      this.maxCompactionSize = targetCompactionSize;
    }
    return true;
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    int targetCompactionSize = conf.getInt(HBASE_OFFPEAK_COMPACTION_MAX_SIZE_KEY, -1);
    if (setMaxCompactionSize(targetCompactionSize)) {
      LOG.info("Update {} from {} to {}", HBASE_OFFPEAK_COMPACTION_MAX_SIZE_KEY, maxCompactionSize, targetCompactionSize);
    }
  }
}
