/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

abstract class RegionServerOperation implements Delayed {
  protected static final Log LOG =
    LogFactory.getLog(RegionServerOperation.class.getName());

  public static enum RegionServerOperationResult {
    /** this operation is completed successfully. */
    OPERATION_SUCCEEDED,
    /** this operation has been put back to the delay queue. */
    OPERATION_DELAYED,
    /** this operation is failed. */
    OPERATION_FAILED
  }

  private long expire;
  protected final HMaster master;
  protected final String serverName; // server sent the message or task
  /* How long we stay on queue.
   */
  private int delay;

  protected RegionServerOperation(HMaster master, String serverName) {
    this.master = master;
    this.serverName = serverName;
    this.delay = this.master.getConfiguration().
      getInt("hbase.server.thread.wakefrequency", 10 * 1000);
    // Set the future time at which we expect to be released from the
    // DelayQueue we're inserted in on lease expiration.
    resetExpiration();
  }

  /**
   * Call before putting this back on the delay queue.
   * @return When we will expire next.
   */
  long resetExpiration() {
    // Set the future time at which we expect to be released from the
    // DelayQueue we're inserted in on lease expiration.
    this.expire = System.currentTimeMillis() + this.delay;
    return this.expire;
  }

  public long getDelay(TimeUnit unit) {
    return unit.convert(this.expire - System.currentTimeMillis(),
      TimeUnit.MILLISECONDS);
  }

  void setDelay(final int d) {
    this.delay = d;
  }

  public int compareTo(Delayed o) {
    return Long.valueOf(getDelay(TimeUnit.MILLISECONDS)
        - o.getDelay(TimeUnit.MILLISECONDS)).intValue();
  }

  protected boolean rootAvailable() {
    return master.getRegionManager().getRootRegionLocation() != null;
  }

  protected boolean metaTableAvailable() {
    boolean available = true;
    int numMetaRegions = master.getRegionManager().numMetaRegions();
    if (numMetaRegions == 0 ||
        numMetaRegions > master.getRegionManager().numOnlineMetaRegions() ||
        master.getRegionManager().metaRegionsInTransition()) {
      // We can't proceed because not all of the meta regions are online.
      // We can't block either because that would prevent the meta region
      // online message from being processed. In order to prevent spinning
      // in the run queue, put this request on the delay queue to give
      // other threads the opportunity to get the meta regions on-line.
      if (LOG.isDebugEnabled()) {
        LOG.debug("numberOfMetaRegions: " +
            master.getRegionManager().numMetaRegions() +
            ", onlineMetaRegions.size(): " +
            master.getRegionManager().numOnlineMetaRegions());
        LOG.debug("Requeuing because not all meta regions are online");
      }
      available = false;
    }
    return available;
  }

  public int compareTo(RegionServerOperation other) {
    return getPriority() - other.getPriority();
  }

  // the Priority of this operation, 0 is lowest priority
  protected int getPriority() {
    return Integer.MAX_VALUE;
  }

  protected abstract RegionServerOperationResult process() throws IOException;
}
