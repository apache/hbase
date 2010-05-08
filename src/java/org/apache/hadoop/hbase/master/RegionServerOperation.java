/**
 * Copyright 2008 The Apache Software Foundation
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

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;

abstract class RegionServerOperation implements Delayed, HConstants {
  protected static final Log LOG =
    LogFactory.getLog(RegionServerOperation.class.getName());

  private long expire;
  protected final HMaster master;
  protected final int numRetries;
  /* How long we stay on queue.
   */
  private int expirationDuration;

  protected RegionServerOperation(HMaster master) {
    this.master = master;
    this.numRetries = master.numRetries;
    // this.master.leaseTimeout is 120 by default.  120/2, which is what it
    // used to be, is a long time to wait on something coming around again.
    // Set a max of 10 seconds to wait.
    this.expirationDuration = Math.min(this.master.leaseTimeout/10, 10);
    resetExpiration();
  }

  public long getDelay(TimeUnit unit) {
    return unit.convert(this.expire - System.currentTimeMillis(),
      TimeUnit.MILLISECONDS);
  }

  public int compareTo(Delayed o) {
    return Long.valueOf(getDelay(TimeUnit.MILLISECONDS) -
      o.getDelay(TimeUnit.MILLISECONDS)).intValue();
  }

  protected void requeue() {
    this.master.getRegionServerOperationQueue().putOnDelayQueue(this);
  }

  /**
   * Call before putting this back on the delay queue.
   * @return When we will expire next.
   */
  long resetExpiration() {
    // Set the future time at which we expect to be released from the
    // DelayQueue we're inserted in on lease expiration.
    this.expire = System.currentTimeMillis() + getExpirationDuration();
    return this.expire;
  }

  int getExpirationDuration() {
    return this.expirationDuration;
  }

  void setExpirationDuration(final int d) {
    this.expirationDuration = d;
  }

  protected boolean rootAvailable() {
    boolean available = true;
    if (master.getRootRegionLocation() == null) {
      available = false;
      requeue();
    }
    return available;
  }

  protected boolean metaTableAvailable() {
    boolean available = true;
    if ((master.regionManager.numMetaRegions() !=
      master.regionManager.numOnlineMetaRegions()) ||
      master.regionManager.metaRegionsInTransition()) {
      // We can't proceed because not all of the meta regions are online.
      // We can't block either because that would prevent the meta region
      // online message from being processed. In order to prevent spinning
      // in the run queue, put this request on the delay queue to give
      // other threads the opportunity to get the meta regions on-line.
      if (LOG.isDebugEnabled()) {
        LOG.debug("numberOfMetaRegions: " +
            master.regionManager.numMetaRegions() +
            ", onlineMetaRegions.size(): " +
            master.regionManager.numOnlineMetaRegions());
        LOG.debug("Requeuing because not all meta regions are online");
      }
      available = false;
      requeue();
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

  protected abstract boolean process() throws IOException;
}