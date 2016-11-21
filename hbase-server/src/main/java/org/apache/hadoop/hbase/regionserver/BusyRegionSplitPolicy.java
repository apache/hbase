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

package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * This class represents a split policy which makes the split decision based
 * on how busy a region is. The metric that is used here is the fraction of
 * total write requests that are blocked due to high memstore utilization.
 * This fractional rate is calculated over a running window of
 * "hbase.busy.policy.aggWindow" milliseconds. The rate is a time-weighted
 * aggregated average of the rate in the current window and the
 * true average rate in the previous window.
 *
 */

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class BusyRegionSplitPolicy extends IncreasingToUpperBoundRegionSplitPolicy {

  private static final Log LOG = LogFactory.getLog(BusyRegionSplitPolicy.class);

  // Maximum fraction blocked write requests before region is considered for split
  private float maxBlockedRequests;
  public static final float DEFAULT_MAX_BLOCKED_REQUESTS = 0.2f;

  // Minimum age of the region in milliseconds before it is considered for split
  private long minAge = -1;
  public static final long DEFAULT_MIN_AGE_MS = 600000;  // 10 minutes

  // The window time in milliseconds over which the blocked requests rate is calculated
  private long aggregationWindow;
  public static final long DEFAULT_AGGREGATION_WINDOW = 300000;  // 5 minutes

  private HRegion region;
  private long prevTime;
  private long startTime;
  private long writeRequestCount;
  private long blockedRequestCount;
  private float blockedRate;

  @Override
  protected void configureForRegion(final HRegion region) {
    super.configureForRegion(region);
    this.region = region;
    Configuration conf = getConf();

    maxBlockedRequests = conf.getFloat("hbase.busy.policy.blockedRequests",
        DEFAULT_MAX_BLOCKED_REQUESTS);
    minAge = conf.getLong("hbase.busy.policy.minAge", DEFAULT_MIN_AGE_MS);
    aggregationWindow = conf.getLong("hbase.busy.policy.aggWindow",
        DEFAULT_AGGREGATION_WINDOW);

    if (maxBlockedRequests < 0.00001f || maxBlockedRequests > 0.99999f) {
      LOG.warn("Threshold for maximum blocked requests is set too low or too high, "
          + " resetting to default of " + DEFAULT_MAX_BLOCKED_REQUESTS);
      maxBlockedRequests = DEFAULT_MAX_BLOCKED_REQUESTS;
    }

    if (aggregationWindow <= 0) {
      LOG.warn("Aggregation window size is too low: " + aggregationWindow
          + ". Resetting it to default of " + DEFAULT_AGGREGATION_WINDOW);
      aggregationWindow = DEFAULT_AGGREGATION_WINDOW;
    }

    init();
  }

  private synchronized void init() {
    startTime = EnvironmentEdgeManager.currentTime();
    prevTime = startTime;
    blockedRequestCount = region.getBlockedRequestsCount();
    writeRequestCount = region.getWriteRequestsCount();
  }

  @Override
  protected boolean shouldSplit() {
    float blockedReqRate = updateRate();
    if (super.shouldSplit()) {
      return true;
    }

    if (EnvironmentEdgeManager.currentTime() <  startTime + minAge) {
      return false;
    }

    for (Store store: region.getStores()) {
      if (!store.canSplit()) {
        return false;
      }
    }

    if (blockedReqRate >= maxBlockedRequests) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to split region " + region.getRegionInfo().getRegionNameAsString()
            + " because it's too busy. Blocked Request rate: " + blockedReqRate);
      }
      return true;
    }

    return false;
  }

  /**
   * Update the blocked request rate based on number of blocked and total write requests in the
   * last aggregation window, or since last call to this method, whichever is farthest in time.
   * Uses weighted rate calculation based on the previous rate and new data.
   *
   * @return Updated blocked request rate.
   */
  private synchronized float updateRate() {
    float aggBlockedRate;
    long curTime = EnvironmentEdgeManager.currentTime();

    long newBlockedReqs = region.getBlockedRequestsCount();
    long newWriteReqs = region.getWriteRequestsCount();

    aggBlockedRate =
        (newBlockedReqs - blockedRequestCount) / (newWriteReqs - writeRequestCount + 0.00001f);

    if (curTime - prevTime >= aggregationWindow) {
      blockedRate = aggBlockedRate;
      prevTime = curTime;
      blockedRequestCount = newBlockedReqs;
      writeRequestCount = newWriteReqs;
    } else if (curTime - startTime >= aggregationWindow) {
      // Calculate the aggregate blocked rate as the weighted sum of
      // previous window's average blocked rate and blocked rate in this window so far.
      float timeSlice = (curTime - prevTime) / (aggregationWindow + 0.0f);
      aggBlockedRate = (1 - timeSlice) * blockedRate + timeSlice * aggBlockedRate;
    } else {
      aggBlockedRate = 0.0f;
    }
    return aggBlockedRate;
  }
}
