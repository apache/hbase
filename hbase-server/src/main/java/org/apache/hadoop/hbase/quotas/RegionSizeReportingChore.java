/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.MetricsRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Chore which sends the region size reports on this RegionServer to the Master.
 */
@InterfaceAudience.Private
public class RegionSizeReportingChore extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(RegionSizeReportingChore.class);

  static final String REGION_SIZE_REPORTING_CHORE_PERIOD_KEY =
      "hbase.regionserver.quotas.region.size.reporting.chore.period";
  static final int REGION_SIZE_REPORTING_CHORE_PERIOD_DEFAULT = 1000 * 60;

  static final String REGION_SIZE_REPORTING_CHORE_DELAY_KEY =
      "hbase.regionserver.quotas.region.size.reporting.chore.delay";
  static final long REGION_SIZE_REPORTING_CHORE_DELAY_DEFAULT = 1000 * 30;

  static final String REGION_SIZE_REPORTING_CHORE_TIMEUNIT_KEY =
      "hbase.regionserver.quotas.region.size.reporting.chore.timeunit";
  static final String REGION_SIZE_REPORTING_CHORE_TIMEUNIT_DEFAULT = TimeUnit.MILLISECONDS.name();

  private final RegionServerServices rsServices;
  private final MetricsRegionServer metrics;

  public RegionSizeReportingChore(RegionServerServices rsServices) {
    super(
        RegionSizeReportingChore.class.getSimpleName(), rsServices,
        getPeriod(rsServices.getConfiguration()), getInitialDelay(rsServices.getConfiguration()),
        getTimeUnit(rsServices.getConfiguration()));
    this.rsServices = rsServices;
    this.metrics = rsServices.getMetrics();
  }

  @Override
  protected void chore() {
    final long start = System.nanoTime();
    try {
      _chore();
    } finally {
      if (metrics != null) {
        metrics.incrementRegionSizeReportingChoreTime(
            TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS));
      }
    }
  }

  void _chore() {
    final RegionServerSpaceQuotaManager quotaManager =
        rsServices.getRegionServerSpaceQuotaManager();
    // Get the HRegionInfo for each online region
    HashSet<RegionInfo> onlineRegionInfos = getOnlineRegionInfos(rsServices.getRegions());
    RegionSizeStore store = quotaManager.getRegionSizeStore();
    // Remove all sizes for non-online regions
    removeNonOnlineRegions(store, onlineRegionInfos);
    rsServices.reportRegionSizesForQuotas(store);
  }

  HashSet<RegionInfo> getOnlineRegionInfos(List<? extends Region> onlineRegions) {
    HashSet<RegionInfo> regionInfos = new HashSet<>();
    onlineRegions.forEach((region) -> regionInfos.add(region.getRegionInfo()));
    return regionInfos;
  }

  void removeNonOnlineRegions(RegionSizeStore store, Set<RegionInfo> onlineRegions) {
    // We have to remove regions which are no longer online from the store, otherwise they will
    // continue to be sent to the Master which will prevent size report expiration.
    if (onlineRegions.isEmpty()) {
      // Easy-case, no online regions means no size reports
      store.clear();
      return;
    }

    Iterator<Entry<RegionInfo,RegionSize>> iter = store.iterator();
    int numEntriesRemoved = 0;
    while (iter.hasNext()) {
      Entry<RegionInfo,RegionSize> entry = iter.next();
      RegionInfo regionInfo = entry.getKey();
      if (!onlineRegions.contains(regionInfo)) {
        numEntriesRemoved++;
        iter.remove();
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Removed " + numEntriesRemoved + " region sizes before reporting to Master "
          + "because they are for non-online regions.");
    }
  }

  /**
   * Extracts the period for the chore from the configuration.
   *
   * @param conf The configuration object.
   * @return The configured chore period or the default value.
   */
  static int getPeriod(Configuration conf) {
    return conf.getInt(
        REGION_SIZE_REPORTING_CHORE_PERIOD_KEY, REGION_SIZE_REPORTING_CHORE_PERIOD_DEFAULT);
  }

  /**
   * Extracts the initial delay for the chore from the configuration.
   *
   * @param conf The configuration object.
   * @return The configured chore initial delay or the default value.
   */
  static long getInitialDelay(Configuration conf) {
    return conf.getLong(
        REGION_SIZE_REPORTING_CHORE_DELAY_KEY, REGION_SIZE_REPORTING_CHORE_DELAY_DEFAULT);
  }

  /**
   * Extracts the time unit for the chore period and initial delay from the configuration. The
   * configuration value for {@link #REGION_SIZE_REPORTING_CHORE_TIMEUNIT_KEY} must correspond to a
   * {@link TimeUnit} value.
   *
   * @param conf The configuration object.
   * @return The configured time unit for the chore period and initial delay or the default value.
   */
  static TimeUnit getTimeUnit(Configuration conf) {
    return TimeUnit.valueOf(conf.get(REGION_SIZE_REPORTING_CHORE_TIMEUNIT_KEY,
        REGION_SIZE_REPORTING_CHORE_TIMEUNIT_DEFAULT));
  }
}
