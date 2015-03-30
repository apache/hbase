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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils;

/**
 * A chore for refreshing the store files for secondary regions hosted in the region server.
 *
 * This chore should run periodically with a shorter interval than HFile TTL
 * ("hbase.master.hfilecleaner.ttl", default 5 minutes).
 * It ensures that if we cannot refresh files longer than that amount, the region
 * will stop serving read requests because the referenced files might have been deleted (by the
 * primary region).
 */
@InterfaceAudience.Private
public class StorefileRefresherChore extends ScheduledChore {

  private static final Log LOG = LogFactory.getLog(StorefileRefresherChore.class);

  /**
   * The period (in milliseconds) for refreshing the store files for the secondary regions.
   */
  public static final String REGIONSERVER_STOREFILE_REFRESH_PERIOD
    = "hbase.regionserver.storefile.refresh.period";
  static final int DEFAULT_REGIONSERVER_STOREFILE_REFRESH_PERIOD = 0; //disabled by default

  /**
   * Whether all storefiles should be refreshed, as opposed to just hbase:meta's
   * Meta region doesn't have WAL replication for replicas enabled yet
   */
  public static final String REGIONSERVER_META_STOREFILE_REFRESH_PERIOD
     = "hbase.regionserver.meta.storefile.refresh.period";
  private HRegionServer regionServer;
  private long hfileTtl;
  private int period;
  private boolean onlyMetaRefresh = true;

  //ts of last time regions store files are refreshed
  private Map<String, Long> lastRefreshTimes; // encodedName -> long

  public StorefileRefresherChore(int period, boolean onlyMetaRefresh, HRegionServer regionServer,
      Stoppable stoppable) {
    super("StorefileRefresherChore", stoppable, period);
    this.period = period;
    this.regionServer = regionServer;
    this.hfileTtl = this.regionServer.getConfiguration().getLong(
      TimeToLiveHFileCleaner.TTL_CONF_KEY, TimeToLiveHFileCleaner.DEFAULT_TTL);
    this.onlyMetaRefresh = onlyMetaRefresh;
    if (period > hfileTtl / 2) {
      throw new RuntimeException(REGIONSERVER_STOREFILE_REFRESH_PERIOD +
        " should be set smaller than half of " + TimeToLiveHFileCleaner.TTL_CONF_KEY);
    }
    lastRefreshTimes = new HashMap<String, Long>();
  }

  @Override
  protected void chore() {
    for (Region r : regionServer.getOnlineRegionsLocalContext()) {
      if (!r.isReadOnly()) {
        // skip checking for this region if it can accept writes
        continue;
      }
      // don't refresh unless enabled for all files, or it the meta region
      // meta region don't have WAL replication for replicas enabled yet
      if (onlyMetaRefresh && !r.getRegionInfo().isMetaTable()) continue;
      String encodedName = r.getRegionInfo().getEncodedName();
      long time = EnvironmentEdgeManager.currentTime();
      if (!lastRefreshTimes.containsKey(encodedName)) {
        lastRefreshTimes.put(encodedName, time);
      }
      try {
        for (Store store : r.getStores()) {
          // TODO: some stores might see new data from flush, while others do not which
          // MIGHT break atomic edits across column families. We can fix this with setting
          // mvcc read numbers that we know every store has seen
          store.refreshStoreFiles();
        }
      } catch (IOException ex) {
        LOG.warn("Exception while trying to refresh store files for region:" + r.getRegionInfo()
          + ", exception:" + StringUtils.stringifyException(ex));

        // Store files have a TTL in the archive directory. If we fail to refresh for that long, we stop serving reads
        if (isRegionStale(encodedName, time)) {
          ((HRegion)r).setReadsEnabled(false); // stop serving reads
        }
        continue;
      }
      lastRefreshTimes.put(encodedName, time);
      ((HRegion)r).setReadsEnabled(true); // restart serving reads
    }

    // remove closed regions
    Iterator<String> lastRefreshTimesIter = lastRefreshTimes.keySet().iterator();
    while (lastRefreshTimesIter.hasNext()) {
      String encodedName = lastRefreshTimesIter.next();
      if (regionServer.getFromOnlineRegions(encodedName) == null) {
        lastRefreshTimesIter.remove();
      }
    }
  }

  protected boolean isRegionStale(String encodedName, long time) {
    long lastRefreshTime = lastRefreshTimes.get(encodedName);
    return time - lastRefreshTime > hfileTtl - period;
  }
}
