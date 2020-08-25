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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.client.ConnectionUtils.locateRow;
import static org.apache.hadoop.hbase.client.ConnectionUtils.locateRowBefore;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaCellComparator;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocateType;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * A cache of meta region locations.
 */
@InterfaceAudience.Private
class MetaLocationCache implements Stoppable {

  private static final Logger LOG = LoggerFactory.getLogger(MetaLocationCache.class);

  @VisibleForTesting
  static final String SYNC_INTERVAL_SECONDS =
    "hbase.master.meta-location-cache.sync-interval-seconds";

  // default sync every 1 second.
  @VisibleForTesting
  static final int DEFAULT_SYNC_INTERVAL_SECONDS = 1;

  private static final String FETCH_TIMEOUT_MS =
    "hbase.master.meta-location-cache.fetch-timeout-ms";

  // default timeout 1 second
  private static final int DEFAULT_FETCH_TIMEOUT_MS = 1000;

  private static final class CacheHolder {

    final NavigableMap<byte[], RegionLocations> cache;

    final List<HRegionLocation> all;

    CacheHolder(List<HRegionLocation> all) {
      this.all = Collections.unmodifiableList(all);
      NavigableMap<byte[], SortedSet<HRegionLocation>> startKeyToLocs =
        new TreeMap<>(MetaCellComparator.ROW_COMPARATOR);
      for (HRegionLocation loc : all) {
        if (loc.getRegion().isSplitParent()) {
          continue;
        }
        startKeyToLocs.computeIfAbsent(loc.getRegion().getStartKey(),
          k -> new TreeSet<>((l1, l2) -> l1.getRegion().compareTo(l2.getRegion()))).add(loc);
      }
      this.cache = startKeyToLocs.entrySet().stream().collect(Collectors.collectingAndThen(
        Collectors.toMap(Map.Entry::getKey, e -> new RegionLocations(e.getValue()), (u, v) -> {
          throw new IllegalStateException();
        }, () -> new TreeMap<>(MetaCellComparator.ROW_COMPARATOR)),
        Collections::unmodifiableNavigableMap));
    }
  }

  private volatile CacheHolder holder;

  private volatile boolean stopped = false;

  MetaLocationCache(MasterServices master) {
    int syncIntervalSeconds =
      master.getConfiguration().getInt(SYNC_INTERVAL_SECONDS, DEFAULT_SYNC_INTERVAL_SECONDS);
    int fetchTimeoutMs =
      master.getConfiguration().getInt(FETCH_TIMEOUT_MS, DEFAULT_FETCH_TIMEOUT_MS);
    master.getChoreService().scheduleChore(new ScheduledChore(
      getClass().getSimpleName() + "-Sync-Chore", this, syncIntervalSeconds, 0, TimeUnit.SECONDS) {

      @Override
      protected void chore() {
        AsyncClusterConnection conn = master.getAsyncClusterConnection();
        if (conn != null) {
          addListener(conn.getAllMetaRegionLocations(fetchTimeoutMs), (locs, error) -> {
            if (error != null) {
              LOG.warn("Failed to fetch all meta region locations from active master", error);
              return;
            }
            holder = new CacheHolder(locs);
          });
        }
      }
    });
  }

  RegionLocations locateMeta(byte[] row, RegionLocateType locateType) {
    if (locateType == RegionLocateType.AFTER) {
      // as we know the exact row after us, so we can just create the new row, and use the same
      // algorithm to locate it.
      row = Arrays.copyOf(row, row.length + 1);
      locateType = RegionLocateType.CURRENT;
    }
    CacheHolder holder = this.holder;
    if (holder == null) {
      return null;
    }
    return locateType.equals(RegionLocateType.BEFORE) ?
      locateRowBefore(holder.cache, TableName.META_TABLE_NAME, row, RegionInfo.DEFAULT_REPLICA_ID) :
      locateRow(holder.cache, TableName.META_TABLE_NAME, row, RegionInfo.DEFAULT_REPLICA_ID);
  }

  List<HRegionLocation> getAllMetaRegionLocations(boolean excludeOfflinedSplitParents) {
    CacheHolder holder = this.holder;
    if (holder == null) {
      return Collections.emptyList();
    }
    if (!excludeOfflinedSplitParents) {
      // just return all the locations
      return holder.all;
    } else {
      return holder.all.stream().filter(l -> !l.getRegion().isSplitParent())
        .collect(Collectors.toList());
    }
  }

  @Override
  public void stop(String why) {
    LOG.info("Stopping meta location cache: {}", why);
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }
}
