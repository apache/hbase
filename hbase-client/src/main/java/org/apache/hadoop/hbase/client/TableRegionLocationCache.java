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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.client.AsyncRegionLocatorHelper.canUpdateOnError;
import static org.apache.hadoop.hbase.client.AsyncRegionLocatorHelper.isEqual;
import static org.apache.hadoop.hbase.client.AsyncRegionLocatorHelper.removeRegionLocation;
import static org.apache.hadoop.hbase.client.ConnectionUtils.isEmptyStopRow;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * The location cache for regions of a table.
 */
@InterfaceAudience.Private
class TableRegionLocationCache {

  private static final Logger LOG = LoggerFactory.getLogger(TableRegionLocationCache.class);

  private final Optional<MetricsConnection> metrics;

  private final ConcurrentNavigableMap<byte[], RegionLocations> cache;

  TableRegionLocationCache(Comparator<byte[]> comparator, Optional<MetricsConnection> metrics) {
    this.metrics = metrics;
    this.cache = new ConcurrentSkipListMap<>(comparator);
  }

  private void recordCacheHit() {
    metrics.ifPresent(MetricsConnection::incrMetaCacheHit);
  }

  private void recordCacheMiss() {
    metrics.ifPresent(MetricsConnection::incrMetaCacheMiss);
  }

  private void recordClearRegionCache() {
    metrics.ifPresent(MetricsConnection::incrMetaCacheNumClearRegion);
  }

  private RegionLocations locateRow(TableName tableName, byte[] row, int replicaId) {
    Map.Entry<byte[], RegionLocations> entry = cache.floorEntry(row);
    if (entry == null) {
      recordCacheMiss();
      return null;
    }
    RegionLocations locs = entry.getValue();
    HRegionLocation loc = locs.getRegionLocation(replicaId);
    if (loc == null) {
      recordCacheMiss();
      return null;
    }
    byte[] endKey = loc.getRegion().getEndKey();
    if (isEmptyStopRow(endKey) || Bytes.compareTo(row, endKey) < 0) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Found {} in cache for {}, row='{}', locateType={}, replicaId={}", loc, tableName,
          Bytes.toStringBinary(row), RegionLocateType.CURRENT, replicaId);
      }
      recordCacheHit();
      return locs;
    } else {
      recordCacheMiss();
      return null;
    }
  }

  private RegionLocations locateRowBefore(TableName tableName, byte[] row, int replicaId) {
    boolean isEmptyStopRow = isEmptyStopRow(row);
    Map.Entry<byte[], RegionLocations> entry =
      isEmptyStopRow ? cache.lastEntry() : cache.lowerEntry(row);
    if (entry == null) {
      recordCacheMiss();
      return null;
    }
    RegionLocations locs = entry.getValue();
    HRegionLocation loc = locs.getRegionLocation(replicaId);
    if (loc == null) {
      recordCacheMiss();
      return null;
    }
    if (isEmptyStopRow(loc.getRegion().getEndKey()) ||
      (!isEmptyStopRow && Bytes.compareTo(loc.getRegion().getEndKey(), row) >= 0)) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Found {} in cache for {}, row='{}', locateType={}, replicaId={}", loc, tableName,
          Bytes.toStringBinary(row), RegionLocateType.BEFORE, replicaId);
      }
      recordCacheHit();
      return locs;
    } else {
      recordCacheMiss();
      return null;
    }
  }

  RegionLocations locate(TableName tableName, byte[] row, int replicaId,
    RegionLocateType locateType) {
    return locateType.equals(RegionLocateType.BEFORE) ? locateRowBefore(tableName, row, replicaId) :
      locateRow(tableName, row, replicaId);
  }

  // if we successfully add the locations to cache, return the locations, otherwise return the one
  // which prevents us being added. The upper layer can use this value to complete pending requests.
  RegionLocations add(RegionLocations locs) {
    LOG.trace("Try adding {} to cache", locs);
    byte[] startKey = locs.getRegionLocation().getRegion().getStartKey();
    for (;;) {
      RegionLocations oldLocs = cache.putIfAbsent(startKey, locs);
      if (oldLocs == null) {
        return locs;
      }
      // check whether the regions are the same, this usually happens when table is split/merged, or
      // deleted and recreated again.
      RegionInfo region = locs.getRegionLocation().getRegion();
      RegionInfo oldRegion = oldLocs.getRegionLocation().getRegion();
      if (region.getEncodedName().equals(oldRegion.getEncodedName())) {
        RegionLocations mergedLocs = oldLocs.mergeLocations(locs);
        if (isEqual(mergedLocs, oldLocs)) {
          // the merged one is the same with the old one, give up
          LOG.trace("Will not add {} to cache because the old value {} " +
            " is newer than us or has the same server name." +
            " Maybe it is updated before we replace it", locs, oldLocs);
          return oldLocs;
        }
        if (cache.replace(startKey, oldLocs, mergedLocs)) {
          return mergedLocs;
        }
      } else {
        // the region is different, here we trust the one we fetched. This maybe wrong but finally
        // the upper layer can detect this and trigger removal of the wrong locations
        if (LOG.isDebugEnabled()) {
          LOG.debug("The newnly fetch region {} is different from the old one {} for row '{}'," +
            " try replaing the old one...", region, oldRegion, Bytes.toStringBinary(startKey));
        }
        if (cache.replace(startKey, oldLocs, locs)) {
          return locs;
        }
      }
    }
  }

  // notice that this is not a constant time operation, do not call it on critical path.
  int size() {
    return cache.size();
  }

  void clearCache(ServerName serverName) {
    for (Map.Entry<byte[], RegionLocations> entry : cache.entrySet()) {
      byte[] regionName = entry.getKey();
      RegionLocations locs = entry.getValue();
      RegionLocations newLocs = locs.removeByServer(serverName);
      if (locs == newLocs) {
        continue;
      }
      if (newLocs.isEmpty()) {
        cache.remove(regionName, locs);
      } else {
        cache.replace(regionName, locs, newLocs);
      }
    }
  }

  void removeLocationFromCache(HRegionLocation loc) {
    byte[] startKey = loc.getRegion().getStartKey();
    for (;;) {
      RegionLocations oldLocs = cache.get(startKey);
      if (oldLocs == null) {
        return;
      }
      HRegionLocation oldLoc = oldLocs.getRegionLocation(loc.getRegion().getReplicaId());
      if (!canUpdateOnError(loc, oldLoc)) {
        return;
      }
      RegionLocations newLocs = removeRegionLocation(oldLocs, loc.getRegion().getReplicaId());
      if (newLocs == null) {
        if (cache.remove(startKey, oldLocs)) {
          recordClearRegionCache();
          return;
        }
      } else {
        if (cache.replace(startKey, oldLocs, newLocs)) {
          recordClearRegionCache();
          return;
        }
      }
    }
  }

  RegionLocations get(byte[] key) {
    return cache.get(key);
  }

  // only used for testing whether we have cached the location for a table.
  @VisibleForTesting
  int getNumberOfCachedRegionLocations() {
    return cache.values().stream().mapToInt(RegionLocations::numNonNullElements).sum();
  }
}
