/*
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
import static org.apache.hadoop.hbase.client.AsyncRegionLocatorHelper.removeRegionLocation;
import static org.apache.hadoop.hbase.client.ConnectionUtils.isEmptyStopRow;
import static org.apache.hadoop.hbase.util.Bytes.BYTES_COMPARATOR;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Objects;

/**
 * Cache of RegionLocations for use by {@link AsyncNonMetaRegionLocator}. Wrapper around
 * ConcurrentSkipListMap ensuring proper access to cached items. Updates are synchronized, but reads
 * are not.
 */
final class AsyncRegionLocationCache {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncRegionLocationCache.class);

  private final ConcurrentNavigableMap<byte[], RegionLocations> cache =
    new ConcurrentSkipListMap<>(BYTES_COMPARATOR);
  private final TableName tableName;

  public AsyncRegionLocationCache(TableName tableName) {
    this.tableName = tableName;
  }

  /**
   * Add the given locations to the cache, merging with existing if necessary. Also cleans out any
   * previously cached locations which may have been superseded by this one (i.e. in case of merged
   * regions). See {@link #cleanProblematicOverlappedRegions(RegionLocations)}
   * @param locs the locations to cache
   * @return the final location (possibly merged) that was added to the cache
   */
  public synchronized RegionLocations add(RegionLocations locs) {
    byte[] startKey = locs.getRegionLocation().getRegion().getStartKey();
    RegionLocations oldLocs = cache.putIfAbsent(startKey, locs);
    if (oldLocs == null) {
      cleanProblematicOverlappedRegions(locs);
      return locs;
    }

    // check whether the regions are the same, this usually happens when table is split/merged,
    // or deleted and recreated again.
    RegionInfo region = locs.getRegionLocation().getRegion();
    RegionInfo oldRegion = oldLocs.getRegionLocation().getRegion();
    if (region.getEncodedName().equals(oldRegion.getEncodedName())) {
      RegionLocations mergedLocs = oldLocs.mergeLocations(locs);
      if (isEqual(mergedLocs, oldLocs)) {
        // the merged one is the same with the old one, give up
        LOG.trace("Will not add {} to cache because the old value {} "
          + " is newer than us or has the same server name."
          + " Maybe it is updated before we replace it", locs, oldLocs);
        return oldLocs;
      }
      locs = mergedLocs;
    } else {
      // the region is different, here we trust the one we fetched. This maybe wrong but finally
      // the upper layer can detect this and trigger removal of the wrong locations
      if (LOG.isDebugEnabled()) {
        LOG.debug("The newly fetch region {} is different from the old one {} for row '{}',"
          + " try replaying the old one...", region, oldRegion, Bytes.toStringBinary(startKey));
      }
    }

    cache.put(startKey, locs);
    cleanProblematicOverlappedRegions(locs);
    return locs;
  }

  /**
   * When caching a location, the region may have been the result of a merge. Check to see if the
   * region's boundaries overlap any other cached locations in a problematic way. Those would have
   * been merge parents which no longer exist. We need to proactively clear them out to avoid a case
   * where a merged region which receives no requests never gets cleared. This causes requests to
   * other merged regions after it to see the wrong cached location.
   * <p>
   * For example, if we have Start_New < Start_Old < End_Old < End_New, then if we only access
   * within range [End_Old, End_New], then it will always return the old region but it will then
   * find out the row is not in the range, and try to get the new region, and then we get
   * [Start_New, End_New), still fall into the same situation.
   * <p>
   * If Start_Old is less than Start_New, even if we have overlap, it is not a problem, as when the
   * row is greater than Start_New, we will locate to the new region, and if the row is less than
   * Start_New, it will fall into the old region's range and we will try to access the region and
   * get a NotServing exception, and then we will clean the cache.
   * <p>
   * See HBASE-27650
   * @param locations the new location that was just cached
   */
  private void cleanProblematicOverlappedRegions(RegionLocations locations) {
    RegionInfo region = locations.getRegionLocation().getRegion();

    boolean isLast = isEmptyStopRow(region.getEndKey());

    while (true) {
      Map.Entry<byte[], RegionLocations> overlap =
        isLast ? cache.lastEntry() : cache.lowerEntry(region.getEndKey());
      if (
        overlap == null || overlap.getValue() == locations
          || Bytes.equals(overlap.getKey(), region.getStartKey())
      ) {
        break;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "Removing cached location {} (endKey={}) because it overlaps with "
            + "new location {} (endKey={})",
          overlap.getValue(),
          Bytes.toStringBinary(overlap.getValue().getRegionLocation().getRegion().getEndKey()),
          locations, Bytes.toStringBinary(locations.getRegionLocation().getRegion().getEndKey()));
      }

      cache.remove(overlap.getKey());
    }
  }

  private boolean isEqual(RegionLocations locs1, RegionLocations locs2) {
    HRegionLocation[] locArr1 = locs1.getRegionLocations();
    HRegionLocation[] locArr2 = locs2.getRegionLocations();
    if (locArr1.length != locArr2.length) {
      return false;
    }
    for (int i = 0; i < locArr1.length; i++) {
      // do not need to compare region info
      HRegionLocation loc1 = locArr1[i];
      HRegionLocation loc2 = locArr2[i];
      if (loc1 == null) {
        if (loc2 != null) {
          return false;
        }
      } else {
        if (loc2 == null) {
          return false;
        }
        if (loc1.getSeqNum() != loc2.getSeqNum()) {
          return false;
        }
        if (!Objects.equal(loc1.getServerName(), loc2.getServerName())) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Returns all cached RegionLocations
   */
  public Collection<RegionLocations> getAll() {
    return Collections.unmodifiableCollection(cache.values());
  }

  /**
   * Gets the RegionLocations for a given region's startKey. This is a direct lookup, if the key
   * does not exist in the cache it will return null.
   * @param startKey region start key to directly look up
   */
  public RegionLocations get(byte[] startKey) {
    return cache.get(startKey);
  }

  /**
   * Finds the RegionLocations for the region with the greatest startKey less than or equal to the
   * given row
   * @param row row to find locations
   */
  public RegionLocations findForRow(byte[] row, int replicaId) {
    Map.Entry<byte[], RegionLocations> entry = cache.floorEntry(row);
    if (entry == null) {
      return null;
    }
    RegionLocations locs = entry.getValue();
    if (locs == null) {
      return null;
    }
    HRegionLocation loc = locs.getRegionLocation(replicaId);
    if (loc == null) {
      return null;
    }
    byte[] endKey = loc.getRegion().getEndKey();
    if (isEmptyStopRow(endKey) || Bytes.compareTo(row, endKey) < 0) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Found {} in cache for {}, row='{}', locateType={}, replicaId={}", loc, tableName,
          Bytes.toStringBinary(row), RegionLocateType.CURRENT, replicaId);
      }
      return locs;
    } else {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Requested row {} comes after region end key of {} for cached location {}",
          Bytes.toStringBinary(row), Bytes.toStringBinary(endKey), locs);
      }
      return null;
    }
  }

  /**
   * Finds the RegionLocations for the region with the greatest startKey strictly less than the
   * given row
   * @param row row to find locations
   */
  public RegionLocations findForBeforeRow(byte[] row, int replicaId) {
    boolean isEmptyStopRow = isEmptyStopRow(row);
    Map.Entry<byte[], RegionLocations> entry =
      isEmptyStopRow ? cache.lastEntry() : cache.lowerEntry(row);
    if (entry == null) {
      return null;
    }
    RegionLocations locs = entry.getValue();
    if (locs == null) {
      return null;
    }
    HRegionLocation loc = locs.getRegionLocation(replicaId);
    if (loc == null) {
      return null;
    }
    if (
      isEmptyStopRow(loc.getRegion().getEndKey())
        || (!isEmptyStopRow && Bytes.compareTo(loc.getRegion().getEndKey(), row) >= 0)
    ) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Found {} in cache for {}, row='{}', locateType={}, replicaId={}", loc, tableName,
          Bytes.toStringBinary(row), RegionLocateType.BEFORE, replicaId);
      }
      return locs;
    } else {
      return null;
    }
  }

  /**
   * Removes the location from the cache if it exists and can be removed.
   * @return true if entry was removed
   */
  public synchronized boolean remove(HRegionLocation loc) {
    byte[] startKey = loc.getRegion().getStartKey();
    RegionLocations oldLocs = cache.get(startKey);
    if (oldLocs == null) {
      return false;
    }

    HRegionLocation oldLoc = oldLocs.getRegionLocation(loc.getRegion().getReplicaId());
    if (!canUpdateOnError(loc, oldLoc)) {
      return false;
    }

    RegionLocations newLocs = removeRegionLocation(oldLocs, loc.getRegion().getReplicaId());
    if (newLocs == null) {
      if (cache.remove(startKey, oldLocs)) {
        return true;
      }
    } else {
      cache.put(startKey, newLocs);
      return true;
    }
    return false;
  }

  /**
   * Returns the size of the region locations cache
   */
  public int size() {
    return cache.size();
  }

  /**
   * Removes serverName from all locations in the cache, fully removing any RegionLocations which
   * are empty after removing the server from it.
   * @param serverName server to remove from locations
   */
  public synchronized boolean removeForServer(ServerName serverName) {
    boolean removed = false;
    for (Map.Entry<byte[], RegionLocations> entry : cache.entrySet()) {
      byte[] regionName = entry.getKey();
      RegionLocations locs = entry.getValue();
      RegionLocations newLocs = locs.removeByServer(serverName);
      if (locs == newLocs) {
        continue;
      }
      if (newLocs.isEmpty()) {
        removed |= cache.remove(regionName, locs);
      } else {
        cache.put(regionName, newLocs);
      }
    }
    return removed;
  }
}
