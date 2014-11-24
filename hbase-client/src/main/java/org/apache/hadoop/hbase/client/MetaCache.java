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

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A cache implementation for region locations from meta.
 */
@InterfaceAudience.Private
public class MetaCache {

  private static final Log LOG = LogFactory.getLog(MetaCache.class);

  /**
   * Map of table to table {@link HRegionLocation}s.
   */
  private final ConcurrentMap<TableName, ConcurrentSkipListMap<byte[], RegionLocations>>
  cachedRegionLocations =
  new ConcurrentHashMap<TableName, ConcurrentSkipListMap<byte[], RegionLocations>>();

  // The presence of a server in the map implies it's likely that there is an
  // entry in cachedRegionLocations that map to this server; but the absence
  // of a server in this map guarentees that there is no entry in cache that
  // maps to the absent server.
  // The access to this attribute must be protected by a lock on cachedRegionLocations
  private final Set<ServerName> cachedServers = new ConcurrentSkipListSet<ServerName>();

  /**
   * Search the cache for a location that fits our table and row key.
   * Return null if no suitable region is located.
   *
   *
   * @param tableName
   * @param row
   * @return Null or region location found in cache.
   */
  public RegionLocations getCachedLocation(final TableName tableName, final byte [] row) {
    ConcurrentSkipListMap<byte[], RegionLocations> tableLocations =
      getTableLocations(tableName);

    Entry<byte[], RegionLocations> e = tableLocations.floorEntry(row);
    if (e == null) {
      return null;
    }
    RegionLocations possibleRegion = e.getValue();

    // make sure that the end key is greater than the row we're looking
    // for, otherwise the row actually belongs in the next region, not
    // this one. the exception case is when the endkey is
    // HConstants.EMPTY_END_ROW, signifying that the region we're
    // checking is actually the last region in the table.
    byte[] endKey = possibleRegion.getRegionLocation().getRegionInfo().getEndKey();
    if (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) ||
        tableName.getRowComparator().compareRows(
            endKey, 0, endKey.length, row, 0, row.length) > 0) {
      return possibleRegion;
    }

    // Passed all the way through, so we got nothing - complete cache miss
    return null;
  }

  /**
   * Put a newly discovered HRegionLocation into the cache.
   * @param tableName The table name.
   * @param source the source of the new location
   * @param location the new location
   */
  public void cacheLocation(final TableName tableName, final ServerName source,
      final HRegionLocation location) {
    assert source != null;
    byte [] startKey = location.getRegionInfo().getStartKey();
    ConcurrentMap<byte[], RegionLocations> tableLocations = getTableLocations(tableName);
    RegionLocations locations = new RegionLocations(new HRegionLocation[] {location}) ;
    RegionLocations oldLocations = tableLocations.putIfAbsent(startKey, locations);
    boolean isNewCacheEntry = (oldLocations == null);
    if (isNewCacheEntry) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Cached location: " + location);
      }
      addToCachedServers(locations);
      return;
    }

    // If the server in cache sends us a redirect, assume it's always valid.
    HRegionLocation oldLocation = oldLocations.getRegionLocation(
      location.getRegionInfo().getReplicaId());
    boolean force = oldLocation != null && oldLocation.getServerName() != null
        && oldLocation.getServerName().equals(source);

    // For redirect if the number is equal to previous
    // record, the most common case is that first the region was closed with seqNum, and then
    // opened with the same seqNum; hence we will ignore the redirect.
    // There are so many corner cases with various combinations of opens and closes that
    // an additional counter on top of seqNum would be necessary to handle them all.
    RegionLocations updatedLocations = oldLocations.updateLocation(location, false, force);
    if (oldLocations != updatedLocations) {
      boolean replaced = tableLocations.replace(startKey, oldLocations, updatedLocations);
      if (replaced && LOG.isTraceEnabled()) {
        LOG.trace("Changed cached location to: " + location);
      }
      addToCachedServers(updatedLocations);
    }
  }

  /**
   * Put a newly discovered HRegionLocation into the cache.
   * @param tableName The table name.
   * @param locations the new locations
   */
  public void cacheLocation(final TableName tableName, final RegionLocations locations) {
    byte [] startKey = locations.getRegionLocation().getRegionInfo().getStartKey();
    ConcurrentMap<byte[], RegionLocations> tableLocations = getTableLocations(tableName);
    RegionLocations oldLocation = tableLocations.putIfAbsent(startKey, locations);
    boolean isNewCacheEntry = (oldLocation == null);
    if (isNewCacheEntry) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Cached location: " + locations);
      }
      addToCachedServers(locations);
      return;
    }

    // merge old and new locations and add it to the cache
    // Meta record might be stale - some (probably the same) server has closed the region
    // with later seqNum and told us about the new location.
    RegionLocations mergedLocation = oldLocation.mergeLocations(locations);
    boolean replaced = tableLocations.replace(startKey, oldLocation, mergedLocation);
    if (replaced && LOG.isTraceEnabled()) {
      LOG.trace("Merged cached locations: " + mergedLocation);
    }
    addToCachedServers(locations);
  }

  private void addToCachedServers(RegionLocations locations) {
    for (HRegionLocation loc : locations.getRegionLocations()) {
      if (loc != null) {
        cachedServers.add(loc.getServerName());
      }
    }
  }

  /**
   * @param tableName
   * @return Map of cached locations for passed <code>tableName</code>
   */
  private ConcurrentSkipListMap<byte[], RegionLocations>
    getTableLocations(final TableName tableName) {
    // find the map of cached locations for this table
    ConcurrentSkipListMap<byte[], RegionLocations> result;
    result = this.cachedRegionLocations.get(tableName);
    // if tableLocations for this table isn't built yet, make one
    if (result == null) {
      result = new ConcurrentSkipListMap<byte[], RegionLocations>(Bytes.BYTES_COMPARATOR);
      ConcurrentSkipListMap<byte[], RegionLocations> old =
          this.cachedRegionLocations.putIfAbsent(tableName, result);
      if (old != null) {
        return old;
      }
    }
    return result;
  }

  /**
   * Check the region cache to see whether a region is cached yet or not.
   * @param tableName tableName
   * @param row row
   * @return Region cached or not.
   */
  public boolean isRegionCached(TableName tableName, final byte[] row) {
    RegionLocations location = getCachedLocation(tableName, row);
    return location != null;
  }

  /**
   * Return the number of cached region for a table. It will only be called
   * from a unit test.
   */
  public int getNumberOfCachedRegionLocations(final TableName tableName) {
    Map<byte[], RegionLocations> tableLocs = this.cachedRegionLocations.get(tableName);
    if (tableLocs == null) {
      return 0;
    }
    int numRegions = 0;
    for (RegionLocations tableLoc : tableLocs.values()) {
      numRegions += tableLoc.numNonNullElements();
    }
    return numRegions;
  }

  /**
   * Delete all cached entries.
   */
  public void clearCache() {
    this.cachedRegionLocations.clear();
    this.cachedServers.clear();
  }

  /**
   * Delete all cached entries of a server.
   */
  public void clearCache(final ServerName serverName) {
    if (!this.cachedServers.contains(serverName)) {
      return;
    }

    boolean deletedSomething = false;
    synchronized (this.cachedServers) {
      // We block here, because if there is an error on a server, it's likely that multiple
      //  threads will get the error  simultaneously. If there are hundreds of thousand of
      //  region location to check, it's better to do this only once. A better pattern would
      //  be to check if the server is dead when we get the region location.
      if (!this.cachedServers.contains(serverName)) {
        return;
      }
      for (ConcurrentMap<byte[], RegionLocations> tableLocations : cachedRegionLocations.values()){
        for (Entry<byte[], RegionLocations> e : tableLocations.entrySet()) {
          RegionLocations regionLocations = e.getValue();
          if (regionLocations != null) {
            RegionLocations updatedLocations = regionLocations.removeByServer(serverName);
            if (updatedLocations != regionLocations) {
              if (updatedLocations.isEmpty()) {
                deletedSomething |= tableLocations.remove(e.getKey(), regionLocations);
              } else {
                deletedSomething |= tableLocations.replace(e.getKey(), regionLocations, updatedLocations);
              }
            }
          }
        }
      }
      this.cachedServers.remove(serverName);
    }
    if (deletedSomething && LOG.isTraceEnabled()) {
      LOG.trace("Removed all cached region locations that map to " + serverName);
    }
  }

  /**
   * Delete all cached entries of a table.
   */
  public void clearCache(final TableName tableName) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Removed all cached region locations for table " + tableName);
    }
    this.cachedRegionLocations.remove(tableName);
  }

  /**
   * Delete a cached location, no matter what it is. Called when we were told to not use cache.
   * @param tableName tableName
   * @param row
   */
  public void clearCache(final TableName tableName, final byte [] row, int replicaId) {
    ConcurrentMap<byte[], RegionLocations> tableLocations = getTableLocations(tableName);

    boolean removed = false;
    RegionLocations regionLocations = getCachedLocation(tableName, row);
    if (regionLocations != null) {
      HRegionLocation toBeRemoved = regionLocations.getRegionLocation(replicaId);
      RegionLocations updatedLocations = regionLocations.remove(replicaId);
      if (updatedLocations != regionLocations) {
        byte[] startKey = regionLocations.getRegionLocation().getRegionInfo().getStartKey();
        if (updatedLocations.isEmpty()) {
          removed = tableLocations.remove(startKey, regionLocations);
        } else {
          removed = tableLocations.replace(startKey, regionLocations, updatedLocations);
        }
      }

      if (removed && LOG.isTraceEnabled() && toBeRemoved != null) {
        LOG.trace("Removed " + toBeRemoved + " from cache");
      }
    }
  }

  /**
   * Delete a cached location, no matter what it is. Called when we were told to not use cache.
   * @param tableName tableName
   * @param row
   */
  public void clearCache(final TableName tableName, final byte [] row) {
    ConcurrentMap<byte[], RegionLocations> tableLocations = getTableLocations(tableName);

    RegionLocations regionLocations = getCachedLocation(tableName, row);
    if (regionLocations != null) {
      byte[] startKey = regionLocations.getRegionLocation().getRegionInfo().getStartKey();
      boolean removed = tableLocations.remove(startKey, regionLocations);
      if (removed && LOG.isTraceEnabled()) {
        LOG.trace("Removed " + regionLocations + " from cache");
      }
    }
  }

  /**
   * Delete a cached location for a table, row and server
   */
  public void clearCache(final TableName tableName, final byte [] row, ServerName serverName) {
    ConcurrentMap<byte[], RegionLocations> tableLocations = getTableLocations(tableName);

    RegionLocations regionLocations = getCachedLocation(tableName, row);
    if (regionLocations != null) {
      RegionLocations updatedLocations = regionLocations.removeByServer(serverName);
      if (updatedLocations != regionLocations) {
        byte[] startKey = regionLocations.getRegionLocation().getRegionInfo().getStartKey();
        boolean removed = false;
        if (updatedLocations.isEmpty()) {
          removed = tableLocations.remove(startKey, regionLocations);
        } else {
          removed = tableLocations.replace(startKey, regionLocations, updatedLocations);
        }
        if (removed && LOG.isTraceEnabled()) {
          LOG.trace("Removed locations of table: " + tableName + " ,row: " + Bytes.toString(row)
            + " mapping to server: " + serverName + " from cache");
        }
      }
    }
  }

  /**
   * Deletes the cached location of the region if necessary, based on some error from source.
   * @param hri The region in question.
   */
  public void clearCache(HRegionInfo hri) {
    ConcurrentMap<byte[], RegionLocations> tableLocations = getTableLocations(hri.getTable());
    RegionLocations regionLocations = tableLocations.get(hri.getStartKey());
    if (regionLocations != null) {
      HRegionLocation oldLocation = regionLocations.getRegionLocation(hri.getReplicaId());
      if (oldLocation == null) return;
      RegionLocations updatedLocations = regionLocations.remove(oldLocation);
      boolean removed = false;
      if (updatedLocations != regionLocations) {
        if (updatedLocations.isEmpty()) {
          removed = tableLocations.remove(hri.getStartKey(), regionLocations);
        } else {
          removed = tableLocations.replace(hri.getStartKey(), regionLocations, updatedLocations);
        }
        if (removed && LOG.isTraceEnabled()) {
          LOG.trace("Removed " + oldLocation + " from cache");
        }
      }
    }
  }

  public void clearCache(final HRegionLocation location) {
    if (location == null) {
      return;
    }
    TableName tableName = location.getRegionInfo().getTable();
    ConcurrentMap<byte[], RegionLocations> tableLocations = getTableLocations(tableName);
    RegionLocations regionLocations = tableLocations.get(location.getRegionInfo().getStartKey());
    if (regionLocations != null) {
      RegionLocations updatedLocations = regionLocations.remove(location);
      boolean removed = false;
      if (updatedLocations != regionLocations) {
        if (updatedLocations.isEmpty()) {
          removed = tableLocations.remove(location.getRegionInfo().getStartKey(), regionLocations);
        } else {
          removed = tableLocations.replace(location.getRegionInfo().getStartKey(), regionLocations, updatedLocations);
        }
        if (removed && LOG.isTraceEnabled()) {
          LOG.trace("Removed " + location + " from cache");
        }
      }
    }
  }
}
