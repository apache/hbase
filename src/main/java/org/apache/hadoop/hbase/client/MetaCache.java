/**
 * Copyright 2014 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Cache for meta information (HRegionLocation)
 */
public class MetaCache {
  static final Log LOG = LogFactory.getLog(MetaCache.class);

  public final Map<Integer, ConcurrentSkipListMap<byte[], HRegionLocation>>
    tableToCache = new ConcurrentHashMap<>();

  // The presence of a server in the map implies it's likely that there is an
  // entry in cachedRegionLocations that map to this server; but the absence
  // of a server in this map guarantees that there is no entry in cache that
  // maps to the absent server.
  private final Set<String> servers = new HashSet<>();

  public Set<String> getServers() {
    return servers;
  }

  /**
   * Gets meta cache map of a table.
   *
   * @return Map of cached locations for passed <code>tableName</code>. A new
   *         map is created if not found.
   */
  public NavigableMap<byte[], HRegionLocation> getForTable(
      final byte[] tableName) {
    // find the map of cached locations for this table
    Integer key = Bytes.mapKey(tableName);
    ConcurrentSkipListMap<byte[], HRegionLocation> result =
        this.tableToCache.get(key);
    if (result == null) {
      synchronized (this.tableToCache) {
        result = this.tableToCache.get(key);
        if (result == null) {
          // if tableLocations for this table isn't built yet, make one
          result = new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);
          this.tableToCache.put(key, result);
        }
      }
    }
    return result;
  }

  /*
   * Searches the cache for a location that fits our table and row key.
   * Return null if no suitable region is located. TODO: synchronization note
   *
   * <p>TODO: This method during writing consumes 15% of CPU doing lookup
   * into the Soft Reference SortedMap.  Improve.
   *
   * @param tableName
   * @param row  a non-null byte array
   * @return Null or region location found in cache.
   */
  HRegionLocation getForRow(final byte[] tableName, final byte[] row) {
    NavigableMap<byte[], HRegionLocation> tableMeta = getForTable(tableName);

    // start to examine the cache. we can only do cache actions
    // if there's something in the cache for this table.
    if (tableMeta.isEmpty()) {
      return null;
    }

    HRegionLocation rl = tableMeta.get(row);
    if (rl != null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Cache hit for row <" + Bytes.toStringBinary(row)
            + "> in tableName " + Bytes.toStringBinary(tableName)
            + ": location server " + rl.getServerAddress()
            + ", location region name "
            + rl.getRegionInfo().getRegionNameAsString());
      }
      return rl;
    }

    // get the matching region for the row
    Entry<byte[], HRegionLocation> entry = tableMeta.floorEntry(row);
    HRegionLocation possibleRegion = (entry == null) ? null : entry.getValue();

    // we need to examine the cached location to verify that it is
    // a match by end key as well.
    if (possibleRegion != null) {
      byte[] endKey = possibleRegion.getRegionInfo().getEndKey();

      // make sure that the end key is greater than the row we're looking
      // for, otherwise the row actually belongs in the next region, not
      // this one. the exception case is when the end key is
      // HConstants.EMPTY_START_ROW, signifying that the region we're
      // checking is actually the last region in the table.
      if (Bytes.equals(endKey, HConstants.EMPTY_END_ROW)
          || KeyValue.getRowComparator(tableName).compareRows(endKey, 0,
              endKey.length, row, 0, row.length) > 0) {
        return possibleRegion;
      }
    }

    // Passed all the way through, so we got nothing - complete cache miss
    return null;
  }

  /**
   * Deletes a cached meta for the specified table name and row if it is
   * located on the (optionally) specified old location.
   */
  public void deleteForRow(final byte[] tableName, final byte[] row,
      HServerAddress oldServer) {
    synchronized (this.tableToCache) {
      Map<byte[], HRegionLocation> tableLocations = getForTable(tableName);

      // start to examine the cache. we can only do cache actions
      // if there's something in the cache for this table.
      if (!tableLocations.isEmpty()) {
        HRegionLocation rl = getForRow(tableName, row);
        if (rl != null) {
          // If oldLocation is specified. deleteLocation only if it is the
          // same.
          if (oldServer != null && !oldServer.equals(rl.getServerAddress()))
            return; // perhaps, some body else cleared and re-populated.

          tableLocations.remove(rl.getRegionInfo().getStartKey());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Removed " + rl.getRegionInfo().getRegionNameAsString()
                + " for tableName=" + Bytes.toStringBinary(tableName)
                + " from cache " + "because of " + Bytes.toStringBinary(row));
          }
        }
      }
    }
  }

  /**
   * Deletes all cached entries of a table that maps to a specific location.
   *
   */
  protected void clearForServer(final String server) {
    boolean deletedSomething = false;
    synchronized (this.tableToCache) {
      if (!servers.contains(server)) {
        return;
      }
      for (Map<byte[], HRegionLocation> tableLocations : tableToCache
          .values()) {
        for (Entry<byte[], HRegionLocation> e : tableLocations.entrySet()) {
          if (e.getValue().getServerAddress().toString().equals(server)) {
            tableLocations.remove(e.getKey());
            deletedSomething = true;
          }
        }
      }
      servers.remove(server);
    }
    if (deletedSomething && LOG.isDebugEnabled()) {
      LOG.debug("Removed all cached region locations that map to " + server);
    }
  }

  /**
   * Adds a newly discovered HRegionLocation into the cache.
   *
   * FIXME the first parameter seems not necessary.
   */
  public void add(final byte[] tableName, final HRegionLocation location) {
    byte[] startKey = location.getRegionInfo().getStartKey();
    HRegionLocation oldLocation;
    synchronized (this.tableToCache) {
      servers.add(location.getServerAddress().toString());
      oldLocation = getForTable(tableName).put(startKey, location);
    }
    if (oldLocation == null) {
      LOG.debug("Cached location for "
          + location.getRegionInfo().getRegionNameAsString() + " is "
          + location.getServerAddress().toString());
    } else if (!oldLocation.equals(location)) {
      LOG.debug("Cached location for "
          + location.getRegionInfo().getRegionNameAsString() + " is changed"
          + " from " + oldLocation.getServerAddress()
          + " to " + location.getServerAddress());
    }
  }

  /**
   * Clears all meta cache
   */
  public void clear() {
    synchronized (this.tableToCache) {
      tableToCache.clear();
      servers.clear();
    }
  }

  /*
   * Returns the number of cached regions for a table. It will only be called
   * from a unit test.
   */
  int getNumber(final byte[] tableName) {
    Integer key = Bytes.mapKey(tableName);
    synchronized (this.tableToCache) {
      Map<byte[], HRegionLocation> tableLocs = this.tableToCache.get(key);

      if (tableLocs == null) {
        return 0;
      }
      return tableLocs.size();
    }
  }

}
