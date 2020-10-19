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

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_META_REPLICA_NUM;
import static org.apache.hadoop.hbase.HConstants.META_REPLICAS_NUM;
import static org.apache.hadoop.hbase.client.ConnectionUtils.isEmptyStopRow;
import static org.apache.hadoop.hbase.util.Bytes.BYTES_COMPARATOR;
import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;

/**
 * MetaReplicaLoadBalanceReplicaSimpleChooser implements a simple meta replica load balancing
 * algorithm. It maintains a stale location cache for each table. Whenever client looks up meta,
 * it first check if the row is the stale location cache, if yes, this means the the location from
 * meta replica is stale, it will go to the primary meta to look up update-to-date location;
 * otherwise, it will randomly pick up a meta replica region for meta lookup. When clients receive
 * RegionNotServedException from region servers, it will add these region locations to the stale
 * location cache. The stale cache will be cleaned up periodically by a chore.
 */

/**
 * StaleLocationCacheEntry is the entry when a stale location is reported by an client.
 */
class StaleLocationCacheEntry {
  // meta replica id where
  private int metaReplicaId;

  // timestamp in milliseconds
  private long timestamp;

  private byte[] endKey;

  StaleLocationCacheEntry(final int metaReplicaId, final byte[] endKey) {
    this.metaReplicaId = metaReplicaId;
    this.endKey = endKey;
    timestamp = System.currentTimeMillis();
  }

  public byte[] getEndKey() {
    return this.endKey;
  }

  public int getMetaReplicaId() {
    return this.metaReplicaId;
  }
  public long getTimestamp() {
    return this.timestamp;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
      .append("endKey", endKey)
      .append("metaReplicaId", metaReplicaId)
      .append("timestamp", timestamp)
      .toString();
  }
}

/**
 * A simple implementation of MetaReplicaLoadBalanceReplicaChooser.
 *
 * It follows a simple algorithm to choose a meta replica to go:
 *
 *  1. If there is no stale location entry for rows it looks up, it will randomly
 *     pick a meta replica region to do lookup.
 *  2. If the location from meta replica region is stale, client gets RegionNotServedException
 *     from region server, in this case, it will create StaleLocationCacheEntry in
 *     MetaReplicaLoadBalanceReplicaSimpleChooser.
 *  3. When client tries to do meta lookup, it checks StaleLocationCache first for rows it tries to
 *     lookup, if entry exists, it will go with primary meta region to do lookup; otherwise, it
 *     will follow step 1.
 *  4. A chore will periodically run to clean up cache entries in the StaleLocationCache.
 */
class MetaReplicaLoadBalanceReplicaSimpleChooser implements MetaReplicaLoadBalanceReplicaChooser {
  private static final Logger LOG =
    LoggerFactory.getLogger(MetaReplicaLoadBalanceReplicaSimpleChooser.class);
  private final long STALE_CACHE_TIMEOUT_IN_MILLISECONDS = 3000; // 3 seconds
  private final int STALE_CACHE_CLEAN_CHORE_INTERVAL = 1500; // 1.5 seconds

  private final class StaleTableCache {
    private final ConcurrentNavigableMap<byte[], StaleLocationCacheEntry> cache =
      new ConcurrentSkipListMap<>(BYTES_COMPARATOR);
  }

  private final ConcurrentMap<TableName, StaleTableCache> staleCache;
  private final int numOfMetaReplicas;
  private final AsyncConnectionImpl conn;

  MetaReplicaLoadBalanceReplicaSimpleChooser(final AsyncConnectionImpl conn) {
    staleCache = new ConcurrentHashMap<>();
    this.numOfMetaReplicas = conn.getConfiguration().getInt(
      META_REPLICAS_NUM, DEFAULT_META_REPLICA_NUM);
    this.conn = conn;
    this.conn.getChoreService().scheduleChore(getCacheCleanupChore(this));
  }

  /**
   * When a client runs into RegionNotServingException, it will call this method to
   * update Chooser's internal state.
   * @param loc the location which causes exception.
   * @param fromMetaReplicaId the meta replica id where the location comes from.
   */
  public void updateCacheOnError(final HRegionLocation loc, final int fromMetaReplicaId) {
    StaleTableCache tableCache =
      computeIfAbsent(staleCache, loc.getRegion().getTable(), StaleTableCache::new);
    byte[] startKey = loc.getRegion().getStartKey();
    tableCache.cache.putIfAbsent(startKey,
      new StaleLocationCacheEntry(fromMetaReplicaId, loc.getRegion().getEndKey()));
    LOG.debug("Add entry to stale cache for table {} with startKey {}, {}",
      loc.getRegion().getTable(), startKey, loc.getRegion().getEndKey());
  }

  /**
   * When it does a meta lookup, it will call this method to find a meta replica to go.
   * @param tablename  table name it looks up
   * @param row   key it looks up.
   * @param locateType locateType, Only BEFORE and CURRENT will be passed in.
   * @return meta replica id
   */
  public int chooseReplicaToGo(final TableName tablename, final byte[] row,
    final RegionLocateType locateType) {
    StaleTableCache tableCache = staleCache.get(tablename);
    int metaReplicaId = 1 + ThreadLocalRandom.current().nextInt(this.numOfMetaReplicas - 1);

    // If there is no entry in StaleCache, pick a random meta replica id.
    if (tableCache == null) {
      return metaReplicaId;
    }

    Map.Entry<byte[], StaleLocationCacheEntry> entry;
    boolean isEmptyStopRow = isEmptyStopRow(row);
    // Only BEFORE and CURRENT are passed in.
    if (locateType == RegionLocateType.BEFORE) {
      entry = isEmptyStopRow ? tableCache.cache.lastEntry() : tableCache.cache.lowerEntry(row);
    } else {
      entry = tableCache.cache.floorEntry(row);
    }

    if (entry == null) {
      return metaReplicaId;
    }

    // Check if the entry times out.
    if ((System.currentTimeMillis() - entry.getValue().getTimestamp()) >=
      STALE_CACHE_TIMEOUT_IN_MILLISECONDS) {
      LOG.debug("Entry for table {} with startKey {}, {} times out", tablename, entry.getKey(),
        entry);
      tableCache.cache.remove(entry.getKey());
      return metaReplicaId;
    }

    byte[] endKey =  entry.getValue().getEndKey();

    if (isEmptyStopRow(endKey)) {
      LOG.debug("Lookup {} goes to primary meta", row);
      return RegionInfo.DEFAULT_REPLICA_ID;
    }

    if (locateType == RegionLocateType.BEFORE) {
      if (!isEmptyStopRow && Bytes.compareTo(endKey, row) >= 0) {
        LOG.debug("Lookup {} goes to primary meta", row);
        return RegionInfo.DEFAULT_REPLICA_ID;
      }
    } else {
      if (Bytes.compareTo(row, endKey) < 0) {
        LOG.debug("Lookup {} goes to primary meta", row);
        return RegionInfo.DEFAULT_REPLICA_ID;
      }
    }

    return metaReplicaId;
  }

  private void cleanupMetaReplicaStaleCache() {
    long curTimeInMills = System.currentTimeMillis();
    for (StaleTableCache tableCache : staleCache.values()) {
      Iterator<Map.Entry<byte[], StaleLocationCacheEntry>> it =
        tableCache.cache.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<byte[], StaleLocationCacheEntry> entry = it.next();
        if (curTimeInMills - entry.getValue().getTimestamp() >=
          STALE_CACHE_TIMEOUT_IN_MILLISECONDS) {
          LOG.debug("clean entry {}, {} from stale cache", entry.getKey(), entry.getValue());
          it.remove();
        }
      }
    }
  }

  private static Stoppable createDummyStoppable() {
    return new Stoppable() {
      private volatile boolean isStopped = false;

      @Override
      public void stop(String why) {
        isStopped = true;
      }

      @Override
      public boolean isStopped() {
        return isStopped;
      }
    };
  }

  public ScheduledChore getCacheCleanupChore(
    final MetaReplicaLoadBalanceReplicaSimpleChooser simpleChooser) {
    Stoppable stoppable = createDummyStoppable();
    return new ScheduledChore("CleanupMetaReplicaStaleCache", stoppable,
      STALE_CACHE_CLEAN_CHORE_INTERVAL) {
      @Override
      protected void chore() {
        simpleChooser.cleanupMetaReplicaStaleCache();
      }
    };
  }
}
