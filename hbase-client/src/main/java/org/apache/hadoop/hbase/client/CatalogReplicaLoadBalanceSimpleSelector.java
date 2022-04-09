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

import static org.apache.hadoop.hbase.client.ConnectionUtils.isEmptyStopRow;
import static org.apache.hadoop.hbase.util.Bytes.BYTES_COMPARATOR;
import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntSupplier;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
/**
 * <p>CatalogReplicaLoadBalanceReplicaSimpleSelector implements a simple catalog replica load
 * balancing algorithm. It maintains a stale location cache for each table. Whenever client looks
 * up location, it first check if the row is the stale location cache. If yes, the location from
 * catalog replica is stale, it will go to the primary region to look up update-to-date location;
 * otherwise, it will randomly pick up a replica region or primary region for lookup. When clients
 * receive RegionNotServedException from region servers, it will add these region locations to the
 * stale location cache. The stale cache will be cleaned up periodically by a chore.</p>
 *
 * It follows a simple algorithm to choose a meta replica region (including primary meta) to go:
 *
 * <ol>
 *  <li>If there is no stale location entry for rows it looks up, it will randomly
 *     pick a meta replica region (including primary meta) to do lookup. </li>
 *  <li>If the location from the replica region is stale, client gets RegionNotServedException
 *     from region server, in this case, it will create StaleLocationCacheEntry in
 *     CatalogReplicaLoadBalanceReplicaSimpleSelector.</li>
 *  <li>When client tries to do location lookup, it checks StaleLocationCache first for rows it
 *     tries to lookup, if entry exists, it will go with primary meta region to do lookup;
 *     otherwise, it will follow step 1.</li>
 *  <li>A chore will periodically run to clean up cache entries in the StaleLocationCache.</li>
 * </ol>
 */
class CatalogReplicaLoadBalanceSimpleSelector implements
  CatalogReplicaLoadBalanceSelector, Stoppable {
  private static final Logger LOG =
    LoggerFactory.getLogger(CatalogReplicaLoadBalanceSimpleSelector.class);
  private final long STALE_CACHE_TIMEOUT_IN_MILLISECONDS = 3000; // 3 seconds
  private final int STALE_CACHE_CLEAN_CHORE_INTERVAL_IN_MILLISECONDS = 1500; // 1.5 seconds
  private final int REFRESH_REPLICA_COUNT_CHORE_INTERVAL_IN_MILLISECONDS = 60000; // 1 minute

  /**
   * StaleLocationCacheEntry is the entry when a stale location is reported by an client.
   */
  private static final class StaleLocationCacheEntry {
    // timestamp in milliseconds
    private final long timestamp;

    private final byte[] endKey;

    StaleLocationCacheEntry(final byte[] endKey) {
      this.endKey = endKey;
      timestamp = EnvironmentEdgeManager.currentTime();
    }

    public byte[] getEndKey() {
      return this.endKey;
    }

    public long getTimestamp() {
      return this.timestamp;
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
        .append("endKey", endKey)
        .append("timestamp", timestamp)
        .toString();
    }
  }

  private final ConcurrentMap<TableName,
    ConcurrentNavigableMap<byte[], StaleLocationCacheEntry>> staleCache = new ConcurrentHashMap<>();
  private volatile int numOfReplicas;
  private final AsyncConnectionImpl conn;
  private final TableName tableName;
  private final IntSupplier getNumOfReplicas;
  private volatile boolean isStopped = false;

  CatalogReplicaLoadBalanceSimpleSelector(TableName tableName, AsyncConnectionImpl conn,
    IntSupplier getNumOfReplicas) {
    this.conn = conn;
    this.tableName = tableName;
    this.getNumOfReplicas = getNumOfReplicas;

    // This numOfReplicas is going to be lazy initialized.
    this.numOfReplicas = CatalogReplicaLoadBalanceSelector.UNINITIALIZED_NUM_OF_REPLICAS;
    // Start chores
    this.conn.getChoreService().scheduleChore(getCacheCleanupChore(this));
    this.conn.getChoreService().scheduleChore(getRefreshReplicaCountChore(this));
  }

  /**
   * When a client runs into RegionNotServingException, it will call this method to
   * update Selector's internal state.
   * @param loc the location which causes exception.
   */
  public void onError(HRegionLocation loc) {
    ConcurrentNavigableMap<byte[], StaleLocationCacheEntry> tableCache =
      computeIfAbsent(staleCache, loc.getRegion().getTable(),
        () -> new ConcurrentSkipListMap<>(BYTES_COMPARATOR));
    byte[] startKey = loc.getRegion().getStartKey();
    tableCache.putIfAbsent(startKey,
      new StaleLocationCacheEntry(loc.getRegion().getEndKey()));
    LOG.debug("Add entry to stale cache for table {} with startKey {}, {}",
      loc.getRegion().getTable(), startKey, loc.getRegion().getEndKey());
  }

  /**
   * Select an random replica id (including the primary replica id). In case there is no replica region configured, return
   * the primary replica id.
   * @return Replica id
   */
  private int getRandomReplicaId() {
    int cachedNumOfReplicas = this.numOfReplicas;
    if (cachedNumOfReplicas == CatalogReplicaLoadBalanceSelector.UNINITIALIZED_NUM_OF_REPLICAS) {
      cachedNumOfReplicas = refreshCatalogReplicaCount();
      this.numOfReplicas = cachedNumOfReplicas;
    }
    // In case of no replica configured, return the primary region id.
    if (cachedNumOfReplicas <= 1) {
      return RegionInfo.DEFAULT_REPLICA_ID;
    }
    return ThreadLocalRandom.current().nextInt(cachedNumOfReplicas);
  }

  /**
   * When it looks up a location, it will call this method to find a replica region to go.
   * For a normal case, > 99% of region locations from catalog/meta replica will be up to date.
   * In extreme cases such as region server crashes, it will depends on how fast replication
   * catches up.
   *
   * @param tablename table name it looks up
   * @param row key it looks up.
   * @param locateType locateType, Only BEFORE and CURRENT will be passed in.
   * @return catalog replica id
   */
  public int select(final TableName tablename, final byte[] row,
    final RegionLocateType locateType) {
    Preconditions.checkArgument(locateType == RegionLocateType.BEFORE ||
        locateType == RegionLocateType.CURRENT,
      "Expected type BEFORE or CURRENT but got: %s", locateType);

    ConcurrentNavigableMap<byte[], StaleLocationCacheEntry> tableCache = staleCache.get(tablename);

    // If there is no entry in StaleCache, select a random replica id.
    if (tableCache == null) {
      return getRandomReplicaId();
    }

    Map.Entry<byte[], StaleLocationCacheEntry> entry;
    boolean isEmptyStopRow = isEmptyStopRow(row);
    // Only BEFORE and CURRENT are passed in.
    if (locateType == RegionLocateType.BEFORE) {
      entry = isEmptyStopRow ? tableCache.lastEntry() : tableCache.lowerEntry(row);
    } else {
      entry = tableCache.floorEntry(row);
    }

    // It is not in the stale cache, return a random replica id.
    if (entry == null) {
      return getRandomReplicaId();
    }

    // The entry here is a possible match for the location. Check if the entry times out first as
    // long comparing is faster than comparing byte arrays(in most cases). It could remove
    // stale entries faster. If the possible match entry does not time out, it will check if
    // the entry is a match for the row passed in and select the replica id accordingly.
    if ((EnvironmentEdgeManager.currentTime() - entry.getValue().getTimestamp()) >=
      STALE_CACHE_TIMEOUT_IN_MILLISECONDS) {
      LOG.debug("Entry for table {} with startKey {}, {} times out", tablename, entry.getKey(),
        entry);
      tableCache.remove(entry.getKey());
      return getRandomReplicaId();
    }

    byte[] endKey =  entry.getValue().getEndKey();

    // The following logic is borrowed from AsyncNonMetaRegionLocator.
    if (isEmptyStopRow(endKey)) {
      LOG.debug("Lookup {} goes to primary region", row);
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

    // Not in stale cache, return a random replica id.
    return getRandomReplicaId();
  }

  // This class implements the Stoppable interface as chores needs a Stopable object, there is
  // no-op on this Stoppable object currently.
  @Override
  public void stop(String why) {
    isStopped = true;
  }

  @Override
  public boolean isStopped() {
    return isStopped;
  }

  private void cleanupReplicaReplicaStaleCache() {
    long curTimeInMills = EnvironmentEdgeManager.currentTime();
    for (ConcurrentNavigableMap<byte[], StaleLocationCacheEntry> tableCache : staleCache.values()) {
      Iterator<Map.Entry<byte[], StaleLocationCacheEntry>> it =
        tableCache.entrySet().iterator();
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

  private int refreshCatalogReplicaCount() {
    int newNumOfReplicas = this.getNumOfReplicas.getAsInt();
    LOG.debug("Refreshed replica count {}", newNumOfReplicas);
    // If the returned number of replicas is -1, it is caused by failure to fetch the
    // replica count. Do not update the numOfReplicas in this case.
    if (newNumOfReplicas == CatalogReplicaLoadBalanceSelector.UNINITIALIZED_NUM_OF_REPLICAS) {
      LOG.error("Failed to fetch Table {}'s region replica count", tableName);
      return this.numOfReplicas;
    }

    int cachedNumOfReplicas = this.numOfReplicas;
    if ((cachedNumOfReplicas == UNINITIALIZED_NUM_OF_REPLICAS) ||
      (cachedNumOfReplicas != newNumOfReplicas)) {
      this.numOfReplicas = newNumOfReplicas;
    }
    return newNumOfReplicas;
  }

  private ScheduledChore getCacheCleanupChore(
    final CatalogReplicaLoadBalanceSimpleSelector selector) {
    return new ScheduledChore("CleanupCatalogReplicaStaleCache", this,
      STALE_CACHE_CLEAN_CHORE_INTERVAL_IN_MILLISECONDS) {
      @Override
      protected void chore() {
        selector.cleanupReplicaReplicaStaleCache();
      }
    };
  }

  private ScheduledChore getRefreshReplicaCountChore(
    final CatalogReplicaLoadBalanceSimpleSelector selector) {
    return new ScheduledChore("RefreshReplicaCountChore", this,
      REFRESH_REPLICA_COUNT_CHORE_INTERVAL_IN_MILLISECONDS) {
      @Override
      protected void chore() {
        selector.refreshCatalogReplicaCount();
      }
    };
  }
}
