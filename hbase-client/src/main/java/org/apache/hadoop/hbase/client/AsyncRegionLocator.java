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

import static org.apache.hadoop.hbase.HConstants.DEFAULT_USE_META_REPLICAS;
import static org.apache.hadoop.hbase.HConstants.USE_META_REPLICAS;
import static org.apache.hadoop.hbase.client.AsyncRegionLocatorHelper.createRegionLocations;
import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.io.netty.util.HashedWheelTimer;
import org.apache.hbase.thirdparty.io.netty.util.Timeout;

/**
 * The asynchronous region locator.
 */
@InterfaceAudience.Private
class AsyncRegionLocator {

  @VisibleForTesting
  static final String MAX_CONCURRENT_LOCATE_REQUEST_PER_TABLE =
    "hbase.client.meta.max.concurrent.locate.per.table";

  private static final int DEFAULT_MAX_CONCURRENT_LOCATE_REQUEST_PER_TABLE = 8;

  @VisibleForTesting
  static final String MAX_CONCURRENT_LOCATE_META_REQUEST =
    "hbase.client.meta.max.concurrent.locate";

  @VisibleForTesting
  static String LOCATE_PREFETCH_LIMIT = "hbase.client.locate.prefetch.limit";

  private static final int DEFAULT_LOCATE_PREFETCH_LIMIT = 10;

  private final HashedWheelTimer retryTimer;

  private final AsyncConnectionImpl conn;

  private final int maxConcurrentLocateRequestPerTable;

  private final int maxConcurrentLocateMetaRequest;

  private final int locatePrefetchLimit;

  private final boolean useMetaReplicas;

  private final ConcurrentMap<TableName, AbstractAsyncTableRegionLocator> table2Locator =
    new ConcurrentHashMap<>();

  public AsyncRegionLocator(AsyncConnectionImpl conn, HashedWheelTimer retryTimer) {
    this.conn = conn;
    this.retryTimer = retryTimer;
    this.maxConcurrentLocateRequestPerTable = conn.getConfiguration().getInt(
      MAX_CONCURRENT_LOCATE_REQUEST_PER_TABLE, DEFAULT_MAX_CONCURRENT_LOCATE_REQUEST_PER_TABLE);
    this.maxConcurrentLocateMetaRequest = conn.getConfiguration()
      .getInt(MAX_CONCURRENT_LOCATE_META_REQUEST, maxConcurrentLocateRequestPerTable);
    this.locatePrefetchLimit =
      conn.getConfiguration().getInt(LOCATE_PREFETCH_LIMIT, DEFAULT_LOCATE_PREFETCH_LIMIT);
    this.useMetaReplicas =
      conn.getConfiguration().getBoolean(USE_META_REPLICAS, DEFAULT_USE_META_REPLICAS);
  }

  private <T> CompletableFuture<T> withTimeout(CompletableFuture<T> future, long timeoutNs,
    Supplier<String> timeoutMsg) {
    if (future.isDone() || timeoutNs <= 0) {
      return future;
    }
    Timeout timeoutTask = retryTimer.newTimeout(t -> {
      if (future.isDone()) {
        return;
      }
      future.completeExceptionally(new TimeoutIOException(timeoutMsg.get()));
    }, timeoutNs, TimeUnit.NANOSECONDS);
    FutureUtils.addListener(future, (loc, error) -> {
      if (error != null && error.getClass() != TimeoutIOException.class) {
        // cancel timeout task if we are not completed by it.
        timeoutTask.cancel();
      }
    });
    return future;
  }

  private boolean isMeta(TableName tableName) {
    return TableName.isMetaTableName(tableName);
  }

  private AbstractAsyncTableRegionLocator getOrCreateTableRegionLocator(TableName tableName) {
    return computeIfAbsent(table2Locator, tableName, () -> {
      if (isMeta(tableName)) {
        return new AsyncMetaTableRegionLocator(conn, tableName, maxConcurrentLocateMetaRequest);
      } else {
        return new AsyncNonMetaTableRegionLocator(conn, tableName,
          maxConcurrentLocateRequestPerTable, locatePrefetchLimit, useMetaReplicas);
      }
    });
  }

  @VisibleForTesting
  CompletableFuture<RegionLocations> getRegionLocations(TableName tableName, byte[] row,
    int replicaId, RegionLocateType locateType, boolean reload) {
    return getOrCreateTableRegionLocator(tableName).getRegionLocations(row, replicaId, locateType,
      reload);
  }

  CompletableFuture<RegionLocations> getRegionLocations(TableName tableName, byte[] row,
    RegionLocateType type, boolean reload, long timeoutNs) {
    CompletableFuture<RegionLocations> future =
      getRegionLocations(tableName, row, RegionReplicaUtil.DEFAULT_REPLICA_ID, type, reload);
    return withTimeout(future, timeoutNs,
      () -> "Timeout(" + TimeUnit.NANOSECONDS.toMillis(timeoutNs) +
        "ms) waiting for region locations for " + tableName + ", row='" +
        Bytes.toStringBinary(row) + "'");
  }

  CompletableFuture<HRegionLocation> getRegionLocation(TableName tableName, byte[] row,
    int replicaId, RegionLocateType type, boolean reload, long timeoutNs) {
    // meta region can not be split right now so we always call the same method.
    // Change it later if the meta table can have more than one regions.
    CompletableFuture<HRegionLocation> future = new CompletableFuture<>();
    CompletableFuture<RegionLocations> locsFuture =
      getRegionLocations(tableName, row, replicaId, type, reload);
    addListener(locsFuture, (locs, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
        return;
      }
      HRegionLocation loc = locs.getRegionLocation(replicaId);
      if (loc == null) {
        future.completeExceptionally(
          new RegionOfflineException("No location for " + tableName + ", row='" +
            Bytes.toStringBinary(row) + "', locateType=" + type + ", replicaId=" + replicaId));
      } else if (loc.getServerName() == null) {
        future.completeExceptionally(
          new RegionOfflineException("No server address listed for region '" +
            loc.getRegion().getRegionNameAsString() + ", row='" + Bytes.toStringBinary(row) +
            "', locateType=" + type + ", replicaId=" + replicaId));
      } else {
        future.complete(loc);
      }
    });
    return withTimeout(future, timeoutNs,
      () -> "Timeout(" + TimeUnit.NANOSECONDS.toMillis(timeoutNs) +
        "ms) waiting for region location for " + tableName + ", row='" + Bytes.toStringBinary(row) +
        "', replicaId=" + replicaId);
  }

  CompletableFuture<HRegionLocation> getRegionLocation(TableName tableName, byte[] row,
    int replicaId, RegionLocateType type, long timeoutNs) {
    return getRegionLocation(tableName, row, replicaId, type, false, timeoutNs);
  }

  CompletableFuture<HRegionLocation> getRegionLocation(TableName tableName, byte[] row,
    RegionLocateType type, boolean reload, long timeoutNs) {
    return getRegionLocation(tableName, row, RegionReplicaUtil.DEFAULT_REPLICA_ID, type, reload,
      timeoutNs);
  }

  CompletableFuture<HRegionLocation> getRegionLocation(TableName tableName, byte[] row,
    RegionLocateType type, long timeoutNs) {
    return getRegionLocation(tableName, row, type, false, timeoutNs);
  }

  /**
   * Get all region locations for a table.
   * <p/>
   * Notice that this method will not read from cache.
   */
  CompletableFuture<List<HRegionLocation>> getAllRegionLocations(TableName tableName,
    boolean excludeOfflinedSplitParents) {
    CompletableFuture<List<HRegionLocation>> future =
      getOrCreateTableRegionLocator(tableName).getAllRegionLocations(excludeOfflinedSplitParents);
    addListener(future, (locs, error) -> {
      if (error != null) {
        return;
      }
      // add locations to cache
      AbstractAsyncTableRegionLocator locator = getOrCreateTableRegionLocator(tableName);
      Map<RegionInfo, List<HRegionLocation>> map = new HashMap<>();
      for (HRegionLocation loc : locs) {
        // do not cache split parent
        if (loc.getRegion() != null && !loc.getRegion().isSplitParent()) {
          map.computeIfAbsent(RegionReplicaUtil.getRegionInfoForDefaultReplica(loc.getRegion()),
            k -> new ArrayList<>()).add(loc);
        }
      }
      for (List<HRegionLocation> l : map.values()) {
        locator.addToCache(new RegionLocations(l));
      }
    });
    return future;
  }

  private void removeLocationFromCache(HRegionLocation loc) {
    AbstractAsyncTableRegionLocator locator = table2Locator.get(loc.getRegion().getTable());
    if (locator == null) {
      return;
    }
    locator.removeLocationFromCache(loc);
  }

  private void addLocationToCache(HRegionLocation loc) {
    getOrCreateTableRegionLocator(loc.getRegion().getTable())
      .addToCache(createRegionLocations(loc));
  }

  private HRegionLocation getCachedLocation(HRegionLocation loc) {
    AbstractAsyncTableRegionLocator locator = table2Locator.get(loc.getRegion().getTable());
    if (locator == null) {
      return null;
    }
    RegionLocations locs = locator.getInCache(loc.getRegion().getStartKey());
    return locs != null ? locs.getRegionLocation(loc.getRegion().getReplicaId()) : null;
  }

  void updateCachedLocationOnError(HRegionLocation loc, Throwable exception) {
    AsyncRegionLocatorHelper.updateCachedLocationOnError(loc, exception, this::getCachedLocation,
      this::addLocationToCache, this::removeLocationFromCache, conn.getConnectionMetrics());
  }

  void clearCache(TableName tableName) {
    AbstractAsyncTableRegionLocator locator = table2Locator.remove(tableName);
    if (locator == null) {
      return;
    }
    locator.clearPendingRequests();
    conn.getConnectionMetrics()
      .ifPresent(metrics -> metrics.incrMetaCacheNumClearRegion(locator.getCacheSize()));
  }

  void clearCache() {
    table2Locator.clear();
  }

  void clearCache(ServerName serverName) {
    for (AbstractAsyncTableRegionLocator locator : table2Locator.values()) {
      locator.clearCache(serverName);
    }
  }

  // only used for testing whether we have cached the location for a region.
  @VisibleForTesting
  RegionLocations getRegionLocationInCache(TableName tableName, byte[] row) {
    AbstractAsyncTableRegionLocator locator = table2Locator.get(tableName);
    if (locator == null) {
      return null;
    }
    return locator.locateInCache(row);
  }

  // only used for testing whether we have cached the location for a table.
  @VisibleForTesting
  int getNumberOfCachedRegionLocations(TableName tableName) {
    AbstractAsyncTableRegionLocator locator = table2Locator.get(tableName);
    if (locator == null) {
      return 0;
    }
    return locator.getNumberOfCachedRegionLocations();
  }
}
