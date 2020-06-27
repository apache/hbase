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

import static org.apache.hadoop.hbase.client.AsyncRegionLocatorHelper.createRegionLocations;
import static org.apache.hadoop.hbase.trace.TraceUtil.REGION_NAMES_KEY;
import static org.apache.hadoop.hbase.trace.TraceUtil.SERVER_NAME_KEY;
import static org.apache.hadoop.hbase.trace.TraceUtil.createSpan;
import static org.apache.hadoop.hbase.trace.TraceUtil.createTableSpan;
import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import com.google.errorprone.annotations.RestrictedApi;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.util.HashedWheelTimer;
import org.apache.hbase.thirdparty.io.netty.util.Timeout;

/**
 * The asynchronous region locator.
 */
@InterfaceAudience.Private
class AsyncRegionLocator {

  static final String MAX_CONCURRENT_LOCATE_REQUEST_PER_TABLE =
    "hbase.client.meta.max.concurrent.locate.per.table";

  private static final int DEFAULT_MAX_CONCURRENT_LOCATE_REQUEST_PER_TABLE = 8;

  static final String MAX_CONCURRENT_LOCATE_META_REQUEST =
    "hbase.client.meta.max.concurrent.locate";

  static String LOCATE_PREFETCH_LIMIT = "hbase.client.locate.prefetch.limit";

  private static final int DEFAULT_LOCATE_PREFETCH_LIMIT = 10;

  private final HashedWheelTimer retryTimer;

  private final AsyncConnectionImpl conn;

  private final int maxConcurrentLocateRequestPerTable;

  private final int maxConcurrentLocateMetaRequest;

  private final int locatePrefetchLimit;

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

  private <T> CompletableFuture<T> tracedLocationFuture(Supplier<CompletableFuture<T>> action,
    Function<T, List<String>> getRegionNames, TableName tableName, String methodName) {
    Span span = createTableSpan("AsyncRegionLocator." + methodName, tableName);
    try (Scope scope = span.makeCurrent()) {
      CompletableFuture<T> future = action.get();
      FutureUtils.addListener(future, (resp, error) -> {
        if (error != null) {
          TraceUtil.setError(span, error);
        } else {
          List<String> regionNames = getRegionNames.apply(resp);
          if (!regionNames.isEmpty()) {
            span.setAttribute(REGION_NAMES_KEY, regionNames);
          }
          span.setStatus(StatusCode.OK);
        }
        span.end();
      });
      return future;
    }
  }

  private List<String> getRegionName(RegionLocations locs) {
    List<String> names = new ArrayList<>();
    for (HRegionLocation loc : locs.getRegionLocations()) {
      if (loc != null) {
        names.add(loc.getRegion().getRegionNameAsString());
      }
    }
    return names;
  }

  private AbstractAsyncTableRegionLocator getOrCreateTableRegionLocator(TableName tableName) {
    return computeIfAbsent(table2Locator, tableName, () -> {
      if (isMeta(tableName)) {
        return new AsyncMetaTableRegionLocator(conn, tableName, maxConcurrentLocateMetaRequest);
      } else {
        return new AsyncNonMetaTableRegionLocator(conn, tableName,
          maxConcurrentLocateRequestPerTable, locatePrefetchLimit);
      }
    });
  }

  CompletableFuture<RegionLocations> getRegionLocations(TableName tableName, byte[] row,
    int replicaId, RegionLocateType locateType, boolean reload) {
    return tracedLocationFuture(() -> {
      return getOrCreateTableRegionLocator(tableName).getRegionLocations(row, replicaId, locateType,
        reload);
    }, this::getRegionName, tableName, "getRegionLocations");
  }

  CompletableFuture<RegionLocations> getRegionLocations(TableName tableName, byte[] row,
    RegionLocateType type, boolean reload, long timeoutNs) {
    return tracedLocationFuture(() -> {
      CompletableFuture<RegionLocations> future =
        getRegionLocations(tableName, row, RegionReplicaUtil.DEFAULT_REPLICA_ID, type, reload);
      return withTimeout(future, timeoutNs,
        () -> "Timeout(" + TimeUnit.NANOSECONDS.toMillis(timeoutNs) +
          "ms) waiting for region locations for " + tableName + ", row='" +
          Bytes.toStringBinary(row) + "'");
    }, this::getRegionName, tableName, "getRegionLocations");
  }

  CompletableFuture<HRegionLocation> getRegionLocation(TableName tableName, byte[] row,
    int replicaId, RegionLocateType type, boolean reload, long timeoutNs) {
    return tracedLocationFuture(() -> {
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
          "ms) waiting for region location for " + tableName + ", row='" +
          Bytes.toStringBinary(row) + "', replicaId=" + replicaId);
    }, loc -> Arrays.asList(loc.getRegion().getRegionNameAsString()), tableName,
      "getRegionLocation");
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

  void clearCache(TableName tableName) {
    TraceUtil.trace(() -> {
    AbstractAsyncTableRegionLocator locator = table2Locator.remove(tableName);
    if (locator == null) {
      return;
    }
    locator.clearPendingRequests();
    conn.getConnectionMetrics()
      .ifPresent(metrics -> metrics.incrMetaCacheNumClearRegion(locator.getCacheSize()));
    }, () -> createTableSpan("AsyncRegionLocator.clearCache", tableName));
  }

  void clearCache(ServerName serverName) {
    TraceUtil.trace(() -> {
      for (AbstractAsyncTableRegionLocator locator : table2Locator.values()) {
        locator.clearCache(serverName);
      }
      conn.getConnectionMetrics().ifPresent(MetricsConnection::incrMetaCacheNumClearServer);
    }, () -> createSpan("AsyncRegionLocator.clearCache").setAttribute(SERVER_NAME_KEY,
      serverName.getServerName()));
  }

  void clearCache() {
    TraceUtil.trace(() -> {
      table2Locator.clear();
    }, "AsyncRegionLocator.clearCache");
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

  // only used for testing whether we have cached the location for a region.
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*/src/test/.*")
  RegionLocations getRegionLocationInCache(TableName tableName, byte[] row) {
    AbstractAsyncTableRegionLocator locator = table2Locator.get(tableName);
    if (locator == null) {
      return null;
    }
    return locator.locateInCache(row);
  }

  // only used for testing whether we have cached the location for a table.
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*/src/test/.*")
  int getNumberOfCachedRegionLocations(TableName tableName) {
    AbstractAsyncTableRegionLocator locator = table2Locator.get(tableName);
    if (locator == null) {
      return 0;
    }
    return locator.getNumberOfCachedRegionLocations();
  }
}
