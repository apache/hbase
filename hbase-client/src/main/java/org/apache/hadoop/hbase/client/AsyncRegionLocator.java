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

import static org.apache.hadoop.hbase.TableName.META_TABLE_NAME;
import static org.apache.hadoop.hbase.trace.TraceUtil.REGION_NAMES_KEY;
import static org.apache.hadoop.hbase.trace.TraceUtil.SERVER_NAME_KEY;
import static org.apache.hadoop.hbase.trace.TraceUtil.createSpan;
import static org.apache.hadoop.hbase.trace.TraceUtil.createTableSpan;
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.io.netty.util.HashedWheelTimer;
import org.apache.hbase.thirdparty.io.netty.util.Timeout;

/**
 * The asynchronous region locator.
 */
@InterfaceAudience.Private
class AsyncRegionLocator {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncRegionLocator.class);

  private final HashedWheelTimer retryTimer;

  private final AsyncConnectionImpl conn;

  private final AsyncMetaRegionLocator metaRegionLocator;

  private final AsyncNonMetaRegionLocator nonMetaRegionLocator;

  AsyncRegionLocator(AsyncConnectionImpl conn, HashedWheelTimer retryTimer) {
    this.conn = conn;
    this.metaRegionLocator = new AsyncMetaRegionLocator(conn.registry);
    this.nonMetaRegionLocator = new AsyncNonMetaRegionLocator(conn);
    this.retryTimer = retryTimer;
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
    Span span = createTableSpan(getClass().getSimpleName() + "." + methodName, tableName);
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

  CompletableFuture<RegionLocations> getRegionLocations(TableName tableName, byte[] row,
    RegionLocateType type, boolean reload, long timeoutNs) {
    return tracedLocationFuture(() -> {
      CompletableFuture<RegionLocations> future = isMeta(tableName) ?
        metaRegionLocator.getRegionLocations(RegionReplicaUtil.DEFAULT_REPLICA_ID, reload) :
        nonMetaRegionLocator.getRegionLocations(tableName, row,
          RegionReplicaUtil.DEFAULT_REPLICA_ID, type, reload);
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
        isMeta(tableName) ? metaRegionLocator.getRegionLocations(replicaId, reload) :
          nonMetaRegionLocator.getRegionLocations(tableName, row, replicaId, type, reload);
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

  void updateCachedLocationOnError(HRegionLocation loc, Throwable exception) {
    if (loc.getRegion().isMetaRegion()) {
      metaRegionLocator.updateCachedLocationOnError(loc, exception);
    } else {
      nonMetaRegionLocator.updateCachedLocationOnError(loc, exception);
    }
  }

  void clearCache(TableName tableName) {
    TraceUtil.trace(() -> {
      LOG.debug("Clear meta cache for {}", tableName);
      if (tableName.equals(META_TABLE_NAME)) {
        metaRegionLocator.clearCache();
      } else {
        nonMetaRegionLocator.clearCache(tableName);
      }
    }, () -> createTableSpan("AsyncRegionLocator.clearCache", tableName));
  }

  void clearCache(ServerName serverName) {
    TraceUtil.trace(() -> {
      LOG.debug("Clear meta cache for {}", serverName);
      metaRegionLocator.clearCache(serverName);
      nonMetaRegionLocator.clearCache(serverName);
      conn.getConnectionMetrics().ifPresent(MetricsConnection::incrMetaCacheNumClearServer);
    }, () -> createSpan("AsyncRegionLocator.clearCache").setAttribute(SERVER_NAME_KEY,
      serverName.getServerName()));
  }

  void clearCache() {
    TraceUtil.trace(() -> {
      metaRegionLocator.clearCache();
      nonMetaRegionLocator.clearCache();
    }, "AsyncRegionLocator.clearCache");
  }

  AsyncNonMetaRegionLocator getNonMetaRegionLocator() {
    return nonMetaRegionLocator;
  }

  // only used for testing whether we have cached the location for a region.
  RegionLocations getRegionLocationInCache(TableName tableName, byte[] row) {
    if (TableName.isMetaTableName(tableName)) {
      return metaRegionLocator.getRegionLocationInCache();
    } else {
      return nonMetaRegionLocator.getRegionLocationInCache(tableName, row);
    }
  }

  // only used for testing whether we have cached the location for a table.
  int getNumberOfCachedRegionLocations(TableName tableName) {
    if (TableName.isMetaTableName(tableName)) {
      return metaRegionLocator.getNumberOfCachedRegionLocations();
    } else {
      return nonMetaRegionLocator.getNumberOfCachedRegionLocations(tableName);
    }
  }
}
