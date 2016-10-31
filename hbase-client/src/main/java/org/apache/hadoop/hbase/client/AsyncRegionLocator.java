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

import static org.apache.hadoop.hbase.HConstants.CATALOG_FAMILY;
import static org.apache.hadoop.hbase.HConstants.NINES;
import static org.apache.hadoop.hbase.HConstants.ZEROES;
import static org.apache.hadoop.hbase.HRegionInfo.createRegionName;
import static org.apache.hadoop.hbase.TableName.META_TABLE_NAME;
import static org.apache.hadoop.hbase.client.ConnectionUtils.isEmptyStopRow;
import static org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil.findException;
import static org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil.isMetaClearingException;
import static org.apache.hadoop.hbase.util.CollectionUtils.computeIfAbsent;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The asynchronous region locator.
 */
@InterfaceAudience.Private
class AsyncRegionLocator {

  private static final Log LOG = LogFactory.getLog(AsyncRegionLocator.class);

  private final AsyncConnectionImpl conn;

  private final AtomicReference<HRegionLocation> metaRegionLocation = new AtomicReference<>();

  private final AtomicReference<CompletableFuture<HRegionLocation>> metaRelocateFuture =
      new AtomicReference<>();

  private final ConcurrentMap<TableName, ConcurrentNavigableMap<byte[], HRegionLocation>> cache =
      new ConcurrentHashMap<>();

  AsyncRegionLocator(AsyncConnectionImpl conn) {
    this.conn = conn;
  }

  private CompletableFuture<HRegionLocation> locateMetaRegion() {
    for (;;) {
      HRegionLocation metaRegionLocation = this.metaRegionLocation.get();
      if (metaRegionLocation != null) {
        return CompletableFuture.completedFuture(metaRegionLocation);
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("Meta region location cache is null, try fetching from registry.");
      }
      if (metaRelocateFuture.compareAndSet(null, new CompletableFuture<>())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Start fetching meta region location from registry.");
        }
        CompletableFuture<HRegionLocation> future = metaRelocateFuture.get();
        conn.registry.getMetaRegionLocation().whenComplete((locs, error) -> {
          if (error != null) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Failed to fetch meta region location from registry", error);
            }
            metaRelocateFuture.getAndSet(null).completeExceptionally(error);
            return;
          }
          HRegionLocation loc = locs.getDefaultRegionLocation();
          if (LOG.isDebugEnabled()) {
            LOG.debug("The fetched meta region location is " + loc);
          }
          // Here we update cache before reset future, so it is possible that someone can get a
          // stale value. Consider this:
          // 1. update cache
          // 2. someone clear the cache and relocate again
          // 3. the metaRelocateFuture is not null so the old future is used.
          // 4. we clear metaRelocateFuture and complete the future in it with the value being
          // cleared in step 2.
          // But we do not think it is a big deal as it rarely happens, and even if it happens, the
          // caller will retry again later, no correctness problems.
          this.metaRegionLocation.set(loc);
          metaRelocateFuture.set(null);
          future.complete(loc);
        });
      } else {
        CompletableFuture<HRegionLocation> future = metaRelocateFuture.get();
        if (future != null) {
          return future;
        }
      }
    }
  }

  private static ConcurrentNavigableMap<byte[], HRegionLocation> createTableCache() {
    return new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);
  }

  private void removeFromCache(HRegionLocation loc) {
    ConcurrentNavigableMap<byte[], HRegionLocation> tableCache =
        cache.get(loc.getRegionInfo().getTable());
    if (tableCache == null) {
      return;
    }
    tableCache.computeIfPresent(loc.getRegionInfo().getStartKey(), (k, oldLoc) -> {
      if (oldLoc.getSeqNum() > loc.getSeqNum()
          || !oldLoc.getServerName().equals(loc.getServerName())) {
        return oldLoc;
      }
      return null;
    });
  }

  private void addToCache(HRegionLocation loc) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Try adding " + loc + " to cache");
    }
    ConcurrentNavigableMap<byte[], HRegionLocation> tableCache = computeIfAbsent(cache,
      loc.getRegionInfo().getTable(), AsyncRegionLocator::createTableCache);
    byte[] startKey = loc.getRegionInfo().getStartKey();
    HRegionLocation oldLoc = tableCache.putIfAbsent(startKey, loc);
    if (oldLoc == null) {
      return;
    }
    if (oldLoc.getSeqNum() > loc.getSeqNum()
        || oldLoc.getServerName().equals(loc.getServerName())) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Will not add " + loc + " to cache because the old value " + oldLoc
            + " is newer than us or has the same server name");
      }
      return;
    }
    tableCache.compute(startKey, (k, oldValue) -> {
      if (oldValue == null || oldValue.getSeqNum() <= loc.getSeqNum()) {
        return loc;
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("Will not add " + loc + " to cache because the old value " + oldValue
            + " is newer than us or has the same server name."
            + " Maybe it is updated before we replace it");
      }
      return oldValue;
    });
  }

  private HRegionLocation locateInCache(TableName tableName, byte[] row) {
    ConcurrentNavigableMap<byte[], HRegionLocation> tableCache = cache.get(tableName);
    if (tableCache == null) {
      return null;
    }
    Map.Entry<byte[], HRegionLocation> entry = tableCache.floorEntry(row);
    if (entry == null) {
      return null;
    }
    HRegionLocation loc = entry.getValue();
    byte[] endKey = loc.getRegionInfo().getEndKey();
    if (isEmptyStopRow(endKey) || Bytes.compareTo(row, endKey) < 0) {
      return loc;
    } else {
      return null;
    }
  }

  private void onScanComplete(CompletableFuture<HRegionLocation> future, TableName tableName,
      byte[] row, List<Result> results, Throwable error, String rowNameInErrorMsg,
      Consumer<HRegionLocation> otherCheck) {
    if (error != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to fetch location of '" + tableName + "', " + rowNameInErrorMsg + "='"
            + Bytes.toStringBinary(row) + "'",
          error);
      }
      future.completeExceptionally(error);
      return;
    }
    if (results.isEmpty()) {
      future.completeExceptionally(new TableNotFoundException(tableName));
      return;
    }
    RegionLocations locs = MetaTableAccessor.getRegionLocations(results.get(0));
    if (LOG.isDebugEnabled()) {
      LOG.debug("The fetched location of '" + tableName + "', " + rowNameInErrorMsg + "='"
          + Bytes.toStringBinary(row) + "' is " + locs);
    }
    if (locs == null || locs.getDefaultRegionLocation() == null) {
      future.completeExceptionally(
        new IOException(String.format("No location found for '%s', %s='%s'", tableName,
          rowNameInErrorMsg, Bytes.toStringBinary(row))));
      return;
    }
    HRegionLocation loc = locs.getDefaultRegionLocation();
    HRegionInfo info = loc.getRegionInfo();
    if (info == null) {
      future.completeExceptionally(
        new IOException(String.format("HRegionInfo is null for '%s', %s='%s'", tableName,
          rowNameInErrorMsg, Bytes.toStringBinary(row))));
      return;
    }
    if (!info.getTable().equals(tableName)) {
      future.completeExceptionally(new TableNotFoundException(
          "Table '" + tableName + "' was not found, got: '" + info.getTable() + "'"));
      return;
    }
    if (info.isSplit()) {
      future.completeExceptionally(new RegionOfflineException(
          "the only available region for the required row is a split parent,"
              + " the daughters should be online soon: '" + info.getRegionNameAsString() + "'"));
      return;
    }
    if (info.isOffline()) {
      future.completeExceptionally(new RegionOfflineException("the region is offline, could"
          + " be caused by a disable table call: '" + info.getRegionNameAsString() + "'"));
      return;
    }
    if (loc.getServerName() == null) {
      future.completeExceptionally(new NoServerForRegionException(
          String.format("No server address listed for region '%s', %s='%s'",
            info.getRegionNameAsString(), rowNameInErrorMsg, Bytes.toStringBinary(row))));
      return;
    }
    otherCheck.accept(loc);
    if (future.isDone()) {
      return;
    }
    addToCache(loc);
    future.complete(loc);
  }

  private CompletableFuture<HRegionLocation> locateInMeta(TableName tableName, byte[] row) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Try locate '" + tableName + "', row='" + Bytes.toStringBinary(row) + "' in meta");
    }
    CompletableFuture<HRegionLocation> future = new CompletableFuture<>();
    byte[] metaKey = createRegionName(tableName, row, NINES, false);
    conn.getTable(META_TABLE_NAME)
        .smallScan(new Scan(metaKey).setReversed(true).setSmall(true).addFamily(CATALOG_FAMILY), 1)
        .whenComplete(
          (results, error) -> onScanComplete(future, tableName, row, results, error, "row", loc -> {
          }));
    return future;
  }

  private CompletableFuture<HRegionLocation> locateRegion(TableName tableName, byte[] row) {
    HRegionLocation loc = locateInCache(tableName, row);
    if (loc != null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Found " + loc + " in cache for '" + tableName + "', row='"
            + Bytes.toStringBinary(row) + "'");
      }
      return CompletableFuture.completedFuture(loc);
    }
    return locateInMeta(tableName, row);
  }

  CompletableFuture<HRegionLocation> getRegionLocation(TableName tableName, byte[] row) {
    if (tableName.equals(META_TABLE_NAME)) {
      return locateMetaRegion();
    } else {
      return locateRegion(tableName, row);
    }
  }

  private HRegionLocation locatePreviousInCache(TableName tableName,
      byte[] startRowOfCurrentRegion) {
    ConcurrentNavigableMap<byte[], HRegionLocation> tableCache = cache.get(tableName);
    if (tableCache == null) {
      return null;
    }
    Map.Entry<byte[], HRegionLocation> entry;
    if (isEmptyStopRow(startRowOfCurrentRegion)) {
      entry = tableCache.lastEntry();
    } else {
      entry = tableCache.lowerEntry(startRowOfCurrentRegion);
    }
    if (entry == null) {
      return null;
    }
    HRegionLocation loc = entry.getValue();
    if (Bytes.equals(loc.getRegionInfo().getEndKey(), startRowOfCurrentRegion)) {
      return loc;
    } else {
      return null;
    }
  }

  private CompletableFuture<HRegionLocation> locatePreviousInMeta(TableName tableName,
      byte[] startRowOfCurrentRegion) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Try locate '" + tableName + "', startRowOfCurrentRegion='"
          + Bytes.toStringBinary(startRowOfCurrentRegion) + "' in meta");
    }
    byte[] metaKey;
    if (isEmptyStopRow(startRowOfCurrentRegion)) {
      byte[] binaryTableName = tableName.getName();
      metaKey = Arrays.copyOf(binaryTableName, binaryTableName.length + 1);
    } else {
      metaKey = createRegionName(tableName, startRowOfCurrentRegion, ZEROES, false);
    }
    CompletableFuture<HRegionLocation> future = new CompletableFuture<>();
    conn.getTable(META_TABLE_NAME)
        .smallScan(new Scan(metaKey).setReversed(true).setSmall(true).addFamily(CATALOG_FAMILY), 1)
        .whenComplete((results, error) -> onScanComplete(future, tableName, startRowOfCurrentRegion,
          results, error, "startRowOfCurrentRegion", loc -> {
            HRegionInfo info = loc.getRegionInfo();
            if (!Bytes.equals(info.getEndKey(), startRowOfCurrentRegion)) {
              future.completeExceptionally(new IOException("The end key of '"
                  + info.getRegionNameAsString() + "' is '" + Bytes.toStringBinary(info.getEndKey())
                  + "', expected '" + Bytes.toStringBinary(startRowOfCurrentRegion) + "'"));
            }
          }));
    return future;
  }

  private CompletableFuture<HRegionLocation> locatePreviousRegion(TableName tableName,
      byte[] startRowOfCurrentRegion) {
    HRegionLocation loc = locatePreviousInCache(tableName, startRowOfCurrentRegion);
    if (loc != null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Found " + loc + " in cache for '" + tableName + "', startRowOfCurrentRegion='"
            + Bytes.toStringBinary(startRowOfCurrentRegion) + "'");
      }
      return CompletableFuture.completedFuture(loc);
    }
    return locatePreviousInMeta(tableName, startRowOfCurrentRegion);
  }

  /**
   * Locate the previous region using the current regions start key. Used for reverse scan.
   */
  CompletableFuture<HRegionLocation> getPreviousRegionLocation(TableName tableName,
      byte[] startRowOfCurrentRegion) {
    if (tableName.equals(META_TABLE_NAME)) {
      return locateMetaRegion();
    } else {
      return locatePreviousRegion(tableName, startRowOfCurrentRegion);
    }
  }

  private boolean canUpdate(HRegionLocation loc, HRegionLocation oldLoc) {
    // Do not need to update if no such location, or the location is newer, or the location is not
    // same with us
    return oldLoc != null && oldLoc.getSeqNum() <= loc.getSeqNum()
        && oldLoc.getServerName().equals(loc.getServerName());
  }

  private void updateCachedLoation(HRegionLocation loc, Throwable exception,
      Function<HRegionLocation, HRegionLocation> cachedLocationSupplier,
      Consumer<HRegionLocation> addToCache, Consumer<HRegionLocation> removeFromCache) {
    HRegionLocation oldLoc = cachedLocationSupplier.apply(loc);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Try updating " + loc + ", the old value is " + oldLoc, exception);
    }
    if (!canUpdate(loc, oldLoc)) {
      return;
    }
    Throwable cause = findException(exception);
    if (LOG.isDebugEnabled()) {
      LOG.debug("The actual exception when updating " + loc, cause);
    }
    if (cause == null || !isMetaClearingException(cause)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "Will not update " + loc + " because the exception is null or not the one we care about");
      }
      return;
    }
    if (cause instanceof RegionMovedException) {
      RegionMovedException rme = (RegionMovedException) cause;
      HRegionLocation newLoc =
          new HRegionLocation(loc.getRegionInfo(), rme.getServerName(), rme.getLocationSeqNum());
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "Try updating " + loc + " with the new location " + newLoc + " constructed by " + rme);
      }
      addToCache.accept(newLoc);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Try removing " + loc + " from cache");
      }
      removeFromCache.accept(loc);
    }
  }

  void updateCachedLocation(HRegionLocation loc, Throwable exception) {
    if (loc.getRegionInfo().isMetaTable()) {
      updateCachedLoation(loc, exception, l -> metaRegionLocation.get(), newLoc -> {
        for (;;) {
          HRegionLocation oldLoc = metaRegionLocation.get();
          if (oldLoc != null && (oldLoc.getSeqNum() > newLoc.getSeqNum()
              || oldLoc.getServerName().equals(newLoc.getServerName()))) {
            return;
          }
          if (metaRegionLocation.compareAndSet(oldLoc, newLoc)) {
            return;
          }
        }
      }, l -> {
        for (;;) {
          HRegionLocation oldLoc = metaRegionLocation.get();
          if (!canUpdate(l, oldLoc) || metaRegionLocation.compareAndSet(oldLoc, null)) {
            return;
          }
        }
      });
    } else {
      updateCachedLoation(loc, exception, l -> {
        ConcurrentNavigableMap<byte[], HRegionLocation> tableCache =
            cache.get(l.getRegionInfo().getTable());
        if (tableCache == null) {
          return null;
        }
        return tableCache.get(l.getRegionInfo().getStartKey());
      }, this::addToCache, this::removeFromCache);
    }
  }

  void clearCache(TableName tableName) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Clear meta cache for " + tableName);
    }
    cache.remove(tableName);
  }
}
