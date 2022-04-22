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
import static org.apache.hadoop.hbase.HConstants.EMPTY_END_ROW;
import static org.apache.hadoop.hbase.HConstants.NINES;
import static org.apache.hadoop.hbase.HConstants.USE_META_REPLICAS;
import static org.apache.hadoop.hbase.HConstants.ZEROES;
import static org.apache.hadoop.hbase.TableName.META_TABLE_NAME;
import static org.apache.hadoop.hbase.client.AsyncRegionLocatorHelper.canUpdateOnError;
import static org.apache.hadoop.hbase.client.AsyncRegionLocatorHelper.createRegionLocations;
import static org.apache.hadoop.hbase.client.AsyncRegionLocatorHelper.isGood;
import static org.apache.hadoop.hbase.client.AsyncRegionLocatorHelper.removeRegionLocation;
import static org.apache.hadoop.hbase.client.ConnectionUtils.createClosestRowAfter;
import static org.apache.hadoop.hbase.client.ConnectionUtils.isEmptyStopRow;
import static org.apache.hadoop.hbase.client.RegionInfo.createRegionName;
import static org.apache.hadoop.hbase.client.RegionLocator.LOCATOR_META_REPLICAS_MODE;
import static org.apache.hadoop.hbase.util.Bytes.BYTES_COMPARATOR;
import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.base.Objects;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The asynchronous locator for regions other than meta.
 */
@InterfaceAudience.Private
class AsyncNonMetaRegionLocator {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncNonMetaRegionLocator.class);

  static final String MAX_CONCURRENT_LOCATE_REQUEST_PER_TABLE =
    "hbase.client.meta.max.concurrent.locate.per.table";

  private static final int DEFAULT_MAX_CONCURRENT_LOCATE_REQUEST_PER_TABLE = 8;

  static String LOCATE_PREFETCH_LIMIT = "hbase.client.locate.prefetch.limit";

  private static final int DEFAULT_LOCATE_PREFETCH_LIMIT = 10;

  private final AsyncConnectionImpl conn;

  private final int maxConcurrentLocateRequestPerTable;

  private final int locatePrefetchLimit;

  // The mode tells if HedgedRead, LoadBalance mode is supported.
  // The default mode is CatalogReplicaMode.None.
  private CatalogReplicaMode metaReplicaMode;
  private CatalogReplicaLoadBalanceSelector metaReplicaSelector;

  private final ConcurrentMap<TableName, TableCache> cache = new ConcurrentHashMap<>();

  private static final class LocateRequest {

    private final byte[] row;

    private final RegionLocateType locateType;

    public LocateRequest(byte[] row, RegionLocateType locateType) {
      this.row = row;
      this.locateType = locateType;
    }

    @Override
    public int hashCode() {
      return Bytes.hashCode(row) ^ locateType.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || obj.getClass() != LocateRequest.class) {
        return false;
      }
      LocateRequest that = (LocateRequest) obj;
      return locateType.equals(that.locateType) && Bytes.equals(row, that.row);
    }
  }

  private static final class TableCache {

    private final ConcurrentNavigableMap<byte[], RegionLocations> cache =
      new ConcurrentSkipListMap<>(BYTES_COMPARATOR);

    private final Set<LocateRequest> pendingRequests = new HashSet<>();

    private final Map<LocateRequest, CompletableFuture<RegionLocations>> allRequests =
      new LinkedHashMap<>();

    public boolean hasQuota(int max) {
      return pendingRequests.size() < max;
    }

    public boolean isPending(LocateRequest req) {
      return pendingRequests.contains(req);
    }

    public void send(LocateRequest req) {
      pendingRequests.add(req);
    }

    public Optional<LocateRequest> getCandidate() {
      return allRequests.keySet().stream().filter(r -> !isPending(r)).findFirst();
    }

    public void clearCompletedRequests(RegionLocations locations) {
      for (Iterator<Map.Entry<LocateRequest, CompletableFuture<RegionLocations>>> iter =
        allRequests.entrySet().iterator(); iter.hasNext();) {
        Map.Entry<LocateRequest, CompletableFuture<RegionLocations>> entry = iter.next();
        if (tryComplete(entry.getKey(), entry.getValue(), locations)) {
          iter.remove();
        }
      }
    }

    private boolean tryComplete(LocateRequest req, CompletableFuture<RegionLocations> future,
        RegionLocations locations) {
      if (future.isDone()) {
        return true;
      }
      if (locations == null) {
        return false;
      }
      HRegionLocation loc = ObjectUtils.firstNonNull(locations.getRegionLocations());
      // we should at least have one location available, otherwise the request should fail and
      // should not arrive here
      assert loc != null;
      boolean completed;
      if (req.locateType.equals(RegionLocateType.BEFORE)) {
        // for locating the row before current row, the common case is to find the previous region
        // in reverse scan, so we check the endKey first. In general, the condition should be
        // startKey < req.row and endKey >= req.row. Here we split it to endKey == req.row ||
        // (endKey > req.row && startKey < req.row). The two conditions are equal since startKey <
        // endKey.
        byte[] endKey = loc.getRegion().getEndKey();
        int c = Bytes.compareTo(endKey, req.row);
        completed = c == 0 || ((c > 0 || Bytes.equals(EMPTY_END_ROW, endKey)) &&
          Bytes.compareTo(loc.getRegion().getStartKey(), req.row) < 0);
      } else {
        completed = loc.getRegion().containsRow(req.row);
      }
      if (completed) {
        future.complete(locations);
        return true;
      } else {
        return false;
      }
    }
  }

  AsyncNonMetaRegionLocator(AsyncConnectionImpl conn) {
    this.conn = conn;
    this.maxConcurrentLocateRequestPerTable = conn.getConfiguration().getInt(
      MAX_CONCURRENT_LOCATE_REQUEST_PER_TABLE, DEFAULT_MAX_CONCURRENT_LOCATE_REQUEST_PER_TABLE);
    this.locatePrefetchLimit =
      conn.getConfiguration().getInt(LOCATE_PREFETCH_LIMIT, DEFAULT_LOCATE_PREFETCH_LIMIT);

    // Get the region locator's meta replica mode.
    this.metaReplicaMode = CatalogReplicaMode.fromString(conn.getConfiguration()
      .get(LOCATOR_META_REPLICAS_MODE, CatalogReplicaMode.NONE.toString()));

    switch (this.metaReplicaMode) {
      case LOAD_BALANCE:
        String replicaSelectorClass = conn.getConfiguration().
          get(RegionLocator.LOCATOR_META_REPLICAS_MODE_LOADBALANCE_SELECTOR,
          CatalogReplicaLoadBalanceSimpleSelector.class.getName());

        this.metaReplicaSelector = CatalogReplicaLoadBalanceSelectorFactory.createSelector(
          replicaSelectorClass, META_TABLE_NAME, conn.getChoreService(), () -> {
            int numOfReplicas = CatalogReplicaLoadBalanceSelector.UNINITIALIZED_NUM_OF_REPLICAS;
            try {
              RegionLocations metaLocations = conn.registry.getMetaRegionLocations().get(
                conn.connConf.getReadRpcTimeoutNs(), TimeUnit.NANOSECONDS);
              numOfReplicas = metaLocations.size();
            } catch (Exception e) {
              LOG.error("Failed to get table {}'s region replication, ", META_TABLE_NAME, e);
            }
            return numOfReplicas;
          });
        break;
      case NONE:
        // If user does not configure LOCATOR_META_REPLICAS_MODE, let's check the legacy config.

        boolean useMetaReplicas = conn.getConfiguration().getBoolean(USE_META_REPLICAS,
          DEFAULT_USE_META_REPLICAS);
        if (useMetaReplicas) {
          this.metaReplicaMode = CatalogReplicaMode.HEDGED_READ;
        }
        break;
      default:
        // Doing nothing
    }
  }

  private TableCache getTableCache(TableName tableName) {
    return computeIfAbsent(cache, tableName, TableCache::new);
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

  // if we successfully add the locations to cache, return the locations, otherwise return the one
  // which prevents us being added. The upper layer can use this value to complete pending requests.
  private RegionLocations addToCache(TableCache tableCache, RegionLocations locs) {
    LOG.trace("Try adding {} to cache", locs);
    byte[] startKey = locs.getRegionLocation().getRegion().getStartKey();
    for (;;) {
      RegionLocations oldLocs = tableCache.cache.putIfAbsent(startKey, locs);
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
        if (tableCache.cache.replace(startKey, oldLocs, mergedLocs)) {
          return mergedLocs;
        }
      } else {
        // the region is different, here we trust the one we fetched. This maybe wrong but finally
        // the upper layer can detect this and trigger removal of the wrong locations
        if (LOG.isDebugEnabled()) {
          LOG.debug("The newnly fetch region {} is different from the old one {} for row '{}'," +
            " try replaing the old one...", region, oldRegion, Bytes.toStringBinary(startKey));
        }
        if (tableCache.cache.replace(startKey, oldLocs, locs)) {
          return locs;
        }
      }
    }
  }

  private void complete(TableName tableName, LocateRequest req, RegionLocations locs,
      Throwable error) {
    if (error != null) {
      LOG.warn("Failed to locate region in '" + tableName + "', row='" +
        Bytes.toStringBinary(req.row) + "', locateType=" + req.locateType, error);
    }
    Optional<LocateRequest> toSend = Optional.empty();
    TableCache tableCache = getTableCache(tableName);
    if (locs != null) {
      RegionLocations addedLocs = addToCache(tableCache, locs);
      synchronized (tableCache) {
        tableCache.pendingRequests.remove(req);
        tableCache.clearCompletedRequests(addedLocs);
        // Remove a complete locate request in a synchronized block, so the table cache must have
        // quota to send a candidate request.
        toSend = tableCache.getCandidate();
        toSend.ifPresent(r -> tableCache.send(r));
      }
      toSend.ifPresent(r -> locateInMeta(tableName, r));
    } else {
      // we meet an error
      assert error != null;
      synchronized (tableCache) {
        tableCache.pendingRequests.remove(req);
        // fail the request itself, no matter whether it is a DoNotRetryIOException, as we have
        // already retried several times
        CompletableFuture<?> future = tableCache.allRequests.remove(req);
        if (future != null) {
          future.completeExceptionally(error);
        }
        tableCache.clearCompletedRequests(null);
        // Remove a complete locate request in a synchronized block, so the table cache must have
        // quota to send a candidate request.
        toSend = tableCache.getCandidate();
        toSend.ifPresent(r -> tableCache.send(r));
      }
      toSend.ifPresent(r -> locateInMeta(tableName, r));
    }
  }

  // return whether we should stop the scan
  private boolean onScanNext(TableName tableName, LocateRequest req, Result result) {
    RegionLocations locs = MetaTableAccessor.getRegionLocations(result);
    if (LOG.isDebugEnabled()) {
      LOG.debug("The fetched location of '{}', row='{}', locateType={} is {}", tableName,
        Bytes.toStringBinary(req.row), req.locateType, locs);
    }
    // remove HRegionLocation with null location, i.e, getServerName returns null.
    if (locs != null) {
      locs = locs.removeElementsWithNullLocation();
    }

    // the default region location should always be presented when fetching from meta, otherwise
    // let's fail the request.
    if (locs == null || locs.getDefaultRegionLocation() == null) {
      complete(tableName, req, null,
        new HBaseIOException(String.format("No location found for '%s', row='%s', locateType=%s",
          tableName, Bytes.toStringBinary(req.row), req.locateType)));
      return true;
    }
    HRegionLocation loc = locs.getDefaultRegionLocation();
    RegionInfo info = loc.getRegion();
    if (info == null) {
      complete(tableName, req, null,
        new HBaseIOException(String.format("HRegionInfo is null for '%s', row='%s', locateType=%s",
          tableName, Bytes.toStringBinary(req.row), req.locateType)));
      return true;
    }
    if (info.isSplitParent()) {
      return false;
    }
    complete(tableName, req, locs, null);
    return true;
  }

  private void recordCacheHit() {
    conn.getConnectionMetrics().ifPresent(MetricsConnection::incrMetaCacheHit);
  }

  private void recordCacheMiss() {
    conn.getConnectionMetrics().ifPresent(MetricsConnection::incrMetaCacheMiss);
  }

  private RegionLocations locateRowInCache(TableCache tableCache, TableName tableName, byte[] row,
      int replicaId) {
    Map.Entry<byte[], RegionLocations> entry = tableCache.cache.floorEntry(row);
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

  private RegionLocations locateRowBeforeInCache(TableCache tableCache, TableName tableName,
      byte[] row, int replicaId) {
    boolean isEmptyStopRow = isEmptyStopRow(row);
    Map.Entry<byte[], RegionLocations> entry =
      isEmptyStopRow ? tableCache.cache.lastEntry() : tableCache.cache.lowerEntry(row);
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

  private void locateInMeta(TableName tableName, LocateRequest req) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Try locate '" + tableName + "', row='" + Bytes.toStringBinary(req.row) +
        "', locateType=" + req.locateType + " in meta");
    }
    byte[] metaStartKey;
    if (req.locateType.equals(RegionLocateType.BEFORE)) {
      if (isEmptyStopRow(req.row)) {
        byte[] binaryTableName = tableName.getName();
        metaStartKey = Arrays.copyOf(binaryTableName, binaryTableName.length + 1);
      } else {
        metaStartKey = createRegionName(tableName, req.row, ZEROES, false);
      }
    } else {
      metaStartKey = createRegionName(tableName, req.row, NINES, false);
    }
    byte[] metaStopKey =
      RegionInfo.createRegionName(tableName, HConstants.EMPTY_START_ROW, "", false);
    Scan scan = new Scan().withStartRow(metaStartKey).withStopRow(metaStopKey, true)
      .addFamily(HConstants.CATALOG_FAMILY).setReversed(true).setCaching(locatePrefetchLimit)
      .setReadType(ReadType.PREAD);

    switch (this.metaReplicaMode) {
      case LOAD_BALANCE:
        int metaReplicaId = this.metaReplicaSelector.select(tableName, req.row, req.locateType);
        if (metaReplicaId != RegionInfo.DEFAULT_REPLICA_ID) {
          // If the selector gives a non-primary meta replica region, then go with it.
          // Otherwise, just go to primary in non-hedgedRead mode.
          scan.setConsistency(Consistency.TIMELINE);
          scan.setReplicaId(metaReplicaId);
        }
        break;
      case HEDGED_READ:
        scan.setConsistency(Consistency.TIMELINE);
        break;
      default:
        // do nothing
    }

    conn.getTable(META_TABLE_NAME).scan(scan, new AdvancedScanResultConsumer() {

      private boolean completeNormally = false;

      private boolean tableNotFound = true;

      @Override
      public void onError(Throwable error) {
        complete(tableName, req, null, error);
      }

      @Override
      public void onComplete() {
        if (tableNotFound) {
          complete(tableName, req, null, new TableNotFoundException(tableName));
        } else if (!completeNormally) {
          complete(tableName, req, null, new IOException(
            "Unable to find region for '" + Bytes.toStringBinary(req.row) + "' in " + tableName));
        }
      }

      @Override
      public void onNext(Result[] results, ScanController controller) {
        if (results.length == 0) {
          return;
        }
        tableNotFound = false;
        int i = 0;
        for (; i < results.length; i++) {
          if (onScanNext(tableName, req, results[i])) {
            completeNormally = true;
            controller.terminate();
            i++;
            break;
          }
        }
        // Add the remaining results into cache
        if (i < results.length) {
          TableCache tableCache = getTableCache(tableName);
          for (; i < results.length; i++) {
            RegionLocations locs = MetaTableAccessor.getRegionLocations(results[i]);
            if (locs == null) {
              continue;
            }
            HRegionLocation loc = locs.getDefaultRegionLocation();
            if (loc == null) {
              continue;
            }
            RegionInfo info = loc.getRegion();
            if (info == null || info.isOffline() || info.isSplitParent()) {
              continue;
            }
            RegionLocations addedLocs = addToCache(tableCache, locs);
            synchronized (tableCache) {
              tableCache.clearCompletedRequests(addedLocs);
            }
          }
        }
      }
    });
  }

  private RegionLocations locateInCache(TableCache tableCache, TableName tableName, byte[] row,
      int replicaId, RegionLocateType locateType) {
    return locateType.equals(RegionLocateType.BEFORE)
      ? locateRowBeforeInCache(tableCache, tableName, row, replicaId)
      : locateRowInCache(tableCache, tableName, row, replicaId);
  }

  // locateToPrevious is true means we will use the start key of a region to locate the region
  // placed before it. Used for reverse scan. See the comment of
  // AsyncRegionLocator.getPreviousRegionLocation.
  private CompletableFuture<RegionLocations> getRegionLocationsInternal(TableName tableName,
      byte[] row, int replicaId, RegionLocateType locateType, boolean reload) {
    // AFTER should be convert to CURRENT before calling this method
    assert !locateType.equals(RegionLocateType.AFTER);
    TableCache tableCache = getTableCache(tableName);
    if (!reload) {
      RegionLocations locs = locateInCache(tableCache, tableName, row, replicaId, locateType);
      if (isGood(locs, replicaId)) {
        return CompletableFuture.completedFuture(locs);
      }
    }
    CompletableFuture<RegionLocations> future;
    LocateRequest req;
    boolean sendRequest = false;
    synchronized (tableCache) {
      // check again
      if (!reload) {
        RegionLocations locs = locateInCache(tableCache, tableName, row, replicaId, locateType);
        if (isGood(locs, replicaId)) {
          return CompletableFuture.completedFuture(locs);
        }
      }
      req = new LocateRequest(row, locateType);
      future = tableCache.allRequests.get(req);
      if (future == null) {
        future = new CompletableFuture<>();
        tableCache.allRequests.put(req, future);
        if (tableCache.hasQuota(maxConcurrentLocateRequestPerTable) && !tableCache.isPending(req)) {
          tableCache.send(req);
          sendRequest = true;
        }
      }
    }
    if (sendRequest) {
      locateInMeta(tableName, req);
    }
    return future;
  }

  CompletableFuture<RegionLocations> getRegionLocations(TableName tableName, byte[] row,
      int replicaId, RegionLocateType locateType, boolean reload) {
    // as we know the exact row after us, so we can just create the new row, and use the same
    // algorithm to locate it.
    if (locateType.equals(RegionLocateType.AFTER)) {
      row = createClosestRowAfter(row);
      locateType = RegionLocateType.CURRENT;
    }
    return getRegionLocationsInternal(tableName, row, replicaId, locateType, reload);
  }

  private void recordClearRegionCache() {
    conn.getConnectionMetrics().ifPresent(MetricsConnection::incrMetaCacheNumClearRegion);
  }

  private void removeLocationFromCache(HRegionLocation loc) {
    TableCache tableCache = cache.get(loc.getRegion().getTable());
    if (tableCache == null) {
      return;
    }
    byte[] startKey = loc.getRegion().getStartKey();
    for (;;) {
      RegionLocations oldLocs = tableCache.cache.get(startKey);
      if (oldLocs == null) {
        return;
      }
      HRegionLocation oldLoc = oldLocs.getRegionLocation(loc.getRegion().getReplicaId());
      if (!canUpdateOnError(loc, oldLoc)) {
        return;
      }
      // Tell metaReplicaSelector that the location is stale. It will create a stale entry
      // with timestamp internally. Next time the client looks up the same location,
      // it will pick a different meta replica region.
      if (this.metaReplicaMode == CatalogReplicaMode.LOAD_BALANCE) {
        metaReplicaSelector.onError(loc);
      }

      RegionLocations newLocs = removeRegionLocation(oldLocs, loc.getRegion().getReplicaId());
      if (newLocs == null) {
        if (tableCache.cache.remove(startKey, oldLocs)) {
          recordClearRegionCache();
          return;
        }
      } else {
        if (tableCache.cache.replace(startKey, oldLocs, newLocs)) {
          recordClearRegionCache();
          return;
        }
      }
    }
  }

  void addLocationToCache(HRegionLocation loc) {
    addToCache(getTableCache(loc.getRegion().getTable()), createRegionLocations(loc));
  }

  private HRegionLocation getCachedLocation(HRegionLocation loc) {
    TableCache tableCache = cache.get(loc.getRegion().getTable());
    if (tableCache == null) {
      return null;
    }
    RegionLocations locs = tableCache.cache.get(loc.getRegion().getStartKey());
    return locs != null ? locs.getRegionLocation(loc.getRegion().getReplicaId()) : null;
  }

  void updateCachedLocationOnError(HRegionLocation loc, Throwable exception) {
    Optional<MetricsConnection> connectionMetrics = conn.getConnectionMetrics();
    AsyncRegionLocatorHelper.updateCachedLocationOnError(loc, exception, this::getCachedLocation,
      this::addLocationToCache, this::removeLocationFromCache, connectionMetrics.orElse(null));
  }

  void clearCache(TableName tableName) {
    TableCache tableCache = cache.remove(tableName);
    if (tableCache == null) {
      return;
    }
    synchronized (tableCache) {
      if (!tableCache.allRequests.isEmpty()) {
        IOException error = new IOException("Cache cleared");
        tableCache.allRequests.values().forEach(f -> f.completeExceptionally(error));
      }
    }
    conn.getConnectionMetrics()
      .ifPresent(metrics -> metrics.incrMetaCacheNumClearRegion(tableCache.cache.size()));
  }

  void clearCache() {
    cache.clear();
  }

  void clearCache(ServerName serverName) {
    for (TableCache tableCache : cache.values()) {
      for (Map.Entry<byte[], RegionLocations> entry : tableCache.cache.entrySet()) {
        byte[] regionName = entry.getKey();
        RegionLocations locs = entry.getValue();
        RegionLocations newLocs = locs.removeByServer(serverName);
        if (locs == newLocs) {
          continue;
        }
        if (newLocs.isEmpty()) {
          tableCache.cache.remove(regionName, locs);
        } else {
          tableCache.cache.replace(regionName, locs, newLocs);
        }
      }
    }
  }

  // only used for testing whether we have cached the location for a region.
  RegionLocations getRegionLocationInCache(TableName tableName, byte[] row) {
    TableCache tableCache = cache.get(tableName);
    if (tableCache == null) {
      return null;
    }
    return locateRowInCache(tableCache, tableName, row, RegionReplicaUtil.DEFAULT_REPLICA_ID);
  }
}
