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

import static org.apache.hadoop.hbase.HConstants.NINES;
import static org.apache.hadoop.hbase.HConstants.ZEROES;
import static org.apache.hadoop.hbase.TableName.META_TABLE_NAME;
import static org.apache.hadoop.hbase.client.ConnectionUtils.createClosestRowAfter;
import static org.apache.hadoop.hbase.client.ConnectionUtils.isEmptyStopRow;
import static org.apache.hadoop.hbase.client.RegionInfo.createRegionName;
import static org.apache.hadoop.hbase.util.Bytes.BYTES_COMPARATOR;
import static org.apache.hadoop.hbase.util.CollectionUtils.computeIfAbsent;

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
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.util.Bytes;
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

  private final AsyncConnectionImpl conn;

  private final int maxConcurrentLocateRequestPerTable;

  private final ConcurrentMap<TableName, TableCache> cache = new ConcurrentHashMap<>();

  private static final class LocateRequest {

    public final byte[] row;

    public final RegionLocateType locateType;

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

    public final ConcurrentNavigableMap<byte[], HRegionLocation> cache =
      new ConcurrentSkipListMap<>(BYTES_COMPARATOR);

    public final Set<LocateRequest> pendingRequests = new HashSet<>();

    public final Map<LocateRequest, CompletableFuture<HRegionLocation>> allRequests =
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

    public void clearCompletedRequests(Optional<HRegionLocation> location) {
      for (Iterator<Map.Entry<LocateRequest, CompletableFuture<HRegionLocation>>> iter =
        allRequests.entrySet().iterator(); iter.hasNext();) {
        Map.Entry<LocateRequest, CompletableFuture<HRegionLocation>> entry = iter.next();
        if (tryComplete(entry.getKey(), entry.getValue(), location)) {
          iter.remove();
        }
      }
    }

    private boolean tryComplete(LocateRequest req, CompletableFuture<HRegionLocation> future,
        Optional<HRegionLocation> location) {
      if (future.isDone()) {
        return true;
      }
      if (!location.isPresent()) {
        return false;
      }
      HRegionLocation loc = location.get();
      boolean completed;
      if (req.locateType.equals(RegionLocateType.BEFORE)) {
        // for locating the row before current row, the common case is to find the previous region
        // in reverse scan, so we check the endKey first. In general, the condition should be
        // startKey < req.row and endKey >= req.row. Here we split it to endKey == req.row ||
        // (endKey > req.row && startKey < req.row). The two conditions are equal since startKey <
        // endKey.
        int c = Bytes.compareTo(loc.getRegion().getEndKey(), req.row);
        completed =
          c == 0 || (c > 0 && Bytes.compareTo(loc.getRegion().getStartKey(), req.row) < 0);
      } else {
        completed = loc.getRegion().containsRow(req.row);
      }
      if (completed) {
        future.complete(loc);
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
  }

  private TableCache getTableCache(TableName tableName) {
    return computeIfAbsent(cache, tableName, TableCache::new);
  }

  private void removeFromCache(HRegionLocation loc) {
    TableCache tableCache = cache.get(loc.getRegion().getTable());
    if (tableCache == null) {
      return;
    }
    tableCache.cache.computeIfPresent(loc.getRegion().getStartKey(), (k, oldLoc) -> {
      if (oldLoc.getSeqNum() > loc.getSeqNum() ||
        !oldLoc.getServerName().equals(loc.getServerName())) {
        return oldLoc;
      }
      return null;
    });
  }

  // return whether we add this loc to cache
  private boolean addToCache(TableCache tableCache, HRegionLocation loc) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Try adding " + loc + " to cache");
    }
    byte[] startKey = loc.getRegion().getStartKey();
    HRegionLocation oldLoc = tableCache.cache.putIfAbsent(startKey, loc);
    if (oldLoc == null) {
      return true;
    }
    if (oldLoc.getSeqNum() > loc.getSeqNum() ||
      oldLoc.getServerName().equals(loc.getServerName())) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Will not add " + loc + " to cache because the old value " + oldLoc +
          " is newer than us or has the same server name");
      }
      return false;
    }
    return loc == tableCache.cache.compute(startKey, (k, oldValue) -> {
      if (oldValue == null || oldValue.getSeqNum() <= loc.getSeqNum()) {
        return loc;
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("Will not add " + loc + " to cache because the old value " + oldValue +
          " is newer than us or has the same server name." +
          " Maybe it is updated before we replace it");
      }
      return oldValue;
    });
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD",
      justification = "Called by lambda expression")
  private void addToCache(HRegionLocation loc) {
    addToCache(getTableCache(loc.getRegion().getTable()), loc);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Try adding " + loc + " to cache");
    }
  }

  private void complete(TableName tableName, LocateRequest req, HRegionLocation loc,
      Throwable error) {
    if (error != null) {
      LOG.warn("Failed to locate region in '" + tableName + "', row='" +
        Bytes.toStringBinary(req.row) + "', locateType=" + req.locateType, error);
    }
    Optional<LocateRequest> toSend = Optional.empty();
    TableCache tableCache = getTableCache(tableName);
    if (loc != null) {
      if (!addToCache(tableCache, loc)) {
        // someone is ahead of us.
        synchronized (tableCache) {
          tableCache.pendingRequests.remove(req);
          tableCache.clearCompletedRequests(Optional.empty());
          // Remove a complete locate request in a synchronized block, so the table cache must have
          // quota to send a candidate request.
          toSend = tableCache.getCandidate();
          toSend.ifPresent(r -> tableCache.send(r));
        }
        toSend.ifPresent(r -> locateInMeta(tableName, r));
        return;
      }
    }
    synchronized (tableCache) {
      tableCache.pendingRequests.remove(req);
      if (error instanceof DoNotRetryIOException) {
        CompletableFuture<?> future = tableCache.allRequests.remove(req);
        if (future != null) {
          future.completeExceptionally(error);
        }
      }
      tableCache.clearCompletedRequests(Optional.ofNullable(loc));
      // Remove a complete locate request in a synchronized block, so the table cache must have
      // quota to send a candidate request.
      toSend = tableCache.getCandidate();
      toSend.ifPresent(r -> tableCache.send(r));
    }
    toSend.ifPresent(r -> locateInMeta(tableName, r));
  }

  // return whether we should stop the scan
  private boolean onScanNext(TableName tableName, LocateRequest req, Result result) {
    RegionLocations locs = MetaTableAccessor.getRegionLocations(result);
    LOG.debug("The fetched location of '{}', row='{}', locateType={} is {}", tableName,
      Bytes.toStringBinary(req.row), req.locateType, locs);

    if (locs == null || locs.getDefaultRegionLocation() == null) {
      complete(tableName, req, null,
        new IOException(String.format("No location found for '%s', row='%s', locateType=%s",
          tableName, Bytes.toStringBinary(req.row), req.locateType)));
      return true;
    }
    HRegionLocation loc = locs.getDefaultRegionLocation();
    RegionInfo info = loc.getRegion();
    if (info == null) {
      complete(tableName, req, null,
        new IOException(String.format("HRegionInfo is null for '%s', row='%s', locateType=%s",
          tableName, Bytes.toStringBinary(req.row), req.locateType)));
      return true;
    }
    if (info.isSplitParent()) {
      return false;
    }
    if (loc.getServerName() == null) {
      complete(tableName, req, null,
        new IOException(
            String.format("No server address listed for region '%s', row='%s', locateType=%s",
              info.getRegionNameAsString(), Bytes.toStringBinary(req.row), req.locateType)));
      return true;
    }
    complete(tableName, req, loc, null);
    return true;
  }

  private HRegionLocation locateRowInCache(TableCache tableCache, TableName tableName, byte[] row) {
    Map.Entry<byte[], HRegionLocation> entry = tableCache.cache.floorEntry(row);
    if (entry == null) {
      return null;
    }
    HRegionLocation loc = entry.getValue();
    byte[] endKey = loc.getRegion().getEndKey();
    if (isEmptyStopRow(endKey) || Bytes.compareTo(row, endKey) < 0) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Found " + loc + " in cache for '" + tableName + "', row='" +
          Bytes.toStringBinary(row) + "', locateType=" + RegionLocateType.CURRENT);
      }
      return loc;
    } else {
      return null;
    }
  }

  private HRegionLocation locateRowBeforeInCache(TableCache tableCache, TableName tableName,
      byte[] row) {
    Map.Entry<byte[], HRegionLocation> entry =
      isEmptyStopRow(row) ? tableCache.cache.lastEntry() : tableCache.cache.lowerEntry(row);
    if (entry == null) {
      return null;
    }
    HRegionLocation loc = entry.getValue();
    if (isEmptyStopRow(loc.getRegion().getEndKey()) ||
      Bytes.compareTo(loc.getRegion().getEndKey(), row) >= 0) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Found " + loc + " in cache for '" + tableName + "', row='" +
          Bytes.toStringBinary(row) + "', locateType=" + RegionLocateType.BEFORE);
      }
      return loc;
    } else {
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
    conn.getTable(META_TABLE_NAME)
      .scan(new Scan().withStartRow(metaStartKey).withStopRow(metaStopKey, true)
        .addFamily(HConstants.CATALOG_FAMILY).setReversed(true).setCaching(5)
        .setReadType(ReadType.PREAD), new AdvancedScanResultConsumer() {

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
                "Unable to find region for " + Bytes.toStringBinary(req.row) + " in " + tableName));
            }
          }

          @Override
          public void onNext(Result[] results, ScanController controller) {
            for (Result result : results) {
              tableNotFound = false;
              if (onScanNext(tableName, req, result)) {
                completeNormally = true;
                controller.terminate();
                return;
              }
            }
          }
        });
  }

  private HRegionLocation locateInCache(TableCache tableCache, TableName tableName, byte[] row,
      RegionLocateType locateType) {
    return locateType.equals(RegionLocateType.BEFORE)
      ? locateRowBeforeInCache(tableCache, tableName, row)
      : locateRowInCache(tableCache, tableName, row);
  }

  // locateToPrevious is true means we will use the start key of a region to locate the region
  // placed before it. Used for reverse scan. See the comment of
  // AsyncRegionLocator.getPreviousRegionLocation.
  private CompletableFuture<HRegionLocation> getRegionLocationInternal(TableName tableName,
      byte[] row, RegionLocateType locateType, boolean reload) {
    // AFTER should be convert to CURRENT before calling this method
    assert !locateType.equals(RegionLocateType.AFTER);
    TableCache tableCache = getTableCache(tableName);
    if (!reload) {
      HRegionLocation loc = locateInCache(tableCache, tableName, row, locateType);
      if (loc != null) {
        return CompletableFuture.completedFuture(loc);
      }
    }
    CompletableFuture<HRegionLocation> future;
    LocateRequest req;
    boolean sendRequest = false;
    synchronized (tableCache) {
      // check again
      if (!reload) {
        HRegionLocation loc = locateInCache(tableCache, tableName, row, locateType);
        if (loc != null) {
          return CompletableFuture.completedFuture(loc);
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

  CompletableFuture<HRegionLocation> getRegionLocation(TableName tableName, byte[] row,
      RegionLocateType locateType, boolean reload) {
    if (locateType.equals(RegionLocateType.BEFORE)) {
      return getRegionLocationInternal(tableName, row, locateType, reload);
    } else {
      // as we know the exact row after us, so we can just create the new row, and use the same
      // algorithm to locate it.
      if (locateType.equals(RegionLocateType.AFTER)) {
        row = createClosestRowAfter(row);
      }
      return getRegionLocationInternal(tableName, row, RegionLocateType.CURRENT, reload);
    }
  }

  void updateCachedLocation(HRegionLocation loc, Throwable exception) {
    AsyncRegionLocator.updateCachedLocation(loc, exception, l -> {
      TableCache tableCache = cache.get(l.getRegion().getTable());
      if (tableCache == null) {
        return null;
      }
      return tableCache.cache.get(l.getRegion().getStartKey());
    }, this::addToCache, this::removeFromCache);
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
  }
}
