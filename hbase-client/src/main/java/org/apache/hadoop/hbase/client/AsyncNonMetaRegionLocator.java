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
import static org.apache.hadoop.hbase.client.AsyncRegionLocator.updateCachedLoation;
import static org.apache.hadoop.hbase.client.ConnectionUtils.createClosestRowAfter;
import static org.apache.hadoop.hbase.client.ConnectionUtils.isEmptyStopRow;
import static org.apache.hadoop.hbase.util.Bytes.BYTES_COMPARATOR;
import static org.apache.hadoop.hbase.util.CollectionUtils.computeIfAbsent;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The asynchronous locator for regions other than meta.
 */
@InterfaceAudience.Private
class AsyncNonMetaRegionLocator {

  private static final Log LOG = LogFactory.getLog(AsyncNonMetaRegionLocator.class);

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
        new HashMap<>();

    public boolean hasQuota(int max) {
      return pendingRequests.size() < max;
    }

    public boolean isPending(LocateRequest req) {
      return pendingRequests.contains(req);
    }

    public void send(LocateRequest req) {
      pendingRequests.add(req);
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
    TableCache tableCache = cache.get(loc.getRegionInfo().getTable());
    if (tableCache == null) {
      return;
    }
    tableCache.cache.computeIfPresent(loc.getRegionInfo().getStartKey(), (k, oldLoc) -> {
      if (oldLoc.getSeqNum() > loc.getSeqNum()
          || !oldLoc.getServerName().equals(loc.getServerName())) {
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
    byte[] startKey = loc.getRegionInfo().getStartKey();
    HRegionLocation oldLoc = tableCache.cache.putIfAbsent(startKey, loc);
    if (oldLoc == null) {
      return true;
    }
    if (oldLoc.getSeqNum() > loc.getSeqNum()
        || oldLoc.getServerName().equals(loc.getServerName())) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Will not add " + loc + " to cache because the old value " + oldLoc
            + " is newer than us or has the same server name");
      }
      return false;
    }
    return loc == tableCache.cache.compute(startKey, (k, oldValue) -> {
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

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD",
      justification = "Called by lambda expression")
  private void addToCache(HRegionLocation loc) {
    addToCache(getTableCache(loc.getRegionInfo().getTable()), loc);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Try adding " + loc + " to cache");
    }
  }

  private boolean tryComplete(LocateRequest req, CompletableFuture<HRegionLocation> future,
      HRegionLocation loc) {
    if (future.isDone()) {
      return true;
    }
    boolean completed;
    if (req.locateType.equals(RegionLocateType.BEFORE)) {
      // for locating the row before current row, the common case is to find the previous region in
      // reverse scan, so we check the endKey first. In general, the condition should be startKey <
      // req.row and endKey >= req.row. Here we split it to endKey == req.row || (endKey > req.row
      // && startKey < req.row). The two conditions are equal since startKey < endKey.
      int c = Bytes.compareTo(loc.getRegionInfo().getEndKey(), req.row);
      completed =
          c == 0 || (c > 0 && Bytes.compareTo(loc.getRegionInfo().getStartKey(), req.row) < 0);
    } else {
      completed = loc.getRegionInfo().containsRow(req.row);
    }
    if (completed) {
      future.complete(loc);
      return true;
    } else {
      return false;
    }
  }

  private void complete(TableName tableName, LocateRequest req, HRegionLocation loc,
      Throwable error) {
    if (error != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to locate region in '" + tableName + "', row='"
            + Bytes.toStringBinary(req.row) + "', locateType=" + req.locateType,
          error);
      }
    }
    LocateRequest toSend = null;
    TableCache tableCache = getTableCache(tableName);
    if (loc != null) {
      if (!addToCache(tableCache, loc)) {
        // someone is ahead of us.
        synchronized (tableCache) {
          tableCache.pendingRequests.remove(req);
        }
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
      if (loc != null) {
        for (Iterator<Map.Entry<LocateRequest, CompletableFuture<HRegionLocation>>> iter =
            tableCache.allRequests.entrySet().iterator(); iter.hasNext();) {
          Map.Entry<LocateRequest, CompletableFuture<HRegionLocation>> entry = iter.next();
          if (tryComplete(entry.getKey(), entry.getValue(), loc)) {
            iter.remove();
          }
        }
      }
      if (!tableCache.allRequests.isEmpty()
          && tableCache.hasQuota(maxConcurrentLocateRequestPerTable)) {
        LocateRequest[] candidates = tableCache.allRequests.keySet().stream()
            .filter(r -> !tableCache.isPending(r)).toArray(LocateRequest[]::new);
        if (candidates.length > 0) {
          // TODO: use a better algorithm to send a request which is more likely to fetch a new
          // location.
          toSend = candidates[ThreadLocalRandom.current().nextInt(candidates.length)];
        }
      }
    }
    if (toSend != null) {
      locateInMeta(tableName, toSend);
    }
  }

  private void onScanComplete(TableName tableName, LocateRequest req, List<Result> results,
      Throwable error) {
    if (error != null) {
      complete(tableName, req, null, error);
      return;
    }
    if (results.isEmpty()) {
      complete(tableName, req, null, new TableNotFoundException(tableName));
      return;
    }
    RegionLocations locs = MetaTableAccessor.getRegionLocations(results.get(0));
    if (LOG.isDebugEnabled()) {
      LOG.debug("The fetched location of '" + tableName + "', row='" + Bytes.toStringBinary(req.row)
          + "', locateType=" + req.locateType + " is " + locs);
    }
    if (locs == null || locs.getDefaultRegionLocation() == null) {
      complete(tableName, req, null,
        new IOException(String.format("No location found for '%s', row='%s', locateType=%s",
          tableName, Bytes.toStringBinary(req.row), req.locateType)));
      return;
    }
    HRegionLocation loc = locs.getDefaultRegionLocation();
    HRegionInfo info = loc.getRegionInfo();
    if (info == null) {
      complete(tableName, req, null,
        new IOException(String.format("HRegionInfo is null for '%s', row='%s', locateType=%s",
          tableName, Bytes.toStringBinary(req.row), req.locateType)));
      return;
    }
    if (!info.getTable().equals(tableName)) {
      complete(tableName, req, null, new TableNotFoundException(
          "Table '" + tableName + "' was not found, got: '" + info.getTable() + "'"));
      return;
    }
    if (info.isSplit()) {
      complete(tableName, req, null,
        new RegionOfflineException(
            "the only available region for the required row is a split parent,"
                + " the daughters should be online soon: '" + info.getRegionNameAsString() + "'"));
      return;
    }
    if (info.isOffline()) {
      complete(tableName, req, null, new RegionOfflineException("the region is offline, could"
          + " be caused by a disable table call: '" + info.getRegionNameAsString() + "'"));
      return;
    }
    if (loc.getServerName() == null) {
      complete(tableName, req, null,
        new NoServerForRegionException(
            String.format("No server address listed for region '%s', row='%s', locateType=%s",
              info.getRegionNameAsString(), Bytes.toStringBinary(req.row), req.locateType)));
      return;
    }
    complete(tableName, req, loc, null);
  }

  private HRegionLocation locateRowInCache(TableCache tableCache, TableName tableName, byte[] row) {
    Map.Entry<byte[], HRegionLocation> entry = tableCache.cache.floorEntry(row);
    if (entry == null) {
      return null;
    }
    HRegionLocation loc = entry.getValue();
    byte[] endKey = loc.getRegionInfo().getEndKey();
    if (isEmptyStopRow(endKey) || Bytes.compareTo(row, endKey) < 0) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Found " + loc + " in cache for '" + tableName + "', row='"
            + Bytes.toStringBinary(row) + "', locateType=" + RegionLocateType.CURRENT);
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
    if (isEmptyStopRow(loc.getRegionInfo().getEndKey())
        || Bytes.compareTo(loc.getRegionInfo().getEndKey(), row) >= 0) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Found " + loc + " in cache for '" + tableName + "', row='"
            + Bytes.toStringBinary(row) + "', locateType=" + RegionLocateType.BEFORE);
      }
      return loc;
    } else {
      return null;
    }
  }

  private void locateInMeta(TableName tableName, LocateRequest req) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Try locate '" + tableName + "', row='" + Bytes.toStringBinary(req.row)
          + "', locateType=" + req.locateType + " in meta");
    }
    byte[] metaKey;
    if (req.locateType.equals(RegionLocateType.BEFORE)) {
      if (isEmptyStopRow(req.row)) {
        byte[] binaryTableName = tableName.getName();
        metaKey = Arrays.copyOf(binaryTableName, binaryTableName.length + 1);
      } else {
        metaKey = createRegionName(tableName, req.row, ZEROES, false);
      }
    } else {
      metaKey = createRegionName(tableName, req.row, NINES, false);
    }
    conn.getRawTable(META_TABLE_NAME)
        .smallScan(new Scan(metaKey).setReversed(true).setSmall(true).addFamily(CATALOG_FAMILY), 1)
        .whenComplete((results, error) -> onScanComplete(tableName, req, results, error));
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
      byte[] row, RegionLocateType locateType) {
    // AFTER should be convert to CURRENT before calling this method
    assert !locateType.equals(RegionLocateType.AFTER);
    TableCache tableCache = getTableCache(tableName);
    HRegionLocation loc = locateInCache(tableCache, tableName, row, locateType);
    if (loc != null) {
      return CompletableFuture.completedFuture(loc);
    }
    CompletableFuture<HRegionLocation> future;
    LocateRequest req;
    boolean sendRequest = false;
    synchronized (tableCache) {
      // check again
      loc = locateInCache(tableCache, tableName, row, locateType);
      if (loc != null) {
        return CompletableFuture.completedFuture(loc);
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
      RegionLocateType locateType) {
    if (locateType.equals(RegionLocateType.BEFORE)) {
      return getRegionLocationInternal(tableName, row, locateType);
    } else {
      // as we know the exact row after us, so we can just create the new row, and use the same
      // algorithm to locate it.
      if (locateType.equals(RegionLocateType.AFTER)) {
        row = createClosestRowAfter(row);
      }
      return getRegionLocationInternal(tableName, row, RegionLocateType.CURRENT);
    }
  }

  void updateCachedLocation(HRegionLocation loc, Throwable exception) {
    updateCachedLoation(loc, exception, l -> {
      TableCache tableCache = cache.get(l.getRegionInfo().getTable());
      if (tableCache == null) {
        return null;
      }
      return tableCache.cache.get(l.getRegionInfo().getStartKey());
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
