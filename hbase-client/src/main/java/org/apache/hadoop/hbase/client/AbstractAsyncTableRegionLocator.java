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

import static org.apache.hadoop.hbase.HConstants.EMPTY_END_ROW;
import static org.apache.hadoop.hbase.client.AsyncRegionLocatorHelper.isGood;
import static org.apache.hadoop.hbase.client.ConnectionUtils.createClosestRowAfter;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * The base class for locating region of a table.
 */
@InterfaceAudience.Private
abstract class AbstractAsyncTableRegionLocator {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractAsyncTableRegionLocator.class);

  protected final AsyncConnectionImpl conn;

  protected final TableName tableName;

  protected final int maxConcurrent;

  protected final TableRegionLocationCache cache;

  protected static final class LocateRequest {

    final byte[] row;

    final RegionLocateType locateType;

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

  private final Set<LocateRequest> pendingRequests = new HashSet<>();

  private final Map<LocateRequest, CompletableFuture<RegionLocations>> allRequests =
    new LinkedHashMap<>();

  AbstractAsyncTableRegionLocator(AsyncConnectionImpl conn, TableName tableName,
    int maxConcurrent, Comparator<byte[]> comparator) {
    this.conn = conn;
    this.tableName = tableName;
    this.maxConcurrent = maxConcurrent;
    this.cache = new TableRegionLocationCache(comparator, conn.getConnectionMetrics());
  }

  private boolean hasQuota() {
    return pendingRequests.size() < maxConcurrent;
  }

  protected final Optional<LocateRequest> getCandidate() {
    return allRequests.keySet().stream().filter(r -> !pendingRequests.contains(r)).findFirst();
  }

  void clearCompletedRequests(RegionLocations locations) {
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

  protected final void onLocateComplete(LocateRequest req, RegionLocations locs, Throwable error) {
    if (error != null) {
      LOG.warn("Failed to locate region in '" + tableName + "', row='" +
        Bytes.toStringBinary(req.row) + "', locateType=" + req.locateType, error);
    }
    Optional<LocateRequest> toSend = Optional.empty();
    if (locs != null) {
      RegionLocations addedLocs = cache.add(locs);
      synchronized (this) {
        pendingRequests.remove(req);
        clearCompletedRequests(addedLocs);
        // Remove a complete locate request in a synchronized block, so the table cache must have
        // quota to send a candidate request.
        toSend = getCandidate();
        toSend.ifPresent(pendingRequests::add);
      }
      toSend.ifPresent(this::locate);
    } else {
      // we meet an error
      assert error != null;
      synchronized (this) {
        pendingRequests.remove(req);
        // fail the request itself, no matter whether it is a DoNotRetryIOException, as we have
        // already retried several times
        CompletableFuture<?> future = allRequests.remove(req);
        if (future != null) {
          future.completeExceptionally(error);
        }
        clearCompletedRequests(null);
        // Remove a complete locate request in a synchronized block, so the table cache must have
        // quota to send a candidate request.
        toSend = getCandidate();
        toSend.ifPresent(pendingRequests::add);
      }
      toSend.ifPresent(this::locate);
    }
  }

  // return false means you do not need to go on, just return. And you do not need to call the above
  // onLocateComplete either when returning false, as we will call it in this method for you, this
  // is why we need to pass the LocateRequest as a parameter.
  protected final boolean validateRegionLocations(RegionLocations locs, LocateRequest req) {
    // remove HRegionLocation with null location, i.e, getServerName returns null.
    if (locs != null) {
      locs = locs.removeElementsWithNullLocation();
    }

    // the default region location should always be presented when fetching from meta, otherwise
    // let's fail the request.
    if (locs == null || locs.getDefaultRegionLocation() == null) {
      onLocateComplete(req, null,
        new HBaseIOException(String.format("No location found for '%s', row='%s', locateType=%s",
          tableName, Bytes.toStringBinary(req.row), req.locateType)));
      return false;
    }
    HRegionLocation loc = locs.getDefaultRegionLocation();
    RegionInfo info = loc.getRegion();
    if (info == null) {
      onLocateComplete(req, null,
        new HBaseIOException(String.format("HRegionInfo is null for '%s', row='%s', locateType=%s",
          tableName, Bytes.toStringBinary(req.row), req.locateType)));
      return false;
    }
    return true;
  }

  protected abstract void locate(LocateRequest req);

  abstract CompletableFuture<List<HRegionLocation>>
    getAllRegionLocations(boolean excludeOfflinedSplitParents);

  CompletableFuture<RegionLocations> getRegionLocations(byte[] row, int replicaId,
    RegionLocateType locateType, boolean reload) {
    if (locateType.equals(RegionLocateType.AFTER)) {
      row = createClosestRowAfter(row);
      locateType = RegionLocateType.CURRENT;
    }
    if (!reload) {
      RegionLocations locs = cache.locate(tableName, row, replicaId, locateType);
      if (isGood(locs, replicaId)) {
        return CompletableFuture.completedFuture(locs);
      }
    }
    CompletableFuture<RegionLocations> future;
    LocateRequest req;
    boolean sendRequest = false;
    synchronized (this) {
      // check again
      if (!reload) {
        RegionLocations locs = cache.locate(tableName, row, replicaId, locateType);
        if (isGood(locs, replicaId)) {
          return CompletableFuture.completedFuture(locs);
        }
      }
      req = new LocateRequest(row, locateType);
      future = allRequests.get(req);
      if (future == null) {
        future = new CompletableFuture<>();
        allRequests.put(req, future);
        if (hasQuota() && !pendingRequests.contains(req)) {
          pendingRequests.add(req);
          sendRequest = true;
        }
      }
    }
    if (sendRequest) {
      locate(req);
    }
    return future;
  }

  void addToCache(RegionLocations locs) {
    cache.add(locs);
  }

  // notice that this is not a constant time operation, do not call it on critical path.
  int getCacheSize() {
    return cache.size();
  }

  void clearPendingRequests() {
    synchronized (this) {
      if (!allRequests.isEmpty()) {
        IOException error = new IOException("Cache cleared");
        for (CompletableFuture<?> future : allRequests.values()) {
          future.completeExceptionally(error);
        }
      }
    }
  }

  void clearCache(ServerName serverName) {
    cache.clearCache(serverName);
  }

  void removeLocationFromCache(HRegionLocation loc) {
    cache.removeLocationFromCache(loc);
  }

  RegionLocations getInCache(byte[] key) {
    return cache.get(key);
  }

  // only used for testing whether we have cached the location for a region.
  @VisibleForTesting
  RegionLocations locateInCache(byte[] row) {
    return cache.locate(tableName, row, RegionReplicaUtil.DEFAULT_REPLICA_ID,
      RegionLocateType.CURRENT);
  }

  // only used for testing whether we have cached the location for a table.
  @VisibleForTesting
  int getNumberOfCachedRegionLocations() {
    return cache.getNumberOfCachedRegionLocations();
  }
}
