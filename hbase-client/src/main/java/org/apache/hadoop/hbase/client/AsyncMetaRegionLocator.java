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

import static org.apache.hadoop.hbase.client.AsyncRegionLocatorHelper.canUpdateOnError;
import static org.apache.hadoop.hbase.client.AsyncRegionLocatorHelper.createRegionLocations;
import static org.apache.hadoop.hbase.client.AsyncRegionLocatorHelper.isGood;
import static org.apache.hadoop.hbase.client.AsyncRegionLocatorHelper.removeRegionLocation;
import static org.apache.hadoop.hbase.client.AsyncRegionLocatorHelper.replaceRegionLocation;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The asynchronous locator for meta region.
 */
@InterfaceAudience.Private
class AsyncMetaRegionLocator {

  private final ConnectionRegistry registry;

  private final AtomicReference<RegionLocations> metaRegionLocations = new AtomicReference<>();

  private final AtomicReference<CompletableFuture<RegionLocations>> metaRelocateFuture =
    new AtomicReference<>();

  AsyncMetaRegionLocator(ConnectionRegistry registry) {
    this.registry = registry;
  }

  /**
   * Get the region locations for meta region. If the location for the given replica is not
   * available in the cached locations, then fetch from the HBase cluster.
   * <p/>
   * The <code>replicaId</code> parameter is important. If the region replication config for meta
   * region is changed, then the cached region locations may not have the locations for new
   * replicas. If we do not check the location for the given replica, we will always return the
   * cached region locations and cause an infinite loop.
   */
  CompletableFuture<RegionLocations> getRegionLocations(int replicaId, boolean reload) {
    return ConnectionUtils.getOrFetch(metaRegionLocations, metaRelocateFuture, reload,
      registry::getMetaRegionLocations, locs -> isGood(locs, replicaId), "meta region location");
  }

  private HRegionLocation getCacheLocation(HRegionLocation loc) {
    RegionLocations locs = metaRegionLocations.get();
    return locs != null ? locs.getRegionLocation(loc.getRegion().getReplicaId()) : null;
  }

  private void addLocationToCache(HRegionLocation loc) {
    for (;;) {
      int replicaId = loc.getRegion().getReplicaId();
      RegionLocations oldLocs = metaRegionLocations.get();
      if (oldLocs == null) {
        RegionLocations newLocs = createRegionLocations(loc);
        if (metaRegionLocations.compareAndSet(null, newLocs)) {
          return;
        }
      }
      HRegionLocation oldLoc = oldLocs.getRegionLocation(replicaId);
      if (oldLoc != null && (oldLoc.getSeqNum() > loc.getSeqNum() ||
        oldLoc.getServerName().equals(loc.getServerName()))) {
        return;
      }
      RegionLocations newLocs = replaceRegionLocation(oldLocs, loc);
      if (metaRegionLocations.compareAndSet(oldLocs, newLocs)) {
        return;
      }
    }
  }

  private void removeLocationFromCache(HRegionLocation loc) {
    for (;;) {
      RegionLocations oldLocs = metaRegionLocations.get();
      if (oldLocs == null) {
        return;
      }
      HRegionLocation oldLoc = oldLocs.getRegionLocation(loc.getRegion().getReplicaId());
      if (!canUpdateOnError(loc, oldLoc)) {
        return;
      }
      RegionLocations newLocs = removeRegionLocation(oldLocs, loc.getRegion().getReplicaId());
      if (metaRegionLocations.compareAndSet(oldLocs, newLocs)) {
        return;
      }
    }
  }

  void updateCachedLocationOnError(HRegionLocation loc, Throwable exception) {
    AsyncRegionLocatorHelper.updateCachedLocationOnError(loc, exception, this::getCacheLocation,
      this::addLocationToCache, this::removeLocationFromCache, null);
  }

  void clearCache() {
    metaRegionLocations.set(null);
  }

  void clearCache(ServerName serverName) {
    for (;;) {
      RegionLocations locs = metaRegionLocations.get();
      if (locs == null) {
        return;
      }
      RegionLocations newLocs = locs.removeByServer(serverName);
      if (locs == newLocs) {
        return;
      }
      if (newLocs.isEmpty()) {
        newLocs = null;
      }
      if (metaRegionLocations.compareAndSet(locs, newLocs)) {
        return;
      }
    }
  }
}
