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
import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The asynchronous locator for meta region.
 */
@InterfaceAudience.Private
class AsyncMetaRegionLocator {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncMetaRegionLocator.class);

  private final AsyncRegistry registry;

  private final AtomicReference<RegionLocations> metaRegionLocations = new AtomicReference<>();

  private final AtomicReference<CompletableFuture<RegionLocations>> metaRelocateFuture =
    new AtomicReference<>();

  AsyncMetaRegionLocator(AsyncRegistry registry) {
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
    for (;;) {
      if (!reload) {
        RegionLocations locs = this.metaRegionLocations.get();
        if (isGood(locs, replicaId)) {
          return CompletableFuture.completedFuture(locs);
        }
      }
      LOG.trace("Meta region location cache is null, try fetching from registry.");
      if (metaRelocateFuture.compareAndSet(null, new CompletableFuture<>())) {
        LOG.debug("Start fetching meta region location from registry.");
        CompletableFuture<RegionLocations> future = metaRelocateFuture.get();
        addListener(registry.getMetaRegionLocation(), (locs, error) -> {
          if (error != null) {
            LOG.debug("Failed to fetch meta region location from registry", error);
            metaRelocateFuture.getAndSet(null).completeExceptionally(error);
            return;
          }
          LOG.debug("The fetched meta region location is {}", locs);
          // Here we update cache before reset future, so it is possible that someone can get a
          // stale value. Consider this:
          // 1. update cache
          // 2. someone clear the cache and relocate again
          // 3. the metaRelocateFuture is not null so the old future is used.
          // 4. we clear metaRelocateFuture and complete the future in it with the value being
          // cleared in step 2.
          // But we do not think it is a big deal as it rarely happens, and even if it happens, the
          // caller will retry again later, no correctness problems.
          this.metaRegionLocations.set(locs);
          metaRelocateFuture.set(null);
          future.complete(locs);
        });
        return future;
      } else {
        CompletableFuture<RegionLocations> future = metaRelocateFuture.get();
        if (future != null) {
          return future;
        }
      }
    }
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
      this::addLocationToCache, this::removeLocationFromCache);
  }

  void clearCache() {
    metaRegionLocations.set(null);
  }
}
