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

import static org.apache.hadoop.hbase.client.AsyncRegionLocator.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * The asynchronous locator for meta region.
 */
@InterfaceAudience.Private
class AsyncMetaRegionLocator {

  private static final Log LOG = LogFactory.getLog(AsyncMetaRegionLocator.class);

  private final AsyncRegistry registry;

  private final AtomicReference<HRegionLocation> metaRegionLocation = new AtomicReference<>();

  private final AtomicReference<CompletableFuture<HRegionLocation>> metaRelocateFuture =
      new AtomicReference<>();

  AsyncMetaRegionLocator(AsyncRegistry registry) {
    this.registry = registry;
  }

  CompletableFuture<HRegionLocation> getRegionLocation() {
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
        registry.getMetaRegionLocation().whenComplete((locs, error) -> {
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

  void updateCachedLocation(HRegionLocation loc, Throwable exception) {
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
  }

  void clearCache() {
    metaRegionLocation.set(null);
  }
}
