/*
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

import static org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil.findException;
import static org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil.isMetaClearingException;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for asynchronous region locator.
 */
@InterfaceAudience.Private
final class AsyncRegionLocatorHelper {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncRegionLocatorHelper.class);

  private AsyncRegionLocatorHelper() {
  }

  static boolean canUpdateOnError(HRegionLocation loc, HRegionLocation oldLoc) {
    // Do not need to update if no such location, or the location is newer, or the location is not
    // the same with us
    if (loc == null || loc.getServerName() == null) {
      return false;
    }
    if (oldLoc == null || oldLoc.getServerName() == null) {
      return false;
    }
    return oldLoc.getSeqNum() <= loc.getSeqNum() &&
      oldLoc.getServerName().equals(loc.getServerName());
  }

  static void updateCachedLocationOnError(HRegionLocation loc, Throwable exception,
      Function<HRegionLocation, HRegionLocation> cachedLocationSupplier,
      Consumer<HRegionLocation> addToCache, Consumer<HRegionLocation> removeFromCache,
      MetricsConnection metrics) {
    HRegionLocation oldLoc = cachedLocationSupplier.apply(loc);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Try updating {} , the old value is {}, error={}", loc, oldLoc,
        exception != null ? exception.toString() : "none");
    }
    if (!canUpdateOnError(loc, oldLoc)) {
      return;
    }
    Throwable cause = findException(exception);
    if (LOG.isDebugEnabled()) {
      LOG.debug("The actual exception when updating {} is {}", loc,
        cause != null ? cause.toString() : "none");
    }
    if (cause == null || !isMetaClearingException(cause)) {
      LOG.debug("Will not update {} because the exception is null or not the one we care about",
        loc);
      return;
    }
    if (cause instanceof RegionMovedException) {
      RegionMovedException rme = (RegionMovedException) cause;
      HRegionLocation newLoc =
        new HRegionLocation(loc.getRegion(), rme.getServerName(), rme.getLocationSeqNum());
      LOG.debug("Try updating {} with the new location {} constructed by {}", loc, newLoc,
        rme.toString());
      addToCache.accept(newLoc);
    } else {
      LOG.debug("Try removing {} from cache", loc);
      if (metrics != null) {
        metrics.incrCacheDroppingExceptions(exception);
      }
      removeFromCache.accept(loc);
    }
  }

  static RegionLocations createRegionLocations(HRegionLocation loc) {
    int replicaId = loc.getRegion().getReplicaId();
    HRegionLocation[] locs = new HRegionLocation[replicaId + 1];
    locs[replicaId] = loc;
    return new RegionLocations(locs);
  }

  /**
   * Create a new {@link RegionLocations} based on the given {@code oldLocs}, and replace the
   * location for the given {@code replicaId} with the given {@code loc}.
   * <p/>
   * All the {@link RegionLocations} in async locator related class are immutable because we want to
   * access them concurrently, so here we need to create a new one, instead of calling
   * {@link RegionLocations#updateLocation(HRegionLocation, boolean, boolean)}.
   */
  static RegionLocations replaceRegionLocation(RegionLocations oldLocs, HRegionLocation loc) {
    int replicaId = loc.getRegion().getReplicaId();
    HRegionLocation[] locs = oldLocs.getRegionLocations();
    locs = Arrays.copyOf(locs, Math.max(replicaId + 1, locs.length));
    locs[replicaId] = loc;
    return new RegionLocations(locs);
  }

  /**
   * Create a new {@link RegionLocations} based on the given {@code oldLocs}, and remove the
   * location for the given {@code replicaId}.
   * <p/>
   * All the {@link RegionLocations} in async locator related class are immutable because we want to
   * access them concurrently, so here we need to create a new one, instead of calling
   * {@link RegionLocations#remove(int)}.
   */
  static RegionLocations removeRegionLocation(RegionLocations oldLocs, int replicaId) {
    HRegionLocation[] locs = oldLocs.getRegionLocations();
    if (locs.length < replicaId + 1) {
      // Here we do not modify the oldLocs so it is safe to return it.
      return oldLocs;
    }
    locs = Arrays.copyOf(locs, locs.length);
    locs[replicaId] = null;
    if (ObjectUtils.firstNonNull(locs) != null) {
      return new RegionLocations(locs);
    } else {
      // if all the locations are null, just return null
      return null;
    }
  }

  static boolean isGood(RegionLocations locs, int replicaId) {
    if (locs == null) {
      return false;
    }
    HRegionLocation loc = locs.getRegionLocation(replicaId);
    return loc != null && loc.getServerName() != null;
  }
}
