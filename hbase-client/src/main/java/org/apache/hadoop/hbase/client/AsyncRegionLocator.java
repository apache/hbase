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
import static org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil.findException;
import static org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil.isMetaClearingException;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The asynchronous region locator.
 */
@InterfaceAudience.Private
class AsyncRegionLocator {

  private static final Log LOG = LogFactory.getLog(AsyncRegionLocator.class);

  private final HashedWheelTimer retryTimer;

  private final AsyncMetaRegionLocator metaRegionLocator;

  private final AsyncNonMetaRegionLocator nonMetaRegionLocator;

  AsyncRegionLocator(AsyncConnectionImpl conn, HashedWheelTimer retryTimer) {
    this.metaRegionLocator = new AsyncMetaRegionLocator(conn.registry);
    this.nonMetaRegionLocator = new AsyncNonMetaRegionLocator(conn);
    this.retryTimer = retryTimer;
  }

  private CompletableFuture<HRegionLocation> withTimeout(CompletableFuture<HRegionLocation> future,
      long timeoutNs, Supplier<String> timeoutMsg) {
    if (future.isDone() || timeoutNs <= 0) {
      return future;
    }
    Timeout timeoutTask = retryTimer.newTimeout(t -> {
      if (future.isDone()) {
        return;
      }
      future.completeExceptionally(new TimeoutIOException(timeoutMsg.get()));
    }, timeoutNs, TimeUnit.NANOSECONDS);
    return future.whenComplete((loc, error) -> {
      if (error.getClass() != TimeoutIOException.class) {
        // cancel timeout task if we are not completed by it.
        timeoutTask.cancel();
      }
    });
  }

  CompletableFuture<HRegionLocation> getRegionLocation(TableName tableName, byte[] row,
      RegionLocateType type, long timeoutNs) {
    // meta region can not be split right now so we always call the same method.
    // Change it later if the meta table can have more than one regions.
    CompletableFuture<HRegionLocation> future =
        tableName.equals(META_TABLE_NAME) ? metaRegionLocator.getRegionLocation()
            : nonMetaRegionLocator.getRegionLocation(tableName, row, type);
    return withTimeout(future, timeoutNs,
      () -> "Timeout(" + TimeUnit.NANOSECONDS.toMillis(timeoutNs)
          + "ms) waiting for region location for " + tableName + ", row='"
          + Bytes.toStringBinary(row) + "'");
  }

  static boolean canUpdate(HRegionLocation loc, HRegionLocation oldLoc) {
    // Do not need to update if no such location, or the location is newer, or the location is not
    // same with us
    return oldLoc != null && oldLoc.getSeqNum() <= loc.getSeqNum()
        && oldLoc.getServerName().equals(loc.getServerName());
  }

  static void updateCachedLoation(HRegionLocation loc, Throwable exception,
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
      metaRegionLocator.updateCachedLocation(loc, exception);
    } else {
      nonMetaRegionLocator.updateCachedLocation(loc, exception);
    }
  }

  void clearCache(TableName tableName) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Clear meta cache for " + tableName);
    }
    if (tableName.equals(META_TABLE_NAME)) {
      metaRegionLocator.clearCache();
    } else {
      nonMetaRegionLocator.clearCache(tableName);
    }
  }
}
