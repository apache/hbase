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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * The implementation of AsyncRegionLocator.
 */
@InterfaceAudience.Private
class AsyncTableRegionLocatorImpl implements AsyncTableRegionLocator {

  private final TableName tableName;

  private final AsyncRegionLocator locator;

  public AsyncTableRegionLocatorImpl(TableName tableName, AsyncRegionLocator locator) {
    this.tableName = tableName;
    this.locator = locator;
  }

  @Override
  public TableName getName() {
    return tableName;
  }

  @Override
  public CompletableFuture<HRegionLocation> getRegionLocation(byte[] row, int replicaId,
      boolean reload) {
    return locator.getRegionLocation(tableName, row, replicaId, RegionLocateType.CURRENT, reload,
      -1L);
  }

  // this is used to prevent stack overflow if there are thousands of regions for the table. If the
  // location is in cache, the CompletableFuture will be completed immediately inside the same
  // thread, and then in the action we will call locate again, also in the same thread. If all the
  // locations are in cache, and we do not use whenCompleteAsync to break the tie, the stack will be
  // very very deep and cause stack overflow.
  @VisibleForTesting
  static final ThreadLocal<MutableInt> STACK_DEPTH = new ThreadLocal<MutableInt>() {

    @Override
    protected MutableInt initialValue() {
      return new MutableInt(0);
    }
  };

  @VisibleForTesting
  static final int MAX_STACK_DEPTH = 16;

  private void locate(CompletableFuture<List<HRegionLocation>> future,
      ConcurrentLinkedQueue<HRegionLocation> result, byte[] row) {
    BiConsumer<HRegionLocation, Throwable> listener = (loc, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
        return;
      }
      result.add(loc);
      if (ConnectionUtils.isEmptyStartRow(loc.getRegion().getStartKey())) {
        future.complete(result.stream()
          .sorted((l1, l2) -> RegionInfo.COMPARATOR.compare(l1.getRegion(), l2.getRegion()))
          .collect(Collectors.toList()));
      } else {
        locate(future, result, loc.getRegion().getStartKey());
      }
    };
    MutableInt depth = STACK_DEPTH.get();
    boolean async = depth.incrementAndGet() >= MAX_STACK_DEPTH;
    try {
      CompletableFuture<HRegionLocation> f =
        locator.getRegionLocation(tableName, row, RegionLocateType.BEFORE, -1L);
      if (async) {
        FutureUtils.addListenerAsync(f, listener);
      } else {
        FutureUtils.addListener(f, listener);
      }
    } finally {
      if (depth.decrementAndGet() == 0) {
        STACK_DEPTH.remove();
      }
    }
  }

  @Override
  public CompletableFuture<List<HRegionLocation>> getAllRegionLocations() {
    ConcurrentLinkedQueue<HRegionLocation> result = new ConcurrentLinkedQueue<>();
    CompletableFuture<List<HRegionLocation>> future = new CompletableFuture<>();
    // start from end to start, as when locating we will do reverse scan, so we will prefetch the
    // location of the regions before the current one.
    locate(future, result, HConstants.EMPTY_END_ROW);
    return future;
  }
}
