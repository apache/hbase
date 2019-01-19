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
package org.apache.hadoop.hbase.regionserver;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Accounting of current heap and data sizes.
 * Tracks 3 sizes:
 * <ol>
 * <li></li>data size: the aggregated size of all key-value not including meta data such as
 * index, time range etc.
 * </li>
 * <li>heap size: the aggregated size of all data that is allocated on-heap including all
 * key-values that reside on-heap and the metadata that resides on-heap
 * </li>
 * <li></li>off-heap size: the aggregated size of all data that is allocated off-heap including all
 * key-values that reside off-heap and the metadata that resides off-heap
 * </li>
 * </ol>
 *
 * 3 examples to illustrate their usage:
 * <p>
 * Consider a store with 100MB of key-values allocated on-heap and 20MB of metadata allocated
 * on-heap. The counters are <100MB, 120MB, 0>, respectively.
 * </p>
 * <p>Consider a store with 100MB of key-values allocated off-heap and 20MB of metadata
 * allocated on-heap (e.g, CAM index). The counters are <100MB, 20MB, 100MB>, respectively.
 * </p>
 * <p>
 * Consider a store with 100MB of key-values from which 95MB are allocated off-heap and 5MB
 * are allocated on-heap (e.g., due to upserts) and 20MB of metadata from which 15MB allocated
 * off-heap (e.g, CCM index) and 5MB allocated on-heap (e.g, CSLM index in active).
 * The counters are <100MB, 10MB, 110MB>, respectively.
 * </p>
 *
 * Like {@link TimeRangeTracker}, it has thread-safe and non-thread-safe implementations.
 */
@InterfaceAudience.Private
public interface MemStoreSizing {
  MemStoreSizing DUD = new MemStoreSizing() {
    private final MemStoreSize mss = new MemStoreSize();

    @Override
    public MemStoreSize getMemStoreSize() {
      return this.mss;
    }

    @Override
    public long getDataSize() {
      return this.mss.getDataSize();
    }

    @Override
    public long getHeapSize() {
      return this.mss.getHeapSize();
    }

    @Override
    public long getOffHeapSize() {
      return this.mss.getOffHeapSize();
    }

    @Override
    public int getCellsCount() {
      return this.mss.getCellsCount();
    }

    @Override
    public long incMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta,
        int cellsCountDelta) {
      throw new RuntimeException("I'm a DUD, you can't use me!");
    }

    @Override
    public boolean compareAndSetDataSize(long expected, long updated) {
      throw new RuntimeException("I'm a DUD, you can't use me!");
    }
  };

  /**
   * @return The new dataSize ONLY as a convenience
   */
  long incMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta,
      int cellsCountDelta);

  default long incMemStoreSize(MemStoreSize delta) {
    return incMemStoreSize(delta.getDataSize(), delta.getHeapSize(), delta.getOffHeapSize(),
      delta.getCellsCount());
  }

  /**
   * @return The new dataSize ONLY as a convenience
   */
  default long decMemStoreSize(long dataSizeDelta, long heapSizeDelta,
      long offHeapSizeDelta, int cellsCountDelta) {
    return incMemStoreSize(-dataSizeDelta, -heapSizeDelta, -offHeapSizeDelta, -cellsCountDelta);
  }

  default long decMemStoreSize(MemStoreSize delta) {
    return incMemStoreSize(-delta.getDataSize(), -delta.getHeapSize(), -delta.getOffHeapSize(),
      -delta.getCellsCount());
  }

  boolean compareAndSetDataSize(long expected, long updated);

  long getDataSize();
  long getHeapSize();
  long getOffHeapSize();
  int getCellsCount();

  /**
   * @return Use this datastructure to return all three settings, {@link #getDataSize()},
   * {@link #getHeapSize()}, and {@link #getOffHeapSize()}, in the one go.
   */
  MemStoreSize getMemStoreSize();
}
