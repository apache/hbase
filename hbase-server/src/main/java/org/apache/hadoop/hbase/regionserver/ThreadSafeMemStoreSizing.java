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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Accounting of current heap and data sizes.
 * Thread-safe. Many threads can do updates against this single instance.
 * @see NonThreadSafeMemStoreSizing
 * @see MemStoreSize
 */
@InterfaceAudience.Private
class ThreadSafeMemStoreSizing implements MemStoreSizing {
  // We used to tie the update of these thread counters so
  // they all changed together under one lock. This was
  // undone. Doesn't seem necessary.
  private final AtomicLong dataSize = new AtomicLong();
  private final AtomicLong heapSize = new AtomicLong();
  private final AtomicLong offHeapSize = new AtomicLong();
  private final AtomicInteger cellsCount = new AtomicInteger();

  ThreadSafeMemStoreSizing() {
    this(0, 0, 0, 0);
  }

  ThreadSafeMemStoreSizing(MemStoreSize mss) {
    this(mss.getDataSize(), mss.getHeapSize(), mss.getOffHeapSize(), mss.getCellsCount());
  }

  ThreadSafeMemStoreSizing(long dataSize, long heapSize, long offHeapSize, int cellsCount) {
    incMemStoreSize(dataSize, heapSize, offHeapSize, cellsCount);
  }

  public MemStoreSize getMemStoreSize() {
    return new MemStoreSize(getDataSize(), getHeapSize(), getOffHeapSize(), getCellsCount());
  }

  @Override
  public long incMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta,
      int cellsCountDelta) {
    this.offHeapSize.addAndGet(offHeapSizeDelta);
    this.heapSize.addAndGet(heapSizeDelta);
    this.cellsCount.addAndGet(cellsCountDelta);
    return this.dataSize.addAndGet(dataSizeDelta);
  }

  @Override
  public boolean compareAndSetDataSize(long expected, long updated) {
    return dataSize.compareAndSet(expected, updated);
  }

  @Override
  public long getDataSize() {
    return dataSize.get();
  }

  @Override
  public long getHeapSize() {
    return heapSize.get();
  }

  @Override
  public long getOffHeapSize() {
    return offHeapSize.get();
  }

  @Override
  public int getCellsCount() {
    return cellsCount.get();
  }

  @Override
  public String toString() {
    return getMemStoreSize().toString();
  }
}
