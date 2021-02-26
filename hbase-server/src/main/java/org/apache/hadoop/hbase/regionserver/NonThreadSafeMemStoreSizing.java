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
 * <em>NOT THREAD SAFE</em>.
 * Use in a 'local' context only where just a single-thread is updating. No concurrency!
 * Used, for example, when summing all Cells in a single batch where result is then applied to the
 * Store.
 * @see ThreadSafeMemStoreSizing
 */
@InterfaceAudience.Private
class NonThreadSafeMemStoreSizing implements MemStoreSizing {
  private long dataSize = 0;
  private long heapSize = 0;
  private long offHeapSize = 0;
  private int cellsCount = 0;

  NonThreadSafeMemStoreSizing() {
    this(0, 0, 0, 0);
  }

  NonThreadSafeMemStoreSizing(MemStoreSize mss) {
    this(mss.getDataSize(), mss.getHeapSize(), mss.getOffHeapSize(), mss.getCellsCount());
  }

  NonThreadSafeMemStoreSizing(long dataSize, long heapSize, long offHeapSize, int cellsCount) {
    incMemStoreSize(dataSize, heapSize, offHeapSize, cellsCount);
  }

  @Override
  public MemStoreSize getMemStoreSize() {
    return new MemStoreSize(this.dataSize, this.heapSize, this.offHeapSize, this.cellsCount);
  }

  @Override
  public long incMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta,
      int cellsCountDelta) {
    this.offHeapSize += offHeapSizeDelta;
    this.heapSize += heapSizeDelta;
    this.dataSize += dataSizeDelta;
    this.cellsCount += cellsCountDelta;
    return this.dataSize;
  }

  @Override
  public boolean compareAndSetDataSize(long expected, long updated) {
    if (dataSize == expected) {
      dataSize = updated;
      return true;
    }
    return false;
  }

  @Override
  public long getDataSize() {
    return dataSize;
  }

  @Override
  public long getHeapSize() {
    return heapSize;
  }

  @Override
  public long getOffHeapSize() {
    return offHeapSize;
  }

  @Override
  public int getCellsCount() {
    return cellsCount;
  }

  @Override
  public String toString() {
    return getMemStoreSize().toString();
  }
}
