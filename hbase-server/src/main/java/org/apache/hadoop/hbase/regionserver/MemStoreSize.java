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

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Reports the data size part and total heap space occupied by the MemStore.
 * Read-only.
 * @see MemStoreSizing
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
public class MemStoreSize {
  // MemStore size tracks 3 sizes:
  // (1) data size: the aggregated size of all key-value not including meta data such as
  // index, time range etc.
  // (2) heap size: the aggregated size of all data that is allocated on-heap including all
  // key-values that reside on-heap and the metadata that resides on-heap
  // (3) off-heap size: the aggregated size of all data that is allocated off-heap including all
  // key-values that reside off-heap and the metadata that resides off-heap
  //
  // 3 examples to illustrate their usage:
  // Consider a store with 100MB of key-values allocated on-heap and 20MB of metadata allocated
  // on-heap. The counters are <100MB, 120MB, 0>, respectively.
  // Consider a store with 100MB of key-values allocated off-heap and 20MB of metadata
  // allocated on-heap (e.g, CAM index). The counters are <100MB, 20MB, 100MB>, respectively.
  // Consider a store with 100MB of key-values from which 95MB are allocated off-heap and 5MB
  // are allocated on-heap (e.g., due to upserts) and 20MB of metadata from which 15MB allocated
  // off-heap (e.g, CCM index) and 5MB allocated on-heap (e.g, CSLM index in active).
  // The counters are <100MB, 10MB, 110MB>, respectively.

  /**
   *'dataSize' tracks the Cell's data bytes size alone (Key bytes, value bytes). A cell's data can
   * be in on heap or off heap area depending on the MSLAB and its configuration to be using on heap
   * or off heap LABs
   */
  protected volatile long dataSize;

  /** 'heapSize' tracks all Cell's heap size occupancy. This will include Cell POJO heap overhead.
   * When Cells in on heap area, this will include the cells data size as well.
   */
  protected volatile long heapSize;

  /** off-heap size: the aggregated size of all data that is allocated off-heap including all
   * key-values that reside off-heap and the metadata that resides off-heap
   */
  protected volatile long offHeapSize;

  public MemStoreSize() {
    this(0L, 0L, 0L);
  }

  public MemStoreSize(long dataSize, long heapSize, long offHeapSize) {
    this.dataSize = dataSize;
    this.heapSize = heapSize;
    this.offHeapSize = offHeapSize;
  }

  protected MemStoreSize(MemStoreSize memStoreSize) {
    this.dataSize = memStoreSize.dataSize;
    this.heapSize = memStoreSize.heapSize;
    this.offHeapSize = memStoreSize.offHeapSize;
  }
  public boolean isEmpty() {
    return this.dataSize == 0 && this.heapSize == 0 && this.offHeapSize == 0;
  }

  public long getDataSize() {
    return this.dataSize;
  }

  public long getHeapSize() {
    return this.heapSize;
  }

  public long getOffHeapSize() {
    return this.offHeapSize;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    MemStoreSize other = (MemStoreSize) obj;
    return this.dataSize == other.dataSize
        && this.heapSize == other.heapSize
        && this.offHeapSize == other.offHeapSize;
  }

  @Override
  public int hashCode() {
    long h = 13 * this.dataSize;
    h = h + 14 * this.heapSize;
    h = h + 15 * this.offHeapSize;
    return (int) h;
  }

  @Override
  public String toString() {
    return "dataSize=" + this.dataSize
        + " , heapSize=" + this.heapSize
        + " , offHeapSize=" + this.offHeapSize;
  }
}
