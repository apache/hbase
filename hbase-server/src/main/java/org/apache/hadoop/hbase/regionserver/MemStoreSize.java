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
package org.apache.hadoop.hbase.regionserver;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Wraps the data size part and total heap space occupied by the memstore.
 */
@InterfaceAudience.Private
public class MemStoreSize {

  // 'dataSize' tracks the Cell's data bytes size alone (Key bytes, value bytes). A cell's data can
  // be in on heap or off heap area depending on the MSLAB and its configuration to be using on heap
  // or off heap LABs
  private long dataSize;
  // 'heapSize' tracks all Cell's heap size occupancy. This will include Cell POJO heap overhead.
  // When Cells in on heap area, this will include the cells data size as well.
  private long heapSize;
  final private boolean isEmpty;

  public MemStoreSize() {
    dataSize = 0;
    heapSize = 0;
    isEmpty = false;
  }

  public MemStoreSize(boolean isEmpty) {
    dataSize = 0;
    heapSize = 0;
    this.isEmpty = isEmpty;
  }

  public boolean isEmpty() {
    return isEmpty;
  }

  public MemStoreSize(long dataSize, long heapSize) {
    this.dataSize = dataSize;
    this.heapSize = heapSize;
    this.isEmpty = false;
  }

  public void incMemStoreSize(long dataSizeDelta, long heapSizeDelta) {
    this.dataSize += dataSizeDelta;
    this.heapSize += heapSizeDelta;
  }

  public void incMemStoreSize(MemStoreSize delta) {
    this.dataSize += delta.dataSize;
    this.heapSize += delta.heapSize;
  }

  public void decMemStoreSize(long dataSizeDelta, long heapSizeDelta) {
    this.dataSize -= dataSizeDelta;
    this.heapSize -= heapSizeDelta;
  }

  public void decMemStoreSize(MemStoreSize delta) {
    this.dataSize -= delta.dataSize;
    this.heapSize -= delta.heapSize;
  }

  public long getDataSize() {
    return isEmpty ? 0 : dataSize;
  }

  public long getHeapSize() {
    return isEmpty ? 0 : heapSize;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof MemStoreSize)) {
      return false;
    }
    MemStoreSize other = (MemStoreSize) obj;
    return this.dataSize == other.dataSize && this.heapSize == other.heapSize;
  }

  @Override
  public int hashCode() {
    long h = 13 * this.dataSize;
    h = h + 14 * this.heapSize;
    return (int) h;
  }

  @Override
  public String toString() {
    return "dataSize=" + this.dataSize + " , heapSize=" + this.heapSize;
  }
}
