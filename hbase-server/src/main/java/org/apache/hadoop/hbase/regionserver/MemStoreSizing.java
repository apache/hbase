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
 * Allows read/write on data/heap size as opposed to {@Link MemStoreSize} which is read-only.
 * For internal use.
 * @see MemStoreSize
 */
@InterfaceAudience.Private
public class MemStoreSizing extends MemStoreSize {
  public static final MemStoreSizing DUD = new MemStoreSizing() {

    @Override public void incMemStoreSize(long dataSizeDelta, long heapSizeDelta,
        long offHeapSizeDelta) {
      throw new RuntimeException("I'm a dud, you can't use me!");
    }

    @Override public void decMemStoreSize(long dataSizeDelta, long heapSizeDelta,
        long offHeapSizeDelta) {
      throw new RuntimeException("I'm a dud, you can't use me!");
    }
  };

  public MemStoreSizing() {
    super();
  }

  public MemStoreSizing(long dataSize, long heapSize, long offHeapSize) {
    super(dataSize, heapSize, offHeapSize);
  }

  public MemStoreSizing(MemStoreSize memStoreSize) {
    super(memStoreSize);
  }

  public void incMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta) {
    this.dataSize += dataSizeDelta;
    this.heapSize += heapSizeDelta;
    this.offHeapSize += offHeapSizeDelta;
  }

  public void incMemStoreSize(MemStoreSize delta) {
    incMemStoreSize(delta.getDataSize(), delta.getHeapSize(), delta.getOffHeapSize());
  }

  public void decMemStoreSize(long dataSizeDelta, long heapSizeDelta, long offHeapSizeDelta) {
    this.dataSize -= dataSizeDelta;
    this.heapSize -= heapSizeDelta;
    this.offHeapSize -= offHeapSizeDelta;
  }

  public void decMemStoreSize(MemStoreSize delta) {
    decMemStoreSize(delta.getDataSize(), delta.getHeapSize(), delta.getOffHeapSize());
  }

  public void empty() {
    this.dataSize = 0L;
    this.heapSize = 0L;
    this.offHeapSize = 0L;
  }

}