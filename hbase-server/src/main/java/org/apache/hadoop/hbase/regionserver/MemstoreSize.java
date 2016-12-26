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

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Wraps the data size part and heap overhead of the memstore.
 */
@InterfaceAudience.Private
public class MemstoreSize {

  private long dataSize;
  private long heapOverhead;
  final private boolean isEmpty;

  static final MemstoreSize EMPTY_SIZE = new MemstoreSize(true);

  public MemstoreSize() {
    dataSize = 0;
    heapOverhead = 0;
    isEmpty = false;
  }

  public MemstoreSize(boolean isEmpty) {
    dataSize = 0;
    heapOverhead = 0;
    this.isEmpty = isEmpty;
  }

  public boolean isEmpty() {
    return isEmpty;
  }

  public MemstoreSize(long dataSize, long heapOverhead) {
    this.dataSize = dataSize;
    this.heapOverhead = heapOverhead;
    this.isEmpty = false;
  }

  public void incMemstoreSize(long dataSize, long heapOverhead) {
    this.dataSize += dataSize;
    this.heapOverhead += heapOverhead;
  }

  public void incMemstoreSize(MemstoreSize size) {
    this.dataSize += size.dataSize;
    this.heapOverhead += size.heapOverhead;
  }

  public void decMemstoreSize(long dataSize, long heapOverhead) {
    this.dataSize -= dataSize;
    this.heapOverhead -= heapOverhead;
  }

  public void decMemstoreSize(MemstoreSize size) {
    this.dataSize -= size.dataSize;
    this.heapOverhead -= size.heapOverhead;
  }

  public long getDataSize() {

    return isEmpty ? 0 : dataSize;
  }

  public long getHeapOverhead() {

    return isEmpty ? 0 : heapOverhead;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof MemstoreSize)) {
      return false;
    }
    MemstoreSize other = (MemstoreSize) obj;
    return getDataSize() == other.dataSize && getHeapOverhead() == other.heapOverhead;
  }

  @Override
  public int hashCode() {
    long h = 13 * this.dataSize;
    h = h + 14 * this.heapOverhead;
    return (int) h;
  }

  @Override
  public String toString() {
    return "dataSize=" + this.dataSize + " , heapOverhead=" + this.heapOverhead;
  }
}
