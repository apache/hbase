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
 * Holds details of the snapshot taken on a MemStore. Details include the snapshot's identifier,
 * count of cells in it and total memory size occupied by all the cells, timestamp information of
 * all the cells and a scanner to read all cells in it.
 */
@InterfaceAudience.Private
public class MemStoreSnapshot {
  private final long id;
  private final int cellsCount;
  private final long dataSize;
  private final long heapOverhead;
  private final TimeRangeTracker timeRangeTracker;
  private final KeyValueScanner scanner;
  private final boolean tagsPresent;

  public MemStoreSnapshot(long id, ImmutableSegment snapshot) {
    this.id = id;
    this.cellsCount = snapshot.getCellsCount();
    this.dataSize = snapshot.keySize();
    this.heapOverhead = snapshot.heapOverhead();
    this.timeRangeTracker = snapshot.getTimeRangeTracker();
    this.scanner = snapshot.getSnapshotScanner();
    this.tagsPresent = snapshot.isTagsPresent();
  }

  /**
   * @return snapshot's identifier.
   */
  public long getId() {
    return id;
  }

  /**
   * @return Number of Cells in this snapshot.
   */
  public int getCellsCount() {
    return cellsCount;
  }

  /**
   * @return Total memory size occupied by this snapshot.
   */
  public long getDataSize() {
    return dataSize;
  }

  public long getHeapOverhead() {
    return this.heapOverhead;
  }

  /**
   * @return {@link TimeRangeTracker} for all the Cells in the snapshot.
   */
  public TimeRangeTracker getTimeRangeTracker() {
    return this.timeRangeTracker;
  }

  /**
   * @return {@link KeyValueScanner} for iterating over the snapshot
   */
  public KeyValueScanner getScanner() {
    return this.scanner;
  }

  /**
   * @return true if tags are present in this snapshot
   */
  public boolean isTagsPresent() {
    return this.tagsPresent;
  }
}