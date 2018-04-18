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

import java.io.Closeable;
import java.util.List;
/**
 * Holds details of the snapshot taken on a MemStore. Details include the snapshot's identifier,
 * count of cells in it and total memory size occupied by all the cells, timestamp information of
 * all the cells and a scanner to read all cells in it.
 */
@InterfaceAudience.Private
public class MemStoreSnapshot implements Closeable {
  private final long id;
  private final int cellsCount;
  private final MemStoreSize memStoreSize;
  private final TimeRangeTracker timeRangeTracker;
  private final List<KeyValueScanner> scanners;
  private final boolean tagsPresent;

  public MemStoreSnapshot(long id, ImmutableSegment snapshot) {
    this.id = id;
    this.cellsCount = snapshot.getCellsCount();
    this.memStoreSize = snapshot.getMemStoreSize();
    this.timeRangeTracker = snapshot.getTimeRangeTracker();
    this.scanners = snapshot.getScanners(Long.MAX_VALUE);
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

  public long getDataSize() {
    return memStoreSize.getDataSize();
  }

  public MemStoreSize getMemStoreSize() {
    return memStoreSize;
  }

  /**
   * @return {@link TimeRangeTracker} for all the Cells in the snapshot.
   */
  public TimeRangeTracker getTimeRangeTracker() {
    return timeRangeTracker;
  }

  /**
   * @return {@link KeyValueScanner} for iterating over the snapshot
   */
  public List<KeyValueScanner> getScanners() {
    return scanners;
  }

  /**
   * @return true if tags are present in this snapshot
   */
  public boolean isTagsPresent() {
    return this.tagsPresent;
  }

  @Override
  public void close() {
    if (this.scanners != null) {
      for (KeyValueScanner scanner : scanners) {
        scanner.close();
      }
    }
  }

}
