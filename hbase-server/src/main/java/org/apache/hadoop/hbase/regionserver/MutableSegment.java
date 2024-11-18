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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A mutable segment in memstore, specifically the active segment.
 */
@InterfaceAudience.Private
public class MutableSegment extends Segment {

  private final AtomicBoolean flushed = new AtomicBoolean(false);

  public final static long DEEP_OVERHEAD =
    ClassSize.align(Segment.DEEP_OVERHEAD + ClassSize.CONCURRENT_SKIPLISTMAP
      + ClassSize.SYNC_TIMERANGE_TRACKER + ClassSize.REFERENCE + ClassSize.ATOMIC_BOOLEAN);

  protected MutableSegment(CellSet<ExtendedCell> cellSet, CellComparator comparator,
    MemStoreLAB memStoreLAB, MemStoreSizing memstoreSizing) {
    super(cellSet, comparator, memStoreLAB, TimeRangeTracker.create(TimeRangeTracker.Type.SYNC));
    incMemStoreSize(0, DEEP_OVERHEAD, 0, 0); // update the mutable segment metadata
    if (memstoreSizing != null) {
      memstoreSizing.incMemStoreSize(0, DEEP_OVERHEAD, 0, 0);
    }
  }

  /**
   * Adds the given cell into the segment
   * @param cell      the cell to add
   * @param mslabUsed whether using MSLAB
   */
  public void add(ExtendedCell cell, boolean mslabUsed, MemStoreSizing memStoreSizing,
    boolean sizeAddedPreOperation) {
    internalAdd(cell, mslabUsed, memStoreSizing, sizeAddedPreOperation);
  }

  public boolean setInMemoryFlushed() {
    return flushed.compareAndSet(false, true);
  }

  /**
   * Returns the first cell in the segment
   * @return the first cell in the segment
   */
  ExtendedCell first() {
    return this.getCellSet().first();
  }

  @Override
  protected long indexEntrySize() {
    return ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY;
  }
}
