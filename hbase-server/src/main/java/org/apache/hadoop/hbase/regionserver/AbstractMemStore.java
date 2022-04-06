/*
 *
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

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.SortedSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;

/**
 * An abstract class, which implements the behaviour shared by all concrete memstore instances.
 */
@InterfaceAudience.Private
public abstract class AbstractMemStore implements MemStore {

  private static final long NO_SNAPSHOT_ID = -1;

  private final Configuration conf;
  private final CellComparator comparator;

  // active segment absorbs write operations
  private volatile MutableSegment active;
  // Snapshot of memstore.  Made for flusher.
  protected volatile ImmutableSegment snapshot;
  protected volatile long snapshotId;
  // Used to track when to flush
  private volatile long timeOfOldestEdit;

  protected RegionServicesForStores regionServices;

  // @formatter:off
  public final static long FIXED_OVERHEAD = (long) ClassSize.OBJECT
    + (5 * ClassSize.REFERENCE)
    + (2 * Bytes.SIZEOF_LONG); // snapshotId, timeOfOldestEdit
  // @formatter:on

  public final static long DEEP_OVERHEAD = FIXED_OVERHEAD;

  public static void addToScanners(List<? extends Segment> segments, long readPt,
      List<KeyValueScanner> scanners) {
    for (Segment item : segments) {
      addToScanners(item, readPt, scanners);
    }
  }

  protected static void addToScanners(Segment segment, long readPt,
      List<KeyValueScanner> scanners) {
    if (!segment.isEmpty()) {
      scanners.add(segment.getScanner(readPt));
    }
  }

  protected AbstractMemStore(final Configuration conf, final CellComparator c,
      final RegionServicesForStores regionServices) {
    this.conf = conf;
    this.comparator = c;
    this.regionServices = regionServices;
    resetActive();
    resetTimeOfOldestEdit();
    this.snapshot = SegmentFactory.instance().createImmutableSegment(c);
    this.snapshotId = NO_SNAPSHOT_ID;
  }

  protected void resetActive() {
    // Record the MutableSegment' heap overhead when initialing
    MemStoreSizing memstoreAccounting = new NonThreadSafeMemStoreSizing();
    // Reset heap to not include any keys
    active = SegmentFactory.instance().createMutableSegment(conf, comparator, memstoreAccounting);
    // regionServices can be null when testing
    if (regionServices != null) {
      regionServices.addMemStoreSize(memstoreAccounting.getDataSize(),
        memstoreAccounting.getHeapSize(), memstoreAccounting.getOffHeapSize(),
        memstoreAccounting.getCellsCount());
    }
  }

  protected void resetTimeOfOldestEdit() {
    this.timeOfOldestEdit = Long.MAX_VALUE;
  }

  /**
   * Updates the wal with the lowest sequence id (oldest entry) that is still in memory
   * @param onlyIfMoreRecent a flag that marks whether to update the sequence id no matter what or
   *                      only if it is greater than the previous sequence id
   */
  public abstract void updateLowestUnflushedSequenceIdInWAL(boolean onlyIfMoreRecent);

  @Override
  public void add(Iterable<Cell> cells, MemStoreSizing memstoreSizing) {
    for (Cell cell : cells) {
      add(cell, memstoreSizing);
    }
  }

  @Override
  public void add(Cell cell, MemStoreSizing memstoreSizing) {
    doAddOrUpsert(cell, 0, memstoreSizing, true);  }

  /*
   * Inserts the specified Cell into MemStore and deletes any existing
   * versions of the same row/family/qualifier as the specified Cell.
   * <p>
   * First, the specified Cell is inserted into the Memstore.
   * <p>
   * If there are any existing Cell in this MemStore with the same row,
   * family, and qualifier, they are removed.
   * <p>
   * Callers must hold the read lock.
   *
   * @param cell the cell to be updated
   * @param readpoint readpoint below which we can safely remove duplicate KVs
   * @param memstoreSizing object to accumulate changed size
   */
  private void upsert(Cell cell, long readpoint, MemStoreSizing memstoreSizing) {
    doAddOrUpsert(cell, readpoint, memstoreSizing, false);
  }

  private void doAddOrUpsert(Cell cell, long readpoint, MemStoreSizing memstoreSizing, boolean
      doAdd) {
    MutableSegment currentActive;
    boolean succ = false;
    while (!succ) {
      currentActive = getActive();
      succ = preUpdate(currentActive, cell, memstoreSizing);
      if (succ) {
        if(doAdd) {
          doAdd(currentActive, cell, memstoreSizing);
        } else {
          doUpsert(currentActive, cell, readpoint, memstoreSizing);
        }
        postUpdate(currentActive);
      }
    }
  }

  protected void doAdd(MutableSegment currentActive, Cell cell, MemStoreSizing memstoreSizing) {
    Cell toAdd = maybeCloneWithAllocator(currentActive, cell, false);
    boolean mslabUsed = (toAdd != cell);
    // This cell data is backed by the same byte[] where we read request in RPC(See
    // HBASE-15180). By default MSLAB is ON and we might have copied cell to MSLAB area. If
    // not we must do below deep copy. Or else we will keep referring to the bigger chunk of
    // memory and prevent it from getting GCed.
    // Copy to MSLAB would not have happened if
    // 1. MSLAB is turned OFF. See "hbase.hregion.memstore.mslab.enabled"
    // 2. When the size of the cell is bigger than the max size supported by MSLAB. See
    // "hbase.hregion.memstore.mslab.max.allocation". This defaults to 256 KB
    // 3. When cells are from Append/Increment operation.
    if (!mslabUsed) {
      toAdd = deepCopyIfNeeded(toAdd);
    }
    internalAdd(currentActive, toAdd, mslabUsed, memstoreSizing);
  }

  private void doUpsert(MutableSegment currentActive, Cell cell, long readpoint, MemStoreSizing
      memstoreSizing) {
    // Add the Cell to the MemStore
    // Use the internalAdd method here since we (a) already have a lock
    // and (b) cannot safely use the MSLAB here without potentially
    // hitting OOME - see TestMemStore.testUpsertMSLAB for a
    // test that triggers the pathological case if we don't avoid MSLAB
    // here.
    // This cell data is backed by the same byte[] where we read request in RPC(See
    // HBASE-15180). We must do below deep copy. Or else we will keep referring to the bigger
    // chunk of memory and prevent it from getting GCed.
    cell = deepCopyIfNeeded(cell);
    boolean sizeAddedPreOperation = sizeAddedPreOperation();
    currentActive.upsert(cell, readpoint, memstoreSizing, sizeAddedPreOperation);
    setOldestEditTimeToNow();
  }

    /**
     * Issue any synchronization and test needed before applying the update
     * @param currentActive the segment to be updated
     * @param cell the cell to be added
     * @param memstoreSizing object to accumulate region size changes
     * @return true iff can proceed with applying the update
     */
  protected abstract boolean preUpdate(MutableSegment currentActive, Cell cell,
      MemStoreSizing memstoreSizing);

  /**
   * Issue any post update synchronization and tests
   * @param currentActive updated segment
   */
  protected abstract void postUpdate(MutableSegment currentActive);

  private static Cell deepCopyIfNeeded(Cell cell) {
    if (cell instanceof ExtendedCell) {
      return ((ExtendedCell) cell).deepClone();
    }
    return cell;
  }

  @Override
  public void upsert(Iterable<Cell> cells, long readpoint, MemStoreSizing memstoreSizing) {
    for (Cell cell : cells) {
      upsert(cell, readpoint, memstoreSizing);
    }
  }

  /**
   * @return Oldest timestamp of all the Cells in the MemStore
   */
  @Override
  public long timeOfOldestEdit() {
    return timeOfOldestEdit;
  }

  /**
   * This method is protected under {@link HStore#lock} write lock,<br/>
   * and this method is used by {@link HStore#updateStorefiles} after flushing is completed.<br/>
   * The passed snapshot was successfully persisted; it can be let go.
   * @param id Id of the snapshot to clean out.
   * @see MemStore#snapshot()
   */
  @Override
  public void clearSnapshot(long id) throws UnexpectedStateException {
    if (this.snapshotId == -1) return;  // already cleared
    if (this.snapshotId != id) {
      throw new UnexpectedStateException("Current snapshot id is " + this.snapshotId + ",passed "
          + id);
    }
    // OK. Passed in snapshot is same as current snapshot. If not-empty,
    // create a new snapshot and let the old one go.
    doClearSnapShot();
  }

  protected void doClearSnapShot() {
    Segment oldSnapshot = this.snapshot;
    if (!this.snapshot.isEmpty()) {
      this.snapshot = SegmentFactory.instance().createImmutableSegment(this.comparator);
    }
    this.snapshotId = NO_SNAPSHOT_ID;
    oldSnapshot.close();
  }

  @Override
  public MemStoreSize getSnapshotSize() {
    return this.snapshot.getMemStoreSize();
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    int i = 1;
    try {
      for (Segment segment : getSegments()) {
        buf.append("Segment (").append(i).append(") ").append(segment.toString()).append("; ");
        i++;
      }
    } catch (IOException e){
      return e.toString();
    }
    return buf.toString();
  }

  protected Configuration getConfiguration() {
    return conf;
  }

  protected void dump(Logger log) {
    getActive().dump(log);
    snapshot.dump(log);
  }


  /*
   * @param a
   * @param b
   * @return Return lowest of a or b or null if both a and b are null
   */
  protected Cell getLowest(final Cell a, final Cell b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return comparator.compareRows(a, b) <= 0? a: b;
  }

  /*
   * @param key Find row that follows this one.  If null, return first.
   * @param set Set to look in for a row beyond <code>row</code>.
   * @return Next row or null if none found.  If one found, will be a new
   * KeyValue -- can be destroyed by subsequent calls to this method.
   */
  protected Cell getNextRow(final Cell key,
      final NavigableSet<Cell> set) {
    Cell result = null;
    SortedSet<Cell> tail = key == null? set: set.tailSet(key);
    // Iterate until we fall into the next row; i.e. move off current row
    for (Cell cell: tail) {
      if (comparator.compareRows(cell, key) <= 0) {
        continue;
      }
      // Note: Not suppressing deletes or expired cells.  Needs to be handled
      // by higher up functions.
      result = cell;
      break;
    }
    return result;
  }

  /**
   * If the segment has a memory allocator the cell is being cloned to this space, and returned;
   * Otherwise the given cell is returned
   *
   * When a cell's size is too big (bigger than maxAlloc), it is not allocated on MSLAB.
   * Since the process of flattening to CellChunkMap assumes that all cells are allocated on MSLAB,
   * during this process, the input parameter forceCloneOfBigCell is set to 'true'
   * and the cell is copied into MSLAB.
   *
   * @param cell the cell to clone
   * @param forceCloneOfBigCell true only during the process of flattening to CellChunkMap.
   * @return either the given cell or its clone
   */
  private Cell maybeCloneWithAllocator(MutableSegment currentActive, Cell cell, boolean
      forceCloneOfBigCell) {
    return currentActive.maybeCloneWithAllocator(cell, forceCloneOfBigCell);
  }

  /*
   * Internal version of add() that doesn't clone Cells with the
   * allocator, and doesn't take the lock.
   *
   * Callers should ensure they already have the read lock taken
   * @param toAdd the cell to add
   * @param mslabUsed whether using MSLAB
   * @param memstoreSizing object to accumulate changed size
   */
  private void internalAdd(MutableSegment currentActive, final Cell toAdd, final boolean
      mslabUsed, MemStoreSizing memstoreSizing) {
    boolean sizeAddedPreOperation = sizeAddedPreOperation();
    currentActive.add(toAdd, mslabUsed, memstoreSizing, sizeAddedPreOperation);
    setOldestEditTimeToNow();
  }

  protected abstract boolean sizeAddedPreOperation();

  private void setOldestEditTimeToNow() {
    if (timeOfOldestEdit == Long.MAX_VALUE) {
      timeOfOldestEdit = EnvironmentEdgeManager.currentTime();
    }
  }

  /**
   * @return The total size of cells in this memstore. We will not consider cells in the snapshot
   */
  protected abstract long keySize();

  /**
   * @return The total heap size of cells in this memstore. We will not consider cells in the
   *         snapshot
   */
  protected abstract long heapSize();

  protected CellComparator getComparator() {
    return comparator;
  }

  MutableSegment getActive() {
    return active;
  }

  ImmutableSegment getSnapshot() {
    return snapshot;
  }

  /**
   * @return an ordered list of segments from most recent to oldest in memstore
   */
  protected abstract List<Segment> getSegments() throws IOException;

}
