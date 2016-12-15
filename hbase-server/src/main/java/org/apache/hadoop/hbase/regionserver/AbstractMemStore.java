/**
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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * An abstract class, which implements the behaviour shared by all concrete memstore instances.
 */
@InterfaceAudience.Private
public abstract class AbstractMemStore implements MemStore {

  private static final long NO_SNAPSHOT_ID = -1;

  private final Configuration conf;
  private final CellComparator comparator;

  // active segment absorbs write operations
  protected volatile MutableSegment active;
  // Snapshot of memstore.  Made for flusher.
  protected volatile ImmutableSegment snapshot;
  protected volatile long snapshotId;
  // Used to track when to flush
  private volatile long timeOfOldestEdit;

  public final static long FIXED_OVERHEAD = ClassSize
      .align(ClassSize.OBJECT + (4 * ClassSize.REFERENCE) + (2 * Bytes.SIZEOF_LONG));

  public final static long DEEP_OVERHEAD = FIXED_OVERHEAD;

  protected AbstractMemStore(final Configuration conf, final CellComparator c) {
    this.conf = conf;
    this.comparator = c;
    resetActive();
    this.snapshot = SegmentFactory.instance().createImmutableSegment(c);
    this.snapshotId = NO_SNAPSHOT_ID;
  }

  protected void resetActive() {
    // Reset heap to not include any keys
    this.active = SegmentFactory.instance().createMutableSegment(conf, comparator);
    this.timeOfOldestEdit = Long.MAX_VALUE;
  }

  /**
   * Updates the wal with the lowest sequence id (oldest entry) that is still in memory
   * @param onlyIfMoreRecent a flag that marks whether to update the sequence id no matter what or
   *                      only if it is greater than the previous sequence id
   */
  public abstract void updateLowestUnflushedSequenceIdInWAL(boolean onlyIfMoreRecent);

  @Override
  public void add(Iterable<Cell> cells, MemstoreSize memstoreSize) {
    for (Cell cell : cells) {
      add(cell, memstoreSize);
    }
  }

  @Override
  public void add(Cell cell, MemstoreSize memstoreSize) {
    Cell toAdd = maybeCloneWithAllocator(cell);
    boolean mslabUsed = (toAdd != cell);
    // This cell data is backed by the same byte[] where we read request in RPC(See HBASE-15180). By
    // default MSLAB is ON and we might have copied cell to MSLAB area. If not we must do below deep
    // copy. Or else we will keep referring to the bigger chunk of memory and prevent it from
    // getting GCed.
    // Copy to MSLAB would not have happened if
    // 1. MSLAB is turned OFF. See "hbase.hregion.memstore.mslab.enabled"
    // 2. When the size of the cell is bigger than the max size supported by MSLAB. See
    // "hbase.hregion.memstore.mslab.max.allocation". This defaults to 256 KB
    // 3. When cells are from Append/Increment operation.
    if (!mslabUsed) {
      toAdd = deepCopyIfNeeded(toAdd);
    }
    internalAdd(toAdd, mslabUsed, memstoreSize);
  }

  private static Cell deepCopyIfNeeded(Cell cell) {
    if (cell instanceof ExtendedCell) {
      return ((ExtendedCell) cell).deepClone();
    }
    return cell;
  }

  @Override
  public void upsert(Iterable<Cell> cells, long readpoint, MemstoreSize memstoreSize) {
    for (Cell cell : cells) {
      upsert(cell, readpoint, memstoreSize);
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
    Segment oldSnapshot = this.snapshot;
    if (!this.snapshot.isEmpty()) {
      this.snapshot = SegmentFactory.instance().createImmutableSegment(this.comparator);
    }
    this.snapshotId = NO_SNAPSHOT_ID;
    oldSnapshot.close();
  }

  @Override
  public MemstoreSize getSnapshotSize() {
    return new MemstoreSize(this.snapshot.keySize(), this.snapshot.heapOverhead());
  }

  @Override
  public String toString() {
    StringBuffer buf = new StringBuffer();
    int i = 1;

    for (Segment segment : getSegments()) {
      buf.append("Segment (" + i + ") " + segment.toString() + "; ");
      i++;
    }

    return buf.toString();
  }

  protected Configuration getConfiguration() {
    return conf;
  }

  protected void dump(Log log) {
    active.dump(log);
    snapshot.dump(log);
  }


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
   * @param memstoreSize
   */
  private void upsert(Cell cell, long readpoint, MemstoreSize memstoreSize) {
    // Add the Cell to the MemStore
    // Use the internalAdd method here since we (a) already have a lock
    // and (b) cannot safely use the MSLAB here without potentially
    // hitting OOME - see TestMemStore.testUpsertMSLAB for a
    // test that triggers the pathological case if we don't avoid MSLAB
    // here.
    // This cell data is backed by the same byte[] where we read request in RPC(See HBASE-15180). We
    // must do below deep copy. Or else we will keep referring to the bigger chunk of memory and
    // prevent it from getting GCed.
    cell = deepCopyIfNeeded(cell);
    this.active.upsert(cell, readpoint, memstoreSize);
    setOldestEditTimeToNow();
    checkActiveSize();
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
  @VisibleForTesting
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
   * @param cell Find the row that comes after this one.  If null, we return the
   *             first.
   * @return Next row or null if none found.
   */
  @VisibleForTesting
  Cell getNextRow(final Cell cell) {
    Cell lowest = null;
    List<Segment> segments = getSegments();
    for (Segment segment : segments) {
      if (lowest == null) {
        //TODO: we may want to move the getNextRow ability to the segment
        lowest = getNextRow(cell, segment.getCellSet());
      } else {
        lowest = getLowest(lowest, getNextRow(cell, segment.getCellSet()));
      }
    }
    return lowest;
  }

  private Cell maybeCloneWithAllocator(Cell cell) {
    return active.maybeCloneWithAllocator(cell);
  }

  /*
   * Internal version of add() that doesn't clone Cells with the
   * allocator, and doesn't take the lock.
   *
   * Callers should ensure they already have the read lock taken
   * @param toAdd the cell to add
   * @param mslabUsed whether using MSLAB
   * @param memstoreSize
   */
  private void internalAdd(final Cell toAdd, final boolean mslabUsed, MemstoreSize memstoreSize) {
    active.add(toAdd, mslabUsed, memstoreSize);
    setOldestEditTimeToNow();
    checkActiveSize();
  }

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
   * @return The total heap overhead of cells in this memstore. We will not consider cells in the
   *         snapshot
   */
  protected abstract long heapOverhead();

  protected CellComparator getComparator() {
    return comparator;
  }

  @VisibleForTesting
  MutableSegment getActive() {
    return active;
  }

  @VisibleForTesting
  ImmutableSegment getSnapshot() {
    return snapshot;
  }

  /**
   * Check whether anything need to be done based on the current active set size
   */
  protected abstract void checkActiveSize();

  /**
   * @return an ordered list of segments from most recent to oldest in memstore
   */
  protected abstract List<Segment> getSegments();

}
