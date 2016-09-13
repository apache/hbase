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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.ShareableMemory;
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

  /*
  * Calculate how the MemStore size has changed.  Includes overhead of the
  * backing Map.
  * @param cell
  * @param notPresent True if the cell was NOT present in the set.
  * @return change in size
  */
  static long heapSizeChange(final Cell cell, final boolean notPresent) {
    return notPresent ? ClassSize.align(ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY
        + CellUtil.estimatedHeapSizeOf(cell)) : 0;
  }

  /**
   * Updates the wal with the lowest sequence id (oldest entry) that is still in memory
   * @param onlyIfMoreRecent a flag that marks whether to update the sequence id no matter what or
   *                      only if it is greater than the previous sequence id
   */
  public abstract void updateLowestUnflushedSequenceIdInWAL(boolean onlyIfMoreRecent);

  /**
   * Write an update
   * @param cell the cell to be added
   * @return approximate size of the passed cell & newly added cell which maybe different than the
   *         passed-in cell
   */
  @Override
  public long add(Cell cell) {
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
    return internalAdd(toAdd, mslabUsed);
  }

  private static Cell deepCopyIfNeeded(Cell cell) {
    // When Cell is backed by a shared memory chunk (this can be a chunk of memory where we read the
    // req into) the Cell instance will be of type ShareableMemory. Later we will add feature to
    // read the RPC request into pooled direct ByteBuffers.
    if (cell instanceof ShareableMemory) {
      return ((ShareableMemory) cell).cloneToCell();
    }
    return cell;
  }

  /**
   * Update or insert the specified Cells.
   * <p>
   * For each Cell, insert into MemStore.  This will atomically upsert the
   * value for that row/family/qualifier.  If a Cell did already exist,
   * it will then be removed.
   * <p>
   * Currently the memstoreTS is kept at 0 so as each insert happens, it will
   * be immediately visible.  May want to change this so it is atomic across
   * all Cells.
   * <p>
   * This is called under row lock, so Get operations will still see updates
   * atomically.  Scans will only see each Cell update as atomic.
   *
   * @param cells the cells to be updated
   * @param readpoint readpoint below which we can safely remove duplicate KVs
   * @return change in memstore size
   */
  @Override
  public long upsert(Iterable<Cell> cells, long readpoint) {
    long size = 0;
    for (Cell cell : cells) {
      size += upsert(cell, readpoint);
    }
    return size;
  }

  /**
   * @return Oldest timestamp of all the Cells in the MemStore
   */
  @Override
  public long timeOfOldestEdit() {
    return timeOfOldestEdit;
  }


  /**
   * Write a delete
   * @param deleteCell the cell to be deleted
   * @return approximate size of the passed key and value.
   */
  @Override
  public long delete(Cell deleteCell) {
    // Delete operation just adds the delete marker cell coming here.
    return add(deleteCell);
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

  /**
   * Get the entire heap usage for this MemStore not including keys in the
   * snapshot.
   */
  @Override
  public long heapSize() {
    return size();
  }

  @Override
  public long getSnapshotSize() {
    return this.snapshot.keySize();
  }

  @Override
  public String toString() {
    StringBuffer buf = new StringBuffer();
    int i = 1;
    try {
      for (Segment segment : getSegments()) {
        buf.append("Segment (" + i + ") " + segment.toString() + "; ");
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

  protected void dump(Log log) {
    active.dump(log);
    snapshot.dump(log);
  }


  /**
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
   * @return change in size of MemStore
   */
  private long upsert(Cell cell, long readpoint) {
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
    long addedSize = internalAdd(cell, false);

    // Get the Cells for the row/family/qualifier regardless of timestamp.
    // For this case we want to clean up any other puts
    Cell firstCell = KeyValueUtil.createFirstOnRow(
        cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
        cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
        cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
    SortedSet<Cell> ss = active.tailSet(firstCell);
    Iterator<Cell> it = ss.iterator();
    // versions visible to oldest scanner
    int versionsVisible = 0;
    while (it.hasNext()) {
      Cell cur = it.next();

      if (cell == cur) {
        // ignore the one just put in
        continue;
      }
      // check that this is the row and column we are interested in, otherwise bail
      if (CellUtil.matchingRow(cell, cur) && CellUtil.matchingQualifier(cell, cur)) {
        // only remove Puts that concurrent scanners cannot possibly see
        if (cur.getTypeByte() == KeyValue.Type.Put.getCode() &&
            cur.getSequenceId() <= readpoint) {
          if (versionsVisible >= 1) {
            // if we get here we have seen at least one version visible to the oldest scanner,
            // which means we can prove that no scanner will see this version

            // false means there was a change, so give us the size.
            long delta = heapSizeChange(cur, true);
            addedSize -= delta;
            active.incSize(-delta);
            it.remove();
            setOldestEditTimeToNow();
          } else {
            versionsVisible++;
          }
        }
      } else {
        // past the row or column, done
        break;
      }
    }
    return addedSize;
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
   * Given the specs of a column, update it, first by inserting a new record,
   * then removing the old one.  Since there is only 1 KeyValue involved, the memstoreTS
   * will be set to 0, thus ensuring that they instantly appear to anyone. The underlying
   * store will ensure that the insert/delete each are atomic. A scanner/reader will either
   * get the new value, or the old value and all readers will eventually only see the new
   * value after the old was removed.
   */
  @VisibleForTesting
  @Override
  public long updateColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long newValue, long now) {
    Cell firstCell = KeyValueUtil.createFirstOnRow(row, family, qualifier);
    // Is there a Cell in 'snapshot' with the same TS? If so, upgrade the timestamp a bit.
    Cell snc = snapshot.getFirstAfter(firstCell);
    if(snc != null) {
      // is there a matching Cell in the snapshot?
      if (CellUtil.matchingRow(snc, firstCell) && CellUtil.matchingQualifier(snc, firstCell)) {
        if (snc.getTimestamp() == now) {
          now += 1;
        }
      }
    }
    // logic here: the new ts MUST be at least 'now'. But it could be larger if necessary.
    // But the timestamp should also be max(now, mostRecentTsInMemstore)

    // so we cant add the new Cell w/o knowing what's there already, but we also
    // want to take this chance to delete some cells. So two loops (sad)

    SortedSet<Cell> ss = this.active.tailSet(firstCell);
    for (Cell cell : ss) {
      // if this isnt the row we are interested in, then bail:
      if (!CellUtil.matchingColumn(cell, family, qualifier)
          || !CellUtil.matchingRow(cell, firstCell)) {
        break; // rows dont match, bail.
      }

      // if the qualifier matches and it's a put, just RM it out of the active.
      if (cell.getTypeByte() == KeyValue.Type.Put.getCode() &&
          cell.getTimestamp() > now && CellUtil.matchingQualifier(firstCell, cell)) {
        now = cell.getTimestamp();
      }
    }

    // create or update (upsert) a new Cell with
    // 'now' and a 0 memstoreTS == immediately visible
    List<Cell> cells = new ArrayList<Cell>(1);
    cells.add(new KeyValue(row, family, qualifier, now, Bytes.toBytes(newValue)));
    return upsert(cells, 1L);
  }

  private Cell maybeCloneWithAllocator(Cell cell) {
    return active.maybeCloneWithAllocator(cell);
  }

  /**
   * Internal version of add() that doesn't clone Cells with the
   * allocator, and doesn't take the lock.
   *
   * Callers should ensure they already have the read lock taken
   * @param toAdd the cell to add
   * @param mslabUsed whether using MSLAB
   * @return the heap size change in bytes
   */
  private long internalAdd(final Cell toAdd, final boolean mslabUsed) {
    long s = active.add(toAdd, mslabUsed);
    setOldestEditTimeToNow();
    checkActiveSize();
    return s;
  }

  private void setOldestEditTimeToNow() {
    if (timeOfOldestEdit == Long.MAX_VALUE) {
      timeOfOldestEdit = EnvironmentEdgeManager.currentTime();
    }
  }

  /**
   * @return The size of the active segment. Means sum of all cell's size.
   */
  protected long keySize() {
    return this.active.keySize();
  }

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
  protected abstract List<Segment> getSegments() throws IOException;

}
