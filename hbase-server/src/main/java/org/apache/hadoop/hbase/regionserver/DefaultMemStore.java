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

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.CollectionBackedScanner;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.htrace.Trace;

/**
 * The MemStore holds in-memory modifications to the Store.  Modifications
 * are {@link Cell}s.  When asked to flush, current memstore is moved
 * to snapshot and is cleared.  We continue to serve edits out of new memstore
 * and backing snapshot until flusher reports in that the flush succeeded. At
 * this point we let the snapshot go.
 *  <p>
 * The MemStore functions should not be called in parallel. Callers should hold
 *  write and read locks. This is done in {@link HStore}.
 *  </p>
 *
 * TODO: Adjust size of the memstore when we remove items because they have
 * been deleted.
 * TODO: With new KVSLS, need to make sure we update HeapSize with difference
 * in KV size.
 */
@InterfaceAudience.Private
public class DefaultMemStore implements MemStore {
  private static final Log LOG = LogFactory.getLog(DefaultMemStore.class);
  static final String USEMSLAB_KEY = "hbase.hregion.memstore.mslab.enabled";
  private static final boolean USEMSLAB_DEFAULT = true;
  static final String MSLAB_CLASS_NAME = "hbase.regionserver.mslab.class";

  private Configuration conf;

  // MemStore.  Use a CellSkipListSet rather than SkipListSet because of the
  // better semantics.  The Map will overwrite if passed a key it already had
  // whereas the Set will not add new Cell if key is same though value might be
  // different.  Value is not important -- just make sure always same
  // reference passed.
  volatile CellSkipListSet cellSet;

  // Snapshot of memstore.  Made for flusher.
  volatile CellSkipListSet snapshot;

  final KeyValue.KVComparator comparator;

  // Used to track own heapSize
  final AtomicLong size;
  private volatile long snapshotSize;

  // Used to track when to flush
  volatile long timeOfOldestEdit = Long.MAX_VALUE;

  TimeRangeTracker timeRangeTracker;
  TimeRangeTracker snapshotTimeRangeTracker;

  volatile MemStoreLAB allocator;
  volatile MemStoreLAB snapshotAllocator;
  volatile long snapshotId;

  /**
   * Default constructor. Used for tests.
   */
  public DefaultMemStore() {
    this(HBaseConfiguration.create(), KeyValue.COMPARATOR);
  }

  /**
   * Constructor.
   * @param c Comparator
   */
  public DefaultMemStore(final Configuration conf,
                  final KeyValue.KVComparator c) {
    this.conf = conf;
    this.comparator = c;
    this.cellSet = new CellSkipListSet(c);
    this.snapshot = new CellSkipListSet(c);
    timeRangeTracker = new TimeRangeTracker();
    snapshotTimeRangeTracker = new TimeRangeTracker();
    this.size = new AtomicLong(DEEP_OVERHEAD);
    this.snapshotSize = 0;
    if (conf.getBoolean(USEMSLAB_KEY, USEMSLAB_DEFAULT)) {
      String className = conf.get(MSLAB_CLASS_NAME, HeapMemStoreLAB.class.getName());
      this.allocator = ReflectionUtils.instantiateWithCustomCtor(className,
          new Class[] { Configuration.class }, new Object[] { conf });
    } else {
      this.allocator = null;
    }
  }

  void dump() {
    for (Cell cell: this.cellSet) {
      LOG.info(cell);
    }
    for (Cell cell: this.snapshot) {
      LOG.info(cell);
    }
  }

  /**
   * Creates a snapshot of the current memstore.
   * Snapshot must be cleared by call to {@link #clearSnapshot(long)}
   */
  @Override
  public MemStoreSnapshot snapshot() {
    // If snapshot currently has entries, then flusher failed or didn't call
    // cleanup.  Log a warning.
    if (!this.snapshot.isEmpty()) {
      LOG.warn("Snapshot called again without clearing previous. " +
          "Doing nothing. Another ongoing flush or did we fail last attempt?");
    } else {
      this.snapshotId = EnvironmentEdgeManager.currentTime();
      this.snapshotSize = keySize();
      if (!this.cellSet.isEmpty()) {
        this.snapshot = this.cellSet;
        this.cellSet = new CellSkipListSet(this.comparator);
        this.snapshotTimeRangeTracker = this.timeRangeTracker;
        this.timeRangeTracker = new TimeRangeTracker();
        // Reset heap to not include any keys
        this.size.set(DEEP_OVERHEAD);
        this.snapshotAllocator = this.allocator;
        // Reset allocator so we get a fresh buffer for the new memstore
        if (allocator != null) {
          String className = conf.get(MSLAB_CLASS_NAME, HeapMemStoreLAB.class.getName());
          this.allocator = ReflectionUtils.instantiateWithCustomCtor(className,
              new Class[] { Configuration.class }, new Object[] { conf });
        }
        timeOfOldestEdit = Long.MAX_VALUE;
      }
    }
    return new MemStoreSnapshot(this.snapshotId, snapshot.size(), this.snapshotSize,
        this.snapshotTimeRangeTracker, new CollectionBackedScanner(snapshot, this.comparator));
  }

  /**
   * The passed snapshot was successfully persisted; it can be let go.
   * @param id Id of the snapshot to clean out.
   * @throws UnexpectedStateException
   * @see #snapshot()
   */
  @Override
  public void clearSnapshot(long id) throws UnexpectedStateException {
    MemStoreLAB tmpAllocator = null;
    if (this.snapshotId != id) {
      throw new UnexpectedStateException("Current snapshot id is " + this.snapshotId + ",passed "
          + id);
    }
    // OK. Passed in snapshot is same as current snapshot. If not-empty,
    // create a new snapshot and let the old one go.
    if (!this.snapshot.isEmpty()) {
      this.snapshot = new CellSkipListSet(this.comparator);
      this.snapshotTimeRangeTracker = new TimeRangeTracker();
    }
    this.snapshotSize = 0;
    this.snapshotId = -1;
    if (this.snapshotAllocator != null) {
      tmpAllocator = this.snapshotAllocator;
      this.snapshotAllocator = null;
    }
    if (tmpAllocator != null) {
      tmpAllocator.close();
    }
  }

  @Override
  public long getFlushableSize() {
    return this.snapshotSize > 0 ? this.snapshotSize : keySize();
  }

  @Override
  public long getSnapshotSize() {
    return this.snapshotSize;
  }

  /**
   * Write an update
   * @param cell
   * @return approximate size of the passed KV & newly added KV which maybe different than the
   *         passed-in KV
   */
  @Override
  public Pair<Long, Cell> add(Cell cell) {
    Cell toAdd = maybeCloneWithAllocator(cell);
    return new Pair<Long, Cell>(internalAdd(toAdd), toAdd);
  }

  @Override
  public long timeOfOldestEdit() {
    return timeOfOldestEdit;
  }

  private boolean addToCellSet(Cell e) {
    boolean b = this.cellSet.add(e);
    setOldestEditTimeToNow();
    return b;
  }

  private boolean removeFromCellSet(Cell e) {
    boolean b = this.cellSet.remove(e);
    setOldestEditTimeToNow();
    return b;
  }

  void setOldestEditTimeToNow() {
    if (timeOfOldestEdit == Long.MAX_VALUE) {
      timeOfOldestEdit = EnvironmentEdgeManager.currentTime();
    }
  }

  /**
   * Internal version of add() that doesn't clone Cells with the
   * allocator, and doesn't take the lock.
   *
   * Callers should ensure they already have the read lock taken
   */
  private long internalAdd(final Cell toAdd) {
    long s = heapSizeChange(toAdd, addToCellSet(toAdd));
    timeRangeTracker.includeTimestamp(toAdd);
    this.size.addAndGet(s);
    return s;
  }

  private Cell maybeCloneWithAllocator(Cell cell) {
    if (allocator == null) {
      return cell;
    }

    int len = KeyValueUtil.length(cell);
    ByteRange alloc = allocator.allocateBytes(len);
    if (alloc == null) {
      // The allocation was too large, allocator decided
      // not to do anything with it.
      return cell;
    }
    assert alloc.getBytes() != null;
    KeyValueUtil.appendToByteArray(cell, alloc.getBytes(), alloc.getOffset());
    KeyValue newKv = new KeyValue(alloc.getBytes(), alloc.getOffset(), len);
    newKv.setSequenceId(cell.getSequenceId());
    return newKv;
  }

  /**
   * Remove n key from the memstore. Only cells that have the same key and the
   * same memstoreTS are removed.  It is ok to not update timeRangeTracker
   * in this call. It is possible that we can optimize this method by using
   * tailMap/iterator, but since this method is called rarely (only for
   * error recovery), we can leave those optimization for the future.
   * @param cell
   */
  @Override
  public void rollback(Cell cell) {
    // If the key is in the snapshot, delete it. We should not update
    // this.size, because that tracks the size of only the memstore and
    // not the snapshot. The flush of this snapshot to disk has not
    // yet started because Store.flush() waits for all rwcc transactions to
    // commit before starting the flush to disk.
    Cell found = this.snapshot.get(cell);
    if (found != null && found.getSequenceId() == cell.getSequenceId()) {
      this.snapshot.remove(cell);
      long sz = heapSizeChange(cell, true);
      this.snapshotSize -= sz;
    }
    // If the key is in the memstore, delete it. Update this.size.
    found = this.cellSet.get(cell);
    if (found != null && found.getSequenceId() == cell.getSequenceId()) {
      removeFromCellSet(cell);
      long s = heapSizeChange(cell, true);
      this.size.addAndGet(-s);
    }
  }

  /**
   * Write a delete
   * @param deleteCell
   * @return approximate size of the passed key and value.
   */
  @Override
  public long delete(Cell deleteCell) {
    long s = 0;
    Cell toAdd = maybeCloneWithAllocator(deleteCell);
    s += heapSizeChange(toAdd, addToCellSet(toAdd));
    timeRangeTracker.includeTimestamp(toAdd);
    this.size.addAndGet(s);
    return s;
  }

  /**
   * @param cell Find the row that comes after this one.  If null, we return the
   * first.
   * @return Next row or null if none found.
   */
  Cell getNextRow(final Cell cell) {
    return getLowest(getNextRow(cell, this.cellSet), getNextRow(cell, this.snapshot));
  }

  /*
   * @param a
   * @param b
   * @return Return lowest of a or b or null if both a and b are null
   */
  private Cell getLowest(final Cell a, final Cell b) {
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
   * @param map Set to look in for a row beyond <code>row</code>.
   * @return Next row or null if none found.  If one found, will be a new
   * KeyValue -- can be destroyed by subsequent calls to this method.
   */
  private Cell getNextRow(final Cell key,
      final NavigableSet<Cell> set) {
    Cell result = null;
    SortedSet<Cell> tail = key == null? set: set.tailSet(key);
    // Iterate until we fall into the next row; i.e. move off current row
    for (Cell cell: tail) {
      if (comparator.compareRows(cell, key) <= 0)
        continue;
      // Note: Not suppressing deletes or expired cells.  Needs to be handled
      // by higher up functions.
      result = cell;
      break;
    }
    return result;
  }

  /**
   * @param state column/delete tracking state
   */
  @Override
  public void getRowKeyAtOrBefore(final GetClosestRowBeforeTracker state) {
    getRowKeyAtOrBefore(cellSet, state);
    getRowKeyAtOrBefore(snapshot, state);
  }

  /*
   * @param set
   * @param state Accumulates deletes and candidates.
   */
  private void getRowKeyAtOrBefore(final NavigableSet<Cell> set,
      final GetClosestRowBeforeTracker state) {
    if (set.isEmpty()) {
      return;
    }
    if (!walkForwardInSingleRow(set, state.getTargetKey(), state)) {
      // Found nothing in row.  Try backing up.
      getRowKeyBefore(set, state);
    }
  }

  /*
   * Walk forward in a row from <code>firstOnRow</code>.  Presumption is that
   * we have been passed the first possible key on a row.  As we walk forward
   * we accumulate deletes until we hit a candidate on the row at which point
   * we return.
   * @param set
   * @param firstOnRow First possible key on this row.
   * @param state
   * @return True if we found a candidate walking this row.
   */
  private boolean walkForwardInSingleRow(final SortedSet<Cell> set,
      final Cell firstOnRow, final GetClosestRowBeforeTracker state) {
    boolean foundCandidate = false;
    SortedSet<Cell> tail = set.tailSet(firstOnRow);
    if (tail.isEmpty()) return foundCandidate;
    for (Iterator<Cell> i = tail.iterator(); i.hasNext();) {
      Cell kv = i.next();
      // Did we go beyond the target row? If so break.
      if (state.isTooFar(kv, firstOnRow)) break;
      if (state.isExpired(kv)) {
        i.remove();
        continue;
      }
      // If we added something, this row is a contender. break.
      if (state.handle(kv)) {
        foundCandidate = true;
        break;
      }
    }
    return foundCandidate;
  }

  /*
   * Walk backwards through the passed set a row at a time until we run out of
   * set or until we get a candidate.
   * @param set
   * @param state
   */
  private void getRowKeyBefore(NavigableSet<Cell> set,
      final GetClosestRowBeforeTracker state) {
    Cell firstOnRow = state.getTargetKey();
    for (Member p = memberOfPreviousRow(set, state, firstOnRow);
        p != null; p = memberOfPreviousRow(p.set, state, firstOnRow)) {
      // Make sure we don't fall out of our table.
      if (!state.isTargetTable(p.cell)) break;
      // Stop looking if we've exited the better candidate range.
      if (!state.isBetterCandidate(p.cell)) break;
      // Make into firstOnRow
      firstOnRow = new KeyValue(p.cell.getRowArray(), p.cell.getRowOffset(), p.cell.getRowLength(),
          HConstants.LATEST_TIMESTAMP);
      // If we find something, break;
      if (walkForwardInSingleRow(p.set, firstOnRow, state)) break;
    }
  }

  /**
   * Only used by tests. TODO: Remove
   *
   * Given the specs of a column, update it, first by inserting a new record,
   * then removing the old one.  Since there is only 1 KeyValue involved, the memstoreTS
   * will be set to 0, thus ensuring that they instantly appear to anyone. The underlying
   * store will ensure that the insert/delete each are atomic. A scanner/reader will either
   * get the new value, or the old value and all readers will eventually only see the new
   * value after the old was removed.
   *
   * @param row
   * @param family
   * @param qualifier
   * @param newValue
   * @param now
   * @return  Timestamp
   */
  @Override
  public long updateColumnValue(byte[] row,
                                byte[] family,
                                byte[] qualifier,
                                long newValue,
                                long now) {
    Cell firstCell = KeyValueUtil.createFirstOnRow(row, family, qualifier);
    // Is there a Cell in 'snapshot' with the same TS? If so, upgrade the timestamp a bit.
    SortedSet<Cell> snSs = snapshot.tailSet(firstCell);
    if (!snSs.isEmpty()) {
      Cell snc = snSs.first();
      // is there a matching Cell in the snapshot?
      if (CellUtil.matchingRow(snc, firstCell) && CellUtil.matchingQualifier(snc, firstCell)) {
        if (snc.getTimestamp() == now) {
          // poop,
          now += 1;
        }
      }
    }

    // logic here: the new ts MUST be at least 'now'. But it could be larger if necessary.
    // But the timestamp should also be max(now, mostRecentTsInMemstore)

    // so we cant add the new Cell w/o knowing what's there already, but we also
    // want to take this chance to delete some cells. So two loops (sad)

    SortedSet<Cell> ss = cellSet.tailSet(firstCell);
    for (Cell cell : ss) {
      // if this isnt the row we are interested in, then bail:
      if (!CellUtil.matchingColumn(cell, family, qualifier)
          || !CellUtil.matchingRow(cell, firstCell)) {
        break; // rows dont match, bail.
      }

      // if the qualifier matches and it's a put, just RM it out of the cellSet.
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

  /**
   * Update or insert the specified KeyValues.
   * <p>
   * For each KeyValue, insert into MemStore.  This will atomically upsert the
   * value for that row/family/qualifier.  If a KeyValue did already exist,
   * it will then be removed.
   * <p>
   * Currently the memstoreTS is kept at 0 so as each insert happens, it will
   * be immediately visible.  May want to change this so it is atomic across
   * all KeyValues.
   * <p>
   * This is called under row lock, so Get operations will still see updates
   * atomically.  Scans will only see each KeyValue update as atomic.
   *
   * @param cells
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
   * Inserts the specified KeyValue into MemStore and deletes any existing
   * versions of the same row/family/qualifier as the specified KeyValue.
   * <p>
   * First, the specified KeyValue is inserted into the Memstore.
   * <p>
   * If there are any existing KeyValues in this MemStore with the same row,
   * family, and qualifier, they are removed.
   * <p>
   * Callers must hold the read lock.
   *
   * @param cell
   * @return change in size of MemStore
   */
  private long upsert(Cell cell, long readpoint) {
    // Add the Cell to the MemStore
    // Use the internalAdd method here since we (a) already have a lock
    // and (b) cannot safely use the MSLAB here without potentially
    // hitting OOME - see TestMemStore.testUpsertMSLAB for a
    // test that triggers the pathological case if we don't avoid MSLAB
    // here.
    long addedSize = internalAdd(cell);

    // Get the Cells for the row/family/qualifier regardless of timestamp.
    // For this case we want to clean up any other puts
    Cell firstCell = KeyValueUtil.createFirstOnRow(
        cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
        cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
        cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
    SortedSet<Cell> ss = cellSet.tailSet(firstCell);
    Iterator<Cell> it = ss.iterator();
    // versions visible to oldest scanner
    int versionsVisible = 0;
    while ( it.hasNext() ) {
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
            this.size.addAndGet(-delta);
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
   * Immutable data structure to hold member found in set and the set it was
   * found in. Include set because it is carrying context.
   */
  private static class Member {
    final Cell cell;
    final NavigableSet<Cell> set;
    Member(final NavigableSet<Cell> s, final Cell kv) {
      this.cell = kv;
      this.set = s;
    }
  }

  /*
   * @param set Set to walk back in.  Pass a first in row or we'll return
   * same row (loop).
   * @param state Utility and context.
   * @param firstOnRow First item on the row after the one we want to find a
   * member in.
   * @return Null or member of row previous to <code>firstOnRow</code>
   */
  private Member memberOfPreviousRow(NavigableSet<Cell> set,
      final GetClosestRowBeforeTracker state, final Cell firstOnRow) {
    NavigableSet<Cell> head = set.headSet(firstOnRow, false);
    if (head.isEmpty()) return null;
    for (Iterator<Cell> i = head.descendingIterator(); i.hasNext();) {
      Cell found = i.next();
      if (state.isExpired(found)) {
        i.remove();
        continue;
      }
      return new Member(head, found);
    }
    return null;
  }

  /**
   * @return scanner on memstore and snapshot in this order.
   */
  @Override
  public List<KeyValueScanner> getScanners(long readPt) {
    return Collections.<KeyValueScanner> singletonList(new MemStoreScanner(readPt));
  }

  /**
   * Check if this memstore may contain the required keys
   * @param scan
   * @return False if the key definitely does not exist in this Memstore
   */
  public boolean shouldSeek(Scan scan, long oldestUnexpiredTS) {
    return (timeRangeTracker.includesTimeRange(scan.getTimeRange()) ||
        snapshotTimeRangeTracker.includesTimeRange(scan.getTimeRange()))
        && (Math.max(timeRangeTracker.getMaximumTimestamp(),
                     snapshotTimeRangeTracker.getMaximumTimestamp()) >=
            oldestUnexpiredTS);
  }

  /*
   * MemStoreScanner implements the KeyValueScanner.
   * It lets the caller scan the contents of a memstore -- both current
   * map and snapshot.
   * This behaves as if it were a real scanner but does not maintain position.
   */
  protected class MemStoreScanner extends NonLazyKeyValueScanner {
    // Next row information for either cellSet or snapshot
    private Cell cellSetNextRow = null;
    private Cell snapshotNextRow = null;

    // last iterated Cells for cellSet and snapshot (to restore iterator state after reseek)
    private Cell cellSetItRow = null;
    private Cell snapshotItRow = null;
    
    // iterator based scanning.
    private Iterator<Cell> cellSetIt;
    private Iterator<Cell> snapshotIt;

    // The cellSet and snapshot at the time of creating this scanner
    private CellSkipListSet cellSetAtCreation;
    private CellSkipListSet snapshotAtCreation;

    // the pre-calculated Cell to be returned by peek() or next()
    private Cell theNext;

    // The allocator and snapshot allocator at the time of creating this scanner
    volatile MemStoreLAB allocatorAtCreation;
    volatile MemStoreLAB snapshotAllocatorAtCreation;
    
    // A flag represents whether could stop skipping Cells for MVCC
    // if have encountered the next row. Only used for reversed scan
    private boolean stopSkippingCellsIfNextRow = false;

    private long readPoint;

    /*
    Some notes...

     So memstorescanner is fixed at creation time. this includes pointers/iterators into
    existing kvset/snapshot.  during a snapshot creation, the kvset is null, and the
    snapshot is moved.  since kvset is null there is no point on reseeking on both,
      we can save us the trouble. During the snapshot->hfile transition, the memstore
      scanner is re-created by StoreScanner#updateReaders().  StoreScanner should
      potentially do something smarter by adjusting the existing memstore scanner.

      But there is a greater problem here, that being once a scanner has progressed
      during a snapshot scenario, we currently iterate past the kvset then 'finish' up.
      if a scan lasts a little while, there is a chance for new entries in kvset to
      become available but we will never see them.  This needs to be handled at the
      StoreScanner level with coordination with MemStoreScanner.

      Currently, this problem is only partly managed: during the small amount of time
      when the StoreScanner has not yet created a new MemStoreScanner, we will miss
      the adds to kvset in the MemStoreScanner.
    */

    MemStoreScanner(long readPoint) {
      super();

      this.readPoint = readPoint;
      cellSetAtCreation = cellSet;
      snapshotAtCreation = snapshot;
      if (allocator != null) {
        this.allocatorAtCreation = allocator;
        this.allocatorAtCreation.incScannerCount();
      }
      if (snapshotAllocator != null) {
        this.snapshotAllocatorAtCreation = snapshotAllocator;
        this.snapshotAllocatorAtCreation.incScannerCount();
      }
      if (Trace.isTracing() && Trace.currentSpan() != null) {
        Trace.currentSpan().addTimelineAnnotation("Creating MemStoreScanner");
      }
    }

    /**
     * Lock on 'this' must be held by caller.
     * @param it
     * @return Next Cell
     */
    private Cell getNext(Iterator<Cell> it) {
      Cell startCell = theNext;
      Cell v = null;
      try {
        while (it.hasNext()) {
          v = it.next();
          if (v.getSequenceId() <= this.readPoint) {
            return v;
          }
          if (stopSkippingCellsIfNextRow && startCell != null
              && comparator.compareRows(v, startCell) > 0) {
            return null;
          }
        }

        return null;
      } finally {
        if (v != null) {
          // in all cases, remember the last Cell iterated to
          if (it == snapshotIt) {
            snapshotItRow = v;
          } else {
            cellSetItRow = v;
          }
        }
      }
    }

    /**
     *  Set the scanner at the seek key.
     *  Must be called only once: there is no thread safety between the scanner
     *   and the memStore.
     * @param key seek value
     * @return false if the key is null or if there is no data
     */
    @Override
    public synchronized boolean seek(Cell key) {
      if (key == null) {
        close();
        return false;
      }
      // kvset and snapshot will never be null.
      // if tailSet can't find anything, SortedSet is empty (not null).
      cellSetIt = cellSetAtCreation.tailSet(key).iterator();
      snapshotIt = snapshotAtCreation.tailSet(key).iterator();
      cellSetItRow = null;
      snapshotItRow = null;

      return seekInSubLists(key);
    }


    /**
     * (Re)initialize the iterators after a seek or a reseek.
     */
    private synchronized boolean seekInSubLists(Cell key){
      cellSetNextRow = getNext(cellSetIt);
      snapshotNextRow = getNext(snapshotIt);

      // Calculate the next value
      theNext = getLowest(cellSetNextRow, snapshotNextRow);

      // has data
      return (theNext != null);
    }


    /**
     * Move forward on the sub-lists set previously by seek.
     * @param key seek value (should be non-null)
     * @return true if there is at least one KV to read, false otherwise
     */
    @Override
    public synchronized boolean reseek(Cell key) {
      /*
      See HBASE-4195 & HBASE-3855 & HBASE-6591 for the background on this implementation.
      This code is executed concurrently with flush and puts, without locks.
      Two points must be known when working on this code:
      1) It's not possible to use the 'kvTail' and 'snapshot'
       variables, as they are modified during a flush.
      2) The ideal implementation for performance would use the sub skip list
       implicitly pointed by the iterators 'kvsetIt' and
       'snapshotIt'. Unfortunately the Java API does not offer a method to
       get it. So we remember the last keys we iterated to and restore
       the reseeked set to at least that point.
       */
      cellSetIt = cellSetAtCreation.tailSet(getHighest(key, cellSetItRow)).iterator();
      snapshotIt = snapshotAtCreation.tailSet(getHighest(key, snapshotItRow)).iterator();

      return seekInSubLists(key);
    }


    @Override
    public synchronized Cell peek() {
      //DebugPrint.println(" MS@" + hashCode() + " peek = " + getLowest());
      return theNext;
    }

    @Override
    public synchronized Cell next() {
      if (theNext == null) {
          return null;
      }

      final Cell ret = theNext;

      // Advance one of the iterators
      if (theNext == cellSetNextRow) {
        cellSetNextRow = getNext(cellSetIt);
      } else {
        snapshotNextRow = getNext(snapshotIt);
      }

      // Calculate the next value
      theNext = getLowest(cellSetNextRow, snapshotNextRow);

      //long readpoint = ReadWriteConsistencyControl.getThreadReadPoint();
      //DebugPrint.println(" MS@" + hashCode() + " next: " + theNext + " next_next: " +
      //    getLowest() + " threadpoint=" + readpoint);
      return ret;
    }

    /*
     * Returns the lower of the two key values, or null if they are both null.
     * This uses comparator.compare() to compare the KeyValue using the memstore
     * comparator.
     */
    private Cell getLowest(Cell first, Cell second) {
      if (first == null && second == null) {
        return null;
      }
      if (first != null && second != null) {
        int compare = comparator.compare(first, second);
        return (compare <= 0 ? first : second);
      }
      return (first != null ? first : second);
    }

    /*
     * Returns the higher of the two cells, or null if they are both null.
     * This uses comparator.compare() to compare the Cell using the memstore
     * comparator.
     */
    private Cell getHighest(Cell first, Cell second) {
      if (first == null && second == null) {
        return null;
      }
      if (first != null && second != null) {
        int compare = comparator.compare(first, second);
        return (compare > 0 ? first : second);
      }
      return (first != null ? first : second);
    }

    public synchronized void close() {
      this.cellSetNextRow = null;
      this.snapshotNextRow = null;

      this.cellSetIt = null;
      this.snapshotIt = null;
      
      if (allocatorAtCreation != null) {
        this.allocatorAtCreation.decScannerCount();
        this.allocatorAtCreation = null;
      }
      if (snapshotAllocatorAtCreation != null) {
        this.snapshotAllocatorAtCreation.decScannerCount();
        this.snapshotAllocatorAtCreation = null;
      }

      this.cellSetItRow = null;
      this.snapshotItRow = null;
    }

    /**
     * MemStoreScanner returns max value as sequence id because it will
     * always have the latest data among all files.
     */
    @Override
    public long getSequenceID() {
      return Long.MAX_VALUE;
    }

    @Override
    public boolean shouldUseScanner(Scan scan, SortedSet<byte[]> columns,
        long oldestUnexpiredTS) {
      return shouldSeek(scan, oldestUnexpiredTS);
    }

    /**
     * Seek scanner to the given key first. If it returns false(means
     * peek()==null) or scanner's peek row is bigger than row of given key, seek
     * the scanner to the previous row of given key
     */
    @Override
    public synchronized boolean backwardSeek(Cell key) {
      seek(key);
      if (peek() == null || comparator.compareRows(peek(), key) > 0) {
        return seekToPreviousRow(key);
      }
      return true;
    }

    /**
     * Separately get the KeyValue before the specified key from kvset and
     * snapshotset, and use the row of higher one as the previous row of
     * specified key, then seek to the first KeyValue of previous row
     */
    @Override
    public synchronized boolean seekToPreviousRow(Cell key) {
      Cell firstKeyOnRow = KeyValueUtil.createFirstOnRow(key.getRowArray(), key.getRowOffset(),
          key.getRowLength());
      SortedSet<Cell> cellHead = cellSetAtCreation.headSet(firstKeyOnRow);
      Cell cellSetBeforeRow = cellHead.isEmpty() ? null : cellHead.last();
      SortedSet<Cell> snapshotHead = snapshotAtCreation
          .headSet(firstKeyOnRow);
      Cell snapshotBeforeRow = snapshotHead.isEmpty() ? null : snapshotHead
          .last();
      Cell lastCellBeforeRow = getHighest(cellSetBeforeRow, snapshotBeforeRow);
      if (lastCellBeforeRow == null) {
        theNext = null;
        return false;
      }
      Cell firstKeyOnPreviousRow = KeyValueUtil.createFirstOnRow(lastCellBeforeRow.getRowArray(),
          lastCellBeforeRow.getRowOffset(), lastCellBeforeRow.getRowLength());
      this.stopSkippingCellsIfNextRow = true;
      seek(firstKeyOnPreviousRow);
      this.stopSkippingCellsIfNextRow = false;
      if (peek() == null
          || comparator.compareRows(peek(), firstKeyOnPreviousRow) > 0) {
        return seekToPreviousRow(lastCellBeforeRow);
      }
      return true;
    }

    @Override
    public synchronized boolean seekToLastRow() {
      Cell first = cellSetAtCreation.isEmpty() ? null : cellSetAtCreation
          .last();
      Cell second = snapshotAtCreation.isEmpty() ? null
          : snapshotAtCreation.last();
      Cell higherCell = getHighest(first, second);
      if (higherCell == null) {
        return false;
      }
      Cell firstCellOnLastRow = KeyValueUtil.createFirstOnRow(higherCell.getRowArray(),
          higherCell.getRowOffset(), higherCell.getRowLength());
      if (seek(firstCellOnLastRow)) {
        return true;
      } else {
        return seekToPreviousRow(higherCell);
      }

    }
  }

  public final static long FIXED_OVERHEAD = ClassSize.align(
      ClassSize.OBJECT + (9 * ClassSize.REFERENCE) + (3 * Bytes.SIZEOF_LONG));

  public final static long DEEP_OVERHEAD = ClassSize.align(FIXED_OVERHEAD +
      ClassSize.ATOMIC_LONG + (2 * ClassSize.TIMERANGE_TRACKER) +
      (2 * ClassSize.CELL_SKIPLIST_SET) + (2 * ClassSize.CONCURRENT_SKIPLISTMAP));

  /*
   * Calculate how the MemStore size has changed.  Includes overhead of the
   * backing Map.
   * @param cell
   * @param notpresent True if the cell was NOT present in the set.
   * @return Size
   */
  static long heapSizeChange(final Cell cell, final boolean notpresent) {
    return notpresent ? ClassSize.align(ClassSize.CONCURRENT_SKIPLISTMAP_ENTRY
        + CellUtil.estimatedHeapSizeOf(cell)) : 0;
  }

  private long keySize() {
    return heapSize() - DEEP_OVERHEAD;
  }

  /**
   * Get the entire heap usage for this MemStore not including keys in the
   * snapshot.
   */
  @Override
  public long heapSize() {
    return size.get();
  }

  @Override
  public long size() {
    return heapSize();
  }

  /**
   * Code to help figure if our approximation of object heap sizes is close
   * enough.  See hbase-900.  Fills memstores then waits so user can heap
   * dump and bring up resultant hprof in something like jprofiler which
   * allows you get 'deep size' on objects.
   * @param args main args
   */
  public static void main(String [] args) {
    RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
    LOG.info("vmName=" + runtime.getVmName() + ", vmVendor=" +
      runtime.getVmVendor() + ", vmVersion=" + runtime.getVmVersion());
    LOG.info("vmInputArguments=" + runtime.getInputArguments());
    DefaultMemStore memstore1 = new DefaultMemStore();
    // TODO: x32 vs x64
    long size = 0;
    final int count = 10000;
    byte [] fam = Bytes.toBytes("col");
    byte [] qf = Bytes.toBytes("umn");
    byte [] empty = new byte[0];
    for (int i = 0; i < count; i++) {
      // Give each its own ts
      Pair<Long, Cell> ret = memstore1.add(new KeyValue(Bytes.toBytes(i), fam, qf, i, empty));
      size += ret.getFirst();
    }
    LOG.info("memstore1 estimated size=" + size);
    for (int i = 0; i < count; i++) {
      Pair<Long, Cell> ret = memstore1.add(new KeyValue(Bytes.toBytes(i), fam, qf, i, empty));
      size += ret.getFirst();
    }
    LOG.info("memstore1 estimated size (2nd loading of same data)=" + size);
    // Make a variably sized memstore.
    DefaultMemStore memstore2 = new DefaultMemStore();
    for (int i = 0; i < count; i++) {
      Pair<Long, Cell> ret = memstore2.add(new KeyValue(Bytes.toBytes(i), fam, qf, i,
        new byte[i]));
      size += ret.getFirst();
    }
    LOG.info("memstore2 estimated size=" + size);
    final int seconds = 30;
    LOG.info("Waiting " + seconds + " seconds while heap dump is taken");
    for (int i = 0; i < seconds; i++) {
      // Thread.sleep(1000);
    }
    LOG.info("Exiting.");
  }

}
