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

package org.apache.hadoop.hbase.procedure2.store.wal;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStoreTracker;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore.ProcedureIterator;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureWALEntry;

/**
 * Helper class that loads the procedures stored in a WAL
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProcedureWALFormatReader {
  private static final Log LOG = LogFactory.getLog(ProcedureWALFormatReader.class);

  // ==============================================================================================
  //  We read the WALs in reverse order. from the newest to the oldest.
  //  We have different entry types:
  //   - INIT: Procedure submitted by the user (also known as 'root procedure')
  //   - INSERT: Children added to the procedure <parentId>:[<childId>, ...]
  //   - UPDATE: The specified procedure was updated
  //   - DELETE: The procedure was removed (completed/rolledback and result TTL expired)
  //
  // In the WAL we can find multiple times the same procedure as UPDATE or INSERT.
  // We read the WAL from top to bottom, so every time we find an entry of the
  // same procedure, that will be the "latest" update.
  //
  // We keep two in-memory maps:
  //  - localProcedureMap: is the map containing the entries in the WAL we are processing
  //  - procedureMap: is the map containing all the procedures we found up to the WAL in process.
  // localProcedureMap is merged with the procedureMap once we reach the WAL EOF.
  //
  // Since we are reading the WALs in reverse order (newest to oldest),
  // if we find an entry related to a procedure we already have in 'procedureMap' we can discard it.
  //
  // The WAL is append-only so the last procedure in the WAL is the one that
  // was in execution at the time we crashed/closed the server.
  // given that, the procedure replay order can be inferred by the WAL order.
  //
  // Example:
  //    WAL-2: [A, B, A, C, D]
  //    WAL-1: [F, G, A, F, B]
  //    Replay-Order: [D, C, A, B, F, G]
  //
  // The "localProcedureMap" keeps a "replayOrder" list. Every time we add the
  // record to the map that record is moved to the head of the "replayOrder" list.
  // Using the example above:
  //    WAL-2 localProcedureMap.replayOrder is [D, C, A, B]
  //    WAL-1 localProcedureMap.replayOrder is [F, G]
  //
  // each time we reach the WAL-EOF, the "replayOrder" list is merged/appended in 'procedureMap'
  // so using the example above we end up with: [D, C, A, B] + [F, G] as replay order.
  //
  //  Fast Start: INIT/INSERT record and StackIDs
  // ---------------------------------------------
  // We have two special record, INIT and INSERT that tracks the first time
  // the procedure was added to the WAL. We can use that information to be able
  // to start procedures before reaching the end of the WAL, or before reading all the WALs.
  // but in some cases the WAL with that record can be already gone.
  // In alternative we can use the stackIds on each procedure,
  // to identify when a procedure is ready to start.
  // If there are gaps in the sum of the stackIds we need to read more WALs.
  //
  // Example (all procs child of A):
  //   WAL-2: [A, B]                   A stackIds = [0, 4], B stackIds = [1, 5]
  //   WAL-1: [A, B, C, D]
  //
  // In the case above we need to read one more WAL to be able to consider
  // the root procedure A and all children as ready.
  // ==============================================================================================
  private final WalProcedureMap localProcedureMap = new WalProcedureMap(1024);
  private final WalProcedureMap procedureMap = new WalProcedureMap(1024);

  //private long compactionLogId;
  private long maxProcId = 0;

  private final ProcedureStoreTracker tracker;
  private final boolean hasFastStartSupport;

  public ProcedureWALFormatReader(final ProcedureStoreTracker tracker) {
    this.tracker = tracker;
    // we support fast-start only if we have a clean shutdown.
    this.hasFastStartSupport = !tracker.isEmpty();
  }

  public void read(ProcedureWALFile log, ProcedureWALFormat.Loader loader) throws IOException {
    FSDataInputStream stream = log.getStream();
    try {
      boolean hasMore = true;
      while (hasMore) {
        ProcedureWALEntry entry = ProcedureWALFormat.readEntry(stream);
        if (entry == null) {
          LOG.warn("nothing left to decode. exiting with missing EOF");
          hasMore = false;
          break;
        }
        switch (entry.getType()) {
          case INIT:
            readInitEntry(entry);
            break;
          case INSERT:
            readInsertEntry(entry);
            break;
          case UPDATE:
          case COMPACT:
            readUpdateEntry(entry);
            break;
          case DELETE:
            readDeleteEntry(entry);
            break;
          case EOF:
            hasMore = false;
            break;
          default:
            throw new CorruptedWALProcedureStoreException("Invalid entry: " + entry);
        }
      }
    } catch (IOException e) {
      LOG.error("got an exception while reading the procedure WAL: " + log, e);
      loader.markCorruptedWAL(log, e);
    }

    if (!localProcedureMap.isEmpty()) {
      log.setProcIds(localProcedureMap.getMinProcId(), localProcedureMap.getMaxProcId());
      procedureMap.mergeTail(localProcedureMap);
      //if (hasFastStartSupport) {
        // TODO: Some procedure may be already runnables (see readInitEntry())
        //       (we can also check the "update map" in the log trackers)
        // --------------------------------------------------
        //EntryIterator iter = procedureMap.fetchReady();
        //if (iter != null) loader.load(iter);
        // --------------------------------------------------
      //}
    }
  }

  public void finalize(ProcedureWALFormat.Loader loader) throws IOException {
    // notify the loader about the max proc ID
    loader.setMaxProcId(maxProcId);

    // fetch the procedure ready to run.
    ProcedureIterator procIter = procedureMap.fetchReady();
    if (procIter != null) loader.load(procIter);

    // remaining procedures have missing link or dependencies
    // consider them as corrupted, manual fix is probably required.
    procIter = procedureMap.fetchAll();
    if (procIter != null) loader.handleCorrupted(procIter);
  }

  private void loadProcedure(final ProcedureWALEntry entry, final ProcedureProtos.Procedure proc) {
    maxProcId = Math.max(maxProcId, proc.getProcId());
    if (isRequired(proc.getProcId())) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("read " + entry.getType() + " entry " + proc.getProcId());
      }
      localProcedureMap.add(proc);
      tracker.setDeleted(proc.getProcId(), false);
    }
  }

  private void readInitEntry(final ProcedureWALEntry entry)
      throws IOException {
    assert entry.getProcedureCount() == 1 : "Expected only one procedure";
    loadProcedure(entry, entry.getProcedure(0));
  }

  private void readInsertEntry(final ProcedureWALEntry entry) throws IOException {
    assert entry.getProcedureCount() >= 1 : "Expected one or more procedures";
    loadProcedure(entry, entry.getProcedure(0));
    for (int i = 1; i < entry.getProcedureCount(); ++i) {
      loadProcedure(entry, entry.getProcedure(i));
    }
  }

  private void readUpdateEntry(final ProcedureWALEntry entry) throws IOException {
    assert entry.getProcedureCount() == 1 : "Expected only one procedure";
    loadProcedure(entry, entry.getProcedure(0));
  }

  private void readDeleteEntry(final ProcedureWALEntry entry) throws IOException {
    assert entry.getProcedureCount() == 0 : "Expected no procedures";
    assert entry.hasProcId() : "expected ProcID";
    if (LOG.isTraceEnabled()) {
      LOG.trace("read delete entry " + entry.getProcId());
    }
    maxProcId = Math.max(maxProcId, entry.getProcId());
    localProcedureMap.remove(entry.getProcId());
    assert !procedureMap.contains(entry.getProcId());
    tracker.setDeleted(entry.getProcId(), true);
  }

  private boolean isDeleted(final long procId) {
    return tracker.isDeleted(procId) == ProcedureStoreTracker.DeleteState.YES;
  }

  private boolean isRequired(final long procId) {
    return !isDeleted(procId) && !procedureMap.contains(procId);
  }

  // ==========================================================================
  //  We keep an in-memory map of the procedures sorted by replay order.
  //  (see the details in the beginning of the file)
  //                      _______________________________________________
  //      procedureMap = | A |   | E |   | C |   |   |   |   | G |   |   |
  //                       D               B
  //      replayOrderHead = C <-> B <-> E <-> D <-> A <-> G
  //
  //  We also have a lazy grouping by "root procedure", and a list of
  //  unlinked procedure. If after reading all the WALs we have unlinked
  //  procedures it means that we had a missing WAL or a corruption.
  //      rootHead = A <-> D <-> G
  //                 B     E
  //                 C
  //      unlinkFromLinkList = None
  // ==========================================================================
  private static class Entry {
    // hash-table next
    protected Entry hashNext;
    // child head
    protected Entry childHead;
    // double-link for rootHead or childHead
    protected Entry linkNext;
    protected Entry linkPrev;
    // replay double-linked-list
    protected Entry replayNext;
    protected Entry replayPrev;
    // procedure-infos
    protected Procedure procedure;
    protected ProcedureProtos.Procedure proto;
    protected boolean ready = false;

    public Entry(Entry hashNext) { this.hashNext = hashNext; }

    public long getProcId() { return proto.getProcId(); }
    public long getParentId() { return proto.getParentId(); }
    public boolean hasParent() { return proto.hasParentId(); }
    public boolean isReady() { return ready; }

    public boolean isCompleted() {
      if (!hasParent()) {
        switch (proto.getState()) {
          case ROLLEDBACK:
            return true;
          case FINISHED:
            return !proto.hasException();
          default:
            break;
        }
      }
      return false;
    }

    public Procedure convert() throws IOException {
      if (procedure == null) {
        procedure = Procedure.convert(proto);
      }
      return procedure;
    }

    public ProcedureInfo convertToInfo() {
      return ProcedureInfo.convert(proto);
    }

    @Override
    public String toString() {
      return "Entry(" + getProcId() + ", parentId=" + getParentId() + ")";
    }
  }

  private static class EntryIterator implements ProcedureIterator {
    private final Entry replayHead;
    private Entry current;

    public EntryIterator(Entry replayHead) {
      this.replayHead = replayHead;
      this.current = replayHead;
    }

    @Override
    public void reset() {
      this.current = replayHead;
    }

    @Override
    public boolean hasNext() {
      return current != null;
    }

    @Override
    public boolean isNextCompleted() {
      return current != null && current.isCompleted();
    }

    @Override
    public void skipNext() {
      current = current.replayNext;
    }

    @Override
    public Procedure nextAsProcedure() throws IOException {
      try {
        return current.convert();
      } finally {
        current = current.replayNext;
      }
    }

    @Override
    public ProcedureInfo nextAsProcedureInfo() {
      try {
        return current.convertToInfo();
      } finally {
        current = current.replayNext;
      }
    }
  }

  private static class WalProcedureMap {
    // procedure hash table
    private Entry[] procedureMap;

    // replay-order double-linked-list
    private Entry replayOrderHead;
    private Entry replayOrderTail;

    // root linked-list
    private Entry rootHead;

    // pending unlinked children (root not present yet)
    private Entry childUnlinkedHead;

    // Track ProcId range
    private long minProcId = Long.MAX_VALUE;
    private long maxProcId = Long.MIN_VALUE;

    public WalProcedureMap(int size) {
      procedureMap = new Entry[size];
      replayOrderHead = null;
      replayOrderTail = null;
      rootHead = null;
      childUnlinkedHead = null;
    }

    public void add(ProcedureProtos.Procedure procProto) {
      trackProcIds(procProto.getProcId());
      Entry entry = addToMap(procProto.getProcId(), procProto.hasParentId());
      boolean isNew = entry.proto == null;
      entry.proto = procProto;
      addToReplayList(entry);

      if (isNew) {
        if (procProto.hasParentId()) {
          childUnlinkedHead = addToLinkList(entry, childUnlinkedHead);
        } else {
          rootHead = addToLinkList(entry, rootHead);
        }
      }
    }

    public boolean remove(long procId) {
      trackProcIds(procId);
      Entry entry = removeFromMap(procId);
      if (entry != null) {
        unlinkFromReplayList(entry);
        unlinkFromLinkList(entry);
        return true;
      }
      return false;
    }

    private void trackProcIds(long procId) {
      minProcId = Math.min(minProcId, procId);
      maxProcId = Math.max(maxProcId, procId);
    }

    public long getMinProcId() {
      return minProcId;
    }

    public long getMaxProcId() {
      return maxProcId;
    }

    public boolean contains(long procId) {
      return getProcedure(procId) != null;
    }

    public boolean isEmpty() {
      return replayOrderHead == null;
    }

    public void clear() {
      for (int i = 0; i < procedureMap.length; ++i) {
        procedureMap[i] = null;
      }
      replayOrderHead = null;
      replayOrderTail = null;
      rootHead = null;
      childUnlinkedHead = null;
      minProcId = Long.MAX_VALUE;
      maxProcId = Long.MIN_VALUE;
    }

    /*
     * Merges two WalProcedureMap,
     * the target is the "global" map, the source is the "local" map.
     *  - The entries in the hashtables are guaranteed to be unique.
     *    On replay we don't load procedures that already exist in the "global"
     *    map (the one we are merging the "local" in to).
     *  - The replayOrderList of the "local" nao will be appended to the "global"
     *    map replay list.
     *  - The "local" map will be cleared at the end of the operation.
     */
    public void mergeTail(WalProcedureMap other) {
      for (Entry p = other.replayOrderHead; p != null; p = p.replayNext) {
        int slotIndex = getMapSlot(p.getProcId());
        p.hashNext = procedureMap[slotIndex];
        procedureMap[slotIndex] = p;
      }

      if (replayOrderHead == null) {
        replayOrderHead = other.replayOrderHead;
        replayOrderTail = other.replayOrderTail;
        rootHead = other.rootHead;
        childUnlinkedHead = other.childUnlinkedHead;
      } else {
        // append replay list
        assert replayOrderTail.replayNext == null;
        assert other.replayOrderHead.replayPrev == null;
        replayOrderTail.replayNext = other.replayOrderHead;
        other.replayOrderHead.replayPrev = replayOrderTail;
        replayOrderTail = other.replayOrderTail;

        // merge rootHead
        if (rootHead == null) {
          rootHead = other.rootHead;
        } else if (other.rootHead != null) {
          Entry otherTail = findLinkListTail(other.rootHead);
          otherTail.linkNext = rootHead;
          rootHead.linkPrev = otherTail;
          rootHead = other.rootHead;
        }

        // merge childUnlinkedHead
        if (childUnlinkedHead == null) {
          childUnlinkedHead = other.childUnlinkedHead;
        } else if (other.childUnlinkedHead != null) {
          Entry otherTail = findLinkListTail(other.childUnlinkedHead);
          otherTail.linkNext = childUnlinkedHead;
          childUnlinkedHead.linkPrev = otherTail;
          childUnlinkedHead = other.childUnlinkedHead;
        }
      }

      other.clear();
    }

    /*
     * Returns an EntryIterator with the list of procedures ready
     * to be added to the executor.
     * A Procedure is ready if its children and parent are ready.
     */
    public EntryIterator fetchReady() {
      buildGraph();

      Entry readyHead = null;
      Entry readyTail = null;
      Entry p = replayOrderHead;
      while (p != null) {
        Entry next = p.replayNext;
        if (p.isReady()) {
          unlinkFromReplayList(p);
          if (readyTail != null) {
            readyTail.replayNext = p;
            p.replayPrev = readyTail;
          } else {
            p.replayPrev = null;
            readyHead = p;
          }
          readyTail = p;
          p.replayNext = null;
        }
        p = next;
      }
      // we need the hash-table lookups for parents, so this must be done
      // out of the loop where we check isReadyToRun()
      for (p = readyHead; p != null; p = p.replayNext) {
        removeFromMap(p.getProcId());
        unlinkFromLinkList(p);
      }
      return readyHead != null ? new EntryIterator(readyHead) : null;
    }

    /*
     * Drain this map and return all procedures in it.
     */
    public EntryIterator fetchAll() {
      Entry head = replayOrderHead;
      for (Entry p = head; p != null; p = p.replayNext) {
        removeFromMap(p.getProcId());
      }
      for (int i = 0; i < procedureMap.length; ++i) {
        assert procedureMap[i] == null : "map not empty i=" + i;
      }
      replayOrderHead = null;
      replayOrderTail = null;
      childUnlinkedHead = null;
      rootHead = null;
      return head != null ? new EntryIterator(head) : null;
    }

    private void buildGraph() {
      Entry p = childUnlinkedHead;
      while (p != null) {
        Entry next = p.linkNext;
        Entry rootProc = getRootProcedure(p);
        if (rootProc != null) {
          rootProc.childHead = addToLinkList(p, rootProc.childHead);
        }
        p = next;
      }

      for (p = rootHead; p != null; p = p.linkNext) {
        checkReadyToRun(p);
      }
    }

    private Entry getRootProcedure(Entry entry) {
      while (entry != null && entry.hasParent()) {
        entry = getProcedure(entry.getParentId());
      }
      return entry;
    }

    /*
     * (see the comprehensive explaination in the beginning of the file)
     * A Procedure is ready when parent and children are ready.
     * "ready" means that we all the information that we need in-memory.
     *
     * Example-1:
     * We have two WALs, we start reading fronm the newest (wal-2)
     *    wal-2 | C B |
     *    wal-1 | A B C |
     *
     * If C and B don't depend on A (A is not the parent), we can start them
     * before reading wal-1. If B is the only one with parent A we can start C
     * and read one more WAL before being able to start B.
     *
     * How do we know with the only information in B that we are not ready.
     *  - easy case, the parent is missing from the global map
     *  - more complex case we look at the Stack IDs
     *
     * The Stack-IDs are added to the procedure order as incremental index
     * tracking how many times that procedure was executed, which is equivalent
     * at the number of times we wrote the procedure to the WAL.
     * In the example above:
     *   wal-2: B has stackId = [1, 2]
     *   wal-1: B has stackId = [1]
     *   wal-1: A has stackId = [0]
     *
     * Since we know that the Stack-IDs are incremental for a Procedure,
     * we notice that there is a gap in the stackIds of B, so something was
     * executed before.
     * To identify when a Procedure is ready we do the sum of the stackIds of
     * the procedure and the parent. if the stackIdSum is equals to the
     * sum of {1..maxStackId} then everything we need is avaiable.
     *
     * Example-2
     *    wal-2 | A |              A stackIds = [0, 2]
     *    wal-1 | A B |            B stackIds = [1]
     *
     * There is a gap between A stackIds so something was executed in between.
     */
    private boolean checkReadyToRun(Entry rootEntry) {
      int stackIdSum = 0;
      int maxStackId = 0;
      for (int i = 0; i < rootEntry.proto.getStackIdCount(); ++i) {
        int stackId = 1 + rootEntry.proto.getStackId(i);
        maxStackId  = Math.max(maxStackId, stackId);
        stackIdSum += stackId;
      }

      for (Entry p = rootEntry.childHead; p != null; p = p.linkNext) {
        for (int i = 0; i < p.proto.getStackIdCount(); ++i) {
          int stackId = 1 + p.proto.getStackId(i);
          maxStackId  = Math.max(maxStackId, stackId);
          stackIdSum += stackId;
        }
      }
      final int cmpStackIdSum = (maxStackId * (maxStackId + 1) / 2);
      if (cmpStackIdSum == stackIdSum) {
        rootEntry.ready = true;
        for (Entry p = rootEntry.childHead; p != null; p = p.linkNext) {
          p.ready = true;
        }
        return true;
      }
      return false;
    }

    private void unlinkFromReplayList(Entry entry) {
      if (replayOrderHead == entry) {
        replayOrderHead = entry.replayNext;
      }
      if (replayOrderTail == entry) {
        replayOrderTail = entry.replayPrev;
      }
      if (entry.replayPrev != null) {
        entry.replayPrev.replayNext = entry.replayNext;
      }
      if (entry.replayNext != null) {
        entry.replayNext.replayPrev = entry.replayPrev;
      }
    }

    private void addToReplayList(final Entry entry) {
      unlinkFromReplayList(entry);
      entry.replayNext = replayOrderHead;
      entry.replayPrev = null;
      if (replayOrderHead != null) {
        replayOrderHead.replayPrev = entry;
      } else {
        replayOrderTail = entry;
      }
      replayOrderHead = entry;
    }

    private void unlinkFromLinkList(Entry entry) {
      if (entry == rootHead) {
        rootHead = entry.linkNext;
      } else if (entry == childUnlinkedHead) {
        childUnlinkedHead = entry.linkNext;
      }
      if (entry.linkPrev != null) {
        entry.linkPrev.linkNext = entry.linkNext;
      }
      if (entry.linkNext != null) {
        entry.linkNext.linkPrev = entry.linkPrev;
      }
    }

    private Entry addToLinkList(Entry entry, Entry linkHead) {
      unlinkFromLinkList(entry);
      entry.linkNext = linkHead;
      entry.linkPrev = null;
      if (linkHead != null) {
        linkHead.linkPrev = entry;
      }
      return entry;
    }

    private Entry findLinkListTail(Entry linkHead) {
      Entry tail = linkHead;
      while (tail.linkNext != null) {
        tail = tail.linkNext;
      }
      return tail;
    }

    private Entry addToMap(final long procId, final boolean hasParent) {
      int slotIndex = getMapSlot(procId);
      Entry entry = getProcedure(slotIndex, procId);
      if (entry != null) return entry;

      entry = new Entry(procedureMap[slotIndex]);
      procedureMap[slotIndex] = entry;
      return entry;
    }

    private Entry removeFromMap(final long procId) {
      int slotIndex = getMapSlot(procId);
      Entry prev = null;
      Entry entry = procedureMap[slotIndex];
      while (entry != null) {
        if (procId == entry.getProcId()) {
          if (prev != null) {
            prev.hashNext = entry.hashNext;
          } else {
            procedureMap[slotIndex] = entry.hashNext;
          }
          entry.hashNext = null;
          return entry;
        }
        prev = entry;
        entry = entry.hashNext;
      }
      return null;
    }

    private Entry getProcedure(final long procId) {
      return getProcedure(getMapSlot(procId), procId);
    }

    private Entry getProcedure(final int slotIndex, final long procId) {
      Entry entry = procedureMap[slotIndex];
      while (entry != null) {
        if (procId == entry.getProcId()) {
          return entry;
        }
        entry = entry.hashNext;
      }
      return null;
    }

    private int getMapSlot(final long procId) {
      return (int)(Procedure.getProcIdHashCode(procId) % procedureMap.length);
    }
  }
}