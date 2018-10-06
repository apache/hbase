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
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore.ProcedureIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * We keep an in-memory map of the procedures sorted by replay order. (see the details in the
 * beginning of {@link ProcedureWALFormatReader}).
 *
 * <pre>
 *      procedureMap = | A |   | E |   | C |   |   |   |   | G |   |   |
 *                       D               B
 *      replayOrderHead = C <-> B <-> E <-> D <-> A <-> G
 *
 *  We also have a lazy grouping by "root procedure", and a list of
 *  unlinked procedures. If after reading all the WALs we have unlinked
 *  procedures it means that we had a missing WAL or a corruption.
 *      rootHead = A <-> D <-> G
 *                 B     E
 *                 C
 *      unlinkFromLinkList = None
 * </pre>
 */
class WALProcedureMap {

  private static final Logger LOG = LoggerFactory.getLogger(WALProcedureMap.class);

  private static class Entry {
    // For bucketed linked lists in hash-table.
    private Entry hashNext;
    // child head
    private Entry childHead;
    // double-link for rootHead or childHead
    private Entry linkNext;
    private Entry linkPrev;
    // replay double-linked-list
    private Entry replayNext;
    private Entry replayPrev;
    // procedure-infos
    private Procedure<?> procedure;
    private ProcedureProtos.Procedure proto;
    private boolean ready = false;

    public Entry(Entry hashNext) {
      this.hashNext = hashNext;
    }

    public long getProcId() {
      return proto.getProcId();
    }

    public long getParentId() {
      return proto.getParentId();
    }

    public boolean hasParent() {
      return proto.hasParentId();
    }

    public boolean isReady() {
      return ready;
    }

    public boolean isFinished() {
      if (!hasParent()) {
        // we only consider 'root' procedures. because for the user 'finished'
        // means when everything up to the 'root' is finished.
        switch (proto.getState()) {
          case ROLLEDBACK:
          case SUCCESS:
            return true;
          default:
            break;
        }
      }
      return false;
    }

    public Procedure<?> convert() throws IOException {
      if (procedure == null) {
        procedure = ProcedureUtil.convertToProcedure(proto);
      }
      return procedure;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("Entry(");
      sb.append(getProcId());
      sb.append(", parentId=");
      sb.append(getParentId());
      sb.append(", class=");
      sb.append(proto.getClassName());
      sb.append(")");
      return sb.toString();
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
    public boolean isNextFinished() {
      return current != null && current.isFinished();
    }

    @Override
    public void skipNext() {
      current = current.replayNext;
    }

    @Override
    public Procedure<?> next() throws IOException {
      try {
        return current.convert();
      } finally {
        current = current.replayNext;
      }
    }
  }

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
  private long minModifiedProcId = Long.MAX_VALUE;
  private long maxModifiedProcId = Long.MIN_VALUE;

  public WALProcedureMap(int size) {
    procedureMap = new Entry[size];
    replayOrderHead = null;
    replayOrderTail = null;
    rootHead = null;
    childUnlinkedHead = null;
  }

  public void add(ProcedureProtos.Procedure procProto) {
    trackProcIds(procProto.getProcId());
    Entry entry = addToMap(procProto.getProcId(), procProto.hasParentId());
    boolean newEntry = entry.proto == null;
    // We have seen procedure WALs where the entries are out of order; see HBASE-18152.
    // To compensate, only replace the Entry procedure if for sure this new procedure
    // is indeed an entry that came later.
    // TODO: Fix the writing of procedure info so it does not violate basic expectation, that WALs
    // contain procedure changes goingfrom start to finish in sequence.
    if (newEntry || isIncreasing(entry.proto, procProto)) {
      entry.proto = procProto;
    }
    addToReplayList(entry);
    if (newEntry) {
      if (procProto.hasParentId()) {
        childUnlinkedHead = addToLinkList(entry, childUnlinkedHead);
      } else {
        rootHead = addToLinkList(entry, rootHead);
      }
    }
  }

  /**
   * @return True if this new procedure is 'richer' than the current one else false and we log this
   *         incidence where it appears that the WAL has older entries appended after newer ones.
   *         See HBASE-18152.
   */
  private static boolean isIncreasing(ProcedureProtos.Procedure current,
      ProcedureProtos.Procedure candidate) {
    // Check that the procedures we see are 'increasing'. We used to compare
    // procedure id first and then update time but it can legitimately go backwards if the
    // procedure is failed or rolled back so that was unreliable. Was going to compare
    // state but lets see if comparing update time enough (unfortunately this issue only
    // seen under load...)
    boolean increasing = current.getLastUpdate() <= candidate.getLastUpdate();
    if (!increasing) {
      LOG.warn("NOT INCREASING! current=" + current + ", candidate=" + candidate);
    }
    return increasing;
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
    minModifiedProcId = Math.min(minModifiedProcId, procId);
    maxModifiedProcId = Math.max(maxModifiedProcId, procId);
  }

  public long getMinModifiedProcId() {
    return minModifiedProcId;
  }

  public long getMaxModifiedProcId() {
    return maxModifiedProcId;
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
    minModifiedProcId = Long.MAX_VALUE;
    maxModifiedProcId = Long.MIN_VALUE;
  }

  /*
   * Merges two WalProcedureMap, the target is the "global" map, the source is the "local" map. -
   * The entries in the hashtables are guaranteed to be unique. On replay we don't load procedures
   * that already exist in the "global" map (the one we are merging the "local" in to). - The
   * replayOrderList of the "local" nao will be appended to the "global" map replay list. - The
   * "local" map will be cleared at the end of the operation.
   */
  public void mergeTail(WALProcedureMap other) {
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
    maxModifiedProcId = Math.max(maxModifiedProcId, other.maxModifiedProcId);
    minModifiedProcId = Math.max(minModifiedProcId, other.minModifiedProcId);

    other.clear();
  }

  /**
   * Returns an EntryIterator with the list of procedures ready to be added to the executor. A
   * Procedure is ready if its children and parent are ready.
   */
  public ProcedureIterator fetchReady() {
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

  /**
   * Drain this map and return all procedures in it.
   */
  public ProcedureIterator fetchAll() {
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

  /**
   * (see the comprehensive explanation in the beginning of {@link ProcedureWALFormatReader}). A
   * Procedure is ready when parent and children are ready. "ready" means that we all the
   * information that we need in-memory.
   * <p/>
   * Example-1:<br/>
   * We have two WALs, we start reading from the newest (wal-2)
   *
   * <pre>
   *    wal-2 | C B |
   *    wal-1 | A B C |
   * </pre>
   *
   * If C and B don't depend on A (A is not the parent), we can start them before reading wal-1. If
   * B is the only one with parent A we can start C. We have to read one more WAL before being able
   * to start B.
   * <p/>
   * How do we know with the only information in B that we are not ready.
   * <ul>
   * <li>easy case, the parent is missing from the global map</li>
   * <li>more complex case we look at the Stack IDs.</li>
   * </ul>
   * The Stack-IDs are added to the procedure order as an incremental index tracking how many times
   * that procedure was executed, which is equivalent to the number of times we wrote the procedure
   * to the WAL. <br/>
   * In the example above:
   *
   * <pre>
   *   wal-2: B has stackId = [1, 2]
   *   wal-1: B has stackId = [1]
   *   wal-1: A has stackId = [0]
   * </pre>
   *
   * Since we know that the Stack-IDs are incremental for a Procedure, we notice that there is a gap
   * in the stackIds of B, so something was executed before.
   * <p/>
   * To identify when a Procedure is ready we do the sum of the stackIds of the procedure and the
   * parent. if the stackIdSum is equal to the sum of {1..maxStackId} then everything we need is
   * available.
   * <p/>
   * Example-2
   *
   * <pre>
   *    wal-2 | A |              A stackIds = [0, 2]
   *    wal-1 | A B |            B stackIds = [1]
   * </pre>
   *
   * There is a gap between A stackIds so something was executed in between.
   */
  private boolean checkReadyToRun(Entry rootEntry) {
    assert !rootEntry.hasParent() : "expected root procedure, got " + rootEntry;

    if (rootEntry.isFinished()) {
      // If the root procedure is finished, sub-procedures should be gone
      if (rootEntry.childHead != null) {
        LOG.error("unexpected active children for root-procedure: {}", rootEntry);
        for (Entry p = rootEntry.childHead; p != null; p = p.linkNext) {
          LOG.error("unexpected active children: {}", p);
        }
      }

      assert rootEntry.childHead == null : "unexpected children on root completion. " + rootEntry;
      rootEntry.ready = true;
      return true;
    }

    int stackIdSum = 0;
    int maxStackId = 0;
    for (int i = 0; i < rootEntry.proto.getStackIdCount(); ++i) {
      int stackId = 1 + rootEntry.proto.getStackId(i);
      maxStackId = Math.max(maxStackId, stackId);
      stackIdSum += stackId;
      LOG.trace("stackId={} stackIdSum={} maxStackid={} {}", stackId, stackIdSum, maxStackId,
        rootEntry);
    }

    for (Entry p = rootEntry.childHead; p != null; p = p.linkNext) {
      for (int i = 0; i < p.proto.getStackIdCount(); ++i) {
        int stackId = 1 + p.proto.getStackId(i);
        maxStackId = Math.max(maxStackId, stackId);
        stackIdSum += stackId;
        LOG.trace("stackId={} stackIdSum={} maxStackid={} {}", stackId, stackIdSum, maxStackId, p);
      }
    }
    // The cmpStackIdSum is this formula for finding the sum of a series of numbers:
    // http://www.wikihow.com/Sum-the-Integers-from-1-to-N#/Image:Sum-the-Integers-from-1-to-N-Step-2-Version-3.jpg
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

  private Entry addToMap(long procId, boolean hasParent) {
    int slotIndex = getMapSlot(procId);
    Entry entry = getProcedure(slotIndex, procId);
    if (entry != null) {
      return entry;
    }

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

  private Entry getProcedure(long procId) {
    return getProcedure(getMapSlot(procId), procId);
  }

  private Entry getProcedure(int slotIndex, long procId) {
    Entry entry = procedureMap[slotIndex];
    while (entry != null) {
      if (procId == entry.getProcId()) {
        return entry;
      }
      entry = entry.hashNext;
    }
    return null;
  }

  private int getMapSlot(long procId) {
    return (int) (Procedure.getProcIdHashCode(procId) % procedureMap.length);
  }
}