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

package org.apache.hadoop.hbase.procedure2.store;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * Keeps track of live procedures.
 *
 * It can be used by the ProcedureStore to identify which procedures are already
 * deleted/completed to avoid the deserialization step on restart
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProcedureStoreTracker {
  // Key is procedure id corresponding to first bit of the bitmap.
  private final TreeMap<Long, BitSetNode> map = new TreeMap<>();

  /**
   * If true, do not remove bits corresponding to deleted procedures. Note that this can result
   * in huge bitmaps overtime.
   * Currently, it's set to true only when building tracker state from logs during recovery. During
   * recovery, if we are sure that a procedure has been deleted, reading its old update entries
   * can be skipped.
   */
  private boolean keepDeletes = false;
  /**
   * If true, it means tracker has incomplete information about the active/deleted procedures.
   * It's set to true only when recovering from old logs. See {@link #isDeleted(long)} docs to
   * understand it's real use.
   */
  private boolean partial = false;

  private long minUpdatedProcId = Long.MAX_VALUE;
  private long maxUpdatedProcId = Long.MIN_VALUE;

  public enum DeleteState { YES, NO, MAYBE }

  /**
   * A bitmap which can grow/merge with other {@link BitSetNode} (if certain conditions are met).
   * Boundaries of bitmap are aligned to multiples of {@link BitSetNode#BITS_PER_WORD}. So the
   * range of a {@link BitSetNode} is from [x * K, y * K) where x and y are integers, y > x and K
   * is BITS_PER_WORD.
   */
  public static class BitSetNode {
    private final static long WORD_MASK = 0xffffffffffffffffL;
    private final static int ADDRESS_BITS_PER_WORD = 6;
    private final static int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;
    private final static int MAX_NODE_SIZE = 1 << ADDRESS_BITS_PER_WORD;

    /**
     * Mimics {@link ProcedureStoreTracker#partial}.
     */
    private final boolean partial;

    /* ----------------------
     * |  updated | deleted |  meaning
     * |     0    |   0     |  proc exists, but hasn't been updated since last resetUpdates().
     * |     1    |   0     |  proc was updated (but not deleted).
     * |     1    |   1     |  proc was deleted.
     * |     0    |   1     |  proc doesn't exist (maybe never created, maybe deleted in past).
    /* ----------------------
     */

    /**
     * Set of procedures which have been updated since last {@link #resetUpdates()}.
     * Useful to track procedures which have been updated since last WAL write.
     */
    private long[] updated;
    /**
     * Keeps track of procedure ids which belong to this bitmap's range and have been deleted.
     * This represents global state since it's not reset on WAL rolls.
     */
    private long[] deleted;
    /**
     * Offset of bitmap i.e. procedure id corresponding to first bit.
     */
    private long start;

    public void dump() {
      System.out.printf("%06d:%06d min=%d max=%d%n", getStart(), getEnd(),
        getActiveMinProcId(), getActiveMaxProcId());
      System.out.println("Update:");
      for (int i = 0; i < updated.length; ++i) {
        for (int j = 0; j < BITS_PER_WORD; ++j) {
          System.out.print((updated[i] & (1L << j)) != 0 ? "1" : "0");
        }
        System.out.println(" " + i);
      }
      System.out.println();
      System.out.println("Delete:");
      for (int i = 0; i < deleted.length; ++i) {
        for (int j = 0; j < BITS_PER_WORD; ++j) {
          System.out.print((deleted[i] & (1L << j)) != 0 ? "1" : "0");
        }
        System.out.println(" " + i);
      }
      System.out.println();
    }

    public BitSetNode(final long procId, final boolean partial) {
      start = alignDown(procId);

      int count = 1;
      updated = new long[count];
      deleted = new long[count];
      for (int i = 0; i < count; ++i) {
        updated[i] = 0;
        deleted[i] = partial ? 0 : WORD_MASK;
      }

      this.partial = partial;
      updateState(procId, false);
    }

    protected BitSetNode(final long start, final long[] updated, final long[] deleted) {
      this.start = start;
      this.updated = updated;
      this.deleted = deleted;
      this.partial = false;
    }

    public BitSetNode(ProcedureProtos.ProcedureStoreTracker.TrackerNode data) {
      start = data.getStartId();
      int size = data.getUpdatedCount();
      updated = new long[size];
      deleted = new long[size];
      for (int i = 0; i < size; ++i) {
        updated[i] = data.getUpdated(i);
        deleted[i] = data.getDeleted(i);
      }
      partial = false;
    }

    public BitSetNode(BitSetNode other) {
      this.start = other.start;
      this.partial = other.partial;
      this.updated = other.updated.clone();
      this.deleted = other.deleted.clone();
    }

    public void update(final long procId) {
      updateState(procId, false);
    }

    public void delete(final long procId) {
      updateState(procId, true);
    }

    public Long getStart() {
      return start;
    }

    public Long getEnd() {
      return start + (updated.length << ADDRESS_BITS_PER_WORD) - 1;
    }

    public boolean contains(final long procId) {
      return start <= procId && procId <= getEnd();
    }

    public DeleteState isDeleted(final long procId) {
      int bitmapIndex = getBitmapIndex(procId);
      int wordIndex = bitmapIndex >> ADDRESS_BITS_PER_WORD;
      if (wordIndex >= deleted.length) {
        return DeleteState.MAYBE;
      }
      return (deleted[wordIndex] & (1L << bitmapIndex)) != 0 ? DeleteState.YES : DeleteState.NO;
    }

    private boolean isUpdated(final long procId) {
      int bitmapIndex = getBitmapIndex(procId);
      int wordIndex = bitmapIndex >> ADDRESS_BITS_PER_WORD;
      if (wordIndex >= updated.length) {
        return false;
      }
      return (updated[wordIndex] & (1L << bitmapIndex)) != 0;
    }

    public boolean isUpdated() {
      // TODO: cache the value
      for (int i = 0; i < updated.length; ++i) {
        if ((updated[i] | deleted[i]) != WORD_MASK) {
          return false;
        }
      }
      return true;
    }

    /**
     * @return true, if there are no active procedures in this BitSetNode, else false.
     */
    public boolean isEmpty() {
      // TODO: cache the value
      for (int i = 0; i < deleted.length; ++i) {
        if (deleted[i] != WORD_MASK) {
          return false;
        }
      }
      return true;
    }

    public void resetUpdates() {
      for (int i = 0; i < updated.length; ++i) {
        updated[i] = 0;
      }
    }

    /**
     * Clears the {@link #deleted} bitmaps.
     */
    public void undeleteAll() {
      for (int i = 0; i < updated.length; ++i) {
        deleted[i] = 0;
      }
    }

    public void unsetPartialFlag() {
      for (int i = 0; i < updated.length; ++i) {
        for (int j = 0; j < BITS_PER_WORD; ++j) {
          if ((updated[i] & (1L << j)) == 0) {
            deleted[i] |= (1L << j);
          }
        }
      }
    }

    /**
     * If an active (non-deleted) procedure in current BitSetNode has been updated in {@code other}
     * BitSetNode, then delete it from current node.
     * @return true if node changed, i.e. some procedure(s) from {@code other} was subtracted from
     * current node.
     */
    public boolean subtract(BitSetNode other) {
      // Assert that other node intersects with this node.
      assert !(other.getEnd() < this.start) && !(this.getEnd() < other.start);
      int thisOffset = 0, otherOffset = 0;
      if (this.start < other.start) {
        thisOffset = (int) (other.start - this.start) / BITS_PER_WORD;
      } else {
        otherOffset = (int) (this.start - other.start) / BITS_PER_WORD;
      }
      int size = Math.min(this.updated.length - thisOffset, other.updated.length - otherOffset);
      boolean nonZeroIntersect = false;
      for (int i = 0; i < size; i++) {
        long intersect = ~this.deleted[thisOffset + i] & other.updated[otherOffset + i];
        if (intersect != 0) {
          this.deleted[thisOffset + i] |= intersect;
          nonZeroIntersect = true;
        }
      }
      return nonZeroIntersect;
    }

    /**
     * Convert to
     * org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureStoreTracker.TrackerNode
     * protobuf.
     */
    public ProcedureProtos.ProcedureStoreTracker.TrackerNode convert() {
      ProcedureProtos.ProcedureStoreTracker.TrackerNode.Builder builder =
        ProcedureProtos.ProcedureStoreTracker.TrackerNode.newBuilder();
      builder.setStartId(start);
      for (int i = 0; i < updated.length; ++i) {
        builder.addUpdated(updated[i]);
        builder.addDeleted(deleted[i]);
      }
      return builder.build();
    }


    // ========================================================================
    //  Grow/Merge Helpers
    // ========================================================================
    public boolean canGrow(final long procId) {
      return Math.abs(procId - start) < MAX_NODE_SIZE;
    }

    public boolean canMerge(final BitSetNode rightNode) {
      // Can just compare 'starts' since boundaries are aligned to multiples of BITS_PER_WORD.
      assert start < rightNode.start;
      return (rightNode.getEnd() - start) < MAX_NODE_SIZE;
    }

    public void grow(final long procId) {
      int delta, offset;

      if (procId < start) {
        // add to head
        long newStart = alignDown(procId);
        delta = (int)(start - newStart) >> ADDRESS_BITS_PER_WORD;
        offset = delta;
        start = newStart;
      } else {
        // Add to tail
        long newEnd = alignUp(procId + 1);
        delta = (int)(newEnd - getEnd()) >> ADDRESS_BITS_PER_WORD;
        offset = 0;
      }

      long[] newBitmap;
      int oldSize = updated.length;

      newBitmap = new long[oldSize + delta];
      for (int i = 0; i < newBitmap.length; ++i) {
        newBitmap[i] = 0;
      }
      System.arraycopy(updated, 0, newBitmap, offset, oldSize);
      updated = newBitmap;

      newBitmap = new long[deleted.length + delta];
      for (int i = 0; i < newBitmap.length; ++i) {
        newBitmap[i] = partial ? 0 : WORD_MASK;
      }
      System.arraycopy(deleted, 0, newBitmap, offset, oldSize);
      deleted = newBitmap;
    }

    public void merge(final BitSetNode rightNode) {
      int delta = (int)(rightNode.getEnd() - getEnd()) >> ADDRESS_BITS_PER_WORD;

      long[] newBitmap;
      int oldSize = updated.length;
      int newSize = (delta - rightNode.updated.length);
      int offset = oldSize + newSize;

      newBitmap = new long[oldSize + delta];
      System.arraycopy(updated, 0, newBitmap, 0, oldSize);
      System.arraycopy(rightNode.updated, 0, newBitmap, offset, rightNode.updated.length);
      updated = newBitmap;

      newBitmap = new long[oldSize + delta];
      System.arraycopy(deleted, 0, newBitmap, 0, oldSize);
      System.arraycopy(rightNode.deleted, 0, newBitmap, offset, rightNode.deleted.length);
      deleted = newBitmap;

      for (int i = 0; i < newSize; ++i) {
        updated[offset + i] = 0;
        deleted[offset + i] = partial ? 0 : WORD_MASK;
      }
    }

    @Override
    public String toString() {
      return "BitSetNode(" + getStart() + "-" + getEnd() + ")";
    }

    // ========================================================================
    //  Min/Max Helpers
    // ========================================================================
    public long getActiveMinProcId() {
      long minProcId = start;
      for (int i = 0; i < deleted.length; ++i) {
        if (deleted[i] == 0) {
          return(minProcId);
        }

        if (deleted[i] != WORD_MASK) {
          for (int j = 0; j < BITS_PER_WORD; ++j) {
            if ((deleted[i] & (1L << j)) != 0) {
              return minProcId + j;
            }
          }
        }

        minProcId += BITS_PER_WORD;
      }
      return minProcId;
    }

    public long getActiveMaxProcId() {
      long maxProcId = getEnd();
      for (int i = deleted.length - 1; i >= 0; --i) {
        if (deleted[i] == 0) {
          return maxProcId;
        }

        if (deleted[i] != WORD_MASK) {
          for (int j = BITS_PER_WORD - 1; j >= 0; --j) {
            if ((deleted[i] & (1L << j)) == 0) {
              return maxProcId - (BITS_PER_WORD - 1 - j);
            }
          }
        }
        maxProcId -= BITS_PER_WORD;
      }
      return maxProcId;
    }

    // ========================================================================
    //  Bitmap Helpers
    // ========================================================================
    private int getBitmapIndex(final long procId) {
      return (int)(procId - start);
    }

    private void updateState(final long procId, final boolean isDeleted) {
      int bitmapIndex = getBitmapIndex(procId);
      int wordIndex = bitmapIndex >> ADDRESS_BITS_PER_WORD;
      long value = (1L << bitmapIndex);

      updated[wordIndex] |= value;
      if (isDeleted) {
        deleted[wordIndex] |= value;
      } else {
        deleted[wordIndex] &= ~value;
      }
    }


    // ========================================================================
    //  Helpers
    // ========================================================================
    /**
     * @return upper boundary (aligned to multiple of BITS_PER_WORD) of bitmap range x belongs to.
     */
    private static long alignUp(final long x) {
      return (x + (BITS_PER_WORD - 1)) & -BITS_PER_WORD;
    }

    /**
     * @return lower boundary (aligned to multiple of BITS_PER_WORD) of bitmap range x belongs to.
     */
    private static long alignDown(final long x) {
      return x & -BITS_PER_WORD;
    }
  }

  public void resetToProto(final ProcedureProtos.ProcedureStoreTracker trackerProtoBuf) {
    reset();
    for (ProcedureProtos.ProcedureStoreTracker.TrackerNode protoNode: trackerProtoBuf.getNodeList()) {
      final BitSetNode node = new BitSetNode(protoNode);
      map.put(node.getStart(), node);
    }
  }

  /**
   * Resets internal state to same as given {@code tracker}. Does deep copy of the bitmap.
   */
  public void resetTo(ProcedureStoreTracker tracker) {
    this.partial = tracker.partial;
    this.minUpdatedProcId = tracker.minUpdatedProcId;
    this.maxUpdatedProcId = tracker.maxUpdatedProcId;
    this.keepDeletes = tracker.keepDeletes;
    for (Map.Entry<Long, BitSetNode> entry : tracker.map.entrySet()) {
      map.put(entry.getKey(), new BitSetNode(entry.getValue()));
    }
  }

  public void insert(long procId) {
    BitSetNode node = getOrCreateNode(procId);
    node.update(procId);
    trackProcIds(procId);
  }

  public void insert(final long[] procIds) {
    for (int i = 0; i < procIds.length; ++i) {
      insert(procIds[i]);
    }
  }

  public void insert(final long procId, final long[] subProcIds) {
    update(procId);
    for (int i = 0; i < subProcIds.length; ++i) {
      insert(subProcIds[i]);
    }
  }

  public void update(long procId) {
    Map.Entry<Long, BitSetNode> entry = map.floorEntry(procId);
    assert entry != null : "expected node to update procId=" + procId;

    BitSetNode node = entry.getValue();
    assert node.contains(procId);
    node.update(procId);
    trackProcIds(procId);
  }

  public void delete(long procId) {
    Map.Entry<Long, BitSetNode> entry = map.floorEntry(procId);
    assert entry != null : "expected node to delete procId=" + procId;

    BitSetNode node = entry.getValue();
    assert node.contains(procId) : "expected procId in the node";
    node.delete(procId);

    if (!keepDeletes && node.isEmpty()) {
      // TODO: RESET if (map.size() == 1)
      map.remove(entry.getKey());
    }

    trackProcIds(procId);
  }

  public void delete(long[] procIds) {
    // TODO: optimize
    Arrays.sort(procIds);
    for (int i = 0; i < procIds.length; ++i) {
      delete(procIds[i]);
    }
  }

  private void trackProcIds(long procId) {
    minUpdatedProcId = Math.min(minUpdatedProcId, procId);
    maxUpdatedProcId = Math.max(maxUpdatedProcId, procId);
  }

  public long getUpdatedMinProcId() {
    return minUpdatedProcId;
  }

  public long getUpdatedMaxProcId() {
    return maxUpdatedProcId;
  }

  @InterfaceAudience.Private
  public void setDeleted(final long procId, final boolean isDeleted) {
    BitSetNode node = getOrCreateNode(procId);
    assert node.contains(procId) : "expected procId=" + procId + " in the node=" + node;
    node.updateState(procId, isDeleted);
    trackProcIds(procId);
  }

  public void reset() {
    this.keepDeletes = false;
    this.partial = false;
    this.map.clear();
    resetUpdates();
  }

  public boolean isUpdated(long procId) {
    final Map.Entry<Long, BitSetNode> entry = map.floorEntry(procId);
    return entry != null && entry.getValue().contains(procId) && entry.getValue().isUpdated(procId);
  }

  /**
   * If {@link #partial} is false, returns state from the bitmap. If no state is found for
   * {@code procId}, returns YES.
   * If partial is true, tracker doesn't have complete view of system state, so it returns MAYBE
   * if there is no update for the procedure or if it doesn't have a state in bitmap. Otherwise,
   * returns state from the bitmap.
   */
  public DeleteState isDeleted(long procId) {
    Map.Entry<Long, BitSetNode> entry = map.floorEntry(procId);
    if (entry != null && entry.getValue().contains(procId)) {
      BitSetNode node = entry.getValue();
      DeleteState state = node.isDeleted(procId);
      return partial && !node.isUpdated(procId) ? DeleteState.MAYBE : state;
    }
    return partial ? DeleteState.MAYBE : DeleteState.YES;
  }

  public long getActiveMinProcId() {
    // TODO: Cache?
    Map.Entry<Long, BitSetNode> entry = map.firstEntry();
    return entry == null ? 0 : entry.getValue().getActiveMinProcId();
  }

  public void setKeepDeletes(boolean keepDeletes) {
    this.keepDeletes = keepDeletes;
    // If not to keep deletes, remove the BitSetNodes which are empty (i.e. contains ids of deleted
    // procedures).
    if (!keepDeletes) {
      Iterator<Map.Entry<Long, BitSetNode>> it = map.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<Long, BitSetNode> entry = it.next();
        if (entry.getValue().isEmpty()) {
          it.remove();
        }
      }
    }
  }

  public boolean isPartial() {
    return partial;
  }

  public void setPartialFlag(boolean isPartial) {
    if (this.partial && !isPartial) {
      for (Map.Entry<Long, BitSetNode> entry : map.entrySet()) {
        entry.getValue().unsetPartialFlag();
      }
    }
    this.partial = isPartial;
  }

  /**
   * @return true, if no procedure is active, else false.
   */
  public boolean isEmpty() {
    for (Map.Entry<Long, BitSetNode> entry : map.entrySet()) {
      if (!entry.getValue().isEmpty()) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return true if any procedure was updated since last call to {@link #resetUpdates()}.
   */
  public boolean isUpdated() {
    for (Map.Entry<Long, BitSetNode> entry : map.entrySet()) {
      if (!entry.getValue().isUpdated()) {
        return false;
      }
    }
    return true;
  }

  public boolean isTracking(long minId, long maxId) {
    // TODO: we can make it more precise, instead of looking just at the block
    return map.floorEntry(minId) != null || map.floorEntry(maxId) != null;
  }

  /**
   * Clears the list of updated procedure ids. This doesn't affect global list of active
   * procedure ids.
   */
  public void resetUpdates() {
    for (Map.Entry<Long, BitSetNode> entry : map.entrySet()) {
      entry.getValue().resetUpdates();
    }
    minUpdatedProcId = Long.MAX_VALUE;
    maxUpdatedProcId = Long.MIN_VALUE;
  }

  public void undeleteAll() {
    for (Map.Entry<Long, BitSetNode> entry : map.entrySet()) {
      entry.getValue().undeleteAll();
    }
  }

  private BitSetNode getOrCreateNode(final long procId) {
    // If procId can fit in left node (directly or by growing it)
    BitSetNode leftNode = null;
    boolean leftCanGrow = false;
    Map.Entry<Long, BitSetNode> leftEntry = map.floorEntry(procId);
    if (leftEntry != null) {
      leftNode = leftEntry.getValue();
      if (leftNode.contains(procId)) {
        return leftNode;
      }
      leftCanGrow = leftNode.canGrow(procId);
    }

    // If procId can fit in right node (directly or by growing it)
    BitSetNode rightNode = null;
    boolean rightCanGrow = false;
    Map.Entry<Long, BitSetNode> rightEntry = map.ceilingEntry(procId);
    if (rightEntry != null) {
      rightNode = rightEntry.getValue();
      rightCanGrow = rightNode.canGrow(procId);
      if (leftNode != null) {
        if (leftNode.canMerge(rightNode)) {
          // merge left and right node
          return mergeNodes(leftNode, rightNode);
        }

        // If left and right nodes can not merge, decide which one to grow.
        if (leftCanGrow && rightCanGrow) {
          if ((procId - leftNode.getEnd()) <= (rightNode.getStart() - procId)) {
            return growNode(leftNode, procId);
          }
          return growNode(rightNode, procId);
        }
      }
    }

    // grow the left node
    if (leftCanGrow) {
      return growNode(leftNode, procId);
    }

    // grow the right node
    if (rightCanGrow) {
      return growNode(rightNode, procId);
    }

    // add new node if there are no left/right nodes which can be used.
    BitSetNode node = new BitSetNode(procId, partial);
    map.put(node.getStart(), node);
    return node;
  }

  /**
   * Grows {@code node} to contain {@code procId} and updates the map.
   * @return {@link BitSetNode} instance which contains {@code procId}.
   */
  private BitSetNode growNode(BitSetNode node, long procId) {
    map.remove(node.getStart());
    node.grow(procId);
    map.put(node.getStart(), node);
    return node;
  }

  /**
   * Merges {@code leftNode} & {@code rightNode} and updates the map.
   */
  private BitSetNode mergeNodes(BitSetNode leftNode, BitSetNode rightNode) {
    assert leftNode.getStart() < rightNode.getStart();
    leftNode.merge(rightNode);
    map.remove(rightNode.getStart());
    return leftNode;
  }

  public void dump() {
    System.out.println("map " + map.size());
    System.out.println("isUpdated " + isUpdated());
    System.out.println("isEmpty " + isEmpty());
    for (Map.Entry<Long, BitSetNode> entry : map.entrySet()) {
      entry.getValue().dump();
    }
  }

  /**
   * Iterates over
   * {@link BitSetNode}s in this.map and subtracts with corresponding ones from {@code other}
   * tracker.
   * @return true if tracker changed, i.e. some procedure from {@code other} were subtracted from
   * current tracker.
   */
  public boolean subtract(ProcedureStoreTracker other) {
    // Can not intersect partial bitmap.
    assert !partial && !other.partial;
    boolean nonZeroIntersect = false;
    for (Map.Entry<Long, BitSetNode> currentEntry : map.entrySet()) {
      BitSetNode currentBitSetNode = currentEntry.getValue();
      Map.Entry<Long, BitSetNode> otherTrackerEntry = other.map.floorEntry(currentEntry.getKey());
      if (otherTrackerEntry == null  // No node in other map with key <= currentEntry.getKey().
          // First entry in other map doesn't intersect with currentEntry.
          || otherTrackerEntry.getValue().getEnd() < currentEntry.getKey()) {
        otherTrackerEntry = other.map.ceilingEntry(currentEntry.getKey());
        if (otherTrackerEntry == null || !currentBitSetNode.contains(otherTrackerEntry.getKey())) {
          // No node in other map intersects with currentBitSetNode's range.
          continue;
        }
      }
      do {
        nonZeroIntersect |= currentEntry.getValue().subtract(otherTrackerEntry.getValue());
        otherTrackerEntry = other.map.higherEntry(otherTrackerEntry.getKey());
      } while (otherTrackerEntry != null && currentBitSetNode.contains(otherTrackerEntry.getKey()));
    }
    return nonZeroIntersect;
  }

  // ========================================================================
  //  Convert to/from Protocol Buffer.
  // ========================================================================

  /**
   * Builds
   * org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureStoreTracker
   * protocol buffer from current state.
   */
  public ProcedureProtos.ProcedureStoreTracker toProto() throws IOException {
    ProcedureProtos.ProcedureStoreTracker.Builder builder =
        ProcedureProtos.ProcedureStoreTracker.newBuilder();
    for (Map.Entry<Long, BitSetNode> entry : map.entrySet()) {
      builder.addNode(entry.getValue().convert());
    }
    return builder.build();
  }
}
