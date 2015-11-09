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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos;

/**
 * Keeps track of live procedures.
 *
 * It can be used by the ProcedureStore to identify which procedures are already
 * deleted/completed to avoid the deserialization step on restart.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProcedureStoreTracker {
  private final TreeMap<Long, BitSetNode> map = new TreeMap<Long, BitSetNode>();

  private boolean keepDeletes = false;
  private boolean partial = false;

  private long minUpdatedProcId = Long.MAX_VALUE;
  private long maxUpdatedProcId = Long.MIN_VALUE;

  public enum DeleteState { YES, NO, MAYBE }

  public static class BitSetNode {
    private final static long WORD_MASK = 0xffffffffffffffffL;
    private final static int ADDRESS_BITS_PER_WORD = 6;
    private final static int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;
    private final static int MAX_NODE_SIZE = 1 << ADDRESS_BITS_PER_WORD;

    private final boolean partial;
    private long[] updated;
    private long[] deleted;
    private long start;

    public void dump() {
      System.out.printf("%06d:%06d min=%d max=%d%n", getStart(), getEnd(),
        getMinProcId(), getMaxProcId());
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

    public static BitSetNode convert(ProcedureProtos.ProcedureStoreTracker.TrackerNode data) {
      long start = data.getStartId();
      int size = data.getUpdatedCount();
      long[] updated = new long[size];
      long[] deleted = new long[size];
      for (int i = 0; i < size; ++i) {
        updated[i] = data.getUpdated(i);
        deleted[i] = data.getDeleted(i);
      }
      return new BitSetNode(start, updated, deleted);
    }

    // ========================================================================
    //  Grow/Merge Helpers
    // ========================================================================
    public boolean canGrow(final long procId) {
      return Math.abs(procId - start) < MAX_NODE_SIZE;
    }

    public boolean canMerge(final BitSetNode rightNode) {
      assert start < rightNode.getEnd();
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
    public long getMinProcId() {
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

    public long getMaxProcId() {
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

      if (isDeleted) {
        updated[wordIndex] |= value;
        deleted[wordIndex] |= value;
      } else {
        updated[wordIndex] |= value;
        deleted[wordIndex] &= ~value;
      }
    }

    // ========================================================================
    //  Helpers
    // ========================================================================
    private static long alignUp(final long x) {
      return (x + (BITS_PER_WORD - 1)) & -BITS_PER_WORD;
    }

    private static long alignDown(final long x) {
      return x & -BITS_PER_WORD;
    }
  }

  public void insert(final Procedure proc, final Procedure[] subprocs) {
    insert(proc.getProcId());
    if (subprocs != null) {
      for (int i = 0; i < subprocs.length; ++i) {
        insert(subprocs[i].getProcId());
      }
    }
  }

  public void update(final Procedure proc) {
    update(proc.getProcId());
  }

  public void insert(long procId) {
    BitSetNode node = getOrCreateNode(procId);
    node.update(procId);
    trackProcIds(procId);
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
  }

  public void clear() {
    this.map.clear();
    resetUpdates();
  }

  public DeleteState isDeleted(long procId) {
    Map.Entry<Long, BitSetNode> entry = map.floorEntry(procId);
    if (entry != null && entry.getValue().contains(procId)) {
      BitSetNode node = entry.getValue();
      DeleteState state = node.isDeleted(procId);
      return partial && !node.isUpdated(procId) ? DeleteState.MAYBE : state;
    }
    return partial ? DeleteState.MAYBE : DeleteState.YES;
  }

  public long getMinProcId() {
    // TODO: Cache?
    Map.Entry<Long, BitSetNode> entry = map.firstEntry();
    return entry == null ? 0 : entry.getValue().getMinProcId();
  }

  public void setKeepDeletes(boolean keepDeletes) {
    this.keepDeletes = keepDeletes;
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

  public void setPartialFlag(boolean isPartial) {
    if (this.partial && !isPartial) {
      for (Map.Entry<Long, BitSetNode> entry : map.entrySet()) {
        entry.getValue().unsetPartialFlag();
      }
    }
    this.partial = isPartial;
  }

  public boolean isEmpty() {
    for (Map.Entry<Long, BitSetNode> entry : map.entrySet()) {
      if (entry.getValue().isEmpty() == false) {
        return false;
      }
    }
    return true;
  }

  public boolean isUpdated() {
    for (Map.Entry<Long, BitSetNode> entry : map.entrySet()) {
      if (entry.getValue().isUpdated() == false) {
        return false;
      }
    }
    return true;
  }

  public boolean isTracking(long minId, long maxId) {
    // TODO: we can make it more precise, instead of looking just at the block
    return map.floorEntry(minId) != null || map.floorEntry(maxId) != null;
  }

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
    // can procId fit in the left node?
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

        if (leftCanGrow && rightCanGrow) {
          if ((procId - leftNode.getEnd()) <= (rightNode.getStart() - procId)) {
            // grow the left node
            return growNode(leftNode, procId);
          }
          // grow the right node
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

    // add new node
    BitSetNode node = new BitSetNode(procId, partial);
    map.put(node.getStart(), node);
    return node;
  }

  private BitSetNode growNode(BitSetNode node, long procId) {
    map.remove(node.getStart());
    node.grow(procId);
    map.put(node.getStart(), node);
    return node;
  }

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

  public void writeTo(final OutputStream stream) throws IOException {
    ProcedureProtos.ProcedureStoreTracker.Builder builder =
        ProcedureProtos.ProcedureStoreTracker.newBuilder();
    for (Map.Entry<Long, BitSetNode> entry : map.entrySet()) {
      builder.addNode(entry.getValue().convert());
    }
    builder.build().writeDelimitedTo(stream);
  }

  public void readFrom(final InputStream stream) throws IOException {
    ProcedureProtos.ProcedureStoreTracker data =
        ProcedureProtos.ProcedureStoreTracker.parseDelimitedFrom(stream);
    map.clear();
    for (ProcedureProtos.ProcedureStoreTracker.TrackerNode protoNode: data.getNodeList()) {
      BitSetNode node = BitSetNode.convert(protoNode);
      map.put(node.getStart(), node);
    }
  }
}
