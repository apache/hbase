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

import java.util.Arrays;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStoreTracker.DeleteState;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * A bitmap which can grow/merge with other {@link BitSetNode} (if certain conditions are met).
 * Boundaries of bitmap are aligned to multiples of {@link BitSetNode#BITS_PER_WORD}. So the range
 * of a {@link BitSetNode} is from [x * K, y * K) where x and y are integers, y > x and K is
 * BITS_PER_WORD.
 * <p/>
 * We have two main bit sets to describe the state of procedures, the meanings are:
 *
 * <pre>
 *  ----------------------
 * | modified | deleted |  meaning
 * |     0    |   0     |  proc exists, but hasn't been updated since last resetUpdates().
 * |     1    |   0     |  proc was updated (but not deleted).
 * |     1    |   1     |  proc was deleted.
 * |     0    |   1     |  proc doesn't exist (maybe never created, maybe deleted in past).
 * ----------------------
 * </pre>
 *
 * The meaning of modified is that, we have modified the state of the procedure, no matter insert,
 * update, or delete. And if it is an insert or update, we will set the deleted to 0, if not we will
 * set the delete to 1.
 * <p/>
 * For a non-partial BitSetNode, the initial modified value is 0 and deleted value is 1. For the
 * partial one, the initial modified value is 0 and the initial deleted value is also 0. In
 * {@link #unsetPartialFlag()} we will reset the deleted to 1 if it is not modified.
 */
@InterfaceAudience.Private
class BitSetNode {
  private static final long WORD_MASK = 0xffffffffffffffffL;
  private static final int ADDRESS_BITS_PER_WORD = 6;
  private static final int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;
  private static final int MAX_NODE_SIZE = 1 << ADDRESS_BITS_PER_WORD;

  /**
   * Mimics {@link ProcedureStoreTracker#partial}. It will effect how we fill the new deleted bits
   * when growing.
   */
  private boolean partial;

  /**
   * Set of procedures which have been modified since last {@link #resetModified()}. Useful to track
   * procedures which have been modified since last WAL write.
   */
  private long[] modified;

  /**
   * Keeps track of procedure ids which belong to this bitmap's range and have been deleted. This
   * represents global state since it's not reset on WAL rolls.
   */
  private long[] deleted;
  /**
   * Offset of bitmap i.e. procedure id corresponding to first bit.
   */
  private long start;

  public void dump() {
    System.out.printf("%06d:%06d min=%d max=%d%n", getStart(), getEnd(), getActiveMinProcId(),
      getActiveMaxProcId());
    System.out.println("Modified:");
    for (int i = 0; i < modified.length; ++i) {
      for (int j = 0; j < BITS_PER_WORD; ++j) {
        System.out.print((modified[i] & (1L << j)) != 0 ? "1" : "0");
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

  public BitSetNode(long procId, boolean partial) {
    start = alignDown(procId);

    int count = 1;
    modified = new long[count];
    deleted = new long[count];
    if (!partial) {
      Arrays.fill(deleted, WORD_MASK);
    }

    this.partial = partial;
    updateState(procId, false);
  }

  public BitSetNode(ProcedureProtos.ProcedureStoreTracker.TrackerNode data) {
    start = data.getStartId();
    int size = data.getUpdatedCount();
    assert size == data.getDeletedCount();
    modified = new long[size];
    deleted = new long[size];
    for (int i = 0; i < size; ++i) {
      modified[i] = data.getUpdated(i);
      deleted[i] = data.getDeleted(i);
    }
    partial = false;
  }

  public BitSetNode(BitSetNode other, boolean resetDelete) {
    this.start = other.start;
    this.partial = other.partial;
    this.modified = other.modified.clone();
    // The resetDelete will be set to true when building cleanup tracker.
    // The intention here is that, if a procedure is not modified in this tracker, then we do not
    // need to take care of it, so we will set deleted to true for these bits, i.e, if modified is
    // 0, then we set deleted to 1, otherwise keep it as is. So here, the equation is
    // deleted |= ~modified, i.e,
    if (resetDelete) {
      this.deleted = new long[other.deleted.length];
      for (int i = 0; i < this.deleted.length; ++i) {
        this.deleted[i] |= ~(other.modified[i]);
      }
    } else {
      this.deleted = other.deleted.clone();
    }
  }

  public void insertOrUpdate(final long procId) {
    updateState(procId, false);
  }

  public void delete(final long procId) {
    updateState(procId, true);
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return start + (modified.length << ADDRESS_BITS_PER_WORD) - 1;
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

  public boolean isModified(long procId) {
    int bitmapIndex = getBitmapIndex(procId);
    int wordIndex = bitmapIndex >> ADDRESS_BITS_PER_WORD;
    if (wordIndex >= modified.length) {
      return false;
    }
    return (modified[wordIndex] & (1L << bitmapIndex)) != 0;
  }

  /**
   * @return true, if all the procedures has been modified.
   */
  public boolean isAllModified() {
    // TODO: cache the value
    for (int i = 0; i < modified.length; ++i) {
      if ((modified[i] | deleted[i]) != WORD_MASK) {
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

  public void resetModified() {
    Arrays.fill(modified, 0);
  }

  public void unsetPartialFlag() {
    partial = false;
    for (int i = 0; i < modified.length; ++i) {
      for (int j = 0; j < BITS_PER_WORD; ++j) {
        if ((modified[i] & (1L << j)) == 0) {
          deleted[i] |= (1L << j);
        }
      }
    }
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
    for (int i = 0; i < modified.length; ++i) {
      builder.addUpdated(modified[i]);
      builder.addDeleted(deleted[i]);
    }
    return builder.build();
  }

  // ========================================================================
  // Grow/Merge Helpers
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
      delta = (int) (start - newStart) >> ADDRESS_BITS_PER_WORD;
      offset = delta;
      start = newStart;
    } else {
      // Add to tail
      long newEnd = alignUp(procId + 1);
      delta = (int) (newEnd - getEnd()) >> ADDRESS_BITS_PER_WORD;
      offset = 0;
    }

    long[] newBitmap;
    int oldSize = modified.length;

    newBitmap = new long[oldSize + delta];
    for (int i = 0; i < newBitmap.length; ++i) {
      newBitmap[i] = 0;
    }
    System.arraycopy(modified, 0, newBitmap, offset, oldSize);
    modified = newBitmap;

    newBitmap = new long[deleted.length + delta];
    for (int i = 0; i < newBitmap.length; ++i) {
      newBitmap[i] = partial ? 0 : WORD_MASK;
    }
    System.arraycopy(deleted, 0, newBitmap, offset, oldSize);
    deleted = newBitmap;
  }

  public void merge(final BitSetNode rightNode) {
    int delta = (int) (rightNode.getEnd() - getEnd()) >> ADDRESS_BITS_PER_WORD;

    long[] newBitmap;
    int oldSize = modified.length;
    int newSize = (delta - rightNode.modified.length);
    int offset = oldSize + newSize;

    newBitmap = new long[oldSize + delta];
    System.arraycopy(modified, 0, newBitmap, 0, oldSize);
    System.arraycopy(rightNode.modified, 0, newBitmap, offset, rightNode.modified.length);
    modified = newBitmap;

    newBitmap = new long[oldSize + delta];
    System.arraycopy(deleted, 0, newBitmap, 0, oldSize);
    System.arraycopy(rightNode.deleted, 0, newBitmap, offset, rightNode.deleted.length);
    deleted = newBitmap;

    for (int i = 0; i < newSize; ++i) {
      modified[offset + i] = 0;
      deleted[offset + i] = partial ? 0 : WORD_MASK;
    }
  }

  @Override
  public String toString() {
    return "BitSetNode(" + getStart() + "-" + getEnd() + ")";
  }

  // ========================================================================
  // Min/Max Helpers
  // ========================================================================
  public long getActiveMinProcId() {
    long minProcId = start;
    for (int i = 0; i < deleted.length; ++i) {
      if (deleted[i] == 0) {
        return (minProcId);
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
  // Bitmap Helpers
  // ========================================================================
  private int getBitmapIndex(final long procId) {
    return (int) (procId - start);
  }

  void updateState(long procId, boolean isDeleted) {
    int bitmapIndex = getBitmapIndex(procId);
    int wordIndex = bitmapIndex >> ADDRESS_BITS_PER_WORD;
    long value = (1L << bitmapIndex);

    modified[wordIndex] |= value;
    if (isDeleted) {
      deleted[wordIndex] |= value;
    } else {
      deleted[wordIndex] &= ~value;
    }
  }

  // ========================================================================
  // Helpers
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