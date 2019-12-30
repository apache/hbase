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
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.stream.LongStream;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * Keeps track of live procedures.
 *
 * It can be used by the ProcedureStore to identify which procedures are already
 * deleted/completed to avoid the deserialization step on restart
 * @deprecated Since 2.3.0, will be removed in 4.0.0. Keep here only for rolling upgrading, now we
 *             use the new region based procedure store.
 */
@Deprecated
@InterfaceAudience.Private
class ProcedureStoreTracker {
  private static final Logger LOG = LoggerFactory.getLogger(ProcedureStoreTracker.class);

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
  boolean partial = false;

  private long minModifiedProcId = Long.MAX_VALUE;
  private long maxModifiedProcId = Long.MIN_VALUE;

  public enum DeleteState { YES, NO, MAYBE }

  public void resetToProto(ProcedureProtos.ProcedureStoreTracker trackerProtoBuf) {
    reset();
    for (ProcedureProtos.ProcedureStoreTracker.TrackerNode protoNode :
            trackerProtoBuf.getNodeList()) {
      final BitSetNode node = new BitSetNode(protoNode);
      map.put(node.getStart(), node);
    }
  }

  /**
   * Resets internal state to same as given {@code tracker}. Does deep copy of the bitmap.
   */
  public void resetTo(ProcedureStoreTracker tracker) {
    resetTo(tracker, false);
  }

  /**
   * Resets internal state to same as given {@code tracker}, and change the deleted flag according
   * to the modified flag if {@code resetDelete} is true. Does deep copy of the bitmap.
   * <p/>
   * The {@code resetDelete} will be set to true when building cleanup tracker, please see the
   * comments in {@link BitSetNode#BitSetNode(BitSetNode, boolean)} to learn how we change the
   * deleted flag if {@code resetDelete} is true.
   */
  public void resetTo(ProcedureStoreTracker tracker, boolean resetDelete) {
    reset();
    // resetDelete will true if we are building the cleanup tracker, as we will reset deleted flags
    // for all the unmodified bits to 1, the partial flag is useless so set it to false for not
    // confusing the developers when debugging.
    this.partial = resetDelete ? false : tracker.partial;
    this.minModifiedProcId = tracker.minModifiedProcId;
    this.maxModifiedProcId = tracker.maxModifiedProcId;
    this.keepDeletes = tracker.keepDeletes;
    for (Map.Entry<Long, BitSetNode> entry : tracker.map.entrySet()) {
      map.put(entry.getKey(), new BitSetNode(entry.getValue(), resetDelete));
    }
  }

  public void insert(long procId) {
    insert(null, procId);
  }

  public void insert(long[] procIds) {
    for (int i = 0; i < procIds.length; ++i) {
      insert(procIds[i]);
    }
  }

  public void insert(long procId, long[] subProcIds) {
    BitSetNode node = update(null, procId);
    for (int i = 0; i < subProcIds.length; ++i) {
      node = insert(node, subProcIds[i]);
    }
  }

  private BitSetNode insert(BitSetNode node, long procId) {
    if (node == null || !node.contains(procId)) {
      node = getOrCreateNode(procId);
    }
    node.insertOrUpdate(procId);
    trackProcIds(procId);
    return node;
  }

  public void update(long procId) {
    update(null, procId);
  }

  private BitSetNode update(BitSetNode node, long procId) {
    node = lookupClosestNode(node, procId);
    assert node != null : "expected node to update procId=" + procId;
    assert node.contains(procId) : "expected procId=" + procId + " in the node";
    if (node == null) {
      throw new NullPointerException("pid=" + procId);
    }
    node.insertOrUpdate(procId);
    trackProcIds(procId);
    return node;
  }

  public void delete(long procId) {
    delete(null, procId);
  }

  public void delete(final long[] procIds) {
    Arrays.sort(procIds);
    BitSetNode node = null;
    for (int i = 0; i < procIds.length; ++i) {
      node = delete(node, procIds[i]);
    }
  }

  private BitSetNode delete(BitSetNode node, long procId) {
    node = lookupClosestNode(node, procId);
    if (node == null || !node.contains(procId)) {
      LOG.warn("The BitSetNode for procId={} does not exist, maybe a double deletion?", procId);
      return node;
    }
    node.delete(procId);
    if (!keepDeletes && node.isEmpty()) {
      // TODO: RESET if (map.size() == 1)
      map.remove(node.getStart());
    }

    trackProcIds(procId);
    return node;
  }

  /**
   * Will be called when restarting where we need to rebuild the ProcedureStoreTracker.
   */
  public void setMinMaxModifiedProcIds(long min, long max) {
    this.minModifiedProcId = min;
    this.maxModifiedProcId = max;
  }
  /**
   * This method is used when restarting where we need to rebuild the ProcedureStoreTracker. The
   * {@link #delete(long)} method above assume that the {@link BitSetNode} exists, but when restart
   * this is not true, as we will read the wal files in reverse order so a delete may come first.
   */
  public void setDeleted(long procId, boolean isDeleted) {
    BitSetNode node = getOrCreateNode(procId);
    assert node.contains(procId) : "expected procId=" + procId + " in the node=" + node;
    node.updateState(procId, isDeleted);
    trackProcIds(procId);
  }

  /**
   * Set the given bit for the procId to delete if it was modified before.
   * <p/>
   * This method is used to test whether a procedure wal file can be safely deleted, as if all the
   * procedures in the given procedure wal file has been modified in the new procedure wal files,
   * then we can delete it.
   */
  public void setDeletedIfModified(long... procId) {
    BitSetNode node = null;
    for (int i = 0; i < procId.length; ++i) {
      node = lookupClosestNode(node, procId[i]);
      if (node != null && node.isModified(procId[i])) {
        node.delete(procId[i]);
      }
    }
  }

  private void setDeleteIf(ProcedureStoreTracker tracker,
      BiFunction<BitSetNode, Long, Boolean> func) {
    BitSetNode trackerNode = null;
    for (BitSetNode node : map.values()) {
      long minProcId = node.getStart();
      long maxProcId = node.getEnd();
      for (long procId = minProcId; procId <= maxProcId; ++procId) {
        if (!node.isModified(procId)) {
          continue;
        }

        trackerNode = tracker.lookupClosestNode(trackerNode, procId);
        if (func.apply(trackerNode, procId)) {
          node.delete(procId);
        }
      }
    }
  }

  /**
   * For the global tracker, we will use this method to build the holdingCleanupTracker, as the
   * modified flags will be cleared after rolling so we only need to test the deleted flags.
   * @see #setDeletedIfModifiedInBoth(ProcedureStoreTracker)
   */
  public void setDeletedIfDeletedByThem(ProcedureStoreTracker tracker) {
    setDeleteIf(tracker, (node, procId) -> node == null || !node.contains(procId) ||
      node.isDeleted(procId) == DeleteState.YES);
  }

  /**
   * Similar with {@link #setDeletedIfModified(long...)}, but here the {@code procId} are given by
   * the {@code tracker}. If a procedure is modified by us, and also by the given {@code tracker},
   * then we mark it as deleted.
   * @see #setDeletedIfModified(long...)
   */
  public void setDeletedIfModifiedInBoth(ProcedureStoreTracker tracker) {
    setDeleteIf(tracker, (node, procId) -> node != null && node.isModified(procId));
  }

  /**
   * lookup the node containing the specified procId.
   * @param node cached node to check before doing a lookup
   * @param procId the procId to lookup
   * @return the node that may contains the procId or null
   */
  private BitSetNode lookupClosestNode(final BitSetNode node, final long procId) {
    if (node != null && node.contains(procId)) {
      return node;
    }

    final Map.Entry<Long, BitSetNode> entry = map.floorEntry(procId);
    return entry != null ? entry.getValue() : null;
  }

  private void trackProcIds(long procId) {
    minModifiedProcId = Math.min(minModifiedProcId, procId);
    maxModifiedProcId = Math.max(maxModifiedProcId, procId);
  }

  public long getModifiedMinProcId() {
    return minModifiedProcId;
  }

  public long getModifiedMaxProcId() {
    return maxModifiedProcId;
  }

  public void reset() {
    this.keepDeletes = false;
    this.partial = false;
    this.map.clear();
    minModifiedProcId = Long.MAX_VALUE;
    maxModifiedProcId = Long.MIN_VALUE;
  }

  public boolean isModified(long procId) {
    final Map.Entry<Long, BitSetNode> entry = map.floorEntry(procId);
    return entry != null && entry.getValue().contains(procId) &&
      entry.getValue().isModified(procId);
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
      return partial && !node.isModified(procId) ? DeleteState.MAYBE : state;
    }
    return partial ? DeleteState.MAYBE : DeleteState.YES;
  }

  public long getActiveMinProcId() {
    Map.Entry<Long, BitSetNode> entry = map.firstEntry();
    return entry == null ? Procedure.NO_PROC_ID : entry.getValue().getActiveMinProcId();
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
   * @return true if all procedure was modified or deleted since last call to
   *         {@link #resetModified()}.
   */
  public boolean isAllModified() {
    for (Map.Entry<Long, BitSetNode> entry : map.entrySet()) {
      if (!entry.getValue().isAllModified()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Will be used when there are too many proc wal files. We will rewrite the states of the active
   * procedures in the oldest proc wal file so that we can delete it.
   * @return all the active procedure ids in this tracker.
   */
  public long[] getAllActiveProcIds() {
    return map.values().stream().map(BitSetNode::getActiveProcIds).filter(p -> p.length > 0)
      .flatMapToLong(LongStream::of).toArray();
  }

  /**
   * Clears the list of updated procedure ids. This doesn't affect global list of active
   * procedure ids.
   */
  public void resetModified() {
    for (Map.Entry<Long, BitSetNode> entry : map.entrySet()) {
      entry.getValue().resetModified();
    }
    minModifiedProcId = Long.MAX_VALUE;
    maxModifiedProcId = Long.MIN_VALUE;
  }

  private BitSetNode getOrCreateNode(long procId) {
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
    System.out.println("isAllModified " + isAllModified());
    System.out.println("isEmpty " + isEmpty());
    for (Map.Entry<Long, BitSetNode> entry : map.entrySet()) {
      entry.getValue().dump();
    }
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
