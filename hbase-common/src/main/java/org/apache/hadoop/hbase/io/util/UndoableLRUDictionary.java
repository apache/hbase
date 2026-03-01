/*
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
package org.apache.hadoop.hbase.io.util;

import java.util.IdentityHashMap;
import java.util.Map;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An LRUDictionary that supports checkpoint and rollback. Used for tag dictionary compression in
 * WAL decoding, where dictionary updates within a single cell must be rolled back if the cell read
 * fails (e.g., due to EOF on a WAL being tailed).
 * <p>
 * On {@link #checkpoint()}, saves the current LRU state. Operations proceed normally against the
 * real dictionary. On {@link #rollback()}, restores the dictionary to the checkpointed state. On
 * {@link #commit()}, discards the saved state.
 */
@InterfaceAudience.Private
public class UndoableLRUDictionary extends LRUDictionary {

  private boolean tracking = false;
  private int savedCurrSize;
  private BidirectionalLRUMap.Node savedHead;
  private BidirectionalLRUMap.Node savedTail;
  private final Map<BidirectionalLRUMap.Node, NodeSnapshot> snapshots = new IdentityHashMap<>();

  private static class NodeSnapshot {
    final BidirectionalLRUMap.Node savedPrev;
    final BidirectionalLRUMap.Node savedNext;
    final byte[] savedContents;
    final int savedOffset;
    final int savedLength;

    NodeSnapshot(BidirectionalLRUMap.Node node) {
      this.savedPrev = node.prev;
      this.savedNext = node.next;
      this.savedContents = node.getContents();
      this.savedOffset = node.offset;
      this.savedLength = node.length;
    }
  }

  public void checkpoint() {
    tracking = true;
    savedCurrSize = backingStore.currSize;
    savedHead = backingStore.head;
    savedTail = backingStore.tail;
    snapshots.clear();
  }

  public void commit() {
    snapshots.clear();
    tracking = false;
  }

  public void rollback() {
    if (!tracking) {
      return;
    }
    for (Map.Entry<BidirectionalLRUMap.Node, NodeSnapshot> entry : snapshots.entrySet()) {
      BidirectionalLRUMap.Node node = entry.getKey();
      NodeSnapshot snap = entry.getValue();
      node.prev = snap.savedPrev;
      node.next = snap.savedNext;
      if (snap.savedContents != null) {
        backingStore.nodeToIndex.remove(node);
        node.setContents(snap.savedContents, snap.savedOffset, snap.savedLength);
        backingStore.nodeToIndex.put(node, findIndexForNode(node));
      }
    }
    backingStore.head = savedHead;
    backingStore.tail = savedTail;
    backingStore.currSize = savedCurrSize;
    snapshots.clear();
    tracking = false;
  }

  private short findIndexForNode(BidirectionalLRUMap.Node node) {
    for (short i = 0; i < backingStore.currSize; i++) {
      if (backingStore.indexToNode[i] == node) {
        return i;
      }
    }
    return -1;
  }

  private void saveIfNeeded(BidirectionalLRUMap.Node node) {
    if (tracking && node != null && !snapshots.containsKey(node)) {
      snapshots.put(node, new NodeSnapshot(node));
    }
  }

  @Override
  public byte[] getEntry(short idx) {
    if (tracking) {
      BidirectionalLRUMap.Node node = backingStore.indexToNode[idx];
      saveIfNeeded(node);
      if (node.prev != null) {
        saveIfNeeded(node.prev);
      }
      if (node.next != null) {
        saveIfNeeded(node.next);
      }
      saveIfNeeded(backingStore.head);
    }
    return backingStore.get(idx);
  }

  @Override
  public short addEntry(byte[] data, int offset, int length) {
    if (tracking) {
      if (backingStore.currSize < backingStore.initSize) {
        BidirectionalLRUMap.Node node = backingStore.indexToNode[backingStore.currSize];
        if (node != null) {
          saveIfNeeded(node);
        }
        saveIfNeeded(backingStore.head);
      } else {
        BidirectionalLRUMap.Node tail = backingStore.tail;
        saveIfNeeded(tail);
        if (tail != null && tail.prev != null) {
          saveIfNeeded(tail.prev);
        }
        saveIfNeeded(backingStore.head);
      }
    }
    return addEntryInternal(data, offset, length, true);
  }
}
