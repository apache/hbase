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

package org.apache.hadoop.hbase.io.util;

import java.util.HashMap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;

/**
 * WALDictionary using an LRU eviction algorithm. Uses a linked list running
 * through a hashtable.  Currently has max of 2^15 entries.  Will start
 * evicting if exceeds this number  The maximum memory we expect this dictionary
 * to take in the worst case is about:
 * <code>(2 ^ 15) * 5 (Regionname, Row key, CF, Column qual, table) * 100 bytes 
 * (these are some big names) = ~16MB</code>.
 * If you want to get silly, even at 1kb entries, it maxes out at 160 megabytes.
 */
@InterfaceAudience.Private
public class LRUDictionary implements Dictionary {

  BidirectionalLRUMap backingStore;
  @Override
  public byte[] getEntry(short idx) {
    return backingStore.get(idx);
  }

  @Override
  public void init(int initialSize) {
    backingStore = new BidirectionalLRUMap(initialSize);
  }
  @Override
  public short findEntry(byte[] data, int offset, int length) {
    short ret = backingStore.findIdx(data, offset, length);
    if (ret == NOT_IN_DICTIONARY) {
      addEntry(data, offset, length);
    }
    return ret;
  }

  @Override
  public short addEntry(byte[] data, int offset, int length) {
    if (length <= 0) return NOT_IN_DICTIONARY;
    return backingStore.put(data, offset, length);
  }

  @Override
  public void clear() {
    backingStore.clear();
  }

  /*
   * Internal class used to implement LRU eviction and dual lookup (by key and
   * value).
   * 
   * This is not thread safe. Don't use in multi-threaded applications.
   */
  static class BidirectionalLRUMap {
    private int currSize = 0;

    // Head and tail of the LRU list.
    private Node head;
    private Node tail;

    private HashMap<Node, Short> nodeToIndex = new HashMap<Node, Short>();
    private Node[] indexToNode;
    private int initSize = 0;

    public BidirectionalLRUMap(int initialSize) {
      initSize = initialSize;
      indexToNode = new Node[initialSize];
    }

    private short put(byte[] array, int offset, int length) {
      // We copy the bytes we want, otherwise we might be holding references to
      // massive arrays in our dictionary (or those arrays might change)
      byte[] stored = new byte[length];
      Bytes.putBytes(stored, 0, array, offset, length);

      if (currSize < initSize) {
        // There is space to add without evicting.
        if (indexToNode[currSize] == null) {
          indexToNode[currSize] = new Node();
        }
        indexToNode[currSize].setContents(stored, 0, stored.length);
        setHead(indexToNode[currSize]);
        short ret = (short) currSize++;
        nodeToIndex.put(indexToNode[ret], ret);
        return ret;
      } else {
        short s = nodeToIndex.remove(tail);
        tail.setContents(stored, 0, stored.length);
        // we need to rehash this.
        nodeToIndex.put(tail, s);
        moveToHead(tail);
        return s;
      }
    }

    private short findIdx(byte[] array, int offset, int length) {
      Short s;
      final Node comparisonNode = new Node();
      comparisonNode.setContents(array, offset, length);
      if ((s = nodeToIndex.get(comparisonNode)) != null) {
        moveToHead(indexToNode[s]);
        return s;
      } else {
        return -1;
      }
    }

    private byte[] get(short idx) {
      Preconditions.checkElementIndex(idx, currSize);
      moveToHead(indexToNode[idx]);
      return indexToNode[idx].container;
    }

    private void moveToHead(Node n) {
      if (head == n) {
        // no-op -- it's already the head.
        return;
      }
      // At this point we definitely have prev, since it's not the head.
      assert n.prev != null;
      // Unlink prev.
      n.prev.next = n.next;

      // Unlink next
      if (n.next != null) {
        n.next.prev = n.prev;
      } else {
        assert n == tail;
        tail = n.prev;
      }
      // Node is now removed from the list. Re-add it at the head.
      setHead(n);
    }
    
    private void setHead(Node n) {
      // assume it's already unlinked from the list at this point.
      n.prev = null;
      n.next = head;
      if (head != null) {
        assert head.prev == null;
        head.prev = n;
      }

      head = n;

      // First entry
      if (tail == null) {
        tail = n;
      }
    }

    private void clear() {
      for (int i = 0; i < currSize; i++) {
        indexToNode[i].next = null;
        indexToNode[i].prev = null;
        indexToNode[i].container = null;
      }
      currSize = 0;
      nodeToIndex.clear();
      tail = null;
      head = null;
    }

    private static class Node {
      byte[] container;
      int offset;
      int length;
      Node next; // link towards the tail
      Node prev; // link towards the head

      public Node() {
      }

      private void setContents(byte[] container, int offset, int length) {
        this.container = container;
        this.offset = offset;
        this.length = length;
      }

      @Override
      public int hashCode() {
        return Bytes.hashCode(container, offset, length);
      }

      @Override
      public boolean equals(Object other) {
        if (!(other instanceof Node)) {
          return false;
        }

        Node casted = (Node) other;
        return Bytes.equals(container, offset, length, casted.container,
            casted.offset, casted.length);
      }
    }
  }
}
