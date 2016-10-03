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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.htrace.Trace;

/**
 * This is the scanner for any MemStore implementation, derived from MemStore.
 * The MemStoreScanner combines KeyValueScanner from different Segments and
 * uses the key-value heap and the reversed key-value heap for the aggregated key-values set.
 * It is assumed that only traversing forward or backward is used (without zigzagging in between)
 */
@InterfaceAudience.Private
public class MemStoreScanner extends NonLazyKeyValueScanner {

  // heap of scanners, lazily initialized
  private KeyValueHeap heap;

  // indicates if the scanner is created for inmemoryCompaction
  private boolean inmemoryCompaction;

  // remember the initial version of the scanners list
  List<KeyValueScanner> scanners;

  private final CellComparator comparator;

  private boolean closed;

  /**
   * Creates either a forward KeyValue heap or Reverse KeyValue heap based on the type of scan
   * and the heap is lazily initialized
   * @param comparator Cell Comparator
   * @param scanners List of scanners, from which the heap will be built
   * @param inmemoryCompaction true if used for inmemoryCompaction.
   *        In this case, creates a forward heap always.
   */
  public MemStoreScanner(CellComparator comparator, List<KeyValueScanner> scanners,
      boolean inmemoryCompaction) throws IOException {
    super();
    this.comparator = comparator;
    this.scanners = scanners;
    if (Trace.isTracing() && Trace.currentSpan() != null) {
      Trace.currentSpan().addTimelineAnnotation("Creating MemStoreScanner");
    }
    this.inmemoryCompaction = inmemoryCompaction;
    if (inmemoryCompaction) {
      // init the forward scanner in case of inmemoryCompaction
      initForwardKVHeapIfNeeded(comparator, scanners);
    }
  }

  /**
   * Creates either a forward KeyValue heap or Reverse KeyValue heap based on the type of scan
   * and the heap is lazily initialized
   * @param comparator Cell Comparator
   * @param scanners List of scanners, from which the heap will be built
   */
  public MemStoreScanner(CellComparator comparator, List<KeyValueScanner> scanners)
      throws IOException {
    this(comparator, scanners, false);
  }

  private void initForwardKVHeapIfNeeded(CellComparator comparator, List<KeyValueScanner> scanners)
      throws IOException {
    if (heap == null) {
      // lazy init
      // In a normal scan case, at the StoreScanner level before the KVHeap is
      // created we do a seek or reseek. So that will happen
      // on all the scanners that the StoreScanner is
      // made of. So when we get any of those call to this scanner we init the
      // heap here with normal forward KVHeap.
      this.heap = new KeyValueHeap(scanners, comparator);
    }
  }

  private boolean initReverseKVHeapIfNeeded(Cell seekKey, CellComparator comparator,
      List<KeyValueScanner> scanners) throws IOException {
    boolean res = false;
    if (heap == null) {
      // lazy init
      // In a normal reverse scan case, at the ReversedStoreScanner level before the
      // ReverseKeyValueheap is
      // created we do a seekToLastRow or backwardSeek. So that will happen
      // on all the scanners that the ReversedStoreSCanner is
      // made of. So when we get any of those call to this scanner we init the
      // heap here with ReversedKVHeap.
      if (CellUtil.matchingRow(seekKey, HConstants.EMPTY_START_ROW)) {
        for (KeyValueScanner scanner : scanners) {
          res |= scanner.seekToLastRow();
        }
      } else {
        for (KeyValueScanner scanner : scanners) {
          res |= scanner.backwardSeek(seekKey);
        }
      }
      this.heap = new ReversedKeyValueHeap(scanners, comparator);
    }
    return res;
  }

  /**
   * Returns the cell from the top-most scanner without advancing the iterator.
   * The backward traversal is assumed, only if specified explicitly
   */
  @Override
  public Cell peek() {
    if (closed) {
      return null;
    }
    if (this.heap != null) {
      return this.heap.peek();
    }
    // Doing this way in case some test cases tries to peek directly to avoid NPE
    return null;
  }

  /**
   * Gets the next cell from the top-most scanner. Assumed forward scanning.
   */
  @Override
  public Cell next() throws IOException {
    if (closed) {
      return null;
    }
    if(this.heap != null) {
      // loop over till the next suitable value
      // take next value from the heap
      for (Cell currentCell = heap.next();
          currentCell != null;
          currentCell = heap.next()) {
        // all the logic of presenting cells is inside the internal KeyValueScanners
        // located inside the heap
        return currentCell;
      }
    }
    return null;
  }

  /**
   * Set the scanner at the seek key. Assumed forward scanning.
   * Must be called only once: there is no thread safety between the scanner
   * and the memStore.
   *
   * @param cell seek value
   * @return false if the key is null or if there is no data
   */
  @Override
  public boolean seek(Cell cell) throws IOException {
    if (closed) {
      return false;
    }
    initForwardKVHeapIfNeeded(comparator, scanners);

    if (cell == null) {
      close();
      return false;
    }

    return heap.seek(cell);
  }

  /**
   * Move forward on the sub-lists set previously by seek. Assumed forward scanning.
   *
   * @param cell seek value (should be non-null)
   * @return true if there is at least one KV to read, false otherwise
   */
  @Override
  public boolean reseek(Cell cell) throws IOException {
    /*
    * See HBASE-4195 & HBASE-3855 & HBASE-6591 for the background on this implementation.
    * This code is executed concurrently with flush and puts, without locks.
    * Two points must be known when working on this code:
    * 1) It's not possible to use the 'kvTail' and 'snapshot'
    *  variables, as they are modified during a flush.
    * 2) The ideal implementation for performance would use the sub skip list
    *  implicitly pointed by the iterators 'kvsetIt' and
    *  'snapshotIt'. Unfortunately the Java API does not offer a method to
    *  get it. So we remember the last keys we iterated to and restore
    *  the reseeked set to at least that point.
    *
    *  TODO: The above comment copied from the original MemStoreScanner
    */
    if (closed) {
      return false;
    }
    initForwardKVHeapIfNeeded(comparator, scanners);
    return heap.reseek(cell);
  }

  /**
   * MemStoreScanner returns Long.MAX_VALUE because it will always have the latest data among all
   * scanners.
   * @see KeyValueScanner#getScannerOrder()
   */
  @Override
  public long getScannerOrder() {
    return Long.MAX_VALUE;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    // Ensuring that all the segment scanners are closed
    if (heap != null) {
      heap.close();
      // It is safe to do close as no new calls will be made to this scanner.
      heap = null;
    } else {
      for (KeyValueScanner scanner : scanners) {
        scanner.close();
      }
    }
    closed = true;
  }

  /**
   * Set the scanner at the seek key. Assumed backward scanning.
   *
   * @param cell seek value
   * @return false if the key is null or if there is no data
   */
  @Override
  public boolean backwardSeek(Cell cell) throws IOException {
    // The first time when this happens it sets the scanners to the seek key
    // passed by the incoming scan's start row
    if (closed) {
      return false;
    }
    initReverseKVHeapIfNeeded(cell, comparator, scanners);
    return heap.backwardSeek(cell);
  }

  /**
   * Assumed backward scanning.
   *
   * @param cell seek value
   * @return false if the key is null or if there is no data
   */
  @Override
  public boolean seekToPreviousRow(Cell cell) throws IOException {
    if (closed) {
      return false;
    }
    initReverseKVHeapIfNeeded(cell, comparator, scanners);
    if (heap.peek() == null) {
      restartBackwardHeap(cell);
    }
    return heap.seekToPreviousRow(cell);
  }

  @Override
  public boolean seekToLastRow() throws IOException {
    if (closed) {
      return false;
    }
    return initReverseKVHeapIfNeeded(KeyValue.LOWESTKEY, comparator, scanners);
  }

  /**
   * Check if this memstore may contain the required keys
   * @return False if the key definitely does not exist in this Memstore
   */
  @Override
  public boolean shouldUseScanner(Scan scan, Store store, long oldestUnexpiredTS) {
    // TODO : Check if this can be removed.
    if (inmemoryCompaction) {
      return true;
    }

    for (KeyValueScanner sc : scanners) {
      if (sc.shouldUseScanner(scan, store, oldestUnexpiredTS)) {
        return true;
      }
    }
    return false;
  }

  // debug method
  @Override
  public String toString() {
    StringBuffer buf = new StringBuffer();
    int i = 1;
    for (KeyValueScanner scanner : scanners) {
      buf.append("scanner (" + i + ") " + scanner.toString() + " ||| ");
      i++;
    }
    return buf.toString();
  }
  /****************** Private methods ******************/
  /**
   * Restructure the ended backward heap after rerunning a seekToPreviousRow()
   * on each scanner
   * @return false if given Cell does not exist in any scanner
   */
  private boolean restartBackwardHeap(Cell cell) throws IOException {
    boolean res = false;
    for (KeyValueScanner scan : scanners) {
      res |= scan.seekToPreviousRow(cell);
    }
    this.heap =
        new ReversedKeyValueHeap(scanners, comparator);
    return res;
  }
}
