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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.htrace.Trace;

/**
 * This is the scanner for any MemStore implementation, derived from MemStore.
 * The MemStoreScanner combines SegmentScanner from different Segments and
 * uses the key-value heap and the reversed key-value heap for the aggregated key-values set.
 * It is assumed that only traversing forward or backward is used (without zigzagging in between)
 */
@InterfaceAudience.Private
public class MemStoreScanner extends NonLazyKeyValueScanner {
  /**
   * Types of cell MemStoreScanner
   */
  static public enum Type {
    UNDEFINED,
    COMPACT_FORWARD,
    USER_SCAN_FORWARD,
    USER_SCAN_BACKWARD
  }

  // heap of scanners used for traversing forward
  private KeyValueHeap forwardHeap;
  // reversed scanners heap for traversing backward
  private ReversedKeyValueHeap backwardHeap;

  // The type of the scan is defined by constructor
  // or according to the first usage
  private Type type = Type.UNDEFINED;

  private long readPoint;
  // remember the initial version of the scanners list
  List<KeyValueScanner> scanners;
  // pointer back to the relevant MemStore
  // is needed for shouldSeek() method
  private AbstractMemStore backwardReferenceToMemStore;

  /**
   * If UNDEFINED type for MemStoreScanner is provided, the forward heap is used as default!
   * After constructor only one heap is going to be initialized for entire lifespan
   * of the MemStoreScanner. A specific scanner can only be one directional!
   *
   * @param ms        Pointer back to the MemStore
   * @param scanners  List of scanners over the segments
   * @param readPt    Read point below which we can safely remove duplicate KVs
   */
  public MemStoreScanner(AbstractMemStore ms, List<KeyValueScanner> scanners, long readPt)
      throws IOException {
    this(ms, scanners, readPt, Type.UNDEFINED);
  }

  /**
   * If UNDEFINED type for MemStoreScanner is provided, the forward heap is used as default!
   * After constructor only one heap is going to be initialized for entire lifespan
   * of the MemStoreScanner. A specific scanner can only be one directional!
   *
   * @param ms        Pointer back to the MemStore
   * @param scanners  List of scanners over the segments
   * @param readPt Read point below which we can safely remove duplicate KVs
   * @param type      The scan type COMPACT_FORWARD should be used for compaction
   */
  public MemStoreScanner(AbstractMemStore ms, List<KeyValueScanner> scanners, long readPt,
      Type type) throws IOException {
    super();
    this.readPoint = readPt;
    this.type = type;
    switch (type) {
      case UNDEFINED:
      case USER_SCAN_FORWARD:
      case COMPACT_FORWARD:
        this.forwardHeap = new KeyValueHeap(scanners, ms.getComparator());
        break;
      case USER_SCAN_BACKWARD:
        this.backwardHeap = new ReversedKeyValueHeap(scanners, ms.getComparator());
        break;
      default:
        throw new IllegalArgumentException("Unknown scanner type in MemStoreScanner");
    }
    this.backwardReferenceToMemStore = ms;
    this.scanners = scanners;
    if (Trace.isTracing() && Trace.currentSpan() != null) {
      Trace.currentSpan().addTimelineAnnotation("Creating MemStoreScanner");
    }
  }

  /**
   * Returns the cell from the top-most scanner without advancing the iterator.
   * The backward traversal is assumed, only if specified explicitly
   */
  @Override
  public synchronized Cell peek() {
    if (type == Type.USER_SCAN_BACKWARD) {
      return backwardHeap.peek();
    }
    return forwardHeap.peek();
  }

  /**
   * Gets the next cell from the top-most scanner. Assumed forward scanning.
   */
  @Override
  public synchronized Cell next() throws IOException {
    KeyValueHeap heap = (Type.USER_SCAN_BACKWARD == type) ? backwardHeap : forwardHeap;

    // loop over till the next suitable value
    // take next value from the heap
    for (Cell currentCell = heap.next();
         currentCell != null;
         currentCell = heap.next()) {

      // all the logic of presenting cells is inside the internal SegmentScanners
      // located inside the heap

      return currentCell;
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
  public synchronized boolean seek(Cell cell) throws IOException {
    assertForward();

    if (cell == null) {
      close();
      return false;
    }

    return forwardHeap.seek(cell);
  }

  /**
   * Move forward on the sub-lists set previously by seek. Assumed forward scanning.
   *
   * @param cell seek value (should be non-null)
   * @return true if there is at least one KV to read, false otherwise
   */
  @Override
  public synchronized boolean reseek(Cell cell) throws IOException {
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
    assertForward();
    return forwardHeap.reseek(cell);
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
  public synchronized void close() {

    if (forwardHeap != null) {
      assert ((type == Type.USER_SCAN_FORWARD) ||
          (type == Type.COMPACT_FORWARD) || (type == Type.UNDEFINED));
      forwardHeap.close();
      forwardHeap = null;
      if (backwardHeap != null) {
        backwardHeap.close();
        backwardHeap = null;
      }
    } else if (backwardHeap != null) {
      assert (type == Type.USER_SCAN_BACKWARD);
      backwardHeap.close();
      backwardHeap = null;
    }
  }

  /**
   * Set the scanner at the seek key. Assumed backward scanning.
   *
   * @param cell seek value
   * @return false if the key is null or if there is no data
   */
  @Override
  public synchronized boolean backwardSeek(Cell cell) throws IOException {
    initBackwardHeapIfNeeded(cell, false);
    return backwardHeap.backwardSeek(cell);
  }

  /**
   * Assumed backward scanning.
   *
   * @param cell seek value
   * @return false if the key is null or if there is no data
   */
  @Override
  public synchronized boolean seekToPreviousRow(Cell cell) throws IOException {
    initBackwardHeapIfNeeded(cell, false);
    if (backwardHeap.peek() == null) {
      restartBackwardHeap(cell);
    }
    return backwardHeap.seekToPreviousRow(cell);
  }

  @Override
  public synchronized boolean seekToLastRow() throws IOException {
    // TODO: it looks like this is how it should be, however ReversedKeyValueHeap class doesn't
    // implement seekToLastRow() method :(
    // however seekToLastRow() was implemented in internal MemStoreScanner
    // so I wonder whether we need to come with our own workaround, or to update
    // ReversedKeyValueHeap
    return initBackwardHeapIfNeeded(KeyValue.LOWESTKEY, true);
  }

  /**
   * Check if this memstore may contain the required keys
   * @return False if the key definitely does not exist in this Memstore
   */
  @Override
  public synchronized boolean shouldUseScanner(Scan scan, Store store, long oldestUnexpiredTS) {

    if (type == Type.COMPACT_FORWARD) {
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
    this.backwardHeap =
        new ReversedKeyValueHeap(scanners, backwardReferenceToMemStore.getComparator());
    return res;
  }

  /**
   * Checks whether the type of the scan suits the assumption of moving backward
   */
  private boolean initBackwardHeapIfNeeded(Cell cell, boolean toLast) throws IOException {
    boolean res = false;
    if (toLast && (type != Type.UNDEFINED)) {
      throw new IllegalStateException(
          "Wrong usage of initBackwardHeapIfNeeded in parameters. The type is:" + type.toString());
    }
    if (type == Type.UNDEFINED) {
      // In case we started from peek, release the forward heap
      // and build backward. Set the correct type. Thus this turn
      // can happen only once
      if ((backwardHeap == null) && (forwardHeap != null)) {
        forwardHeap.close();
        forwardHeap = null;
        // before building the heap seek for the relevant key on the scanners,
        // for the heap to be built from the scanners correctly
        for (KeyValueScanner scan : scanners) {
          if (toLast) {
            res |= scan.seekToLastRow();
          } else {
            res |= scan.backwardSeek(cell);
          }
        }
        this.backwardHeap =
            new ReversedKeyValueHeap(scanners, backwardReferenceToMemStore.getComparator());
        type = Type.USER_SCAN_BACKWARD;
      }
    }

    if (type == Type.USER_SCAN_FORWARD) {
      throw new IllegalStateException("Traversing backward with forward scan");
    }
    return res;
  }

  /**
   * Checks whether the type of the scan suits the assumption of moving forward
   */
  private void assertForward() throws IllegalStateException {
    if (type == Type.UNDEFINED) {
      type = Type.USER_SCAN_FORWARD;
    }

    if (type == Type.USER_SCAN_BACKWARD) {
      throw new IllegalStateException("Traversing forward with backward scan");
    }
  }
}
