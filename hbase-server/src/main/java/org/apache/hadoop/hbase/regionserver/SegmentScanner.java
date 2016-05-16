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
import java.util.Iterator;
import java.util.SortedSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;

/**
 * A scanner of a single memstore segment.
 */
@InterfaceAudience.Private
public class SegmentScanner implements KeyValueScanner {

  /**
   * Order of this scanner relative to other scanners. See
   * {@link KeyValueScanner#getScannerOrder()}.
   */
  private long scannerOrder;
  private static final long DEFAULT_SCANNER_ORDER = Long.MAX_VALUE;

  // the observed structure
  private final Segment segment;
  // the highest relevant MVCC
  private long readPoint;
  // the current iterator that can be reinitialized by
  // seek(), backwardSeek(), or reseek()
  private Iterator<Cell> iter;
  // the pre-calculated cell to be returned by peek()
  private Cell current = null;
  // or next()
  // A flag represents whether could stop skipping KeyValues for MVCC
  // if have encountered the next row. Only used for reversed scan
  private boolean stopSkippingKVsIfNextRow = false;
  // last iterated KVs by seek (to restore the iterator state after reseek)
  private Cell last = null;

  protected SegmentScanner(Segment segment, long readPoint) {
    this(segment, readPoint, DEFAULT_SCANNER_ORDER);
  }

  /**
   * @param scannerOrder see {@link KeyValueScanner#getScannerOrder()}.
   * Scanners are ordered from 0 (oldest) to newest in increasing order.
   */
  protected SegmentScanner(Segment segment, long readPoint, long scannerOrder) {
    this.segment = segment;
    this.readPoint = readPoint;
    iter = segment.iterator();
    // the initialization of the current is required for working with heap of SegmentScanners
    current = getNext();
    //increase the reference count so the underlying structure will not be de-allocated
    this.segment.incScannerCount();
    this.scannerOrder = scannerOrder;
  }

  /**
   * Look at the next Cell in this scanner, but do not iterate the scanner
   * @return the currently observed Cell
   */
  @Override
  public Cell peek() {          // sanity check, the current should be always valid
    if (current!=null && current.getSequenceId() > readPoint) {
      throw new RuntimeException("current is invalid: read point is "+readPoint+", " +
          "while current sequence id is " +current.getSequenceId());
    }
    return current;
  }

  /**
   * Return the next Cell in this scanner, iterating the scanner
   * @return the next Cell or null if end of scanner
   */
  @Override
  public Cell next() throws IOException {
    Cell oldCurrent = current;
    current = getNext();                  // update the currently observed Cell
    return oldCurrent;
  }

  /**
   * Seek the scanner at or after the specified Cell.
   * @param cell seek value
   * @return true if scanner has values left, false if end of scanner
   */
  @Override
  public boolean seek(Cell cell) throws IOException {
    if(cell == null) {
      close();
      return false;
    }
    // restart the iterator from new key
    iter = segment.tailSet(cell).iterator();
    // last is going to be reinitialized in the next getNext() call
    last = null;
    current = getNext();
    return (current != null);
  }

  /**
   * Reseek the scanner at or after the specified KeyValue.
   * This method is guaranteed to seek at or after the required key only if the
   * key comes after the current position of the scanner. Should not be used
   * to seek to a key which may come before the current position.
   *
   * @param cell seek value (should be non-null)
   * @return true if scanner has values left, false if end of scanner
   */
  @Override
  public boolean reseek(Cell cell) throws IOException {

    /*
    See HBASE-4195 & HBASE-3855 & HBASE-6591 for the background on this implementation.
    This code is executed concurrently with flush and puts, without locks.
    The ideal implementation for performance would use the sub skip list implicitly
    pointed by the iterator. Unfortunately the Java API does not offer a method to
    get it. So we remember the last keys we iterated to and restore
    the reseeked set to at least that point.
    */
    iter = segment.tailSet(getHighest(cell, last)).iterator();
    current = getNext();
    return (current != null);
  }

  /**
   * Seek the scanner at or before the row of specified Cell, it firstly
   * tries to seek the scanner at or after the specified Cell, return if
   * peek KeyValue of scanner has the same row with specified Cell,
   * otherwise seek the scanner at the first Cell of the row which is the
   * previous row of specified KeyValue
   *
   * @param key seek Cell
   * @return true if the scanner is at the valid KeyValue, false if such Cell does not exist
   */
  @Override
  public boolean backwardSeek(Cell key) throws IOException {
    seek(key);    // seek forward then go backward
    if (peek() == null || segment.compareRows(peek(), key) > 0) {
      return seekToPreviousRow(key);
    }
    return true;
  }

  /**
   * Seek the scanner at the first Cell of the row which is the previous row
   * of specified key
   *
   * @param cell seek value
   * @return true if the scanner at the first valid Cell of previous row,
   *     false if not existing such Cell
   */
  @Override
  public boolean seekToPreviousRow(Cell cell) throws IOException {
    boolean keepSeeking;
    Cell key = cell;
    do {
      Cell firstKeyOnRow = CellUtil.createFirstOnRow(key);
      SortedSet<Cell> cellHead = segment.headSet(firstKeyOnRow);
      Cell lastCellBeforeRow = cellHead.isEmpty() ? null : cellHead.last();
      if (lastCellBeforeRow == null) {
        current = null;
        return false;
      }
      Cell firstKeyOnPreviousRow = CellUtil.createFirstOnRow(lastCellBeforeRow);
      this.stopSkippingKVsIfNextRow = true;
      seek(firstKeyOnPreviousRow);
      this.stopSkippingKVsIfNextRow = false;
      if (peek() == null
          || segment.getComparator().compareRows(peek(), firstKeyOnPreviousRow) > 0) {
        keepSeeking = true;
        key = firstKeyOnPreviousRow;
        continue;
      } else {
        keepSeeking = false;
      }
    } while (keepSeeking);
    return true;
  }

  /**
   * Seek the scanner at the first KeyValue of last row
   *
   * @return true if scanner has values left, false if the underlying data is empty
   */
  @Override
  public boolean seekToLastRow() throws IOException {
    Cell higherCell = segment.isEmpty() ? null : segment.last();
    if (higherCell == null) {
      return false;
    }

    Cell firstCellOnLastRow = CellUtil.createFirstOnRow(higherCell);

    if (seek(firstCellOnLastRow)) {
      return true;
    } else {
      return seekToPreviousRow(higherCell);
    }
  }

  /**
   * @see KeyValueScanner#getScannerOrder()
   */
  @Override
  public long getScannerOrder() {
    return scannerOrder;
  }

  /**
   * Close the KeyValue scanner.
   */
  @Override
  public void close() {
    getSegment().decScannerCount();
  }

  /**
   * This functionality should be resolved in the higher level which is
   * MemStoreScanner, currently returns true as default. Doesn't throw
   * IllegalStateException in order not to change the signature of the
   * overridden method
   */
  @Override
  public boolean shouldUseScanner(Scan scan, Store store, long oldestUnexpiredTS) {
    return true;
  }
  /**
   * This scanner is working solely on the in-memory MemStore therefore this
   * interface is not relevant.
   */
  @Override
  public boolean requestSeek(Cell c, boolean forward, boolean useBloom)
      throws IOException {

    throw new IllegalStateException(
        "requestSeek cannot be called on MutableCellSetSegmentScanner");
  }

  /**
   * This scanner is working solely on the in-memory MemStore and doesn't work on
   * store files, MutableCellSetSegmentScanner always does the seek,
   * therefore always returning true.
   */
  @Override
  public boolean realSeekDone() {
    return true;
  }

  /**
   * This function should be never called on scanners that always do real seek operations (i.e. most
   * of the scanners and also this one). The easiest way to achieve this is to call
   * {@link #realSeekDone()} first.
   */
  @Override
  public void enforceSeek() throws IOException {
    throw new IllegalStateException(
        "enforceSeek cannot be called on MutableCellSetSegmentScanner");
  }

  /**
   * @return true if this is a file scanner. Otherwise a memory scanner is assumed.
   */
  @Override
  public boolean isFileScanner() {
    return false;
  }

  /**
   * @return the next key in the index (the key to seek to the next block)
   *     if known, or null otherwise
   *     Not relevant for in-memory scanner
   */
  @Override
  public Cell getNextIndexedKey() {
    return null;
  }

  /**
   * Called after a batch of rows scanned (RPC) and set to be returned to client. Any in between
   * cleanup can be done here. Nothing to be done for MutableCellSetSegmentScanner.
   */
  @Override
  public void shipped() throws IOException {
    // do nothing
  }

  /**
   * Returns whether the given scan should seek in this segment
   * @return whether the given scan should seek in this segment
   */
  public boolean shouldSeek(Scan scan, long oldestUnexpiredTS) {
    return getSegment().shouldSeek(scan,oldestUnexpiredTS);
  }

  protected Segment getSegment(){
    return segment;
  }

  //debug method
  @Override
  public String toString() {
    String res = "Store segment scanner of type "+this.getClass().getName()+"; ";
    res += "Scanner order " + getScannerOrder() + "; ";
    res += getSegment().toString();
    return res;
  }

  /********************* Private Methods **********************/

  /**
   * Private internal method for iterating over the segment,
   * skipping the cells with irrelevant MVCC
   */
  private Cell getNext() {
    Cell startKV = current;
    Cell next = null;

    try {
      while (iter.hasNext()) {
        next = iter.next();
        if (next.getSequenceId() <= this.readPoint) {
          return next;                    // skip irrelevant versions
        }
        if (stopSkippingKVsIfNextRow &&   // for backwardSeek() stay in the
            startKV != null &&        // boundaries of a single row
            segment.compareRows(next, startKV) > 0) {
          return null;
        }
      } // end of while

      return null; // nothing found
    } finally {
      if (next != null) {
        // in all cases, remember the last KV we iterated to, needed for reseek()
        last = next;
      }
    }
  }

  /**
   * Private internal method that returns the higher of the two key values, or null
   * if they are both null
   */
  private Cell getHighest(Cell first, Cell second) {
    if (first == null && second == null) {
      return null;
    }
    if (first != null && second != null) {
      int compare = segment.compare(first, second);
      return (compare > 0 ? first : second);
    }
    return (first != null ? first : second);
  }

}
