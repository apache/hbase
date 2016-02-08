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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;

/**
 * An abstraction for store segment scanner.
 */
@InterfaceAudience.Private
public abstract class SegmentScanner implements KeyValueScanner {

  private long sequenceID = Long.MAX_VALUE;

  protected abstract Segment getSegment();

  /**
   * Get the sequence id associated with this KeyValueScanner. This is required
   * for comparing multiple files (or memstore segments) scanners to find out
   * which one has the latest data.
   *
   */
  @Override
  public long getSequenceID() {
    return sequenceID;
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
   * Set the sequence id of the scanner.
   * This is used to determine an order between memory segment scanners.
   * @param x a unique sequence id
   */
  public void setSequenceID(long x) {
    sequenceID = x;
  }

  /**
   * Returns whether the given scan should seek in this segment
   * @return whether the given scan should seek in this segment
   */
  public boolean shouldSeek(Scan scan, long oldestUnexpiredTS) {
    return getSegment().shouldSeek(scan,oldestUnexpiredTS);
  }

  //debug method
  @Override
  public String toString() {
    String res = "Store segment scanner of type "+this.getClass().getName()+"; ";
    res += "sequence id "+getSequenceID()+"; ";
    res += getSegment().toString();
    return res;
  }

}
