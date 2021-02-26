/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;

/**
 * ReversedKeyValueHeap is used for supporting reversed scanning. Compared with
 * KeyValueHeap, its scanner comparator is a little different (see
 * ReversedKVScannerComparator), all seek is backward seek(see
 * {@link KeyValueScanner#backwardSeek}), and it will jump to the previous row
 * if it is already at the end of one row when calling next().
 */
@InterfaceAudience.Private
public class ReversedKeyValueHeap extends KeyValueHeap {

  /**
   * @param scanners
   * @param comparator
   * @throws IOException
   */
  public ReversedKeyValueHeap(List<? extends KeyValueScanner> scanners,
      CellComparator comparator) throws IOException {
    super(scanners, new ReversedKVScannerComparator(comparator));
  }

  @Override
  public boolean seek(Cell seekKey) throws IOException {
    throw new IllegalStateException(
        "seek cannot be called on ReversedKeyValueHeap");
  }

  @Override
  public boolean reseek(Cell seekKey) throws IOException {
    throw new IllegalStateException(
        "reseek cannot be called on ReversedKeyValueHeap");
  }

  @Override
  public boolean requestSeek(Cell key, boolean forward, boolean useBloom)
      throws IOException {
    throw new IllegalStateException(
        "requestSeek cannot be called on ReversedKeyValueHeap");
  }

  @Override
  public boolean seekToPreviousRow(Cell seekKey) throws IOException {
    if (current == null) {
      return false;
    }
    heap.add(current);
    current = null;

    KeyValueScanner scanner;
    while ((scanner = heap.poll()) != null) {
      Cell topKey = scanner.peek();
      if (comparator.getComparator().compareRows(topKey, seekKey) < 0) {
        // Row of Top KeyValue is before Seek row.
        heap.add(scanner);
        current = pollRealKV();
        return current != null;
      }

      if (!scanner.seekToPreviousRow(seekKey)) {
        this.scannersForDelayedClose.add(scanner);
      } else {
        heap.add(scanner);
      }
    }

    // Heap is returning empty, scanner is done
    return false;
  }

  @Override
  public boolean backwardSeek(Cell seekKey) throws IOException {
    if (current == null) {
      return false;
    }
    heap.add(current);
    current = null;

    KeyValueScanner scanner;
    while ((scanner = heap.poll()) != null) {
      Cell topKey = scanner.peek();
      if ((CellUtil.matchingRows(seekKey, topKey) && comparator
          .getComparator().compare(seekKey, topKey) <= 0)
          || comparator.getComparator().compareRows(seekKey, topKey) > 0) {
        heap.add(scanner);
        current = pollRealKV();
        return current != null;
      }
      if (!scanner.backwardSeek(seekKey)) {
        this.scannersForDelayedClose.add(scanner);
      } else {
        heap.add(scanner);
      }
    }
    return false;
  }

  @Override
  public Cell next() throws IOException {
    if (this.current == null) {
      return null;
    }
    Cell kvReturn = this.current.next();
    Cell kvNext = this.current.peek();
    if (kvNext == null
        || this.comparator.kvComparator.compareRows(kvNext, kvReturn) > 0) {
      if (this.current.seekToPreviousRow(kvReturn)) {
        this.heap.add(this.current);
      } else {
        this.scannersForDelayedClose.add(this.current);
      }
      this.current = null;
      this.current = pollRealKV();
    } else {
      KeyValueScanner topScanner = this.heap.peek();
      if (topScanner != null
          && this.comparator.compare(this.current, topScanner) > 0) {
        this.heap.add(this.current);
        this.current = null;
        this.current = pollRealKV();
      }
    }
    return kvReturn;
  }

  /**
   * In ReversedKVScannerComparator, we compare the row of scanners' peek values
   * first, sort bigger one before the smaller one. Then compare the KeyValue if
   * they have the equal row, sort smaller one before the bigger one
   */
  private static class ReversedKVScannerComparator extends
      KVScannerComparator {

    /**
     * Constructor
     * @param kvComparator
     */
    public ReversedKVScannerComparator(CellComparator kvComparator) {
      super(kvComparator);
    }

    @Override
    public int compare(KeyValueScanner left, KeyValueScanner right) {
      int rowComparison = compareRows(left.peek(), right.peek());
      if (rowComparison != 0) {
        return -rowComparison;
      }
      return super.compare(left, right);
    }

    /**
     * Compares rows of two KeyValue
     * @param left
     * @param right
     * @return less than 0 if left is smaller, 0 if equal etc..
     */
    public int compareRows(Cell left, Cell right) {
      return super.kvComparator.compareRows(left, right);
    }
  }

  @Override
  public boolean seekToLastRow() throws IOException {
    throw new NotImplementedException(HConstants.NOT_IMPLEMENTED);
  }
}
