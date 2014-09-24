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
import java.util.NavigableSet;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.client.Scan;

/**
 * ReversedStoreScanner extends from StoreScanner, and is used to support
 * reversed scanning.
 */
@InterfaceAudience.Private
class ReversedStoreScanner extends StoreScanner implements KeyValueScanner {

  /**
   * Opens a scanner across memstore, snapshot, and all StoreFiles. Assumes we
   * are not in a compaction.
   * 
   * @param store who we scan
   * @param scanInfo
   * @param scan the spec
   * @param columns which columns we are scanning
   * @throws IOException
   */
  ReversedStoreScanner(Store store, ScanInfo scanInfo, Scan scan,
      NavigableSet<byte[]> columns, long readPt)
      throws IOException {
    super(store, scanInfo, scan, columns, readPt);
  }

  /** Constructor for testing. */
  ReversedStoreScanner(final Scan scan, ScanInfo scanInfo, ScanType scanType,
      final NavigableSet<byte[]> columns, final List<KeyValueScanner> scanners)
      throws IOException {
    super(scan, scanInfo, scanType, columns, scanners,
        HConstants.LATEST_TIMESTAMP);
  }

  @Override
  protected void resetKVHeap(List<? extends KeyValueScanner> scanners,
      KVComparator comparator) throws IOException {
    // Combine all seeked scanners with a heap
    heap = new ReversedKeyValueHeap(scanners, comparator);
  }

  @Override
  protected void seekScanners(List<? extends KeyValueScanner> scanners,
      Cell seekKey, boolean isLazy, boolean isParallelSeek)
      throws IOException {
    // Seek all scanners to the start of the Row (or if the exact matching row
    // key does not exist, then to the start of the previous matching Row).
    if (CellUtil.matchingRow(seekKey, HConstants.EMPTY_START_ROW)) {
      for (KeyValueScanner scanner : scanners) {
        scanner.seekToLastRow();
      }
    } else {
      for (KeyValueScanner scanner : scanners) {
        scanner.backwardSeek(seekKey);
      }
    }
  }

  @Override
  protected boolean seekToNextRow(Cell kv) throws IOException {
    return seekToPreviousRow(kv);
  }

  /**
   * Do a backwardSeek in a reversed StoreScanner(scan backward)
   */
  @Override
  protected boolean seekAsDirection(Cell kv) throws IOException {
    return backwardSeek(kv);
  }

  @Override
  protected void checkScanOrder(Cell prevKV, Cell kv,
      KeyValue.KVComparator comparator) throws IOException {
    // Check that the heap gives us KVs in an increasing order for same row and
    // decreasing order for different rows.
    assert prevKV == null || comparator == null || comparator.compareRows(kv, prevKV) < 0
        || (comparator.matchingRows(kv, prevKV) && comparator.compare(kv,
            prevKV) >= 0) : "Key " + prevKV
        + " followed by a " + "error order key " + kv + " in cf " + store
        + " in reversed scan";
  }

  @Override
  public boolean reseek(Cell kv) throws IOException {
    throw new IllegalStateException(
        "reseek cannot be called on ReversedStoreScanner");
  }

  @Override
  public boolean seek(Cell key) throws IOException {
    throw new IllegalStateException(
        "seek cannot be called on ReversedStoreScanner");
  }

  @Override
  public boolean seekToPreviousRow(Cell key) throws IOException {
    lock.lock();
    try {
      checkReseek();
      return this.heap.seekToPreviousRow(key);
    } finally {
      lock.unlock();
    }

  }
  
  @Override
  public boolean backwardSeek(Cell key) throws IOException {
    lock.lock();
    try {
      checkReseek();
      return this.heap.backwardSeek(key);
    } finally {
      lock.unlock();
    }
  }
}
