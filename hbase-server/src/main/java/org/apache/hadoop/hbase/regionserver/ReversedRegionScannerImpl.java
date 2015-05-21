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

import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion.RegionScannerImpl;

/**
 * ReversibleRegionScannerImpl extends from RegionScannerImpl, and is used to
 * support reversed scanning.
 */
@InterfaceAudience.Private
class ReversedRegionScannerImpl extends RegionScannerImpl {

  /**
   * @param scan
   * @param additionalScanners
   * @param region
   * @throws IOException
   */
  ReversedRegionScannerImpl(Scan scan,
      List<KeyValueScanner> additionalScanners, HRegion region)
      throws IOException {
    region.super(scan, additionalScanners, region);
  }

  @Override
  protected void initializeKVHeap(List<KeyValueScanner> scanners,
      List<KeyValueScanner> joinedScanners, HRegion region) throws IOException {
    this.storeHeap = new ReversedKeyValueHeap(scanners, region.getComparator());
    if (!joinedScanners.isEmpty()) {
      this.joinedHeap = new ReversedKeyValueHeap(joinedScanners,
          region.getComparator());
    }
  }

  @Override
  protected boolean isStopRow(byte[] currentRow, int offset, short length) {
    return currentRow == null
        || (super.stopRow != null && region.getComparator().compareRows(
            stopRow, 0, stopRow.length, currentRow, offset, length) >= super.isScan);
  }

  @Override
  protected boolean nextRow(ScannerContext scannerContext, byte[] currentRow, int offset,
      short length) throws IOException {
    assert super.joinedContinuationRow == null : "Trying to go to next row during joinedHeap read.";
    byte row[] = new byte[length];
    System.arraycopy(currentRow, offset, row, 0, length);
    this.storeHeap.seekToPreviousRow(KeyValueUtil.createFirstOnRow(row));
    resetFilters();

    // Calling the hook in CP which allows it to do a fast forward
    if (this.region.getCoprocessorHost() != null) {
      return this.region.getCoprocessorHost().postScannerFilterRow(this,
          currentRow, offset, length);
    }
    return true;
  }

}
