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
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mob.MobCell;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ReversedMobStoreScanner extends from ReversedStoreScanner, and is used to support reversed
 * scanning in both the memstore and the MOB store.
 */
@InterfaceAudience.Private
public class ReversedMobStoreScanner extends ReversedStoreScanner {

  private static final Logger LOG = LoggerFactory.getLogger(ReversedMobStoreScanner.class);
  private boolean cacheMobBlocks = false;
  private boolean rawMobScan = false;
  private boolean readEmptyValueOnMobCellMiss = false;
  private final HMobStore mobStore;
  private final List<MobCell> referencedMobCells;

  ReversedMobStoreScanner(HStore store, ScanInfo scanInfo, Scan scan, NavigableSet<byte[]> columns,
      long readPt) throws IOException {
    super(store, scanInfo, scan, columns, readPt);
    cacheMobBlocks = MobUtils.isCacheMobBlocks(scan);
    rawMobScan = MobUtils.isRawMobScan(scan);
    readEmptyValueOnMobCellMiss = MobUtils.isReadEmptyValueOnMobCellMiss(scan);
    if (!(store instanceof HMobStore)) {
      throw new IllegalArgumentException("The store " + store + " is not a HMobStore");
    }
    mobStore = (HMobStore) store;
    this.referencedMobCells = new ArrayList<>();
  }

  /**
   * Firstly reads the cells from the HBase. If the cell is a reference cell (which has the
   * reference tag), the scanner need seek this cell from the mob file, and use the cell found
   * from the mob file as the result.
   */
  @Override
  public boolean next(List<Cell> outResult, ScannerContext ctx) throws IOException {
    boolean result = super.next(outResult, ctx);
    if (!rawMobScan) {
      // retrieve the mob data
      if (outResult.isEmpty()) {
        return result;
      }
      long mobKVCount = 0;
      long mobKVSize = 0;
      for (int i = 0; i < outResult.size(); i++) {
        Cell cell = outResult.get(i);
        if (MobUtils.isMobReferenceCell(cell)) {
          MobCell mobCell =
              mobStore.resolve(cell, cacheMobBlocks, readPt, readEmptyValueOnMobCellMiss);
          mobKVCount++;
          mobKVSize += mobCell.getCell().getValueLength();
          outResult.set(i, mobCell.getCell());
          // Keep the MobCell here unless we shipped the RPC or close the scanner.
          referencedMobCells.add(mobCell);
        }
      }
      mobStore.updateMobScanCellsCount(mobKVCount);
      mobStore.updateMobScanCellsSize(mobKVSize);
    }
    return result;
  }

  private void freeAllReferencedMobCells() throws IOException {
    for (MobCell mobCell : referencedMobCells) {
      mobCell.close();
    }
    referencedMobCells.clear();
  }

  @Override
  public void shipped() throws IOException {
    super.shipped();
    this.freeAllReferencedMobCells();
  }

  @Override
  public void close() {
    super.close();
    try {
      this.freeAllReferencedMobCells();
    } catch (IOException e) {
      LOG.warn("Failed to free referenced mob cells: ", e);
    }
  }
}
