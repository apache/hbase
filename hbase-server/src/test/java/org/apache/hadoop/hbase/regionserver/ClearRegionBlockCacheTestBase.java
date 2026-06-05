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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class ClearRegionBlockCacheTestBase {

  private static final byte[] FAMILY = Bytes.toBytes("family");
  private static final byte[][] SPLIT_KEY = new byte[][] { Bytes.toBytes("5") };
  private static final int NUM_RS = 2;

  private static TableName tableName;
  private static HBaseTestingUtility htu;
  private static HRegionServer rs1;
  private static HRegionServer rs2;

  protected static void setUpCluster(TableName testTableName) throws Exception {
    setUpCluster(testTableName, false);
  }

  protected static void setUpBucketCacheCluster(TableName testTableName) throws Exception {
    setUpCluster(testTableName, true);
  }

  private static void setUpCluster(TableName testTableName, boolean bucketCache) throws Exception {
    tableName = testTableName;
    htu = new HBaseTestingUtility();
    if (bucketCache) {
      htu.getConfiguration().set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
      htu.getConfiguration().setInt(HConstants.BUCKET_CACHE_SIZE_KEY, 30);
    }
    htu.startMiniCluster(NUM_RS);
    rs1 = htu.getMiniHBaseCluster().getRegionServer(0);
    rs2 = htu.getMiniHBaseCluster().getRegionServer(1);

    try (Table table = htu.createTable(testTableName, FAMILY, SPLIT_KEY)) {
      htu.loadNumericRows(table, FAMILY, 1, 10);
      htu.flush(testTableName);
    }
  }

  protected static void tearDownCluster() throws Exception {
    if (htu != null) {
      htu.shutdownMiniCluster();
    }
  }

  @BeforeEach
  void clearBlockCacheBeforeTest() {
    clearRegionBlockCache(rs1);
    clearRegionBlockCache(rs2);
  }

  @Test
  void testClearBlockCache() throws Exception {
    BlockCache blockCache1 = rs1.getBlockCache().get();
    BlockCache blockCache2 = rs2.getBlockCache().get();

    long initialBlockCount1 = blockCache1.getBlockCount();
    long initialBlockCount2 = blockCache2.getBlockCount();

    // scan will cause blocks to be added in BlockCache
    scanAllRegionsForRS(rs1);
    assertEquals(blockCache1.getBlockCount() - initialBlockCount1,
      htu.getNumHFilesForRS(rs1, tableName, FAMILY));
    clearRegionBlockCache(rs1);

    scanAllRegionsForRS(rs2);
    assertEquals(blockCache2.getBlockCount() - initialBlockCount2,
      htu.getNumHFilesForRS(rs2, tableName, FAMILY));
    clearRegionBlockCache(rs2);

    assertEquals(initialBlockCount1, blockCache1.getBlockCount(), "" + blockCache1.getBlockCount());
    assertEquals(initialBlockCount2, blockCache2.getBlockCount(), "" + blockCache2.getBlockCount());
  }

  @Test
  void testClearBlockCacheFromAdmin() throws Exception {
    Admin admin = htu.getAdmin();

    BlockCache blockCache1 = rs1.getBlockCache().get();
    BlockCache blockCache2 = rs2.getBlockCache().get();
    long initialBlockCount1 = blockCache1.getBlockCount();
    long initialBlockCount2 = blockCache2.getBlockCount();

    // scan will cause blocks to be added in BlockCache
    scanAllRegionsForRS(rs1);
    assertEquals(blockCache1.getBlockCount() - initialBlockCount1,
      htu.getNumHFilesForRS(rs1, tableName, FAMILY));
    scanAllRegionsForRS(rs2);
    assertEquals(blockCache2.getBlockCount() - initialBlockCount2,
      htu.getNumHFilesForRS(rs2, tableName, FAMILY));

    CacheEvictionStats stats = admin.clearBlockCache(tableName);
    assertEquals(
      htu.getNumHFilesForRS(rs1, tableName, FAMILY) + htu.getNumHFilesForRS(rs2, tableName, FAMILY),
      stats.getEvictedBlocks());
    assertEquals(initialBlockCount1, blockCache1.getBlockCount());
    assertEquals(initialBlockCount2, blockCache2.getBlockCount());
  }

  @Test
  void testClearBlockCacheFromAsyncAdmin() throws Exception {
    try (AsyncConnection conn =
      ConnectionFactory.createAsyncConnection(htu.getConfiguration()).get()) {
      AsyncAdmin admin = conn.getAdmin();

      BlockCache blockCache1 = rs1.getBlockCache().get();
      BlockCache blockCache2 = rs2.getBlockCache().get();
      long initialBlockCount1 = blockCache1.getBlockCount();
      long initialBlockCount2 = blockCache2.getBlockCount();

      // scan will cause blocks to be added in BlockCache
      scanAllRegionsForRS(rs1);
      assertEquals(blockCache1.getBlockCount() - initialBlockCount1,
        htu.getNumHFilesForRS(rs1, tableName, FAMILY));
      scanAllRegionsForRS(rs2);
      assertEquals(blockCache2.getBlockCount() - initialBlockCount2,
        htu.getNumHFilesForRS(rs2, tableName, FAMILY));

      CacheEvictionStats stats = admin.clearBlockCache(tableName).get();
      assertEquals(htu.getNumHFilesForRS(rs1, tableName, FAMILY)
        + htu.getNumHFilesForRS(rs2, tableName, FAMILY), stats.getEvictedBlocks());
      assertEquals(initialBlockCount1, blockCache1.getBlockCount());
      assertEquals(initialBlockCount2, blockCache2.getBlockCount());
    }
  }

  private void scanAllRegionsForRS(HRegionServer rs) throws IOException {
    for (Region region : rs.getRegions(tableName)) {
      try (RegionScanner scanner = region.getScanner(new Scan())) {
        List<Cell> cells = new ArrayList<>();
        while (scanner.next(cells)) {
          cells.clear();
        }
      }
    }
  }

  private void clearRegionBlockCache(HRegionServer rs) {
    for (Region region : rs.getRegions(tableName)) {
      rs.clearRegionBlockCache(region);
    }
  }
}
