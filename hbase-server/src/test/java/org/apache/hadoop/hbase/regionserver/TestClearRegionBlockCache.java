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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

@Category(MediumTests.class)
@RunWith(Parameterized.class)
public class TestClearRegionBlockCache {
  private static final TableName TABLE_NAME = TableName.valueOf("testClearRegionBlockCache");
  private static final byte[] FAMILY = Bytes.toBytes("family");
  private static final byte[][] SPLIT_KEY = new byte[][] { Bytes.toBytes("5") };
  private static final int NUM_MASTERS = 1;
  private static final int NUM_RS = 2;

  private final HBaseTestingUtility HTU = new HBaseTestingUtility();

  private Configuration CONF = HTU.getConfiguration();
  private Table table;
  private HRegionServer rs1, rs2;
  private MiniHBaseCluster cluster;

  @Parameterized.Parameter public String cacheType;

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Object[] data() {
    return new Object[] { "lru", "bucket" };
  }

  @Before
  public void setup() throws Exception {
    if (cacheType.equals("bucket")) {
      CONF.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
      CONF.setInt(HConstants.BUCKET_CACHE_SIZE_KEY, 30);
    }

    cluster = HTU.startMiniCluster(NUM_MASTERS, NUM_RS);
    rs1 = cluster.getRegionServer(0);
    rs2 = cluster.getRegionServer(1);

    // Create table
    table = HTU.createTable(TABLE_NAME, FAMILY, SPLIT_KEY);
  }

  @After
  public void teardown() throws Exception {
    HTU.shutdownMiniCluster();
  }

  @Test
  public void testClearBlockCache() throws Exception {
    HTU.loadNumericRows(table, FAMILY, 1, 10);
    HTU.flush(TABLE_NAME);

    BlockCache blockCache1 = rs1.getCacheConfig().getBlockCache();
    BlockCache blockCache2 = rs2.getCacheConfig().getBlockCache();

    long initialBlockCount1 = blockCache1.getBlockCount();
    long initialBlockCount2 = blockCache2.getBlockCount();

    // scan will cause blocks to be added in BlockCache
    scanAllRegionsForRS(rs1);
    assertEquals(blockCache1.getBlockCount() - initialBlockCount1,
                 HTU.getNumHFilesForRS(rs1, TABLE_NAME, FAMILY));
    clearRegionBlockCache(rs1);

    scanAllRegionsForRS(rs2);
    assertEquals(blockCache2.getBlockCount() - initialBlockCount2,
                 HTU.getNumHFilesForRS(rs2, TABLE_NAME, FAMILY));
    clearRegionBlockCache(rs2);

    assertEquals(initialBlockCount1, blockCache1.getBlockCount());
    assertEquals(initialBlockCount2, blockCache2.getBlockCount());
  }

  private void scanAllRegionsForRS(HRegionServer rs) throws IOException {
    for (Region region : rs.getRegions(TABLE_NAME)) {
      RegionScanner scanner = region.getScanner(new Scan());
      while (scanner.next(new ArrayList<Cell>()));
    }
  }

  private void clearRegionBlockCache(HRegionServer rs) {
    for (Region region : rs.getRegions(TABLE_NAME)) {
      rs.clearRegionBlockCache(region);
    }
  }
}
