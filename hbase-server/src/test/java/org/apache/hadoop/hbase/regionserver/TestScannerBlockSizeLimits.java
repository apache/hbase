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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ LargeTests.class })
public class TestScannerBlockSizeLimits {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestScannerBlockSizeLimits.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final TableName TABLE = TableName.valueOf("TestScannerBlockSizeLimits");
  private static final byte[] FAMILY1 = Bytes.toBytes("0");
  private static final byte[] FAMILY2 = Bytes.toBytes("1");

  private static final byte[] DATA = new byte[1000];
  private static final byte[][] FAMILIES = new byte[][] { FAMILY1, FAMILY2 };

  private static final byte[] COLUMN1 = Bytes.toBytes(0);
  private static final byte[] COLUMN2 = Bytes.toBytes(1);
  private static final byte[] COLUMN3 = Bytes.toBytes(2);
  private static final byte[] COLUMN5 = Bytes.toBytes(5);

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(HConstants.HBASE_SERVER_SCANNER_MAX_RESULT_SIZE_KEY, 4200);
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE, FAMILIES, 1, 2048);
    createTestData();
  }

  @Before
  public void setupEach() throws Exception {
    HRegionServer regionServer = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    for (HRegion region : regionServer.getRegions(TABLE)) {
      System.out.println("Clearing cache for region " + region.getRegionInfo().getEncodedName());
      regionServer.clearRegionBlockCache(region);
    }
  }

  private static void createTestData() throws IOException, InterruptedException {
    RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(TABLE);
    String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();
    HRegion region = TEST_UTIL.getRSForFirstRegionInTable(TABLE).getRegion(regionName);

    for (int i = 1; i < 10; i++) {
      // 5 columns per row, in 2 families
      // Each column value is 1000 bytes, which is enough to fill a full block with row and header.
      // So 5 blocks per row in FAMILY1
      Put put = new Put(Bytes.toBytes(i));
      for (int j = 0; j < 6; j++) {
        put.addColumn(FAMILY1, Bytes.toBytes(j), DATA);
      }

      // Additional block in FAMILY2 (notably smaller than block size)
      put.addColumn(FAMILY2, COLUMN1, DATA);

      region.put(put);

      if (i % 2 == 0) {
        region.flush(true);
      }
    }

    // we've created 10 storefiles at this point, 5 per family
    region.flush(true);

  }

  /**
   * Simplest test that ensures we don't count block sizes too much. These 2 requested cells are in
   * the same block, so should be returned in 1 request. If we mis-counted blocks, it'd come in 2
   * requests.
   */
  @Test
  public void testSingleBlock() throws IOException {
    Table table = TEST_UTIL.getConnection().getTable(TABLE);

    ResultScanner scanner =
      table.getScanner(getBaseScan().withStartRow(Bytes.toBytes(1)).withStopRow(Bytes.toBytes(2))
        .addColumn(FAMILY1, COLUMN1).addColumn(FAMILY1, COLUMN2).setReadType(Scan.ReadType.STREAM));

    ScanMetrics metrics = scanner.getScanMetrics();

    scanner.next(100);

    // we fetch 2 columns from 1 row, so about 2 blocks
    assertEquals(1, metrics.countOfRowsScanned.get());
    assertEquals(1, metrics.countOfRPCcalls.get());
  }

  /**
   * We enable cursors and partial results to give us more granularity over counting of results, and
   * we enable STREAM so that no auto switching from pread to stream occurs -- this throws off the
   * rpc counts.
   */
  private Scan getBaseScan() {
    return new Scan().setScanMetricsEnabled(true).setNeedCursorResult(true)
      .setAllowPartialResults(true).setReadType(Scan.ReadType.STREAM);
  }
}
