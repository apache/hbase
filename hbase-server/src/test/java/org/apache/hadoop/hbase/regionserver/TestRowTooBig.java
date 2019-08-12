/**
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowTooBigException;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test case to check HRS throws {@link org.apache.hadoop.hbase.client.RowTooBigException}
 * when row size exceeds configured limits.
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestRowTooBig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRowTooBig.class);

  private final static HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static Path rootRegionDir;
  private static final HTableDescriptor TEST_HTD =
    new HTableDescriptor(TableName.valueOf(TestRowTooBig.class.getSimpleName()));

  @BeforeClass
  public static void before() throws Exception {
    HTU.startMiniCluster();
    HTU.getConfiguration().setLong(HConstants.TABLE_MAX_ROWSIZE_KEY,
      10 * 1024 * 1024L);
    rootRegionDir = HTU.getDataTestDirOnTestFS("TestRowTooBig");
  }

  @AfterClass
  public static void after() throws Exception {
    HTU.shutdownMiniCluster();
  }

  /**
   * Usecase:
   *  - create a row with 5 large  cells (5 Mb each)
   *  - flush memstore but don't compact storefiles.
   *  - try to Get whole row.
   *
   * OOME happened before we actually get to reading results, but
   * during seeking, as each StoreFile gets it's own scanner,
   * and each scanner seeks after the first KV.
   * @throws IOException
   */
  @Test(expected = RowTooBigException.class)
  public void testScannersSeekOnFewLargeCells() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");

    HTableDescriptor htd = TEST_HTD;
    HColumnDescriptor hcd = new HColumnDescriptor(fam1);
    if (htd.hasFamily(hcd.getName())) {
      htd.modifyFamily(hcd);
    } else {
      htd.addFamily(hcd);
    }

    final HRegionInfo hri =
      new HRegionInfo(htd.getTableName(), HConstants.EMPTY_END_ROW,
        HConstants.EMPTY_END_ROW);
    HRegion region =
        HBaseTestingUtility.createRegionAndWAL(hri, rootRegionDir, HTU.getConfiguration(), htd);
    try {
      // Add 5 cells to memstore
      for (int i = 0; i < 5 ; i++) {
        Put put = new Put(row1);

        byte[] value = new byte[5 * 1024 * 1024];
        put.addColumn(fam1, Bytes.toBytes("col_" + i), value);
        region.put(put);
        region.flush(true);
      }

      Get get = new Get(row1);
      region.get(get);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(region);
    }
  }

  /**
   * Usecase:
   *
   *  - create a row with 1M cells, 10 bytes in each
   *  - flush & run major compaction
   *  - try to Get whole row.
   *
   *  OOME happened in StoreScanner.next(..).
   *
   * @throws IOException
   */
  @Test(expected = RowTooBigException.class)
  public void testScanAcrossManySmallColumns() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");

    HTableDescriptor htd = TEST_HTD;
    HColumnDescriptor hcd = new HColumnDescriptor(fam1);
    if (htd.hasFamily(hcd.getName())) {
      htd.modifyFamily(hcd);
    } else {
      htd.addFamily(hcd);
    }

    final HRegionInfo hri =
      new HRegionInfo(htd.getTableName(), HConstants.EMPTY_END_ROW,
        HConstants.EMPTY_END_ROW);
    HRegion region =
        HBaseTestingUtility.createRegionAndWAL(hri, rootRegionDir, HTU.getConfiguration(), htd);
    try {
      // Add to memstore
      for (int i = 0; i < 10; i++) {
        Put put = new Put(row1);
        for (int j = 0; j < 10 * 10000; j++) {
          byte[] value = new byte[10];
          put.addColumn(fam1, Bytes.toBytes("col_" + i + "_" + j), value);
        }
        region.put(put);
        region.flush(true);
      }
      region.compact(true);

      Get get = new Get(row1);
      region.get(get);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(region);
    }
  }
}
