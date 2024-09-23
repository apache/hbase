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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
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

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
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
    assertEquals(4120, metrics.countOfBlockBytesScanned.get());
    assertEquals(1, metrics.countOfRowsScanned.get());
    assertEquals(1, metrics.getCountOfRPCcalls().get());
  }

  /**
   * Tests that we check size limit after filterRowKey. When filterRowKey, we call nextRow to skip
   * to next row. This should be efficient in this case, but we still need to check size limits
   * after each row is processed. So in this test, we accumulate some block IO reading row 1, then
   * skip row 2 and should return early at that point. The next rpc call starts with row3 blocks
   * loaded, so can return the whole row in one rpc. If we were not checking size limits, we'd have
   * been able to load an extra row 3 cell into the first rpc and thus split row 3 across multiple
   * Results.
   */
  @Test
  public void testCheckLimitAfterFilterRowKey() throws IOException {

    Table table = TEST_UTIL.getConnection().getTable(TABLE);

    ResultScanner scanner = table.getScanner(getBaseScan().addColumn(FAMILY1, COLUMN1)
      .addColumn(FAMILY1, COLUMN2).addColumn(FAMILY1, COLUMN3).addFamily(FAMILY2)
      .setFilter(new RowFilter(CompareOperator.NOT_EQUAL, new BinaryComparator(Bytes.toBytes(2)))));

    ScanMetrics metrics = scanner.getScanMetrics();

    boolean foundRow3 = false;
    for (Result result : scanner) {
      Set<Integer> rows = new HashSet<>();
      for (Cell cell : result.rawCells()) {
        rows.add(Bytes.toInt(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
      }
      if (rows.contains(3)) {
        assertFalse("expected row3 to come all in one result, but found it in two results",
          foundRow3);
        assertEquals(1, rows.size());
        foundRow3 = true;
      }
    }

    // 22 blocks, last one is 1030 bytes (approx 3 per row for 8 rows, but some compaction happens
    // in family2 since each row only has 1 cell there and 2 can fit per block)
    assertEquals(44290, metrics.countOfBlockBytesScanned.get());
    // We can return 22 blocks in 9 RPCs, but need an extra one to check for more rows at end
    assertEquals(10, metrics.getCountOfRPCcalls().get());
  }

  /**
   * After RegionScannerImpl.populateResults, row filters are run. If row is excluded due to
   * filter.filterRow(), nextRow() is called which might accumulate more block IO. Validates that in
   * this case we still honor block limits.
   */
  @Test
  public void testCheckLimitAfterFilteringRowCellsDueToFilterRow() throws IOException {
    Table table = TEST_UTIL.getConnection().getTable(TABLE);

    ResultScanner scanner = table.getScanner(getBaseScan().withStartRow(Bytes.toBytes(1), true)
      .addColumn(FAMILY1, COLUMN1).addColumn(FAMILY1, COLUMN2).setReadType(Scan.ReadType.STREAM)
      .setFilter(new SkipFilter(new QualifierFilter(CompareOperator.EQUAL,
        new BinaryComparator(Bytes.toBytes("dasfasf"))))));

    // Our filter doesn't end up matching any real columns, so expect only cursors
    for (Result result : scanner) {
      assertTrue(result.isCursor());
    }

    ScanMetrics metrics = scanner.getScanMetrics();

    // scanning over 9 rows, filtering on 2 contiguous columns each, so 9 blocks total
    assertEquals(18540, metrics.countOfBlockBytesScanned.get());
    // limited to 4200 bytes per which is enough for 3 blocks (exceed limit after loading 3rd)
    // so that's 3 RPC and the last RPC pulls the cells loaded by the last block
    assertEquals(4, metrics.getCountOfRPCcalls().get());
  }

  /**
   * At the end of the loop in StoreScanner, we do one more check of size limits. This is to catch
   * block size being exceeded while filtering cells within a store. Test to ensure that we do that,
   * otherwise we'd see no cursors below.
   */
  @Test
  public void testCheckLimitAfterFilteringCell() throws IOException {
    Table table = TEST_UTIL.getConnection().getTable(TABLE);

    ResultScanner scanner = table.getScanner(getBaseScan()
      .setFilter(new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator(COLUMN2))));

    int cursors = 0;
    for (Result result : scanner) {
      if (result.isCursor()) {
        cursors++;
      }
    }
    ScanMetrics metrics = scanner.getScanMetrics();
    System.out.println(metrics.countOfBlockBytesScanned.get());

    // 9 rows, total of 32 blocks (last one is 1030)
    assertEquals(64890, metrics.countOfBlockBytesScanned.get());
    // We can return 32 blocks in approx 11 RPCs but we need 2 cursors due to the narrow filter
    assertEquals(2, cursors);
    assertEquals(11, metrics.getCountOfRPCcalls().get());
  }

  /**
   * After RegionScannerImpl.populateResults, row filters are run. If row is excluded due to
   * filter.filterRowCells(), we fall through to a final results.isEmpty() check near the end of the
   * method. If results are empty at this point (which they are), nextRow() is called which might
   * accumulate more block IO. Validates that in this case we still honor block limits.
   */
  @Test
  public void testCheckLimitAfterFilteringRowCells() throws IOException {
    Table table = TEST_UTIL.getConnection().getTable(TABLE);

    ResultScanner scanner = table
      .getScanner(getBaseScan().withStartRow(Bytes.toBytes(1), true).addColumn(FAMILY1, COLUMN1)
        .setReadType(Scan.ReadType.STREAM).setFilter(new SingleColumnValueExcludeFilter(FAMILY1,
          COLUMN1, CompareOperator.EQUAL, new BinaryComparator(DATA))));

    // Since we use SingleColumnValueExcludeFilter and dont include any other columns, the column
    // we load to test ends up being excluded from the result. So we only expect cursors here.
    for (Result result : scanner) {
      assertTrue(result.isCursor());
    }

    ScanMetrics metrics = scanner.getScanMetrics();

    // Our filter causes us to read the first column of each row, then INCLUDE_AND_SEEK_NEXT_ROW.
    // So we load 1 block per row, and there are 9 rows. So 9 blocks
    assertEquals(18540, metrics.countOfBlockBytesScanned.get());
    // We can return 9 blocks in 3 RPCs, but need 1 more to check for more results (returns 0)
    assertEquals(4, metrics.getCountOfRPCcalls().get());
  }

  /**
   * Tests that when we seek over blocks we dont include them in the block size of the request
   */
  @Test
  public void testSeekNextUsingHint() throws IOException {
    Table table = TEST_UTIL.getConnection().getTable(TABLE);

    ResultScanner scanner = table.getScanner(
      getBaseScan().addFamily(FAMILY1).setFilter(new ColumnPaginationFilter(1, COLUMN5)));

    scanner.next(100);
    ScanMetrics metrics = scanner.getScanMetrics();

    // We have to read the first cell/block of each row, then can skip to the last block. So that's
    // 2 blocks per row to read (18 blocks total)
    assertEquals(37080, metrics.countOfBlockBytesScanned.get());
    // Our max scan size is enough to read 3 blocks per RPC, plus one final RPC to finish region.
    assertEquals(7, metrics.getCountOfRPCcalls().get());
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
