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

import static org.apache.hadoop.hbase.CellUtil.createCell;
import static org.apache.hadoop.hbase.KeyValueTestUtil.create;
import static org.apache.hadoop.hbase.regionserver.KeyValueScanFixture.scanFixture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.OptionalInt;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Can't be small as it plays with EnvironmentEdgeManager
@Category({RegionServerTests.class, MediumTests.class})
public class TestStoreScanner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestStoreScanner.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestStoreScanner.class);
  @Rule public TestName name = new TestName();
  private static final String CF_STR = "cf";
  private static final byte[] CF = Bytes.toBytes(CF_STR);
  static Configuration CONF = HBaseConfiguration.create();
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private ScanInfo scanInfo = new ScanInfo(CONF, CF, 0, Integer.MAX_VALUE, Long.MAX_VALUE,
      KeepDeletedCells.FALSE, HConstants.DEFAULT_BLOCKSIZE, 0, CellComparator.getInstance(), false);

  /**
   * From here on down, we have a bunch of defines and specific CELL_GRID of Cells. The
   * CELL_GRID then has a Scanner that can fake out 'block' transitions. All this elaborate
   * setup is for tests that ensure we don't overread, and that the {@link StoreScanner} is not
   * overly enthusiastic.
   */
  private static final byte[] ZERO = new byte[] {'0'};
  private static final byte[] ZERO_POINT_ZERO = new byte[] {'0', '.', '0'};
  private static final byte[] ONE = new byte[] {'1'};
  private static final byte[] TWO = new byte[] {'2'};
  private static final byte[] TWO_POINT_TWO = new byte[] {'2', '.', '2'};
  private static final byte[] THREE = new byte[] {'3'};
  private static final byte[] FOUR = new byte[] {'4'};
  private static final byte[] FIVE = new byte[] {'5'};
  private static final byte[] VALUE = new byte[] {'v'};
  private static final int CELL_GRID_BLOCK2_BOUNDARY = 4;
  private static final int CELL_GRID_BLOCK3_BOUNDARY = 11;
  private static final int CELL_GRID_BLOCK4_BOUNDARY = 15;
  private static final int CELL_GRID_BLOCK5_BOUNDARY = 19;

  /**
   * Five rows by four columns distinguished by column qualifier (column qualifier is one of the
   * four rows... ONE, TWO, etc.). Exceptions are a weird row after TWO; it is TWO_POINT_TWO.
   * And then row FOUR has five columns finishing w/ row FIVE having a single column.
   * We will use this to test scan does the right thing as it
   * we do Gets, StoreScanner#optimize, and what we do on (faked) block boundaries.
   */
  private static final Cell[] CELL_GRID = new Cell [] {
    createCell(ONE, CF, ONE, 1L, KeyValue.Type.Put.getCode(), VALUE),
    createCell(ONE, CF, TWO, 1L, KeyValue.Type.Put.getCode(), VALUE),
    createCell(ONE, CF, THREE, 1L, KeyValue.Type.Put.getCode(), VALUE),
    createCell(ONE, CF, FOUR, 1L, KeyValue.Type.Put.getCode(), VALUE),
    // Offset 4 CELL_GRID_BLOCK2_BOUNDARY
    createCell(TWO, CF, ONE, 1L, KeyValue.Type.Put.getCode(), VALUE),
    createCell(TWO, CF, TWO, 1L, KeyValue.Type.Put.getCode(), VALUE),
    createCell(TWO, CF, THREE, 1L, KeyValue.Type.Put.getCode(), VALUE),
    createCell(TWO, CF, FOUR, 1L, KeyValue.Type.Put.getCode(), VALUE),
    createCell(TWO_POINT_TWO, CF, ZERO, 1L, KeyValue.Type.Put.getCode(), VALUE),
    createCell(TWO_POINT_TWO, CF, ZERO_POINT_ZERO, 1L, KeyValue.Type.Put.getCode(), VALUE),
    createCell(TWO_POINT_TWO, CF, FIVE, 1L, KeyValue.Type.Put.getCode(), VALUE),
    // Offset 11! CELL_GRID_BLOCK3_BOUNDARY
    createCell(THREE, CF, ONE, 1L, KeyValue.Type.Put.getCode(), VALUE),
    createCell(THREE, CF, TWO, 1L, KeyValue.Type.Put.getCode(), VALUE),
    createCell(THREE, CF, THREE, 1L, KeyValue.Type.Put.getCode(), VALUE),
    createCell(THREE, CF, FOUR, 1L, KeyValue.Type.Put.getCode(), VALUE),
    // Offset 15 CELL_GRID_BLOCK4_BOUNDARY
    createCell(FOUR, CF, ONE, 1L, KeyValue.Type.Put.getCode(), VALUE),
    createCell(FOUR, CF, TWO, 1L, KeyValue.Type.Put.getCode(), VALUE),
    createCell(FOUR, CF, THREE, 1L, KeyValue.Type.Put.getCode(), VALUE),
    createCell(FOUR, CF, FOUR, 1L, KeyValue.Type.Put.getCode(), VALUE),
    // Offset 19 CELL_GRID_BLOCK5_BOUNDARY
    createCell(FOUR, CF, FIVE, 1L, KeyValue.Type.Put.getCode(), VALUE),
    createCell(FIVE, CF, ZERO, 1L, KeyValue.Type.Put.getCode(), VALUE),
  };

  private static class KeyValueHeapWithCount extends KeyValueHeap {

    final AtomicInteger count;

    public KeyValueHeapWithCount(List<? extends KeyValueScanner> scanners,
        CellComparator comparator, AtomicInteger count) throws IOException {
      super(scanners, comparator);
      this.count = count;
    }

    @Override
    public Cell peek() {
      this.count.incrementAndGet();
      return super.peek();
    }
  }

  /**
   * A StoreScanner for our CELL_GRID above. Fakes the block transitions. Does counts of
   * calls to optimize and counts of when optimize actually did an optimize.
   */
  private static class CellGridStoreScanner extends StoreScanner {
    // Count of how often optimize is called and of how often it does an optimize.
    AtomicInteger count;
    final AtomicInteger optimization = new AtomicInteger(0);

    CellGridStoreScanner(final Scan scan, ScanInfo scanInfo) throws IOException {
      super(scan, scanInfo, scan.getFamilyMap().get(CF), Arrays.<KeyValueScanner> asList(
        new KeyValueScanner[] { new KeyValueScanFixture(CellComparator.getInstance(), CELL_GRID) }));
    }

    @Override
    protected void resetKVHeap(List<? extends KeyValueScanner> scanners,
        CellComparator comparator) throws IOException {
      if (count == null) {
        count = new AtomicInteger(0);
      }
      heap = newKVHeap(scanners, comparator);
    }

    @Override
    protected KeyValueHeap newKVHeap(List<? extends KeyValueScanner> scanners,
        CellComparator comparator) throws IOException {
      return new KeyValueHeapWithCount(scanners, comparator, count);
    }

    @Override
    protected boolean trySkipToNextRow(Cell cell) throws IOException {
      boolean optimized = super.trySkipToNextRow(cell);
      LOG.info("Cell=" + cell + ", nextIndex=" + CellUtil.toString(getNextIndexedKey(), false)
          + ", optimized=" + optimized);
      if (optimized) {
        optimization.incrementAndGet();
      }
      return optimized;
    }

    @Override
    protected boolean trySkipToNextColumn(Cell cell) throws IOException {
      boolean optimized = super.trySkipToNextColumn(cell);
      LOG.info("Cell=" + cell + ", nextIndex=" + CellUtil.toString(getNextIndexedKey(), false)
          + ", optimized=" + optimized);
      if (optimized) {
        optimization.incrementAndGet();
      }
      return optimized;
    }

    @Override
    public Cell getNextIndexedKey() {
      // Fake block boundaries by having index of next block change as we go through scan.
      return count.get() > CELL_GRID_BLOCK4_BOUNDARY?
          PrivateCellUtil.createFirstOnRow(CELL_GRID[CELL_GRID_BLOCK5_BOUNDARY]):
            count.get() > CELL_GRID_BLOCK3_BOUNDARY?
                PrivateCellUtil.createFirstOnRow(CELL_GRID[CELL_GRID_BLOCK4_BOUNDARY]):
                  count.get() > CELL_GRID_BLOCK2_BOUNDARY?
                      PrivateCellUtil.createFirstOnRow(CELL_GRID[CELL_GRID_BLOCK3_BOUNDARY]):
                        PrivateCellUtil.createFirstOnRow(CELL_GRID[CELL_GRID_BLOCK2_BOUNDARY]);
    }
  };

  private static final int CELL_WITH_VERSIONS_BLOCK2_BOUNDARY = 4;

  private static final Cell[] CELL_WITH_VERSIONS = new Cell [] {
    createCell(ONE, CF, ONE, 2L, KeyValue.Type.Put.getCode(), VALUE),
    createCell(ONE, CF, ONE, 1L, KeyValue.Type.Put.getCode(), VALUE),
    createCell(ONE, CF, TWO, 2L, KeyValue.Type.Put.getCode(), VALUE),
    createCell(ONE, CF, TWO, 1L, KeyValue.Type.Put.getCode(), VALUE),
    // Offset 4 CELL_WITH_VERSIONS_BLOCK2_BOUNDARY
    createCell(TWO, CF, ONE, 1L, KeyValue.Type.Put.getCode(), VALUE),
    createCell(TWO, CF, TWO, 1L, KeyValue.Type.Put.getCode(), VALUE),
  };

  private static class CellWithVersionsStoreScanner extends StoreScanner {
    // Count of how often optimize is called and of how often it does an optimize.
    final AtomicInteger optimization = new AtomicInteger(0);

    CellWithVersionsStoreScanner(final Scan scan, ScanInfo scanInfo) throws IOException {
      super(scan, scanInfo, scan.getFamilyMap().get(CF),
          Arrays.<KeyValueScanner> asList(new KeyValueScanner[] {
              new KeyValueScanFixture(CellComparator.getInstance(), CELL_WITH_VERSIONS) }));
    }

    @Override
    protected boolean trySkipToNextColumn(Cell cell) throws IOException {
      boolean optimized = super.trySkipToNextColumn(cell);
      LOG.info("Cell=" + cell + ", nextIndex=" + CellUtil.toString(getNextIndexedKey(), false)
          + ", optimized=" + optimized);
      if (optimized) {
        optimization.incrementAndGet();
      }
      return optimized;
    }

    @Override
    public Cell getNextIndexedKey() {
      // Fake block boundaries by having index of next block change as we go through scan.
      return PrivateCellUtil
          .createFirstOnRow(CELL_WITH_VERSIONS[CELL_WITH_VERSIONS_BLOCK2_BOUNDARY]);
    }
  };

  private static class CellWithVersionsNoOptimizeStoreScanner extends StoreScanner {
    // Count of how often optimize is called and of how often it does an optimize.
    final AtomicInteger optimization = new AtomicInteger(0);

    CellWithVersionsNoOptimizeStoreScanner(Scan scan, ScanInfo scanInfo) throws IOException {
      super(scan, scanInfo, scan.getFamilyMap().get(CF),
          Arrays.<KeyValueScanner> asList(new KeyValueScanner[] {
              new KeyValueScanFixture(CellComparator.getInstance(), CELL_WITH_VERSIONS) }));
    }

    @Override
    protected boolean trySkipToNextColumn(Cell cell) throws IOException {
      boolean optimized = super.trySkipToNextColumn(cell);
      LOG.info("Cell=" + cell + ", nextIndex=" + CellUtil.toString(getNextIndexedKey(), false)
          + ", optimized=" + optimized);
      if (optimized) {
        optimization.incrementAndGet();
      }
      return optimized;
    }

    @Override
    public Cell getNextIndexedKey() {
      return null;
    }
  };

  @Test
  public void testWithColumnCountGetFilter() throws Exception {
    Get get = new Get(ONE);
    get.readAllVersions();
    get.addFamily(CF);
    get.setFilter(new ColumnCountGetFilter(2));

    try (CellWithVersionsNoOptimizeStoreScanner scannerNoOptimize =
        new CellWithVersionsNoOptimizeStoreScanner(new Scan(get), this.scanInfo)) {
      List<Cell> results = new ArrayList<>();
      while (scannerNoOptimize.next(results)) {
        continue;
      }
      assertEquals(2, results.size());
      assertTrue(CellUtil.matchingColumn(results.get(0), CELL_WITH_VERSIONS[0]));
      assertTrue(CellUtil.matchingColumn(results.get(1), CELL_WITH_VERSIONS[2]));
      assertTrue("Optimize should do some optimizations",
        scannerNoOptimize.optimization.get() == 0);
    }

    get.setFilter(new ColumnCountGetFilter(2));
    try (CellWithVersionsStoreScanner scanner =
        new CellWithVersionsStoreScanner(new Scan(get), this.scanInfo)) {
      List<Cell> results = new ArrayList<>();
      while (scanner.next(results)) {
        continue;
      }
      assertEquals(2, results.size());
      assertTrue(CellUtil.matchingColumn(results.get(0), CELL_WITH_VERSIONS[0]));
      assertTrue(CellUtil.matchingColumn(results.get(1), CELL_WITH_VERSIONS[2]));
      assertTrue("Optimize should do some optimizations", scanner.optimization.get() > 0);
    }
  }

  /*
   * Test utility for building a NavigableSet for scanners.
   * @param strCols
   * @return
   */
  NavigableSet<byte[]> getCols(String ...strCols) {
    NavigableSet<byte[]> cols = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    for (String col : strCols) {
      byte[] bytes = Bytes.toBytes(col);
      cols.add(bytes);
    }
    return cols;
  }

  @Test
  public void testFullRowGetDoesNotOverreadWhenRowInsideOneBlock() throws IOException {
    // Do a Get against row two. Row two is inside a block that starts with row TWO but ends with
    // row TWO_POINT_TWO. We should read one block only.
    Get get = new Get(TWO);
    Scan scan = new Scan(get);
    try (CellGridStoreScanner scanner = new CellGridStoreScanner(scan, this.scanInfo)) {
      List<Cell> results = new ArrayList<>();
      while (scanner.next(results)) {
        continue;
      }
      // Should be four results of column 1 (though there are 5 rows in the CELL_GRID -- the
      // TWO_POINT_TWO row does not have a a column ONE.
      assertEquals(4, results.size());
      // We should have gone the optimize route 5 times totally... an INCLUDE for the four cells
      // in the row plus the DONE on the end.
      assertEquals(5, scanner.count.get());
      // For a full row Get, there should be no opportunity for scanner optimization.
      assertEquals(0, scanner.optimization.get());
    }
  }

  @Test
  public void testFullRowSpansBlocks() throws IOException {
    // Do a Get against row FOUR. It spans two blocks.
    Get get = new Get(FOUR);
    Scan scan = new Scan(get);
    try (CellGridStoreScanner scanner = new CellGridStoreScanner(scan, this.scanInfo)) {
      List<Cell> results = new ArrayList<>();
      while (scanner.next(results)) {
        continue;
      }
      // Should be four results of column 1 (though there are 5 rows in the CELL_GRID -- the
      // TWO_POINT_TWO row does not have a a column ONE.
      assertEquals(5, results.size());
      // We should have gone the optimize route 6 times totally... an INCLUDE for the five cells
      // in the row plus the DONE on the end.
      assertEquals(6, scanner.count.get());
      // For a full row Get, there should be no opportunity for scanner optimization.
      assertEquals(0, scanner.optimization.get());
    }
  }

  /**
   * Test optimize in StoreScanner. Test that we skip to the next 'block' when we it makes sense
   * reading the block 'index'.
   * @throws IOException
   */
  @Test
  public void testOptimize() throws IOException {
    Scan scan = new Scan();
    // A scan that just gets the first qualifier on each row of the CELL_GRID
    scan.addColumn(CF, ONE);
    try (CellGridStoreScanner scanner = new CellGridStoreScanner(scan, this.scanInfo)) {
      List<Cell> results = new ArrayList<>();
      while (scanner.next(results)) {
        continue;
      }
      // Should be four results of column 1 (though there are 5 rows in the CELL_GRID -- the
      // TWO_POINT_TWO row does not have a a column ONE.
      assertEquals(4, results.size());
      for (Cell cell: results) {
        assertTrue(Bytes.equals(ONE, 0, ONE.length,
            cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
      }
      assertTrue("Optimize should do some optimizations", scanner.optimization.get() > 0);
    }
  }

  /**
   * Ensure the optimize Scan method in StoreScanner does not get in the way of a Get doing minimum
   * work... seeking to start of block and then SKIPPING until we find the wanted Cell.
   * This 'simple' scenario mimics case of all Cells fitting inside a single HFileBlock.
   * See HBASE-15392. This test is a little cryptic. Takes a bit of staring to figure what it up to.
   * @throws IOException
   */
  @Test
  public void testOptimizeAndGet() throws IOException {
    // First test a Get of two columns in the row R2. Every Get is a Scan. Get columns named
    // R2 and R3.
    Get get = new Get(TWO);
    get.addColumn(CF, TWO);
    get.addColumn(CF, THREE);
    Scan scan = new Scan(get);
    try (CellGridStoreScanner scanner = new CellGridStoreScanner(scan, this.scanInfo)) {
      List<Cell> results = new ArrayList<>();
      // For a Get there should be no more next's after the first call.
      assertEquals(false, scanner.next(results));
      // Should be one result only.
      assertEquals(2, results.size());
      // And we should have gone through optimize twice only.
      assertEquals("First qcode is SEEK_NEXT_COL and second INCLUDE_AND_SEEK_NEXT_ROW", 3,
        scanner.count.get());
    }
  }

  /**
   * Ensure that optimize does not cause the Get to do more seeking than required. Optimize
   * (see HBASE-15392) was causing us to seek all Cells in a block when a Get Scan if the next block
   * index/start key was a different row to the current one. A bug. We'd call next too often
   * because we had to exhaust all Cells in the current row making us load the next block just to
   * discard what we read there. This test is a little cryptic. Takes a bit of staring to figure
   * what it up to.
   * @throws IOException
   */
  @Test
  public void testOptimizeAndGetWithFakedNextBlockIndexStart() throws IOException {
    // First test a Get of second column in the row R2. Every Get is a Scan. Second column has a
    // qualifier of R2.
    Get get = new Get(THREE);
    get.addColumn(CF, TWO);
    Scan scan = new Scan(get);
    try (CellGridStoreScanner scanner = new CellGridStoreScanner(scan, this.scanInfo)) {
      List<Cell> results = new ArrayList<>();
      // For a Get there should be no more next's after the first call.
      assertEquals(false, scanner.next(results));
      // Should be one result only.
      assertEquals(1, results.size());
      // And we should have gone through optimize twice only.
      assertEquals("First qcode is SEEK_NEXT_COL and second INCLUDE_AND_SEEK_NEXT_ROW", 2,
        scanner.count.get());
    }
  }

  @Test
  public void testScanTimeRange() throws IOException {
    String r1 = "R1";
    // returns only 1 of these 2 even though same timestamp
    KeyValue [] kvs = new KeyValue[] {
        create(r1, CF_STR, "a", 1, KeyValue.Type.Put, "dont-care"),
        create(r1, CF_STR, "a", 2, KeyValue.Type.Put, "dont-care"),
        create(r1, CF_STR, "a", 3, KeyValue.Type.Put, "dont-care"),
        create(r1, CF_STR, "a", 4, KeyValue.Type.Put, "dont-care"),
        create(r1, CF_STR, "a", 5, KeyValue.Type.Put, "dont-care"),
    };
    List<KeyValueScanner> scanners = Arrays.<KeyValueScanner>asList(
        new KeyValueScanner[] {
            new KeyValueScanFixture(CellComparator.getInstance(), kvs)
    });
    Scan scanSpec = new Scan().withStartRow(Bytes.toBytes(r1));
    scanSpec.setTimeRange(0, 6);
    scanSpec.readAllVersions();
    List<Cell> results = null;
    try (StoreScanner scan = new StoreScanner(scanSpec, scanInfo, getCols("a"), scanners)) {
      results = new ArrayList<>();
      assertEquals(true, scan.next(results));
      assertEquals(5, results.size());
      assertEquals(kvs[kvs.length - 1], results.get(0));
    }
    // Scan limited TimeRange
    scanSpec = new Scan().withStartRow(Bytes.toBytes(r1));
    scanSpec.setTimeRange(1, 3);
    scanSpec.readAllVersions();
    try (StoreScanner scan = new StoreScanner(scanSpec, scanInfo, getCols("a"), scanners)) {
      results = new ArrayList<>();
      assertEquals(true, scan.next(results));
      assertEquals(2, results.size());
    }
    // Another range.
    scanSpec = new Scan().withStartRow(Bytes.toBytes(r1));
    scanSpec.setTimeRange(5, 10);
    scanSpec.readAllVersions();
    try (StoreScanner scan = new StoreScanner(scanSpec, scanInfo, getCols("a"), scanners)) {
      results = new ArrayList<>();
      assertEquals(true, scan.next(results));
      assertEquals(1, results.size());
    }
    // See how TimeRange and Versions interact.
    // Another range.
    scanSpec = new Scan().withStartRow(Bytes.toBytes(r1));
    scanSpec.setTimeRange(0, 10);
    scanSpec.readVersions(3);
    try (StoreScanner scan = new StoreScanner(scanSpec, scanInfo, getCols("a"), scanners)) {
      results = new ArrayList<>();
      assertEquals(true, scan.next(results));
      assertEquals(3, results.size());
    }
  }

  @Test
  public void testScanSameTimestamp() throws IOException {
    // returns only 1 of these 2 even though same timestamp
    KeyValue [] kvs = new KeyValue[] {
        create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
    };
    List<KeyValueScanner> scanners = Arrays.asList(
        new KeyValueScanner[] {
            new KeyValueScanFixture(CellComparator.getInstance(), kvs)
        });

    Scan scanSpec = new Scan().withStartRow(Bytes.toBytes("R1"));
    // this only uses maxVersions (default=1) and TimeRange (default=all)
    try (StoreScanner scan = new StoreScanner(scanSpec, scanInfo, getCols("a"), scanners)) {
      List<Cell> results = new ArrayList<>();
      assertEquals(true, scan.next(results));
      assertEquals(1, results.size());
      assertEquals(kvs[0], results.get(0));
    }
  }

  /*
   * Test test shows exactly how the matcher's return codes confuses the StoreScanner
   * and prevent it from doing the right thing.  Seeking once, then nexting twice
   * should return R1, then R2, but in this case it doesnt.
   * TODO this comment makes no sense above. Appears to do the right thing.
   * @throws IOException
   */
  @Test
  public void testWontNextToNext() throws IOException {
    // build the scan file:
    KeyValue [] kvs = new KeyValue[] {
        create("R1", "cf", "a", 2, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
        create("R2", "cf", "a", 1, KeyValue.Type.Put, "dont-care")
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);

    Scan scanSpec = new Scan().withStartRow(Bytes.toBytes("R1"));
    // this only uses maxVersions (default=1) and TimeRange (default=all)
    try (StoreScanner scan = new StoreScanner(scanSpec, scanInfo, getCols("a"), scanners)) {
      List<Cell> results = new ArrayList<>();
      scan.next(results);
      assertEquals(1, results.size());
      assertEquals(kvs[0], results.get(0));
      // should be ok...
      // now scan _next_ again.
      results.clear();
      scan.next(results);
      assertEquals(1, results.size());
      assertEquals(kvs[2], results.get(0));

      results.clear();
      scan.next(results);
      assertEquals(0, results.size());
    }
  }


  @Test
  public void testDeleteVersionSameTimestamp() throws IOException {
    KeyValue [] kvs = new KeyValue [] {
        create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "a", 1, KeyValue.Type.Delete, "dont-care"),
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    Scan scanSpec = new Scan().withStartRow(Bytes.toBytes("R1"));
    try (StoreScanner scan = new StoreScanner(scanSpec, scanInfo, getCols("a"), scanners)) {
      List<Cell> results = new ArrayList<>();
      assertFalse(scan.next(results));
      assertEquals(0, results.size());
    }
  }

  /*
   * Test the case where there is a delete row 'in front of' the next row, the scanner
   * will move to the next row.
   */
  @Test
  public void testDeletedRowThenGoodRow() throws IOException {
    KeyValue [] kvs = new KeyValue [] {
        create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "a", 1, KeyValue.Type.Delete, "dont-care"),
        create("R2", "cf", "a", 20, KeyValue.Type.Put, "dont-care")
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    Scan scanSpec = new Scan().withStartRow(Bytes.toBytes("R1"));
    try (StoreScanner scan = new StoreScanner(scanSpec, scanInfo, getCols("a"), scanners)) {
      List<Cell> results = new ArrayList<>();
      assertEquals(true, scan.next(results));
      assertEquals(0, results.size());

      assertEquals(true, scan.next(results));
      assertEquals(1, results.size());
      assertEquals(kvs[2], results.get(0));

      assertEquals(false, scan.next(results));
    }
  }

  @Test
  public void testDeleteVersionMaskingMultiplePuts() throws IOException {
    long now = System.currentTimeMillis();
    KeyValue [] kvs1 = new KeyValue[] {
        create("R1", "cf", "a", now, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "a", now, KeyValue.Type.Delete, "dont-care")
    };
    KeyValue [] kvs2 = new KeyValue[] {
        create("R1", "cf", "a", now-500, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "a", now-100, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "a", now, KeyValue.Type.Put, "dont-care")
    };
    List<KeyValueScanner> scanners = scanFixture(kvs1, kvs2);

    try (StoreScanner scan = new StoreScanner(new Scan().withStartRow(Bytes.toBytes("R1")),
        scanInfo, getCols("a"), scanners)) {
      List<Cell> results = new ArrayList<>();
      // the two put at ts=now will be masked by the 1 delete, and
      // since the scan default returns 1 version we'll return the newest
      // key, which is kvs[2], now-100.
      assertEquals(true, scan.next(results));
      assertEquals(1, results.size());
      assertEquals(kvs2[1], results.get(0));
    }
  }

  @Test
  public void testDeleteVersionsMixedAndMultipleVersionReturn() throws IOException {
    long now = System.currentTimeMillis();
    KeyValue [] kvs1 = new KeyValue[] {
        create("R1", "cf", "a", now, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "a", now, KeyValue.Type.Delete, "dont-care")
    };
    KeyValue [] kvs2 = new KeyValue[] {
        create("R1", "cf", "a", now-500, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "a", now+500, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "a", now, KeyValue.Type.Put, "dont-care"),
        create("R2", "cf", "z", now, KeyValue.Type.Put, "dont-care")
    };
    List<KeyValueScanner> scanners = scanFixture(kvs1, kvs2);

    Scan scanSpec = new Scan().withStartRow(Bytes.toBytes("R1")).readVersions(2);
    try (StoreScanner scan = new StoreScanner(scanSpec, scanInfo, getCols("a"), scanners)) {
      List<Cell> results = new ArrayList<>();
      assertEquals(true, scan.next(results));
      assertEquals(2, results.size());
      assertEquals(kvs2[1], results.get(0));
      assertEquals(kvs2[0], results.get(1));
    }
  }

  @Test
  public void testWildCardOneVersionScan() throws IOException {
    KeyValue [] kvs = new KeyValue [] {
        create("R1", "cf", "a", 2, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "b", 1, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "a", 1, KeyValue.Type.DeleteColumn, "dont-care"),
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    try (StoreScanner scan =
        new StoreScanner(new Scan().withStartRow(Bytes.toBytes("R1")), scanInfo, null, scanners)) {
      List<Cell> results = new ArrayList<>();
      assertEquals(true, scan.next(results));
      assertEquals(2, results.size());
      assertEquals(kvs[0], results.get(0));
      assertEquals(kvs[1], results.get(1));
    }
  }

  @Test
  public void testWildCardScannerUnderDeletes() throws IOException {
    KeyValue [] kvs = new KeyValue [] {
        create("R1", "cf", "a", 2, KeyValue.Type.Put, "dont-care"), // inc
        // orphaned delete column.
        create("R1", "cf", "a", 1, KeyValue.Type.DeleteColumn, "dont-care"),
        // column b
        create("R1", "cf", "b", 2, KeyValue.Type.Put, "dont-care"), // inc
        create("R1", "cf", "b", 1, KeyValue.Type.Put, "dont-care"), // inc
        // column c
        create("R1", "cf", "c", 10, KeyValue.Type.Delete, "dont-care"),
        create("R1", "cf", "c", 10, KeyValue.Type.Put, "dont-care"), // no
        create("R1", "cf", "c", 9, KeyValue.Type.Put, "dont-care"),  // inc
        // column d
        create("R1", "cf", "d", 11, KeyValue.Type.Put, "dont-care"), // inc
        create("R1", "cf", "d", 10, KeyValue.Type.DeleteColumn, "dont-care"),
        create("R1", "cf", "d", 9, KeyValue.Type.Put, "dont-care"),  // no
        create("R1", "cf", "d", 8, KeyValue.Type.Put, "dont-care"),  // no

    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    try (StoreScanner scan =
        new StoreScanner(new Scan().readVersions(2), scanInfo, null, scanners)) {
      List<Cell> results = new ArrayList<>();
      assertEquals(true, scan.next(results));
      assertEquals(5, results.size());
      assertEquals(kvs[0], results.get(0));
      assertEquals(kvs[2], results.get(1));
      assertEquals(kvs[3], results.get(2));
      assertEquals(kvs[6], results.get(3));
      assertEquals(kvs[7], results.get(4));
    }
  }

  @Test
  public void testDeleteFamily() throws IOException {
    KeyValue[] kvs = new KeyValue[] {
        create("R1", "cf", "a", 100, KeyValue.Type.DeleteFamily, "dont-care"),
        create("R1", "cf", "b", 11, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "c", 11, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "d", 11, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "e", 11, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "e", 11, KeyValue.Type.DeleteColumn, "dont-care"),
        create("R1", "cf", "f", 11, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "g", 11, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "g", 11, KeyValue.Type.Delete, "dont-care"),
        create("R1", "cf", "h", 11, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "i", 11, KeyValue.Type.Put, "dont-care"),
        create("R2", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    try (StoreScanner scan =
        new StoreScanner(new Scan().readAllVersions(), scanInfo, null, scanners)) {
      List<Cell> results = new ArrayList<>();
      assertEquals(true, scan.next(results));
      assertEquals(0, results.size());
      assertEquals(true, scan.next(results));
      assertEquals(1, results.size());
      assertEquals(kvs[kvs.length - 1], results.get(0));

      assertEquals(false, scan.next(results));
    }
  }

  @Test
  public void testDeleteColumn() throws IOException {
    KeyValue [] kvs = new KeyValue[] {
        create("R1", "cf", "a", 10, KeyValue.Type.DeleteColumn, "dont-care"),
        create("R1", "cf", "a", 9, KeyValue.Type.Delete, "dont-care"),
        create("R1", "cf", "a", 8, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "b", 5, KeyValue.Type.Put, "dont-care")
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    try (StoreScanner scan = new StoreScanner(new Scan(), scanInfo, null, scanners)) {
      List<Cell> results = new ArrayList<>();
      assertEquals(true, scan.next(results));
      assertEquals(1, results.size());
      assertEquals(kvs[3], results.get(0));
    }
  }

  private static final KeyValue[] kvs = new KeyValue[] {
        create("R1", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "b", 11, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "c", 11, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "d", 11, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "e", 11, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "f", 11, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "g", 11, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "h", 11, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "i", 11, KeyValue.Type.Put, "dont-care"),
        create("R2", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
    };

  @Test
  public void testSkipColumn() throws IOException {
    List<KeyValueScanner> scanners = scanFixture(kvs);
    try (StoreScanner scan = new StoreScanner(new Scan(), scanInfo, getCols("a", "d"), scanners)) {
      List<Cell> results = new ArrayList<>();
      assertEquals(true, scan.next(results));
      assertEquals(2, results.size());
      assertEquals(kvs[0], results.get(0));
      assertEquals(kvs[3], results.get(1));
      results.clear();

      assertEquals(true, scan.next(results));
      assertEquals(1, results.size());
      assertEquals(kvs[kvs.length - 1], results.get(0));

      results.clear();
      assertEquals(false, scan.next(results));
    }
  }

  /*
   * Test expiration of KeyValues in combination with a configured TTL for
   * a column family (as should be triggered in a major compaction).
   */
  @Test
  public void testWildCardTtlScan() throws IOException {
    long now = System.currentTimeMillis();
    KeyValue [] kvs = new KeyValue[] {
        create("R1", "cf", "a", now-1000, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "b", now-10, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "c", now-200, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "d", now-10000, KeyValue.Type.Put, "dont-care"),
        create("R2", "cf", "a", now, KeyValue.Type.Put, "dont-care"),
        create("R2", "cf", "b", now-10, KeyValue.Type.Put, "dont-care"),
        create("R2", "cf", "c", now-200, KeyValue.Type.Put, "dont-care"),
        create("R2", "cf", "c", now-1000, KeyValue.Type.Put, "dont-care")
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    Scan scan = new Scan();
    scan.readVersions(1);
    ScanInfo scanInfo = new ScanInfo(CONF, CF, 0, 1, 500, KeepDeletedCells.FALSE,
        HConstants.DEFAULT_BLOCKSIZE, 0, CellComparator.getInstance(), false);
    try (StoreScanner scanner = new StoreScanner(scan, scanInfo, null, scanners)) {
      List<Cell> results = new ArrayList<>();
      assertEquals(true, scanner.next(results));
      assertEquals(2, results.size());
      assertEquals(kvs[1], results.get(0));
      assertEquals(kvs[2], results.get(1));
      results.clear();

      assertEquals(true, scanner.next(results));
      assertEquals(3, results.size());
      assertEquals(kvs[4], results.get(0));
      assertEquals(kvs[5], results.get(1));
      assertEquals(kvs[6], results.get(2));
      results.clear();

      assertEquals(false, scanner.next(results));
    }
  }

  @Test
  public void testScannerReseekDoesntNPE() throws Exception {
    List<KeyValueScanner> scanners = scanFixture(kvs);
    try (StoreScanner scan = new StoreScanner(new Scan(), scanInfo, getCols("a", "d"), scanners)) {
      // Previously a updateReaders twice in a row would cause an NPE. In test this would also
      // normally cause an NPE because scan.store is null. So as long as we get through these
      // two calls we are good and the bug was quashed.
      scan.updateReaders(Collections.emptyList(), Collections.emptyList());
      scan.updateReaders(Collections.emptyList(), Collections.emptyList());
      scan.peek();
    }
  }

  @Test @Ignore("this fails, since we don't handle deletions, etc, in peek")
  public void testPeek() throws Exception {
    KeyValue[] kvs = new KeyValue [] {
        create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
        create("R1", "cf", "a", 1, KeyValue.Type.Delete, "dont-care"),
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    Scan scanSpec = new Scan().withStartRow(Bytes.toBytes("R1"));
    try (StoreScanner scan = new StoreScanner(scanSpec, scanInfo, getCols("a"), scanners)) {
      assertNull(scan.peek());
    }
  }

  /**
   * Ensure that expired delete family markers don't override valid puts
   */
  @Test
  public void testExpiredDeleteFamily() throws Exception {
    long now = System.currentTimeMillis();
    KeyValue[] kvs = new KeyValue[] {
        new KeyValue(Bytes.toBytes("R1"), Bytes.toBytes("cf"), null, now-1000,
            KeyValue.Type.DeleteFamily),
        create("R1", "cf", "a", now-10, KeyValue.Type.Put,
            "dont-care"),
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    Scan scan = new Scan();
    scan.readVersions(1);
    // scanner with ttl equal to 500
    ScanInfo scanInfo = new ScanInfo(CONF, CF, 0, 1, 500, KeepDeletedCells.FALSE,
        HConstants.DEFAULT_BLOCKSIZE, 0, CellComparator.getInstance(), false);
    try (StoreScanner scanner = new StoreScanner(scan, scanInfo, null, scanners)) {
      List<Cell> results = new ArrayList<>();
      assertEquals(true, scanner.next(results));
      assertEquals(1, results.size());
      assertEquals(kvs[1], results.get(0));
      results.clear();

      assertEquals(false, scanner.next(results));
    }
  }

  @Test
  public void testDeleteMarkerLongevity() throws Exception {
    try {
      final long now = System.currentTimeMillis();
      EnvironmentEdgeManagerTestHelper.injectEdge(new EnvironmentEdge() {
        @Override
        public long currentTime() {
          return now;
        }
      });
      KeyValue[] kvs = new KeyValue[]{
        /*0*/ new KeyValue(Bytes.toBytes("R1"), Bytes.toBytes("cf"), null,
        now - 100, KeyValue.Type.DeleteFamily), // live
        /*1*/ new KeyValue(Bytes.toBytes("R1"), Bytes.toBytes("cf"), null,
        now - 1000, KeyValue.Type.DeleteFamily), // expired
        /*2*/ create("R1", "cf", "a", now - 50,
        KeyValue.Type.Put, "v3"), // live
        /*3*/ create("R1", "cf", "a", now - 55,
        KeyValue.Type.Delete, "dontcare"), // live
        /*4*/ create("R1", "cf", "a", now - 55,
        KeyValue.Type.Put, "deleted-version v2"), // deleted
        /*5*/ create("R1", "cf", "a", now - 60,
        KeyValue.Type.Put, "v1"), // live
        /*6*/ create("R1", "cf", "a", now - 65,
        KeyValue.Type.Put, "v0"), // max-version reached
        /*7*/ create("R1", "cf", "a",
        now - 100, KeyValue.Type.DeleteColumn, "dont-care"), // max-version
        /*8*/ create("R1", "cf", "b", now - 600,
        KeyValue.Type.DeleteColumn, "dont-care"), //expired
        /*9*/ create("R1", "cf", "b", now - 70,
        KeyValue.Type.Put, "v2"), //live
        /*10*/ create("R1", "cf", "b", now - 750,
        KeyValue.Type.Put, "v1"), //expired
        /*11*/ create("R1", "cf", "c", now - 500,
        KeyValue.Type.Delete, "dontcare"), //expired
        /*12*/ create("R1", "cf", "c", now - 600,
        KeyValue.Type.Put, "v1"), //expired
        /*13*/ create("R1", "cf", "c", now - 1000,
        KeyValue.Type.Delete, "dontcare"), //expired
        /*14*/ create("R1", "cf", "d", now - 60,
        KeyValue.Type.Put, "expired put"), //live
        /*15*/ create("R1", "cf", "d", now - 100,
        KeyValue.Type.Delete, "not-expired delete"), //live
      };
      List<KeyValueScanner> scanners = scanFixture(kvs);
      ScanInfo scanInfo = new ScanInfo(CONF, Bytes.toBytes("cf"),
        0 /* minVersions */,
        2 /* maxVersions */, 500 /* ttl */,
        KeepDeletedCells.FALSE /* keepDeletedCells */,
        HConstants.DEFAULT_BLOCKSIZE /* block size */,
        200, /* timeToPurgeDeletes */
        CellComparator.getInstance(), false);
      try (StoreScanner scanner =
          new StoreScanner(scanInfo, OptionalInt.of(2), ScanType.COMPACT_DROP_DELETES, scanners)) {
        List<Cell> results = new ArrayList<>();
        results = new ArrayList<>();
        assertEquals(true, scanner.next(results));
        assertEquals(kvs[0], results.get(0));
        assertEquals(kvs[2], results.get(1));
        assertEquals(kvs[3], results.get(2));
        assertEquals(kvs[5], results.get(3));
        assertEquals(kvs[9], results.get(4));
        assertEquals(kvs[14], results.get(5));
        assertEquals(kvs[15], results.get(6));
        assertEquals(7, results.size());
      }
    } finally {
      EnvironmentEdgeManagerTestHelper.reset();
    }
  }

  @Test
  public void testPreadNotEnabledForCompactionStoreScanners() throws Exception {
    long now = System.currentTimeMillis();
    KeyValue[] kvs = new KeyValue[] {
        new KeyValue(Bytes.toBytes("R1"), Bytes.toBytes("cf"), null, now - 1000,
            KeyValue.Type.DeleteFamily),
        create("R1", "cf", "a", now - 10, KeyValue.Type.Put, "dont-care"), };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    ScanInfo scanInfo = new ScanInfo(CONF, CF, 0, 1, 500, KeepDeletedCells.FALSE,
        HConstants.DEFAULT_BLOCKSIZE, 0, CellComparator.getInstance(), false);
    try (StoreScanner storeScanner = new StoreScanner(scanInfo, OptionalInt.empty(),
        ScanType.COMPACT_RETAIN_DELETES, scanners)) {
      assertFalse(storeScanner.isScanUsePread());
    }
  }
}
