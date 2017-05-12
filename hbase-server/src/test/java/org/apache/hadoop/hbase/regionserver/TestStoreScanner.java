/*
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

import static org.apache.hadoop.hbase.regionserver.KeyValueScanFixture.scanFixture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.junit.experimental.categories.Category;

import junit.framework.TestCase;

// Can't be small as it plays with EnvironmentEdgeManager
@Category(MediumTests.class)
public class TestStoreScanner extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestStoreScanner.class);
  private static final String CF_STR = "cf";
  final static byte [] CF = Bytes.toBytes(CF_STR);
  static Configuration CONF = HBaseConfiguration.create();
  private ScanInfo scanInfo = new ScanInfo(CONF, CF, 0, Integer.MAX_VALUE,
      Long.MAX_VALUE, KeepDeletedCells.FALSE, 0, KeyValue.COMPARATOR);
  private ScanType scanType = ScanType.USER_SCAN;

  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * From here on down, we have a bunch of defines and specific CELL_GRID of Cells. The
   * CELL_GRID then has a Scanner that can fake out 'block' transitions. All this elaborate
   * setup is for tests that ensure we don't overread, and that the
   * {@link StoreScanner#optimize(org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher.MatchCode,
   * Cell)} is not overly enthusiastic.
   */
  private static final byte [] ZERO = new byte [] {'0'};
  private static final byte [] ZERO_POINT_ZERO = new byte [] {'0', '.', '0'};
  private static final byte [] ONE = new byte [] {'1'};
  private static final byte [] TWO = new byte [] {'2'};
  private static final byte [] TWO_POINT_TWO = new byte [] {'2', '.', '2'};
  private static final byte [] THREE = new byte [] {'3'};
  private static final byte [] FOUR = new byte [] {'4'};
  private static final byte [] FIVE = new byte [] {'5'};
  private static final byte [] VALUE = new byte [] {'v'};
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
  private static final KeyValue [] CELL_GRID = new KeyValue [] {
    new KeyValue(ONE, CF, ONE, 1L, KeyValue.Type.Put, VALUE),
    new KeyValue(ONE, CF, TWO, 1L, KeyValue.Type.Put, VALUE),
    new KeyValue(ONE, CF, THREE, 1L, KeyValue.Type.Put, VALUE),
    new KeyValue(ONE, CF, FOUR, 1L, KeyValue.Type.Put, VALUE),
    // Offset 4 CELL_GRID_BLOCK2_BOUNDARY
    new KeyValue(TWO, CF, ONE, 1L, KeyValue.Type.Put, VALUE),
    new KeyValue(TWO, CF, TWO, 1L, KeyValue.Type.Put, VALUE),
    new KeyValue(TWO, CF, THREE, 1L, KeyValue.Type.Put, VALUE),
    new KeyValue(TWO, CF, FOUR, 1L, KeyValue.Type.Put, VALUE),
    new KeyValue(TWO_POINT_TWO, CF, ZERO, 1L, KeyValue.Type.Put, VALUE),
    new KeyValue(TWO_POINT_TWO, CF, ZERO_POINT_ZERO, 1L, KeyValue.Type.Put, VALUE),
    new KeyValue(TWO_POINT_TWO, CF, FIVE, 1L, KeyValue.Type.Put, VALUE),
    // Offset 11! CELL_GRID_BLOCK3_BOUNDARY
    new KeyValue(THREE, CF, ONE, 1L, KeyValue.Type.Put, VALUE),
    new KeyValue(THREE, CF, TWO, 1L, KeyValue.Type.Put, VALUE),
    new KeyValue(THREE, CF, THREE, 1L, KeyValue.Type.Put, VALUE),
    new KeyValue(THREE, CF, FOUR, 1L, KeyValue.Type.Put, VALUE),
    // Offset 15 CELL_GRID_BLOCK4_BOUNDARY
    new KeyValue(FOUR, CF, ONE, 1L, KeyValue.Type.Put, VALUE),
    new KeyValue(FOUR, CF, TWO, 1L, KeyValue.Type.Put, VALUE),
    new KeyValue(FOUR, CF, THREE, 1L, KeyValue.Type.Put, VALUE),
    new KeyValue(FOUR, CF, FOUR, 1L, KeyValue.Type.Put, VALUE),
    // Offset 19 CELL_GRID_BLOCK5_BOUNDARY
    new KeyValue(FOUR, CF, FIVE, 1L, KeyValue.Type.Put, VALUE),
    new KeyValue(FIVE, CF, ZERO, 1L, KeyValue.Type.Put, VALUE),
  };

  private static class KeyValueHeapWithCount extends KeyValueHeap {

    final AtomicInteger count;

    public KeyValueHeapWithCount(List<? extends KeyValueScanner> scanners,
        KVComparator comparator, AtomicInteger count) throws IOException {
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

    CellGridStoreScanner(final Scan scan, ScanInfo scanInfo, ScanType scanType)
    throws IOException {
      super(scan, scanInfo, scanType, scan.getFamilyMap().get(CF),
        Arrays.<KeyValueScanner>asList(
          new KeyValueScanner[] {new KeyValueScanFixture(KeyValue.COMPARATOR, CELL_GRID)}));
    }

    protected void resetKVHeap(List<? extends KeyValueScanner> scanners,
        KVComparator comparator) throws IOException {
      if (count == null) {
        count = new AtomicInteger(0);
      }
      heap = new KeyValueHeapWithCount(scanners, comparator, count);
    }

    protected boolean trySkipToNextRow(Cell cell) throws IOException {
      boolean optimized = super.trySkipToNextRow(cell);
      LOG.info("Cell=" + cell + ", nextIndex=" + CellUtil.toString(getNextIndexedKey(), false)
          + ", optimized=" + optimized);
      if (optimized) {
        optimization.incrementAndGet();
      }
      return optimized;
    }

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
          KeyValueUtil.createFirstOnRow(CELL_GRID[CELL_GRID_BLOCK5_BOUNDARY].getRow()):
            count.get() > CELL_GRID_BLOCK3_BOUNDARY?
                KeyValueUtil.createFirstOnRow(CELL_GRID[CELL_GRID_BLOCK4_BOUNDARY].getRow()):
                  count.get() > CELL_GRID_BLOCK2_BOUNDARY?
                      KeyValueUtil.createFirstOnRow(CELL_GRID[CELL_GRID_BLOCK3_BOUNDARY].getRow()):
                        KeyValueUtil.createFirstOnRow(CELL_GRID[CELL_GRID_BLOCK2_BOUNDARY].getRow());
    }
  };

  private static final int CELL_WITH_VERSIONS_BLOCK2_BOUNDARY = 4;

  private static final KeyValue [] CELL_WITH_VERSIONS = new KeyValue [] {
    new KeyValue(ONE, CF, ONE, 2L, KeyValue.Type.Put, VALUE),
    new KeyValue(ONE, CF, ONE, 1L, KeyValue.Type.Put, VALUE),
    new KeyValue(ONE, CF, TWO, 2L, KeyValue.Type.Put, VALUE),
    new KeyValue(ONE, CF, TWO, 1L, KeyValue.Type.Put, VALUE),
    // Offset 4 CELL_WITH_VERSIONS_BLOCK2_BOUNDARY
    new KeyValue(TWO, CF, ONE, 1L, KeyValue.Type.Put, VALUE),
    new KeyValue(TWO, CF, TWO, 1L, KeyValue.Type.Put, VALUE),
  };

  private static class CellWithVersionsStoreScanner extends StoreScanner {
    // Count of how often optimize is called and of how often it does an optimize.
    final AtomicInteger optimization = new AtomicInteger(0);

    CellWithVersionsStoreScanner(final Scan scan, ScanInfo scanInfo, ScanType scanType)
        throws IOException {
      super(scan, scanInfo, scanType, scan.getFamilyMap().get(CF), Arrays
          .<KeyValueScanner> asList(new KeyValueScanner[] { new KeyValueScanFixture(
            KeyValue.COMPARATOR, CELL_WITH_VERSIONS) }));
    }

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
      return KeyValueUtil.createFirstOnRow(CELL_WITH_VERSIONS[CELL_WITH_VERSIONS_BLOCK2_BOUNDARY].getRow());
    }
  };

  private static class CellWithVersionsNoOptimizeStoreScanner extends StoreScanner {
    // Count of how often optimize is called and of how often it does an optimize.
    final AtomicInteger optimization = new AtomicInteger(0);

    CellWithVersionsNoOptimizeStoreScanner(final Scan scan, ScanInfo scanInfo, ScanType scanType)
        throws IOException {
      super(scan, scanInfo, scanType, scan.getFamilyMap().get(CF), Arrays
          .<KeyValueScanner> asList(new KeyValueScanner[] { new KeyValueScanFixture(
            KeyValue.COMPARATOR, CELL_WITH_VERSIONS) }));
    }

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

  public void testWithColumnCountGetFilter() throws Exception {
    Get get = new Get(ONE);
    get.setMaxVersions();
    get.addFamily(CF);
    get.setFilter(new ColumnCountGetFilter(2));

    CellWithVersionsNoOptimizeStoreScanner scannerNoOptimize = new CellWithVersionsNoOptimizeStoreScanner(
        new Scan(get), this.scanInfo, this.scanType);
    try {
      List<Cell> results = new ArrayList<>();
      while (scannerNoOptimize.next(results)) {
        continue;
      }
      assertEquals(2, results.size());
      assertTrue(CellUtil.matchingColumn(results.get(0), CELL_WITH_VERSIONS[0]));
      assertTrue(CellUtil.matchingColumn(results.get(1), CELL_WITH_VERSIONS[2]));
      assertTrue("Optimize should do some optimizations",
        scannerNoOptimize.optimization.get() == 0);
    } finally {
      scannerNoOptimize.close();
    }

    get.setFilter(new ColumnCountGetFilter(2));
    CellWithVersionsStoreScanner scanner = new CellWithVersionsStoreScanner(new Scan(get),
        this.scanInfo, this.scanType);
    try {
      List<Cell> results = new ArrayList<>();
      while (scanner.next(results)) {
        continue;
      }
      assertEquals(2, results.size());
      assertTrue(CellUtil.matchingColumn(results.get(0), CELL_WITH_VERSIONS[0]));
      assertTrue(CellUtil.matchingColumn(results.get(1), CELL_WITH_VERSIONS[2]));
      assertTrue("Optimize should do some optimizations", scanner.optimization.get() > 0);
    } finally {
      scanner.close();
    }
  }

  public void testFullRowGetDoesNotOverreadWhenRowInsideOneBlock() throws IOException {
    // Do a Get against row two. Row two is inside a block that starts with row TWO but ends with
    // row TWO_POINT_TWO. We should read one block only.
    Get get = new Get(TWO);
    Scan scan = new Scan(get);
    CellGridStoreScanner scanner = new CellGridStoreScanner(scan, this.scanInfo, this.scanType);
    try {
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
    } finally {
      scanner.close();
    }
  }

  public void testFullRowSpansBlocks() throws IOException {
    // Do a Get against row FOUR. It spans two blocks.
    Get get = new Get(FOUR);
    Scan scan = new Scan(get);
    CellGridStoreScanner scanner = new CellGridStoreScanner(scan, this.scanInfo, this.scanType);
    try {
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
    } finally {
      scanner.close();
    }
  }

  /**
   * Test optimize in StoreScanner. Test that we skip to the next 'block' when we it makes sense
   * reading the block 'index'.
   * @throws IOException
   */
  public void testOptimize() throws IOException {
    Scan scan = new Scan();
    // A scan that just gets the first qualifier on each row of the CELL_GRID
    scan.addColumn(CF, ONE);
    CellGridStoreScanner scanner = new CellGridStoreScanner(scan, this.scanInfo, this.scanType);
    try {
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
    } finally {
      scanner.close();
    }
  }

  /**
   * Ensure the optimize Scan method in StoreScanner does not get in the way of a Get doing minimum
   * work... seeking to start of block and then SKIPPING until we find the wanted Cell.
   * This 'simple' scenario mimics case of all Cells fitting inside a single HFileBlock.
   * See HBASE-15392. This test is a little cryptic. Takes a bit of staring to figure what it up to.
   * @throws IOException
   */
  public void testOptimizeAndGet() throws IOException {
    // First test a Get of two columns in the row R2. Every Get is a Scan. Get columns named
    // R2 and R3.
    Get get = new Get(TWO);
    get.addColumn(CF, TWO);
    get.addColumn(CF, THREE);
    Scan scan = new Scan(get);
    CellGridStoreScanner scanner = new CellGridStoreScanner(scan, this.scanInfo, this.scanType);
    try {
      List<Cell> results = new ArrayList<>();
      // For a Get there should be no more next's after the first call.
      assertEquals(false, scanner.next(results));
      // Should be one result only.
      assertEquals(2, results.size());
      // And we should have gone through optimize twice only.
      assertEquals("First qcode is SEEK_NEXT_COL and second INCLUDE_AND_SEEK_NEXT_ROW",
        3, scanner.count.get());
    } finally {
      scanner.close();
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
  public void testOptimizeAndGetWithFakedNextBlockIndexStart() throws IOException {
    // First test a Get of second column in the row R2. Every Get is a Scan. Second column has a
    // qualifier of R2.
    Get get = new Get(THREE);
    get.addColumn(CF, TWO);
    Scan scan = new Scan(get);
    CellGridStoreScanner scanner = new CellGridStoreScanner(scan, this.scanInfo, this.scanType);
    try {
      List<Cell> results = new ArrayList<>();
      // For a Get there should be no more next's after the first call.
      assertEquals(false, scanner.next(results));
      // Should be one result only.
      assertEquals(1, results.size());
      // And we should have gone through optimize twice only.
      assertEquals("First qcode is SEEK_NEXT_COL and second INCLUDE_AND_SEEK_NEXT_ROW",
        2, scanner.count.get());
    } finally {
      scanner.close();
    }
  }

  /*
   * Test utility for building a NavigableSet for scanners.
   * @param strCols
   * @return
   */
  NavigableSet<byte[]> getCols(String ...strCols) {
    NavigableSet<byte[]> cols = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    for (String col : strCols) {
      byte[] bytes = Bytes.toBytes(col);
      cols.add(bytes);
    }
    return cols;
  }

  public void testScanTimeRange() throws IOException {
    String r1 = "R1";
    // returns only 1 of these 2 even though same timestamp
    KeyValue [] kvs = new KeyValue[] {
        KeyValueTestUtil.create(r1, CF_STR, "a", 1, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create(r1, CF_STR, "a", 2, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create(r1, CF_STR, "a", 3, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create(r1, CF_STR, "a", 4, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create(r1, CF_STR, "a", 5, KeyValue.Type.Put, "dont-care"),
    };
    List<KeyValueScanner> scanners = Arrays.<KeyValueScanner>asList(
        new KeyValueScanner[] {
            new KeyValueScanFixture(KeyValue.COMPARATOR, kvs)
    });
    Scan scanSpec = new Scan(Bytes.toBytes(r1));
    scanSpec.setTimeRange(0, 6);
    scanSpec.setMaxVersions();
    StoreScanner scan = new StoreScanner(scanSpec, scanInfo, scanType,
        getCols("a"), scanners);
    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(5, results.size());
    assertEquals(kvs[kvs.length - 1], results.get(0));
    // Scan limited TimeRange
    scanSpec = new Scan(Bytes.toBytes(r1));
    scanSpec.setTimeRange(1, 3);
    scanSpec.setMaxVersions();
    scan = new StoreScanner(scanSpec, scanInfo, scanType, getCols("a"),
        scanners);
    results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(2, results.size());
    // Another range.
    scanSpec = new Scan(Bytes.toBytes(r1));
    scanSpec.setTimeRange(5, 10);
    scanSpec.setMaxVersions();
    scan = new StoreScanner(scanSpec, scanInfo, scanType, getCols("a"),
        scanners);
    results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(1, results.size());
    // See how TimeRange and Versions interact.
    // Another range.
    scanSpec = new Scan(Bytes.toBytes(r1));
    scanSpec.setTimeRange(0, 10);
    scanSpec.setMaxVersions(3);
    scan = new StoreScanner(scanSpec, scanInfo, scanType, getCols("a"),
        scanners);
    results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(3, results.size());
  }

  public void testScanSameTimestamp() throws IOException {
    // returns only 1 of these 2 even though same timestamp
    KeyValue [] kvs = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
    };
    List<KeyValueScanner> scanners = Arrays.asList(
        new KeyValueScanner[] {
            new KeyValueScanFixture(KeyValue.COMPARATOR, kvs)
        });

    Scan scanSpec = new Scan(Bytes.toBytes("R1"));
    // this only uses maxVersions (default=1) and TimeRange (default=all)
    StoreScanner scan = new StoreScanner(scanSpec, scanInfo, scanType,
        getCols("a"), scanners);

    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(1, results.size());
    assertEquals(kvs[0], results.get(0));
  }

  /*
   * Test test shows exactly how the matcher's return codes confuses the StoreScanner
   * and prevent it from doing the right thing.  Seeking once, then nexting twice
   * should return R1, then R2, but in this case it doesnt.
   * TODO this comment makes no sense above. Appears to do the right thing.
   * @throws IOException
   */
  public void testWontNextToNext() throws IOException {
    // build the scan file:
    KeyValue [] kvs = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", 2, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "a", 1, KeyValue.Type.Put, "dont-care")
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);

    Scan scanSpec = new Scan(Bytes.toBytes("R1"));
    // this only uses maxVersions (default=1) and TimeRange (default=all)
    StoreScanner scan = new StoreScanner(scanSpec, scanInfo, scanType,
        getCols("a"), scanners);

    List<Cell> results = new ArrayList<Cell>();
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


  public void testDeleteVersionSameTimestamp() throws IOException {
    KeyValue [] kvs = new KeyValue [] {
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Delete, "dont-care"),
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    Scan scanSpec = new Scan(Bytes.toBytes("R1"));
    StoreScanner scan = new StoreScanner(scanSpec, scanInfo, scanType,
        getCols("a"), scanners);

    List<Cell> results = new ArrayList<Cell>();
    assertFalse(scan.next(results));
    assertEquals(0, results.size());
  }

  /*
   * Test the case where there is a delete row 'in front of' the next row, the scanner
   * will move to the next row.
   */
  public void testDeletedRowThenGoodRow() throws IOException {
    KeyValue [] kvs = new KeyValue [] {
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Delete, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "a", 20, KeyValue.Type.Put, "dont-care")
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    Scan scanSpec = new Scan(Bytes.toBytes("R1"));
    StoreScanner scan = new StoreScanner(scanSpec, scanInfo, scanType,
        getCols("a"), scanners);

    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(0, results.size());

    assertEquals(true, scan.next(results));
    assertEquals(1, results.size());
    assertEquals(kvs[2], results.get(0));

    assertEquals(false, scan.next(results));
  }

  public void testDeleteVersionMaskingMultiplePuts() throws IOException {
    long now = System.currentTimeMillis();
    KeyValue [] kvs1 = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", now, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", now, KeyValue.Type.Delete, "dont-care")
    };
    KeyValue [] kvs2 = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", now-500, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", now-100, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", now, KeyValue.Type.Put, "dont-care")
    };
    List<KeyValueScanner> scanners = scanFixture(kvs1, kvs2);

    StoreScanner scan = new StoreScanner(new Scan(Bytes.toBytes("R1")),
        scanInfo, scanType, getCols("a"), scanners);
    List<Cell> results = new ArrayList<Cell>();
    // the two put at ts=now will be masked by the 1 delete, and
    // since the scan default returns 1 version we'll return the newest
    // key, which is kvs[2], now-100.
    assertEquals(true, scan.next(results));
    assertEquals(1, results.size());
    assertEquals(kvs2[1], results.get(0));
  }
  public void testDeleteVersionsMixedAndMultipleVersionReturn() throws IOException {
    long now = System.currentTimeMillis();
    KeyValue [] kvs1 = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", now, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", now, KeyValue.Type.Delete, "dont-care")
    };
    KeyValue [] kvs2 = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", now-500, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", now+500, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", now, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "z", now, KeyValue.Type.Put, "dont-care")
    };
    List<KeyValueScanner> scanners = scanFixture(kvs1, kvs2);

    Scan scanSpec = new Scan(Bytes.toBytes("R1")).setMaxVersions(2);
    StoreScanner scan = new StoreScanner(scanSpec, scanInfo, scanType,
        getCols("a"), scanners);
    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(2, results.size());
    assertEquals(kvs2[1], results.get(0));
    assertEquals(kvs2[0], results.get(1));
  }

  public void testWildCardOneVersionScan() throws IOException {
    KeyValue [] kvs = new KeyValue [] {
        KeyValueTestUtil.create("R1", "cf", "a", 2, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "b", 1, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.DeleteColumn, "dont-care"),
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    StoreScanner scan = new StoreScanner(new Scan(Bytes.toBytes("R1")),
        scanInfo, scanType, null, scanners);
    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(2, results.size());
    assertEquals(kvs[0], results.get(0));
    assertEquals(kvs[1], results.get(1));
  }

  public void testWildCardScannerUnderDeletes() throws IOException {
    KeyValue [] kvs = new KeyValue [] {
        KeyValueTestUtil.create("R1", "cf", "a", 2, KeyValue.Type.Put, "dont-care"), // inc
        // orphaned delete column.
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.DeleteColumn, "dont-care"),
        // column b
        KeyValueTestUtil.create("R1", "cf", "b", 2, KeyValue.Type.Put, "dont-care"), // inc
        KeyValueTestUtil.create("R1", "cf", "b", 1, KeyValue.Type.Put, "dont-care"), // inc
        // column c
        KeyValueTestUtil.create("R1", "cf", "c", 10, KeyValue.Type.Delete, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "c", 10, KeyValue.Type.Put, "dont-care"), // no
        KeyValueTestUtil.create("R1", "cf", "c", 9, KeyValue.Type.Put, "dont-care"),  // inc
        // column d
        KeyValueTestUtil.create("R1", "cf", "d", 11, KeyValue.Type.Put, "dont-care"), // inc
        KeyValueTestUtil.create("R1", "cf", "d", 10, KeyValue.Type.DeleteColumn, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "d", 9, KeyValue.Type.Put, "dont-care"),  // no
        KeyValueTestUtil.create("R1", "cf", "d", 8, KeyValue.Type.Put, "dont-care"),  // no

    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    StoreScanner scan = new StoreScanner(new Scan().setMaxVersions(2),
        scanInfo, scanType, null, scanners);
    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(5, results.size());
    assertEquals(kvs[0], results.get(0));
    assertEquals(kvs[2], results.get(1));
    assertEquals(kvs[3], results.get(2));
    assertEquals(kvs[6], results.get(3));
    assertEquals(kvs[7], results.get(4));
  }

  public void testDeleteFamily() throws IOException {
    KeyValue [] kvs = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", 100, KeyValue.Type.DeleteFamily, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "b", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "c", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "d", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "e", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "e", 11, KeyValue.Type.DeleteColumn, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "f", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "g", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "g", 11, KeyValue.Type.Delete, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "h", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "i", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    StoreScanner scan = new StoreScanner(
        new Scan().setMaxVersions(Integer.MAX_VALUE), scanInfo, scanType, null,
        scanners);
    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(0, results.size());
    assertEquals(true, scan.next(results));
    assertEquals(1, results.size());
    assertEquals(kvs[kvs.length-1], results.get(0));

    assertEquals(false, scan.next(results));
  }

  public void testDeleteColumn() throws IOException {
    KeyValue [] kvs = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", 10, KeyValue.Type.DeleteColumn, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 9, KeyValue.Type.Delete, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 8, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "b", 5, KeyValue.Type.Put, "dont-care")
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    StoreScanner scan = new StoreScanner(new Scan(), scanInfo, scanType, null,
        scanners);
    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(1, results.size());
    assertEquals(kvs[3], results.get(0));
  }

  private static final  KeyValue [] kvs = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "b", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "c", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "d", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "e", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "f", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "g", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "h", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "i", 11, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "a", 11, KeyValue.Type.Put, "dont-care"),
    };

  public void testSkipColumn() throws IOException {
    List<KeyValueScanner> scanners = scanFixture(kvs);
    StoreScanner scan = new StoreScanner(new Scan(), scanInfo, scanType,
        getCols("a", "d"), scanners);

    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scan.next(results));
    assertEquals(2, results.size());
    assertEquals(kvs[0], results.get(0));
    assertEquals(kvs[3], results.get(1));
    results.clear();

    assertEquals(true, scan.next(results));
    assertEquals(1, results.size());
    assertEquals(kvs[kvs.length-1], results.get(0));

    results.clear();
    assertEquals(false, scan.next(results));
  }

  /*
   * Test expiration of KeyValues in combination with a configured TTL for
   * a column family (as should be triggered in a major compaction).
   */
  public void testWildCardTtlScan() throws IOException {
    long now = System.currentTimeMillis();
    KeyValue [] kvs = new KeyValue[] {
        KeyValueTestUtil.create("R1", "cf", "a", now-1000, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "b", now-10, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "c", now-200, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "d", now-10000, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "a", now, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "b", now-10, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "c", now-200, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R2", "cf", "c", now-1000, KeyValue.Type.Put, "dont-care")
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    Scan scan = new Scan();
    scan.setMaxVersions(1);
    ScanInfo scanInfo = new ScanInfo(CONF, CF, 0, 1, 500, KeepDeletedCells.FALSE, 0,
        KeyValue.COMPARATOR);
    ScanType scanType = ScanType.USER_SCAN;
    StoreScanner scanner =
      new StoreScanner(scan, scanInfo, scanType,
          null, scanners);

    List<Cell> results = new ArrayList<Cell>();
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

  public void testScannerReseekDoesntNPE() throws Exception {
    List<KeyValueScanner> scanners = scanFixture(kvs);
    StoreScanner scan = new StoreScanner(new Scan(), scanInfo, scanType,
        getCols("a", "d"), scanners);

    // Previously a updateReaders twice in a row would cause an NPE.  In test this would also
    // normally cause an NPE because scan.store is null.  So as long as we get through these
    // two calls we are good and the bug was quashed.

    scan.updateReaders(Collections.EMPTY_LIST, Collections.EMPTY_LIST);

    scan.updateReaders(Collections.EMPTY_LIST, Collections.EMPTY_LIST);

    scan.peek();
  }


  /**
   * TODO this fails, since we don't handle deletions, etc, in peek
   */
  public void SKIP_testPeek() throws Exception {
    KeyValue [] kvs = new KeyValue [] {
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Put, "dont-care"),
        KeyValueTestUtil.create("R1", "cf", "a", 1, KeyValue.Type.Delete, "dont-care"),
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    Scan scanSpec = new Scan(Bytes.toBytes("R1"));
    StoreScanner scan = new StoreScanner(scanSpec, scanInfo, scanType,
        getCols("a"), scanners);
    assertNull(scan.peek());
  }

  /**
   * Ensure that expired delete family markers don't override valid puts
   */
  public void testExpiredDeleteFamily() throws Exception {
    long now = System.currentTimeMillis();
    KeyValue [] kvs = new KeyValue[] {
        new KeyValue(Bytes.toBytes("R1"), Bytes.toBytes("cf"), null, now-1000,
            KeyValue.Type.DeleteFamily),
        KeyValueTestUtil.create("R1", "cf", "a", now-10, KeyValue.Type.Put,
            "dont-care"),
    };
    List<KeyValueScanner> scanners = scanFixture(kvs);
    Scan scan = new Scan();
    scan.setMaxVersions(1);
    // scanner with ttl equal to 500
    ScanInfo scanInfo = new ScanInfo(CONF, CF, 0, 1, 500, KeepDeletedCells.FALSE, 0,
        KeyValue.COMPARATOR);
    ScanType scanType = ScanType.USER_SCAN;
    StoreScanner scanner =
        new StoreScanner(scan, scanInfo, scanType, null, scanners);

    List<Cell> results = new ArrayList<Cell>();
    assertEquals(true, scanner.next(results));
    assertEquals(1, results.size());
    assertEquals(kvs[1], results.get(0));
    results.clear();

    assertEquals(false, scanner.next(results));
  }

  public void testDeleteMarkerLongevity() throws Exception {
    try {
      final long now = System.currentTimeMillis();
      EnvironmentEdgeManagerTestHelper.injectEdge(new EnvironmentEdge() {
        public long currentTime() {
          return now;
        }
      });
      KeyValue[] kvs = new KeyValue[]{
        /*0*/ new KeyValue(Bytes.toBytes("R1"), Bytes.toBytes("cf"), null,
        now - 100, KeyValue.Type.DeleteFamily), // live
        /*1*/ new KeyValue(Bytes.toBytes("R1"), Bytes.toBytes("cf"), null,
        now - 1000, KeyValue.Type.DeleteFamily), // expired
        /*2*/ KeyValueTestUtil.create("R1", "cf", "a", now - 50,
        KeyValue.Type.Put, "v3"), // live
        /*3*/ KeyValueTestUtil.create("R1", "cf", "a", now - 55,
        KeyValue.Type.Delete, "dontcare"), // live
        /*4*/ KeyValueTestUtil.create("R1", "cf", "a", now - 55,
        KeyValue.Type.Put, "deleted-version v2"), // deleted
        /*5*/ KeyValueTestUtil.create("R1", "cf", "a", now - 60,
        KeyValue.Type.Put, "v1"), // live
        /*6*/ KeyValueTestUtil.create("R1", "cf", "a", now - 65,
        KeyValue.Type.Put, "v0"), // max-version reached
        /*7*/ KeyValueTestUtil.create("R1", "cf", "a",
        now - 100, KeyValue.Type.DeleteColumn, "dont-care"), // max-version
        /*8*/ KeyValueTestUtil.create("R1", "cf", "b", now - 600,
        KeyValue.Type.DeleteColumn, "dont-care"), //expired
        /*9*/ KeyValueTestUtil.create("R1", "cf", "b", now - 70,
        KeyValue.Type.Put, "v2"), //live
        /*10*/ KeyValueTestUtil.create("R1", "cf", "b", now - 750,
        KeyValue.Type.Put, "v1"), //expired
        /*11*/ KeyValueTestUtil.create("R1", "cf", "c", now - 500,
        KeyValue.Type.Delete, "dontcare"), //expired
        /*12*/ KeyValueTestUtil.create("R1", "cf", "c", now - 600,
        KeyValue.Type.Put, "v1"), //expired
        /*13*/ KeyValueTestUtil.create("R1", "cf", "c", now - 1000,
        KeyValue.Type.Delete, "dontcare"), //expired
        /*14*/ KeyValueTestUtil.create("R1", "cf", "d", now - 60,
        KeyValue.Type.Put, "expired put"), //live
        /*15*/ KeyValueTestUtil.create("R1", "cf", "d", now - 100,
        KeyValue.Type.Delete, "not-expired delete"), //live
      };
      List<KeyValueScanner> scanners = scanFixture(kvs);
      Scan scan = new Scan();
      scan.setMaxVersions(2);
      ScanInfo scanInfo = new ScanInfo(CONF, Bytes.toBytes("cf"),
        0 /* minVersions */,
        2 /* maxVersions */, 500 /* ttl */,
        KeepDeletedCells.FALSE /* keepDeletedCells */,
        200, /* timeToPurgeDeletes */
        KeyValue.COMPARATOR);
      StoreScanner scanner =
        new StoreScanner(scan, scanInfo,
          ScanType.COMPACT_DROP_DELETES, null, scanners,
          HConstants.OLDEST_TIMESTAMP);
      List<Cell> results = new ArrayList<Cell>();
      results = new ArrayList<Cell>();
      assertEquals(true, scanner.next(results));
      assertEquals(kvs[0], results.get(0));
      assertEquals(kvs[2], results.get(1));
      assertEquals(kvs[3], results.get(2));
      assertEquals(kvs[5], results.get(3));
      assertEquals(kvs[9], results.get(4));
      assertEquals(kvs[14], results.get(5));
      assertEquals(kvs[15], results.get(6));
      assertEquals(7, results.size());
      scanner.close();
    }finally{
    EnvironmentEdgeManagerTestHelper.reset();
    }
  }

}

