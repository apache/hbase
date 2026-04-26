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
package org.apache.hadoop.hbase.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Integration tests for HBASE-29974 Path 1: {@link Filter#getHintForRejectedRow(Cell)} allows the
 * scan pipeline to seek directly past rejected rows instead of iterating through every cell in each
 * rejected row one-by-one via {@code nextRow()}.
 * <p>
 * Each test verifies two properties simultaneously:
 * <ol>
 * <li><b>Correctness</b> — the scan returns exactly the rows that are not rejected by
 * {@link Filter#filterRowKey(Cell)}, regardless of whether the hint path or the legacy
 * cell-iteration path is used.</li>
 * <li><b>Efficiency</b> — when a filter provides a hint that jumps over all N rejected rows in one
 * seek, {@code getHintForRejectedRow} is called exactly once (not N times), proving that the
 * scanner did not fall back to cell-by-cell iteration for the skipped rows.</li>
 * </ol>
 */
@Category({ FilterTests.class, MediumTests.class })
public class TestFilterHintForRejectedRow {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFilterHintForRejectedRow.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final int CELLS_PER_ROW = 10;
  private static final byte[] VALUE = Bytes.toBytes("value");

  private HRegion region;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws Exception {
    TableDescriptor tableDescriptor =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
    RegionInfo info = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName()).build();
    this.region = HBaseTestingUtil.createRegionAndWAL(info, TEST_UTIL.getDataTestDir(),
      TEST_UTIL.getConfiguration(), tableDescriptor);
  }

  @After
  public void tearDown() throws Exception {
    HBaseTestingUtil.closeRegionAndWAL(this.region);
  }

  // -------------------------------------------------------------------------
  // Helper
  // -------------------------------------------------------------------------

  /**
   * Writes {@code count} rows into the test region. Row keys are formatted as {@code "row-XX"}
   * (zero-padded to two digits) and each row contains {@link #CELLS_PER_ROW} cells with qualifier
   * {@code "q-NN"}.
   * @param prefix string prefix used for both row keys and qualifier names
   * @param count  number of rows to write
   * @throws IOException if any put fails
   */
  private void writeRows(String prefix, int count) throws IOException {
    for (int i = 0; i < count; i++) {
      byte[] row = Bytes.toBytes(String.format("%s-%02d", prefix, i));
      Put p = new Put(row);
      p.setDurability(Durability.SKIP_WAL);
      for (int j = 0; j < CELLS_PER_ROW; j++) {
        p.addColumn(FAMILY, Bytes.toBytes(String.format("q-%02d", j)), VALUE);
      }
      this.region.put(p);
    }
    this.region.flush(true);
  }

  // -------------------------------------------------------------------------
  // Tests
  // -------------------------------------------------------------------------

  /**
   * HBASE-29974 Path 1 — single-batch seek hint.
   * <p>
   * The filter rejects the first 5 rows ({@code "row-00"} through {@code "row-04"}) via
   * {@link Filter#filterRowKey(Cell)} and, for every rejected row, returns a hint pointing directly
   * to {@code "row-05"} via {@link Filter#getHintForRejectedRow(Cell)}.
   * <p>
   * Expected behaviour:
   * <ul>
   * <li>The scanner seeks directly to {@code "row-05"} after the very first rejection, bypassing
   * cells in rows {@code "row-01"} through {@code "row-04"} entirely.</li>
   * <li>{@code getHintForRejectedRow} is called exactly once (not five times), confirming no
   * cell-by-cell iteration occurred for the skipped rows.</li>
   * <li>Rows {@code "row-05"} through {@code "row-09"} are returned with all their cells
   * intact.</li>
   * </ul>
   */
  @Test
  public void testHintJumpsOverAllRejectedRowsInOneSingleSeek() throws IOException {
    final String prefix = "hint-single";
    final int rejectedCount = 5;
    final int acceptedCount = 5;
    writeRows(prefix, rejectedCount + acceptedCount);

    final byte[] acceptedStartRow = Bytes.toBytes(String.format("%s-%02d", prefix, rejectedCount));
    final AtomicInteger hintCallCount = new AtomicInteger(0);

    FilterBase hintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        // Reject any row whose key is lexicographically less than acceptedStartRow.
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }

      /**
       * Returns a hint pointing directly to the first accepted row, regardless of which rejected
       * row triggered this call. Because the hint bypasses all remaining rejected rows in one seek,
       * this method should be invoked exactly once for the whole scan.
       */
      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        hintCallCount.incrementAndGet();
        return PrivateCellUtil.createFirstOnRow(acceptedStartRow);
      }
    };

    List<Cell> results = new ArrayList<>();
    try (RegionScanner scanner =
      region.getScanner(new Scan().addFamily(FAMILY).setFilter(hintFilter))) {
      List<Cell> rowCells = new ArrayList<>();
      boolean hasMore;
      do {
        hasMore = scanner.next(rowCells);
        results.addAll(rowCells);
        rowCells.clear();
      } while (hasMore);
    }

    // ----- Correctness assertions -----
    assertEquals("Scan must return exactly the cells from the " + acceptedCount + " accepted rows",
      acceptedCount * CELLS_PER_ROW, results.size());
    for (Cell c : results) {
      assertTrue("Every returned cell must belong to the accepted row range",
        Bytes.compareTo(c.getRowArray(), c.getRowOffset(), c.getRowLength(), acceptedStartRow, 0,
          acceptedStartRow.length) >= 0);
    }

    // ----- Efficiency assertion -----
    // The hint jumps over all 5 rejected rows in one seek, so the filter should be asked for
    // a hint exactly once — not once per rejected row.
    assertEquals(
      "getHintForRejectedRow must be called exactly once: the hint skips all rejected rows", 1,
      hintCallCount.get());
  }

  /**
   * HBASE-29974 Path 1 — backward compatibility: null hint falls through to {@code nextRow()}.
   * <p>
   * When {@link Filter#getHintForRejectedRow(Cell)} returns {@code null} (the default from
   * {@link FilterBase}), the scanner must fall back to the existing cell-by-cell {@code nextRow()}
   * behaviour and still return the correct results. This test acts as a regression guard to confirm
   * that the null-hint path does not alter observable scan results.
   */
  @Test
  public void testNullHintFallsThroughToLegacyNextRowBehaviour() throws IOException {
    final String prefix = "hint-null";
    final int rejectedCount = 3;
    final int acceptedCount = 3;
    writeRows(prefix, rejectedCount + acceptedCount);

    final byte[] acceptedStartRow = Bytes.toBytes(String.format("%s-%02d", prefix, rejectedCount));

    // This filter rejects rows but does NOT override getHintForRejectedRow (returns null).
    FilterBase noHintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }
    };

    List<Cell> results = new ArrayList<>();
    try (RegionScanner scanner =
      region.getScanner(new Scan().addFamily(FAMILY).setFilter(noHintFilter))) {
      List<Cell> rowCells = new ArrayList<>();
      boolean hasMore;
      do {
        hasMore = scanner.next(rowCells);
        results.addAll(rowCells);
        rowCells.clear();
      } while (hasMore);
    }

    assertEquals("Null-hint path must still return the correct cells from the accepted rows",
      acceptedCount * CELLS_PER_ROW, results.size());
    for (Cell c : results) {
      assertTrue("Every returned cell must belong to the accepted row range",
        Bytes.compareTo(c.getRowArray(), c.getRowOffset(), c.getRowLength(), acceptedStartRow, 0,
          acceptedStartRow.length) >= 0);
    }
  }

  /**
   * HBASE-29974 Path 1 — hint pointing beyond the last row terminates the scan cleanly.
   * <p>
   * If the filter's {@link Filter#getHintForRejectedRow(Cell)} returns a position that is past the
   * end of the table (beyond all written rows), the scan must complete without returning any
   * results and without throwing an exception.
   */
  @Test
  public void testHintBeyondLastRowTerminatesScanGracefully() throws IOException {
    final String prefix = "hint-beyond";
    writeRows(prefix, 5);

    // The hint points past "zzz", which is lexicographically after all "hint-beyond-XX" rows.
    final byte[] beyondAllRows = Bytes.toBytes("zzz-beyond-table-end");

    FilterBase beyondHintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return true; // reject every row
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return PrivateCellUtil.createFirstOnRow(beyondAllRows);
      }
    };

    List<Cell> results = new ArrayList<>();
    try (RegionScanner scanner =
      region.getScanner(new Scan().addFamily(FAMILY).setFilter(beyondHintFilter))) {
      List<Cell> rowCells = new ArrayList<>();
      boolean hasMore;
      do {
        hasMore = scanner.next(rowCells);
        results.addAll(rowCells);
        rowCells.clear();
      } while (hasMore);
    }

    assertTrue("When the hint is past the last row, no cells should be returned",
      results.isEmpty());
  }

  /**
   * HBASE-29974 Path 1 — hint for every rejected row (per-row hint stepping).
   * <p>
   * When the filter provides a hint that advances only one row at a time (i.e. it always points to
   * the immediate next row key), {@code getHintForRejectedRow} is called once per rejected row.
   * This verifies that the hint mechanism works correctly in the incremental-step case, not just
   * the bulk-jump case.
   */
  @Test
  public void testPerRowHintCalledOncePerRejectedRow() throws IOException {
    final String prefix = "hint-perrow";
    final int rejectedCount = 4;
    final int acceptedCount = 2;
    writeRows(prefix, rejectedCount + acceptedCount);

    final byte[] acceptedStartRow = Bytes.toBytes(String.format("%s-%02d", prefix, rejectedCount));
    final AtomicInteger hintCallCount = new AtomicInteger(0);

    FilterBase perRowHintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }

      /**
       * Returns a hint pointing to the cell immediately after the current one (one row at a time).
       * This causes the scanner to seek per-row, so this method is called once for each rejected
       * row.
       */
      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        hintCallCount.incrementAndGet();
        // Create a key that is one step past the current row key.
        return PrivateCellUtil.createFirstOnNextRow(firstRowCell);
      }
    };

    List<Cell> results = new ArrayList<>();
    try (RegionScanner scanner =
      region.getScanner(new Scan().addFamily(FAMILY).setFilter(perRowHintFilter))) {
      List<Cell> rowCells = new ArrayList<>();
      boolean hasMore;
      do {
        hasMore = scanner.next(rowCells);
        results.addAll(rowCells);
        rowCells.clear();
      } while (hasMore);
    }

    // ----- Correctness -----
    assertEquals("Scan must return exactly the cells from the " + acceptedCount + " accepted rows",
      acceptedCount * CELLS_PER_ROW, results.size());
    for (Cell c : results) {
      assertTrue("Every returned cell must belong to the accepted row range",
        Bytes.compareTo(c.getRowArray(), c.getRowOffset(), c.getRowLength(), acceptedStartRow, 0,
          acceptedStartRow.length) >= 0);
    }

    // ----- Hint call count -----
    // One call per rejected row: each per-row hint advances the scanner by exactly one row.
    assertEquals(
      "getHintForRejectedRow must be called once per rejected row in the per-row hint strategy",
      rejectedCount, hintCallCount.get());
  }
}
