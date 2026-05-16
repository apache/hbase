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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(FilterTests.TAG)
@Tag(MediumTests.TAG)
public class TestFilterHintForRejectedRow {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] FAMILY2 = Bytes.toBytes("g");
  private static final int CELLS_PER_ROW = 10;
  private static final byte[] VALUE = Bytes.toBytes("value");

  private HRegion region;

  @BeforeEach
  public void setUp(TestInfo testInfo) throws Exception {
    TableDescriptor tableDescriptor =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(testInfo.getTestMethod().get().getName()))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY2)).build();
    RegionInfo info = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName()).build();
    this.region = HBaseTestingUtility.createRegionAndWAL(info, TEST_UTIL.getDataTestDir(),
      TEST_UTIL.getConfiguration(), tableDescriptor);
  }

  @AfterEach
  public void tearDown() throws Exception {
    HBaseTestingUtility.closeRegionAndWAL(this.region);
  }

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

  private void writeRowsMultiFamily(String prefix, int count) throws IOException {
    for (int i = 0; i < count; i++) {
      byte[] row = Bytes.toBytes(String.format("%s-%02d", prefix, i));
      Put p = new Put(row);
      p.setDurability(Durability.SKIP_WAL);
      for (int j = 0; j < CELLS_PER_ROW; j++) {
        byte[] qual = Bytes.toBytes(String.format("q-%02d", j));
        p.addColumn(FAMILY, qual, VALUE);
        p.addColumn(FAMILY2, qual, VALUE);
      }
      this.region.put(p);
    }
    this.region.flush(true);
  }

  private List<Cell> scanAll(Scan scan) throws IOException {
    List<Cell> results = new ArrayList<>();
    try (RegionScanner scanner = region.getScanner(scan)) {
      List<Cell> rowCells = new ArrayList<>();
      boolean hasMore;
      do {
        hasMore = scanner.next(rowCells);
        results.addAll(rowCells);
        rowCells.clear();
      } while (hasMore);
    }
    return results;
  }

  @Test
  public void testHintAndNoHintReturnSameCellsOnSameData() throws IOException {
    final String prefix = "row";
    final int rejectedCount = 5;
    final int acceptedCount = 5;
    writeRows(prefix, rejectedCount + acceptedCount);

    final byte[] acceptedStartRow = Bytes.toBytes(String.format("%s-%02d", prefix, rejectedCount));
    final AtomicInteger hintCallCount = new AtomicInteger(0);

    FilterBase hintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        hintCallCount.incrementAndGet();
        return PrivateCellUtil.createFirstOnRow(acceptedStartRow);
      }
    };

    FilterBase noHintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }
    };

    List<Cell> hintResults = scanAll(new Scan().addFamily(FAMILY).setFilter(hintFilter));
    List<Cell> noHintResults = scanAll(new Scan().addFamily(FAMILY).setFilter(noHintFilter));

    assertEquals(noHintResults.size(), hintResults.size(),
      "Both paths must return the same number of cells");
    for (int i = 0; i < hintResults.size(); i++) {
      assertTrue(CellUtil.equals(hintResults.get(i), noHintResults.get(i)),
        "Cell mismatch at index " + i);
    }
    assertEquals(acceptedCount * CELLS_PER_ROW, hintResults.size());
    assertEquals(1, hintCallCount.get(),
      "getHintForRejectedRow must be called exactly once: the hint skips all rejected rows");
  }

  @Test
  public void testHintBeyondLastRowTerminatesScanGracefully() throws IOException {
    final String prefix = "hint-beyond";
    writeRows(prefix, 5);

    final byte[] beyondAllRows = Bytes.toBytes("zzz-beyond-table-end");

    FilterBase beyondHintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return true;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return PrivateCellUtil.createFirstOnRow(beyondAllRows);
      }
    };

    List<Cell> results = scanAll(new Scan().addFamily(FAMILY).setFilter(beyondHintFilter));
    assertTrue(results.isEmpty(),
      "When the hint is past the last row, no cells should be returned");
  }

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

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        hintCallCount.incrementAndGet();
        return PrivateCellUtil.createFirstOnNextRow(firstRowCell);
      }
    };

    List<Cell> results = scanAll(new Scan().addFamily(FAMILY).setFilter(perRowHintFilter));

    assertEquals(acceptedCount * CELLS_PER_ROW, results.size());
    for (Cell c : results) {
      assertTrue(
        Bytes.compareTo(c.getRowArray(), c.getRowOffset(), c.getRowLength(), acceptedStartRow, 0,
          acceptedStartRow.length) >= 0,
        "Every returned cell must belong to the accepted row range");
    }
    assertEquals(rejectedCount, hintCallCount.get(),
      "getHintForRejectedRow must be called once per rejected row in the per-row hint strategy");
  }

  @Test
  public void testReversedScanWithHint() throws IOException {
    final String prefix = "row";
    final int totalRows = 10;
    writeRows(prefix, totalRows);

    // Accept rows 00-04 (lower half), reject rows 05-09 (upper half).
    // In a reversed scan, the scanner starts at row-09 and moves backward.
    final byte[] rejectThreshold = Bytes.toBytes(String.format("%s-%02d", prefix, 5));
    final byte[] hintTarget = Bytes.toBytes(String.format("%s-%02d", prefix, 4));
    final AtomicInteger hintCallCount = new AtomicInteger(0);

    FilterBase hintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          rejectThreshold, 0, rejectThreshold.length) >= 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        hintCallCount.incrementAndGet();
        // In reversed scan, hint must point backward (to a smaller row key).
        return PrivateCellUtil.createFirstOnRow(hintTarget);
      }
    };

    FilterBase noHintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          rejectThreshold, 0, rejectThreshold.length) >= 0;
      }
    };

    Scan reversedHintScan = new Scan().addFamily(FAMILY).setReversed(true).setFilter(hintFilter);
    Scan reversedNoHintScan =
      new Scan().addFamily(FAMILY).setReversed(true).setFilter(noHintFilter);

    List<Cell> hintResults = scanAll(reversedHintScan);
    List<Cell> noHintResults = scanAll(reversedNoHintScan);

    assertEquals(noHintResults.size(), hintResults.size(),
      "Both paths must return the same number of cells");
    for (int i = 0; i < hintResults.size(); i++) {
      assertTrue(CellUtil.equals(hintResults.get(i), noHintResults.get(i)),
        "Cell mismatch at index " + i);
    }
    assertEquals(5 * CELLS_PER_ROW, hintResults.size());
    assertEquals(1, hintCallCount.get(),
      "getHintForRejectedRow must be called exactly once for reversed scan");
  }

  @Test
  public void testGetSkipHintWithTimeRangeIntegration() throws IOException {
    final long insideTs = 2000;
    final long outsideTs = 500;
    final int rowCount = 5;

    for (int i = 0; i < rowCount; i++) {
      byte[] row = Bytes.toBytes(String.format("skiprow-%02d", i));
      Put p = new Put(row);
      p.setDurability(Durability.SKIP_WAL);
      p.addColumn(FAMILY, Bytes.toBytes("q"), insideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("q"), outsideTs, VALUE);
      region.put(p);
    }
    region.flush(true);

    final AtomicInteger skipHintCalls = new AtomicInteger(0);

    FilterBase skipHintFilter = new FilterBase() {
      @Override
      public Cell getSkipHint(Cell skippedCell) {
        skipHintCalls.incrementAndGet();
        return PrivateCellUtil.createFirstOnNextRow(skippedCell);
      }
    };

    Scan hintScan = new Scan().addFamily(FAMILY).setTimeRange(1000, 3000).setFilter(skipHintFilter);
    Scan noHintScan = new Scan().addFamily(FAMILY).setTimeRange(1000, 3000);

    List<Cell> hintResults = scanAll(hintScan);
    List<Cell> noHintResults = scanAll(noHintScan);

    assertEquals(noHintResults.size(), hintResults.size(),
      "Both paths must return the same number of cells");
    for (int i = 0; i < hintResults.size(); i++) {
      assertTrue(CellUtil.equals(hintResults.get(i), noHintResults.get(i)),
        "Cell mismatch at index " + i);
    }
    assertEquals(rowCount, hintResults.size());
  }

  @Test
  public void testBackwardHintFallsBackToNextRow() throws IOException {
    final String prefix = "row";
    writeRows(prefix, 5);

    final byte[] row00 = Bytes.toBytes(String.format("%s-%02d", prefix, 0));
    final byte[] acceptedStartRow = Bytes.toBytes(String.format("%s-%02d", prefix, 2));

    FilterBase backwardHintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return PrivateCellUtil.createFirstOnRow(row00);
      }
    };

    FilterBase noHintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }
    };

    List<Cell> hintResults = scanAll(new Scan().addFamily(FAMILY).setFilter(backwardHintFilter));
    List<Cell> noHintResults = scanAll(new Scan().addFamily(FAMILY).setFilter(noHintFilter));

    assertEquals(noHintResults.size(), hintResults.size(),
      "Backward hint must produce same results as no-hint path");
    for (int i = 0; i < hintResults.size(); i++) {
      assertTrue(CellUtil.equals(hintResults.get(i), noHintResults.get(i)),
        "Cell mismatch at index " + i);
    }
  }

  @Test
  public void testSameRowHintFallsBackToNextRow() throws IOException {
    final String prefix = "row";
    writeRows(prefix, 5);

    final byte[] acceptedStartRow = Bytes.toBytes(String.format("%s-%02d", prefix, 2));
    final AtomicInteger hintCallCount = new AtomicInteger(0);

    FilterBase sameRowHintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        hintCallCount.incrementAndGet();
        return PrivateCellUtil.createFirstOnRow(CellUtil.cloneRow(firstRowCell));
      }
    };

    FilterBase noHintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }
    };

    List<Cell> hintResults = scanAll(new Scan().addFamily(FAMILY).setFilter(sameRowHintFilter));
    List<Cell> noHintResults = scanAll(new Scan().addFamily(FAMILY).setFilter(noHintFilter));

    assertEquals(noHintResults.size(), hintResults.size(),
      "Same-row hint must produce same results as no-hint path");
    for (int i = 0; i < hintResults.size(); i++) {
      assertTrue(CellUtil.equals(hintResults.get(i), noHintResults.get(i)),
        "Cell mismatch at index " + i);
    }
    assertEquals(3 * CELLS_PER_ROW, hintResults.size());
  }

  @Test
  public void testCoprocessorHookCalledOncePerHintedRejection() throws IOException {
    final String prefix = "row";
    final int rejectedCount = 3;
    final int acceptedCount = 3;
    writeRows(prefix, rejectedCount + acceptedCount);

    final byte[] acceptedStartRow = Bytes.toBytes(String.format("%s-%02d", prefix, rejectedCount));
    final AtomicInteger hintCallCount = new AtomicInteger(0);

    FilterBase hintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        hintCallCount.incrementAndGet();
        return PrivateCellUtil.createFirstOnRow(acceptedStartRow);
      }
    };

    List<Cell> results = scanAll(new Scan().addFamily(FAMILY).setFilter(hintFilter));

    assertEquals(1, hintCallCount.get());
    assertEquals(acceptedCount * CELLS_PER_ROW, results.size());
  }

  @Test
  public void testMultiFamilyScanWithHint() throws IOException {
    final String prefix = "row";
    final int rejectedCount = 3;
    final int acceptedCount = 3;
    writeRowsMultiFamily(prefix, rejectedCount + acceptedCount);

    final byte[] acceptedStartRow = Bytes.toBytes(String.format("%s-%02d", prefix, rejectedCount));
    final AtomicInteger hintCallCount = new AtomicInteger(0);

    FilterBase hintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        hintCallCount.incrementAndGet();
        return PrivateCellUtil.createFirstOnRow(acceptedStartRow);
      }
    };

    FilterBase noHintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }
    };

    // Scan both families.
    Scan hintScan = new Scan().addFamily(FAMILY).addFamily(FAMILY2).setFilter(hintFilter);
    Scan noHintScan = new Scan().addFamily(FAMILY).addFamily(FAMILY2).setFilter(noHintFilter);

    List<Cell> hintResults = scanAll(hintScan);
    List<Cell> noHintResults = scanAll(noHintScan);

    assertEquals(noHintResults.size(), hintResults.size(),
      "Both paths must return the same number of cells");
    for (int i = 0; i < hintResults.size(); i++) {
      assertTrue(CellUtil.equals(hintResults.get(i), noHintResults.get(i)),
        "Cell mismatch at index " + i);
    }
    assertEquals(2 * CELLS_PER_ROW * acceptedCount, hintResults.size());
    assertEquals(1, hintCallCount.get());
  }

  @Test
  public void testDataCorrectnessVsUnfilteredScan() throws IOException {
    final String prefix = "row";
    final int totalRows = 8;
    writeRows(prefix, totalRows);

    final byte[] acceptedStartRow = Bytes.toBytes(String.format("%s-%02d", prefix, 3));

    FilterBase hintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return PrivateCellUtil.createFirstOnRow(acceptedStartRow);
      }
    };

    List<Cell> filteredResults = scanAll(new Scan().addFamily(FAMILY).setFilter(hintFilter));

    // Unfiltered scan starting at acceptedStartRow — should return the exact same cells.
    Scan unfilteredScan = new Scan().addFamily(FAMILY).withStartRow(acceptedStartRow);
    List<Cell> unfilteredResults = scanAll(unfilteredScan);

    assertEquals(unfilteredResults.size(), filteredResults.size(),
      "Filtered and unfiltered scans must return same cell count");
    for (int i = 0; i < filteredResults.size(); i++) {
      assertTrue(CellUtil.equals(filteredResults.get(i), unfilteredResults.get(i)),
        "Cell mismatch at index " + i);
    }
  }

  @Test
  public void testHintOvershootingStopRowTerminatesGracefully() throws IOException {
    final String prefix = "row";
    final int totalRows = 10;
    writeRows(prefix, totalRows);

    // Scan rows 00-06 (stopRow=row-07, exclusive). Reject rows 00-02, hint to row-09.
    // The hint overshoots the stop row, so the scan should terminate with no results
    // from the rejected range and only include rows 03-06 from the non-rejected range.
    final byte[] stopRow = Bytes.toBytes(String.format("%s-%02d", prefix, 7));
    final byte[] acceptedStartRow = Bytes.toBytes(String.format("%s-%02d", prefix, 3));
    final byte[] hintTarget = Bytes.toBytes(String.format("%s-%02d", prefix, 9));

    FilterBase hintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        // Hint past the stop row — scanner should terminate gracefully.
        return PrivateCellUtil.createFirstOnRow(hintTarget);
      }
    };

    Scan scan = new Scan().addFamily(FAMILY).withStopRow(stopRow).setFilter(hintFilter);
    List<Cell> results = scanAll(scan);

    // The hint jumps past stopRow, so no cells should be returned.
    assertTrue(results.isEmpty(), "Hint past stop row must terminate scan with no results");
  }

  @Test
  public void testHintWithinStopRowReturnsCorrectResults() throws IOException {
    final String prefix = "row";
    final int totalRows = 10;
    writeRows(prefix, totalRows);

    // Scan rows 00-06 (stopRow=row-07, exclusive). Reject rows 00-02, hint to row-03.
    final byte[] stopRow = Bytes.toBytes(String.format("%s-%02d", prefix, 7));
    final byte[] acceptedStartRow = Bytes.toBytes(String.format("%s-%02d", prefix, 3));

    FilterBase hintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return PrivateCellUtil.createFirstOnRow(acceptedStartRow);
      }
    };

    Scan hintScan = new Scan().addFamily(FAMILY).withStopRow(stopRow).setFilter(hintFilter);
    List<Cell> hintResults = scanAll(hintScan);

    // Should get rows 03-06 (4 rows).
    Scan baselineScan =
      new Scan().addFamily(FAMILY).withStartRow(acceptedStartRow).withStopRow(stopRow);
    List<Cell> baselineResults = scanAll(baselineScan);

    assertEquals(baselineResults.size(), hintResults.size(),
      "Hint within stop row must return same results as baseline");
    for (int i = 0; i < hintResults.size(); i++) {
      assertTrue(CellUtil.equals(hintResults.get(i), baselineResults.get(i)),
        "Cell mismatch at index " + i);
    }
    assertEquals(4 * CELLS_PER_ROW, hintResults.size());
  }

  @Test
  public void testGetSkipHintCalledForReversedScan() throws IOException {
    final long insideTs = 1500;
    final long tooNewTs = 5000;
    final int rowCount = 5;
    final byte[] qInside = Bytes.toBytes("q-inside");
    final byte[] qNew = Bytes.toBytes("q-new");

    // Write two qualifiers per row: "q-inside" at ts=1500 (within range) and "q-new" at
    // ts=5000 (above the range upper bound). The time-range gate fires for the too-new cell
    // before version tracking can absorb it, ensuring getSkipHint is consulted.
    for (int i = 0; i < rowCount; i++) {
      byte[] row = Bytes.toBytes(String.format("skiprev-%02d", i));
      Put p = new Put(row);
      p.setDurability(Durability.SKIP_WAL);
      p.addColumn(FAMILY, qInside, insideTs, VALUE);
      p.addColumn(FAMILY, qNew, tooNewTs, VALUE);
      region.put(p);
    }
    region.flush(true);

    final AtomicInteger skipHintCalls = new AtomicInteger(0);

    // Return null to verify the code path is reached without altering scan semantics.
    FilterBase skipHintFilter = new FilterBase() {
      @Override
      public Cell getSkipHint(Cell skippedCell) {
        skipHintCalls.incrementAndGet();
        return null;
      }
    };

    // Reversed scan with time range [1000, 3000): insideTs cells pass, tooNewTs cells are
    // structurally skipped (tsCmp > 0). The skip hint should be consulted.
    Scan hintScan = new Scan().addFamily(FAMILY).setTimeRange(1000, 3000).setReversed(true)
      .setFilter(skipHintFilter);
    Scan noHintScan = new Scan().addFamily(FAMILY).setTimeRange(1000, 3000).setReversed(true);

    List<Cell> hintResults = scanAll(hintScan);
    List<Cell> noHintResults = scanAll(noHintScan);

    assertEquals(noHintResults.size(), hintResults.size(),
      "Both paths must return the same number of cells");
    for (int i = 0; i < hintResults.size(); i++) {
      assertTrue(CellUtil.equals(hintResults.get(i), noHintResults.get(i)),
        "Cell mismatch at index " + i);
    }
    assertEquals(rowCount, hintResults.size());
    assertTrue(skipHintCalls.get() > 0,
      "getSkipHint must be called at least once for reversed scan");
  }

  // ---- FilterList AND hint delegation integration tests ----

  @Test
  public void testFilterListANDHintDelegation() throws IOException {
    final String prefix = "row";
    final int rejectedCount = 5;
    final int acceptedCount = 5;
    writeRows(prefix, rejectedCount + acceptedCount);

    final byte[] acceptedStartRow = Bytes.toBytes(String.format("%s-%02d", prefix, rejectedCount));
    final AtomicInteger hintCallsA = new AtomicInteger(0);
    final AtomicInteger hintCallsB = new AtomicInteger(0);

    // Both filters reject the same rows; both provide hints to the accepted start.
    // AND merging takes max — both point to the same target here.
    FilterBase filterA = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        hintCallsA.incrementAndGet();
        return PrivateCellUtil.createFirstOnRow(acceptedStartRow);
      }
    };

    FilterBase filterB = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        hintCallsB.incrementAndGet();
        return PrivateCellUtil.createFirstOnRow(acceptedStartRow);
      }
    };

    FilterList andFilter =
      new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(filterA, filterB));

    FilterBase noHintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }
    };

    List<Cell> hintResults = scanAll(new Scan().addFamily(FAMILY).setFilter(andFilter));
    List<Cell> noHintResults = scanAll(new Scan().addFamily(FAMILY).setFilter(noHintFilter));

    assertEquals(noHintResults.size(), hintResults.size(),
      "AND FilterList with hints must return same cells as no-hint path");
    for (int i = 0; i < hintResults.size(); i++) {
      assertTrue(CellUtil.equals(hintResults.get(i), noHintResults.get(i)),
        "Cell mismatch at index " + i);
    }
    assertEquals(acceptedCount * CELLS_PER_ROW, hintResults.size());
    assertTrue(hintCallsA.get() > 0, "Sub-filter A hint must be consulted");
    assertTrue(hintCallsB.get() > 0, "Sub-filter B hint must be consulted");
  }

  @Test
  public void testFilterListORHintDelegation() throws IOException {
    final String prefix = "row";
    final int rejectedCount = 5;
    final int acceptedCount = 5;
    writeRows(prefix, rejectedCount + acceptedCount);

    final byte[] acceptedStartRow = Bytes.toBytes(String.format("%s-%02d", prefix, rejectedCount));

    // Both filters reject the same rows, OR requires ALL to reject.
    // Both provide hints to the accepted start. OR merging takes min — same target.
    FilterBase filterA = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return PrivateCellUtil.createFirstOnRow(acceptedStartRow);
      }
    };

    FilterBase filterB = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return PrivateCellUtil.createFirstOnRow(acceptedStartRow);
      }
    };

    FilterList orFilter =
      new FilterList(FilterList.Operator.MUST_PASS_ONE, Arrays.asList(filterA, filterB));

    FilterBase noHintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }
    };

    List<Cell> hintResults = scanAll(new Scan().addFamily(FAMILY).setFilter(orFilter));
    List<Cell> noHintResults = scanAll(new Scan().addFamily(FAMILY).setFilter(noHintFilter));

    assertEquals(noHintResults.size(), hintResults.size(),
      "OR FilterList with hints must return same cells as no-hint path");
    for (int i = 0; i < hintResults.size(); i++) {
      assertTrue(CellUtil.equals(hintResults.get(i), noHintResults.get(i)),
        "Cell mismatch at index " + i);
    }
    assertEquals(acceptedCount * CELLS_PER_ROW, hintResults.size());
  }

  @Test
  public void testFilterListANDWithOneNullHintSubFilter() throws IOException {
    final String prefix = "row";
    final int rejectedCount = 3;
    final int acceptedCount = 3;
    writeRows(prefix, rejectedCount + acceptedCount);

    final byte[] acceptedStartRow = Bytes.toBytes(String.format("%s-%02d", prefix, rejectedCount));
    final AtomicInteger hintCalls = new AtomicInteger(0);

    // One sub-filter provides a hint, the other returns null.
    // AND ignores nulls, so the non-null hint should be used.
    FilterBase hintProvider = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        hintCalls.incrementAndGet();
        return PrivateCellUtil.createFirstOnRow(acceptedStartRow);
      }
    };

    FilterBase noHintProvider = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }
    };

    FilterList andFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL,
      Arrays.asList(hintProvider, noHintProvider));

    List<Cell> results = scanAll(new Scan().addFamily(FAMILY).setFilter(andFilter));
    assertEquals(acceptedCount * CELLS_PER_ROW, results.size());
    assertEquals(1, hintCalls.get(),
      "Hint provider must be called; AND ignores the null from the other sub-filter");
  }

  @Test
  public void testFilterListORWithOneNullHintSubFilter() throws IOException {
    final String prefix = "row";
    final int rejectedCount = 3;
    final int acceptedCount = 3;
    writeRows(prefix, rejectedCount + acceptedCount);

    final byte[] acceptedStartRow = Bytes.toBytes(String.format("%s-%02d", prefix, rejectedCount));

    // One sub-filter provides a hint, the other returns null.
    // OR returns null if ANY sub-filter returns null, so no hint optimization.
    FilterBase hintProvider = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return PrivateCellUtil.createFirstOnRow(acceptedStartRow);
      }
    };

    FilterBase noHintProvider = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }
    };

    FilterList orFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE,
      Arrays.asList(hintProvider, noHintProvider));

    FilterBase baseline = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }
    };

    List<Cell> orResults = scanAll(new Scan().addFamily(FAMILY).setFilter(orFilter));
    List<Cell> baselineResults = scanAll(new Scan().addFamily(FAMILY).setFilter(baseline));

    assertEquals(baselineResults.size(), orResults.size(),
      "OR with one null hint must still return correct results (falls back to no-hint path)");
    for (int i = 0; i < orResults.size(); i++) {
      assertTrue(CellUtil.equals(orResults.get(i), baselineResults.get(i)),
        "Cell mismatch at index " + i);
    }
  }

  @Test
  public void testNestedFilterListHintDelegation() throws IOException {
    final String prefix = "row";
    final int rejectedCount = 4;
    final int acceptedCount = 4;
    writeRows(prefix, rejectedCount + acceptedCount);

    final byte[] acceptedStartRow = Bytes.toBytes(String.format("%s-%02d", prefix, rejectedCount));

    // Nested: AND(OR(hintA, hintB), hintC)
    // All filters reject the same rows and hint to the same target.
    FilterBase hintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return PrivateCellUtil.createFirstOnRow(acceptedStartRow);
      }
    };

    FilterBase hintFilter2 = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return PrivateCellUtil.createFirstOnRow(acceptedStartRow);
      }
    };

    FilterBase hintFilter3 = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return PrivateCellUtil.createFirstOnRow(acceptedStartRow);
      }
    };

    FilterList innerOR =
      new FilterList(FilterList.Operator.MUST_PASS_ONE, Arrays.asList(hintFilter, hintFilter2));
    FilterList outerAND =
      new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(innerOR, hintFilter3));

    FilterBase baseline = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }
    };

    List<Cell> nestedResults = scanAll(new Scan().addFamily(FAMILY).setFilter(outerAND));
    List<Cell> baselineResults = scanAll(new Scan().addFamily(FAMILY).setFilter(baseline));

    assertEquals(baselineResults.size(), nestedResults.size(),
      "Nested FilterList must return same results as baseline");
    for (int i = 0; i < nestedResults.size(); i++) {
      assertTrue(CellUtil.equals(nestedResults.get(i), baselineResults.get(i)),
        "Cell mismatch at index " + i);
    }
    assertEquals(acceptedCount * CELLS_PER_ROW, nestedResults.size());
  }

  @Test
  public void testWhileMatchFilterHintDelegation() throws IOException {
    final String prefix = "row";
    final int rejectedCount = 3;
    final int acceptedCount = 3;
    writeRows(prefix, rejectedCount + acceptedCount);

    final byte[] acceptedStartRow = Bytes.toBytes(String.format("%s-%02d", prefix, rejectedCount));
    final AtomicInteger hintCalls = new AtomicInteger(0);

    FilterBase innerHintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          acceptedStartRow, 0, acceptedStartRow.length) < 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        hintCalls.incrementAndGet();
        return PrivateCellUtil.createFirstOnRow(acceptedStartRow);
      }
    };

    WhileMatchFilter wmFilter = new WhileMatchFilter(innerHintFilter);

    // WhileMatchFilter delegates filterRowKey and sets filterAllRemaining on first true.
    // The scanner checks isFilterDoneInternal() BEFORE calling getHintForRejectedRow,
    // so the hint path is short-circuited and the scan terminates immediately.
    List<Cell> results = scanAll(new Scan().addFamily(FAMILY).setFilter(wmFilter));

    assertTrue(results.isEmpty(), "WhileMatchFilter terminates scan on first rejection");
    assertEquals(0, hintCalls.get(),
      "WhileMatchFilter sets filterAllRemaining before getHintForRejectedRow is consulted");
  }

  @Test
  public void testFilterListANDReversedScanHint() throws IOException {
    final String prefix = "row";
    final int totalRows = 10;
    writeRows(prefix, totalRows);

    // Accept rows 00-04, reject rows 05-09.
    // In reversed scan, scanner starts at row-09 and moves backward.
    final byte[] rejectThreshold = Bytes.toBytes(String.format("%s-%02d", prefix, 5));
    final byte[] hintTarget = Bytes.toBytes(String.format("%s-%02d", prefix, 4));

    FilterBase rejectFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          rejectThreshold, 0, rejectThreshold.length) >= 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return PrivateCellUtil.createFirstOnRow(hintTarget);
      }
    };

    FilterList andFilter =
      new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(rejectFilter));

    FilterBase noHintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          rejectThreshold, 0, rejectThreshold.length) >= 0;
      }
    };

    Scan hintScan = new Scan().addFamily(FAMILY).setReversed(true).setFilter(andFilter);
    Scan noHintScan = new Scan().addFamily(FAMILY).setReversed(true).setFilter(noHintFilter);

    List<Cell> hintResults = scanAll(hintScan);
    List<Cell> noHintResults = scanAll(noHintScan);

    assertEquals(noHintResults.size(), hintResults.size(),
      "Reversed AND FilterList must return same cells");
    for (int i = 0; i < hintResults.size(); i++) {
      assertTrue(CellUtil.equals(hintResults.get(i), noHintResults.get(i)),
        "Cell mismatch at index " + i);
    }
    assertEquals(5 * CELLS_PER_ROW, hintResults.size());
  }

  @Test
  public void testFilterListORReversedScanHint() throws IOException {
    final String prefix = "row";
    final int totalRows = 10;
    writeRows(prefix, totalRows);

    final byte[] rejectThreshold = Bytes.toBytes(String.format("%s-%02d", prefix, 5));
    final byte[] hintTarget = Bytes.toBytes(String.format("%s-%02d", prefix, 4));

    FilterBase rejectFilterA = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          rejectThreshold, 0, rejectThreshold.length) >= 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return PrivateCellUtil.createFirstOnRow(hintTarget);
      }
    };

    FilterBase rejectFilterB = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          rejectThreshold, 0, rejectThreshold.length) >= 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return PrivateCellUtil.createFirstOnRow(hintTarget);
      }
    };

    FilterList orFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE,
      Arrays.asList(rejectFilterA, rejectFilterB));

    FilterBase noHintFilter = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          rejectThreshold, 0, rejectThreshold.length) >= 0;
      }
    };

    Scan hintScan = new Scan().addFamily(FAMILY).setReversed(true).setFilter(orFilter);
    Scan noHintScan = new Scan().addFamily(FAMILY).setReversed(true).setFilter(noHintFilter);

    List<Cell> hintResults = scanAll(hintScan);
    List<Cell> noHintResults = scanAll(noHintScan);

    assertEquals(noHintResults.size(), hintResults.size(),
      "Reversed OR FilterList must return same cells");
    for (int i = 0; i < hintResults.size(); i++) {
      assertTrue(CellUtil.equals(hintResults.get(i), noHintResults.get(i)),
        "Cell mismatch at index " + i);
    }
    assertEquals(5 * CELLS_PER_ROW, hintResults.size());
  }

  @Test
  public void testColumnRangeFilterGetSkipHintIntegration() throws IOException {
    final long insideTs = 2000;
    final long outsideTs = 500;
    final int rowCount = 5;

    for (int i = 0; i < rowCount; i++) {
      byte[] row = Bytes.toBytes(String.format("colrange-%02d", i));
      Put p = new Put(row);
      p.setDurability(Durability.SKIP_WAL);
      // Qualifiers: "a", "b", "c", "d" — ColumnRangeFilter will select "b" to "c".
      p.addColumn(FAMILY, Bytes.toBytes("a"), insideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("a"), outsideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("b"), insideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("b"), outsideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("c"), insideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("c"), outsideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("d"), insideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("d"), outsideTs, VALUE);
      region.put(p);
    }
    region.flush(true);

    ColumnRangeFilter colFilter =
      new ColumnRangeFilter(Bytes.toBytes("b"), true, Bytes.toBytes("c"), true);

    // Baseline: same column range logic via filterCell, but no getSkipHint override.
    FilterBase noHintBaseline = new FilterBase() {
      private final ColumnRangeFilter delegate =
        new ColumnRangeFilter(Bytes.toBytes("b"), true, Bytes.toBytes("c"), true);

      @Override
      public ReturnCode filterCell(Cell c) throws IOException {
        return delegate.filterCell(c);
      }
    };

    // Time range [1000, 3000): insideTs cells pass, outsideTs cells hit the time-range gate.
    // ColumnRangeFilter.getSkipHint() should be consulted for structurally skipped cells.
    Scan hintScan = new Scan().addFamily(FAMILY).setTimeRange(1000, 3000).setFilter(colFilter);
    Scan noHintScan =
      new Scan().addFamily(FAMILY).setTimeRange(1000, 3000).setFilter(noHintBaseline);

    List<Cell> hintResults = scanAll(hintScan);
    List<Cell> noHintResults = scanAll(noHintScan);

    assertEquals(noHintResults.size(), hintResults.size(),
      "Hint-aware scan must return same cells as no-hint baseline");
    // Should get "b" and "c" qualifiers for each row, only insideTs versions.
    assertEquals(rowCount * 2, hintResults.size());
  }

  @Test
  public void testColumnPrefixFilterGetSkipHintIntegration() throws IOException {
    final long insideTs = 2000;
    final long outsideTs = 500;
    final int rowCount = 5;

    for (int i = 0; i < rowCount; i++) {
      byte[] row = Bytes.toBytes(String.format("colpfx-%02d", i));
      Put p = new Put(row);
      p.setDurability(Durability.SKIP_WAL);
      // Qualifiers: "aaa", "abc", "abd", "xyz"
      p.addColumn(FAMILY, Bytes.toBytes("aaa"), insideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("aaa"), outsideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("abc"), insideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("abc"), outsideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("abd"), insideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("abd"), outsideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("xyz"), insideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("xyz"), outsideTs, VALUE);
      region.put(p);
    }
    region.flush(true);

    // ColumnPrefixFilter with prefix "ab" should match "abc" and "abd".
    ColumnPrefixFilter prefixFilter = new ColumnPrefixFilter(Bytes.toBytes("ab"));

    // Baseline: same prefix logic via filterCell, but no getSkipHint override.
    FilterBase noHintBaseline = new FilterBase() {
      private final ColumnPrefixFilter delegate = new ColumnPrefixFilter(Bytes.toBytes("ab"));

      @Override
      public ReturnCode filterCell(Cell c) throws IOException {
        return delegate.filterCell(c);
      }
    };

    Scan hintScan = new Scan().addFamily(FAMILY).setTimeRange(1000, 3000).setFilter(prefixFilter);
    Scan noHintScan =
      new Scan().addFamily(FAMILY).setTimeRange(1000, 3000).setFilter(noHintBaseline);

    List<Cell> hintResults = scanAll(hintScan);
    List<Cell> noHintResults = scanAll(noHintScan);

    assertEquals(noHintResults.size(), hintResults.size(),
      "Hint-aware scan must return same cells as no-hint baseline");
    // Should get "abc" and "abd" qualifiers for each row, only insideTs versions.
    assertEquals(rowCount * 2, hintResults.size());
  }

  @Test
  public void testMultipleColumnPrefixFilterGetSkipHintIntegration() throws IOException {
    final long insideTs = 2000;
    final long outsideTs = 500;
    final int rowCount = 5;

    for (int i = 0; i < rowCount; i++) {
      byte[] row = Bytes.toBytes(String.format("mcpfx-%02d", i));
      Put p = new Put(row);
      p.setDurability(Durability.SKIP_WAL);
      // Qualifiers: "aaa", "abc", "abd", "bbb", "xyz"
      p.addColumn(FAMILY, Bytes.toBytes("aaa"), insideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("aaa"), outsideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("abc"), insideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("abc"), outsideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("abd"), insideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("abd"), outsideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("bbb"), insideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("bbb"), outsideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("xyz"), insideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("xyz"), outsideTs, VALUE);
      region.put(p);
    }
    region.flush(true);

    // MultipleColumnPrefixFilter with prefixes "ab" and "bb" should match "abc", "abd", "bbb".
    MultipleColumnPrefixFilter mcpFilter =
      new MultipleColumnPrefixFilter(new byte[][] { Bytes.toBytes("ab"), Bytes.toBytes("bb") });

    // Baseline: same prefix logic via filterCell, but no getSkipHint override.
    FilterBase noHintBaseline = new FilterBase() {
      private final MultipleColumnPrefixFilter delegate =
        new MultipleColumnPrefixFilter(new byte[][] { Bytes.toBytes("ab"), Bytes.toBytes("bb") });

      @Override
      public ReturnCode filterCell(Cell c) throws IOException {
        return delegate.filterCell(c);
      }
    };

    Scan hintScan = new Scan().addFamily(FAMILY).setTimeRange(1000, 3000).setFilter(mcpFilter);
    Scan noHintScan =
      new Scan().addFamily(FAMILY).setTimeRange(1000, 3000).setFilter(noHintBaseline);

    List<Cell> hintResults = scanAll(hintScan);
    List<Cell> noHintResults = scanAll(noHintScan);

    assertEquals(noHintResults.size(), hintResults.size(),
      "Hint-aware scan must return same cells as no-hint baseline");
    // Should get "abc", "abd", "bbb" qualifiers for each row, only insideTs versions.
    assertEquals(rowCount * 3, hintResults.size());
  }

  @Test
  public void testFilterListANDGetSkipHintComposition() throws IOException {
    final long insideTs = 2000;
    final long outsideTs = 500;
    final int rowCount = 5;

    for (int i = 0; i < rowCount; i++) {
      byte[] row = Bytes.toBytes(String.format("composed-%02d", i));
      Put p = new Put(row);
      p.setDurability(Durability.SKIP_WAL);
      p.addColumn(FAMILY, Bytes.toBytes("a"), insideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("a"), outsideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("b"), insideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("b"), outsideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("c"), insideTs, VALUE);
      p.addColumn(FAMILY, Bytes.toBytes("c"), outsideTs, VALUE);
      region.put(p);
    }
    region.flush(true);

    // Compose ColumnRangeFilter("b","c") AND a custom skip-hint filter.
    // The AND composition should take the max hint.
    final AtomicInteger skipHintCalls = new AtomicInteger(0);
    ColumnRangeFilter colRange =
      new ColumnRangeFilter(Bytes.toBytes("b"), true, Bytes.toBytes("c"), true);
    FilterBase customSkipHint = new FilterBase() {
      @Override
      public Cell getSkipHint(Cell skippedCell) {
        skipHintCalls.incrementAndGet();
        return PrivateCellUtil.createFirstOnNextRow(skippedCell);
      }
    };

    FilterList andFilter =
      new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(colRange, customSkipHint));

    // Baseline: same column range via filterCell, no getSkipHint.
    FilterBase noHintBaseline = new FilterBase() {
      private final ColumnRangeFilter delegate =
        new ColumnRangeFilter(Bytes.toBytes("b"), true, Bytes.toBytes("c"), true);

      @Override
      public ReturnCode filterCell(Cell c) throws IOException {
        return delegate.filterCell(c);
      }
    };

    Scan hintScan = new Scan().addFamily(FAMILY).setTimeRange(1000, 3000).setFilter(andFilter);
    Scan noHintScan =
      new Scan().addFamily(FAMILY).setTimeRange(1000, 3000).setFilter(noHintBaseline);

    List<Cell> hintResults = scanAll(hintScan);
    List<Cell> noHintResults = scanAll(noHintScan);

    assertEquals(noHintResults.size(), hintResults.size(),
      "AND composed skip-hint must return same cells as no-hint baseline");
    // Should get "b" and "c" for each row, only insideTs.
    assertEquals(rowCount * 2, hintResults.size());
  }

  @Test
  public void testFilterListANDDivergentHints() throws IOException {
    final String prefix = "row";
    final int totalRows = 10;
    writeRows(prefix, totalRows);

    // Filter A rejects rows 0-4, hints to row-03 (a conservative hint).
    // Filter B rejects rows 0-6, hints to row-07 (a more aggressive hint).
    // Both reject rows 0-4 (overlap). AND merges => takes max => row-07.
    // Rows 0-6 are rejected by the composite (at least one rejects each).
    // Scan should return rows 07-09.
    final byte[] rejectThresholdA = Bytes.toBytes(String.format("%s-%02d", prefix, 5));
    final byte[] hintTargetA = Bytes.toBytes(String.format("%s-%02d", prefix, 3));
    final byte[] rejectThresholdB = Bytes.toBytes(String.format("%s-%02d", prefix, 7));
    final byte[] hintTargetB = Bytes.toBytes(String.format("%s-%02d", prefix, 7));

    FilterBase filterA = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          rejectThresholdA, 0, rejectThresholdA.length) < 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return PrivateCellUtil.createFirstOnRow(hintTargetA);
      }
    };

    FilterBase filterB = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          rejectThresholdB, 0, rejectThresholdB.length) < 0;
      }

      @Override
      public Cell getHintForRejectedRow(Cell firstRowCell) {
        return PrivateCellUtil.createFirstOnRow(hintTargetB);
      }
    };

    FilterList andFilter =
      new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(filterA, filterB));

    FilterBase baseline = new FilterBase() {
      @Override
      public boolean filterRowKey(Cell cell) {
        return Bytes.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          rejectThresholdB, 0, rejectThresholdB.length) < 0;
      }
    };

    List<Cell> hintResults = scanAll(new Scan().addFamily(FAMILY).setFilter(andFilter));
    List<Cell> baselineResults = scanAll(new Scan().addFamily(FAMILY).setFilter(baseline));

    assertEquals(baselineResults.size(), hintResults.size(),
      "AND with divergent hints must return same cells as baseline");
    for (int i = 0; i < hintResults.size(); i++) {
      assertTrue(CellUtil.equals(hintResults.get(i), baselineResults.get(i)),
        "Cell mismatch at index " + i);
    }
    assertEquals(3 * CELLS_PER_ROW, hintResults.size());
  }
}
