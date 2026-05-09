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
}
