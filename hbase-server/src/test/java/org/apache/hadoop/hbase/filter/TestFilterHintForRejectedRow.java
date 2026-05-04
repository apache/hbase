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
import org.apache.hadoop.hbase.CellUtil;
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

@Category({ FilterTests.class, MediumTests.class })
public class TestFilterHintForRejectedRow {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFilterHintForRejectedRow.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] FAMILY2 = Bytes.toBytes("g");
  private static final int CELLS_PER_ROW = 10;
  private static final byte[] VALUE = Bytes.toBytes("value");

  private HRegion region;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws Exception {
    TableDescriptor tableDescriptor =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY2)).build();
    RegionInfo info = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName()).build();
    this.region = HBaseTestingUtil.createRegionAndWAL(info, TEST_UTIL.getDataTestDir(),
      TEST_UTIL.getConfiguration(), tableDescriptor);
  }

  @After
  public void tearDown() throws Exception {
    HBaseTestingUtil.closeRegionAndWAL(this.region);
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

    assertEquals("Both paths must return the same number of cells", noHintResults.size(),
      hintResults.size());
    for (int i = 0; i < hintResults.size(); i++) {
      assertTrue("Cell mismatch at index " + i,
        CellUtil.equals(hintResults.get(i), noHintResults.get(i)));
    }
    assertEquals(acceptedCount * CELLS_PER_ROW, hintResults.size());
    assertEquals(
      "getHintForRejectedRow must be called exactly once: the hint skips all rejected rows", 1,
      hintCallCount.get());
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
    assertTrue("When the hint is past the last row, no cells should be returned",
      results.isEmpty());
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
      assertTrue("Every returned cell must belong to the accepted row range",
        Bytes.compareTo(c.getRowArray(), c.getRowOffset(), c.getRowLength(), acceptedStartRow, 0,
          acceptedStartRow.length) >= 0);
    }
    assertEquals(
      "getHintForRejectedRow must be called once per rejected row in the per-row hint strategy",
      rejectedCount, hintCallCount.get());
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

    assertEquals("Both paths must return the same number of cells", noHintResults.size(),
      hintResults.size());
    for (int i = 0; i < hintResults.size(); i++) {
      assertTrue("Cell mismatch at index " + i,
        CellUtil.equals(hintResults.get(i), noHintResults.get(i)));
    }
    assertEquals(5 * CELLS_PER_ROW, hintResults.size());
    assertEquals("getHintForRejectedRow must be called exactly once for reversed scan", 1,
      hintCallCount.get());
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

    assertEquals("Both paths must return the same number of cells", noHintResults.size(),
      hintResults.size());
    for (int i = 0; i < hintResults.size(); i++) {
      assertTrue("Cell mismatch at index " + i,
        CellUtil.equals(hintResults.get(i), noHintResults.get(i)));
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

    assertEquals("Backward hint must produce same results as no-hint path", noHintResults.size(),
      hintResults.size());
    for (int i = 0; i < hintResults.size(); i++) {
      assertTrue("Cell mismatch at index " + i,
        CellUtil.equals(hintResults.get(i), noHintResults.get(i)));
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

    assertEquals("Same-row hint must produce same results as no-hint path", noHintResults.size(),
      hintResults.size());
    for (int i = 0; i < hintResults.size(); i++) {
      assertTrue("Cell mismatch at index " + i,
        CellUtil.equals(hintResults.get(i), noHintResults.get(i)));
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

    assertEquals("Both paths must return the same number of cells", noHintResults.size(),
      hintResults.size());
    for (int i = 0; i < hintResults.size(); i++) {
      assertTrue("Cell mismatch at index " + i,
        CellUtil.equals(hintResults.get(i), noHintResults.get(i)));
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

    assertEquals("Filtered and unfiltered scans must return same cell count",
      unfilteredResults.size(), filteredResults.size());
    for (int i = 0; i < filteredResults.size(); i++) {
      assertTrue("Cell mismatch at index " + i,
        CellUtil.equals(filteredResults.get(i), unfilteredResults.get(i)));
    }
  }
}
