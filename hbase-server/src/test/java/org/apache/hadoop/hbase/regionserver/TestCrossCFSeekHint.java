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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test that SEEK_NEXT_USING_HINT with a hint pointing to a different column family does not degrade
 * into cell-by-cell traversal.
 * <p>
 * HBASE-28902: StoreScanner uses InnerStoreCellComparator which compares column families by length
 * only (an optimization valid within a single store). When the filter hint points to a different
 * CF, this comparison can produce the wrong result, causing the seek to be skipped and falling back
 * to heap.next() for every cell.
 */
@Tag(RegionServerTests.TAG)
@Tag(MediumTests.TAG)
public class TestCrossCFSeekHint {

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final byte[] ROW1 = Bytes.toBytes("row1");
  private static final byte[] ROW2 = Bytes.toBytes("row2");
  private static final byte[] QUAL = Bytes.toBytes("q");
  private static final int NUM_CELLS = 100;

  // Two CFs with different lengths: "aa" sorts first, "b" sorts second.
  private static final byte[] CF_FIRST = Bytes.toBytes("aa");
  private static final byte[] CF_SECOND = Bytes.toBytes("b");

  // Two CFs with same length: "aa" sorts first, "bb" sorts second.
  private static final byte[] CF_SECOND_SAME_LEN = Bytes.toBytes("bb");

  static Stream<Arguments> params() {
    return Stream.of(
      // Forward scan: data in "aa" (first), hint to "b" (second). Different lengths.
      Arguments.of(CF_FIRST, CF_SECOND, true, false),
      Arguments.of(CF_FIRST, CF_SECOND, false, false),
      // Forward scan: data in "aa" (first), hint to "bb" (second). Same lengths.
      Arguments.of(CF_FIRST, CF_SECOND_SAME_LEN, true, false),
      Arguments.of(CF_FIRST, CF_SECOND_SAME_LEN, false, false),
      // Reverse scan: data in "b" (second), hint to "aa" (first). Different lengths.
      Arguments.of(CF_SECOND, CF_FIRST, true, true), Arguments.of(CF_SECOND, CF_FIRST, false, true),
      // Reverse scan: data in "bb" (second), hint to "aa" (first). Same lengths.
      Arguments.of(CF_SECOND_SAME_LEN, CF_FIRST, true, true),
      Arguments.of(CF_SECOND_SAME_LEN, CF_FIRST, false, true));
  }

  /**
   * Filter that INCLUDEs cells from the data family up to a threshold qualifier, then returns
   * SEEK_NEXT_USING_HINT with a hint pointing to the target family. All target family cells are
   * INCLUDEd. Counts filterCell calls to verify no cell-by-cell traversal.
   * <p>
   * This simulates a filter that wants some cells from CF1, then skips ahead to CF2. If the fix
   * permanently closes CF1's store scanner, CF1 cells would be missing on subsequent rows.
   */
  public static class CrossCFHintFilter extends FilterBase {
    private final byte[] targetFamily;
    private final byte[] targetQualifier;
    private final byte[] hintThreshold;
    private long filterCellCount;

    /**
     * @param targetFamily    the CF to hint/seek to
     * @param targetQualifier qualifier for the hint cell
     * @param hintThreshold   when a data CF cell's qualifier >= this, return SEEK_NEXT_USING_HINT
     *                        instead of INCLUDE. Cells before this threshold are INCLUDEd.
     */
    public CrossCFHintFilter(byte[] targetFamily, byte[] targetQualifier, byte[] hintThreshold) {
      this.targetFamily = targetFamily;
      this.targetQualifier = targetQualifier;
      this.hintThreshold = hintThreshold;
    }

    @Override
    public ReturnCode filterCell(Cell c) throws IOException {
      filterCellCount++;
      if (CellUtil.matchingFamily(c, targetFamily)) {
        return ReturnCode.INCLUDE;
      }
      // Data CF cell: include if qualifier < threshold, otherwise hint to target CF
      if (
        Bytes.compareTo(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength(),
          hintThreshold, 0, hintThreshold.length) < 0
      ) {
        return ReturnCode.INCLUDE;
      }
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }

    @Override
    public Cell getNextCellHint(Cell currentCell) {
      return new KeyValue(CellUtil.cloneRow(currentCell), targetFamily, targetQualifier,
        Long.MAX_VALUE, KeyValue.Type.Maximum);
    }

    public long getFilterCellCount() {
      return filterCellCount;
    }
  }

  @ParameterizedTest
  @MethodSource("params")
  public void testCrossCFSeekHint(byte[] dataCF, byte[] targetCF, boolean flush, boolean reversed)
    throws IOException {
    String suffix = Bytes.toString(dataCF) + "_" + Bytes.toString(targetCF)
      + (flush ? "_flush" : "_mem") + (reversed ? "_rev" : "");
    TableName tableName = TableName.valueOf("TestCrossCFSeekHint_" + suffix);
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(dataCF))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(targetCF)).build();
    RegionInfo info = RegionInfoBuilder.newBuilder(tableName).build();
    HRegion region = HBaseTestingUtil.createRegionAndWAL(info, TEST_UTIL.getDataTestDir(),
      TEST_UTIL.getConfiguration(), td);

    try {
      // Write two rows. Each row has NUM_CELLS qualifiers in dataCF (q00000..q00099)
      // and one cell in targetCF.
      for (byte[] row : new byte[][] { ROW1, ROW2 }) {
        for (int i = 0; i < NUM_CELLS; i++) {
          Put p = new Put(row);
          p.addColumn(dataCF, Bytes.toBytes(String.format("q%05d", i)), Bytes.toBytes("v"));
          region.put(p);
        }
        Put p = new Put(row);
        p.addColumn(targetCF, QUAL, Bytes.toBytes("target"));
        region.put(p);
      }
      if (flush) {
        region.flush(true);
      }

      // The filter INCLUDEs dataCF cells with qualifier < "q00002" (i.e. q00000, q00001),
      // then returns SEEK_NEXT_USING_HINT at q00002 with a hint to targetCF.
      // This means each row should return: dataCF:q00000, dataCF:q00001, targetCF:q.
      byte[] hintThreshold = Bytes.toBytes("q00002");
      int includedPerRow = 2; // q00000, q00001
      CrossCFHintFilter filter = new CrossCFHintFilter(targetCF, QUAL, hintThreshold);
      Scan scan = new Scan();
      scan.setFilter(filter);
      scan.setReversed(reversed);

      List<Cell> row1Results = new ArrayList<>();
      List<Cell> row2Results = new ArrayList<>();
      try (RegionScanner scanner = region.getScanner(scan)) {
        boolean hasMore = scanner.next(row1Results);
        assertTrue(hasMore, "Should have more rows after first row");
        scanner.next(row2Results);
      }

      // First row: includedPerRow data CF cells + 1 target CF cell
      assertEquals(includedPerRow + 1, row1Results.size(), "First row result count");

      // Second row: if the data CF's store scanner was permanently closed on the
      // first row, we would lose the data CF cells here.
      assertEquals(includedPerRow + 1, row2Results.size(),
        "Second row must also return data CF cells; store scanner must survive cross-CF hint");

      // Verify no cell-by-cell traversal of the remaining 98 data CF cells per row.
      // Per row: includedPerRow INCLUDEs + 1 SEEK_NEXT_USING_HINT + 1 target CF INCLUDE.
      int callsPerRow = includedPerRow + 1 + 1;
      assertEquals(callsPerRow * 2, filter.getFilterCellCount(),
        "Cross-CF hint should not cause cell-by-cell traversal (HBASE-28902)");
    } finally {
      HBaseTestingUtil.closeRegionAndWAL(region);
    }
  }
}
