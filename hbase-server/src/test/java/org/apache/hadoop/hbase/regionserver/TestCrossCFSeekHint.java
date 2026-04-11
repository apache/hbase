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

  private static final byte[] ROW = Bytes.toBytes("row");
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
   * Filter that returns SEEK_NEXT_USING_HINT for cells not in the target family, with a hint
   * pointing to the target family. Counts how many times filterCell is called so we can verify the
   * scanner didn't traverse all cells.
   */
  public static class CrossCFHintFilter extends FilterBase {
    private final byte[] targetFamily;
    private final byte[] targetQualifier;
    private long filterCellCount;

    public CrossCFHintFilter(byte[] targetFamily, byte[] targetQualifier) {
      this.targetFamily = targetFamily;
      this.targetQualifier = targetQualifier;
    }

    @Override
    public ReturnCode filterCell(Cell c) throws IOException {
      filterCellCount++;
      if (CellUtil.matchingFamily(c, targetFamily)) {
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
      for (int i = 0; i < NUM_CELLS; i++) {
        Put p = new Put(ROW);
        p.addColumn(dataCF, Bytes.toBytes(String.format("q%05d", i)), Bytes.toBytes("v"));
        region.put(p);
      }
      Put p = new Put(ROW);
      p.addColumn(targetCF, QUAL, Bytes.toBytes("target"));
      region.put(p);
      if (flush) {
        region.flush(true);
      }

      CrossCFHintFilter filter = new CrossCFHintFilter(targetCF, QUAL);
      Scan scan = new Scan();
      scan.setFilter(filter);
      scan.setReversed(reversed);

      List<Cell> results = new ArrayList<>();
      try (RegionScanner scanner = region.getScanner(scan)) {
        scanner.next(results);
      }

      assertEquals(1, results.size(), "Should return the cell from target CF");
      assertTrue(CellUtil.matchingFamily(results.get(0), targetCF),
        "Result should be from target CF");

      // 1 call for the first data CF cell (hint triggers close) + 1 for the target CF cell
      assertEquals(2, filter.getFilterCellCount(),
        "Cross-CF hint should not cause cell-by-cell traversal (HBASE-28902)");
    } finally {
      HBaseTestingUtil.closeRegionAndWAL(region);
    }
  }
}
