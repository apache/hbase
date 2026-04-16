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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(FilterTests.TAG)
@Tag(SmallTests.TAG)
public class TestDependentColumnFilter {

  private static final Logger LOG = LoggerFactory.getLogger(TestDependentColumnFilter.class);
  private static final byte[][] ROWS = { Bytes.toBytes("test1"), Bytes.toBytes("test2") };
  private static final byte[][] FAMILIES =
    { Bytes.toBytes("familyOne"), Bytes.toBytes("familyTwo") };
  private static final long STAMP_BASE = EnvironmentEdgeManager.currentTime();
  private static final long[] STAMPS = { STAMP_BASE - 100, STAMP_BASE - 200, STAMP_BASE - 300 };
  private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
  private static final byte[][] BAD_VALS =
    { Bytes.toBytes("bad1"), Bytes.toBytes("bad2"), Bytes.toBytes("bad3") };
  private static final byte[] MATCH_VAL = Bytes.toBytes("match");
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  List<KeyValue> testVals;
  private HRegion region;

  @BeforeEach
  public void setUp() throws Exception {
    testVals = makeTestVals();

    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(this.getClass().getSimpleName()));
    HColumnDescriptor hcd0 = new HColumnDescriptor(FAMILIES[0]);
    hcd0.setMaxVersions(3);
    htd.addFamily(hcd0);
    HColumnDescriptor hcd1 = new HColumnDescriptor(FAMILIES[1]);
    hcd1.setMaxVersions(3);
    htd.addFamily(hcd1);
    HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);
    this.region = HBaseTestingUtility.createRegionAndWAL(info, TEST_UTIL.getDataTestDir(),
      TEST_UTIL.getConfiguration(), htd);
    addData();
  }

  @AfterEach
  public void tearDown() throws Exception {
    HBaseTestingUtility.closeRegionAndWAL(this.region);
  }

  private void addData() throws IOException {
    Put put = new Put(ROWS[0]);
    // add in an entry for each stamp, with 2 as a "good" value
    put.addColumn(FAMILIES[0], QUALIFIER, STAMPS[0], BAD_VALS[0]);
    put.addColumn(FAMILIES[0], QUALIFIER, STAMPS[1], BAD_VALS[1]);
    put.addColumn(FAMILIES[0], QUALIFIER, STAMPS[2], MATCH_VAL);
    // add in entries for stamps 0 and 2.
    // without a value check both will be "accepted"
    // with one 2 will be accepted(since the corresponding ts entry
    // has a matching value
    put.addColumn(FAMILIES[1], QUALIFIER, STAMPS[0], BAD_VALS[0]);
    put.addColumn(FAMILIES[1], QUALIFIER, STAMPS[2], BAD_VALS[2]);

    this.region.put(put);

    put = new Put(ROWS[1]);
    put.addColumn(FAMILIES[0], QUALIFIER, STAMPS[0], BAD_VALS[0]);
    // there is no corresponding timestamp for this so it should never pass
    put.addColumn(FAMILIES[0], QUALIFIER, STAMPS[2], MATCH_VAL);
    // if we reverse the qualifiers this one should pass
    put.addColumn(FAMILIES[1], QUALIFIER, STAMPS[0], MATCH_VAL);
    // should pass
    put.addColumn(FAMILIES[1], QUALIFIER, STAMPS[1], BAD_VALS[2]);

    this.region.put(put);
  }

  private List<KeyValue> makeTestVals() {
    List<KeyValue> testVals = new ArrayList<>();
    testVals.add(new KeyValue(ROWS[0], FAMILIES[0], QUALIFIER, STAMPS[0], BAD_VALS[0]));
    testVals.add(new KeyValue(ROWS[0], FAMILIES[0], QUALIFIER, STAMPS[1], BAD_VALS[1]));
    testVals.add(new KeyValue(ROWS[0], FAMILIES[1], QUALIFIER, STAMPS[1], BAD_VALS[2]));
    testVals.add(new KeyValue(ROWS[0], FAMILIES[1], QUALIFIER, STAMPS[0], MATCH_VAL));
    testVals.add(new KeyValue(ROWS[0], FAMILIES[1], QUALIFIER, STAMPS[2], BAD_VALS[2]));

    return testVals;
  }

  /**
   * This shouldn't be confused with TestFilter#verifyScan as expectedKeys is not the per row total,
   * but the scan total
   */
  private void verifyScan(Scan s, long expectedRows, long expectedCells) throws IOException {
    InternalScanner scanner = this.region.getScanner(s);
    List<Cell> results = new ArrayList<>();
    int i = 0;
    int cells = 0;
    for (boolean done = true; done; i++) {
      done = scanner.next(results);
      Arrays.sort(results.toArray(new Cell[results.size()]), CellComparatorImpl.COMPARATOR);
      LOG.info("counter=" + i + ", " + results);
      if (results.isEmpty()) break;
      cells += results.size();
      assertTrue(expectedRows > i, "Scanned too many rows! Only expected " + expectedRows
        + " total but already scanned " + (i + 1));
      assertTrue(expectedCells >= cells,
        "Expected " + expectedCells + " cells total but " + "already scanned " + cells);
      results.clear();
    }
    assertEquals(expectedRows, i, "Expected " + expectedRows + " rows but scanned " + i + " rows");
    assertEquals(expectedCells, cells,
      "Expected " + expectedCells + " cells but scanned " + cells + " cells");
  }

  /**
   * Test scans using a DependentColumnFilter
   */
  @Test
  public void testScans() throws Exception {
    Filter filter = new DependentColumnFilter(FAMILIES[0], QUALIFIER);

    Scan scan = new Scan();
    scan.setFilter(filter);
    scan.setMaxVersions(Integer.MAX_VALUE);

    verifyScan(scan, 2, 8);

    // drop the filtering cells
    filter = new DependentColumnFilter(FAMILIES[0], QUALIFIER, true);
    scan = new Scan();
    scan.setFilter(filter);
    scan.setMaxVersions(Integer.MAX_VALUE);

    verifyScan(scan, 2, 3);

    // include a comparator operation
    filter = new DependentColumnFilter(FAMILIES[0], QUALIFIER, false, CompareOperator.EQUAL,
      new BinaryComparator(MATCH_VAL));
    scan = new Scan();
    scan.setFilter(filter);
    scan.setMaxVersions(Integer.MAX_VALUE);

    /*
     * expecting to get the following 3 cells row 0 put.add(FAMILIES[0], QUALIFIER, STAMPS[2],
     * MATCH_VAL); put.add(FAMILIES[1], QUALIFIER, STAMPS[2], BAD_VALS[2]); row 1
     * put.add(FAMILIES[0], QUALIFIER, STAMPS[2], MATCH_VAL);
     */
    verifyScan(scan, 2, 3);

    // include a comparator operation and drop comparator
    filter = new DependentColumnFilter(FAMILIES[0], QUALIFIER, true, CompareOperator.EQUAL,
      new BinaryComparator(MATCH_VAL));
    scan = new Scan();
    scan.setFilter(filter);
    scan.setMaxVersions(Integer.MAX_VALUE);

    /*
     * expecting to get the following 1 cell row 0 put.add(FAMILIES[1], QUALIFIER, STAMPS[2],
     * BAD_VALS[2]);
     */
    verifyScan(scan, 1, 1);

  }

  /**
   * Test that the filter correctly drops rows without a corresponding timestamp
   */
  @Test
  public void testFilterDropping() throws Exception {
    Filter filter = new DependentColumnFilter(FAMILIES[0], QUALIFIER);
    List<Cell> accepted = new ArrayList<>();
    for (Cell val : testVals) {
      if (filter.filterCell(val) == ReturnCode.INCLUDE) {
        accepted.add(val);
      }
    }
    assertEquals(5, accepted.size(), "check all values accepted from filterCell");

    filter.filterRowCells(accepted);
    assertEquals(4, accepted.size(),
      "check filterRow(List<KeyValue>) dropped cell without corresponding column entry");

    // start do it again with dependent column dropping on
    filter = new DependentColumnFilter(FAMILIES[1], QUALIFIER, true);
    accepted.clear();
    for (KeyValue val : testVals) {
      if (filter.filterCell(val) == ReturnCode.INCLUDE) {
        accepted.add(val);
      }
    }
    assertEquals(2, accepted.size(), "check the filtering column cells got dropped");

    filter.filterRowCells(accepted);
    assertEquals(2, accepted.size(), "check cell retention");
  }

  /**
   * Test for HBASE-8794. Avoid NullPointerException in DependentColumnFilter.toString().
   */
  @Test
  public void testToStringWithNullComparator() {
    // Test constructor that implicitly sets a null comparator
    Filter filter = new DependentColumnFilter(FAMILIES[0], QUALIFIER);
    assertNotNull(filter.toString());
    assertTrue(filter.toString().contains("null"),
      "check string contains 'null' as compatator is null");

    // Test constructor with explicit null comparator
    filter = new DependentColumnFilter(FAMILIES[0], QUALIFIER, true, CompareOperator.EQUAL, null);
    assertNotNull(filter.toString());
    assertTrue(filter.toString().contains("null"),
      "check string contains 'null' as compatator is null");
  }

  @Test
  public void testToStringWithNonNullComparator() {
    Filter filter = new DependentColumnFilter(FAMILIES[0], QUALIFIER, true, CompareOperator.EQUAL,
      new BinaryComparator(MATCH_VAL));
    assertNotNull(filter.toString());
    assertTrue(filter.toString().contains("match"), "check string contains comparator value");
  }

}
