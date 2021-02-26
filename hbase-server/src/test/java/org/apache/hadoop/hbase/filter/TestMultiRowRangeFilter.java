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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(LargeTests.class)
public class TestMultiRowRangeFilter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMultiRowRangeFilter.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Logger LOG = LoggerFactory.getLogger(TestMultiRowRangeFilter.class);
  private byte[] family = Bytes.toBytes("family");
  private byte[] qf = Bytes.toBytes("qf");
  private byte[] value = Bytes.toBytes("val");
  private TableName tableName;
  private int numRows = 100;

  @Rule
  public TestName name = new TestName();

  /**
   * @throws Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  /**
   * @throws Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRowKeyPrefixWithEmptyPrefix() throws IOException {
    byte[] prefix = {};
    byte[][] rowKeyPrefixes = new byte[1][];
    rowKeyPrefixes[0] = prefix;
    MultiRowRangeFilter filter = new MultiRowRangeFilter(rowKeyPrefixes);
    List<RowRange> actualRanges = filter.getRowRanges();
    List<RowRange> expectedRanges = new ArrayList<>();
    expectedRanges.add(
      new RowRange(HConstants.EMPTY_START_ROW, true, HConstants.EMPTY_END_ROW, false)
    );
    assertRangesEqual(expectedRanges, actualRanges);
  }

  @Test
  public void testRowKeyPrefixWithLastIncrementablePrefix() throws IOException {
    byte[] prefix = {(byte) 0x12, (byte) 0x23, (byte) 0xFF, (byte) 0xFE};
    byte[][] rowKeyPrefixes = new byte[1][];
    rowKeyPrefixes[0] = prefix;
    MultiRowRangeFilter filter = new MultiRowRangeFilter(rowKeyPrefixes);
    List<RowRange> actualRanges = filter.getRowRanges();
    List<RowRange> expectedRanges = new ArrayList<>();
    final byte[] expectedStop = {(byte) 0x12, (byte) 0x23, (byte) 0xFF, (byte) 0xFF};
    expectedRanges.add(new RowRange(prefix, true, expectedStop , false));
    assertRangesEqual(expectedRanges, actualRanges);
  }

  @Test
  public void testRowKeyPrefixWithoutLastIncrementablePrefix() throws IOException {
    byte[] prefix = {(byte) 0x12, (byte) 0x23, (byte) 0xFF, (byte) 0xFF};
    byte[][] rowKeyPrefixes = new byte[1][];
    rowKeyPrefixes[0] = prefix;
    MultiRowRangeFilter filter = new MultiRowRangeFilter(rowKeyPrefixes);
    List<RowRange> actualRanges = filter.getRowRanges();
    List<RowRange> expectedRanges = new ArrayList<>();
    final byte[] expectedStop = {(byte) 0x12, (byte) 0x24};
    expectedRanges.add(new RowRange(prefix, true, expectedStop , false));
    assertRangesEqual(expectedRanges, actualRanges);
  }

  @Test
  public void testRowKeyPrefixWithMergablePrefix() throws IOException {
    byte[] prefix1 = {(byte) 0x12, (byte) 0x23, (byte) 0xFF, (byte) 0xFE};
    byte[] prefix2 = {(byte) 0x12, (byte) 0x23, (byte) 0xFF, (byte) 0xFF};
    byte[][] rowKeyPrefixes = new byte[2][];
    rowKeyPrefixes[0] = prefix1;
    rowKeyPrefixes[1] = prefix2;
    MultiRowRangeFilter filter = new MultiRowRangeFilter(rowKeyPrefixes);
    List<RowRange> actualRanges = filter.getRowRanges();
    List<RowRange> expectedRanges = new ArrayList<>();
    final byte[] expectedStop = {(byte) 0x12, (byte) 0x24};
    expectedRanges.add(new RowRange(prefix1, true, expectedStop , false));
    assertRangesEqual(expectedRanges, actualRanges);
  }

  @Test
  public void testRanges() throws IOException {
    byte[] key1Start = new byte[] {-3};
    byte[] key1End  = new byte[] {-2};

    byte[] key2Start = new byte[] {5};
    byte[] key2End  = new byte[] {6};

    byte[] badKey = new byte[] {-10};

    MultiRowRangeFilter filter = new MultiRowRangeFilter(Arrays.asList(
      new MultiRowRangeFilter.RowRange(key1Start, true, key1End, false),
      new MultiRowRangeFilter.RowRange(key2Start, true, key2End, false)
        ));
    filter.filterRowKey(KeyValueUtil.createFirstOnRow(badKey));
    /*
     * FAILS -- includes BAD key!
     * Expected :SEEK_NEXT_USING_HINT
     * Actual   :INCLUDE
     * */
    assertEquals(Filter.ReturnCode.SEEK_NEXT_USING_HINT, filter.filterCell(null));
  }

  @Test
  public void testOutOfOrderScannerNextException() throws Exception {
    MultiRowRangeFilter filter = new MultiRowRangeFilter(Arrays.asList(
            new MultiRowRangeFilter.RowRange(Bytes.toBytes("b"), true, Bytes.toBytes("c"), true),
            new MultiRowRangeFilter.RowRange(Bytes.toBytes("d"), true, Bytes.toBytes("e"), true)
    ));
    filter.filterRowKey(KeyValueUtil.createFirstOnRow(Bytes.toBytes("a")));
    assertEquals(Filter.ReturnCode.SEEK_NEXT_USING_HINT, filter.filterCell(null));
    filter.filterRowKey(KeyValueUtil.createFirstOnRow(Bytes.toBytes("b")));
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(null));
    filter.filterRowKey(KeyValueUtil.createFirstOnRow(Bytes.toBytes("c")));
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(null));
    filter.filterRowKey(KeyValueUtil.createFirstOnRow(Bytes.toBytes("d")));
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(null));
    filter.filterRowKey(KeyValueUtil.createFirstOnRow(Bytes.toBytes("e")));
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterCell(null));
  }

  @Test
  public void testMergeAndSortWithEmptyStartRow() throws IOException {
    List<RowRange> ranges = new ArrayList<>();
    ranges.add(new RowRange(Bytes.toBytes(""), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(15), true, Bytes.toBytes(40), false));
    List<RowRange> actualRanges = MultiRowRangeFilter.sortAndMerge(ranges);
    List<RowRange> expectedRanges = new ArrayList<>();
    expectedRanges.add(new RowRange(Bytes.toBytes(""), true, Bytes.toBytes(40), false));
    assertRangesEqual(expectedRanges, actualRanges);
  }

  @Test
  public void testMergeAndSortWithEmptyStopRow() throws IOException {
    List<RowRange> ranges = new ArrayList<>();
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(15), true, Bytes.toBytes(""), false));
    ranges.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(70), false));
    List<RowRange> actualRanges = MultiRowRangeFilter.sortAndMerge(ranges);
    List<RowRange> expectedRanges = new ArrayList<>();
    expectedRanges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(""), false));
    assertRangesEqual(expectedRanges, actualRanges);
  }

  @Test
  public void testMergeAndSortWithEmptyStartRowAndStopRow() throws IOException {
    List<RowRange> ranges = new ArrayList<>();
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(""), true, Bytes.toBytes(""), false));
    ranges.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(70), false));
    List<RowRange> actualRanges = MultiRowRangeFilter.sortAndMerge(ranges);
    List<RowRange> expectedRanges = new ArrayList<>();
    expectedRanges.add(new RowRange(Bytes.toBytes(""), true, Bytes.toBytes(""), false));
    assertRangesEqual(expectedRanges, actualRanges);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testMultiRowRangeWithoutRange() throws IOException {
    List<RowRange> ranges = new ArrayList<>();
    new MultiRowRangeFilter(ranges);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testMultiRowRangeWithInvalidRange() throws IOException {
    List<RowRange> ranges = new ArrayList<>();
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    // the start row larger than the stop row
    ranges.add(new RowRange(Bytes.toBytes(80), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(70), false));
    new MultiRowRangeFilter(ranges);
  }

  @Test
  public void testMergeAndSortWithoutOverlap() throws IOException {
    List<RowRange> ranges = new ArrayList<>();
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(40), false));
    ranges.add(new RowRange(Bytes.toBytes(60), true, Bytes.toBytes(70), false));
    List<RowRange> actualRanges = MultiRowRangeFilter.sortAndMerge(ranges);
    List<RowRange> expectedRanges = new ArrayList<>();
    expectedRanges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    expectedRanges.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(40), false));
    expectedRanges.add(new RowRange(Bytes.toBytes(60), true, Bytes.toBytes(70), false));
    assertRangesEqual(expectedRanges, actualRanges);
  }

  @Test
  public void testMergeAndSortWithOverlap() throws IOException {
    List<RowRange> ranges = new ArrayList<>();
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(15), true, Bytes.toBytes(40), false));
    ranges.add(new RowRange(Bytes.toBytes(20), true, Bytes.toBytes(30), false));
    ranges.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(50), false));
    ranges.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(70), false));
    ranges.add(new RowRange(Bytes.toBytes(90), true, Bytes.toBytes(100), false));
    ranges.add(new RowRange(Bytes.toBytes(95), true, Bytes.toBytes(100), false));
    List<RowRange> actualRanges = MultiRowRangeFilter.sortAndMerge(ranges);
    List<RowRange> expectedRanges = new ArrayList<>();
    expectedRanges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(70), false));
    expectedRanges.add(new RowRange(Bytes.toBytes(90), true, Bytes.toBytes(100), false));
    assertRangesEqual(expectedRanges, actualRanges);
  }

  @Test
  public void testMergeAndSortWithStartRowInclusive() throws IOException {
    List<RowRange> ranges = new ArrayList<>();
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(20), true, Bytes.toBytes(""), false));
    List<RowRange> actualRanges = MultiRowRangeFilter.sortAndMerge(ranges);
    List<RowRange> expectedRanges = new ArrayList<>();
    expectedRanges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(""), false));
    assertRangesEqual(expectedRanges, actualRanges);
  }

  @Test
  public void testMergeAndSortWithRowExclusive() throws IOException {
    List<RowRange> ranges = new ArrayList<>();
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(20), false, Bytes.toBytes(""), false));
    List<RowRange> actualRanges = MultiRowRangeFilter.sortAndMerge(ranges);
    List<RowRange> expectedRanges = new ArrayList<>();
    expectedRanges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    expectedRanges.add(new RowRange(Bytes.toBytes(20), false, Bytes.toBytes(""), false));
    assertRangesEqual(expectedRanges, actualRanges);
  }

  @Test
  public void testMergeAndSortWithRowInclusive() throws IOException {
    List<RowRange> ranges = new ArrayList<>();
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), true));
    ranges.add(new RowRange(Bytes.toBytes(20), false, Bytes.toBytes(""), false));
    List<RowRange> actualRanges = MultiRowRangeFilter.sortAndMerge(ranges);
    List<RowRange> expectedRanges = new ArrayList<>();
    expectedRanges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(""), false));
    assertRangesEqual(expectedRanges, actualRanges);
  }

  public void assertRangesEqual(List<RowRange> expected, List<RowRange> actual) {
    assertEquals(expected.size(), actual.size());
    for(int i = 0; i < expected.size(); i++) {
      Assert.assertTrue(Bytes.equals(expected.get(i).getStartRow(), actual.get(i).getStartRow()));
      Assert.assertTrue(expected.get(i).isStartRowInclusive() ==
          actual.get(i).isStartRowInclusive());
      Assert.assertTrue(Bytes.equals(expected.get(i).getStopRow(), actual.get(i).getStopRow()));
      Assert.assertTrue(expected.get(i).isStopRowInclusive() ==
          actual.get(i).isStopRowInclusive());
    }
  }

  @Test
  public void testMultiRowRangeFilterWithRangeOverlap() throws IOException {
    tableName = TableName.valueOf(name.getMethodName());
    Table ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);
    generateRows(numRows, ht, family, qf, value);

    Scan scan = new Scan();
    scan.setMaxVersions();

    List<RowRange> ranges = new ArrayList<>();
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(15), true, Bytes.toBytes(40), false));
    ranges.add(new RowRange(Bytes.toBytes(65), true, Bytes.toBytes(75), false));
    ranges.add(new RowRange(Bytes.toBytes(60), true, null, false));
    ranges.add(new RowRange(Bytes.toBytes(60), true, Bytes.toBytes(80), false));

    MultiRowRangeFilter filter = new MultiRowRangeFilter(ranges);
    scan.setFilter(filter);
    int resultsSize = getResultsSize(ht, scan);
    LOG.info("found " + resultsSize + " results");
    List<Cell> results1 = getScanResult(Bytes.toBytes(10), Bytes.toBytes(40), ht);
    List<Cell> results2 = getScanResult(Bytes.toBytes(60), Bytes.toBytes(""), ht);

    assertEquals(results1.size() + results2.size(), resultsSize);

    ht.close();
  }

  @Test
  public void testMultiRowRangeFilterWithoutRangeOverlap() throws IOException {
    tableName = TableName.valueOf(name.getMethodName());
    Table ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);
    generateRows(numRows, ht, family, qf, value);

    Scan scan = new Scan();
    scan.setMaxVersions();

    List<RowRange> ranges = new ArrayList<>();
    ranges.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(40), false));
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(60), true, Bytes.toBytes(70), false));

    MultiRowRangeFilter filter = new MultiRowRangeFilter(ranges);
    scan.setFilter(filter);
    int resultsSize = getResultsSize(ht, scan);
    LOG.info("found " + resultsSize + " results");
    List<Cell> results1 = getScanResult(Bytes.toBytes(10), Bytes.toBytes(20), ht);
    List<Cell> results2 = getScanResult(Bytes.toBytes(30), Bytes.toBytes(40), ht);
    List<Cell> results3 = getScanResult(Bytes.toBytes(60), Bytes.toBytes(70), ht);

    assertEquals(results1.size() + results2.size() + results3.size(), resultsSize);

    ht.close();
  }

  @Test
  public void testMultiRowRangeFilterWithEmptyStartRow() throws IOException {
    tableName = TableName.valueOf(name.getMethodName());
    Table ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);
    generateRows(numRows, ht, family, qf, value);
    Scan scan = new Scan();
    scan.setMaxVersions();

    List<RowRange> ranges = new ArrayList<>();
    ranges.add(new RowRange(Bytes.toBytes(""), true, Bytes.toBytes(10), false));
    ranges.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(40), false));

    MultiRowRangeFilter filter = new MultiRowRangeFilter(ranges);
    scan.setFilter(filter);
    int resultsSize = getResultsSize(ht, scan);
    List<Cell> results1 = getScanResult(Bytes.toBytes(""), Bytes.toBytes(10), ht);
    List<Cell> results2 = getScanResult(Bytes.toBytes(30), Bytes.toBytes(40), ht);
    assertEquals(results1.size() + results2.size(), resultsSize);

    ht.close();
  }

  @Test
  public void testMultiRowRangeFilterWithEmptyStopRow() throws IOException {
    tableName = TableName.valueOf(name.getMethodName());
    Table ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);
    generateRows(numRows, ht, family, qf, value);
    Scan scan = new Scan();
    scan.setMaxVersions();

    List<RowRange> ranges = new ArrayList<>();
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(""), false));
    ranges.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(40), false));

    MultiRowRangeFilter filter = new MultiRowRangeFilter(ranges);
    scan.setFilter(filter);
    int resultsSize = getResultsSize(ht, scan);
    List<Cell> results1 = getScanResult(Bytes.toBytes(10), Bytes.toBytes(""), ht);
    assertEquals(results1.size(), resultsSize);

    ht.close();
  }

  @Test
  public void testMultiRowRangeFilterWithInclusive() throws IOException {
    tableName = TableName.valueOf(name.getMethodName());
    Table ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);
    generateRows(numRows, ht, family, qf, value);

    Scan scan = new Scan();
    scan.setMaxVersions();

    List<RowRange> ranges = new ArrayList<>();
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(20), true, Bytes.toBytes(40), false));
    ranges.add(new RowRange(Bytes.toBytes(65), true, Bytes.toBytes(75), false));
    ranges.add(new RowRange(Bytes.toBytes(60), true, null, false));
    ranges.add(new RowRange(Bytes.toBytes(60), true, Bytes.toBytes(80), false));

    MultiRowRangeFilter filter = new MultiRowRangeFilter(ranges);
    scan.setFilter(filter);
    int resultsSize = getResultsSize(ht, scan);
    LOG.info("found " + resultsSize + " results");
    List<Cell> results1 = getScanResult(Bytes.toBytes(10), Bytes.toBytes(40), ht);
    List<Cell> results2 = getScanResult(Bytes.toBytes(60), Bytes.toBytes(""), ht);

    assertEquals(results1.size() + results2.size(), resultsSize);

    ht.close();
  }

  @Test
  public void testMultiRowRangeFilterWithExclusive() throws IOException {
    tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 6000000);
    Table ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);
    ht.setReadRpcTimeout(600000);
    ht.setOperationTimeout(6000000);
    generateRows(numRows, ht, family, qf, value);

    Scan scan = new Scan();
    scan.setMaxVersions();

    List<RowRange> ranges = new ArrayList<>();
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(20), false, Bytes.toBytes(40), false));
    ranges.add(new RowRange(Bytes.toBytes(65), true, Bytes.toBytes(75), false));

    MultiRowRangeFilter filter = new MultiRowRangeFilter(ranges);
    scan.setFilter(filter);
    int resultsSize = getResultsSize(ht, scan);
    LOG.info("found " + resultsSize + " results");
    List<Cell> results1 = getScanResult(Bytes.toBytes(10), Bytes.toBytes(40), ht);
    List<Cell> results2 = getScanResult(Bytes.toBytes(65), Bytes.toBytes(75), ht);

    assertEquals((results1.size() - 1) + results2.size(), resultsSize);

    ht.close();
  }

  @Test
  public void testMultiRowRangeWithFilterListAndOperator() throws IOException {
    tableName = TableName.valueOf(name.getMethodName());
    Table ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);
    generateRows(numRows, ht, family, qf, value);

    Scan scan = new Scan();
    scan.setMaxVersions();

    List<RowRange> ranges1 = new ArrayList<>();
    ranges1.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges1.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(40), false));
    ranges1.add(new RowRange(Bytes.toBytes(60), true, Bytes.toBytes(70), false));

    MultiRowRangeFilter filter1 = new MultiRowRangeFilter(ranges1);

    List<RowRange> ranges2 = new ArrayList<>();
    ranges2.add(new RowRange(Bytes.toBytes(20), true, Bytes.toBytes(40), false));
    ranges2.add(new RowRange(Bytes.toBytes(80), true, Bytes.toBytes(90), false));

    MultiRowRangeFilter filter2 = new MultiRowRangeFilter(ranges2);

    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    scan.setFilter(filterList);
    int resultsSize = getResultsSize(ht, scan);
    LOG.info("found " + resultsSize + " results");
    List<Cell> results1 = getScanResult(Bytes.toBytes(30), Bytes.toBytes(40), ht);

    assertEquals(results1.size(), resultsSize);

    ht.close();
  }

  @Test
  public void testMultiRowRangeWithFilterListOrOperator() throws IOException {
    tableName = TableName.valueOf(name.getMethodName());
    Table ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);
    generateRows(numRows, ht, family, qf, value);

    Scan scan = new Scan();
    scan.setMaxVersions();

    List<RowRange> ranges1 = new ArrayList<>();
    ranges1.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(40), false));
    ranges1.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges1.add(new RowRange(Bytes.toBytes(60), true, Bytes.toBytes(70), false));

    MultiRowRangeFilter filter1 = new MultiRowRangeFilter(ranges1);

    List<RowRange> ranges2 = new ArrayList<>();
    ranges2.add(new RowRange(Bytes.toBytes(20), true, Bytes.toBytes(40), false));
    ranges2.add(new RowRange(Bytes.toBytes(80), true, Bytes.toBytes(90), false));

    MultiRowRangeFilter filter2 = new MultiRowRangeFilter(ranges2);

    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    scan.setFilter(filterList);
    int resultsSize = getResultsSize(ht, scan);
    LOG.info("found " + resultsSize + " results");
    List<Cell> results1 = getScanResult(Bytes.toBytes(10), Bytes.toBytes(40), ht);
    List<Cell> results2 = getScanResult(Bytes.toBytes(60), Bytes.toBytes(70), ht);
    List<Cell> results3 = getScanResult(Bytes.toBytes(80), Bytes.toBytes(90), ht);

    assertEquals(results1.size() + results2.size() + results3.size(),resultsSize);

    ht.close();
  }

  @Test
  public void testOneRowRange() throws IOException {
    tableName = TableName.valueOf(name.getMethodName());
    Table ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);
    generateRows(numRows, ht, family, qf, value);
    ArrayList<MultiRowRangeFilter.RowRange> rowRangesList = new ArrayList<>();
    rowRangesList
        .add(new MultiRowRangeFilter.RowRange(Bytes.toBytes(50), true, Bytes.toBytes(50), true));
    Scan scan = new Scan();
    scan.setFilter(new MultiRowRangeFilter(rowRangesList));
    int resultsSize = getResultsSize(ht, scan);
    assertEquals(1, resultsSize);
    rowRangesList.clear();
    rowRangesList
        .add(new MultiRowRangeFilter.RowRange(Bytes.toBytes(50), true, Bytes.toBytes(51), false));
    scan = new Scan();
    scan.setFilter(new MultiRowRangeFilter(rowRangesList));
    resultsSize = getResultsSize(ht, scan);
    assertEquals(1, resultsSize);
    rowRangesList.clear();
    rowRangesList
        .add(new MultiRowRangeFilter.RowRange(Bytes.toBytes(50), true, Bytes.toBytes(51), true));
    scan = new Scan();
    scan.setFilter(new MultiRowRangeFilter(rowRangesList));
    resultsSize = getResultsSize(ht, scan);
    assertEquals(2, resultsSize);
    ht.close();
  }

  @Test
  public void testReverseMultiRowRangeFilterWithinTable() throws IOException {
    tableName = TableName.valueOf(name.getMethodName());
    Table ht = TEST_UTIL.createTable(tableName, family);
    generateRows(numRows, ht, family, qf, value);

    Scan scan = new Scan();
    scan.setReversed(true);
    List<RowRange> ranges = Arrays.asList(
        new RowRange(Bytes.toBytes(20), true, Bytes.toBytes(30), true),
        new RowRange(Bytes.toBytes(50), true, Bytes.toBytes(60), true)
    );
    MultiRowRangeFilter filter = new MultiRowRangeFilter(ranges);
    scan.setFilter(filter);

    List<Integer> expectedResults = new ArrayList<>();
    for (int i = 60; i >= 50; i--) {
      expectedResults.add(i);
    }
    for (int i = 30; i >= 20; i--) {
      expectedResults.add(i);
    }

    List<Cell> results = getResults(ht, scan);
    List<Integer> actualResults = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    for (Cell result : results) {
      int observedValue = Bytes.toInt(
          result.getRowArray(), result.getRowOffset(), result.getRowLength());
      actualResults.add(observedValue);
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(observedValue);
    }
    assertEquals("Saw results: " + sb.toString(), 22, results.size());
  }

  @Test
  public void testReverseMultiRowRangeFilterIncludingMaxRow() throws IOException {
    tableName = TableName.valueOf(name.getMethodName());
    Table ht = TEST_UTIL.createTable(tableName, family);
    for (String rowkey : Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h")) {
      byte[] row = Bytes.toBytes(rowkey);
      Put p = new Put(row);
      p.addColumn(family, qf, value);
      ht.put(p);
    }
    TEST_UTIL.flush();

    Scan scan = new Scan();
    scan.setReversed(true);
    List<RowRange> ranges = Arrays.asList(
        new RowRange(Bytes.toBytes("b"), true, Bytes.toBytes("c"), true),
        new RowRange(Bytes.toBytes("f"), true, Bytes.toBytes("h"), true)
    );
    MultiRowRangeFilter filter = new MultiRowRangeFilter(ranges);
    scan.setFilter(filter);

    List<String> expected = Arrays.asList("h", "g", "f", "c", "b");
    List<String> actual = new ArrayList<>();
    for (Cell cell : getResults(ht, scan)) {
      actual.add(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
    }

    assertEquals(expected, actual);
  }

  @Test
  public void testReverseMultiRowRangeFilterIncludingMinRow() throws IOException {
    tableName = TableName.valueOf(name.getMethodName());
    Table ht = TEST_UTIL.createTable(tableName, family);
    for (String rowkey : Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h")) {
      byte[] row = Bytes.toBytes(rowkey);
      Put p = new Put(row);
      p.addColumn(family, qf, value);
      ht.put(p);
    }
    TEST_UTIL.flush();

    Scan scan = new Scan();
    scan.setReversed(true);
    List<RowRange> ranges = Arrays.asList(
        new RowRange(Bytes.toBytes("a"), true, Bytes.toBytes("c"), true),
        new RowRange(Bytes.toBytes("f"), true, Bytes.toBytes("g"), true)
    );
    MultiRowRangeFilter filter = new MultiRowRangeFilter(ranges);
    scan.setFilter(filter);

    List<String> expected = Arrays.asList("g", "f", "c", "b", "a");
    List<String> actual = new ArrayList<>();
    for (Cell cell : getResults(ht, scan)) {
      actual.add(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
    }

    assertEquals(expected, actual);
  }

  @Test
  public void testReverseMultiRowRangeFilterIncludingMinAndMaxRow() throws IOException {
    tableName = TableName.valueOf(name.getMethodName());
    Table ht = TEST_UTIL.createTable(tableName, family);
    for (String rowkey : Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h")) {
      byte[] row = Bytes.toBytes(rowkey);
      Put p = new Put(row);
      p.addColumn(family, qf, value);
      ht.put(p);
    }
    TEST_UTIL.flush();

    Scan scan = new Scan();
    scan.setReversed(true);
    List<RowRange> ranges = Arrays.asList(
        new RowRange(Bytes.toBytes("a"), true, Bytes.toBytes("c"), true),
        new RowRange(Bytes.toBytes("f"), true, Bytes.toBytes("h"), true)
    );
    MultiRowRangeFilter filter = new MultiRowRangeFilter(ranges);
    scan.setFilter(filter);

    List<String> expected = Arrays.asList("h", "g", "f", "c", "b", "a");
    List<String> actual = new ArrayList<>();
    for (Cell cell : getResults(ht, scan)) {
      actual.add(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
    }

    assertEquals(expected, actual);
  }

  private void generateRows(int numberOfRows, Table ht, byte[] family, byte[] qf, byte[] value)
      throws IOException {
    for (int i = 0; i < numberOfRows; i++) {
      byte[] row = Bytes.toBytes(i);
      Put p = new Put(row);
      p.addColumn(family, qf, value);
      ht.put(p);
    }
    TEST_UTIL.flush();
  }

  private List<Cell> getScanResult(byte[] startRow, byte[] stopRow, Table ht) throws IOException {
    Scan scan = new Scan();
    scan.setMaxVersions();
    if(!Bytes.toString(startRow).isEmpty()) {
      scan.setStartRow(startRow);
    }
    if(!Bytes.toString(stopRow).isEmpty()) {
      scan.setStopRow(stopRow);
    }
    ResultScanner scanner = ht.getScanner(scan);
    List<Cell> kvList = new ArrayList<>();
    Result r;
    while ((r = scanner.next()) != null) {
      for (Cell kv : r.listCells()) {
        kvList.add(kv);
      }
    }
    scanner.close();
    return kvList;
  }

  private List<Cell> getResults(Table ht, Scan scan) throws IOException {
    ResultScanner scanner = ht.getScanner(scan);
    List<Cell> results = new ArrayList<>();
    Result r;
    while ((r = scanner.next()) != null) {
      for (Cell kv : r.listCells()) {
        results.add(kv);
      }
    }
    scanner.close();
    return results;
  }

  private int getResultsSize(Table ht, Scan scan) throws IOException {
    return getResults(ht, scan).size();
  }
}
