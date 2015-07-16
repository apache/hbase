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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestMultiRowRangeFilter {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestMultiRowRangeFilter.class);
  private byte[] family = Bytes.toBytes("family");
  private byte[] qf = Bytes.toBytes("qf");
  private byte[] value = Bytes.toBytes("val");
  private TableName tableName;
  private int numRows = 100;

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
  public void testOutOfOrderScannerNextException() throws Exception {
    MultiRowRangeFilter filter = new MultiRowRangeFilter(Arrays.asList(
            new MultiRowRangeFilter.RowRange(Bytes.toBytes("b"), true, Bytes.toBytes("c"), true),
            new MultiRowRangeFilter.RowRange(Bytes.toBytes("d"), true, Bytes.toBytes("e"), true)
    ));
    filter.filterRowKey(KeyValueUtil.createFirstOnRow(Bytes.toBytes("a")));
    assertEquals(Filter.ReturnCode.SEEK_NEXT_USING_HINT, filter.filterKeyValue(null));
    filter.filterRowKey(KeyValueUtil.createFirstOnRow(Bytes.toBytes("b")));
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterKeyValue(null));
    filter.filterRowKey(KeyValueUtil.createFirstOnRow(Bytes.toBytes("c")));
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterKeyValue(null));
    filter.filterRowKey(KeyValueUtil.createFirstOnRow(Bytes.toBytes("d")));
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterKeyValue(null));
    filter.filterRowKey(KeyValueUtil.createFirstOnRow(Bytes.toBytes("e")));
    assertEquals(Filter.ReturnCode.INCLUDE, filter.filterKeyValue(null));
  }

  @Test
  public void testMergeAndSortWithEmptyStartRow() throws IOException {
    List<RowRange> ranges = new ArrayList<RowRange>();
    ranges.add(new RowRange(Bytes.toBytes(""), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(15), true, Bytes.toBytes(40), false));
    List<RowRange> actualRanges = MultiRowRangeFilter.sortAndMerge(ranges);
    List<RowRange> expectedRanges = new ArrayList<RowRange>();
    expectedRanges.add(new RowRange(Bytes.toBytes(""), true, Bytes.toBytes(40), false));
    assertRangesEqual(expectedRanges, actualRanges);
  }

  @Test
  public void testMergeAndSortWithEmptyStopRow() throws IOException {
    List<RowRange> ranges = new ArrayList<RowRange>();
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(15), true, Bytes.toBytes(""), false));
    ranges.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(70), false));
    List<RowRange> actualRanges = MultiRowRangeFilter.sortAndMerge(ranges);
    List<RowRange> expectedRanges = new ArrayList<RowRange>();
    expectedRanges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(""), false));
    assertRangesEqual(expectedRanges, actualRanges);
  }

  @Test
  public void testMergeAndSortWithEmptyStartRowAndStopRow() throws IOException {
    List<RowRange> ranges = new ArrayList<RowRange>();
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(""), true, Bytes.toBytes(""), false));
    ranges.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(70), false));
    List<RowRange> actualRanges = MultiRowRangeFilter.sortAndMerge(ranges);
    List<RowRange> expectedRanges = new ArrayList<RowRange>();
    expectedRanges.add(new RowRange(Bytes.toBytes(""), true, Bytes.toBytes(""), false));
    assertRangesEqual(expectedRanges, actualRanges);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testMultiRowRangeWithoutRange() throws IOException {
    List<RowRange> ranges = new ArrayList<RowRange>();
    new MultiRowRangeFilter(ranges);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testMultiRowRangeWithInvalidRange() throws IOException {
    List<RowRange> ranges = new ArrayList<RowRange>();
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    // the start row larger than the stop row
    ranges.add(new RowRange(Bytes.toBytes(80), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(70), false));
    new MultiRowRangeFilter(ranges);
  }

  @Test
  public void testMergeAndSortWithoutOverlap() throws IOException {
    List<RowRange> ranges = new ArrayList<RowRange>();
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(40), false));
    ranges.add(new RowRange(Bytes.toBytes(60), true, Bytes.toBytes(70), false));
    List<RowRange> actualRanges = MultiRowRangeFilter.sortAndMerge(ranges);
    List<RowRange> expectedRanges = new ArrayList<RowRange>();
    expectedRanges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    expectedRanges.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(40), false));
    expectedRanges.add(new RowRange(Bytes.toBytes(60), true, Bytes.toBytes(70), false));
    assertRangesEqual(expectedRanges, actualRanges);
  }

  @Test
  public void testMergeAndSortWithOverlap() throws IOException {
    List<RowRange> ranges = new ArrayList<RowRange>();
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(15), true, Bytes.toBytes(40), false));
    ranges.add(new RowRange(Bytes.toBytes(20), true, Bytes.toBytes(30), false));
    ranges.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(50), false));
    ranges.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(70), false));
    ranges.add(new RowRange(Bytes.toBytes(90), true, Bytes.toBytes(100), false));
    ranges.add(new RowRange(Bytes.toBytes(95), true, Bytes.toBytes(100), false));
    List<RowRange> actualRanges = MultiRowRangeFilter.sortAndMerge(ranges);
    List<RowRange> expectedRanges = new ArrayList<RowRange>();
    expectedRanges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(70), false));
    expectedRanges.add(new RowRange(Bytes.toBytes(90), true, Bytes.toBytes(100), false));
    assertRangesEqual(expectedRanges, actualRanges);
  }

  @Test
  public void testMergeAndSortWithStartRowInclusive() throws IOException {
    List<RowRange> ranges = new ArrayList<RowRange>();
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(20), true, Bytes.toBytes(""), false));
    List<RowRange> actualRanges = MultiRowRangeFilter.sortAndMerge(ranges);
    List<RowRange> expectedRanges = new ArrayList<RowRange>();
    expectedRanges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(""), false));
    assertRangesEqual(expectedRanges, actualRanges);
  }

  @Test
  public void testMergeAndSortWithRowExclusive() throws IOException {
    List<RowRange> ranges = new ArrayList<RowRange>();
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges.add(new RowRange(Bytes.toBytes(20), false, Bytes.toBytes(""), false));
    List<RowRange> actualRanges = MultiRowRangeFilter.sortAndMerge(ranges);
    List<RowRange> expectedRanges = new ArrayList<RowRange>();
    expectedRanges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    expectedRanges.add(new RowRange(Bytes.toBytes(20), false, Bytes.toBytes(""), false));
    assertRangesEqual(expectedRanges, actualRanges);
  }

  @Test
  public void testMergeAndSortWithRowInclusive() throws IOException {
    List<RowRange> ranges = new ArrayList<RowRange>();
    ranges.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), true));
    ranges.add(new RowRange(Bytes.toBytes(20), false, Bytes.toBytes(""), false));
    List<RowRange> actualRanges = MultiRowRangeFilter.sortAndMerge(ranges);
    List<RowRange> expectedRanges = new ArrayList<RowRange>();
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
    tableName = TableName.valueOf("testMultiRowRangeFilterWithRangeOverlap");
    Table ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);
    generateRows(numRows, ht, family, qf, value);

    Scan scan = new Scan();
    scan.setMaxVersions();

    List<RowRange> ranges = new ArrayList<RowRange>();
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
    tableName = TableName.valueOf("testMultiRowRangeFilterWithoutRangeOverlap");
    Table ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);
    generateRows(numRows, ht, family, qf, value);

    Scan scan = new Scan();
    scan.setMaxVersions();

    List<RowRange> ranges = new ArrayList<RowRange>();
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
    tableName = TableName.valueOf("testMultiRowRangeFilterWithEmptyStartRow");
    Table ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);
    generateRows(numRows, ht, family, qf, value);
    Scan scan = new Scan();
    scan.setMaxVersions();

    List<RowRange> ranges = new ArrayList<RowRange>();
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
    tableName = TableName.valueOf("testMultiRowRangeFilterWithEmptyStopRow");
    Table ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);
    generateRows(numRows, ht, family, qf, value);
    Scan scan = new Scan();
    scan.setMaxVersions();

    List<RowRange> ranges = new ArrayList<RowRange>();
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
    tableName = TableName.valueOf("testMultiRowRangeFilterWithInclusive");
    Table ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);
    generateRows(numRows, ht, family, qf, value);

    Scan scan = new Scan();
    scan.setMaxVersions();

    List<RowRange> ranges = new ArrayList<RowRange>();
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
    tableName = TableName.valueOf("testMultiRowRangeFilterWithExclusive");
    Table ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);
    generateRows(numRows, ht, family, qf, value);

    Scan scan = new Scan();
    scan.setMaxVersions();

    List<RowRange> ranges = new ArrayList<RowRange>();
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
    tableName = TableName.valueOf("TestMultiRowRangeFilterWithFilterListAndOperator");
    Table ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);
    generateRows(numRows, ht, family, qf, value);

    Scan scan = new Scan();
    scan.setMaxVersions();

    List<RowRange> ranges1 = new ArrayList<RowRange>();
    ranges1.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges1.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(40), false));
    ranges1.add(new RowRange(Bytes.toBytes(60), true, Bytes.toBytes(70), false));

    MultiRowRangeFilter filter1 = new MultiRowRangeFilter(ranges1);

    List<RowRange> ranges2 = new ArrayList<RowRange>();
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
    tableName = TableName.valueOf("TestMultiRowRangeFilterWithFilterListOrOperator");
    Table ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);
    generateRows(numRows, ht, family, qf, value);

    Scan scan = new Scan();
    scan.setMaxVersions();

    List<RowRange> ranges1 = new ArrayList<RowRange>();
    ranges1.add(new RowRange(Bytes.toBytes(30), true, Bytes.toBytes(40), false));
    ranges1.add(new RowRange(Bytes.toBytes(10), true, Bytes.toBytes(20), false));
    ranges1.add(new RowRange(Bytes.toBytes(60), true, Bytes.toBytes(70), false));

    MultiRowRangeFilter filter1 = new MultiRowRangeFilter(ranges1);

    List<RowRange> ranges2 = new ArrayList<RowRange>();
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
    List<Cell> kvList = new ArrayList<Cell>();
    Result r;
    while ((r = scanner.next()) != null) {
      for (Cell kv : r.listCells()) {
        kvList.add(kv);
      }
    }
    return kvList;
  }

  private int getResultsSize(Table ht, Scan scan) throws IOException {
    ResultScanner scanner = ht.getScanner(scan);
    List<Cell> results = new ArrayList<Cell>();
    Result r;
    while ((r = scanner.next()) != null) {
      for (Cell kv : r.listCells()) {
        results.add(kv);
      }
    }
    return results.size();
  }
}
