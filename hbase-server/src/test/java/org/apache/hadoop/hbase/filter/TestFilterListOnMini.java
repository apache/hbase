/**
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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
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

/**
 * Tests filter Lists in ways that rely on a MiniCluster. Where possible, favor tests in
 * TestFilterList and TestFilterFromRegionSide instead.
 */
@Category({ MediumTests.class, FilterTests.class })
public class TestFilterListOnMini {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFilterListOnMini.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestFilterListOnMini.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testFiltersWithOR() throws Exception {
    TableName tn = TableName.valueOf(name.getMethodName());
    Table table = TEST_UTIL.createTable(tn, new String[] { "cf1", "cf2" });
    byte[] CF1 = Bytes.toBytes("cf1");
    byte[] CF2 = Bytes.toBytes("cf2");
    Put put1 = new Put(Bytes.toBytes("0"));
    put1.addColumn(CF1, Bytes.toBytes("col_a"), Bytes.toBytes(0));
    table.put(put1);
    Put put2 = new Put(Bytes.toBytes("0"));
    put2.addColumn(CF2, Bytes.toBytes("col_b"), Bytes.toBytes(0));
    table.put(put2);
    FamilyFilter filterCF1 =
        new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(CF1));
    FamilyFilter filterCF2 =
        new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(CF2));
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    filterList.addFilter(filterCF1);
    filterList.addFilter(filterCF2);
    Scan scan = new Scan();
    scan.setFilter(filterList);
    ResultScanner scanner = table.getScanner(scan);
    LOG.info("Filter list: " + filterList);
    for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      Assert.assertEquals(2, rr.size());
    }
  }

  /**
   * Test case for HBASE-21620
   */
  @Test
  public void testColumnPrefixFilterConcatWithOR() throws Exception {
    TableName tn = TableName.valueOf(name.getMethodName());
    byte[] cf1 = Bytes.toBytes("f1");
    byte[] row = Bytes.toBytes("row");
    byte[] value = Bytes.toBytes("value");
    String[] columns = new String[]{
      "1544768273917010001_lt",
      "1544768273917010001_w_1",
      "1544768723910010001_ca_1",
      "1544768723910010001_lt",
      "1544768723910010001_ut_1",
      "1544768723910010001_w_5",
      "1544769779710010001_lt",
      "1544769779710010001_w_5",
      "1544769883529010001_lt",
      "1544769883529010001_w_5",
      "1544769915805010001_lt",
      "1544769915805010001_w_5",
      "1544779883529010001_lt",
      "1544770422942010001_lt",
      "1544770422942010001_w_5"
    };
    Table table = TEST_UTIL.createTable(tn, cf1);
    for (int i = 0; i < columns.length; i++) {
      Put put = new Put(row).addColumn(cf1, Bytes.toBytes(columns[i]), value);
      table.put(put);
    }
    Scan scan = new Scan();
    scan.withStartRow(row).withStopRow(row, true)
        .setFilter(new FilterList(Operator.MUST_PASS_ONE,
            new ColumnPrefixFilter(Bytes.toBytes("1544770422942010001_")),
            new ColumnPrefixFilter(Bytes.toBytes("1544769883529010001_"))));
    ResultScanner scanner = table.getScanner(scan);
    Result result;
    int resultCount = 0;
    int cellCount = 0;
    while ((result = scanner.next()) != null) {
      cellCount += result.listCells().size();
      resultCount++;
    }
    Assert.assertEquals(resultCount, 1);
    Assert.assertEquals(cellCount, 4);
  }
}
