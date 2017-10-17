/**
 *
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Tests filter Lists in ways that rely on a MiniCluster. Where possible, favor tests in
 * TestFilterList and TestFilterFromRegionSide instead.
 */
@Category({ MediumTests.class, FilterTests.class })
public class TestFilterListOnMini {

  private static final Log LOG = LogFactory.getLog(TestFilterListOnMini.class);
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
}
