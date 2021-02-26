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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 */
@Category({FilterTests.class, MediumTests.class})
public class TestFuzzyRowAndColumnRangeFilter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFuzzyRowAndColumnRangeFilter.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Logger LOG = LoggerFactory.getLogger(TestFuzzyRowAndColumnRangeFilter.class);

  @Rule
  public TestName name = new TestName();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    // Nothing to do.
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    // Nothing to do.
  }

  @Test
  public void Test() throws Exception {
    String cf = "f";
    Table ht = TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()),
            Bytes.toBytes(cf), Integer.MAX_VALUE);

    // 10 byte row key - (2 bytes 4 bytes 4 bytes)
    // 4 byte qualifier
    // 4 byte value

    for (int i1 = 0; i1 < 2; i1++) {
      for (int i2 = 0; i2 < 5; i2++) {
        byte[] rk = new byte[10];

        ByteBuffer buf = ByteBuffer.wrap(rk);
        buf.clear();
        buf.putShort((short) 2);
        buf.putInt(i1);
        buf.putInt(i2);

        for (int c = 0; c < 5; c++) {
          byte[] cq = new byte[4];
          Bytes.putBytes(cq, 0, Bytes.toBytes(c), 0, 4);

          Put p = new Put(rk);
          p.setDurability(Durability.SKIP_WAL);
          p.addColumn(cf.getBytes(), cq, Bytes.toBytes(c));
          ht.put(p);
          LOG.info("Inserting: rk: " + Bytes.toStringBinary(rk) + " cq: "
                  + Bytes.toStringBinary(cq));
        }
      }
    }

    TEST_UTIL.flush();

    // test passes
    runTest(ht, 0, 10);

    // test fails
    runTest(ht, 1, 8);
  }

  private void runTest(Table hTable, int cqStart, int expectedSize) throws IOException {
    // [0, 2, ?, ?, ?, ?, 0, 0, 0, 1]
    byte[] fuzzyKey = new byte[10];
    ByteBuffer buf = ByteBuffer.wrap(fuzzyKey);
    buf.clear();
    buf.putShort((short) 2);
    for (int i = 0; i < 4; i++)
      buf.put((byte)63);
    buf.putInt((short)1);

    byte[] mask = new byte[] {0 , 0, 1, 1, 1, 1, 0, 0, 0, 0};

    Pair<byte[], byte[]> pair = new Pair<>(fuzzyKey, mask);
    FuzzyRowFilter fuzzyRowFilter = new FuzzyRowFilter(Lists.newArrayList(pair));
    ColumnRangeFilter columnRangeFilter = new ColumnRangeFilter(Bytes.toBytes(cqStart), true
            , Bytes.toBytes(4), true);
    //regular test
    runScanner(hTable, expectedSize, fuzzyRowFilter, columnRangeFilter);
    //reverse filter order test
    runScanner(hTable, expectedSize, columnRangeFilter, fuzzyRowFilter);
  }

  private void runScanner(Table hTable, int expectedSize, Filter... filters) throws IOException {
    String cf = "f";
    Scan scan = new Scan();
    scan.addFamily(cf.getBytes());
    FilterList filterList = new FilterList(filters);
    scan.setFilter(filterList);

    ResultScanner scanner = hTable.getScanner(scan);
    List<Cell> results = new ArrayList<>();
    Result result;
    long timeBeforeScan = System.currentTimeMillis();
    while ((result = scanner.next()) != null) {
      for (Cell kv : result.listCells()) {
        LOG.info("Got rk: " + Bytes.toStringBinary(CellUtil.cloneRow(kv)) + " cq: "
                + Bytes.toStringBinary(CellUtil.cloneQualifier(kv)));
        results.add(kv);
      }
    }
    long scanTime = System.currentTimeMillis() - timeBeforeScan;
    scanner.close();

    LOG.info("scan time = " + scanTime + "ms");
    LOG.info("found " + results.size() + " results");

    assertEquals(expectedSize, results.size());
  }
}
