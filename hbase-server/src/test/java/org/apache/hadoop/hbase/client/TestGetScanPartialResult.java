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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Testcase for HBASE-21032, where use the wrong readType from a Scan instance which is actually a
 * get scan and cause returning only 1 cell per rpc call.
 */
@Category({ ClientTests.class, MediumTests.class })
public class TestGetScanPartialResult {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestGetScanPartialResult.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final TableName TABLE = TableName.valueOf("table");
  private static final byte[] CF = { 'c', 'f' };
  private static final byte[] ROW = { 'r', 'o', 'w' };
  private static final int VALUE_SIZE = 10000;
  private static final int NUM_COLUMNS = 300;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster();
    TEST_UTIL.createTable(TABLE, CF);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private static byte[] makeLargeValue(int size) {
    byte[] v = new byte[size];
    for (int i = 0; i < size; i++) {
      v[i] = 0;
    }
    return v;
  }

  @Test
  public void test() throws IOException {
    try (Table t = TEST_UTIL.getConnection().getTable(TABLE)) {
      // populate a row with bunch of columns and large values
      // to cause scan to return partials
      byte[] val = makeLargeValue(VALUE_SIZE);
      Put p = new Put(ROW);
      for (int i = 0; i < NUM_COLUMNS; i++) {
        p.addColumn(CF, Integer.toString(i).getBytes(), val);
      }
      t.put(p);

      Scan scan = new Scan();
      scan.withStartRow(ROW);
      scan.withStopRow(ROW, true);
      scan.setAllowPartialResults(true);
      scan.setMaxResultSize(2L * 1024 * 1024);
      scan.readVersions(1);
      ResultScanner scanner = t.getScanner(scan);

      int nResults = 0;
      int nCells = 0;
      for (Result result = scanner.next(); (result != null); result = scanner.next()) {
        nResults++;
        nCells += result.listCells().size();
      }
      assertEquals(NUM_COLUMNS, nCells);
      assertTrue(nResults < 5);
    }
  }
}
