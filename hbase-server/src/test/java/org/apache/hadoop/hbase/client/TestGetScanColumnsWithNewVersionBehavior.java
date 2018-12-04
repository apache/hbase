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

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.ArrayList;

/**
 * Testcase for HBASE-21032, where use the wrong readType from a Scan instance which is actually a
 * get scan and cause returning only 1 cell per rpc call.
 */
@Category({ ClientTests.class, MediumTests.class })
public class TestGetScanColumnsWithNewVersionBehavior {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestGetScanColumnsWithNewVersionBehavior.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final TableName TABLE = TableName.valueOf("table");
  private static final byte[] CF = { 'c', 'f' };
  private static final byte[] ROW = { 'r', 'o', 'w' };
  private static final byte[] COLA = { 'a' };
  private static final byte[] COLB = { 'b' };
  private static final byte[] COLC = { 'c' };
  private static final long TS = 42;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    ColumnFamilyDescriptor cd = ColumnFamilyDescriptorBuilder
        .newBuilder(CF)
        .setNewVersionBehavior(true)
        .build();
    TEST_UTIL.createTable(TableDescriptorBuilder
        .newBuilder(TABLE)
        .setColumnFamily(cd)
        .build(), null);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws IOException {
    try (Table t = TEST_UTIL.getConnection().getTable(TABLE)) {
      Cell [] expected = new Cell[2];
      expected[0] = new KeyValue(ROW, CF, COLA, TS, COLA);
      expected[1] = new KeyValue(ROW, CF, COLC, TS, COLC);

      Put p = new Put(ROW);
      p.addColumn(CF, COLA, TS, COLA);
      p.addColumn(CF, COLB, TS, COLB);
      p.addColumn(CF, COLC, TS, COLC);
      t.put(p);

      // check get request
      Get get = new Get(ROW);
      get.addColumn(CF, COLA);
      get.addColumn(CF, COLC);
      Result getResult = t.get(get);
      assertArrayEquals(expected, getResult.rawCells());

      // check scan request
      Scan scan = new Scan(ROW);
      scan.addColumn(CF, COLA);
      scan.addColumn(CF, COLC);
      ResultScanner scanner = t.getScanner(scan);
      List scanResult = new ArrayList<Cell>();
      for (Result result = scanner.next(); (result != null); result = scanner.next()) {
          scanResult.addAll(result.listCells());
      }
      assertArrayEquals(expected, scanResult.toArray(new Cell[scanResult.size()]));
    }
  }
}
