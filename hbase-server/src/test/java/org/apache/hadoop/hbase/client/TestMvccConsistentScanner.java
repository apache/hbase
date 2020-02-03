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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ MediumTests.class, ClientTests.class })
public class TestMvccConsistentScanner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMvccConsistentScanner.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static Connection CONN;

  private static final byte[] CF = Bytes.toBytes("cf");

  private static final byte[] CQ1 = Bytes.toBytes("cq1");

  private static final byte[] CQ2 = Bytes.toBytes("cq2");

  private static final byte[] CQ3 = Bytes.toBytes("cq3");
  @Rule
  public TestName testName = new TestName();

  private TableName tableName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster(2);
    CONN = ConnectionFactory.createConnection(UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    CONN.close();
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    tableName = TableName.valueOf(testName.getMethodName().replaceAll("[^0-9a-zA-Z]", "_"));
    UTIL.createTable(tableName, CF);
    UTIL.waitTableAvailable(tableName);
  }

  private void put(byte[] row, byte[] cq, byte[] value) throws IOException {
    try (Table table = CONN.getTable(tableName)) {
      table.put(new Put(row).addColumn(CF, cq, value));
    }
  }

  private void move() throws IOException, InterruptedException {
    RegionInfo region =
        UTIL.getHBaseCluster().getRegions(tableName).stream().findAny().get().getRegionInfo();
    HRegionServer rs =
        UTIL.getHBaseCluster().getRegionServerThreads().stream().map(t -> t.getRegionServer())
            .filter(r -> !r.getOnlineTables().contains(tableName)).findAny().get();
    UTIL.getAdmin().move(region.getEncodedNameAsBytes(), rs.getServerName());
    while (UTIL.getRSForFirstRegionInTable(tableName) != rs) {
      Thread.sleep(100);
    }
  }

  @Test
  public void testRowAtomic() throws IOException, InterruptedException {
    byte[] row = Bytes.toBytes("row");
    put(row, CQ1, Bytes.toBytes(1));
    put(row, CQ2, Bytes.toBytes(2));
    try (Table table = CONN.getTable(tableName);
        ResultScanner scanner = table.getScanner(new Scan().setBatch(1).setCaching(1))) {
      Result result = scanner.next();
      assertEquals(1, result.rawCells().length);
      assertEquals(1, Bytes.toInt(result.getValue(CF, CQ1)));
      move();
      put(row, CQ3, Bytes.toBytes(3));
      result = scanner.next();
      assertEquals(1, result.rawCells().length);
      assertEquals(2, Bytes.toInt(result.getValue(CF, CQ2)));
      assertNull(scanner.next());
    }
  }

  @Test
  public void testCrossRowAtomicInRegion() throws IOException, InterruptedException {
    put(Bytes.toBytes("row1"), CQ1, Bytes.toBytes(1));
    put(Bytes.toBytes("row2"), CQ1, Bytes.toBytes(2));
    try (Table table = CONN.getTable(tableName);
        ResultScanner scanner = table.getScanner(new Scan().setCaching(1))) {
      Result result = scanner.next();
      assertArrayEquals(Bytes.toBytes("row1"), result.getRow());
      assertEquals(1, Bytes.toInt(result.getValue(CF, CQ1)));
      move();
      put(Bytes.toBytes("row3"), CQ1, Bytes.toBytes(3));
      result = scanner.next();
      assertArrayEquals(Bytes.toBytes("row2"), result.getRow());
      assertEquals(2, Bytes.toInt(result.getValue(CF, CQ1)));
      assertNull(scanner.next());
    }
  }
}
