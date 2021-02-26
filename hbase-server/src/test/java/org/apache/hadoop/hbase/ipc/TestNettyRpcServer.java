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
package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ RPCTests.class, MediumTests.class })
public class TestNettyRpcServer {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestNettyRpcServer.class);

  @Rule
  public TestName name = new TestName();
  private static HBaseTestingUtility TEST_UTIL;

  private static TableName TABLE;
  private static byte[] FAMILY = Bytes.toBytes("f1");
  private static byte[] PRIVATE_COL = Bytes.toBytes("private");
  private static byte[] PUBLIC_COL = Bytes.toBytes("public");

  @Before
  public void setup() {
    TABLE = TableName.valueOf(name.getMethodName());
  }

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.getConfiguration().set(
        RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY,
        NettyRpcServer.class.getName());
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testNettyRpcServer() throws Exception {
    final Table table = TEST_UTIL.createTable(TABLE, FAMILY);
    try {
      // put some test data
      List<Put> puts = new ArrayList<Put>(100);
      for (int i = 0; i < 100; i++) {
        Put p = new Put(Bytes.toBytes(i));
        p.addColumn(FAMILY, PRIVATE_COL, Bytes.toBytes("secret " + i));
        p.addColumn(FAMILY, PUBLIC_COL, Bytes.toBytes("info " + i));
        puts.add(p);
      }
      table.put(puts);

      // read to verify it.
      Scan scan = new Scan();
      scan.setCaching(16);
      ResultScanner rs = table.getScanner(scan);
      int rowcnt = 0;
      for (Result r : rs) {
        rowcnt++;
        int rownum = Bytes.toInt(r.getRow());
        assertTrue(r.containsColumn(FAMILY, PRIVATE_COL));
        assertEquals("secret " + rownum,
            Bytes.toString(r.getValue(FAMILY, PRIVATE_COL)));
        assertTrue(r.containsColumn(FAMILY, PUBLIC_COL));
        assertEquals("info " + rownum,
            Bytes.toString(r.getValue(FAMILY, PUBLIC_COL)));
      }
      assertEquals("Expected 100 rows returned", 100, rowcnt);
    } finally {
      table.close();
    }
  }

}
