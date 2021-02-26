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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

/**
 * Fix an infinite loop in {@link AsyncNonMetaRegionLocator}, see the comments on HBASE-21943 for
 * more details.
 */
@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTableLocateRegionForDeletedTable {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableLocateRegionForDeletedTable.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] QUALIFIER = Bytes.toBytes("cq");

  private static byte[] VALUE = Bytes.toBytes("value");

  private static AsyncConnection ASYNC_CONN;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    TEST_UTIL.getAdmin().balancerSwitch(false, true);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
    assertFalse(ASYNC_CONN.isClosed());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Closeables.close(ASYNC_CONN, true);
    assertTrue(ASYNC_CONN.isClosed());
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws IOException, InterruptedException {
    try (Table table = TEST_UTIL.getConnection().getTable(TABLE_NAME)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(FAMILY, QUALIFIER, VALUE));
      }
    }
    TEST_UTIL.getAdmin().split(TABLE_NAME, Bytes.toBytes(50));
    TEST_UTIL.waitFor(60000,
      () -> TEST_UTIL.getMiniHBaseCluster().getRegions(TABLE_NAME).size() == 2);
    // make sure we can access the split regions
    try (Table table = TEST_UTIL.getConnection().getTable(TABLE_NAME)) {
      for (int i = 0; i < 100; i++) {
        assertFalse(table.get(new Get(Bytes.toBytes(i))).isEmpty());
      }
    }
    // let's cache the two old locations
    AsyncTableRegionLocator locator = ASYNC_CONN.getRegionLocator(TABLE_NAME);
    locator.getRegionLocation(Bytes.toBytes(0)).join();
    locator.getRegionLocation(Bytes.toBytes(99)).join();
    // recreate the table
    TEST_UTIL.getAdmin().disableTable(TABLE_NAME);
    TEST_UTIL.getAdmin().deleteTable(TABLE_NAME);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    // confirm that we can still get the correct location
    assertFalse(ASYNC_CONN.getTable(TABLE_NAME).exists(new Get(Bytes.toBytes(99))).join());
  }
}
