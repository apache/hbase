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

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({MediumTests.class, ClientTests.class})
public class TestPutWithDelete {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestPutWithDelete.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

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

  @Test
  public void testHbasePutDeleteCell() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final byte[] rowKey = Bytes.toBytes("12345");
    final byte[] family = Bytes.toBytes("cf");
    Table table = TEST_UTIL.createTable(tableName, family);
    TEST_UTIL.waitTableAvailable(tableName.getName(), 5000);
    try {
      // put one row
      Put put = new Put(rowKey);
      put.addColumn(family, Bytes.toBytes("A"), Bytes.toBytes("a"));
      put.addColumn(family, Bytes.toBytes("B"), Bytes.toBytes("b"));
      put.addColumn(family, Bytes.toBytes("C"), Bytes.toBytes("c"));
      put.addColumn(family, Bytes.toBytes("D"), Bytes.toBytes("d"));
      table.put(put);
      // get row back and assert the values
      Get get = new Get(rowKey);
      Result result = table.get(get);
      assertTrue("Column A value should be a",
          Bytes.toString(result.getValue(family, Bytes.toBytes("A"))).equals("a"));
      assertTrue("Column B value should be b",
          Bytes.toString(result.getValue(family, Bytes.toBytes("B"))).equals("b"));
      assertTrue("Column C value should be c",
          Bytes.toString(result.getValue(family, Bytes.toBytes("C"))).equals("c"));
      assertTrue("Column D value should be d",
          Bytes.toString(result.getValue(family, Bytes.toBytes("D"))).equals("d"));
      // put the same row again with C column deleted
      put = new Put(rowKey);
      put.addColumn(family, Bytes.toBytes("A"), Bytes.toBytes("a1"));
      put.addColumn(family, Bytes.toBytes("B"), Bytes.toBytes("b1"));
      KeyValue marker = new KeyValue(rowKey, family, Bytes.toBytes("C"),
          HConstants.LATEST_TIMESTAMP, KeyValue.Type.DeleteColumn);
      put.addColumn(family, Bytes.toBytes("D"), Bytes.toBytes("d1"));
      put.add(marker);
      table.put(put);
      // get row back and assert the values
      get = new Get(rowKey);
      result = table.get(get);
      assertTrue("Column A value should be a1",
          Bytes.toString(result.getValue(family, Bytes.toBytes("A"))).equals("a1"));
      assertTrue("Column B value should be b1",
          Bytes.toString(result.getValue(family, Bytes.toBytes("B"))).equals("b1"));
      assertTrue("Column C should not exist",
          result.getValue(family, Bytes.toBytes("C")) == null);
      assertTrue("Column D value should be d1",
          Bytes.toString(result.getValue(family, Bytes.toBytes("D"))).equals("d1"));
    } finally {
      table.close();
    }
  }
}


