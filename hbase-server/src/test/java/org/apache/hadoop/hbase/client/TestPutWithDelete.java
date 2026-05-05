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

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(MediumTests.TAG)
@Tag(ClientTests.TAG)
public class TestPutWithDelete {

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testHbasePutDeleteCell(TestInfo testInfo) throws Exception {
    final TableName tableName = TableName.valueOf(testInfo.getTestMethod().get().getName());
    final byte[] rowKey = Bytes.toBytes("12345");
    final byte[] family = Bytes.toBytes("cf");
    try (Table table = TEST_UTIL.createTable(tableName, family)) {
      TEST_UTIL.waitTableAvailable(tableName.getName(), 5000);
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
      assertTrue(Bytes.toString(result.getValue(family, Bytes.toBytes("A"))).equals("a"),
        "Column A value should be a");
      assertTrue(Bytes.toString(result.getValue(family, Bytes.toBytes("B"))).equals("b"),
        "Column B value should be b");
      assertTrue(Bytes.toString(result.getValue(family, Bytes.toBytes("C"))).equals("c"),
        "Column C value should be c");
      assertTrue(Bytes.toString(result.getValue(family, Bytes.toBytes("D"))).equals("d"),
        "Column D value should be d");
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
      assertTrue(Bytes.toString(result.getValue(family, Bytes.toBytes("A"))).equals("a1"),
        "Column A value should be a1");
      assertTrue(Bytes.toString(result.getValue(family, Bytes.toBytes("B"))).equals("b1"),
        "Column B value should be b1");
      assertTrue(result.getValue(family, Bytes.toBytes("C")) == null, "Column C should not exist");
      assertTrue(Bytes.toString(result.getValue(family, Bytes.toBytes("D"))).equals("d1"),
        "Column D value should be d1");
    }
  }
}
