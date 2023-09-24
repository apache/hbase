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
package org.apache.hadoop.hbase.client.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, MediumTests.class })
public class TestMultiThreadedClientExample {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static String tableName = "test_mt_table";
  private static Table table;
  static final TableName MY_TABLE_NAME = TableName.valueOf(tableName);
  private static byte[] familyName = Bytes.toBytes("d");
  private static byte[] columnName = Bytes.toBytes("col");

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMultiThreadedClientExample.class);

  @BeforeClass
  public static void setup() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    table = TEST_UTIL.createTable(MY_TABLE_NAME, familyName);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.deleteTable(MY_TABLE_NAME);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMultiThreadedClientExample() throws Exception {
    MultiThreadedClientExample example = new MultiThreadedClientExample();
    example.setConf(TEST_UTIL.getConfiguration());
    String[] args = { tableName, "200" };
    // Define assertions to check the returned data here
    assertEquals(0, example.run(args));
    // Define assertions to check the row count of the table
    int rows = TEST_UTIL.countRows(table);
    assertNotEquals(0, rows);
  }
}
