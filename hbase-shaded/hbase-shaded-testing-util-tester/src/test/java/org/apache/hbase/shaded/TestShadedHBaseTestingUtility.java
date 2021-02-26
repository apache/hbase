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
package org.apache.hbase.shaded;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
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
public class TestShadedHBaseTestingUtility {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestShadedHBaseTestingUtility.class);

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCreateTable() throws Exception {
    TableName tableName = TableName.valueOf("test");

    Table table = TEST_UTIL.createTable(tableName, "cf");

    Put put1 = new Put(Bytes.toBytes("r1"));
    put1.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("c"), Bytes.toBytes(1));
    table.put(put1);

    Put put2 = new Put(Bytes.toBytes("r2"));
    put2.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("c"), Bytes.toBytes(2));
    table.put(put2);

    int rows = TEST_UTIL.countRows(tableName);
    assertEquals(2, rows);
  }
}
