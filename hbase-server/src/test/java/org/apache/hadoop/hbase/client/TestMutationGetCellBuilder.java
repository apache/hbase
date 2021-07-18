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

import java.util.Arrays;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({MediumTests.class, ClientTests.class})
public class TestMutationGetCellBuilder {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMutationGetCellBuilder.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMutationGetCellBuilder() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final byte[] rowKey = Bytes.toBytes("12345678");
    final byte[] uselessRowKey = Bytes.toBytes("123");
    final byte[] family = Bytes.toBytes("cf");
    final byte[] qualifier = Bytes.toBytes("foo");
    final long now = EnvironmentEdgeManager.currentTime();
    try (Table table = TEST_UTIL.createTable(tableName, family)) {
      TEST_UTIL.waitTableAvailable(tableName.getName(), 5000);
      // put one row
      Put put = new Put(rowKey);
      CellBuilder cellBuilder = put.getCellBuilder().setQualifier(qualifier)
              .setFamily(family).setValue(Bytes.toBytes("bar")).setTimestamp(now);
      //setRow is useless
      cellBuilder.setRow(uselessRowKey);
      put.add(cellBuilder.build());
      byte[] cloneRow = CellUtil.cloneRow(cellBuilder.build());
      assertTrue("setRow must be useless", !Arrays.equals(cloneRow, uselessRowKey));
      table.put(put);

      // get the row back and assert the values
      Get get = new Get(rowKey);
      get.setTimestamp(now);
      Result result = table.get(get);
      assertTrue("row key must be same", Arrays.equals(result.getRow(), rowKey));
      assertTrue("Column foo value should be bar",
          Bytes.toString(result.getValue(family, qualifier)).equals("bar"));

      //Delete that row
      Delete delete = new Delete(rowKey);
      cellBuilder = delete.getCellBuilder().setQualifier(qualifier)
              .setFamily(family);
      //if this row has been deleted,then can check setType is useless.
      cellBuilder.setType(Cell.Type.Put);
      delete.add(cellBuilder.build());
      table.delete(delete);

      //check this row whether exist
      get = new Get(rowKey);
      get.setTimestamp(now);
      result = table.get(get);
      assertTrue("Column foo should not exist",
              result.getValue(family, qualifier) == null);
    }
  }
}


