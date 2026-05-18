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

import java.util.Arrays;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(MediumTests.TAG)
@Tag(ClientTests.TAG)
public class TestMutationGetCellBuilder {

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
  public void testMutationGetCellBuilder(TestInfo testInfo) throws Exception {
    final TableName tableName = TableName.valueOf(testInfo.getTestMethod().get().getName());
    final byte[] rowKey = Bytes.toBytes("12345678");
    final byte[] uselessRowKey = Bytes.toBytes("123");
    final byte[] family = Bytes.toBytes("cf");
    final byte[] qualifier = Bytes.toBytes("foo");
    final long now = EnvironmentEdgeManager.currentTime();
    try (Table table = TEST_UTIL.createTable(tableName, family)) {
      TEST_UTIL.waitTableAvailable(tableName.getName(), 5000);
      // put one row
      Put put = new Put(rowKey);
      CellBuilder cellBuilder = put.getCellBuilder().setQualifier(qualifier).setFamily(family)
        .setValue(Bytes.toBytes("bar")).setTimestamp(now);
      // setRow is useless
      cellBuilder.setRow(uselessRowKey);
      put.add(cellBuilder.build());
      byte[] cloneRow = CellUtil.cloneRow(cellBuilder.build());
      assertTrue(!Arrays.equals(cloneRow, uselessRowKey), "setRow must be useless");
      table.put(put);

      // get the row back and assert the values
      Get get = new Get(rowKey);
      get.setTimestamp(now);
      Result result = table.get(get);
      assertTrue(Arrays.equals(result.getRow(), rowKey), "row key must be same");
      assertTrue(Bytes.toString(result.getValue(family, qualifier)).equals("bar"),
        "Column foo value should be bar");

      // Delete that row
      Delete delete = new Delete(rowKey);
      cellBuilder = delete.getCellBuilder().setQualifier(qualifier).setFamily(family);
      // if this row has been deleted,then can check setType is useless.
      cellBuilder.setType(Cell.Type.Put);
      delete.add(cellBuilder.build());
      table.delete(delete);

      // check this row whether exist
      get = new Get(rowKey);
      get.setTimestamp(now);
      result = table.get(get);
      assertTrue(result.getValue(family, qualifier) == null, "Column foo should not exist");
    }
  }
}
