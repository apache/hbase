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

package org.apache.hadoop.hbase;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.TimestampType;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category({MediumTests.class})
public class TestClockWithCluster {
  private static final Log LOG = LogFactory.getLog(TestClockWithCluster.class);
  private static final  HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static Connection connection;
  private byte[] columnFamily = Bytes.toBytes("testCF");
  @BeforeClass
  public static void setupClass() throws Exception {
    UTIL.startMiniCluster(1);
    connection = ConnectionFactory.createConnection(UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    connection.close();
    UTIL.shutdownMiniCluster();
  }

  private void verifyTimestamps(Table table, final byte[] f, int startRow, int endRow,
      TimestampType timestamp) throws IOException {
    for (int i = startRow; i < endRow; i++) {
      String failMsg = "Failed verification of row :" + i;
      byte[] data = Bytes.toBytes(String.valueOf(i));
      Get get = new Get(data);
      Result result = table.get(get);
      Cell cell = result.getColumnLatestCell(f, null);
      assertTrue(failMsg, timestamp.isLikelyOfType(cell.getTimestamp()));
    }
  }

  @Test
  public void testNewTablesAreCreatedWithSystemClock() throws IOException {
    Admin admin = connection.getAdmin();
    TableName tableName = TableName.valueOf("TestNewTablesAreSystemByDefault");
    admin.createTable(new HTableDescriptor(tableName).addFamily(new
      HColumnDescriptor(columnFamily)));

    Table table = connection.getTable(tableName);

    ClockType clockType = admin.getTableDescriptor(tableName).getClockType();
    assertEquals(ClockType.SYSTEM, clockType);
    // write
    UTIL.loadNumericRows(table, columnFamily, 0, 1000);
    // read , check if the it is same.
    UTIL.verifyNumericRows(table, Bytes.toBytes("testCF"), 0, 1000, 0);

    // This check will be useful if Clock type were to be system monotonic or HLC.
    verifyTimestamps(table, columnFamily, 0, 1000, TimestampType.PHYSICAL);
  }

  @Test
  public void testMetaTableClockTypeIsSystem() throws IOException {
    Admin admin = connection.getAdmin();
    Table table = connection.getTable(TableName.META_TABLE_NAME);
    ClockType clockType = admin.getTableDescriptor(TableName.META_TABLE_NAME).getClockType();
    assertEquals(ClockType.SYSTEM, clockType);
  }

  @Test
  public void testMetaTableTimestampsAreSystem() throws IOException {
    // Checks timestamps of whatever is present in meta table currently.
    // ToDo: Include complete meta table sample with all column families to check all paths of
    // meta table modification.
    Table table = connection.getTable(TableName.META_TABLE_NAME);
    Result result = table.getScanner(new Scan()).next();
    for (Cell cell : result.rawCells()) {
      assertTrue(TimestampType.PHYSICAL.isLikelyOfType(cell.getTimestamp()));
    }
  }
}