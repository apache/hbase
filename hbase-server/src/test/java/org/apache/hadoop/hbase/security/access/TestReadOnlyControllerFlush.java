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
package org.apache.hadoop.hbase.security.access;

import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Tag(SecurityTests.TAG)
@Tag(LargeTests.TAG)
@SuppressWarnings("deprecation")
public class TestReadOnlyControllerFlush {

  private final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final TableName TEST_TABLE = TableName.valueOf("read_only_flush_test_table");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("read_only_flush_col_fam");
  private static HRegionServer hRegionServer;
  private static HMaster hMaster;
  private static Configuration conf;
  private static Connection connection;
  private static SingleProcessHBaseCluster cluster;

  private static Table testTable;

  @BeforeEach
  public void beforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();

    // Shorten the run time of failed unit tests by limiting retries and the session timeout
    // threshold
    conf.setInt(HBASE_CLIENT_RETRIES_NUMBER, 1);
    conf.setInt(HConstants.ZK_SESSION_TIMEOUT, 1000);

    // Set up test class with Read-Only mode disabled so a table can be created
    conf.setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, false);

    try {
      // Start the test cluster
      cluster = TEST_UTIL.startMiniCluster(1);

      hMaster = cluster.getMaster();
      hRegionServer = cluster.getRegionServerThreads().get(0).getRegionServer();
      connection = ConnectionFactory.createConnection(conf);

      // Create a test table and insert a row so the memstore has data to flush
      testTable = TEST_UTIL.createTable(TEST_TABLE, TEST_FAMILY);
      Put put = new Put(Bytes.toBytes("row1"));
      put.addColumn(TEST_FAMILY, null, Bytes.toBytes("value1"));
      testTable.put(put);
    } catch (Exception e) {
      disableReadOnlyMode();
      TEST_UTIL.deleteTable(TEST_TABLE);
      if (connection != null) {
        connection.close();
      }
      TEST_UTIL.shutdownMiniCluster();
      throw new RuntimeException(e);
    }
  }

  @AfterEach
  public void afterClass() throws Exception {
    if (connection != null) {
      connection.close();
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  private static void enableReadOnlyMode() {
    SecureTestUtil.enableReadOnlyMode(conf, hMaster, hRegionServer);
  }

  private static void disableReadOnlyMode() {
    SecureTestUtil.disableReadOnlyMode(conf, hMaster, hRegionServer);
  }

  @Test
  public void testFlushTableWithReadOnlyDisabled() throws IOException {
    disableReadOnlyMode();
    try (Admin admin = TEST_UTIL.getAdmin()) {
      admin.flush(TEST_TABLE);
    }
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  public void testCannotFlushTableWithReadOnlyEnabled() throws IOException {
    enableReadOnlyMode();
    try (Admin admin = TEST_UTIL.getAdmin()) {
      IOException exception = assertThrows(IOException.class, () -> {
        admin.flush(TEST_TABLE);
      });
      assertTrue(exception.getMessage().contains("Operation not allowed in Read-Only Mode"));
    }
  }
}
