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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ SecurityTests.class, LargeTests.class })
@SuppressWarnings("deprecation")
public class TestReadOnlyController {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReadOnlyController.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestReadOnlyController.class);
  private static final String READ_ONLY_CONTROLLER_NAME = ReadOnlyController.class.getName();
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final TableName TEST_TABLE = TableName.valueOf("read_only_test_table");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("read_only_table_col_fam");
  private static HRegionServer hRegionServer;
  private static HMaster hMaster;
  private static Configuration conf;
  private static Connection connection;
  private static SingleProcessHBaseCluster cluster;

  private static Table testTable;
  @Rule
  public TestName name = new TestName();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();

    // Shorten the run time of failed unit tests by limiting retries and the session timeout
    // threshold
    conf.setInt(HBASE_CLIENT_RETRIES_NUMBER, 1);
    conf.setInt(HConstants.ZK_SESSION_TIMEOUT, 1000);

    // Set up test class with Read-Only mode disabled so a table can be created
    conf.setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, false);

    // Add ReadOnlyController coprocessors to the master, region server, and regions
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, READ_ONLY_CONTROLLER_NAME);
    conf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY, READ_ONLY_CONTROLLER_NAME);
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, READ_ONLY_CONTROLLER_NAME);

    try {
      // Start the test cluster
      cluster = TEST_UTIL.startMiniCluster(1);

      hMaster = cluster.getMaster();
      hRegionServer = cluster.getRegionServerThreads().get(0).getRegionServer();
      connection = ConnectionFactory.createConnection(conf);

      // Create a test table
      testTable = TEST_UTIL.createTable(TEST_TABLE, TEST_FAMILY);
    } catch (Exception e) {
      // Delete the created table, and clean up the connection and cluster before throwing an
      // exception
      disableReadOnlyMode();
      TEST_UTIL.deleteTable(TEST_TABLE);
      connection.close();
      TEST_UTIL.shutdownMiniCluster();
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (connection != null) {
      connection.close();
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  private static void enableReadOnlyMode() {
    // Dynamically enable Read-Only mode if it is not active
    if (!isReadOnlyModeEnabled()) {
      LOG.info("Dynamically enabling Read-Only mode by setting {} to true",
        HConstants.HBASE_GLOBAL_READONLY_ENABLED_DEFAULT);
      conf.setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, true);
      notifyObservers();
    }
  }

  private static void disableReadOnlyMode() {
    // Dynamically disable Read-Only mode if it is active
    if (isReadOnlyModeEnabled()) {
      LOG.info("Dynamically disabling Read-Only mode by setting {} to false",
        HConstants.HBASE_GLOBAL_READONLY_ENABLED_DEFAULT);
      conf.setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, false);
      notifyObservers();
    }
  }

  private static boolean isReadOnlyModeEnabled() {
    return conf.getBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY,
      HConstants.HBASE_GLOBAL_READONLY_ENABLED_DEFAULT);
  }

  private static void notifyObservers() {
    LOG.info("Notifying observers about configuration changes");
    hMaster.getConfigurationManager().notifyAllObservers(conf);
    hRegionServer.getConfigurationManager().notifyAllObservers(conf);
  }

  // The test case for successfully creating a table with Read-Only mode disabled happens when
  // setting up the test class, so we only need a test function for a failed table creation.
  @Test
  public void testCannotCreateTableWithReadOnlyEnabled() throws IOException {
    // Expect an IOException to result from the createTable attempt since Read-Only mode is enabled
    enableReadOnlyMode();
    TableName newTable = TableName.valueOf("bad_read_only_test_table");
    exception.expect(IOException.class);
    exception.expectMessage("Operation not allowed in Read-Only Mode");

    // This should throw the IOException
    TEST_UTIL.createTable(newTable, TEST_FAMILY);
  }

  @Test
  public void testPutWithReadOnlyDisabled() throws IOException {
    // Successfully put a row in the table since Read-Only mode is disabled
    disableReadOnlyMode();
    final byte[] row2 = Bytes.toBytes("row2");
    final byte[] value = Bytes.toBytes("efgh");
    Put put = new Put(row2);
    put.addColumn(TEST_FAMILY, null, value);
    testTable.put(put);
  }

  @Test
  public void testCannotPutWithReadOnlyEnabled() throws IOException {
    // Prepare a Put command with Read-Only mode enabled
    enableReadOnlyMode();
    final byte[] row1 = Bytes.toBytes("row1");
    final byte[] value = Bytes.toBytes("abcd");
    Put put = new Put(row1);
    put.addColumn(TEST_FAMILY, null, value);

    // Expect an IOException to result from the Put attempt
    exception.expect(IOException.class);
    exception.expectMessage("Operation not allowed in Read-Only Mode");

    // This should throw the IOException
    testTable.put(put);
  }

  @Test
  public void testBatchPutWithReadOnlyDisabled() throws IOException, InterruptedException {
    // Successfully create and run a batch Put operation with Read-Only mode disabled
    disableReadOnlyMode();
    List<Row> actions = new ArrayList<>();
    actions.add(new Put(Bytes.toBytes("row10")).addColumn(TEST_FAMILY, null, Bytes.toBytes("10")));
    actions.add(new Delete(Bytes.toBytes("row10")));
    testTable.batch(actions, null);
  }

  @Test
  public void testCannotBatchPutWithReadOnlyEnabled() throws IOException, InterruptedException {
    // Create a batch Put operation that is expected to fail with Read-Only mode enabled
    enableReadOnlyMode();
    List<Row> actions = new ArrayList<>();
    actions.add(new Put(Bytes.toBytes("row11")).addColumn(TEST_FAMILY, null, Bytes.toBytes("11")));
    actions.add(new Delete(Bytes.toBytes("row11")));

    // Expect an IOException to result from the batch Put attempt
    exception.expect(IOException.class);
    exception.expectMessage("Operation not allowed in Read-Only Mode");

    // This should throw the IOException
    testTable.batch(actions, null);
  }
}
