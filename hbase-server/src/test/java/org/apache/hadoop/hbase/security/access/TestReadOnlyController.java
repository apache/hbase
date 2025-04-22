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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
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

  private static final Logger LOG = LoggerFactory.getLogger(TestAccessController.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static TableName TEST_TABLE = TableName.valueOf("readonlytesttable");
  private static byte[] TEST_FAMILY = Bytes.toBytes("readonlytablecolfam");
  private static Configuration conf;
  private static Connection connection;

  private static RegionServerCoprocessorEnvironment RSCP_ENV;

  private static Table TestTable;
  @Rule
  public TestName name = new TestName();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    // Only try once so that if there is failure in connection then test should fail faster
    conf.setInt("hbase.ipc.client.connect.max.retries", 1);
    // Shorter session timeout is added so that in case failures test should not take more time
    conf.setInt(HConstants.ZK_SESSION_TIMEOUT, 1000);
    // Enable ReadOnly mode for the cluster
    conf.setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, true);
    // Add the ReadOnlyController coprocessor for region server to interrupt any write operation
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, ReadOnlyController.class.getName());
    // Add the ReadOnlyController coprocessor to for master to interrupt any write operation
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, ReadOnlyController.class.getName());
    // Start the test cluster
    TEST_UTIL.startMiniCluster(2);
    // Get connection to the HBase
    connection = ConnectionFactory.createConnection(conf);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (connection != null) {
      connection.close();
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(expected = IOException.class)
  public void testCreateTable() throws IOException {
    TEST_UTIL.createTable(TEST_TABLE, TEST_FAMILY);
  }
}
