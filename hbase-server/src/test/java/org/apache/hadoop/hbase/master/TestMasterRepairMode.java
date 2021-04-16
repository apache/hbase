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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, LargeTests.class })
public class TestMasterRepairMode {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterRepairMode.class);

  @Rule
  public TestName name = new TestName();

  private static final Logger LOG = LoggerFactory.getLogger(TestMasterRepairMode.class);

  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");

  private static HBaseTestingUtility TEST_UTIL;

  @Before
  public void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private void enableMaintenanceMode() {
    Configuration c = TEST_UTIL.getConfiguration();
    c.setBoolean(HMaster.MAINTENANCE_MODE, true);
    c.setInt("hbase.master.init.timeout.localHBaseCluster", 30000);
  }

  @Test
  public void testNewCluster() throws Exception {
    enableMaintenanceMode();

    TEST_UTIL.startMiniCluster(
      StartMiniClusterOption.builder().numRegionServers(0).numDataNodes(3).build());

    Connection conn = TEST_UTIL.getConnection();
    assertTrue(conn.getAdmin().isMasterInMaintenanceMode());

    try (Table table = conn.getTable(TableName.META_TABLE_NAME);
      ResultScanner scanner = table.getScanner(new Scan())) {
      assertNotNull("Could not read meta.", scanner.next());
    }
  }

  @Test
  public void testExistingCluster() throws Exception {
    TableName testRepairMode = TableName.valueOf(name.getMethodName());

    TEST_UTIL.startMiniCluster();
    Table t = TEST_UTIL.createTable(testRepairMode, FAMILYNAME);
    Put p = new Put(Bytes.toBytes("r"));
    p.addColumn(FAMILYNAME, Bytes.toBytes("c"), new byte[0]);
    t.put(p);

    TEST_UTIL.shutdownMiniHBaseCluster();

    LOG.info("Starting master-only");

    enableMaintenanceMode();
    TEST_UTIL.startMiniHBaseCluster(
      StartMiniClusterOption.builder().numRegionServers(0).createRootDir(false).build());

    Connection conn = TEST_UTIL.getConnection();
    assertTrue(conn.getAdmin().isMasterInMaintenanceMode());

    try (Table table = conn.getTable(TableName.META_TABLE_NAME);
      ResultScanner scanner = table.getScanner(HConstants.TABLE_FAMILY);
      Stream<Result> results = StreamSupport.stream(scanner.spliterator(), false)) {
      assertTrue("Did not find user table records while reading hbase:meta",
        results.anyMatch(r -> Arrays.equals(r.getRow(), testRepairMode.getName())));
    }
    try (AsyncConnection asyncConn =
      ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get()) {
      // use async table so we can set the timeout and retry value to let the operation fail fast
      AsyncTable<?> table = asyncConn.getTableBuilder(testRepairMode)
        .setScanTimeout(5, TimeUnit.SECONDS).setMaxRetries(2).build();
      assertThrows("Should not be able to access user-space tables in repair mode.",
        Exception.class, () -> {
          try (ResultScanner scanner = table.getScanner(new Scan())) {
            scanner.next();
          }
        });
    }
  }
}
