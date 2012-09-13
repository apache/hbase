/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test cases for master to recover from ZK session expiry.
 */
@Category(MediumTests.class)
public class TestMasterZKSessionRecovery {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  /**
   * The default timeout is 5 minutes.
   * Shorten it so that the test won't wait for too long.
   */
  static {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong("hbase.master.zksession.recover.timeout", 50000);
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        MockLoadBalancer.class, LoadBalancer.class);
  }

  @Before
  public void setUp() throws Exception {
    // Start a cluster of one regionserver.
    TEST_UTIL.startMiniCluster(1);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Tests that the master does not call retainAssignment after recovery from
   * expired zookeeper session. Without the HBASE-6046 fix master always tries
   * to assign all the user regions by calling retainAssignment.
   */
  @Test
  public void testRegionAssignmentAfterMasterRecoveryDueToZKExpiry() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    cluster.startRegionServer();
    HMaster m = cluster.getMaster();
    // now the cluster is up. So assign some regions.
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    byte[][] SPLIT_KEYS = new byte[][] { Bytes.toBytes("a"), Bytes.toBytes("b"),
        Bytes.toBytes("c"), Bytes.toBytes("d"), Bytes.toBytes("e"), Bytes.toBytes("f"),
        Bytes.toBytes("g"), Bytes.toBytes("h"), Bytes.toBytes("i"), Bytes.toBytes("j") };

    String tableName = "testRegionAssignmentAfterMasterRecoveryDueToZKExpiry";
    admin.createTable(new HTableDescriptor(tableName), SPLIT_KEYS);
    ZooKeeperWatcher zooKeeperWatcher = HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL);
    ZKAssign.blockUntilNoRIT(zooKeeperWatcher);
    m.getZooKeeperWatcher().close();
    MockLoadBalancer.retainAssignCalled = false;
    m.abort("Test recovery from zk session expired", new KeeperException.SessionExpiredException());
    assertFalse(m.isStopped());
    // The recovered master should not call retainAssignment, as it is not a
    // clean startup.
    assertFalse("Retain assignment should not be called", MockLoadBalancer.retainAssignCalled);
  }

  static class MockLoadBalancer extends DefaultLoadBalancer {
    static boolean retainAssignCalled = false;

    @Override
    public Map<ServerName, List<HRegionInfo>> retainAssignment(
        Map<HRegionInfo, ServerName> regions, List<ServerName> servers) {
      retainAssignCalled = true;
      return super.retainAssignment(regions, servers);
    }
  }

  /**
   * Tests whether the logs are split when master recovers from a expired
   * zookeeper session and an RS goes down.
   */
  @Test(timeout = 60000)
  public void testLogSplittingAfterMasterRecoveryDueToZKExpiry() throws IOException,
      KeeperException, InterruptedException {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    cluster.startRegionServer();
    HMaster m = cluster.getMaster();
    // now the cluster is up. So assign some regions.
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    byte[][] SPLIT_KEYS = new byte[][] { Bytes.toBytes("1"), Bytes.toBytes("2"),
        Bytes.toBytes("3"), Bytes.toBytes("4"), Bytes.toBytes("5") };

    String tableName = "testLogSplittingAfterMasterRecoveryDueToZKExpiry";
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor("col");
    htd.addFamily(hcd);
    admin.createTable(htd, SPLIT_KEYS);
    ZooKeeperWatcher zooKeeperWatcher = HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL);
    ZKAssign.blockUntilNoRIT(zooKeeperWatcher);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);

    Put p = null;
    int numberOfPuts = 0;
    for (numberOfPuts = 0; numberOfPuts < 6; numberOfPuts++) {
      p = new Put(Bytes.toBytes(numberOfPuts));
      p.add(Bytes.toBytes("col"), Bytes.toBytes("ql"), Bytes.toBytes("value" + numberOfPuts));
      table.put(p);
    }
    m.getZooKeeperWatcher().close();
    m.abort("Test recovery from zk session expired", new KeeperException.SessionExpiredException());
    assertFalse(m.isStopped());
    cluster.getRegionServer(0).abort("Aborting");
    // Without patch for HBASE-6046 this test case will always timeout
    // with patch the test case should pass.
    Scan scan = new Scan();
    int numberOfRows = 0;
    ResultScanner scanner = table.getScanner(scan);
    Result[] result = scanner.next(1);
    while (result != null && result.length > 0) {
      numberOfRows++;
      result = scanner.next(1);
    }
    assertEquals("Number of rows should be equal to number of puts.", numberOfPuts, numberOfRows);
  }
}

