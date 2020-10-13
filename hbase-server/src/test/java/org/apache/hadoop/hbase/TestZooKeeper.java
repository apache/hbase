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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coordination.ZkSplitLogWorkerCoordination;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.balancer.SimpleLoadBalancer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MiscTests.class, MediumTests.class})
public class TestZooKeeper {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestZooKeeper.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestZooKeeper.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Test we can first start the ZK cluster by itself
    Configuration conf = TEST_UTIL.getConfiguration();
    // A couple of tests rely on master expiring ZK session, hence killing the only master. So it
    // makes sense only for ZK registry. Enforcing it.
    conf.set(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
        HConstants.ZK_CONNECTION_REGISTRY_CLASS);
    TEST_UTIL.startMiniDFSCluster(2);
    TEST_UTIL.startMiniZKCluster();
    conf.setInt(HConstants.ZK_SESSION_TIMEOUT, 1000);
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, MockLoadBalancer.class,
        LoadBalancer.class);
    TEST_UTIL.startMiniDFSCluster(2);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(2).numRegionServers(2).build();
    TEST_UTIL.startMiniHBaseCluster(option);
  }

  @After
  public void after() throws Exception {
    try {
      TEST_UTIL.getHBaseCluster().waitForActiveAndReadyMaster(10000);
      // Some regionserver could fail to delete its znode.
      // So shutdown could hang. Let's kill them all instead.
      TEST_UTIL.getHBaseCluster().killAll();

      // Still need to clean things up
      TEST_UTIL.shutdownMiniHBaseCluster();
    } finally {
      TEST_UTIL.getTestFileSystem().delete(CommonFSUtils.getRootDir(TEST_UTIL.getConfiguration()),
        true);
      ZKUtil.deleteNodeRecursively(TEST_UTIL.getZooKeeperWatcher(), "/hbase");
    }
  }

  @Test
  public void testRegionServerSessionExpired() throws Exception {
    LOG.info("Starting " + name.getMethodName());
    TEST_UTIL.expireRegionServerSession(0);
    testSanity(name.getMethodName());
  }

  @Test
  public void testMasterSessionExpired() throws Exception {
    LOG.info("Starting " + name.getMethodName());
    TEST_UTIL.expireMasterSession();
    testSanity(name.getMethodName());
  }

  /**
   * Master recovery when the znode already exists. Internally, this
   *  test differs from {@link #testMasterSessionExpired} because here
   *  the master znode will exist in ZK.
   */
  @Test
  public void testMasterZKSessionRecoveryFailure() throws Exception {
    LOG.info("Starting " + name.getMethodName());
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster m = cluster.getMaster();
    m.abort("Test recovery from zk session expired",
        new KeeperException.SessionExpiredException());
    assertTrue(m.isStopped()); // Master doesn't recover any more
    testSanity(name.getMethodName());
  }

  /**
   * Make sure we can use the cluster
   */
  private void testSanity(final String testName) throws Exception {
    String tableName = testName + "_" + System.currentTimeMillis();
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("fam")).build();
    LOG.info("Creating table " + tableName);
    Admin admin = TEST_UTIL.getAdmin();
    try {
      admin.createTable(desc);
    } finally {
      admin.close();
    }

    Table table = TEST_UTIL.getConnection().getTable(desc.getTableName());
    Put put = new Put(Bytes.toBytes("testrow"));
    put.addColumn(Bytes.toBytes("fam"), Bytes.toBytes("col"), Bytes.toBytes("testdata"));
    LOG.info("Putting table " + tableName);
    table.put(put);
    table.close();
  }

  /**
   * Tests that the master does not call retainAssignment after recovery from expired zookeeper
   * session. Without the HBASE-6046 fix master always tries to assign all the user regions by
   * calling retainAssignment.
   */
  @Test
  public void testRegionAssignmentAfterMasterRecoveryDueToZKExpiry() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    cluster.startRegionServer();
    cluster.waitForActiveAndReadyMaster(10000);
    HMaster m = cluster.getMaster();
    final ZKWatcher zkw = m.getZooKeeper();
    // now the cluster is up. So assign some regions.
    try (Admin admin = TEST_UTIL.getAdmin()) {
      byte[][] SPLIT_KEYS = new byte[][] { Bytes.toBytes("a"), Bytes.toBytes("b"),
          Bytes.toBytes("c"), Bytes.toBytes("d"), Bytes.toBytes("e"), Bytes.toBytes("f"),
          Bytes.toBytes("g"), Bytes.toBytes("h"), Bytes.toBytes("i"), Bytes.toBytes("j") };
      TableDescriptor htd =
          TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
              .setColumnFamily(ColumnFamilyDescriptorBuilder.of(HConstants.CATALOG_FAMILY)).build();
      admin.createTable(htd, SPLIT_KEYS);
      TEST_UTIL.waitUntilNoRegionsInTransition(60000);
      m.getZooKeeper().close();
      MockLoadBalancer.retainAssignCalled = false;
      final int expectedNumOfListeners = countPermanentListeners(zkw);
      m.abort("Test recovery from zk session expired",
          new KeeperException.SessionExpiredException());
      assertTrue(m.isStopped()); // Master doesn't recover any more
      // The recovered master should not call retainAssignment, as it is not a
      // clean startup.
      assertFalse("Retain assignment should not be called", MockLoadBalancer.retainAssignCalled);
      // number of listeners should be same as the value before master aborted
      // wait for new master is initialized
      cluster.waitForActiveAndReadyMaster(120000);
      final HMaster newMaster = cluster.getMasterThread().getMaster();
      assertEquals(expectedNumOfListeners, countPermanentListeners(newMaster.getZooKeeper()));
    }
  }

  /**
   * Count listeners in zkw excluding listeners, that belongs to workers or other
   * temporary processes.
   */
  private int countPermanentListeners(ZKWatcher watcher) {
    return countListeners(watcher, ZkSplitLogWorkerCoordination.class);
  }

  /**
   * Count listeners in zkw excluding provided classes
   */
  private int countListeners(ZKWatcher watcher, Class<?>... exclude) {
    int cnt = 0;
    for (Object o : watcher.getListeners()) {
      boolean skip = false;
      for (Class<?> aClass : exclude) {
        if (aClass.isAssignableFrom(o.getClass())) {
          skip = true;
          break;
        }
      }
      if (!skip) {
        cnt += 1;
      }
    }
    return cnt;
  }

  /**
   * Tests whether the logs are split when master recovers from a expired zookeeper session and an
   * RS goes down.
   */
  @Test
  public void testLogSplittingAfterMasterRecoveryDueToZKExpiry() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    cluster.startRegionServer();
    TableName tableName = TableName.valueOf(name.getMethodName());
    byte[] family = Bytes.toBytes("col");
    try (Admin admin = TEST_UTIL.getAdmin()) {
      byte[][] SPLIT_KEYS = new byte[][] { Bytes.toBytes("1"), Bytes.toBytes("2"),
        Bytes.toBytes("3"), Bytes.toBytes("4"), Bytes.toBytes("5") };
      TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
          .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).build();
      admin.createTable(htd, SPLIT_KEYS);
    }
    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
    HMaster m = cluster.getMaster();
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      int numberOfPuts;
      for (numberOfPuts = 0; numberOfPuts < 6; numberOfPuts++) {
        Put p = new Put(Bytes.toBytes(numberOfPuts));
        p.addColumn(Bytes.toBytes("col"), Bytes.toBytes("ql"),
          Bytes.toBytes("value" + numberOfPuts));
        table.put(p);
      }
      m.abort("Test recovery from zk session expired",
        new KeeperException.SessionExpiredException());
      assertTrue(m.isStopped()); // Master doesn't recover any more
      cluster.killRegionServer(TEST_UTIL.getRSForFirstRegionInTable(tableName).getServerName());
      // Without patch for HBASE-6046 this test case will always timeout
      // with patch the test case should pass.
      int numberOfRows = 0;
      try (ResultScanner scanner = table.getScanner(new Scan())) {
        while (scanner.next() != null) {
          numberOfRows++;
        }
      }
      assertEquals("Number of rows should be equal to number of puts.", numberOfPuts, numberOfRows);
    }
  }

  static class MockLoadBalancer extends SimpleLoadBalancer {
    static boolean retainAssignCalled = false;

    @Override
    @NonNull
    public Map<ServerName, List<RegionInfo>> retainAssignment(
        Map<RegionInfo, ServerName> regions, List<ServerName> servers) throws HBaseIOException {
      retainAssignCalled = true;
      return super.retainAssignment(regions, servers);
    }
  }

}

