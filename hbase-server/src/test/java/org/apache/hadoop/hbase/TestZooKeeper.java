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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coordination.ZkSplitLogWorkerCoordination;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.balancer.SimpleLoadBalancer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.EmptyWatcher;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;


@Category({MiscTests.class, LargeTests.class})
public class TestZooKeeper {
  private static final Log LOG = LogFactory.getLog(TestZooKeeper.class);

  private final static HBaseTestingUtility
      TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Test we can first start the ZK cluster by itself
    Configuration conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniDFSCluster(2);
    TEST_UTIL.startMiniZKCluster();
    conf.setInt(HConstants.ZK_SESSION_TIMEOUT, 1000);
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, MockLoadBalancer.class,
        LoadBalancer.class);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniHBaseCluster(2, 2);
  }

  @After
  public void after() throws Exception {
    try {
      // Some regionserver could fail to delete its znode.
      // So shutdown could hang. Let's kill them all instead.
      TEST_UTIL.getHBaseCluster().killAll();

      // Still need to clean things up
      TEST_UTIL.shutdownMiniHBaseCluster();
    } finally {
      TEST_UTIL.getTestFileSystem().delete(FSUtils.getRootDir(TEST_UTIL.getConfiguration()), true);
      ZKUtil.deleteNodeRecursively(TEST_UTIL.getZooKeeperWatcher(), "/hbase");
    }
  }

  @Test (timeout = 120000)
  public void testRegionServerSessionExpired() throws Exception {
    LOG.info("Starting " + name.getMethodName());
    TEST_UTIL.expireRegionServerSession(0);
    testSanity(name.getMethodName());
  }

  @Test(timeout = 300000)
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
  @Test(timeout = 300000)
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
   * @throws Exception
   */
  private void testSanity(final String testName) throws Exception{
    String tableName = testName + "_" + System.currentTimeMillis();
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    HColumnDescriptor family = new HColumnDescriptor("fam");
    desc.addFamily(family);
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
   * Create a znode with data
   * @throws Exception
   */
  @Test
  public void testCreateWithParents() throws Exception {
    ZKWatcher zkw =
        new ZKWatcher(new Configuration(TEST_UTIL.getConfiguration()),
            TestZooKeeper.class.getName(), null);
    byte[] expectedData = new byte[] { 1, 2, 3 };
    ZKUtil.createWithParents(zkw, "/l1/l2/l3/l4/testCreateWithParents", expectedData);
    byte[] data = ZKUtil.getData(zkw, "/l1/l2/l3/l4/testCreateWithParents");
    assertTrue(Bytes.equals(expectedData, data));
    ZKUtil.deleteNodeRecursively(zkw, "/l1");

    ZKUtil.createWithParents(zkw, "/testCreateWithParents", expectedData);
    data = ZKUtil.getData(zkw, "/testCreateWithParents");
    assertTrue(Bytes.equals(expectedData, data));
    ZKUtil.deleteNodeRecursively(zkw, "/testCreateWithParents");
  }

  /**
   * Create a bunch of znodes in a hierarchy, try deleting one that has childs (it will fail), then
   * delete it recursively, then delete the last znode
   * @throws Exception
   */
  @Test
  public void testZNodeDeletes() throws Exception {
    ZKWatcher zkw = new ZKWatcher(
      new Configuration(TEST_UTIL.getConfiguration()),
      TestZooKeeper.class.getName(), null);
    ZKUtil.createWithParents(zkw, "/l1/l2/l3/l4");
    try {
      ZKUtil.deleteNode(zkw, "/l1/l2");
      fail("We should not be able to delete if znode has childs");
    } catch (KeeperException ex) {
      assertNotNull(ZKUtil.getDataNoWatch(zkw, "/l1/l2/l3/l4", null));
    }
    ZKUtil.deleteNodeRecursively(zkw, "/l1/l2");
    // make sure it really is deleted
    assertNull(ZKUtil.getDataNoWatch(zkw, "/l1/l2/l3/l4", null));

    // do the same delete again and make sure it doesn't crash
    ZKUtil.deleteNodeRecursively(zkw, "/l1/l2");

    ZKUtil.deleteNode(zkw, "/l1");
    assertNull(ZKUtil.getDataNoWatch(zkw, "/l1/l2", null));
  }

  /**
   * A test for HBASE-3238
   * @throws IOException A connection attempt to zk failed
   * @throws InterruptedException One of the non ZKUtil actions was interrupted
   * @throws KeeperException Any of the zookeeper connections had a
   * KeeperException
   */
  @Test
  public void testCreateSilentIsReallySilent() throws InterruptedException,
      KeeperException, IOException {
    Configuration c = TEST_UTIL.getConfiguration();

    String aclZnode = "/aclRoot";
    String quorumServers = ZKConfig.getZKQuorumServersString(c);
    int sessionTimeout = 5 * 1000; // 5 seconds
    ZooKeeper zk = new ZooKeeper(quorumServers, sessionTimeout, EmptyWatcher.instance);
    zk.addAuthInfo("digest", "hbase:rox".getBytes());

    // Assumes the  root of the ZooKeeper space is writable as it creates a node
    // wherever the cluster home is defined.
    ZKWatcher zk2 = new ZKWatcher(TEST_UTIL.getConfiguration(),
      "testCreateSilentIsReallySilent", null);

    // Save the previous ACL
    Stat s =  null;
    List<ACL> oldACL = null;
    while (true) {
      try {
        s = new Stat();
        oldACL = zk.getACL("/", s);
        break;
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case SESSIONEXPIRED:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception", e);
            Threads.sleep(100);
            break;
         default:
            throw e;
        }
      }
    }

    // I set this acl after the attempted creation of the cluster home node.
    // Add retries in case of retryable zk exceptions.
    while (true) {
      try {
        zk.setACL("/", ZooDefs.Ids.CREATOR_ALL_ACL, -1);
        break;
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case SESSIONEXPIRED:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            Threads.sleep(100);
            break;
         default:
            throw e;
        }
      }
    }

    while (true) {
      try {
        zk.create(aclZnode, null, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
        break;
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case SESSIONEXPIRED:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            Threads.sleep(100);
            break;
         default:
            throw e;
        }
      }
    }
    zk.close();
    ZKUtil.createAndFailSilent(zk2, aclZnode);

    // Restore the ACL
    ZooKeeper zk3 = new ZooKeeper(quorumServers, sessionTimeout, EmptyWatcher.instance);
    zk3.addAuthInfo("digest", "hbase:rox".getBytes());
    try {
      zk3.setACL("/", oldACL, -1);
    } finally {
      zk3.close();
    }
 }

  /**
   * Test should not fail with NPE when getChildDataAndWatchForNewChildren
   * invoked with wrongNode
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testGetChildDataAndWatchForNewChildrenShouldNotThrowNPE()
      throws Exception {
    ZKWatcher zkw = new ZKWatcher(TEST_UTIL.getConfiguration(), name.getMethodName(), null);
    ZKUtil.getChildDataAndWatchForNewChildren(zkw, "/wrongNode");
  }

  /**
   * Tests that the master does not call retainAssignment after recovery from expired zookeeper
   * session. Without the HBASE-6046 fix master always tries to assign all the user regions by
   * calling retainAssignment.
   */
  @Test(timeout = 300000)
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
      HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
      htd.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
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
  @Test(timeout = 300000)
  public void testLogSplittingAfterMasterRecoveryDueToZKExpiry() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    cluster.startRegionServer();
    HMaster m = cluster.getMaster();
    // now the cluster is up. So assign some regions.
    Admin admin = TEST_UTIL.getAdmin();
    Table table = null;
    try {
      byte[][] SPLIT_KEYS = new byte[][] { Bytes.toBytes("1"), Bytes.toBytes("2"),
        Bytes.toBytes("3"), Bytes.toBytes("4"), Bytes.toBytes("5") };

      HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
      HColumnDescriptor hcd = new HColumnDescriptor("col");
      htd.addFamily(hcd);
      admin.createTable(htd, SPLIT_KEYS);
      TEST_UTIL.waitUntilNoRegionsInTransition(60000);
      table = TEST_UTIL.getConnection().getTable(htd.getTableName());
      Put p;
      int numberOfPuts;
      for (numberOfPuts = 0; numberOfPuts < 6; numberOfPuts++) {
        p = new Put(Bytes.toBytes(numberOfPuts));
        p.addColumn(Bytes.toBytes("col"), Bytes.toBytes("ql"),
                Bytes.toBytes("value" + numberOfPuts));
        table.put(p);
      }
      m.getZooKeeper().close();
      m.abort("Test recovery from zk session expired",
        new KeeperException.SessionExpiredException());
      assertTrue(m.isStopped()); // Master doesn't recover any more
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
      assertEquals("Number of rows should be equal to number of puts.", numberOfPuts,
        numberOfRows);
    } finally {
      if (table != null) table.close();
      admin.close();
    }
  }

  static class MockLoadBalancer extends SimpleLoadBalancer {
    static boolean retainAssignCalled = false;

    @Override
    public Map<ServerName, List<RegionInfo>> retainAssignment(
        Map<RegionInfo, ServerName> regions, List<ServerName> servers) throws HBaseIOException {
      retainAssignCalled = true;
      return super.retainAssignment(regions, servers);
    }
  }

}

