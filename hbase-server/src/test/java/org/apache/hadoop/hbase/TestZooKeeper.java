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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
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
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;



@Category({MiscTests.class, LargeTests.class})
public class TestZooKeeper {
  private static final Log LOG = LogFactory.getLog(TestZooKeeper.class);

  private final static HBaseTestingUtility
      TEST_UTIL = new HBaseTestingUtility();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Test we can first start the ZK cluster by itself
    Configuration conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniDFSCluster(2);
    TEST_UTIL.startMiniZKCluster();
    conf.setBoolean("dfs.support.append", true);
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

  private ZooKeeperWatcher getZooKeeperWatcher(Connection c)
  throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method getterZK = c.getClass().getDeclaredMethod("getKeepAliveZooKeeperWatcher");
    getterZK.setAccessible(true);
    return (ZooKeeperWatcher) getterZK.invoke(c);
  }


  /**
   * See HBASE-1232 and http://wiki.apache.org/hadoop/ZooKeeper/FAQ#4.
   * @throws IOException
   * @throws InterruptedException
   */
  // fails frequently, disabled for now, see HBASE-6406
  //@Test
  public void testClientSessionExpired() throws Exception {
    Configuration c = new Configuration(TEST_UTIL.getConfiguration());

    // We don't want to share the connection as we will check its state
    c.set(HConstants.HBASE_CLIENT_INSTANCE_ID, "1111");

    Connection connection = ConnectionFactory.createConnection(c);

    ZooKeeperWatcher connectionZK = getZooKeeperWatcher(connection);
    LOG.info("ZooKeeperWatcher= 0x"+ Integer.toHexString(
      connectionZK.hashCode()));
    LOG.info("getRecoverableZooKeeper= 0x"+ Integer.toHexString(
      connectionZK.getRecoverableZooKeeper().hashCode()));
    LOG.info("session="+Long.toHexString(
      connectionZK.getRecoverableZooKeeper().getSessionId()));

    TEST_UTIL.expireSession(connectionZK);

    LOG.info("Before using zkw state=" +
      connectionZK.getRecoverableZooKeeper().getState());
    // provoke session expiration by doing something with ZK
    try {
      connectionZK.getRecoverableZooKeeper().getZooKeeper().exists(
        "/1/1", false);
    } catch (KeeperException ignored) {
    }

    // Check that the old ZK connection is closed, means we did expire
    States state = connectionZK.getRecoverableZooKeeper().getState();
    LOG.info("After using zkw state=" + state);
    LOG.info("session="+Long.toHexString(
      connectionZK.getRecoverableZooKeeper().getSessionId()));

    // It's asynchronous, so we may have to wait a little...
    final long limit1 = System.currentTimeMillis() + 3000;
    while (System.currentTimeMillis() < limit1 && state != States.CLOSED){
      state = connectionZK.getRecoverableZooKeeper().getState();
    }
    LOG.info("After using zkw loop=" + state);
    LOG.info("ZooKeeper should have timed out");
    LOG.info("session="+Long.toHexString(
      connectionZK.getRecoverableZooKeeper().getSessionId()));

    // It's surprising but sometimes we can still be in connected state.
    // As it's known (even if not understood) we don't make the the test fail
    // for this reason.)
    // Assert.assertTrue("state=" + state, state == States.CLOSED);

    // Check that the client recovered
    ZooKeeperWatcher newConnectionZK = getZooKeeperWatcher(connection);

    States state2 = newConnectionZK.getRecoverableZooKeeper().getState();
    LOG.info("After new get state=" +state2);

    // As it's an asynchronous event we may got the same ZKW, if it's not
    //  yet invalidated. Hence this loop.
    final long limit2 = System.currentTimeMillis() + 3000;
    while (System.currentTimeMillis() < limit2 &&
      state2 != States.CONNECTED && state2 != States.CONNECTING) {

      newConnectionZK = getZooKeeperWatcher(connection);
      state2 = newConnectionZK.getRecoverableZooKeeper().getState();
    }
    LOG.info("After new get state loop=" + state2);

    Assert.assertTrue(
      state2 == States.CONNECTED || state2 == States.CONNECTING);

    connection.close();
  }

  @Test (timeout = 120000)
  public void testRegionServerSessionExpired() throws Exception {
    LOG.info("Starting testRegionServerSessionExpired");
    TEST_UTIL.expireRegionServerSession(0);
    testSanity("testRegionServerSessionExpired");
  }

  @Test(timeout = 300000)
  public void testMasterSessionExpired() throws Exception {
    LOG.info("Starting testMasterSessionExpired");
    TEST_UTIL.expireMasterSession();
    testSanity("testMasterSessionExpired");
  }

  /**
   * Master recovery when the znode already exists. Internally, this
   *  test differs from {@link #testMasterSessionExpired} because here
   *  the master znode will exist in ZK.
   */
  @Test(timeout = 300000)
  public void testMasterZKSessionRecoveryFailure() throws Exception {
    LOG.info("Starting testMasterZKSessionRecoveryFailure");
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster m = cluster.getMaster();
    m.abort("Test recovery from zk session expired",
        new KeeperException.SessionExpiredException());
    assertTrue(m.isStopped()); // Master doesn't recover any more
    testSanity("testMasterZKSessionRecoveryFailure");
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
    Admin admin = TEST_UTIL.getHBaseAdmin();
    try {
      admin.createTable(desc);
    } finally {
      admin.close();
    }

    Table table = TEST_UTIL.getConnection().getTable(desc.getTableName());
    Put put = new Put(Bytes.toBytes("testrow"));
    put.add(Bytes.toBytes("fam"),
        Bytes.toBytes("col"), Bytes.toBytes("testdata"));
    LOG.info("Putting table " + tableName);
    table.put(put);
    table.close();
  }

  @Test
  public void testMultipleZK()
  throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Table localMeta = TEST_UTIL.getConnection().getTable(TableName.META_TABLE_NAME);
    Configuration otherConf = new Configuration(TEST_UTIL.getConfiguration());
    otherConf.set(HConstants.ZOOKEEPER_QUORUM, "127.0.0.1");
    Connection connection = ConnectionFactory.createConnection(otherConf);
    Table ipMeta = connection.getTable(TableName.META_TABLE_NAME);

    // dummy, just to open the connection
    final byte [] row = new byte [] {'r'};
    localMeta.exists(new Get(row));
    ipMeta.exists(new Get(row));

    // make sure they aren't the same
    ZooKeeperWatcher z1 =
      getZooKeeperWatcher(ConnectionFactory.createConnection(localMeta.getConfiguration()));
    ZooKeeperWatcher z2 =
      getZooKeeperWatcher(ConnectionFactory.createConnection(otherConf));
    assertFalse(z1 == z2);
    assertFalse(z1.getQuorum().equals(z2.getQuorum()));

    localMeta.close();
    ipMeta.close();
    connection.close();
  }

  /**
   * Create a znode with data
   * @throws Exception
   */
  @Test
  public void testCreateWithParents() throws Exception {
    ZooKeeperWatcher zkw =
        new ZooKeeperWatcher(new Configuration(TEST_UTIL.getConfiguration()),
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
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(
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

  @Test
  public void testClusterKey() throws Exception {
    testKey("server", 2181, "hbase");
    testKey("server1,server2,server3", 2181, "hbase");
    try {
      ZKUtil.transformClusterKey("2181:hbase");
    } catch (IOException ex) {
      // OK
    }
  }

  @Test
  public void testClusterKeyWithMultiplePorts() throws Exception {
    // server has different port than the default port
    testKey("server1:2182", 2181, "hbase", true);
    // multiple servers have their own port
    testKey("server1:2182,server2:2183,server3:2184", 2181, "hbase", true);
    // one server has no specified port, should use default port
    testKey("server1:2182,server2,server3:2184", 2181, "hbase", true);
    // the last server has no specified port, should use default port
    testKey("server1:2182,server2:2183,server3", 2181, "hbase", true);
    // multiple servers have no specified port, should use default port for those servers
    testKey("server1:2182,server2,server3:2184,server4", 2181, "hbase", true);
    // same server, different ports
    testKey("server1:2182,server1:2183,server1", 2181, "hbase", true);
    // mix of same server/different port and different server
    testKey("server1:2182,server2:2183,server1", 2181, "hbase", true);
  }

  private void testKey(String ensemble, int port, String znode)
      throws IOException {
    testKey(ensemble, port, znode, false); // not support multiple client ports
  }

  private void testKey(String ensemble, int port, String znode, Boolean multiplePortSupport)
      throws IOException {
    Configuration conf = new Configuration();
    String key = ensemble+":"+port+":"+znode;
    String ensemble2 = null;
    ZKUtil.ZKClusterKey zkClusterKey = ZKUtil.transformClusterKey(key);
    if (multiplePortSupport) {
      ensemble2 = ZKUtil.standardizeQuorumServerString(ensemble, Integer.toString(port));
      assertEquals(ensemble2, zkClusterKey.quorumString);
    }
    else {
      assertEquals(ensemble, zkClusterKey.quorumString);
    }
    assertEquals(port, zkClusterKey.clientPort);
    assertEquals(znode, zkClusterKey.znodeParent);

    ZKUtil.applyClusterKeyToConf(conf, key);
    assertEquals(zkClusterKey.quorumString, conf.get(HConstants.ZOOKEEPER_QUORUM));
    assertEquals(zkClusterKey.clientPort, conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, -1));
    assertEquals(zkClusterKey.znodeParent, conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));

    String reconstructedKey = ZKUtil.getZooKeeperClusterKey(conf);
    if (multiplePortSupport) {
      String key2 = ensemble2 + ":" + port + ":" + znode;
      assertEquals(key2, reconstructedKey);
    }
    else {
      assertEquals(key, reconstructedKey);
    }
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
    ZooKeeperWatcher zk2 = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
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
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
        "testGetChildDataAndWatchForNewChildrenShouldNotThrowNPE", null);
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
    final ZooKeeperWatcher zkw = m.getZooKeeper();
    // now the cluster is up. So assign some regions.
    try (Admin admin = TEST_UTIL.getHBaseAdmin()) {
      byte[][] SPLIT_KEYS = new byte[][] { Bytes.toBytes("a"), Bytes.toBytes("b"),
          Bytes.toBytes("c"), Bytes.toBytes("d"), Bytes.toBytes("e"), Bytes.toBytes("f"),
          Bytes.toBytes("g"), Bytes.toBytes("h"), Bytes.toBytes("i"), Bytes.toBytes("j") };
      String tableName = "testRegionAssignmentAfterMasterRecoveryDueToZKExpiry";
      HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
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
  private int countPermanentListeners(ZooKeeperWatcher watcher) {
    return countListeners(watcher, ZkSplitLogWorkerCoordination.class);
  }

  /**
   * Count listeners in zkw excluding provided classes
   */
  private int countListeners(ZooKeeperWatcher watcher, Class<?>... exclude) {
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
    Admin admin = TEST_UTIL.getHBaseAdmin();
    Table table = null;
    try {
      byte[][] SPLIT_KEYS = new byte[][] { Bytes.toBytes("1"), Bytes.toBytes("2"),
        Bytes.toBytes("3"), Bytes.toBytes("4"), Bytes.toBytes("5") };

      String tableName = "testLogSplittingAfterMasterRecoveryDueToZKExpiry";
      HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
      HColumnDescriptor hcd = new HColumnDescriptor("col");
      htd.addFamily(hcd);
      admin.createTable(htd, SPLIT_KEYS);
      TEST_UTIL.waitUntilNoRegionsInTransition(60000);
      table = TEST_UTIL.getConnection().getTable(htd.getTableName());
      Put p;
      int numberOfPuts;
      for (numberOfPuts = 0; numberOfPuts < 6; numberOfPuts++) {
        p = new Put(Bytes.toBytes(numberOfPuts));
        p.add(Bytes.toBytes("col"), Bytes.toBytes("ql"), Bytes.toBytes("value" + numberOfPuts));
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
    public Map<ServerName, List<HRegionInfo>> retainAssignment(
        Map<HRegionInfo, ServerName> regions, List<ServerName> servers) {
      retainAssignCalled = true;
      return super.retainAssignment(regions, servers);
    }
  }

}

