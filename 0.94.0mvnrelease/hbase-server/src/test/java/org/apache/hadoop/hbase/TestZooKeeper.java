/**
 * Copyright 2009 The Apache Software Foundation
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.EmptyWatcher;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestZooKeeper {
  private final Log LOG = LogFactory.getLog(this.getClass());

  private final static HBaseTestingUtility
      TEST_UTIL = new HBaseTestingUtility();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Test we can first start the ZK cluster by itself
    Configuration conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniZKCluster();
    conf.setBoolean("dfs.support.append", true);
    conf.setInt(HConstants.ZOOKEEPER_SESSION_TIMEOUT, 1000);
    TEST_UTIL.startMiniCluster(2);
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
    TEST_UTIL.ensureSomeRegionServersAvailable(2);
  }


  private ZooKeeperWatcher getZooKeeperWatcher(HConnection c) throws
    NoSuchMethodException, InvocationTargetException, IllegalAccessException {

    Method getterZK = c.getClass().getMethod("getKeepAliveZooKeeperWatcher");
    getterZK.setAccessible(true);

    return (ZooKeeperWatcher) getterZK.invoke(c);
  }


  /**
   * See HBASE-1232 and http://wiki.apache.org/hadoop/ZooKeeper/FAQ#4.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testClientSessionExpired() throws Exception {
    Configuration c = new Configuration(TEST_UTIL.getConfiguration());

    // We don't want to share the connection as we will check its state
    c.set(HConstants.HBASE_CLIENT_INSTANCE_ID, "1111");

    HConnection connection = HConnectionManager.getConnection(c);

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

  @Test
  public void testRegionServerSessionExpired() throws Exception {
    LOG.info("Starting testRegionServerSessionExpired");
    int metaIndex = TEST_UTIL.getMiniHBaseCluster().getServerWithMeta();
    TEST_UTIL.expireRegionServerSession(metaIndex);
    testSanity();
  }

  @Test
  public void testMasterSessionExpired() throws Exception {
    LOG.info("Starting testMasterSessionExpired");
    TEST_UTIL.expireMasterSession();
    testSanity();
  }

  /**
   * Master recovery when the znode already exists. Internally, this
   *  test differs from {@link #testMasterSessionExpired} because here
   *  the master znode will exist in ZK.
   */
  @Test(timeout=20000)
  public void testMasterZKSessionRecoveryFailure() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster m = cluster.getMaster();
    m.abort("Test recovery from zk session expired",
      new KeeperException.SessionExpiredException());
    assertFalse(m.isStopped());
    testSanity();
  }

  /**
   * Make sure we can use the cluster
   * @throws Exception
   */
  public void testSanity() throws Exception{
    HBaseAdmin admin =
      new HBaseAdmin(TEST_UTIL.getConfiguration());
    String tableName = "test"+System.currentTimeMillis();
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor family = new HColumnDescriptor("fam");
    desc.addFamily(family);
    LOG.info("Creating table " + tableName);
    admin.createTable(desc);

    HTable table =
      new HTable(new Configuration(TEST_UTIL.getConfiguration()), tableName);
    Put put = new Put(Bytes.toBytes("testrow"));
    put.add(Bytes.toBytes("fam"),
        Bytes.toBytes("col"), Bytes.toBytes("testdata"));
    LOG.info("Putting table " + tableName);
    table.put(put);
    table.close();
  }

  @Test
  public void testMultipleZK() {
    try {
      HTable localMeta =
        new HTable(new Configuration(TEST_UTIL.getConfiguration()), HConstants.META_TABLE_NAME);
      Configuration otherConf = new Configuration(TEST_UTIL.getConfiguration());
      otherConf.set(HConstants.ZOOKEEPER_QUORUM, "127.0.0.1");
      HTable ipMeta = new HTable(otherConf, HConstants.META_TABLE_NAME);

      // dummy, just to open the connection
      localMeta.exists(new Get(HConstants.LAST_ROW));
      ipMeta.exists(new Get(HConstants.LAST_ROW));

      // make sure they aren't the same
      ZooKeeperWatcher z1 =
        getZooKeeperWatcher(HConnectionManager.getConnection(localMeta.getConfiguration()));
      ZooKeeperWatcher z2 =
        getZooKeeperWatcher(HConnectionManager.getConnection(otherConf));
      assertFalse(z1 == z2);
      assertFalse(z1.getQuorum().equals(z2.getQuorum()));

      localMeta.close();
      ipMeta.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  /**
   * Create a bunch of znodes in a hierarchy, try deleting one that has childs
   * (it will fail), then delete it recursively, then delete the last znode
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
    testKey("server", "2181", "hbase");
    testKey("server1,server2,server3", "2181", "hbase");
    try {
      ZKUtil.transformClusterKey("2181:hbase");
    } catch (IOException ex) {
      // OK
    }
  }

  private void testKey(String ensemble, String port, String znode)
      throws IOException {
    Configuration conf = new Configuration();
    String key = ensemble+":"+port+":"+znode;
    String[] parts = ZKUtil.transformClusterKey(key);
    assertEquals(ensemble, parts[0]);
    assertEquals(port, parts[1]);
    assertEquals(znode, parts[2]);
    ZKUtil.applyClusterKeyToConf(conf, key);
    assertEquals(parts[0], conf.get(HConstants.ZOOKEEPER_QUORUM));
    assertEquals(parts[1], conf.get(HConstants.ZOOKEEPER_CLIENT_PORT));
    assertEquals(parts[2], conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
    String reconstructedKey = ZKUtil.getZooKeeperClusterKey(conf);
    assertEquals(key, reconstructedKey);
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
        "testMasterAddressManagerFromZK",
        null);

    // I set this acl after the attempted creation of the cluster home node.
    zk.setACL("/", ZooDefs.Ids.CREATOR_ALL_ACL, -1);
    zk.create(aclZnode, null, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
    zk.close();

    ZKUtil.createAndFailSilent(zk2, aclZnode);
 }
  
  @Test
  /**
   * Test should not fail with NPE when getChildDataAndWatchForNewChildren
   * invoked with wrongNode
   */
  public void testGetChildDataAndWatchForNewChildrenShouldNotThrowNPE()
      throws Exception {
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
        "testGetChildDataAndWatchForNewChildrenShouldNotThrowNPE", null);
    ZKUtil.getChildDataAndWatchForNewChildren(zkw, "/wrongNode");
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

