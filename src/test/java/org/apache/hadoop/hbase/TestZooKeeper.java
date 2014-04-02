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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.RuntimeExceptionAbortStrategy;
import org.apache.hadoop.hbase.util.TagRunner;
import org.apache.hadoop.hbase.util.TestTag;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(TagRunner.class)
public class TestZooKeeper {
  private final Log LOG = LogFactory.getLog(this.getClass());

  private final static HBaseTestingUtility
      TEST_UTIL = new HBaseTestingUtility();

  private static Configuration conf;
  private static int NUM_REGIONSERVER = 5;
  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Test we can first start the ZK cluster by itself
    conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniZKCluster();
    conf.setBoolean("dfs.support.append", true);
    conf.setInt(HConstants.ZOOKEEPER_SESSION_TIMEOUT, 1000);
    TEST_UTIL.startMiniCluster(NUM_REGIONSERVER);
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
    TEST_UTIL.ensureSomeRegionServersAvailable(NUM_REGIONSERVER);
  }

  /**
   * See HBASE-1232 and http://wiki.apache.org/hadoop/ZooKeeper/FAQ#4.
   * @throws IOException
   * @throws InterruptedException
   */
  // Marked as unstable and recorded at #2747689
  @TestTag({ "unstable" })
  @Test (timeout = 300000)
  public void testClientSessionExpired()
      throws IOException, InterruptedException {
    new HTable(conf, HConstants.META_TABLE_NAME);

    ZooKeeperWrapper zkw =
        ZooKeeperWrapper.createInstance(conf, "testClientSessionExpired",
            new RuntimeExceptionAbortStrategy());
    zkw.registerListener(EmptyWatcher.instance);
    String quorumServers = zkw.getQuorumServers();
    int sessionTimeout = zkw.getSessionTimeout();

    HConnection connection = HConnectionManager.getConnection(conf);
    ZooKeeperWrapper connectionZK = connection.getZooKeeperWrapper();
    long sessionID = connectionZK.getSessionID();
    byte[] password = connectionZK.getSessionPassword();

    // close the zk session for connectionZK
    ZooKeeper zk = new ZooKeeper(quorumServers, sessionTimeout,
        EmptyWatcher.instance, sessionID, password);
    zk.close();
    Thread.sleep(sessionTimeout * 3L);
    connection.relocateRegion(HConstants.ROOT_TABLE_NAME,
        HConstants.EMPTY_BYTE_ARRAY);

    // The zk session for connectionZK should NOT time out since the client will
    // reconnect to zk again once the session is expired.
    assertFalse(connectionZK.isAborted());
    // The zk session for zkw should NOT time out
    assertFalse(zkw.isAborted());

    // close the zk session for zkw
    sessionID = zkw.getSessionID();
    password = zkw.getSessionPassword();
    sessionTimeout = zkw.getSessionTimeout();
    zk = new ZooKeeper(quorumServers, sessionTimeout,
        EmptyWatcher.instance, sessionID, password);
    zk.close();
    while (!zkw.isAborted()) {
      Thread.sleep(sessionTimeout * 3L);
    }
  }

  /**
   * Make sure we can use the cluster
   * @throws Exception
   */
  public void testSanity() throws Exception{

    HBaseAdmin admin = new HBaseAdmin(conf);
    String tableName = "test"+System.currentTimeMillis();
    HTableDescriptor desc =
        new HTableDescriptor(tableName);
    HColumnDescriptor family = new HColumnDescriptor("fam");
    desc.addFamily(family);
    admin.createTable(desc);

    HTable table = new HTable(conf, tableName);
    Put put = new Put(Bytes.toBytes("testrow"));
    put.add(Bytes.toBytes("fam"),
        Bytes.toBytes("col"), Bytes.toBytes("testdata"));
    table.put(put);

  }

  @Test
  public void testMultipleZK() {
    try {
      HTable localMeta = new HTable(conf, HConstants.META_TABLE_NAME);
      Configuration otherConf = HBaseConfiguration.create(conf);
      otherConf.set(HConstants.ZOOKEEPER_QUORUM, "127.0.0.1");
      HTable ipMeta = new HTable(otherConf, HConstants.META_TABLE_NAME);

      // dummy, just to open the connection
      localMeta.exists(new Get(HConstants.LAST_ROW));
      ipMeta.exists(new Get(HConstants.LAST_ROW));

      // make sure they aren't the same
      assertFalse(HConnectionManager.getClientZKConnection(conf)
          .getZooKeeperWrapper() == HConnectionManager.getClientZKConnection(
          otherConf).getZooKeeperWrapper());
      assertFalse(HConnectionManager.getConnection(conf)
          .getZooKeeperWrapper().getQuorumServers().equals(HConnectionManager
          .getConnection(otherConf).getZooKeeperWrapper().getQuorumServers()));
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
    ZooKeeperWrapper zkw =
        ZooKeeperWrapper.createInstance(conf, "testZNodeDeletes",
            new RuntimeExceptionAbortStrategy());
    zkw.registerListener(EmptyWatcher.instance);
    zkw.ensureExists("/l1/l2/l3/l4");
    try {
      zkw.deleteZNode("/l1/l2");
      fail("We should not be able to delete if znode has childs");
    } catch (KeeperException ex) {
      assertNotNull(zkw.getData("/l1/l2/l3", "l4"));
    }
    zkw.deleteZNode("/l1/l2", true);
    assertNull(zkw.getData("/l1/l2/l3", "l4"));
    zkw.deleteZNode("/l1");
    assertNull(zkw.getData("/l1", "l2"));
  }


  // Marked as unstable and recorded at #2747689
  @TestTag({ "unstable" })
  @Test (timeout = 300000)
  public void testRegionServerSessionExpired() throws Exception{
    LOG.info("Starting testRegionServerSessionExpired");
    new HTable(conf, HConstants.META_TABLE_NAME);
    TEST_UTIL.expireRegionServerSession(0);
    // to wait for the meta region coming online.
    Thread.sleep(1000);
    testSanity();
  }

  @Test (timeout = 300000)
  public void testMasterSessionExpired() throws Exception {
    LOG.info("Starting testMasterSessionExpired");
    new HTable(conf, HConstants.META_TABLE_NAME);
    TEST_UTIL.expireMasterSession();

    List<RegionServerThread> regionServerThreadList =
      TEST_UTIL.getHBaseCluster().getRegionServerThreads();
    for (RegionServerThread regionServerThread : regionServerThreadList) {
      regionServerThread.getRegionServer().kill();
    }
  }
}
