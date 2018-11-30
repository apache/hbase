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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests the ReplicationTrackerZKImpl class and ReplicationListener interface. One
 * MiniZKCluster is used throughout the entire class. The cluster is initialized with the creation
 * of the rsZNode. All other znode creation/initialization is handled by the replication state
 * interfaces (i.e. ReplicationPeers, etc.). Each test case in this class should ensure that the
 * MiniZKCluster is cleaned and returned to it's initial state (i.e. nothing but the rsZNode).
 */
@Category({ReplicationTests.class, MediumTests.class})
public class TestReplicationTrackerZKImpl {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationTrackerZKImpl.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationTrackerZKImpl.class);

  private static Configuration conf;
  private static HBaseTestingUtility utility;

  // Each one of the below variables are reinitialized before every test case
  private ZKWatcher zkw;
  private ReplicationPeers rp;
  private ReplicationTracker rt;
  private AtomicInteger rsRemovedCount;
  private String rsRemovedData;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    utility = new HBaseTestingUtility();
    utility.startMiniZKCluster();
    conf = utility.getConfiguration();
    ZKWatcher zk = HBaseTestingUtility.getZooKeeperWatcher(utility);
    ZKUtil.createWithParents(zk, zk.getZNodePaths().rsZNode);
  }

  @Before
  public void setUp() throws Exception {
    zkw = HBaseTestingUtility.getZooKeeperWatcher(utility);
    String fakeRs1 = ZNodePaths.joinZNode(zkw.getZNodePaths().rsZNode,
            "hostname1.example.org:1234");
    try {
      ZKClusterId.setClusterId(zkw, new ClusterId());
      rp = ReplicationFactory.getReplicationPeers(zkw, conf);
      rp.init();
      rt = ReplicationFactory.getReplicationTracker(zkw, new DummyServer(fakeRs1),
        new DummyServer(fakeRs1));
    } catch (Exception e) {
      fail("Exception during test setup: " + e);
    }
    rsRemovedCount = new AtomicInteger(0);
    rsRemovedData = "";
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    utility.shutdownMiniZKCluster();
  }

  @Test
  public void testGetListOfRegionServers() throws Exception {
    // 0 region servers
    assertEquals(0, rt.getListOfRegionServers().size());

    // 1 region server
    ZKUtil.createWithParents(zkw,
      ZNodePaths.joinZNode(zkw.getZNodePaths().rsZNode, "hostname1.example.org:1234"));
    assertEquals(1, rt.getListOfRegionServers().size());

    // 2 region servers
    ZKUtil.createWithParents(zkw,
      ZNodePaths.joinZNode(zkw.getZNodePaths().rsZNode, "hostname2.example.org:1234"));
    assertEquals(2, rt.getListOfRegionServers().size());

    // 1 region server
    ZKUtil.deleteNode(zkw,
      ZNodePaths.joinZNode(zkw.getZNodePaths().rsZNode, "hostname2.example.org:1234"));
    assertEquals(1, rt.getListOfRegionServers().size());

    // 0 region server
    ZKUtil.deleteNode(zkw,
      ZNodePaths.joinZNode(zkw.getZNodePaths().rsZNode, "hostname1.example.org:1234"));
    assertEquals(0, rt.getListOfRegionServers().size());
  }

  @Test
  public void testRegionServerRemovedEvent() throws Exception {
    ZKUtil.createAndWatch(zkw,
      ZNodePaths.joinZNode(zkw.getZNodePaths().rsZNode, "hostname2.example.org:1234"),
      HConstants.EMPTY_BYTE_ARRAY);
    rt.registerListener(new DummyReplicationListener());
    // delete one
    ZKUtil.deleteNode(zkw,
      ZNodePaths.joinZNode(zkw.getZNodePaths().rsZNode, "hostname2.example.org:1234"));
    // wait for event
    while (rsRemovedCount.get() < 1) {
      Thread.sleep(5);
    }
    assertEquals("hostname2.example.org:1234", rsRemovedData);
  }

  @Test
  public void testPeerNameControl() throws Exception {
    int exists = 0;
    rp.getPeerStorage().addPeer("6",
      ReplicationPeerConfig.newBuilder().setClusterKey(utility.getClusterKey()).build(), true,
      SyncReplicationState.NONE);

    try {
      rp.getPeerStorage().addPeer("6",
        ReplicationPeerConfig.newBuilder().setClusterKey(utility.getClusterKey()).build(), true,
        SyncReplicationState.NONE);
    } catch (ReplicationException e) {
      if (e.getCause() instanceof KeeperException.NodeExistsException) {
        exists++;
      }
    }

    assertEquals(1, exists);

    // clean up
    rp.getPeerStorage().removePeer("6");
  }

  private class DummyReplicationListener implements ReplicationListener {

    @Override
    public void regionServerRemoved(String regionServer) {
      rsRemovedData = regionServer;
      rsRemovedCount.getAndIncrement();
      LOG.debug("Received regionServerRemoved event: " + regionServer);
    }
  }

  private class DummyServer implements Server {
    private String serverName;
    private boolean isAborted = false;
    private boolean isStopped = false;

    public DummyServer(String serverName) {
      this.serverName = serverName;
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }

    @Override
    public ZKWatcher getZooKeeper() {
      return zkw;
    }

    @Override
    public CoordinatedStateManager getCoordinatedStateManager() {
      return null;
    }

    @Override
    public ClusterConnection getConnection() {
      return null;
    }

    @Override
    public ServerName getServerName() {
      return ServerName.valueOf(this.serverName);
    }

    @Override
    public void abort(String why, Throwable e) {
      LOG.info("Aborting " + serverName);
      this.isAborted = true;
    }

    @Override
    public boolean isAborted() {
      return this.isAborted;
    }

    @Override
    public void stop(String why) {
      this.isStopped = true;
    }

    @Override
    public boolean isStopped() {
      return this.isStopped;
    }

    @Override
    public ChoreService getChoreService() {
      return null;
    }

    @Override
    public ClusterConnection getClusterConnection() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public FileSystem getFileSystem() {
      return null;
    }

    @Override
    public boolean isStopping() {
      return false;
    }

    @Override
    public Connection createConnection(Configuration conf) throws IOException {
      return null;
    }

    @Override
    public AsyncClusterConnection getAsyncClusterConnection() {
      return null;
    }
  }
}
