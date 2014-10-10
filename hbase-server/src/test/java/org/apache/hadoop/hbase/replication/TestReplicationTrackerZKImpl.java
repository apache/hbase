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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.Test;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

/**
 * This class tests the ReplicationTrackerZKImpl class and ReplicationListener interface. One
 * MiniZKCluster is used throughout the entire class. The cluster is initialized with the creation
 * of the rsZNode. All other znode creation/initialization is handled by the replication state
 * interfaces (i.e. ReplicationPeers, etc.). Each test case in this class should ensure that the
 * MiniZKCluster is cleaned and returned to it's initial state (i.e. nothing but the rsZNode).
 */
@Category(MediumTests.class)
public class TestReplicationTrackerZKImpl {

  private static final Log LOG = LogFactory.getLog(TestReplicationTrackerZKImpl.class);

  private static Configuration conf;
  private static HBaseTestingUtility utility;

  // Each one of the below variables are reinitialized before every test case
  private ZooKeeperWatcher zkw;
  private ReplicationPeers rp;
  private ReplicationTracker rt;
  private AtomicInteger rsRemovedCount;
  private String rsRemovedData;
  private AtomicInteger plChangedCount;
  private List<String> plChangedData;
  private AtomicInteger peerRemovedCount;
  private String peerRemovedData;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    utility = new HBaseTestingUtility();
    utility.startMiniZKCluster();
    conf = utility.getConfiguration();
    ZooKeeperWatcher zk = HBaseTestingUtility.getZooKeeperWatcher(utility);
    ZKUtil.createWithParents(zk, zk.rsZNode);
  }

  @Before
  public void setUp() throws Exception {
    zkw = HBaseTestingUtility.getZooKeeperWatcher(utility);
    String fakeRs1 = ZKUtil.joinZNode(zkw.rsZNode, "hostname1.example.org:1234");
    try {
      ZKClusterId.setClusterId(zkw, new ClusterId());
      rp = ReplicationFactory.getReplicationPeers(zkw, conf, zkw);
      rp.init();
      rt = ReplicationFactory.getReplicationTracker(zkw, rp, conf, zkw, new DummyServer(fakeRs1));
    } catch (Exception e) {
      fail("Exception during test setup: " + e);
    }
    rsRemovedCount = new AtomicInteger(0);
    rsRemovedData = "";
    plChangedCount = new AtomicInteger(0);
    plChangedData = new ArrayList<String>();
    peerRemovedCount = new AtomicInteger(0);
    peerRemovedData = "";
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
    ZKUtil.createWithParents(zkw, ZKUtil.joinZNode(zkw.rsZNode, "hostname1.example.org:1234"));
    assertEquals(1, rt.getListOfRegionServers().size());

    // 2 region servers
    ZKUtil.createWithParents(zkw, ZKUtil.joinZNode(zkw.rsZNode, "hostname2.example.org:1234"));
    assertEquals(2, rt.getListOfRegionServers().size());

    // 1 region server
    ZKUtil.deleteNode(zkw, ZKUtil.joinZNode(zkw.rsZNode, "hostname2.example.org:1234"));
    assertEquals(1, rt.getListOfRegionServers().size());

    // 0 region server
    ZKUtil.deleteNode(zkw, ZKUtil.joinZNode(zkw.rsZNode, "hostname1.example.org:1234"));
    assertEquals(0, rt.getListOfRegionServers().size());
  }

  @Test(timeout = 30000)
  public void testRegionServerRemovedEvent() throws Exception {
    ZKUtil.createAndWatch(zkw, ZKUtil.joinZNode(zkw.rsZNode, "hostname2.example.org:1234"),
      HConstants.EMPTY_BYTE_ARRAY);
    rt.registerListener(new DummyReplicationListener());
    // delete one
    ZKUtil.deleteNode(zkw, ZKUtil.joinZNode(zkw.rsZNode, "hostname2.example.org:1234"));
    // wait for event
    while (rsRemovedCount.get() < 1) {
      Thread.sleep(5);
    }
    assertEquals("hostname2.example.org:1234", rsRemovedData);
  }

  @Test(timeout = 30000)
  public void testPeerRemovedEvent() throws Exception {
    rp.addPeer("5", utility.getClusterKey());
    rt.registerListener(new DummyReplicationListener());
    rp.removePeer("5");
    // wait for event
    while (peerRemovedCount.get() < 1) {
      Thread.sleep(5);
    }
    assertEquals("5", peerRemovedData);
  }

  @Test(timeout = 30000)
  public void testPeerListChangedEvent() throws Exception {
    // add a peer
    rp.addPeer("5", utility.getClusterKey());
    zkw.getRecoverableZooKeeper().getZooKeeper().getChildren("/hbase/replication/peers/5", true);
    rt.registerListener(new DummyReplicationListener());
    rp.disablePeer("5");
    ZKUtil.deleteNode(zkw, "/hbase/replication/peers/5/peer-state");
    // wait for event
    int tmp = plChangedCount.get();
    while (plChangedCount.get() <= tmp) {
      Thread.sleep(5);
    }
    assertEquals(1, plChangedData.size());
    assertTrue(plChangedData.contains("5"));

    // clean up
    //ZKUtil.deleteNode(zkw, "/hbase/replication/peers/5");
    rp.removePeer("5");
  }

  @Test(timeout = 30000)
  public void testPeerNameControl() throws Exception {
    int exists = 0;
    int hyphen = 0;
    rp.addPeer("6", utility.getClusterKey(), null);
    
    try{
      rp.addPeer("6", utility.getClusterKey(), null);
    }catch(IllegalArgumentException e){
      exists++;
    }

    try{
      rp.addPeer("6-ec2", utility.getClusterKey(), null);
    }catch(IllegalArgumentException e){
      hyphen++;
    }
    assertEquals(1, exists);
    assertEquals(1, hyphen);
    
    // clean up
    rp.removePeer("6");
  }
  
  private class DummyReplicationListener implements ReplicationListener {

    @Override
    public void regionServerRemoved(String regionServer) {
      rsRemovedData = regionServer;
      rsRemovedCount.getAndIncrement();
      LOG.debug("Received regionServerRemoved event: " + regionServer);
    }

    @Override
    public void peerRemoved(String peerId) {
      peerRemovedData = peerId;
      peerRemovedCount.getAndIncrement();
      LOG.debug("Received peerRemoved event: " + peerId);
    }

    @Override
    public void peerListChanged(List<String> peerIds) {
      plChangedData.clear();
      plChangedData.addAll(peerIds);
      plChangedCount.getAndIncrement();
      LOG.debug("Received peerListChanged event");
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
    public ZooKeeperWatcher getZooKeeper() {
      return zkw;
    }

    @Override
    public CatalogTracker getCatalogTracker() {
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
  }
}

