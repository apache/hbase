/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.List;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({ReplicationTests.class, MediumTests.class})
public class TestReplicationStateHBaseImpl {

  private static Configuration conf;
  private static HBaseTestingUtility utility;
  private static ZooKeeperWatcher zkw;
  private static String replicationZNode;

  private static ReplicationQueues rq1;
  private static ReplicationQueues rq2;
  private static ReplicationQueues rq3;
  private static ReplicationQueuesClient rqc;
  private static ReplicationPeers rp;


  private static final String server0 = ServerName.valueOf("hostname0.example.org", 1234, -1L)
    .toString();
  private static final String server1 = ServerName.valueOf("hostname1.example.org", 1234, 1L)
    .toString();
  private static final String server2 = ServerName.valueOf("hostname2.example.org", 1234, 1L)
    .toString();
  private static final String server3 = ServerName.valueOf("hostname3.example.org", 1234, 1L)
    .toString();

  private static DummyServer ds0;
  private static DummyServer ds1;
  private static DummyServer ds2;
  private static DummyServer ds3;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    utility = new HBaseTestingUtility();
    conf = utility.getConfiguration();
    conf.setClass("hbase.region.replica.replication.replicationQueues.class",
      TableBasedReplicationQueuesImpl.class, ReplicationQueues.class);
    conf.setClass("hbase.region.replica.replication.replicationQueuesClient.class",
        TableBasedReplicationQueuesClientImpl.class, ReplicationQueuesClient.class);
    utility.startMiniCluster();
    zkw = HBaseTestingUtility.getZooKeeperWatcher(utility);
    String replicationZNodeName = conf.get("zookeeper.znode.replication", "replication");
    replicationZNode = ZKUtil.joinZNode(zkw.baseZNode, replicationZNodeName);
  }

  @Before
  public void setUp() {
    try {
      ds0 = new DummyServer(server0);
      rqc = ReplicationFactory.getReplicationQueuesClient(new ReplicationQueuesClientArguments(
        conf, ds0));
      ds1 = new DummyServer(server1);
      rq1 = ReplicationFactory.getReplicationQueues(new ReplicationQueuesArguments(conf, ds1, zkw));
      rq1.init(server1);
      ds2 = new DummyServer(server2);
      rq2 = ReplicationFactory.getReplicationQueues(new ReplicationQueuesArguments(conf, ds2, zkw));
      rq2.init(server2);
      ds3 = new DummyServer(server3);
      rq3 = ReplicationFactory.getReplicationQueues(new ReplicationQueuesArguments(conf, ds3, zkw));
      rq3.init(server3);
      rp = ReplicationFactory.getReplicationPeers(zkw, conf, zkw);
      rp.init();
    } catch (Exception e) {
      fail("testReplicationStateHBaseConstruction received an exception" + e.getMessage());
    }
  }

  @Test
  public void checkNamingSchema() throws Exception {
    assertTrue(rq1.isThisOurRegionServer(server1));
    assertTrue(!rq1.isThisOurRegionServer(server1 + "a"));
    assertTrue(!rq1.isThisOurRegionServer(null));
  }

  @Test
  public void testSingleReplicationQueuesHBaseImpl() {
    try {
      // Test adding in WAL files
      assertEquals(0, rq1.getAllQueues().size());
      rq1.addLog("Queue1", "WALLogFile1.1");
      assertEquals(1, rq1.getAllQueues().size());
      rq1.addLog("Queue1", "WALLogFile1.2");
      rq1.addLog("Queue1", "WALLogFile1.3");
      rq1.addLog("Queue1", "WALLogFile1.4");
      rq1.addLog("Queue2", "WALLogFile2.1");
      rq1.addLog("Queue3", "WALLogFile3.1");
      assertEquals(3, rq1.getAllQueues().size());
      assertEquals(4, rq1.getLogsInQueue("Queue1").size());
      assertEquals(1, rq1.getLogsInQueue("Queue2").size());
      assertEquals(1, rq1.getLogsInQueue("Queue3").size());
      // Make sure that abortCount is still 0
      assertEquals(0, ds1.getAbortCount());
      // Make sure that getting a log from a non-existent queue triggers an abort
      assertNull(rq1.getLogsInQueue("Queue4"));
      assertEquals(1, ds1.getAbortCount());
    } catch (ReplicationException e) {
      e.printStackTrace();
      fail("testAddLog received a ReplicationException");
    }
    try {

      // Test updating the log positions
      assertEquals(0L, rq1.getLogPosition("Queue1", "WALLogFile1.1"));
      rq1.setLogPosition("Queue1", "WALLogFile1.1", 123L);
      assertEquals(123L, rq1.getLogPosition("Queue1", "WALLogFile1.1"));
      rq1.setLogPosition("Queue1", "WALLogFile1.1", 123456789L);
      assertEquals(123456789L, rq1.getLogPosition("Queue1", "WALLogFile1.1"));
      rq1.setLogPosition("Queue2", "WALLogFile2.1", 242L);
      assertEquals(242L, rq1.getLogPosition("Queue2", "WALLogFile2.1"));
      rq1.setLogPosition("Queue3", "WALLogFile3.1", 243L);
      assertEquals(243L, rq1.getLogPosition("Queue3", "WALLogFile3.1"));

      // Test that setting log positions in non-existing logs will cause an abort
      assertEquals(1, ds1.getAbortCount());
      rq1.setLogPosition("NotHereQueue", "WALLogFile3.1", 243L);
      assertEquals(2, ds1.getAbortCount());
      rq1.setLogPosition("NotHereQueue", "NotHereFile", 243L);
      assertEquals(3, ds1.getAbortCount());
      rq1.setLogPosition("Queue1", "NotHereFile", 243l);
      assertEquals(4, ds1.getAbortCount());

      // Test reading log positions for non-existent queues and WAL's
      try {
        rq1.getLogPosition("Queue1", "NotHereWAL");
        fail("Replication queue should have thrown a ReplicationException for reading from a " +
          "non-existent WAL");
      } catch (ReplicationException e) {
      }
      try {
        rq1.getLogPosition("NotHereQueue", "NotHereWAL");
        fail("Replication queue should have thrown a ReplicationException for reading from a " +
          "non-existent queue");
      } catch (ReplicationException e) {
      }
      // Test removing logs
      rq1.removeLog("Queue1", "WALLogFile1.1");
      assertEquals(3, rq1.getLogsInQueue("Queue1").size());
      // Test removing queues
      rq1.removeQueue("Queue2");
      assertEquals(2, rq1.getAllQueues().size());
      assertNull(rq1.getLogsInQueue("Queue2"));
      // Test that getting logs from a non-existent queue aborts
      assertEquals(5, ds1.getAbortCount());
      // Test removing all queues for a Region Server
      rq1.removeAllQueues();
      assertEquals(0, rq1.getAllQueues().size());
      assertNull(rq1.getLogsInQueue("Queue1"));
      // Test that getting logs from a non-existent queue aborts
      assertEquals(6, ds1.getAbortCount());
      // Test removing a non-existent queue does not cause an abort. This is because we can
      // attempt to remove a queue that has no corresponding Replication Table row (if we never
      // registered a WAL for it)
      rq1.removeQueue("NotHereQueue");
      assertEquals(6, ds1.getAbortCount());
    } catch (ReplicationException e) {
      e.printStackTrace();
      fail("testAddLog received a ReplicationException");
    }
  }

  @Test
  public void TestMultipleReplicationQueuesHBaseImpl () {
    try {
      rp.registerPeer("Queue1", new ReplicationPeerConfig().setClusterKey("localhost:2818:/bogus1"));
      rp.registerPeer("Queue2", new ReplicationPeerConfig().setClusterKey("localhost:2818:/bogus2"));
      rp.registerPeer("Queue3", new ReplicationPeerConfig().setClusterKey("localhost:2818:/bogus3"));
    } catch (ReplicationException e) {
      fail("Failed to add peers to ReplicationPeers");
    }
    try {
      // Test adding in WAL files
      rq1.addLog("Queue1", "WALLogFile1.1");
      rq1.addLog("Queue1", "WALLogFile1.2");
      rq1.addLog("Queue1", "WALLogFile1.3");
      rq1.addLog("Queue1", "WALLogFile1.4");
      rq1.addLog("Queue2", "WALLogFile2.1");
      rq1.addLog("Queue3", "WALLogFile3.1");
      rq2.addLog("Queue1", "WALLogFile1.1");
      rq2.addLog("Queue1", "WALLogFile1.2");
      rq2.addLog("Queue2", "WALLogFile2.1");
      rq3.addLog("Queue1", "WALLogFile1.1");
      // Test adding logs to replication queues
      assertEquals(3, rq1.getAllQueues().size());
      assertEquals(2, rq2.getAllQueues().size());
      assertEquals(1, rq3.getAllQueues().size());
      assertEquals(4, rq1.getLogsInQueue("Queue1").size());
      assertEquals(1, rq1.getLogsInQueue("Queue2").size());
      assertEquals(1, rq1.getLogsInQueue("Queue3").size());
      assertEquals(2, rq2.getLogsInQueue("Queue1").size());
      assertEquals(1, rq2.getLogsInQueue("Queue2").size());
      assertEquals(1, rq3.getLogsInQueue("Queue1").size());
    } catch (ReplicationException e) {
      e.printStackTrace();
      fail("testAddLogs received a ReplicationException");
    }
    try {
      // Test setting and reading offset in queues
      rq1.setLogPosition("Queue1", "WALLogFile1.1", 1l);
      rq1.setLogPosition("Queue1", "WALLogFile1.2", 2l);
      rq1.setLogPosition("Queue1", "WALLogFile1.3", 3l);
      rq1.setLogPosition("Queue2", "WALLogFile2.1", 4l);
      rq1.setLogPosition("Queue2", "WALLogFile2.2", 5l);
      rq1.setLogPosition("Queue3", "WALLogFile3.1", 6l);
      rq2.setLogPosition("Queue1", "WALLogFile1.1", 7l);
      rq2.setLogPosition("Queue2", "WALLogFile2.1", 8l);
      rq3.setLogPosition("Queue1", "WALLogFile1.1", 9l);
      assertEquals(1l, rq1.getLogPosition("Queue1", "WALLogFile1.1"));
      assertEquals(2l, rq1.getLogPosition("Queue1", "WALLogFile1.2"));
      assertEquals(4l, rq1.getLogPosition("Queue2", "WALLogFile2.1"));
      assertEquals(6l, rq1.getLogPosition("Queue3", "WALLogFile3.1"));
      assertEquals(7l, rq2.getLogPosition("Queue1", "WALLogFile1.1"));
      assertEquals(8l, rq2.getLogPosition("Queue2", "WALLogFile2.1"));
      assertEquals(9l, rq3.getLogPosition("Queue1", "WALLogFile1.1"));
      assertEquals(rq1.getListOfReplicators().size(), 3);
      assertEquals(rq2.getListOfReplicators().size(), 3);
      assertEquals(rq3.getListOfReplicators().size(), 3);
    } catch (ReplicationException e) {
      fail("testAddLogs threw a ReplicationException");
    }
    try {
      // Test claiming queues
      List<String> claimedQueuesFromRq2 = rq1.getUnClaimedQueueIds(server2);
      // Check to make sure that list of peers with outstanding queues is decremented by one
      // after claimQueues
      // Check to make sure that we claimed the proper number of queues
      assertEquals(2, claimedQueuesFromRq2.size());
      assertTrue(claimedQueuesFromRq2.contains("Queue1-" + server2));
      assertTrue(claimedQueuesFromRq2.contains("Queue2-" + server2));
      assertEquals(2, rq1.claimQueue(server2, "Queue1-" + server2).getSecond().size());
      assertEquals(1, rq1.claimQueue(server2, "Queue2-" + server2).getSecond().size());
      rq1.removeReplicatorIfQueueIsEmpty(server2);
      assertEquals(rq1.getListOfReplicators().size(), 2);
      assertEquals(rq2.getListOfReplicators().size(), 2);
      assertEquals(rq3.getListOfReplicators().size(), 2);
      assertEquals(5, rq1.getAllQueues().size());
      // Check that all the logs in the other queue were claimed
      assertEquals(2, rq1.getLogsInQueue("Queue1-" + server2).size());
      assertEquals(1, rq1.getLogsInQueue("Queue2-" + server2).size());
      // Check that the offsets of the claimed queues are the same
      assertEquals(7l, rq1.getLogPosition("Queue1-" + server2, "WALLogFile1.1"));
      assertEquals(8l, rq1.getLogPosition("Queue2-" + server2, "WALLogFile2.1"));
      // Check that the queues were properly removed from rq2
      assertEquals(0, rq2.getAllQueues().size());
      assertNull(rq2.getLogsInQueue("Queue1"));
      assertNull(rq2.getLogsInQueue("Queue2"));
      // Check that non-existent peer queues are not claimed
      rq1.addLog("UnclaimableQueue", "WALLogFile1.1");
      rq1.addLog("UnclaimableQueue", "WALLogFile1.2");
      assertEquals(6, rq1.getAllQueues().size());
      List<String> claimedQueuesFromRq1 = rq3.getUnClaimedQueueIds(server1);
      for(String queue : claimedQueuesFromRq1) {
        rq3.claimQueue(server1, queue);
      }
      rq3.removeReplicatorIfQueueIsEmpty(server1);
      assertEquals(rq1.getListOfReplicators().size(), 1);
      assertEquals(rq2.getListOfReplicators().size(), 1);
      assertEquals(rq3.getListOfReplicators().size(), 1);
      // Note that we do not pick up the queue: UnclaimableQueue which was not registered in
      // Replication Peers
      assertEquals(6, rq3.getAllQueues().size());
      // Test claiming non-existing queues
      List<String> noQueues = rq3.getUnClaimedQueueIds("NotARealServer");
      assertNull(noQueues);
      assertEquals(6, rq3.getAllQueues().size());
      // Test claiming own queues
      noQueues = rq3.getUnClaimedQueueIds(server3);
      Assert.assertNull(noQueues);
      assertEquals(6, rq3.getAllQueues().size());
      // Check that rq3 still remain on list of replicators
      assertEquals(1, rq3.getListOfReplicators().size());
    } catch (ReplicationException e) {
      fail("testClaimQueue threw a ReplicationException");
    }
  }

  @Test
  public void TestReplicationQueuesClient() throws Exception{

    // Test ReplicationQueuesClient log tracking
    rq1.addLog("Queue1", "WALLogFile1.1");
    assertEquals(1, rqc.getLogsInQueue(server1, "Queue1").size());
    rq1.removeLog("Queue1", "WALLogFile1.1");
    assertEquals(0, rqc.getLogsInQueue(server1, "Queue1").size());
    rq2.addLog("Queue2", "WALLogFile2.1");
    rq2.addLog("Queue2", "WALLogFile2.2");
    assertEquals(2, rqc.getLogsInQueue(server2, "Queue2").size());
    rq3.addLog("Queue1", "WALLogFile1.1");
    rq3.addLog("Queue3", "WALLogFile3.1");
    rq3.addLog("Queue3", "WALLogFile3.2");

    // Test ReplicationQueueClient log tracking for faulty cases
    assertEquals(0, ds0.getAbortCount());
    assertNull(rqc.getLogsInQueue("NotHereServer", "NotHereQueue"));
    assertNull(rqc.getLogsInQueue(server1, "NotHereQueue"));
    assertNull(rqc.getLogsInQueue("NotHereServer", "WALLogFile1.1"));
    assertEquals(3, ds0.getAbortCount());
    // Test ReplicationQueueClient replicators
    List<String> replicators = rqc.getListOfReplicators();
    assertEquals(3, replicators.size());
    assertTrue(replicators.contains(server1));
    assertTrue(replicators.contains(server2));
    rq1.removeQueue("Queue1");
    assertEquals(2, rqc.getListOfReplicators().size());

    // Test ReplicationQueuesClient queue tracking
    assertEquals(0, rqc.getAllQueues(server1).size());
    rq1.addLog("Queue2", "WALLogFile2.1");
    rq1.addLog("Queue3", "WALLogFile3.1");
    assertEquals(2, rqc.getAllQueues(server1).size());
    rq1.removeAllQueues();
    assertEquals(0, rqc.getAllQueues(server1).size());

    // Test ReplicationQueuesClient queue tracking for faulty cases
    assertEquals(0, rqc.getAllQueues("NotHereServer").size());

    // Test ReplicationQueuesClient get all WAL's
    assertEquals(5 , rqc.getAllWALs().size());
    rq3.removeLog("Queue1", "WALLogFile1.1");
    assertEquals(4, rqc.getAllWALs().size());
    rq3.removeAllQueues();
    assertEquals(2, rqc.getAllWALs().size());
    rq2.removeAllQueues();
    assertEquals(0, rqc.getAllWALs().size());
  }

  @After
  public void clearQueues() throws Exception{
    rq1.removeAllQueues();
    rq2.removeAllQueues();
    rq3.removeAllQueues();
    assertEquals(0, rq1.getAllQueues().size());
    assertEquals(0, rq2.getAllQueues().size());
    assertEquals(0, rq3.getAllQueues().size());
    ds0.resetAbortCount();
    ds1.resetAbortCount();
    ds2.resetAbortCount();
    ds3.resetAbortCount();
  }

  @After
  public void tearDown() throws KeeperException, IOException {
    ZKUtil.deleteNodeRecursively(zkw, replicationZNode);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    utility.shutdownMiniCluster();
    utility.shutdownMiniZKCluster();
  }

  static class DummyServer implements Server {
    private String serverName;
    private boolean isAborted = false;
    private boolean isStopped = false;
    private int abortCount = 0;

    public DummyServer(String serverName) {
      this.serverName = serverName;
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
      return null;
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
    public MetaTableLocator getMetaTableLocator() {
      return null;
    }

    @Override
    public ServerName getServerName() {
      return ServerName.valueOf(this.serverName);
    }

    @Override
    public void abort(String why, Throwable e) {
      abortCount++;
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
      return null;
    }

    public int getAbortCount() {
      return abortCount;
    }

    public void resetAbortCount() {
      abortCount = 0;
    }

  }
}
