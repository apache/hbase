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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;

/**
 * White box testing for replication state interfaces. Implementations should extend this class, and
 * initialize the interfaces properly.
 */
public abstract class TestReplicationStateBasic {

  protected ReplicationQueues rq1;
  protected ReplicationQueues rq2;
  protected ReplicationQueues rq3;
  protected ReplicationQueuesClient rqc;
  protected String server1 = ServerName.valueOf("hostname1.example.org", 1234, -1L).toString();
  protected String server2 = ServerName.valueOf("hostname2.example.org", 1234, -1L).toString();
  protected String server3 = ServerName.valueOf("hostname3.example.org", 1234, -1L).toString();
  protected ReplicationPeers rp;
  protected static final String ID_ONE = "1";
  protected static final String ID_TWO = "2";
  protected static String KEY_ONE;
  protected static String KEY_TWO;

  // For testing when we try to replicate to ourself
  protected String OUR_ID = "3";
  protected String OUR_KEY;

  protected static int zkTimeoutCount;
  protected static final int ZK_MAX_COUNT = 300;
  protected static final int ZK_SLEEP_INTERVAL = 100; // millis

  private static final Log LOG = LogFactory.getLog(TestReplicationStateBasic.class);

  @Before
  public void setUp() {
    zkTimeoutCount = 0;
  }

  @Test
  public void testReplicationQueuesClient() throws ReplicationException, KeeperException {
    rqc.init();
    // Test methods with empty state
    assertEquals(0, rqc.getListOfReplicators().size());
    assertNull(rqc.getLogsInQueue(server1, "qId1"));
    assertNull(rqc.getAllQueues(server1));

    /*
     * Set up data Two replicators: -- server1: three queues with 0, 1 and 2 log files each --
     * server2: zero queues
     */
    rq1.init(server1);
    rq2.init(server2);
    rq1.addLog("qId1", "trash");
    rq1.removeLog("qId1", "trash");
    rq1.addLog("qId2", "filename1");
    rq1.addLog("qId3", "filename2");
    rq1.addLog("qId3", "filename3");
    rq2.addLog("trash", "trash");
    rq2.removeQueue("trash");

    List<String> reps = rqc.getListOfReplicators();
    assertEquals(2, reps.size());
    assertTrue(server1, reps.contains(server1));
    assertTrue(server2, reps.contains(server2));

    assertNull(rqc.getLogsInQueue("bogus", "bogus"));
    assertNull(rqc.getLogsInQueue(server1, "bogus"));
    assertEquals(0, rqc.getLogsInQueue(server1, "qId1").size());
    assertEquals(1, rqc.getLogsInQueue(server1, "qId2").size());
    assertEquals("filename1", rqc.getLogsInQueue(server1, "qId2").get(0));

    assertNull(rqc.getAllQueues("bogus"));
    assertEquals(0, rqc.getAllQueues(server2).size());
    List<String> list = rqc.getAllQueues(server1);
    assertEquals(3, list.size());
    assertTrue(list.contains("qId2"));
    assertTrue(list.contains("qId3"));
  }

  @Test
  public void testReplicationQueues() throws ReplicationException {
    rq1.init(server1);
    rq2.init(server2);
    rq3.init(server3);
    //Initialize ReplicationPeer so we can add peers (we don't transfer lone queues)
    rp.init();

    // 3 replicators should exist
    assertEquals(3, rq1.getListOfReplicators().size());
    rq1.removeQueue("bogus");
    rq1.removeLog("bogus", "bogus");
    rq1.removeAllQueues();
    assertNull(rq1.getAllQueues());
    assertEquals(0, rq1.getLogPosition("bogus", "bogus"));
    assertNull(rq1.getLogsInQueue("bogus"));
    assertEquals(0, rq1.claimQueues(ServerName.valueOf("bogus", 1234, -1L).toString()).size());

    rq1.setLogPosition("bogus", "bogus", 5L);

    populateQueues();

    assertEquals(3, rq1.getListOfReplicators().size());
    assertEquals(0, rq2.getLogsInQueue("qId1").size());
    assertEquals(5, rq3.getLogsInQueue("qId5").size());
    assertEquals(0, rq3.getLogPosition("qId1", "filename0"));
    rq3.setLogPosition("qId5", "filename4", 354L);
    assertEquals(354L, rq3.getLogPosition("qId5", "filename4"));

    assertEquals(5, rq3.getLogsInQueue("qId5").size());
    assertEquals(0, rq2.getLogsInQueue("qId1").size());
    assertEquals(0, rq1.getAllQueues().size());
    assertEquals(1, rq2.getAllQueues().size());
    assertEquals(5, rq3.getAllQueues().size());

    assertEquals(0, rq3.claimQueues(server1).size());
    assertEquals(2, rq3.getListOfReplicators().size());

    SortedMap<String, SortedSet<String>> queues = rq2.claimQueues(server3);
    assertEquals(5, queues.size());
    assertEquals(1, rq2.getListOfReplicators().size());

    // Try to claim our own queues
    assertEquals(0, rq2.claimQueues(server2).size());

    assertEquals(6, rq2.getAllQueues().size());

    rq2.removeAllQueues();

    assertEquals(0, rq2.getListOfReplicators().size());
  }

  @Test
  public void testHfileRefsReplicationQueues() throws ReplicationException, KeeperException {
    rp.init();
    rq1.init(server1);
    rqc.init();

    List<String> files1 = new ArrayList<String>(3);
    files1.add("file_1");
    files1.add("file_2");
    files1.add("file_3");
    assertNull(rqc.getReplicableHFiles(ID_ONE));
    assertEquals(0, rqc.getAllPeersFromHFileRefsQueue().size());
    rp.addPeer(ID_ONE, new ReplicationPeerConfig().setClusterKey(KEY_ONE), null);
    rq1.addPeerToHFileRefs(ID_ONE);
    rq1.addHFileRefs(ID_ONE, files1);
    assertEquals(1, rqc.getAllPeersFromHFileRefsQueue().size());
    assertEquals(3, rqc.getReplicableHFiles(ID_ONE).size());
    List<String> files2 = new ArrayList<>(files1);
    String removedString = files2.remove(0);
    rq1.removeHFileRefs(ID_ONE, files2);
    assertEquals(1, rqc.getReplicableHFiles(ID_ONE).size());
    files2 = new ArrayList<>(1);
    files2.add(removedString);
    rq1.removeHFileRefs(ID_ONE, files2);
    assertEquals(0, rqc.getReplicableHFiles(ID_ONE).size());
    rp.removePeer(ID_ONE);
  }

  @Test
  public void testRemovePeerForHFileRefs() throws ReplicationException, KeeperException {
    rq1.init(server1);
    rqc.init();

    rp.init();
    rp.addPeer(ID_ONE, new ReplicationPeerConfig().setClusterKey(KEY_ONE), null);
    rp.addPeer(ID_TWO, new ReplicationPeerConfig().setClusterKey(KEY_TWO), null);

    List<String> files1 = new ArrayList<String>(3);
    files1.add("file_1");
    files1.add("file_2");
    files1.add("file_3");
    rq1.addPeerToHFileRefs(ID_ONE);
    rq1.addHFileRefs(ID_ONE, files1);
    rq1.addPeerToHFileRefs(ID_TWO);
    rq1.addHFileRefs(ID_TWO, files1);
    assertEquals(2, rqc.getAllPeersFromHFileRefsQueue().size());
    assertEquals(3, rqc.getReplicableHFiles(ID_ONE).size());
    assertEquals(3, rqc.getReplicableHFiles(ID_TWO).size());

    rp.removePeer(ID_ONE);
    rq1.removePeerFromHFileRefs(ID_ONE);
    assertEquals(1, rqc.getAllPeersFromHFileRefsQueue().size());
    assertNull(rqc.getReplicableHFiles(ID_ONE));
    assertEquals(3, rqc.getReplicableHFiles(ID_TWO).size());

    rp.removePeer(ID_TWO);
    rq1.removePeerFromHFileRefs(ID_TWO);
    assertEquals(0, rqc.getAllPeersFromHFileRefsQueue().size());
    assertNull(rqc.getReplicableHFiles(ID_TWO));
  }

  @Test
  public void testReplicationPeers() throws Exception {
    rp.init();

    // Test methods with non-existent peer ids
    try {
      rp.removePeer("bogus");
      fail("Should have thrown an IllegalArgumentException when passed a bogus peerId");
    } catch (IllegalArgumentException e) {
    }
    try {
      rp.enablePeer("bogus");
      fail("Should have thrown an IllegalArgumentException when passed a bogus peerId");
    } catch (IllegalArgumentException e) {
    }
    try {
      rp.disablePeer("bogus");
      fail("Should have thrown an IllegalArgumentException when passed a bogus peerId");
    } catch (IllegalArgumentException e) {
    }
    try {
      rp.getStatusOfPeer("bogus");
      fail("Should have thrown an IllegalArgumentException when passed a bogus peerId");
    } catch (IllegalArgumentException e) {
    }
    assertFalse(rp.peerAdded("bogus"));
    rp.peerRemoved("bogus");

    assertNull(rp.getPeerConf("bogus"));
    assertNumberOfPeers(0);

    // Add some peers
    rp.addPeer(ID_ONE, new ReplicationPeerConfig().setClusterKey(KEY_ONE), null);
    assertNumberOfPeers(1);
    rp.addPeer(ID_TWO, new ReplicationPeerConfig().setClusterKey(KEY_TWO), null);
    assertNumberOfPeers(2);

    // Test methods with a peer that is added but not connected
    try {
      rp.getStatusOfPeer(ID_ONE);
      fail("There are no connected peers, should have thrown an IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }
    assertEquals(KEY_ONE, ZKConfig.getZooKeeperClusterKey(rp.getPeerConf(ID_ONE).getSecond()));
    rp.removePeer(ID_ONE);
    rp.peerRemoved(ID_ONE);
    assertNumberOfPeers(1);

    // Add one peer
    rp.addPeer(ID_ONE, new ReplicationPeerConfig().setClusterKey(KEY_ONE), null);
    rp.peerAdded(ID_ONE);
    assertNumberOfPeers(2);
    assertTrue(rp.getStatusOfPeer(ID_ONE));
    rp.disablePeer(ID_ONE);
    assertConnectedPeerStatus(false, ID_ONE);
    rp.enablePeer(ID_ONE);
    assertConnectedPeerStatus(true, ID_ONE);

    // Disconnect peer
    rp.peerRemoved(ID_ONE);
    assertNumberOfPeers(2);
    try {
      rp.getStatusOfPeer(ID_ONE);
      fail("There are no connected peers, should have thrown an IllegalArgumentException");
    } catch (IllegalArgumentException e) {
    }
  }

  protected void assertConnectedPeerStatus(boolean status, String peerId) throws Exception {
    // we can first check if the value was changed in the store, if it wasn't then fail right away
    if (status != rp.getStatusOfPeerFromBackingStore(peerId)) {
      fail("ConnectedPeerStatus was " + !status + " but expected " + status + " in ZK");
    }
    while (true) {
      if (status == rp.getStatusOfPeer(peerId)) {
        return;
      }
      if (zkTimeoutCount < ZK_MAX_COUNT) {
        LOG.debug("ConnectedPeerStatus was " + !status + " but expected " + status
            + ", sleeping and trying again.");
        Thread.sleep(ZK_SLEEP_INTERVAL);
      } else {
        fail("Timed out waiting for ConnectedPeerStatus to be " + status);
      }
    }
  }

  protected void assertNumberOfPeers(int total) {
    assertEquals(total, rp.getAllPeerConfigs().size());
    assertEquals(total, rp.getAllPeerIds().size());
    assertEquals(total, rp.getAllPeerIds().size());
  }

  /*
   * three replicators: rq1 has 0 queues, rq2 has 1 queue with no logs, rq3 has 5 queues with 1, 2,
   * 3, 4, 5 log files respectively
   */
  protected void populateQueues() throws ReplicationException {
    rq1.addLog("trash", "trash");
    rq1.removeQueue("trash");

    rq2.addLog("qId1", "trash");
    rq2.removeLog("qId1", "trash");

    for (int i = 1; i < 6; i++) {
      for (int j = 0; j < i; j++) {
        rq3.addLog("qId" + i, "filename" + j);
      }
      //Add peers for the corresponding queues so they are not orphans
      rp.addPeer("qId" + i, new ReplicationPeerConfig().setClusterKey("bogus" + i), null);
    }
  }
}

