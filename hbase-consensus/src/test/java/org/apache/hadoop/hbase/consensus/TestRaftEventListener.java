package org.apache.hadoop.hbase.consensus;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.hadoop.hbase.consensus.client.QuorumClient;
import org.apache.hadoop.hbase.consensus.protocol.Payload;
import org.apache.hadoop.hbase.consensus.quorum.QuorumInfo;
import org.apache.hadoop.hbase.consensus.quorum.RaftQuorumContext;
import org.apache.hadoop.hbase.consensus.server.LocalConsensusServer;
import org.apache.hadoop.hbase.consensus.server.peer.PeerServer;
import org.apache.hadoop.hbase.regionserver.DataStoreState;
import org.apache.hadoop.hbase.regionserver.RaftEventListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;

import static org.junit.Assert.assertEquals;

public class TestRaftEventListener {

  private static int QUORUM_SIZE = 5;
  private static int QUORUM_MAJORITY = 3;
  private static QuorumInfo quorumInfo;
  private static RaftTestUtil RAFT_TEST_UTIL = new RaftTestUtil();
  private static QuorumClient client;
  private int transactionNum = 0;

  private ReplicationLoadForUnitTest loader;

  /**
   * A set keeps track of the current unavailable peers
   */
  private ConcurrentSkipListSet<String> unavailablePeerSet = new ConcurrentSkipListSet<>();

  public class MockedRaftEventListener implements RaftEventListener {

    public ByteBuffer becameLeader() {
      return null;
    }

    public void becameNonLeader() {}
    public void commit(final long index, final Payload payload) {}
    public long getMinUnpersistedIndex() { return -1; }

    @Override
    public DataStoreState getState() {
      return null;
    }

    @Override
    public void updatePeerAvailabilityStatus(String peerAddress, boolean isAvailable) {
      if (isAvailable) {
        unavailablePeerSet.remove(peerAddress);
      } else {
        unavailablePeerSet.add(peerAddress);
      }
    }

    @Override
    public void closeDataStore() {}

    @Override
    public boolean canStepDown() {
      return false;
    }
  }

  private MockedRaftEventListener listener = new MockedRaftEventListener();

  @Before
  public void setUp() throws Exception {
    RAFT_TEST_UTIL.disableVerboseLogging();
    RAFT_TEST_UTIL.createRaftCluster(QUORUM_SIZE);
    RAFT_TEST_UTIL.setUsePeristentLog(true);
    RAFT_TEST_UTIL.assertAllServersRunning();
    quorumInfo = RAFT_TEST_UTIL.initializePeers();
    RAFT_TEST_UTIL.addQuorum(quorumInfo, RAFT_TEST_UTIL.getScratchSetup(QUORUM_SIZE));
    RAFT_TEST_UTIL.startQuorum(quorumInfo);
    client = RAFT_TEST_UTIL.getQuorumClient(quorumInfo);

    // Register the listener for the highest rank, which is equal to QUORUM_SIZE;
    for (Map.Entry<String, PeerServer> entry :
        RAFT_TEST_UTIL.getRaftQuorumContextByRank(quorumInfo, QUORUM_SIZE).getPeerServers().entrySet()) {
      entry.getValue().registerDataStoreEventListener(listener);
    }

    loader = new ReplicationLoadForUnitTest(quorumInfo, client, RAFT_TEST_UTIL, QUORUM_SIZE,
      QUORUM_MAJORITY);
  }

  @After
  public void tearDown() throws Exception {
    RAFT_TEST_UTIL.shutdown();
  }

  @Test(timeout=500000)
  public void testRaftEventListenerForAvailability()
    throws InterruptedException, IOException {
    // Start the client load
    loader.startReplicationLoad(100);

    // Sleep for 5 sec
    transactionNum = loader.makeProgress(1000, transactionNum);

    // Stop the replica whose rank is 4
    RaftQuorumContext c4 = RAFT_TEST_UTIL.getRaftQuorumContextByRank(quorumInfo, 4);
    System.out.println("Stopping one quorum member: " + c4);
    LocalConsensusServer s4 = RAFT_TEST_UTIL.stopLocalConsensusServer(quorumInfo, 4);

    // Sleep for 1 sec
    transactionNum = loader.makeProgress(2000, transactionNum);
    assertEquals(1, unavailablePeerSet.size());

    // Start the replica whose rank is 4
    RAFT_TEST_UTIL.restartLocalConsensusServer(s4, quorumInfo, c4.getMyAddress());
    System.out.println("Restarted one quorum member: " + c4);

    // Sleep for 5 sec
    transactionNum = loader.makeProgress(3000, transactionNum);
    assertEquals(0, unavailablePeerSet.size());
    System.out.println("There is no element in the unavailablePeerSet !");
    loader.stopReplicationLoad();
  }
}
