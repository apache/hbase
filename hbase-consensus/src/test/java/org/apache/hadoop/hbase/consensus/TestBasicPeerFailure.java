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


import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.consensus.client.QuorumClient;
import org.apache.hadoop.hbase.consensus.quorum.QuorumInfo;
import org.apache.hadoop.hbase.consensus.quorum.RaftQuorumContext;
import org.apache.hadoop.hbase.consensus.server.LocalConsensusServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TestBasicPeerFailure {
  private static final Logger LOG = LoggerFactory.getLogger(
          TestBasicPeerFailure.class);
  private static int QUORUM_SIZE = 5;
  private static int QUORUM_MAJORITY = 3;
  private static QuorumInfo quorumInfo;
  private static RaftTestUtil RAFT_TEST_UTIL = new RaftTestUtil();
  private static QuorumClient client;
  private static volatile int transactionNum = 0;
  private ReplicationLoadForUnitTest loader;

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
    transactionNum = 0;
    loader = new ReplicationLoadForUnitTest(quorumInfo, client, RAFT_TEST_UTIL, QUORUM_SIZE,
      QUORUM_MAJORITY);
  }

  @After
  public void tearDown() throws Exception {
    RAFT_TEST_UTIL.shutdown();
  }

  @Test(timeout=500000)
  public void testSinglePeerFailureAndRecovery()
    throws InterruptedException, IOException {
    simulateFailureEvent(1);
  }

  @Test(timeout=500000)
  public void testMultiplePeerFailureAndRecovery()
    throws InterruptedException, IOException {
    simulateFailureEvent(2);
  }

  @Test(timeout=60000)
  public void testStepDownOnNoProgress() throws InterruptedException {
    final long sleepTime =
            2 * HConstants.PROGRESS_TIMEOUT_INTERVAL_IN_MILLISECONDS;
    RaftQuorumContext c5 = RAFT_TEST_UTIL.getRaftQuorumContextByRank(quorumInfo, 5);

    // Start the client load
    loader.startReplicationLoad(100);

    // Let the traffic fly for a while
    transactionNum = loader.makeProgress(sleepTime, transactionNum);
    Assert.assertTrue(c5.isLeader());

    // Stop the majority of replicas
    for (int i = 0; i < QUORUM_MAJORITY; i++) {
      System.out.println("Stopping replica with rank " + (i + 1));
      RAFT_TEST_UTIL.stopLocalConsensusServer(quorumInfo, i + 1);
    }

    Thread.sleep(2 * HConstants.PROGRESS_TIMEOUT_INTERVAL_IN_MILLISECONDS);
    Assert.assertTrue(c5.isLeader());

    // Let the request timeout
    System.out.printf("Waiting %d seconds for the request to time out\n",
            HConstants.DEFAULT_APPEND_ENTRIES_TIMEOUT_IN_MILLISECONDS / 1000);
    Thread.sleep(HConstants.DEFAULT_APPEND_ENTRIES_TIMEOUT_IN_MILLISECONDS);
    Assert.assertTrue("Leader should step down", c5.isFollower());
  }

  private void simulateFailureEvent(final int failureInterval)
    throws InterruptedException, IOException {
    int failureCnt = 0;
    final long sleepTime =
       2 * HConstants.PROGRESS_TIMEOUT_INTERVAL_IN_MILLISECONDS * failureInterval;

    // Start the client load
    loader.startReplicationLoad(100);

    // Let the traffic fly for a while
    transactionNum = loader.makeProgress(sleepTime, transactionNum);

    // Get all the quorum contexts from rank 5 to rank 3
    RaftQuorumContext c5 = RAFT_TEST_UTIL.getRaftQuorumContextByRank(quorumInfo, 5);
    RaftQuorumContext c4 = RAFT_TEST_UTIL.getRaftQuorumContextByRank(quorumInfo, 4);
    RaftQuorumContext c3 = RAFT_TEST_UTIL.getRaftQuorumContextByRank(quorumInfo, 3);

    // Shutdown 1st quorum member whose rank is 5.
    System.out.println("Stopping one quorum member: " + c5);
    LocalConsensusServer s5 = RAFT_TEST_UTIL.stopLocalConsensusServer(quorumInfo, 5);

    // Let the traffic fly for a while
    if ((++failureCnt % failureInterval) == 0) {
      transactionNum = loader.makeProgress(sleepTime, transactionNum);
      Assert.assertTrue("Rank 4 shall be the leader of the quorum", c4.isLeader());
    }

    // Shutdown 2nd quorum member whose rank 4
    System.out.println("Stopping another quorum member: " + c4);
    LocalConsensusServer s4 = RAFT_TEST_UTIL.stopLocalConsensusServer(quorumInfo, 4);

    // Let the traffic fly for a while
    if ((++failureCnt % failureInterval) == 0) {
      transactionNum = loader.makeProgress(sleepTime, transactionNum);
      Assert.assertTrue("Rank 3 shall be the leader of the quorum", c3.isLeader());
    }

    // Restart the quorum member whose rank is 4
    c4 = RAFT_TEST_UTIL.restartLocalConsensusServer(s4, quorumInfo, c4.getMyAddress());
    System.out.println("Restarted one quorum member: " + c4);

    // Let the traffic fly for a while
    if ((++failureCnt % failureInterval) == 0) {
      transactionNum = loader.makeProgress(sleepTime, transactionNum);

      while(!c4.isLeader()) {
        System.out.println("Wait for the rank 4 to take over the leadership");
        Thread.sleep(sleepTime);
      }
      Assert.assertTrue("Rank 4 shall be the leader of the quorum", c4.isLeader());
      transactionNum = loader.makeProgress(sleepTime, transactionNum);
    }
    // Restart the quorum member whose rank is 5
    c5 = RAFT_TEST_UTIL.restartLocalConsensusServer(s5, quorumInfo, c5.getMyAddress());
    System.out.println("Restarted another quorum member: " + c5);

    // Let the traffic fly for a while
    if (++failureCnt % failureInterval == 0) {
      transactionNum = loader.makeProgress(sleepTime, transactionNum);
      while(!c5.isLeader()) {

        System.out.println("Wait for the rank 5 to take over the leadership");
        Thread.sleep(sleepTime);
      }
      Assert.assertTrue("Rank 5 shall be the leader of the quorum", c5.isLeader());
    }

    loader.slowDownReplicationLoad();

    // Verify logs are identical across all the quorum members
    while (!RAFT_TEST_UTIL.verifyLogs(quorumInfo, QUORUM_SIZE)) {
      Thread.sleep(10 * 1000);
      System.out.println("Verifying logs ....");
      Assert.assertTrue("Rank 5 shall be the leader of the quorum", c5.isLeader());
    }

    // Stop the client load
    loader.stopReplicationLoad();

    System.out.println(transactionNum + " transactions have been successfully replicated");
  }
}
