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


import com.google.common.base.Stopwatch;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.consensus.client.QuorumClient;
import org.apache.hadoop.hbase.consensus.client.QuorumThriftClientAgent;
import org.apache.hadoop.hbase.consensus.quorum.QuorumInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestCommitDeadline {
  private static RaftTestUtil RAFT_TEST_UTIL = new RaftTestUtil();
  private static int NUM_REPLICAS = 3;
  private static int QUORUM_SIZE = 2;

  private QuorumInfo quorumInfo;
  private QuorumClient client;
  private QuorumThriftClientAgent leader;

  @Test
  public void testQuorumAgentCommitDeadline() throws Exception {
    long deadline = HConstants.QUORUM_AGENT_COMMIT_DEADLINE_DEFAULT;

    // Do a transaction to make the QuorumClient lookup the leader.
    client.replicateCommits(RAFT_TEST_UTIL.generateTransaction(1024));
    leader = client.getLeader();
    assertNotNull(leader);

    // A successful commit should complete within the set deadline.
    Stopwatch stopwatch = new Stopwatch(); //Stopwatch.createStarted();
    assertTrue(leader.replicateCommit(quorumInfo.getQuorumName(),
            RAFT_TEST_UTIL.generateTransaction(1024)) > 0);
    assertTrue("The commit should complete within the deadline",
            stopwatch.elapsedTime(TimeUnit.MILLISECONDS) < deadline);

    // Stop the majority of the replicas. The leader should remain leader.
    for (int i = 0; i < QUORUM_SIZE; i++) {
      RAFT_TEST_UTIL.stopLocalConsensusServer(quorumInfo, i + 1);
    }
    leader = client.getLeader();
    assertNotNull(leader);

    // An unsuccessful commit should throw after the deadline expires.
    stopwatch.reset().start();
    Exception expectedException = null;
    try {
      leader.replicateCommit(quorumInfo.getQuorumName(),
              RAFT_TEST_UTIL.generateTransaction(1024));
    } catch (Exception e) {
      expectedException = e;
    }
    long elapsed = stopwatch.elapsedTime(TimeUnit.MILLISECONDS);
    assertTrue("Elapsed time should be within 10% of the deadline",
            Math.abs(deadline - elapsed) < 0.1 * deadline);
    assertNotNull("A TimeoutException should have been thrown",
            expectedException);
  }

  @Before
  public void setUp() throws Exception {
    RAFT_TEST_UTIL.disableVerboseLogging();
    RAFT_TEST_UTIL.createRaftCluster(NUM_REPLICAS);
    RAFT_TEST_UTIL.assertAllServersRunning();
    RAFT_TEST_UTIL.setUsePeristentLog(true);

    quorumInfo = RAFT_TEST_UTIL.initializePeers();

    RAFT_TEST_UTIL.addQuorum(quorumInfo,
      RAFT_TEST_UTIL.getScratchSetup(NUM_REPLICAS));
    RAFT_TEST_UTIL.startQuorum(quorumInfo);

    client = RAFT_TEST_UTIL.getQuorumClient(quorumInfo);
  }

  @After
  public void tearDown() {
    RAFT_TEST_UTIL.shutdown();
  }
}
