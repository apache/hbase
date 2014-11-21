package org.apache.hadoop.hbase.consensus;

import com.google.common.base.Stopwatch;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.consensus.client.QuorumClient;
import org.apache.hadoop.hbase.consensus.client.QuorumThriftClientAgent;
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

  private HRegionInfo regionInfo;
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
    assertTrue(leader.replicateCommit(regionInfo.getEncodedName(),
            RAFT_TEST_UTIL.generateTransaction(1024)) > 0);
    assertTrue("The commit should complete within the deadline",
            stopwatch.elapsedTime(TimeUnit.MILLISECONDS) < deadline);

    // Stop the majority of the replicas. The leader should remain leader.
    for (int i = 0; i < QUORUM_SIZE; i++) {
      RAFT_TEST_UTIL.stopLocalConsensusServer(regionInfo, i + 1);
    }
    leader = client.getLeader();
    assertNotNull(leader);

    // An unsuccessful commit should throw after the deadline expires.
    stopwatch.reset().start();
    Exception expectedException = null;
    try {
      leader.replicateCommit(regionInfo.getEncodedName(),
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

    regionInfo = RAFT_TEST_UTIL.initializePeers();

    RAFT_TEST_UTIL.addQuorum(regionInfo,
            RAFT_TEST_UTIL.getScratchSetup(NUM_REPLICAS));
    RAFT_TEST_UTIL.startQuorum(regionInfo);

    client = RAFT_TEST_UTIL.getQuorumClient(regionInfo.getQuorumInfo());
  }

  @After
  public void tearDown() {
    RAFT_TEST_UTIL.shutdown();
  }
}
