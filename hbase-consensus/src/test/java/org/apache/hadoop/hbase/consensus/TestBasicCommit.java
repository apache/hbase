package org.apache.hadoop.hbase.consensus;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.consensus.client.QuorumClient;
import org.apache.hadoop.hbase.consensus.quorum.QuorumInfo;
import org.apache.hadoop.hbase.consensus.quorum.RaftQuorumContext;
import org.apache.hadoop.hbase.consensus.server.LocalConsensusServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestBasicCommit {
  private static int QUORUM_SIZE = 3;
  private static int QUORUM_MAJORITY = 2;
  private static QuorumInfo quorumInfo;
  private static RaftTestUtil RAFT_TEST_UTIL = new RaftTestUtil();
  private static QuorumClient client;
  private static volatile int transactionNum = 0;
  private ReplicationLoadForUnitTest loader;

  private int failureInterval = 1;

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

  @Test(timeout=120000)
  public void testAllPeerFailureAndRecovery()
    throws InterruptedException, IOException {
    int failureCnt = 0;
    final long sleepTime =
      2 * HConstants.PROGRESS_TIMEOUT_INTERVAL_IN_MILLISECONDS * failureInterval;

    // Start the client load
    loader.startReplicationLoad(100);

    // Let the traffic fly for a while
    transactionNum = loader.makeProgress(sleepTime, transactionNum);

    // Get all the quorum contexts from rank 3 to rank 1
    RaftQuorumContext c3 = RAFT_TEST_UTIL.getRaftQuorumContextByRank(quorumInfo, 3);
    RaftQuorumContext c2 = RAFT_TEST_UTIL.getRaftQuorumContextByRank(quorumInfo, 2);
    RaftQuorumContext c1 = RAFT_TEST_UTIL.getRaftQuorumContextByRank(quorumInfo, 1);

    // Shutdown 1st quorum member whose rank is 1.
    System.out.println("Stopping one quorum member: " + c1);
    LocalConsensusServer s1 = RAFT_TEST_UTIL.stopLocalConsensusServer(quorumInfo, 1);

    // Shutdown 2nd quorum member whose rank 2
    System.out.println("Stopping another quorum member: " + c2);
    LocalConsensusServer s2 = RAFT_TEST_UTIL.stopLocalConsensusServer(quorumInfo, 2);

    // Sleep for some time to make sure the leader is stuck in retry

    Thread.sleep(HConstants.PROGRESS_TIMEOUT_INTERVAL_IN_MILLISECONDS);

    // Shutdown 3rd quorum member whose rank 3
    System.out.println("Stopping another quorum member: " + c3);
    LocalConsensusServer s3 = RAFT_TEST_UTIL.stopLocalConsensusServer(quorumInfo, 3);

    // Restart 3
    c3 = RAFT_TEST_UTIL.restartLocalConsensusServer(s3, quorumInfo, c3.getMyAddress());
    System.out.println("Restarted one quorum member: " + c3);

    // Restart 2
    c2 = RAFT_TEST_UTIL.restartLocalConsensusServer(s2, quorumInfo, c2.getMyAddress());
    System.out.println("Restarted one quorum member: " + c2);

    // Restart 1
    c1 = RAFT_TEST_UTIL.restartLocalConsensusServer(s1, quorumInfo, c1.getMyAddress());
    System.out.println("Restarted one quorum member: " + c1);

    // Let the traffic fly for a while
    if ((++failureCnt % failureInterval) == 0) {
      transactionNum = loader.makeProgress(sleepTime, transactionNum);

      while(!c3.isLeader()) {
        System.out.println("Wait for the rank 3 to take over the leadership");
        Thread.sleep(sleepTime);
      }
      Assert.assertTrue("Rank 3 shall be the leader of the quorum", c3.isLeader());
      transactionNum = loader.makeProgress(sleepTime, transactionNum);
    }

    loader.slowDownReplicationLoad();

    // Verify logs are identical across all the quorum members
    while (!RAFT_TEST_UTIL.verifyLogs(quorumInfo, QUORUM_SIZE)) {
      Thread.sleep(10 * 1000);
      System.out.println("Verifying logs ....");
      Assert.assertTrue("Rank 3 shall be the leader of the quorum", c3.isLeader());
    }

    // Stop the client load
    loader.stopReplicationLoad();

    System.out.println(transactionNum + " transactions have been successfully replicated");
  }

}
