package org.apache.hadoop.hbase.consensus;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.consensus.client.QuorumClient;
import org.apache.hadoop.hbase.consensus.protocol.ConsensusHost;
import org.apache.hadoop.hbase.consensus.quorum.RaftQuorumContext;
import org.apache.hadoop.hbase.consensus.server.LocalConsensusServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TestPersistLastVotedFor {
  private static int QUORUM_SIZE = 3;
  private static int QUORUM_MAJORITY = 2;
  private static HRegionInfo regionInfo;
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
    regionInfo = RAFT_TEST_UTIL.initializePeers();
    RAFT_TEST_UTIL.addQuorum(regionInfo, RAFT_TEST_UTIL.getScratchSetup(QUORUM_SIZE));
    RAFT_TEST_UTIL.startQuorum(regionInfo);

    client = RAFT_TEST_UTIL.getQuorumClient(regionInfo.getQuorumInfo());
    transactionNum = 0;
    loader = new ReplicationLoadForUnitTest(regionInfo, client, RAFT_TEST_UTIL, QUORUM_SIZE,
      QUORUM_MAJORITY);
  }

  @After
  public void tearDown() throws Exception {
    RAFT_TEST_UTIL.shutdown();
  }

  @Test
  public void testLastVotedForIsPersisted() throws IOException, InterruptedException {
    // Add some transactions
    loader.startReplicationLoad(1000);
    transactionNum = loader.makeProgress(100, transactionNum);
    loader.stopReplicationLoad();

    RaftQuorumContext leader =
      RAFT_TEST_UTIL.getLeaderQuorumContext(regionInfo);
    // What is the current lastVotedFor
    ConsensusHost initialLastVotedFor = leader.getLastVotedFor();

    // Stop the consensusServer. lastVotedFor should have been persisted.
    LocalConsensusServer consensusServer =
      RAFT_TEST_UTIL.stopLocalConsensusServer(leader.getMyAddress());

    RaftQuorumContext newQuorumContext =
      RAFT_TEST_UTIL.restartLocalConsensusServer(consensusServer, regionInfo,
      leader.getMyAddress());
    ConsensusHost lastVotedForAsReadFromDisk =
      newQuorumContext.getLastVotedFor();
    Assert.assertEquals("Last Voted For was not persisted properly",
      initialLastVotedFor, lastVotedForAsReadFromDisk);

    // Let us try if the persisting works, if the lastVotedFor is null.
    newQuorumContext.clearVotedFor();
    consensusServer =
      RAFT_TEST_UTIL.stopLocalConsensusServer(newQuorumContext.getMyAddress());
    RaftQuorumContext newQuorumContextAfterSecondRestart =
      RAFT_TEST_UTIL.restartLocalConsensusServer(consensusServer, regionInfo,
        newQuorumContext.getMyAddress());

    ConsensusHost emptyLastVotedFor =
      newQuorumContextAfterSecondRestart.getLastVotedFor();
    Assert.assertNull(emptyLastVotedFor);
  }
}
