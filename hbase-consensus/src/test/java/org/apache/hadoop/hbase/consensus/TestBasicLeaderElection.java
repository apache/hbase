package org.apache.hadoop.hbase.consensus;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.consensus.quorum.QuorumInfo;
import org.apache.hadoop.hbase.consensus.quorum.RaftQuorumContext;
import org.apache.hadoop.hbase.consensus.server.LocalConsensusServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;

import static junit.framework.Assert.assertEquals;

@RunWith(value = Parameterized.class)
public class TestBasicLeaderElection {

  private static int QUORUM_SIZE = 5;
  private static QuorumInfo quorumInfo;
  private static RaftTestUtil RAFT_TEST_UTIL = new RaftTestUtil();
  private final List<int[]> mockLogs;

  @Before
  public void setUp() throws Exception {
    RAFT_TEST_UTIL.createRaftCluster(QUORUM_SIZE);
    RAFT_TEST_UTIL.setUsePeristentLog(true);
    RAFT_TEST_UTIL.assertAllServersRunning();
    quorumInfo = RAFT_TEST_UTIL.initializePeers();
    RAFT_TEST_UTIL.addQuorum(quorumInfo, mockLogs);
    RAFT_TEST_UTIL.startQuorum(quorumInfo);
  }

  @After
  public void tearDown() throws Exception {
    RAFT_TEST_UTIL.shutdown();
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return RaftTestDataProvider.getRaftBasicLogTestData();
  }

  public TestBasicLeaderElection(List<int[]> logs) {
    this.mockLogs = logs;
  }

  /**
   * This test function is to test the protocol is able to make progress within a period of time
   * based on the on-disk transaction log
   */
  @Test(timeout=50000)
  public void testConsensusProtocol() {
    int leaderCnt;
    do {
      leaderCnt = 0;
      try {
      // Sleep for MAX_TIMEOUT time for leader election to complete
      Thread.sleep(HConstants.PROGRESS_TIMEOUT_INTERVAL_IN_MILLISECONDS * 2);
      } catch (InterruptedException e) {}

      for (LocalConsensusServer server : RAFT_TEST_UTIL.getServers().values()) {
        RaftQuorumContext c = server.getHandler().getRaftQuorumContext(
          quorumInfo.getQuorumName());
        if (c.isLeader()) {
          leaderCnt++;
        }
      }
    }
    while(!RAFT_TEST_UTIL.verifyLogs(quorumInfo, QUORUM_SIZE) && leaderCnt != 1);
    assertEquals("There should be only one leader", 1, leaderCnt);
  }
}
