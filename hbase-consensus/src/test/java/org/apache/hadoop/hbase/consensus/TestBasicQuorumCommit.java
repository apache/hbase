package org.apache.hadoop.hbase.consensus;

import org.apache.hadoop.hbase.consensus.client.QuorumClient;
import org.apache.hadoop.hbase.consensus.quorum.QuorumInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static junit.framework.Assert.fail;

@RunWith(value = Parameterized.class)
public class TestBasicQuorumCommit {
  private static final Logger LOG = LoggerFactory.getLogger(
    TestBasicQuorumCommit.class);
  private static final int MULTIPLE_COMMIT_NUM = 10;

  private static final int QUORUM_SIZE = 5;
  private static final int QUORUM_MAJORITY = 3;
  private static QuorumInfo quorumInfo;
  private static RaftTestUtil RAFT_TEST_UTIL = new RaftTestUtil();
  private final List<int[]> mockLogs;
  private QuorumClient client;

  @Before
  public void setUp() throws Exception {
    RAFT_TEST_UTIL.createRaftCluster(QUORUM_SIZE);
    RAFT_TEST_UTIL.assertAllServersRunning();
    RAFT_TEST_UTIL.setUsePeristentLog(true);
    quorumInfo = RAFT_TEST_UTIL.initializePeers();
    RAFT_TEST_UTIL.addQuorum(quorumInfo, mockLogs);
    RAFT_TEST_UTIL.startQuorum(quorumInfo);
    client = RAFT_TEST_UTIL.getQuorumClient(quorumInfo);
  }

  @After
  public void tearDown() throws Exception {
    RAFT_TEST_UTIL.shutdown();
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return RaftTestDataProvider.getRaftBasicLogTestData();
  }

  public TestBasicQuorumCommit(List<int[]> logs) {
    this.mockLogs = logs;
  }

  @Test(timeout=50000)
  public void testMultipleCommit() {
    for (int i = 0 ; i <= MULTIPLE_COMMIT_NUM; i++) {
      testSingleCommit();
      LOG.info("Passed the " + i + " commit !");
    }
    // Verify all the logs across the quorum are the same
    while(!RAFT_TEST_UTIL.verifyLogs(quorumInfo, QUORUM_SIZE)) {
      RAFT_TEST_UTIL.dumpStates(quorumInfo);
      try {
        // Sleep for MAX_TIMEOUT time for leader election to complete
        Thread.sleep(HConstants.QUORUM_CLIENT_COMMIT_DEADLINE_DEFAULT);
      } catch (InterruptedException e) {
        LOG.warn("We are told to exit.");
        System.exit(1);
      }
    }
  }

  private void testSingleCommit() {
    try {
      RAFT_TEST_UTIL.dumpStates(quorumInfo);
      client.replicateCommits(Arrays.asList(generateTestingWALEdit()));
      RAFT_TEST_UTIL.dumpStates(quorumInfo);
      // Verify all the logs across the majority are the same
      RAFT_TEST_UTIL.verifyLogs(quorumInfo, QUORUM_MAJORITY);
    } catch (Exception e) {
      LOG.error("Errors: ", e);
      fail("Unexpected exception: " + e.getMessage());
    }
  }

  private static WALEdit generateTestingWALEdit() {
    KeyValue kv = KeyValue.createFirstOnRow(Bytes.toBytes("TestQuorum"));
    return new WALEdit(Arrays.asList(kv));
  }
}
