package org.apache.hadoop.hbase.consensus.log;

import org.apache.hadoop.hbase.consensus.RaftTestUtil;
import org.apache.hadoop.hbase.consensus.ReplicationLoadForUnitTest;
import org.apache.hadoop.hbase.consensus.client.FetchTask;
import org.apache.hadoop.hbase.consensus.client.QuorumClient;
import org.apache.hadoop.hbase.consensus.quorum.QuorumInfo;
import org.apache.hadoop.hbase.consensus.quorum.RaftQuorumContext;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestRemoteLogFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(TestRemoteLogFetcher.class);
  private static int QUORUM_SIZE = 3;
  private static int QUORUM_MAJORITY = 2;
  private static QuorumInfo quorumInfo;
  private static RaftTestUtil RAFT_TEST_UTIL = new RaftTestUtil();
  private static QuorumClient client;
  private static volatile int transactionNum = 0;
  private ReplicationLoadForUnitTest loader;

  @Before
  public void setUp() throws Exception {
//    RAFT_TEST_UTIL.disableVerboseLogging();
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

  @Test(timeout=60000)
  public void testLogFileStatusRetrieval() throws Exception {
    RaftQuorumContext c3 = RAFT_TEST_UTIL.getRaftQuorumContextByRank(quorumInfo, 3);
    RaftQuorumContext c2 = RAFT_TEST_UTIL.getRaftQuorumContextByRank(quorumInfo, 2);
    RaftQuorumContext c1 = RAFT_TEST_UTIL.getRaftQuorumContextByRank(quorumInfo, 1);

    TransactionLogManager l3 = (TransactionLogManager)c3.getLogManager();
    // Around 60 indices per log file on peer 3
    l3.setRaftLogRollSize(1024 * 3000);
    TransactionLogManager l1 = (TransactionLogManager)c1.getLogManager();
    // Around 40 indices per log file on peer 1
    l1.setRaftLogRollSize(1024 * 2000);

    // Start the client load
    loader.startReplicationLoad(100);

    // Let the traffic fly for a while
    transactionNum = loader.makeProgress(10000, transactionNum);
    l3.rollCommittedLogs();
    l1.rollCommittedLogs();

    RemoteLogFetcher fetcher = new RemoteLogFetcher(c2);
    fetcher.initializeQuorumClients();
    List<Pair<String, List<LogFileInfo>>> statuses = fetcher.getPeerCommittedLogStatus(0);
    assertFalse(statuses.isEmpty());
    long minIndex = Long.MAX_VALUE;
    long maxIndex = Long.MIN_VALUE;
    for (Pair<String, List<LogFileInfo>> status : statuses) {
      for (LogFileInfo info : status.getSecond()) {
        if (info.getLastIndex() > maxIndex) {
          maxIndex = info.getLastIndex();
        }
        if (info.getInitialIndex() < minIndex) {
          minIndex = info.getInitialIndex();
        }
      }
    }
    assertTrue(minIndex == 1);
    assertTrue(maxIndex > 1);
    LOG.debug("Fetch log files for range [" + minIndex + ", " + maxIndex + "]");

    Collection<FetchTask> tasks = fetcher.createFetchTasks(statuses, minIndex);
    for (FetchTask task : tasks) {
      LOG.debug(task.toString());
    }
    validateIndexCoverage(minIndex, maxIndex, tasks);

    // Stop the client load
    loader.stopReplicationLoad();
  }

  /**
   * Validate indexes in all fetch tasks can be merged into one range, which matches
   * the maximum index on any other peer
   */
  private void validateIndexCoverage(long minIndex, long maxIndex, Collection<FetchTask> tasks) {
    List<Interval> intervals = new ArrayList<>();
    for (FetchTask task : tasks) {
      List<LogFileInfo> fileInfos = task.getFileInfos();
      for (LogFileInfo info : fileInfos) {
        Interval interval = new Interval(info.getInitialIndex(), info.getLastIndex());
        intervals.add(interval);
      }
    }

    Collections.sort(intervals, new Comparator<Interval>() {
      @Override
      public int compare(Interval i1, Interval i2) {
        if (i1.start < i2.start) {
          return -1;
        } else if (i1.start > i2.start) {
          return 1;
        } else {
          if (i1.end < i2.end) {
            return -1;
          } else if (i1.end > i2.end) {
            return 1;
          } else {
            return 0;
          }
        }
      }
    });

    // Merge sorted intervals into a set of minimum discrete intervals
    ArrayList<Interval> res = new ArrayList<>();
    for (Interval cur : intervals) {
      if (res.isEmpty()) {
        res.add(cur);
      } else {
        Interval last = res.get(res.size() - 1);
        if (last.end >= cur.start - 1) {
          last.end = Math.max(last.end, cur.end);
        } else {
          res.add(cur);
        }
      }
    }

    assertEquals("Indices should merge into one interval", 1, res.size());
    Interval interval = res.get(0);
    assertEquals("Min index should match", minIndex, interval.start);
    assertEquals("Max index should match", maxIndex, interval.end);
  }

  private class Interval {
    public long start;
    public long end;
    public Interval(long s, long e) { start = s; end = e; }
  }
}

