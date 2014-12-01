package org.apache.hadoop.hbase.consensus;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.consensus.client.QuorumClient;
import org.apache.hadoop.hbase.consensus.quorum.QuorumInfo;
import org.apache.hadoop.hbase.consensus.quorum.RaftQuorumContext;
import org.apache.hadoop.hbase.consensus.server.InstrumentedConsensusServiceImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@RunWith(value = Parameterized.class)
public class TestBasicPeerSlow {
  private static int QUORUM_SIZE = 5;
  private static int QUORUM_MAJORITY = 3;
  private static QuorumInfo quorumInfo;
  private static RaftTestUtil RAFT_TEST_UTIL = new RaftTestUtil();
  private static QuorumClient client;
  private static volatile int transactionNums = 0;
  private static ThreadPoolExecutor loadGeneratorExecutor;
  private static volatile boolean stop = false;
  private static volatile long clientTrafficFrequency;
  private List<int[]> testEvents;

  @Before
  public void setUp() throws Exception {
    clientTrafficFrequency = 10;
    RaftTestUtil.disableVerboseLogging();
    RAFT_TEST_UTIL.createRaftCluster(QUORUM_SIZE);
    RAFT_TEST_UTIL.setUsePeristentLog(true);
    RAFT_TEST_UTIL.assertAllServersRunning();
    quorumInfo = RAFT_TEST_UTIL.initializePeers();
    RAFT_TEST_UTIL.addQuorum(quorumInfo, RaftTestUtil.getScratchSetup(QUORUM_SIZE));
    RAFT_TEST_UTIL.startQuorum(quorumInfo);
    client = RAFT_TEST_UTIL.getQuorumClient(quorumInfo);

    transactionNums = 0;
    stop = false;
  }

  @After
  public void tearDown() throws Exception {
    RAFT_TEST_UTIL.shutdown();
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {

    List<int[]> test1 = Arrays.asList(new int[][]{
      {1, InstrumentedConsensusServiceImpl.PacketDropStyle.RANDOM.ordinal()},
      {2, InstrumentedConsensusServiceImpl.PacketDropStyle.RANDOM.ordinal()},
    });

    List<int[]> test2 = Arrays.asList(new int[][]{
      {3, InstrumentedConsensusServiceImpl.PacketDropStyle.ALWAYS.ordinal()},
      {4, InstrumentedConsensusServiceImpl.PacketDropStyle.ALWAYS.ordinal()},
    });

    List<int[]> test3= Arrays.asList(new int[][]{
      {3, InstrumentedConsensusServiceImpl.PacketDropStyle.ALWAYS.ordinal()},
      {4, InstrumentedConsensusServiceImpl.PacketDropStyle.RANDOM.ordinal()},
    });

    List<int[]> test4 = Arrays.asList(new int[][]{
      {1, InstrumentedConsensusServiceImpl.PacketDropStyle.RANDOM.ordinal()},
      {2, InstrumentedConsensusServiceImpl.PacketDropStyle.RANDOM.ordinal()},
      {3, InstrumentedConsensusServiceImpl.PacketDropStyle.RANDOM.ordinal()},
    });

    List<int[]> test5= Arrays.asList(new int[][]{
      {2, InstrumentedConsensusServiceImpl.PacketDropStyle.RANDOM.ordinal()},
      {3, InstrumentedConsensusServiceImpl.PacketDropStyle.RANDOM.ordinal()},
      {4, InstrumentedConsensusServiceImpl.PacketDropStyle.ALWAYS.ordinal()},
    });

    List<int[]> test6= Arrays.asList(new int[][]{
      {1, InstrumentedConsensusServiceImpl.PacketDropStyle.ALWAYS.ordinal()},
      {2, InstrumentedConsensusServiceImpl.PacketDropStyle.ALWAYS.ordinal()},
      {3, InstrumentedConsensusServiceImpl.PacketDropStyle.RANDOM.ordinal()},
    });

    Object[][] data = new Object[][] {
      { test1 },
      { test2 },
      { test3 },
      { test4 },
      { test5 },
      { test6 }
    };
    return Arrays.asList(data);

  }

  public TestBasicPeerSlow(List<int[]> testEvents) {
    this.testEvents = testEvents;
  }

  @Test(timeout=180000)
  public void testSingleSlowFollower() throws InterruptedException {
    final long sleepTime = HConstants.PROGRESS_TIMEOUT_INTERVAL_IN_MILLISECONDS;

    // Start the client load
    startReplicationLoad(100);

    // Let the traffic fly for a while
    makeProgress(sleepTime, transactionNums, true);

    // Simulate all the events
    for (int [] event: testEvents) {
      // Get the rank as well as the style
      assert event.length == 2;
      int rank = event[0];
      InstrumentedConsensusServiceImpl.PacketDropStyle style =
        InstrumentedConsensusServiceImpl.PacketDropStyle.values()[event[1]];

      RaftQuorumContext context = RAFT_TEST_UTIL.getRaftQuorumContextByRank(quorumInfo, rank);
      RAFT_TEST_UTIL.simulatePacketDropForServer(quorumInfo, rank, style);

      System.out.println("Set package drop for the quorum: " + context + " as " + style);
    }


    boolean waitForProgress = true;

    // In case we are affecting majority or more number of peers, then don't
    // wait for progress in the quorum. Just make sure that we recover, once
    // things are back to normal
    if (testEvents.size() >= QUORUM_MAJORITY) {
      waitForProgress = false;
    }
    // Let the traffic fly for a while
    makeProgress(sleepTime, transactionNums, waitForProgress);

    // Stop the package drop
    for (int [] event: testEvents) {
      // Get the rank as well as the style
      assert event.length == 2;
      int rank = event[0];
      InstrumentedConsensusServiceImpl.PacketDropStyle nodrop =
        InstrumentedConsensusServiceImpl.PacketDropStyle.NONE;

      RaftQuorumContext context = RAFT_TEST_UTIL.getRaftQuorumContextByRank(quorumInfo, rank);
      RAFT_TEST_UTIL.simulatePacketDropForServer(quorumInfo, rank, nodrop);

      System.out.println("Reset package drop for the quorum: " + context + " as " + nodrop);
    }

    // Slow down the client traffic;
    clientTrafficFrequency = clientTrafficFrequency * 5;

    // Let the traffic fly for a while
    makeProgress(sleepTime, transactionNums, true);

    // Verify logs are identical across all the quorum members
    while (!RAFT_TEST_UTIL.verifyLogs(quorumInfo, QUORUM_SIZE)) {
      Thread.sleep(5 * 1000);
      clientTrafficFrequency = clientTrafficFrequency * 10;
      System.out.println("Verifying logs ....");
    }

    // Stop the replication load
    stopReplicationLoad();

    System.out.println(transactionNums + " transactions have been successfully replicated");
  }

  private int makeProgress(long sleepTime, int prevLoad, boolean waitForProgress)
    throws InterruptedException {
    System.out.println("Let the client load fly for " + sleepTime + " ms");
    Thread.sleep(sleepTime);
    RAFT_TEST_UTIL.printStatusOfQuorum(quorumInfo);

    int i = 0;
    while ((waitForProgress && transactionNums <= prevLoad) ||
      (!waitForProgress && (++i <= 1))) {
      System.out.println("No Progress ! prev " + prevLoad + " current " + transactionNums);
      RAFT_TEST_UTIL.printStatusOfQuorum(quorumInfo);
      Thread.sleep(sleepTime);
    }

    return transactionNums;
  }

  private void startReplicationLoad(final int progressInterval) {
    loadGeneratorExecutor = new ThreadPoolExecutor(1, 1,
      0L, TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue<Runnable>());

    loadGeneratorExecutor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          while (!stop) {
            try {
              client
                .replicateCommits(RaftTestUtil.generateTransaction(1 * 1024));
              if ((++transactionNums) % progressInterval == 0) {
                System.out.println("Sent " + transactionNums +
                  "transactions to the quorum");
                RAFT_TEST_UTIL.printStatusOfQuorum(quorumInfo);
              }
            } catch (Exception e) {
              System.out.print(String.format("Cannot replicate transaction" + e));
            }
            Thread.sleep(clientTrafficFrequency);
          }
        } catch (InterruptedException e) {
          System.out.println("Failed to replicate transactions due to  " + e);
        }
      }
    });
  }

  private void stopReplicationLoad() throws InterruptedException {
    stop = true;
    loadGeneratorExecutor.shutdownNow();
    loadGeneratorExecutor.awaitTermination(10, TimeUnit.SECONDS);
    System.out.println("Shutdown the replication load and " + transactionNums + " transactions " +
      "have been successfully replicated");
  }
}
