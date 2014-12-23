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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.consensus.exceptions.LeaderNotReadyException;
import org.apache.hadoop.hbase.consensus.exceptions.NewLeaderException;
import org.apache.hadoop.hbase.consensus.quorum.QuorumAgent;
import org.apache.hadoop.hbase.consensus.quorum.QuorumInfo;
import org.apache.hadoop.hbase.consensus.quorum.RaftQuorumContext;
import org.apache.hadoop.hbase.consensus.server.InstrumentedConsensusServiceImpl;
import org.apache.hadoop.hbase.consensus.server.LocalConsensusServer;
import org.apache.hadoop.hbase.consensus.server.peer.states.PeerHandleAppendResponse;

import org.apache.hadoop.hbase.consensus.util.RaftUtil;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A test utility for Unit Testing RAFT protocol by itself.
 */
public class LocalTestBed {
  private static final Logger LOG = LoggerFactory.getLogger(LocalTestBed.class);

  private static int DEFAULT_RAFT_CONSENSUS_START_PORT_NUMBER = 60080;
  private static int QUORUM_SIZE = 5;
  private static final int QUORUM_MAJORITY = 3;
  private static int nextPortNumber;

  private Configuration conf;
  private static QuorumInfo quorumInfo;
  private static RaftTestUtil RAFT_TEST_UTIL = new RaftTestUtil();
  private final List<int[]> mockLogs;
  private boolean checkLeaderCount = false;
  private boolean usePersistentLog = true;
  private boolean autoRestartThriftServer = true;
  private AtomicLong commitSuccessCount = new AtomicLong(0);
  private AtomicLong commitFailureCount = new AtomicLong(0);
  private HealthChecker checker = null;

  public static String LOCAL_HOST = "127.0.0.1";
  ConcurrentHashMap<String, LocalConsensusServer> servers;

  public final long BigBang = System.currentTimeMillis();

  public static class HealthChecker extends Thread {
    private QuorumInfo   quorumInfo;
    private RaftTestUtil  testUtil;
    private boolean       autoRestartThriftServer;
    private long          checkInterval;
    private AtomicBoolean time2die = new AtomicBoolean(false);

    public HealthChecker(QuorumInfo quorumInfo, RaftTestUtil testUtil, boolean autoRestartThriftServer, long checkInterval) {
      this.quorumInfo = quorumInfo;
      this.testUtil = testUtil;
      this.autoRestartThriftServer = autoRestartThriftServer;
      this.checkInterval = checkInterval;
    }

    @Override
    public void run() {
      long previousCheckTime = 0L;
      while (!time2die.get()) {
        try {
          long now = System.currentTimeMillis();
          if (now >= previousCheckTime + checkInterval) {
            LOG.info("checking the health of all quorum members ......");
            testUtil.checkHealth(quorumInfo, autoRestartThriftServer);
            previousCheckTime = now = System.currentTimeMillis();
          }
          long sleepTime = previousCheckTime + checkInterval - now;
          if (sleepTime > 0) {
            Thread.sleep(sleepTime);
          }
        } catch (InterruptedException ex) {
          LOG.info("Time to Die!!");
          time2die.set(true);
        } catch (Throwable stone) {
          LOG.warn("Caught an exception while checking health", stone);
        }
      }
    }

    public void shutdown() {
      time2die.set(true);
    }
  }


  public static class LinkState {
    private HServerAddress  src;
    private HServerAddress  dst;
    private State           state;
    private long            delay = 0L;
    private long            hiccupStopTime = 0L;
    private long            hiccupStartTime = 0L;

    public static enum State {
      UP, DOWN
    }

    public LinkState(HServerAddress src, HServerAddress dst, State state) {
      this.src = src;
      this.dst = dst;
      this.state = state;
    }

    public HServerAddress getSrc() {
      return src;
    }

    public HServerAddress getDst() {
      return dst;
    }

    public void setDelay(long delay) {
      this.delay = delay;
    }

    public boolean inHiccup(long now) {
      return hiccupStartTime <= now && now <= hiccupStopTime;
    }

    public long getDelay(long now) {
      return delay;
    }

    public void setHiccupTime(long start, long stop) {
      hiccupStartTime = start;
      hiccupStopTime = stop;
    }

    public long getHiccupStartTime() {
      return hiccupStartTime;
    }

    public long getHiccupStopTime() {
      return hiccupStopTime;
    }

    @Override
    public String toString() {
      return "[" + src + "--" + dst + "]";
    }
  }

  public static class NodeState {
    private State state;
    public static enum State {
      UP, DOWN, INTERMITTENT, SLOW
    }

    public NodeState(State state) {
    }

    public NodeState changeState(State state) {
      return this;
    }
  }

  public static class Chaos {
    private TestConfig config;
    private Random prng;

    // the map is maintained per pair of nodes
    private ConcurrentHashMap<HServerAddress, ConcurrentHashMap<HServerAddress, LinkState>> links
      = new ConcurrentHashMap<HServerAddress, ConcurrentHashMap<HServerAddress, LinkState>>();

    private ConcurrentHashMap<String, NodeState> nodes =
      new ConcurrentHashMap<String, NodeState>();

    public Chaos() {
      this(new TestConfig());
    }

    public Chaos(TestConfig config) {
      this.config = config;
      prng = new Random(config.getLong("seed", System.currentTimeMillis()));
    }

    public LinkState getLinkState(HServerAddress src, HServerAddress dst) {
      if (src == null || dst == null || src.equals(dst)) {
        return null;
      }
      if (src.toString().compareTo(dst.toString()) > 0) {
        HServerAddress tmp = src;
        src = dst;
        dst = tmp;
      }
      if (links.get(src) == null) {
        links.put(src, new ConcurrentHashMap<HServerAddress, LinkState>());
      }
      if (links.get(src).get(dst) == null) {
        links.get(src).put(dst, new LinkState(src, dst, LinkState.State.UP));
      }
      return links.get(src).get(dst);
    }

    public void updateNetworkStates(long now, boolean clear) {
      double defaultDropRate = config.getDouble("packet-drop-rate", 0.0);
      double maxRandomDelay = config.getDouble("max-random-delay", 20.0);
      double minRandomDelay = config.getDouble("min-random-delay", 00.0);

      long maxHiccupDuration = config.getLong("max-hiccup-duration", 10000L);
      long minHiccupDuration = config.getLong("min-hiccup-duration",  1000L);
      long maxHiccupLinks = config.getLong("max-hiccup-links", 1L);
      long hiccupLambda = config.getLong("hiccup-lambda",  -1L);

      ConcurrentHashMap<String, LocalConsensusServer> servers = RAFT_TEST_UTIL.getServers();
      Set<LinkState> currentHiccups = new HashSet<LinkState>();
      Set<LinkState> allHiccups = new HashSet<LinkState>(); // include future hiccups
      Set<LinkState> nohiccups = new HashSet<LinkState>();
      for (HServerAddress dst : quorumInfo.getPeersWithRank().keySet()) {
        dst = RaftUtil.getLocalConsensusAddress(dst);
        for (HServerAddress src : quorumInfo.getPeersWithRank().keySet()) {
          src = RaftUtil.getLocalConsensusAddress(src);
          if (src.equals(dst)) {
            continue;
          }
          LinkState link = getLinkState(src, dst);
          if (link.inHiccup(now)) {
            currentHiccups.add(link);
            allHiccups.add(link);
          } else if (link.getHiccupStartTime() > now) {
            allHiccups.add(link);
          } else {
            // We know link.getHiccupStopTime() < now, too
            nohiccups.add(link);
          }
        }
      }

      if (currentHiccups.size() > 0) {
        LOG.debug("The following links are in hiccup: " + currentHiccups);
      }

      List<LinkState> futureHiccups = new ArrayList<LinkState>(nohiccups);

      // Spread the love; otherwise, the first few links are always in hiccups (now
      // and in future).
      Collections.shuffle(futureHiccups);

      for (LinkState link : futureHiccups) {
        // Calculate the next hiccup time
        long delta = poissonRandomInterarrivalDelay(prng, (double)hiccupLambda);
        long hiccupStartTime = link.getHiccupStopTime() == 0 ? now + delta : link.getHiccupStopTime() + delta;
        long duration = (long)(prng.nextDouble()*(maxHiccupDuration-minHiccupDuration) + minHiccupDuration);
        long hiccupStopTime = hiccupStartTime + duration;

        // before we schedule this hiccup, make sure we don't have too many
        // hiccups at the same time (both now and in future).
        LinkState tmp = new LinkState(link.getSrc(), link.getDst(), LinkState.State.UP);
        tmp.setHiccupTime(hiccupStartTime, hiccupStopTime);
        allHiccups.add(tmp);
        int nhiccups =computeMaxHiccupLinks(allHiccups);
        allHiccups.remove(tmp);

        if (nhiccups < maxHiccupLinks) {
          LOG.debug("---- scheduling a future hiccup for " + link + " [" + hiccupStartTime + ", " + hiccupStopTime + "]");
          link.setHiccupTime(hiccupStartTime, hiccupStopTime);
          allHiccups.add(link);
        } else {
          LOG.debug("---- too many hiccups right now; not scheduling a future hiccup for " + link + " [" + hiccupStartTime + ", " + hiccupStopTime + "]");
        }
      }

      for (HServerAddress dst : quorumInfo.getPeersWithRank().keySet()) {
        dst = RaftUtil.getLocalConsensusAddress(dst);
        InstrumentedConsensusServiceImpl service = (InstrumentedConsensusServiceImpl)
          (servers.get(dst.getHostAddressWithPort()).getHandler());
        for (HServerAddress src : quorumInfo.getPeersWithRank().keySet()) {
          src = RaftUtil.getLocalConsensusAddress(src);
          if (!src.equals(dst) && !clear) {
            long delay = (long)(prng.nextDouble()*(maxRandomDelay-minRandomDelay) + minRandomDelay);
            double dropRate = defaultDropRate;
            boolean inHiccup = false;
            if (hiccupLambda > 0) {
              LinkState link = getLinkState(src, dst);
              if (link.inHiccup(now)) {
                LOG.debug("---- " + link + " in hiccup right now!");
                dropRate = 1.1; // make sure we take care of floating-point inaccuracies
                inHiccup = true;
              }
            }
            service.setHiccup(src.toString(), inHiccup);
            service.setPacketDropRate(src.toString(), dropRate);
            service.setPacketDelay(src.toString(), delay);
          } else {
            service.setHiccup(src.toString(), false);
            service.setPacketDropRate(src.toString(), 0.0);
            service.setPacketDelay(src.toString(), 0L);
          }
        }
      }
    }

    /**
     *  computeMaxHiccupLinks
     *
     *  Compute the maximum number of overlapping hiccups.
     *
     *  We can really use a unit test here.
     */
    public int computeMaxHiccupLinks(Set<LinkState> hiccups) {
      if (hiccups == null || hiccups.size() == 0) {
        return 0;
      }
      List<Long> endpoints = new ArrayList<Long>();
      for (LinkState x : hiccups) {
        endpoints.add(x.getHiccupStartTime());
        endpoints.add(x.getHiccupStopTime());
      }
      Collections.sort(endpoints);

      // we are guaranteed here that endpoints.size() >= 2
      int maxCount = 0;
      for (int i=1; i<endpoints.size(); i++) {
        Long start = endpoints.get(i-1);
        Long stop = endpoints.get(i);
        int count = 0;
        for (LinkState link : hiccups) {
          // Two intervals are disjoint iff one's start > the other's end
          // because there exists a point that partitions the one-dimensional
          // space.
          boolean disjoint = link.getHiccupStartTime() > stop
            || start > link.getHiccupStopTime();
          if (!disjoint) {
            count ++;
          }
        }
        maxCount = Math.max(maxCount, count);
      }
      return maxCount;
    }
  }

  public static class Adversary {
    private Chaos       chaos;
    private long        previousNetworkChangeTime = 0L;
    private TestConfig  config;

    public Adversary(Chaos chaos, TestConfig config) {
      this.chaos = chaos;
      this.config = config;
    }

    public Chaos getChaos() {
      return chaos;
    }

    public List<TestEvent> getEvents(long now) {
      if (now - previousNetworkChangeTime > 1000L) {
        LOG.info("Changing network delays ......");
        chaos.updateNetworkStates(now, false);
        previousNetworkChangeTime = now;
      }
      return new ArrayList<TestEvent>();
    }
  }


  public static class TestEvent {
    private EventType     type;
    private long          when;
    private List<String>  nodes;
    private List<WALEdit> edits;
    private long          sleepAmount;
    private Map<String, Map<String, Long>> delayMap;

    public static enum EventType {
      START, STOP, NOOP, SLEEP, NEW_COMMITS, UPDATE_DELAYS, CRASH_NODES, START_NODES, DONE;
    }

    public TestEvent(EventType type) {
      this(type, System.currentTimeMillis());
    }

    public TestEvent(EventType type, long when) {
      this.type = type;
      this.when = when;
    }

    public EventType type() {
      return type;
    }

    public long when() {
      return when;
    }

    public long sleepAmount() {
      return sleepAmount;
    }

    public List<String> nodes() {
      return nodes;
    }

    public Map<String, Map<String, Long>> delayMap() {
      return delayMap;
    }

    public List<WALEdit> edits() {
      return edits;
    }

    public TestEvent sleep(long ms) {
      this.sleepAmount = ms;
      return this;
    }

    public TestEvent crash(List<String> nodes) {
      this.nodes = nodes;
      return this;
    }

    public TestEvent start(List<String> nodes) {
      this.nodes = nodes;
      return this;
    }

    public TestEvent updateDelays(Map<String, Map<String, Long>> delayMap) {
      this.delayMap = delayMap;
      return this;
    }

    public TestEvent newCommits(List<WALEdit> edits) {
      this.edits = edits;
      return this;
    }
  }

  public static class TestRequest implements Runnable {
    public static enum State {
      READY, RUNNING, DONE, FAILED
    }
    private LocalTestBed    testbed;
    private List<WALEdit>   edits;
    private State           state = State.READY;

    public TestRequest(LocalTestBed testbed, List<WALEdit> edits) {
      this.testbed = testbed;
      this.edits = edits;
    }

    public synchronized State state() {
      return state;
    }

    public void run() {
      synchronized (this) { state = State.RUNNING; }
      LOG.info("commiting ......");
      testbed.dumpStates();
      boolean done = testbed.testSingleCommit(edits);
      synchronized (this) { state = done ? State.DONE : State.FAILED; }
      testbed.incrementCommitCount(done);
      LOG.info("DONE commiting ......");
      testbed.dumpStates();
    }
  }


  public static class TestConfig {

    private Map<String, String> kvs = new HashMap<String, String>();

    public TestConfig(String... args) {
      kvs.put("seed", "" + System.currentTimeMillis());
      kvs.put("packet-arrival-lambda", "100.0");
      kvs.put("max-test-time", "3600000");
      kvs.put("max-commits", "100");
      kvs.put("event-loop-step", "1");
      kvs.put("min-random-delay", "10");
      kvs.put("max-random-delay", "100");
      kvs.put("packet-drop-rate", "0.00");

      for (String arg : args) {
        String [] pair = arg.trim().split("=");
        if (pair.length == 2) {
          kvs.put(pair[0], pair[1]);
        } else if (pair.length == 1) {
          kvs.put(pair[0], "");
        }
      }
    }

    public String get(String key) {
      return kvs.get(key);
    }

    public String get(String key, String defaultValue) {
      if (kvs.get(key) == null) {
        return defaultValue;
      } else {
        return kvs.get(key);
      }
    }

    public double getDouble(String key, double defaultValue) {
      String value = kvs.get(key);
      if (value == null) {
        return defaultValue;
      }
      try {
        return Double.parseDouble(value);
      } catch (Exception ex) {
        return defaultValue;
      }
    }

    public long getLong(String key, long defaultValue) {
      String value = kvs.get(key);
      if (value == null) {
        return defaultValue;
      }
      try {
        return Long.parseLong(value);
      } catch (Exception ex) {
        return defaultValue;
      }
    }

    public int getInt(String key, int defaultValue) {
      String value = kvs.get(key);
      if (value == null) {
        return defaultValue;
      }
      try {
        return Integer.parseInt(value);
      } catch (Exception ex) {
        return defaultValue;
      }
    }
  }

  public static class Test implements Runnable {
    private LocalTestBed  testbed;
    private TestConfig    config;
    private long          testStartTime;
    private long          previousTick;
    private int           ncommits = 0;
    private Adversary     adversary;
    private long          testEndTime;
    private long          nextCommitTime;
    private Random        prng;

    public Test(TestConfig config, LocalTestBed testbed) {
      this.config = config;
      this.testbed = testbed;
    }

    public void init(Adversary adversary) {
      prng = new Random(config.getLong("seed", System.currentTimeMillis()));
      testStartTime = System.currentTimeMillis();
      previousTick = 0L;
      this.adversary = adversary;
      nextCommitTime = testStartTime  + 5000L; // one second to settle
      LOG.info("nextCommitTime = " + nextCommitTime);
    }

    public void incrementSuccessfulCommits() {
    }

    public List<TestEvent> getNextEvents(long now) {
      List<TestEvent> events = new ArrayList<TestEvent>();
      long wakeupTime = previousTick + config.getLong("event-loop-step", 50L);
      long delay = Math.max(wakeupTime, now) - now;
      if (delay > 0) {
        noThrowSleep(delay);
      }
      previousTick = Math.max(wakeupTime, now);

      for (TestEvent event : adversary.getEvents(now)) {
        events.add(event);
      }
      // flip a coin
      if (now >= nextCommitTime) {
        events.add(new TestEvent(TestEvent.EventType.NEW_COMMITS).newCommits(generateTestingWALEdit()));
        nextCommitTime += poissonRandomInterarrivalDelay(prng, config.getDouble("packet-arrival-lambda", 100));
        ncommits ++;
      }
      return events;
    }

    public void run() {
      BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
      ThreadPoolExecutor fakeClient = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS, workQueue);
      long previousStateDumpTime = 0L;
      List<TestRequest> requests = new ArrayList<TestRequest>();
      List<TestRequest> processedRequests = new ArrayList<TestRequest>();
  mainloop:
      while ((long)processedRequests.size() < config.getLong("max-commits", 10L)) {
        List<TestEvent> events = getNextEvents(System.currentTimeMillis());
        for (TestEvent event : events) {
          switch (event.type()) {
            case SLEEP:
              noThrowSleep(event.sleepAmount());
              break;

            case NEW_COMMITS:
              if (requests.size() <= 0) {
                TestRequest request = new TestRequest(testbed, event.edits());
                requests.add(request);
                fakeClient.execute(request);
              }
              break;

            case UPDATE_DELAYS:
              break;

            case CRASH_NODES:
              break;

            case START_NODES:
              break;

            default:
              break;
          }
          if (testbed.checkLeaderCount()) {
            Assert.assertTrue(testbed.getLeaderCount() < 2);
          }
        }
        List<TestRequest> tmp = new ArrayList<TestRequest>();
        for (TestRequest request : requests) {
          switch(request.state()) {
            case FAILED:
            case DONE:
              processedRequests.add(request);
              break;
            default:
              tmp.add(request);
          }
        }
        requests = tmp;
        if (previousStateDumpTime + 2000L < System.currentTimeMillis()) {
          long t1 = System.nanoTime();
          testbed.dumpStates();
          long t2 = System.nanoTime();
          LOG.info("Dumping States took " + (t2-t1)/1000L + " us");
          previousStateDumpTime = System.currentTimeMillis();
        }
      }

      LOG.info(
          "\n\n"
          + "-------------------------------------------\n"
          + "SHUTTING DOWN THE SIMULATOR ......\n"
          + "-------------------------------------------\n"
      );

      fakeClient.shutdown();

      try {
        fakeClient.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
      } catch (InterruptedException ex) {
        LOG.error("Time to die!");
        System.exit(1);
      }

      int ndone = 0;
      int nfail = 0;
      for (TestRequest request : processedRequests) {
        switch (request.state()) {
          case FAILED:
            nfail ++;
            break;
          case DONE:
            ndone ++;
            break;

          default:
            LOG.error("There are requests that have not been processed!");
            System.exit(1);
        }
      }
      LOG.info("\n\n"
          + "--------------------------------------------------------\n"
          + "Successfully committed " + ndone  + " client requests\n"
          + "    Stats\n"
          + "    - droppped " + testbed.getPacketDropCount() + " packets (" + testbed.getHiccupPacketDropCount() + " due to hiccups)\n"
          + "--------------------------------------------------------\n"
      );
      if (nfail > 0) {
        LOG.error("There are failed requests!");
        System.exit(1);
      }

      LOG.info("Clearing network states ......");
      adversary.getChaos().updateNetworkStates(System.currentTimeMillis(), true);
      LOG.info("-------- Verifying log consistency amongst all quorum members");
      while (!RAFT_TEST_UTIL.verifyLogs(quorumInfo, QUORUM_SIZE, true)) {
        testbed.dumpStates();
        if (testbed.checkLeaderCount()) {
          Assert.assertTrue(testbed.getLeaderCount() < 2);
        }
        noThrowSleep(5000L);
      }
      LOG.info("-------- Done verifying log consistency amongst all quorum members");
      testbed.dumpStates();
    }
  }


  public void start() throws Exception {
    start(new TestConfig());
  }

  public void start(TestConfig config) throws Exception {
    PeerHandleAppendResponse.setGlobalMaxBatchLogs((int)config.getLong("max-batch-logs", 64));
    int port  = config.getInt("regionserver-port", -1);
    if (port > 0) {
      LOG.info("Setting the HBASE region server port to " + port);
      RAFT_TEST_UTIL.getConf().setInt(HConstants.REGIONSERVER_PORT, port);
    }

    long timeout = config.getLong("handle-rpc-timeout", -1);
    if (timeout > 0) {
      LOG.info("Setting the RPC Error timeout to " + timeout + " ms");
      RAFT_TEST_UTIL.getConf().setLong(HConstants.RAFT_PEERSERVER_HANDLE_RPC_TIMEOUT_MS, timeout);
    }

    RAFT_TEST_UTIL.setUsePeristentLog(usePersistentLog);
    RAFT_TEST_UTIL.createRaftCluster(QUORUM_SIZE);
    RAFT_TEST_UTIL.assertAllServersRunning();
    quorumInfo = RAFT_TEST_UTIL.initializePeers();
    RAFT_TEST_UTIL.addQuorum(quorumInfo, mockLogs);
    RAFT_TEST_UTIL.startQuorum(quorumInfo);
    checker = new HealthChecker(quorumInfo, RAFT_TEST_UTIL, autoRestartThriftServer, 30000L);
    checker.start();
  }

  public void stop() throws Exception {
    if (checker != null) {
      checker.shutdown();
    }
    RAFT_TEST_UTIL.shutdown();
  }

  public LocalTestBed() {
    this(null);
  }

  public LocalTestBed(List<int[]> logs) {
    if (logs == null) {
      logs = new ArrayList<int[]>();
      for (int i=0; i<QUORUM_SIZE; i++) {
        logs.add(new int[] {});
      }
    }
    mockLogs = logs;
  }

  private static List<WALEdit> generateTestingWALEdit() {
    KeyValue kv = KeyValue.createFirstOnRow(Bytes.toBytes("TestQuorum"));
    return Arrays.asList(new WALEdit(Arrays.asList(kv)));
  }

  public void dumpStates() {
    RAFT_TEST_UTIL.dumpStates(quorumInfo);
    LOG.info("Total Commit = " + commitSuccessCount.get()+ " successes and " + commitFailureCount.get() + " failures "
        + " with " + getPacketDropCount()  + "(" + getHiccupPacketDropCount() + ") total-dropped (hiccup) packets "
        + " and " + RAFT_TEST_UTIL.getServerRestartCount() + " server restarts "
        + " in " + (System.currentTimeMillis() - BigBang)/1000L + " secs");
  }

  public void incrementCommitCount(boolean success) {
    if (success) {
      commitSuccessCount.incrementAndGet();
    } else {
      commitFailureCount.incrementAndGet();
    }
  }

  private boolean doCommit(final WALEdit edit) {
    if (edit == null) {
      return true;
    }
    retry_loop:
    while (true) {
      try {
        QuorumAgent agent = getLeader();
        if (agent == null) {
          LOG.info("NULL leader. Sleep for some time ......");
          try {
            Thread.sleep(1000L);
          } catch (InterruptedException ex) {
            LOG.info("Time to Die!");
            return false;
          }
          continue;
        }
        agent.syncAppend(edit);
        // Verify all the logs across the majority are the same
        RAFT_TEST_UTIL.verifyLogs(quorumInfo, QUORUM_MAJORITY, false);
        return true;
      } catch (NewLeaderException e) {
        LOG.warn("Got a new leader in the quorum: " + e.getNewLeaderAddress());
        RAFT_TEST_UTIL.verifyLogs(quorumInfo, QUORUM_MAJORITY, false);
      } catch (Exception e) {
        Throwable cause = e;
        while (cause != null) {
          if (cause instanceof NewLeaderException || cause instanceof LeaderNotReadyException) {
            continue retry_loop;
          }
          cause = cause.getCause();
        }
        LOG.error("Errors: ", e);
      }
    }
  }

  public boolean testSingleCommit(List<WALEdit> edits) {
    boolean success = true;
    if (edits.size() == 0) {
      return true;
    }

    for (final WALEdit edit : edits) {
      if ((success = doCommit(edit)) == false) {
        break;
      }
    }
    return success;
  }

  public QuorumAgent getLeader() {
    RaftQuorumContext leaderQuorum = null;
    do {
      int leaderCnt = 0;
      for (LocalConsensusServer server : RAFT_TEST_UTIL.getServers().values()) {
        RaftQuorumContext c = server.getHandler().getRaftQuorumContext(quorumInfo.getQuorumName());
        if (c != null && c.isLeader()) {
          leaderQuorum = c;
          leaderCnt++;
        }
      }
      if (checkLeaderCount()) {
        Assert.assertTrue(getLeaderCount() < 2);
      }
    } while (leaderQuorum == null );
    return leaderQuorum.getQuorumAgentInstance();
  }

  public int getLeaderCount() {
    int leaderCnt = 0;
    for (LocalConsensusServer server : RAFT_TEST_UTIL.getServers().values()) {
      RaftQuorumContext c = server.getHandler().getRaftQuorumContext(quorumInfo.getQuorumName());
      if (c != null && c.isLeader()) {
        leaderCnt++;
      }
    }
    return leaderCnt;
  }

  public static void noThrowSleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  public void runConsensusProtocol() throws Exception {
    runConsensusProtocol(new TestConfig());
  }

  public void runConsensusProtocol(TestConfig config) throws Exception {
    start(config);

    try {
      Chaos chaos = new Chaos(config);
      Test test = new Test(config, this);
      Adversary adversary = new Adversary(chaos, config);
      test.init(adversary);
      test.run();
      LOG.info("Stopping the experiment ......");
      stop();
    } catch (Throwable fit) {
      LOG.error("runConsensusProtocol caught a fit", fit);
      System.exit(1);
    }
  }

  public boolean checkLeaderCount() {
    return checkLeaderCount;
  }

  public void parseOptions(String[] args) {
    for (String arg : args) {
      if (arg.equalsIgnoreCase("--check-leader-count")) {
        checkLeaderCount = true;
      } else if (arg.equalsIgnoreCase("--no-check-leader-count")) {
        checkLeaderCount = false;
      } else if (arg.equalsIgnoreCase("--persistent-log")) {
        usePersistentLog = true;
      } else if (arg.equalsIgnoreCase("--no-persistent-log")) {
        usePersistentLog = false;
      } else if (arg.equalsIgnoreCase("--auto-restart-thrift")) {
        autoRestartThriftServer = true;
      } else if (arg.equalsIgnoreCase("--no-auto-restart-thrift")) {
        autoRestartThriftServer = false;
      }
    }
  }

  public long getPacketDropCount() {
    return RAFT_TEST_UTIL.getPacketDropCount(quorumInfo);
  }

  public long getHiccupPacketDropCount() {
    return RAFT_TEST_UTIL.getHiccupPacketDropCount(quorumInfo);
  }

  // shoplifted from http://preshing.com/20111007/how-to-generate-random-timings-for-a-poisson-process/
  public static long poissonRandomInterarrivalDelay(Random prng, double lambda) {
    return Math.max(0L, (long)(-Math.log(1.0 - prng.nextDouble()) * lambda));
  }

  public static void main(String[] args) throws Exception {
    LocalTestBed testbed = new LocalTestBed();
    testbed.parseOptions(args);
    testbed.runConsensusProtocol(new TestConfig(args));
    LOG.info("\n\n"
        + "--------------------------------------------------------\n"
        + "SUCCESS\n"
        + "    Stats\n"
        + "    - droppped " + testbed.getPacketDropCount() + " packets (" + testbed.getHiccupPacketDropCount() + " due to hiccups)\n"
        + "--------------------------------------------------------\n"
    );
    System.exit(0);
  }
}
