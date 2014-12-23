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


import com.facebook.swift.service.ThriftEventHandler;
import com.facebook.swift.service.ThriftServerConfig;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.consensus.client.QuorumClient;
import org.apache.hadoop.hbase.consensus.client.QuorumThriftClientAgent;
import org.apache.hadoop.hbase.consensus.log.CommitLogManagerInterface;
import org.apache.hadoop.hbase.consensus.log.InMemoryLogManager;
import org.apache.hadoop.hbase.consensus.log.TransactionLogManager;
import org.apache.hadoop.hbase.consensus.protocol.EditId;
import org.apache.hadoop.hbase.consensus.quorum.AggregateTimer;
import org.apache.hadoop.hbase.consensus.quorum.QuorumInfo;
import org.apache.hadoop.hbase.consensus.quorum.RaftQuorumContext;
import org.apache.hadoop.hbase.consensus.rpc.PeerStatus;
import org.apache.hadoop.hbase.consensus.server.ConsensusServiceImpl;
import org.apache.hadoop.hbase.consensus.server.InstrumentedConsensusServiceImpl;
import org.apache.hadoop.hbase.consensus.server.LocalConsensusServer;
import org.apache.hadoop.hbase.consensus.util.RaftUtil;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;
import org.apache.hadoop.hbase.util.serial.SerialExecutorService;
import org.apache.hadoop.net.DNS;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;


/**
 * A test utility for Unit Testing RAFT protocol by itself.
 */
public class RaftTestUtil {
  private static final Logger LOG = LoggerFactory.getLogger(RaftTestUtil.class);

  ConcurrentHashMap<String, LocalConsensusServer> servers;

  private static int DEFAULT_RAFT_CONSENSUS_START_PORT_NUMBER = 60080;

  private static int nextPortNumber;
  private Configuration conf = HBaseConfiguration.create();
  ExecutorService pool;
  private String raftDirectory;
  private AtomicLong serverRestartCount = new AtomicLong(0);
  private Class<? extends RaftQuorumContext> raftQuorumContextClass =
    RaftQuorumContext.class;

  private boolean usePersistentLog = false;

  public static String LOCAL_HOST;

  static {
    try {
      LOCAL_HOST = DNS.getDefaultHost("default", "default");
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    ;
  }

  private long seedIndex = HConstants.UNDEFINED_TERM_INDEX;

  /**
   * Creates a cluster of num nodes and starts the Consensus Server on each of
   * the nodes.
   * @param num number of nodes in the cluster
   * @return true if cluster was successfully created, else false
   */
  public boolean createRaftCluster(int num) {

    raftDirectory = "/tmp/" + System.currentTimeMillis();

    if (num % 2 == 0) {
      return false;
    }
    nextPortNumber =
      conf.getInt(HConstants.REGIONSERVER_PORT, HConstants.DEFAULT_REGIONSERVER_PORT) +
        HConstants.CONSENSUS_SERVER_PORT_JUMP;

    for (int i = 0; i < num; i++) {
      addServer(nextPortNumber++);
    }

    for (LocalConsensusServer server : servers.values()) {
      server.startService();
    }
    pool = Executors.newFixedThreadPool(
      HConstants.DEFAULT_QUORUM_CLIENT_NUM_WORKERS,
      new DaemonThreadFactory("QuorumClient-"));
    return true;
  }

  public LocalConsensusServer addServer(int port) {
    List<ThriftEventHandler> handlers = new ArrayList<>();
    conf.setLong(HConstants.QUORUM_CLIENT_COMMIT_DEADLINE_KEY, 20000);
    ThriftServerConfig config =  new ThriftServerConfig()
      .setWorkerThreads(conf.getInt(HConstants.CONSENSUS_SERVER_WORKER_THREAD,
        HConstants.DEFAULT_CONSENSUS_SERVER_WORKER_THREAD))
      .setPort(port)
      .setIoThreadCount(conf.getInt(HConstants.CONSENSUS_SERVER_IO_THREAD,
        HConstants.DEFAULT_CONSENSUS_SERVER_IO_THREAD));
    InstrumentedConsensusServiceImpl service =
      (InstrumentedConsensusServiceImpl) ConsensusServiceImpl.
        createTestConsensusServiceImpl();

    LocalConsensusServer server = new LocalConsensusServer(service, handlers, conf);
    server.initialize(config);
    HServerAddress addr = new HServerAddress(LOCAL_HOST + ":" + port);
    getServers().put(addr.getHostAddressWithPort(), server);
    service.setIdentity(LOCAL_HOST + ":" + port);
    server.startService();
    return server;
  }

  public static int getNextPortNumber() {
    return nextPortNumber;
  }

  public static void setNextPortNumber(int nextPortNumber) {
    RaftTestUtil.nextPortNumber = nextPortNumber;
  }

  public void setRaftQuorumContextClass(Class<? extends RaftQuorumContext> clazz) {
    raftQuorumContextClass = clazz;
  }

  public Configuration getConf() {
    return conf;
  }

  public ConcurrentHashMap<String, LocalConsensusServer> getServers() {
    if (servers == null) {
      servers = new ConcurrentHashMap<>();
    }
    return servers;
  }

  /**
   * Creates a Raft context.
   *
   * Allows override.
   */
  public RaftQuorumContext createRaftQuorumContext(QuorumInfo info, Configuration conf,
      HServerAddress address, LocalConsensusServer server) {
    String mBeansPrefix = "Test.";
    try {
      return raftQuorumContextClass.getConstructor(QuorumInfo.class, Configuration.class,
        HServerAddress.class, String.class, AggregateTimer.class, SerialExecutorService.class,
        ExecutorService.class).newInstance(info, conf, address,
        mBeansPrefix, server.aggregateTimer, server.serialExecutorService,
        server.getExecServiceForThriftClients());
    } catch (Exception e) {
      LOG.error("Could not construct a RaftQuorumContext of type: " + raftQuorumContextClass
          + ", because of: ", e);
      return null;
    }
  }

  /**
   * Creates a new quorum from the given quorum info object.
   * @param quorumInfo The quorum info.
   * @param mockLogs Mock logs for the quorum members.
   */
  public void addQuorum(final QuorumInfo quorumInfo, List<int[]> mockLogs)
          throws IOException {
    int i = 0;
    int[] mockLog;
    for (LocalConsensusServer server : servers.values()) {
      mockLog = null;
      if (mockLogs != null) {
        mockLog = mockLogs.get(i++);
      }
      addQuorumForServer(server, quorumInfo, mockLog);
    }
  }

  public void addQuorumForServer(final LocalConsensusServer server,
                                 final QuorumInfo info, int[] mockLog)
          throws IOException {
    HServerAddress consensusServerAddress = new HServerAddress(LOCAL_HOST,
      server.getThriftServer().getPort());

    Configuration conf = HBaseConfiguration.create(getConf());
    conf.set(HConstants.RAFT_TRANSACTION_LOG_DIRECTORY_KEY,
      raftDirectory + "/" +
        consensusServerAddress.getHostAddressWithPort() + "/wal");
    conf.set(HConstants.RAFT_METADATA_DIRECTORY_KEY,
      raftDirectory + "/" +
        consensusServerAddress.getHostAddressWithPort() + "/metadata");

    conf.setInt(
      HConstants.RAFT_LOG_DELETION_INTERVAL_KEY,
      100);
    conf.setLong(HConstants.CONSENSUS_PUSH_APPEND_MAX_BATCH_BYTES_KEY,
      256 * 1024L);
    conf.setInt(HConstants.CONSENSUS_PUSH_APPEND_MAX_BATCH_LOGS_KEY, 32);

    conf.setLong(HConstants.RAFT_PEERSERVER_HANDLE_RPC_TIMEOUT_MS, 100);

    RaftQuorumContext context = createRaftQuorumContext(info, conf,
      consensusServerAddress, server);
    RaftQuorumContext originalContext = server.getHandler().addRaftQuorumContext(context);
    if (originalContext != null) {
      // Stop the original consensus quorum
      originalContext.stop(true);
    }

    if (mockLog != null) {
      mockTransactionLog(context, mockLog);
    }
  }

  public long getServerRestartCount() {
    return serverRestartCount.get();
  }

  public void checkHealth(QuorumInfo quorumInfo, boolean reset) {
    for (LocalConsensusServer server : servers.values()) {
      boolean healthy = false;
      LOG.info("Checking the health of ThriftServer for " + server + " ......");
      try {
        healthy = checkHealth(quorumInfo, server);
      } catch (Exception ex) {
        LOG.error("Failed to check the status for " + server, ex);
      }
      LOG.info("ThriftServer for " + server + " is " + (healthy ? "healthy" : "unhealthy"));
      if (!healthy && reset) {
        try {
          LOG.info("Restarting an unhealthy ThriftServer for " + server);
          serverRestartCount.incrementAndGet();
          server.restartService();
        } catch (Throwable stone) {
          LOG.error("Caught an error while trying to restart " + server, stone);
        }
      }
    }
  }

  public boolean checkHealth(QuorumInfo quorumInfo, LocalConsensusServer server) throws Exception {
    HServerAddress consensusServerAddress = new HServerAddress(LOCAL_HOST,
      server.getThriftServer().getPort());
    int timeout = 5000;
    LOG.info("Getting QuorumThriftClientAgent for " + consensusServerAddress);
    QuorumThriftClientAgent agent = new QuorumThriftClientAgent(
        consensusServerAddress.toString(), timeout, timeout, timeout, 3);
    LOG.info("QuorumThriftClientAgent for " + consensusServerAddress + " = " + agent);
    PeerStatus status = agent.getPeerStatus(quorumInfo.getQuorumName());
    LOG.info("PeerStatus for " + consensusServerAddress + ": " + status);
    return status != null;
  }

  public RaftQuorumContext restartLocalConsensusServer(LocalConsensusServer server,
                                                       final QuorumInfo quorumInfo,
                                                       final String contextAddress)

    throws IOException {
    server.restartService();
    getServers().put(contextAddress, server);


    HServerAddress consensusServerAddress = new HServerAddress(LOCAL_HOST,
      server.getThriftServer().getPort());
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.RAFT_TRANSACTION_LOG_DIRECTORY_KEY, raftDirectory +
      "/"+ consensusServerAddress.getHostAddressWithPort() + "/wal");
    conf.set(HConstants.RAFT_METADATA_DIRECTORY_KEY,
      raftDirectory + "/" +
        consensusServerAddress.getHostAddressWithPort() + "/metadata");
    conf.setInt(
      HConstants.RAFT_LOG_DELETION_INTERVAL_KEY,
      100);
    RaftQuorumContext context = createRaftQuorumContext(quorumInfo, conf,
      consensusServerAddress, server);
    context.initializeLog();
    context.reseedStartIndex(seedIndex);
    context.startStateMachines();
    server.getHandler().addRaftQuorumContext(context);
    return context;
  }

  public void startQuorum(final QuorumInfo quorumInfo)
    throws IOException {
    for (LocalConsensusServer server : servers.values()) {
      RaftQuorumContext context =
        server.getHandler().getRaftQuorumContext(quorumInfo.getQuorumName());
      context.initializeLog();
      context.reseedStartIndex(seedIndex);
      context.startStateMachines();
    }
  }

  /**
   * Stops all the consensus servers.
   */
  public void shutdown() {
    if (servers != null) {
      for (LocalConsensusServer server : servers.values()) {
        server.stopService();

        for (RaftQuorumContext context :
          server.getHandler().getQuorumContextMapSnapshot().values()) {
          context.stop(true);
        }
      }
      servers = null;
    }
    if (pool != null) {
      pool.shutdownNow();
    }
  }

  public static void setLogging(final Level level) {
    List<String> modules = Arrays.asList(
      "com.facebook.nifty.core",
      "org.apache.hadoop.hbase.consensus.fsm",
      "org.apache.hadoop.hbase.consensus.raft",
      "org.apache.hadoop.hbase.consensus.server",
      "org.apache.hadoop.hbase.consensus.server.peer",
      "org.apache.hadoop.hbase.consensus.server.peer.states",
      "org.apache.hadoop.hbase.consensus.log",
      "org.apache.hadoop.hbase.consensus.quorum"
    );
    for (String module : modules) {
      org.apache.log4j.Logger.getLogger(module).setLevel(level);
    }
  }

  public static void disableVerboseLogging() {
    setLogging(Level.ERROR);
  }

  public static void enableVerboseLogging() {
    setLogging(Level.DEBUG);
  }

  public QuorumClient getQuorumClient(QuorumInfo regionInfo) throws IOException {
    return new QuorumClient(regionInfo, conf, pool);
  }

  private void mockTransactionLog(final RaftQuorumContext context, int[] terms)
          throws IOException {

    CommitLogManagerInterface log = null;
    if (usePersistentLog) {
      log = new TransactionLogManager(context.getConf(), context.getQuorumName()
      , HConstants.UNDEFINED_TERM_INDEX);
    } else {
      log = new InMemoryLogManager();
      context.setLogManager(log);
    }

    log.initialize(context);
    int index = 0;
    List<KeyValue> kvs = new ArrayList<>();
    kvs.add(KeyValue.LOWESTKEY);
    WALEdit edit = new WALEdit(kvs);
    List<WALEdit> entries = new ArrayList<>();
    entries.add(edit);

    long prevTerm = HConstants.UNDEFINED_TERM_INDEX;
    long committedIndex = HConstants.UNDEFINED_TERM_INDEX;
    for (int term : terms) {
      // Entry can only be considered committed to the Data Store if the current
      // term has at least more than one entry committed to the transaction log
      if (prevTerm == term) {
        committedIndex = index - 1;
      }
      log.append(new EditId(term, index), committedIndex,
              WALEdit.serializeToByteBuffer(entries, System.currentTimeMillis(),
                      Compression.Algorithm.NONE));
      ++index;
      prevTerm = term;
    }
  }

  // shop-lifted from http://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Longest_common_substring
  // we should really use longest common subsequence of EditIds. what the heck.
  // let's get it running first.

  private static String longestCommonSubstring(String S1, String S2)
  {
      int Start = 0;
      int Max = 0;
      for (int i = 0; i < S1.length(); i++)
      {
          for (int j = 0; j < S2.length(); j++)
          {
              int x = 0;
              while (S1.charAt(i + x) == S2.charAt(j + x))
              {
                  x++;
                  if (((i + x) >= S1.length()) || ((j + x) >= S2.length())) {
                    break;
                  }
              }
              if (x > Max)
              {
                  Max = x;
                  Start = i;
              }
           }
      }
      return S1.substring(Start, (Start + Max));
  }

  public static String summarizeInterval(String str) {
    String[] list = str.split(",");
    if (list == null || list.length == 0) {
      return "[]";
    } else if (list.length == 1) {
      return "[" + list[0].trim() + "]";
    } else {
      return "[" + list[0].trim() + ", ..., " + list[list.length-1].trim() + "]";
    }
  }

  public void dumpStates(final QuorumInfo info) {
    LOG.info("---- logs for region " + info + ":");
    List<String> logs = new ArrayList<String>();
    List<Integer> ports = new ArrayList<Integer>();
    for (LocalConsensusServer server : servers.values()) {
      final CommitLogManagerInterface log =
        server.getHandler().getRaftQuorumContext(info.getQuorumName()).getLogManager();
      logs.add(log.dumpLogs(-1));
      ports.add(server.getThriftServer().getPort());
    }
    String common = "";
    if (logs.size() > 1) {
      common = longestCommonSubstring(logs.get(0), logs.get(1));
      for (int i=2; i<logs.size(); i++) {
        common = longestCommonSubstring(common, logs.get(i));
      }
    }
    String newcommon = summarizeInterval(common);
    for (int i=0; i<logs.size(); i++) {
      String str = logs.get(i);
      if (common.length() > 10) {
        logs.set(i, str.replace(common, newcommon + " "));
      }
    }
    // try again
    for (int i=0; i<logs.size(); i++) {
      LOG.info("    ---- logs for " + LOCAL_HOST + ":" + ports.get(i) + " = " + logs.get(i));
    }
  }

  public boolean verifyLogs(final QuorumInfo info, int majorityCount) {
    return verifyLogs(info, majorityCount, true);
  }

  public boolean verifyLogs(final QuorumInfo info, int majorityCount, boolean verbose) {
    return verifyLogs(servers.values(), info, majorityCount, verbose);
  }


  public static boolean verifyLogs(final Collection<LocalConsensusServer> consensusServers,
      final QuorumInfo info, int majorityCount, boolean verbose) {
    if (verbose) {
      LOG.debug("Verify logs for " + info.getQuorumName());
    }

    int quorumSize = info.getPeersWithRank().size();
    List<CommitLogManagerInterface> logs = new ArrayList<>(quorumSize);
    for (LocalConsensusServer server : consensusServers) {
      RaftQuorumContext context =
        server.getHandler().getRaftQuorumContext(info.getQuorumName());

      if (context == null ||
        info.getPeersWithRank().get(RaftUtil.getHRegionServerAddress(
          new HServerAddress(RaftTestUtil.LOCAL_HOST,
            server.getThriftServer().getPort())
        )) == null) {
        if (verbose) {
          if (context == null) {
            LOG.debug("Context is null for " + server);
          } else {
            LOG.debug("Given Server is not a part of the quorum: " + server);
          }
        }
        continue;
      }

      final CommitLogManagerInterface log = context.getLogManager();
      logs.add(log);

      System.out.println(context + " ; " + context.getPaxosState() + " ; " + log.getLogState());
    }

    for (int ii = 0; ii < quorumSize - 1; ii++) {
      int numMatches = 1;
      for (int jj = ii + 1; jj < quorumSize; jj++) {
        numMatches += verifyLog(logs.get(ii), logs.get(jj));
        if (numMatches == majorityCount) {
          if (verbose) {
            LOG.debug(numMatches + " logs match for " + info.getQuorumName());
          }
          return true;
        }
      }
    }
    LOG.debug("Logs don't match for " + info.getQuorumName());
    return false;
  }

  public void assertAllServersRunning() throws Exception {
    for (LocalConsensusServer server : servers.values()) {
      assert server.getThriftServer().isRunning() == true;
    }
  }

  public QuorumInfo initializePeers() {
    Map<HServerAddress, Integer> peers = new HashMap<>();
    int rank = servers.size();
    for (LocalConsensusServer server : servers.values()) {
      int regionServerPort =
        server.getThriftServer().getPort() - HConstants.CONSENSUS_SERVER_PORT_JUMP;
      peers.put(new HServerAddress(RaftTestUtil.LOCAL_HOST, regionServerPort),
        Math.max(rank--, 0));
    }

    HTableDescriptor table = new HTableDescriptor(RaftTestUtil.class.getName());
    byte [] FAMILY = Bytes.toBytes("family");
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY).setMaxVersions(Integer.MAX_VALUE);
    table.addFamily(hcd);

    Map<String, Map<HServerAddress, Integer>> peerMap = new HashMap<>();
    peerMap.put(QuorumInfo.LOCAL_DC_KEY, peers);

    String quorumName = "dummyTable,123,deadbeef.";
    return new QuorumInfo(peerMap, quorumName);
  }

  public QuorumInfo resetPeers(QuorumInfo quorumInfo, List<int[]> logs) throws Exception {

    addQuorum(quorumInfo, logs);
    return quorumInfo;
  }

  public void setSeedIndex(long seedIndex) {
    this.seedIndex = seedIndex;
  }

  public static List<int[]> getScratchSetup(int quorumSize) {
    List<int[]> logs = new ArrayList<>(quorumSize);
    for (int i = 0; i < quorumSize; i++) {
      logs.add(i, new int[] {1});
    }
    return logs;
  }

  public static List<WALEdit> generateTransaction(int size) {
    List<WALEdit> testData = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String payloadSizeName = FileUtils.byteCountToDisplaySize(size);
      KeyValue kv = KeyValue.generateKeyValue(payloadSizeName, size);

      List<KeyValue> kvs = new ArrayList<>();
      for (int j = 0; j < 10; j++) {
        kvs.add(kv);
      }

      testData.add(new WALEdit(kvs));
    }
    return testData;
  }

  /**
   * Verifies whether the two logs are same or not
   * @param log1
   * @param log2
   * @return 1 if they are same, else 0
   */
  private static int verifyLog(final CommitLogManagerInterface log1,
                               final CommitLogManagerInterface log2) {

    EditId log1Marker = log1.getLastEditID();
    EditId log2Marker = log2.getLastEditID();

    while (log1Marker != TransactionLogManager.UNDEFINED_EDIT_ID &&
      log2Marker != TransactionLogManager.UNDEFINED_EDIT_ID) {

      // Handle the seed file case, where the index is the same but the term
      // is SEED_TERM.
      if ((log1Marker.getTerm() != HConstants.SEED_TERM &&
        log2Marker.getTerm() != HConstants.SEED_TERM &&
        !log1Marker.equals(log2Marker)) ||
        log1Marker.getIndex() != log2Marker.getIndex()) {
        return 0;
      }

      log1Marker = log1.getPreviousEditID(log1Marker);
      log2Marker = log2.getPreviousEditID(log2Marker);
    }

    if (log1Marker != TransactionLogManager.UNDEFINED_EDIT_ID ||
      log2Marker != TransactionLogManager.UNDEFINED_EDIT_ID) {
      return 0;
    }
    return 1;
  }

  public boolean simulatePacketDropForServer(final QuorumInfo quorumInfo, int rank,
                                             final InstrumentedConsensusServiceImpl.PacketDropStyle style) {
    HServerAddress server = null;

    for (HServerAddress s : quorumInfo.getPeersWithRank().keySet()) {
      if (quorumInfo.getPeersWithRank().get(s) == rank) {
        server = s;
        break;
      }
    }

    if (server == null) {
      return false;
    }

    InstrumentedConsensusServiceImpl service = (InstrumentedConsensusServiceImpl)
      (servers.get(RaftUtil.getLocalConsensusAddress(server).getHostAddressWithPort()).
        getHandler());
    service.setPacketDropStyle(style);
    return true;
  }

  public long getHiccupPacketDropCount(final QuorumInfo quorumInfo) {
    long count = 0;
    for (HServerAddress server : quorumInfo.getPeersWithRank().keySet()) {
      InstrumentedConsensusServiceImpl service = (InstrumentedConsensusServiceImpl)
              (servers.get(RaftUtil.getLocalConsensusAddress(server).getHostAddressWithPort()).getHandler());
      count += service.getHiccupPacketDropCount();
    }
    return count;
  }

  public long getPacketDropCount(final QuorumInfo quorumInfo) {
    long count = 0;
    for (HServerAddress server : quorumInfo.getPeersWithRank().keySet()) {
      InstrumentedConsensusServiceImpl service = (InstrumentedConsensusServiceImpl)
              (servers.get(RaftUtil.getLocalConsensusAddress(server).getHostAddressWithPort()).getHandler());
      count += service.getPacketDropCount();
    }
    return count;
  }

  public void setUsePeristentLog(boolean usePersistentLog) {
    this.usePersistentLog = usePersistentLog;
  }

  public RaftQuorumContext getRaftQuorumContextByAddress(QuorumInfo quorumInfo,
    String address) {
    return getServers().get(address).getHandler()
      .getRaftQuorumContext(quorumInfo.getQuorumName());
  }

  public RaftQuorumContext getRaftQuorumContextByRank(QuorumInfo quorumInfo, int rank) {
    String peerAddress = null;
    for (HServerAddress addr : quorumInfo.getPeersWithRank().keySet()) {
      if (quorumInfo.getPeersWithRank().get(addr) == rank) {
        peerAddress = RaftUtil.getLocalConsensusAddress(addr).getHostAddressWithPort();
      }
    }

    return getServers().get(peerAddress).getHandler().
      getRaftQuorumContext(quorumInfo.getQuorumName());
  }

  public LocalConsensusServer stopLocalConsensusServer(QuorumInfo quorumInfo, int rank) {
    String peerAddress = null;
    for (HServerAddress addr : quorumInfo.getPeersWithRank().keySet()) {
      if (quorumInfo.getPeersWithRank().get(addr) == rank) {
        peerAddress = RaftUtil.getLocalConsensusAddress(addr).getHostAddressWithPort();
      }
    }

    return stopLocalConsensusServer(peerAddress);
  }

  public LocalConsensusServer stopLocalConsensusServer(String peerAddress) {
    LocalConsensusServer server = getServers().remove(peerAddress);
    server.stopService();

    return server;
  }

  public void printStatusOfQuorum(QuorumInfo quorumInfo) {
    System.out.println(" ======= Status Update =========");
    for (LocalConsensusServer server : getServers().values()) {
      RaftQuorumContext context =
        server.getHandler().getRaftQuorumContext(quorumInfo.getQuorumName());
      System.out.println(context + " ; " + context.getPaxosState() + " ; " + context.getLogState());
    }
    System.out.println(" ================");
  }

  public List<RaftQuorumContext> getQuorumContexts(
    final QuorumInfo quorumInfo) {
      Set<HServerAddress> replias = quorumInfo.getPeersWithRank().keySet();
      List<RaftQuorumContext> contexts = new ArrayList<>(replias.size());

      for (HServerAddress address : replias) {
        String consensusServerAddress =
                RaftUtil.getLocalConsensusAddress(address).
                        getHostAddressWithPort();
        if (getServers().containsKey(consensusServerAddress)) {
          contexts.add(getRaftQuorumContextByAddress(quorumInfo,
                  consensusServerAddress));
        }
      }
      return contexts;
    }

  public RaftQuorumContext getLeaderQuorumContext(QuorumInfo quorumInfo) {
    RaftQuorumContext leader = null;
    for (RaftQuorumContext context : getQuorumContexts(quorumInfo)) {
      if (context.isLeader()) {
        leader = context;
      }
    }
    return leader;
  }

  public void waitForLeader(final QuorumInfo quorumInfo)
          throws InterruptedException {
    while (getLeaderQuorumContext(quorumInfo) == null) {
      Thread.sleep(500);
    }
  }
}
