package org.apache.hadoop.hbase.consensus.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.client.NoLeaderForRegionException;
import org.apache.hadoop.hbase.consensus.log.LogFileInfo;
import org.apache.hadoop.hbase.consensus.quorum.QuorumInfo;
import org.apache.hadoop.hbase.consensus.rpc.LogState;
import org.apache.hadoop.hbase.consensus.rpc.PeerStatus;
import org.apache.hadoop.hbase.consensus.util.RaftUtil;
import org.apache.hadoop.hbase.regionserver.DataStoreState;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * QuorumClient is the user facing agent for replicating commits (WALEdits) on
 * a per-quorum level. Once set up, the replicateCommits(List<WALEdit>) method
 * will transparently handle contacting the leader, sending the edits to be
 * replicated, handling the retries / errors, etc.
 */
public class QuorumClient {
  private final static Logger LOG = LoggerFactory.getLogger(QuorumClient.class);
  private Map<String, QuorumThriftClientAgent> agentMap;
  private List<QuorumThriftClientAgent> agents;
  private final String regionId;
  private final Configuration conf;
  private final long maxOperationLatencyInMillis;
  private final long sleepIntervalInMillis;
  private QuorumThriftClientAgent leader = null;
  private ExecutorService pool;

  /**
   * Creates the QuorumClient.
   * @param regionInfo The QuorumInfo which describes the quorum.
   * @param conf The configuration object.
   * @param pool The thread pool to be used.
   * @throws IOException
   */
  public QuorumClient(QuorumInfo regionInfo, Configuration conf,
    ExecutorService pool) throws IOException {
    this.regionId = regionInfo.getQuorumName();
    this.conf = conf;
    this.maxOperationLatencyInMillis = conf.getLong(
            HConstants.QUORUM_CLIENT_COMMIT_DEADLINE_KEY,
            HConstants.QUORUM_CLIENT_COMMIT_DEADLINE_DEFAULT);
    this.sleepIntervalInMillis = conf.getLong(
      HConstants.QUORUM_CLIENT_RETRY_SLEEP_INTERVAL,
      HConstants.QUORUM_CLIENT_RETRY_SLEEP_INTERVAL_DEFAULT
    );
    updateConfig(regionInfo);
    this.pool = pool;
  }

  protected QuorumClient(String regionId, final Configuration conf,
                         ExecutorService pool) throws IOException {
    this(RaftUtil.createDummyRegionInfo(regionId).getQuorumInfo(), conf, pool);
  }

  public synchronized long replicateCommits(List<WALEdit> txns)
    throws IOException {
    int numRetries = 0;
    long endTime = System.currentTimeMillis() + maxOperationLatencyInMillis;
    while (System.currentTimeMillis() <= endTime) {
      try {
        if (agents == null || agents.isEmpty()) {
          LOG.error("No quorum agent is running");
          return HConstants.UNDEFINED_TERM_INDEX;
        }
        if (leader == null) {
          waitForLeader(endTime);
        }

        if (leader == null) {
          ++numRetries;
          LOG.error(String.format("%s cannot find leader. Retry : %d",
            regionId, numRetries));
          Threads.sleep(this.sleepIntervalInMillis);
        } else {
          return leader.replicateCommit(this.regionId, txns);
        }
      } catch (Exception e) {
        ++numRetries;
        LOG.error(String.format("%s Unable to replicate the commit. Retry number" +
          " %d", regionId, numRetries), e);
        Threads.sleep(this.sleepIntervalInMillis);
        resetLeader();
      }
    }
    throw new IOException(String.format("%s Unable to replicate the commit." +
        " Retried %s times",
      regionId, numRetries));
  }

  /**
   * Request the quorum to change to a new config.
   * @param config the new config
   */
  public synchronized boolean changeQuorum(final QuorumInfo config)
    throws IOException {

    int numRetries = 0;
    long endTime = System.currentTimeMillis() + this.maxOperationLatencyInMillis;
    while (System.currentTimeMillis() <= endTime) {
      try {
        if (agents == null || agents.isEmpty()) {
          LOG.error(String.format("%s : No quorum agent is running", regionId));
          return false;
        }
        if (leader == null) {
          waitForLeader(endTime);
        }

        if (leader != null) {
          return leader.changeQuorum(config.getQuorumName(),
            QuorumInfo.serializeToBuffer(Arrays.asList(config)));
        } else {
          ++numRetries;
          LOG.error(String.format("%s cannot find leader. Retry number: %d",
            regionId, numRetries));
          Threads.sleep(this.sleepIntervalInMillis);
        }
      } catch (Exception e) {
        ++numRetries;
        LOG.error(String.format("%s Unable to send membership change request." +
          "Retry number %d.", regionId, numRetries), e);
        Threads.sleep(this.sleepIntervalInMillis);
        resetLeader();
      }
    }
    throw new IOException(String.format(
      "%s Unable to send membership" + " change request. Retry number %d.",
      regionId, numRetries));
  }

  private void resetLeader() {
    this.leader = null;
  }

  private void waitForLeader(long endTime) throws NoLeaderForRegionException {
    ExecutorCompletionService<String> service =
      new ExecutorCompletionService<>(pool);
    while (System.currentTimeMillis() < endTime) {
      for (final QuorumThriftClientAgent agent : agents) {
        service.submit(new Callable<String>() {
          @Override
          public String call() throws Exception {
            try {
              return agent.getLeader(regionId);
            } catch (Exception e) {
              LOG.error(String.format("%s: Cannot talk to quorum server: %s",
                regionId, agent.getServerAddress()), e);
              throw e;
            }
          }
        });
      }
      // as soon as the first future is finished we are done
      for (int index = 0; index < agents.size(); index++) {
        try {
          final Future<String> future = service.take();
          String leaderAddrString = future.get();
          if (leaderAddrString != null && !leaderAddrString.isEmpty()) {
            HServerAddress leaderAddr = new HServerAddress(leaderAddrString);
            leader = agentMap.get(leaderAddr.getHostNameWithPort());
            if (leader != null) {
              LOG.debug(String.format(
                "The current leader for the region %s is %s.",
                regionId, leader.getServerAddress()));
              return;
            }
          }
        } catch (ExecutionException e) {
          LOG.error(String.format("%s Error appeared:", regionId), e);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOG.error(String.format("%s Error appeared:", regionId), e);
        }
      }
    }
    throw new NoLeaderForRegionException(
      String.format("No leader found for region %s", regionId));
  }

  public synchronized PeerStatus[] getPeerStatus() {
    PeerStatus[] statuses = new PeerStatus[this.agents.size()];
    for (int i = 0; i < agents.size(); i++) {
      try {
        statuses[i] = agents.get(i).getPeerStatus(regionId);
      } catch (Throwable e) {
        statuses[i] = new PeerStatus(regionId, -1, HConstants.UNDEFINED_TERM_INDEX,
          PeerStatus.RAFT_STATE.INVALID, new LogState(e.toString()),
          "ERROR", new DataStoreState("ERROR"));
      }
      statuses[i].setPeerAddress(agents.get(i).getServerAddress());
    }

    Arrays.sort(statuses);
    return statuses;
  }

  public synchronized List<Pair<String, List<LogFileInfo>>> getPeerCommittedLogStatus(
      HServerAddress localServerAddr,
      String quorumName,
      long minIndex) throws Exception {
    List<Pair<String, List<LogFileInfo>>> result = new ArrayList<>();
    if (agents == null || agents.isEmpty()) {
      return result;
    }
    String localConsensusAddr =
        RaftUtil.getLocalConsensusAddress(localServerAddr).getHostNameWithPort();
    for (QuorumThriftClientAgent agent : agents) {
      if (!agent.getServerAddress().equals(localConsensusAddr)) {
        List<LogFileInfo> logFiles = agent.getCommittedLogStatus(quorumName, minIndex);
        if (logFiles != null) {
          result.add(new Pair<>(agent.getServerAddress(), logFiles));
        }
      }
    }

    return result;
  }

  public synchronized void close() {
    for (QuorumThriftClientAgent agent : agents) {
      agent.close();
    }
  }

  public synchronized QuorumThriftClientAgent getLeader() {
    return leader;
  }

  private void updateConfig(final QuorumInfo config) {
    // Close the current agents
    if (agents != null) {
      close();
    }

    int connectionTimeout =
      conf.getInt("hbase.regionserver.quorum.client.connection.timeout", 10000);
    int readTimeout =
      conf.getInt("hbase.regionserver.quorum.client.read.timeout", 10000);
    int writeTimeout =
      conf.getInt("hbase.regionserver.quorum.client.write.timeout", 10000);
    int retryCount =
      conf.getInt("hbase.regionserver.quorum.client.retry.cnt", 1);
    this.agents = new ArrayList<>(config.getPeersWithRank().size());
    this.agentMap = new HashMap<>();
    for (HServerAddress address : config.getPeersWithRank().keySet()) {
      try {
        QuorumThriftClientAgent agent = new QuorumThriftClientAgent(
            RaftUtil.getLocalConsensusAddress(address).getHostAddressWithPort(),
            connectionTimeout, readTimeout, writeTimeout, retryCount);
        this.agents.add(agent);
        agentMap.put(agent.getServerAddress(), agent);
      } catch (IOException e) {
        LOG.error("Unable to initialize quorum thrift agent for " + address);
      }
    }
  }

  /**
   * Updated the ThriftClientAgents based on the given quorum info.
   * @param config
   */
  public synchronized void refreshConfig(final QuorumInfo config) {
    updateConfig(config);
  }
}
