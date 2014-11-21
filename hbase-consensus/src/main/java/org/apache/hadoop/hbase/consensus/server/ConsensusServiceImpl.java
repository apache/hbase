package org.apache.hadoop.hbase.consensus.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.consensus.client.FetchTask;
import org.apache.hadoop.hbase.consensus.log.LogFileInfo;
import org.apache.hadoop.hbase.consensus.log.RemoteLogFetcher;
import org.apache.hadoop.hbase.consensus.protocol.ConsensusHost;
import org.apache.hadoop.hbase.consensus.quorum.QuorumAgent;
import org.apache.hadoop.hbase.consensus.quorum.QuorumInfo;
import org.apache.hadoop.hbase.consensus.quorum.QuorumMembershipChangeRequest;
import org.apache.hadoop.hbase.consensus.quorum.RaftQuorumContext;
import org.apache.hadoop.hbase.consensus.raft.events.AppendRequestEvent;
import org.apache.hadoop.hbase.consensus.raft.events.QuorumMembershipChangeEvent;
import org.apache.hadoop.hbase.consensus.raft.events.VoteRequestEvent;
import org.apache.hadoop.hbase.consensus.rpc.AppendRequest;
import org.apache.hadoop.hbase.consensus.rpc.AppendResponse;
import org.apache.hadoop.hbase.consensus.rpc.LogState;
import org.apache.hadoop.hbase.consensus.rpc.PeerStatus;
import org.apache.hadoop.hbase.consensus.rpc.VoteRequest;
import org.apache.hadoop.hbase.consensus.rpc.VoteResponse;
import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;
import org.apache.hadoop.hbase.regionserver.DataStoreState;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConsensusServiceImpl implements ConsensusService {
  private static final Logger LOG = LoggerFactory.getLogger(
    ConsensusServiceImpl.class);

  /** TODO: replace this with HRegionServer.getRegions() API once we
   *  integrate HBase with the protocol
   */
  private ConcurrentHashMap<String, RaftQuorumContext> quorumContextMap = new ConcurrentHashMap<>();

  protected ConsensusServiceImpl() {}

  public static ConsensusService createConsensusServiceImpl() {
    return new ConsensusServiceImpl();
  }

  public static ConsensusService createTestConsensusServiceImpl() {
    return new InstrumentedConsensusServiceImpl();
  }

  @Override
  public ListenableFuture<AppendResponse> appendEntries(AppendRequest appendRequest) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Receiving " + appendRequest);
    }
    boolean success = false;

    appendRequest.createAppendResponse();

    RaftQuorumContext c = getRaftQuorumContext(appendRequest.getRegionId());
    if (c != null) {
      // Add an event for the correct state machine
      success = c.offerEvent(new AppendRequestEvent(appendRequest));
    }

    if (!success) {
      appendRequest.setError(new ThriftHBaseException(new Exception(
          "Unable to complete AppendEntries: " + appendRequest)));
    }
    return appendRequest.getResponse();
  }

  @Override
  public ListenableFuture<VoteResponse> requestVote(VoteRequest voteRequest) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Receiving " + voteRequest);
    }

    voteRequest.createVoteResponse();

    RaftQuorumContext c = getRaftQuorumContext(voteRequest.getRegionId());

    boolean success = false;
    if (c != null) {
      // Add an event for the correct state machine
      success = c.offerEvent(new VoteRequestEvent(voteRequest));
    } else {
      LOG.error("There is no such region " + voteRequest.getRegionId() +
        " in the current quorum server");
    }

    if (!success) {
      voteRequest.setError(new ThriftHBaseException(
          new Exception("Unable to complete Vote Request: " + voteRequest)));
    }

    return voteRequest.getResponse();
  }

  @Override
  public ListenableFuture<Long> replicateCommit(String regionId, List<WALEdit> txns) throws ThriftHBaseException {
    RaftQuorumContext c = getRaftQuorumContext(regionId);
    if (c != null) {
      QuorumAgent agent = c.getQuorumAgentInstance();
      try {
        return agent.asyncAppend(txns);
      } catch (IOException e) {
        throw new ThriftHBaseException(e);
      }
    } else {
      Exception e = new Exception("Unable to find the " +
              "region information for " + regionId);
      LOG.error(e.getMessage());
      return Futures.immediateFailedFuture(new ThriftHBaseException(e));
    }
  }

  @Override
  public ListenableFuture<Boolean> changeQuorum(String regionId, ByteBuffer config) {

    RaftQuorumContext c = getRaftQuorumContext(regionId);

    if (c != null) {
      List<QuorumInfo> configs = QuorumInfo.deserializeFromByteBuffer(config);
      if (configs == null || configs.size() != 1) {
        return Futures.immediateCheckedFuture(false);
      }
      // Make sure the config request change is really needed.
      if (configs.get(0).equals(c.getQuorumInfo())) {
        return Futures.immediateCheckedFuture(true);
      }

      final QuorumMembershipChangeRequest request =
        new QuorumMembershipChangeRequest(configs.get(0));
      // Add an event for the correct state machine
      boolean success = c.offerEvent(new QuorumMembershipChangeEvent(request));
      if (!success) {
        return Futures.immediateCheckedFuture(false);
      }
      return request;
    } else {
      LOG.debug("Ignored " + config);
    }

    return Futures.immediateCheckedFuture(false);
  }

  @Override
  public ListenableFuture<String> getLeader(String regionId) {
    RaftQuorumContext c = getRaftQuorumContext(regionId);
    String leader = "";
    if (c != null) {
      ConsensusHost host = c.getLeader();
      if (host != null) {
        leader = host.getHostId();
      }
    }
    if (leader.isEmpty() && LOG.isTraceEnabled()) {
      LOG.trace("Leader is unknown for " + regionId);
    }
    return Futures.immediateFuture(leader);
  }

  @Override
  public ListenableFuture<PeerStatus> getPeerStatus(String quorum) {
    RaftQuorumContext c = getRaftQuorumContext(quorum);

    if (c != null) {
     return Futures.immediateFuture(c.getStatus());
    } else {
      return Futures.immediateFuture(new PeerStatus(quorum, -1, HConstants.UNDEFINED_TERM_INDEX,
        PeerStatus.RAFT_STATE.INVALID, new LogState("Wrong Quorum"), "Error",
        new DataStoreState("Wrong Quorum")));
    }
  }

  @Override
  public ListenableFuture<List<LogFileInfo>> getCommittedLogStatus(String quorumName, long minIndex) {
    RaftQuorumContext c = getRaftQuorumContext(quorumName);
    List<LogFileInfo> info = null;

    if (c != null) {
      info = c.getCommittedLogStatus(minIndex);
    } else {
      info = new ArrayList<>();
    }

    return Futures.immediateFuture(info);
  }

  @Override
  public void stopService() {
    LOG.info("Clear all the RaftQuorum from the quorumContextMap !");
    // stop all the context before clearing
    for (RaftQuorumContext context : quorumContextMap.values()) {
      context.stop(true);
    }

    this.quorumContextMap.clear();
  }

  @Override
  public ImmutableMap<String, RaftQuorumContext> getQuorumContextMapSnapshot() {
    return ImmutableMap.copyOf(this.quorumContextMap);
  }

  @Override
  public RaftQuorumContext addRaftQuorumContext(final RaftQuorumContext c) {
    LOG.info("Adding RaftQuorumContext " + c + " to the quorumContextMap");
    return this.quorumContextMap.put(c.getQuorumName(), c);
  }

  @Override
  public boolean removeRaftQuorumContext(String regionName) {
    LOG.info("Removing quorum for the region " + regionName + " from the quorumContextMap");
    return this.quorumContextMap.remove(regionName) != null;
  }

  @Override
  public RaftQuorumContext getRaftQuorumContext(String regionId) {
    return this.quorumContextMap.get(regionId);
  }

  @Override public void close() throws Exception {
    // Do nothing. Added the AutoCloseable only for the client cleanup
  }

  @Override
  public ListenableFuture<Void> fetchLogs(List<FetchTask> tasks, String regionId) {
    return new RemoteLogFetcher().executeTasks(tasks, regionId);
  }

  @Override public ListenableFuture<List<PeerStatus>> getAllPeerStatuses() {
    List<PeerStatus> statuses = new ArrayList<>();
    Map<String, RaftQuorumContext> quorumMap = getQuorumContextMapSnapshot();

    for (RaftQuorumContext quorum : quorumMap.values()) {
      statuses.add(quorum.getStatus());
    }

    return Futures.immediateFuture(statuses);
  }
}
