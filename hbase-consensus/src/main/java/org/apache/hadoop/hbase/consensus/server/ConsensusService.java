package org.apache.hadoop.hbase.consensus.server;

import com.facebook.swift.service.ThriftException;
import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.consensus.client.FetchTask;
import org.apache.hadoop.hbase.consensus.log.LogFileInfo;
import org.apache.hadoop.hbase.consensus.quorum.RaftQuorumContext;
import org.apache.hadoop.hbase.consensus.rpc.AppendRequest;
import org.apache.hadoop.hbase.consensus.rpc.AppendResponse;
import org.apache.hadoop.hbase.consensus.rpc.PeerStatus;
import org.apache.hadoop.hbase.consensus.rpc.VoteRequest;
import org.apache.hadoop.hbase.consensus.rpc.VoteResponse;
import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.nio.ByteBuffer;
import java.util.List;

@ThriftService
public interface ConsensusService extends AutoCloseable {

  @ThriftMethod(value = "appendEntries", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  ListenableFuture<AppendResponse> appendEntries(AppendRequest appendRequest);

  @ThriftMethod(value = "requestVote", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  ListenableFuture<VoteResponse> requestVote(VoteRequest request);

  @ThriftMethod
  ListenableFuture<PeerStatus> getPeerStatus(String quorumName);

  @ThriftMethod(value = "replicateCommit", exception = {
        @ThriftException(type = ThriftHBaseException.class, id = 1) })
  ListenableFuture<Long> replicateCommit(String regionId, List<WALEdit> txns)
    throws ThriftHBaseException;

  @ThriftMethod
  ListenableFuture<Boolean> changeQuorum(String regionId, final ByteBuffer config);

  @ThriftMethod
  ListenableFuture<String> getLeader(String regionId);

  @ThriftMethod
  ListenableFuture<List<LogFileInfo>> getCommittedLogStatus(String quorumName,
                                                            long minIndex);

  @ThriftMethod
  // TODO @gauravm
  // Remove?
  ListenableFuture<Void> fetchLogs(List<FetchTask> tasks, String regionId);

  @ThriftMethod
  ListenableFuture<List<PeerStatus>> getAllPeerStatuses();

  ImmutableMap<String, RaftQuorumContext> getQuorumContextMapSnapshot();

  RaftQuorumContext getRaftQuorumContext(String regionId);

  RaftQuorumContext addRaftQuorumContext(final RaftQuorumContext c);

  // TODO @gauravm
  // Remove?
  boolean removeRaftQuorumContext(final String regionName);

  void stopService();
}
