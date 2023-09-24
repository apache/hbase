/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.replication;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ServerProcedureInterface;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationGroupOffset;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSyncUp;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AssignReplicationQueuesState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AssignReplicationQueuesStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

@InterfaceAudience.Private
public class AssignReplicationQueuesProcedure
  extends StateMachineProcedure<MasterProcedureEnv, AssignReplicationQueuesState>
  implements ServerProcedureInterface {

  private static final Logger LOG = LoggerFactory.getLogger(AssignReplicationQueuesProcedure.class);

  private ServerName crashedServer;

  private RetryCounter retryCounter;

  public AssignReplicationQueuesProcedure() {
  }

  public AssignReplicationQueuesProcedure(ServerName crashedServer) {
    this.crashedServer = crashedServer;
  }

  @Override
  public ServerName getServerName() {
    return crashedServer;
  }

  @Override
  public boolean hasMetaTableRegion() {
    return false;
  }

  @Override
  public ServerOperationType getServerOperationType() {
    return ServerOperationType.CLAIM_REPLICATION_QUEUES;
  }

  private void addMissingQueues(MasterProcedureEnv env) throws ReplicationException {
    ReplicationQueueStorage storage = env.getReplicationPeerManager().getQueueStorage();

    Set<String> existingQueuePeerIds = new HashSet<>();
    List<ReplicationQueueId> queueIds = storage.listAllQueueIds(crashedServer);
    for (Iterator<ReplicationQueueId> iter = queueIds.iterator(); iter.hasNext();) {
      ReplicationQueueId queueId = iter.next();
      if (!queueId.isRecovered()) {
        existingQueuePeerIds.add(queueId.getPeerId());
      }
    }
    List<ReplicationPeerDescription> peers = env.getReplicationPeerManager().listPeers(null);
    for (ReplicationPeerDescription peer : peers) {
      if (!existingQueuePeerIds.contains(peer.getPeerId())) {
        ReplicationQueueId queueId = new ReplicationQueueId(crashedServer, peer.getPeerId());
        LOG.debug("Add replication queue {} for claiming", queueId);
        env.getReplicationPeerManager().getQueueStorage().setOffset(queueId,
          crashedServer.toString(), ReplicationGroupOffset.BEGIN, Collections.emptyMap());
      }
    }
  }

  private Flow claimQueues(MasterProcedureEnv env) throws ReplicationException, IOException {
    Set<String> existingPeerIds = env.getReplicationPeerManager().listPeers(null).stream()
      .map(ReplicationPeerDescription::getPeerId).collect(Collectors.toSet());
    ReplicationQueueStorage storage = env.getReplicationPeerManager().getQueueStorage();
    // filter out replication queue for deleted peers
    List<ReplicationQueueId> queueIds = storage.listAllQueueIds(crashedServer).stream()
      .filter(q -> existingPeerIds.contains(q.getPeerId())).collect(Collectors.toList());
    if (queueIds.isEmpty()) {
      LOG.debug("Finish claiming replication queues for {}", crashedServer);
      // we are done
      return Flow.NO_MORE_STATE;
    }
    LOG.debug("There are {} replication queues need to be claimed for {}", queueIds.size(),
      crashedServer);
    List<ServerName> targetServers =
      env.getMasterServices().getServerManager().getOnlineServersList();
    if (targetServers.isEmpty()) {
      throw new ReplicationException("no region server available");
    }
    Collections.shuffle(targetServers);
    for (int i = 0, n = Math.min(queueIds.size(), targetServers.size()); i < n; i++) {
      addChildProcedure(
        new ClaimReplicationQueueRemoteProcedure(queueIds.get(i), targetServers.get(i)));
    }
    retryCounter = null;
    return Flow.HAS_MORE_STATE;
  }

  // check whether ReplicationSyncUp has already done the work for us, if so, we should skip
  // claiming the replication queues and deleting them instead.
  private boolean shouldSkip(MasterProcedureEnv env) throws IOException {
    MasterFileSystem mfs = env.getMasterFileSystem();
    Path syncUpDir = new Path(mfs.getRootDir(), ReplicationSyncUp.INFO_DIR);
    return mfs.getFileSystem().exists(new Path(syncUpDir, crashedServer.getServerName()));
  }

  private void removeQueues(MasterProcedureEnv env) throws ReplicationException, IOException {
    ReplicationQueueStorage storage = env.getReplicationPeerManager().getQueueStorage();
    for (ReplicationQueueId queueId : storage.listAllQueueIds(crashedServer)) {
      storage.removeQueue(queueId);
    }
    MasterFileSystem mfs = env.getMasterFileSystem();
    Path syncUpDir = new Path(mfs.getRootDir(), ReplicationSyncUp.INFO_DIR);
    // remove the region server record file
    mfs.getFileSystem().delete(new Path(syncUpDir, crashedServer.getServerName()), false);
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, AssignReplicationQueuesState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    try {
      switch (state) {
        case ASSIGN_REPLICATION_QUEUES_ADD_MISSING_QUEUES:
          if (shouldSkip(env)) {
            setNextState(AssignReplicationQueuesState.ASSIGN_REPLICATION_QUEUES_REMOVE_QUEUES);
            return Flow.HAS_MORE_STATE;
          } else {
            addMissingQueues(env);
            retryCounter = null;
            setNextState(AssignReplicationQueuesState.ASSIGN_REPLICATION_QUEUES_CLAIM);
            return Flow.HAS_MORE_STATE;
          }
        case ASSIGN_REPLICATION_QUEUES_CLAIM:
          if (shouldSkip(env)) {
            retryCounter = null;
            setNextState(AssignReplicationQueuesState.ASSIGN_REPLICATION_QUEUES_REMOVE_QUEUES);
            return Flow.HAS_MORE_STATE;
          } else {
            return claimQueues(env);
          }
        case ASSIGN_REPLICATION_QUEUES_REMOVE_QUEUES:
          removeQueues(env);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (Exception e) {
      if (retryCounter == null) {
        retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
      }
      long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
      LOG.warn("Failed to claim replication queues for {}, suspend {} secs", crashedServer,
        backoff / 1000, e);
      setTimeout(Math.toIntExact(backoff));
      setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
      skipPersistence();
      throw new ProcedureSuspendedException();
    }
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, AssignReplicationQueuesState state)
    throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected AssignReplicationQueuesState getState(int stateId) {
    return AssignReplicationQueuesState.forNumber(stateId);
  }

  @Override
  protected int getStateId(AssignReplicationQueuesState state) {
    return state.getNumber();
  }

  @Override
  protected AssignReplicationQueuesState getInitialState() {
    return AssignReplicationQueuesState.ASSIGN_REPLICATION_QUEUES_ADD_MISSING_QUEUES;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    serializer.serialize(AssignReplicationQueuesStateData.newBuilder()
      .setCrashedServer(ProtobufUtil.toServerName(crashedServer)).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    AssignReplicationQueuesStateData proto =
      serializer.deserialize(AssignReplicationQueuesStateData.class);
    crashedServer = ProtobufUtil.toServerName(proto.getCrashedServer());
  }

}
