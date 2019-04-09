/**
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
import java.util.List;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SyncReplicationReplayWALState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SyncReplicationReplayWALStateData;

/**
 * The procedure for replaying a set of remote wals. It will get an available region server and
 * schedule a {@link SyncReplicationReplayWALRemoteProcedure} to actually send the request to region
 * server.
 */
@InterfaceAudience.Private
public class SyncReplicationReplayWALProcedure
    extends AbstractPeerNoLockProcedure<SyncReplicationReplayWALState> {

  private static final Logger LOG =
    LoggerFactory.getLogger(SyncReplicationReplayWALProcedure.class);

  private ServerName worker = null;

  private List<String> wals;

  public SyncReplicationReplayWALProcedure() {
  }

  public SyncReplicationReplayWALProcedure(String peerId, List<String> wals) {
    this.peerId = peerId;
    this.wals = wals;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, SyncReplicationReplayWALState state)
      throws ProcedureSuspendedException {
    SyncReplicationReplayWALManager syncReplicationReplayWALManager =
      env.getMasterServices().getSyncReplicationReplayWALManager();
    switch (state) {
      case ASSIGN_WORKER:
        worker = syncReplicationReplayWALManager.acquirePeerWorker(peerId, this);
        setNextState(SyncReplicationReplayWALState.DISPATCH_WALS_TO_WORKER);
        return Flow.HAS_MORE_STATE;
      case DISPATCH_WALS_TO_WORKER:
        addChildProcedure(new SyncReplicationReplayWALRemoteProcedure(peerId, wals, worker));
        setNextState(SyncReplicationReplayWALState.RELEASE_WORKER);
        return Flow.HAS_MORE_STATE;
      case RELEASE_WORKER:
        boolean finished = false;
        try {
          finished = syncReplicationReplayWALManager.isReplayWALFinished(wals.get(0));
        } catch (IOException e) {
          long backoff = ProcedureUtil.getBackoffTimeMs(attempts);
          LOG.warn("Failed to check whether replay wals {} finished for peer id={}" +
            ", sleep {} secs and retry", wals, peerId, backoff / 1000, e);
          throw suspend(backoff);
        }
        syncReplicationReplayWALManager.releasePeerWorker(peerId, worker,
          env.getProcedureScheduler());
        if (!finished) {
          LOG.warn("Failed to replay wals {} for peer id={}, retry", wals, peerId);
          setNextState(SyncReplicationReplayWALState.ASSIGN_WORKER);
          return Flow.HAS_MORE_STATE;
        }
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, SyncReplicationReplayWALState state)
      throws IOException, InterruptedException {
    if (state == getInitialState()) {
      return;
    }
    throw new UnsupportedOperationException();
  }

  @Override
  protected SyncReplicationReplayWALState getState(int state) {
    return SyncReplicationReplayWALState.forNumber(state);
  }

  @Override
  protected int getStateId(SyncReplicationReplayWALState state) {
    return state.getNumber();
  }

  @Override
  protected SyncReplicationReplayWALState getInitialState() {
    return SyncReplicationReplayWALState.ASSIGN_WORKER;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    SyncReplicationReplayWALStateData.Builder builder =
      SyncReplicationReplayWALStateData.newBuilder().setPeerId(peerId).addAllWal(wals);
    if (worker != null) {
      builder.setWorker(ProtobufUtil.toServerName(worker));
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    SyncReplicationReplayWALStateData data =
      serializer.deserialize(SyncReplicationReplayWALStateData.class);
    peerId = data.getPeerId();
    wals = data.getWalList();
    if (data.hasWorker()) {
      worker = ProtobufUtil.toServerName(data.getWorker());
    }
  }

  @Override
  public PeerOperationType getPeerOperationType() {
    return PeerOperationType.SYNC_REPLICATION_REPLAY_WAL;
  }

  @Override
  protected void afterReplay(MasterProcedureEnv env) {
    // If the procedure is not finished and the worker is not null, we should add it to the used
    // worker set, to prevent the worker being used by others.
    if (worker != null && !isFinished()) {
      env.getMasterServices().getSyncReplicationReplayWALManager().addUsedPeerWorker(peerId,
        worker);
    }
  }
}
