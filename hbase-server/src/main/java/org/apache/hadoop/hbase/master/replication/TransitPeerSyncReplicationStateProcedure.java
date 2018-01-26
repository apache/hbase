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

import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerModificationState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.TransitPeerSyncReplicationStateStateData;

/**
 * The procedure for transit current cluster state for a synchronous replication peer.
 */
@InterfaceAudience.Private
public class TransitPeerSyncReplicationStateProcedure extends ModifyPeerProcedure {

  private static final Logger LOG =
    LoggerFactory.getLogger(TransitPeerSyncReplicationStateProcedure.class);

  private SyncReplicationState state;

  public TransitPeerSyncReplicationStateProcedure() {
  }

  public TransitPeerSyncReplicationStateProcedure(String peerId, SyncReplicationState state) {
    super(peerId);
    this.state = state;
  }

  @Override
  public PeerOperationType getPeerOperationType() {
    return PeerOperationType.TRANSIT_SYNC_REPLICATION_STATE;
  }

  @Override
  protected void prePeerModification(MasterProcedureEnv env)
      throws IOException, ReplicationException {
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preTransitReplicationPeerSyncReplicationState(peerId, state);
    }
    env.getReplicationPeerManager().preTransitPeerSyncReplicationState(peerId, state);
  }

  @Override
  protected void updatePeerStorage(MasterProcedureEnv env) throws ReplicationException {
    env.getReplicationPeerManager().transitPeerSyncReplicationState(peerId, state);
  }

  @Override
  protected void postPeerModification(MasterProcedureEnv env)
    throws IOException, ReplicationException {
    LOG.info("Successfully transit current cluster state to {} in synchronous replication peer {}",
      state, peerId);
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      env.getMasterCoprocessorHost().postTransitReplicationPeerSyncReplicationState(peerId, state);
    }
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    serializer.serialize(TransitPeerSyncReplicationStateStateData.newBuilder()
        .setSyncReplicationState(ReplicationPeerConfigUtil.toSyncReplicationState(state)).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    TransitPeerSyncReplicationStateStateData data =
        serializer.deserialize(TransitPeerSyncReplicationStateStateData.class);
    state = ReplicationPeerConfigUtil.toSyncReplicationState(data.getSyncReplicationState());
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, PeerModificationState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    switch (state) {
      case PRE_PEER_MODIFICATION:
        try {
          prePeerModification(env);
        } catch (IOException e) {
          LOG.warn("{} failed to call pre CP hook or the pre check is failed for peer {}, " +
            "mark the procedure as failure and give up", getClass().getName(), peerId, e);
          setFailure("master-" + getPeerOperationType().name().toLowerCase() + "-peer", e);
          releaseLatch();
          return Flow.NO_MORE_STATE;
        } catch (ReplicationException e) {
          LOG.warn("{} failed to call prePeerModification for peer {}, retry", getClass().getName(),
            peerId, e);
          throw new ProcedureYieldException();
        }
        setNextState(PeerModificationState.UPDATE_PEER_STORAGE);
        return Flow.HAS_MORE_STATE;
      case UPDATE_PEER_STORAGE:
        try {
          updatePeerStorage(env);
        } catch (ReplicationException e) {
          LOG.warn("{} update peer storage for peer {} failed, retry", getClass().getName(), peerId,
            e);
          throw new ProcedureYieldException();
        }
        setNextState(PeerModificationState.REFRESH_PEER_ON_RS);
        return Flow.HAS_MORE_STATE;
      case REFRESH_PEER_ON_RS:
        // TODO: Need add child procedure for every RegionServer
        setNextState(PeerModificationState.POST_PEER_MODIFICATION);
        return Flow.HAS_MORE_STATE;
      case POST_PEER_MODIFICATION:
        try {
          postPeerModification(env);
        } catch (ReplicationException e) {
          LOG.warn("{} failed to call postPeerModification for peer {}, retry",
            getClass().getName(), peerId, e);
          throw new ProcedureYieldException();
        } catch (IOException e) {
          LOG.warn("{} failed to call post CP hook for peer {}, " +
            "ignore since the procedure has already done", getClass().getName(), peerId, e);
        }
        releaseLatch();
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  private void releaseLatch() {
    ProcedurePrepareLatch.releaseLatch(latch, this);
  }
}
