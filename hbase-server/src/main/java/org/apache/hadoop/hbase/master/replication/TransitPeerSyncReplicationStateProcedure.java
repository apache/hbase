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
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerSyncReplicationStateTransitionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.TransitPeerSyncReplicationStateStateData;

/**
 * The procedure for transit current sync replication state for a synchronous replication peer.
 */
@InterfaceAudience.Private
public class TransitPeerSyncReplicationStateProcedure
    extends AbstractPeerProcedure<PeerSyncReplicationStateTransitionState> {

  private static final Logger LOG =
      LoggerFactory.getLogger(TransitPeerSyncReplicationStateProcedure.class);

  private SyncReplicationState fromState;

  private SyncReplicationState toState;

  private boolean enabled;

  public TransitPeerSyncReplicationStateProcedure() {
  }

  public TransitPeerSyncReplicationStateProcedure(String peerId, SyncReplicationState state) {
    super(peerId);
    this.toState = state;
  }

  @Override
  public PeerOperationType getPeerOperationType() {
    return PeerOperationType.TRANSIT_SYNC_REPLICATION_STATE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    TransitPeerSyncReplicationStateStateData.Builder builder =
        TransitPeerSyncReplicationStateStateData.newBuilder()
          .setToState(ReplicationPeerConfigUtil.toSyncReplicationState(toState));
    if (fromState != null) {
      builder.setFromState(ReplicationPeerConfigUtil.toSyncReplicationState(fromState));
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    TransitPeerSyncReplicationStateStateData data =
        serializer.deserialize(TransitPeerSyncReplicationStateStateData.class);
    toState = ReplicationPeerConfigUtil.toSyncReplicationState(data.getToState());
    if (data.hasFromState()) {
      fromState = ReplicationPeerConfigUtil.toSyncReplicationState(data.getFromState());
    }
  }

  @Override
  protected PeerSyncReplicationStateTransitionState getState(int stateId) {
    return PeerSyncReplicationStateTransitionState.forNumber(stateId);
  }

  @Override
  protected int getStateId(PeerSyncReplicationStateTransitionState state) {
    return state.getNumber();
  }

  @Override
  protected PeerSyncReplicationStateTransitionState getInitialState() {
    return PeerSyncReplicationStateTransitionState.PRE_PEER_SYNC_REPLICATION_STATE_TRANSITION;
  }

  private void preTransit(MasterProcedureEnv env) throws IOException {
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preTransitReplicationPeerSyncReplicationState(peerId, toState);
    }
    ReplicationPeerDescription desc =
      env.getReplicationPeerManager().preTransitPeerSyncReplicationState(peerId, toState);
    if (toState == SyncReplicationState.ACTIVE) {
      Path remoteWALDirForPeer =
        ReplicationUtils.getPeerRemoteWALDir(desc.getPeerConfig().getRemoteWALDir(), peerId);
      // check whether the remote wal directory is present
      if (!remoteWALDirForPeer.getFileSystem(env.getMasterConfiguration())
        .exists(remoteWALDirForPeer)) {
        throw new DoNotRetryIOException(
          "The remote WAL directory " + remoteWALDirForPeer + " does not exist");
      }
    }
    fromState = desc.getSyncReplicationState();
    enabled = desc.isEnabled();
  }

  private void postTransit(MasterProcedureEnv env) throws IOException {
    LOG.info(
      "Successfully transit current cluster state from {} to {} for sync replication peer {}",
      fromState, toState, peerId);
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      env.getMasterCoprocessorHost().postTransitReplicationPeerSyncReplicationState(peerId,
        fromState, toState);
    }
  }

  private List<RegionInfo> getRegionsToReopen(MasterProcedureEnv env) {
    return env.getReplicationPeerManager().getPeerConfig(peerId).get().getTableCFsMap().keySet()
      .stream()
      .flatMap(tn -> env.getAssignmentManager().getRegionStates().getRegionsOfTable(tn).stream())
      .collect(Collectors.toList());
  }

  private void createDirForRemoteWAL(MasterProcedureEnv env)
      throws ProcedureYieldException, IOException {
    MasterFileSystem mfs = env.getMasterFileSystem();
    Path remoteWALDir = new Path(mfs.getWALRootDir(), ReplicationUtils.REMOTE_WAL_DIR_NAME);
    Path remoteWALDirForPeer = ReplicationUtils.getPeerRemoteWALDir(remoteWALDir, peerId);
    FileSystem walFs = mfs.getWALFileSystem();
    if (walFs.exists(remoteWALDirForPeer)) {
      LOG.warn("Wal dir {} already exists, usually this should not happen, continue anyway",
        remoteWALDirForPeer);
    } else if (!walFs.mkdirs(remoteWALDirForPeer)) {
      LOG.warn("Can not create remote wal dir {}", remoteWALDirForPeer);
      throw new ProcedureYieldException();
    }
  }

  private void setNextStateAfterRefreshBegin() {
    if (fromState.equals(SyncReplicationState.ACTIVE)) {
      setNextState(toState.equals(SyncReplicationState.STANDBY)
        ? PeerSyncReplicationStateTransitionState.REMOVE_ALL_REPLICATION_QUEUES_IN_PEER
        : PeerSyncReplicationStateTransitionState.REOPEN_ALL_REGIONS_IN_PEER);
    } else if (fromState.equals(SyncReplicationState.DOWNGRADE_ACTIVE)) {
      setNextState(toState.equals(SyncReplicationState.STANDBY)
        ? PeerSyncReplicationStateTransitionState.REMOVE_ALL_REPLICATION_QUEUES_IN_PEER
        : PeerSyncReplicationStateTransitionState.REOPEN_ALL_REGIONS_IN_PEER);
    } else {
      assert toState.equals(SyncReplicationState.DOWNGRADE_ACTIVE);
      setNextState(PeerSyncReplicationStateTransitionState.REPLAY_REMOTE_WAL_IN_PEER);
    }
  }

  private void setNextStateAfterRefreshEnd() {
    if (toState == SyncReplicationState.STANDBY) {
      setNextState(
        enabled ? PeerSyncReplicationStateTransitionState.SYNC_REPLICATION_SET_PEER_ENABLED
          : PeerSyncReplicationStateTransitionState.CREATE_DIR_FOR_REMOTE_WAL);
    } else {
      setNextState(
        PeerSyncReplicationStateTransitionState.POST_PEER_SYNC_REPLICATION_STATE_TRANSITION);
    }
  }

  private void replayRemoteWAL() {
    addChildProcedure(new RecoverStandbyProcedure[] { new RecoverStandbyProcedure(peerId) });
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env,
      PeerSyncReplicationStateTransitionState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    switch (state) {
      case PRE_PEER_SYNC_REPLICATION_STATE_TRANSITION:
        try {
          preTransit(env);
        } catch (IOException e) {
          LOG.warn("Failed to call pre CP hook or the pre check is failed for peer {} " +
            "when transiting sync replication peer state to {}, " +
            "mark the procedure as failure and give up", peerId, toState, e);
          setFailure("master-transit-peer-sync-replication-state", e);
          return Flow.NO_MORE_STATE;
        }
        setNextState(PeerSyncReplicationStateTransitionState.SET_PEER_NEW_SYNC_REPLICATION_STATE);
        return Flow.HAS_MORE_STATE;
      case SET_PEER_NEW_SYNC_REPLICATION_STATE:
        try {
          env.getReplicationPeerManager().setPeerNewSyncReplicationState(peerId, toState);
          if (toState.equals(SyncReplicationState.STANDBY) && enabled) {
            // disable the peer if we are going to transit to STANDBY state, as we need to remove
            // all the pending replication files. If we do not disable the peer and delete the wal
            // queues on zk directly, RS will get NoNode exception when updating the wal position
            // and crash.
            env.getReplicationPeerManager().disablePeer(peerId);
          }
        } catch (ReplicationException e) {
          LOG.warn("Failed to update peer storage for peer {} when starting transiting sync " +
            "replication peer state from {} to {}, retry", peerId, fromState, toState, e);
          throw new ProcedureYieldException();
        }
        setNextState(
          PeerSyncReplicationStateTransitionState.REFRESH_PEER_SYNC_REPLICATION_STATE_ON_RS_BEGIN);
        return Flow.HAS_MORE_STATE;
      case REFRESH_PEER_SYNC_REPLICATION_STATE_ON_RS_BEGIN:
        addChildProcedure(env.getMasterServices().getServerManager().getOnlineServersList().stream()
          .map(sn -> new RefreshPeerProcedure(peerId, getPeerOperationType(), sn, 0))
          .toArray(RefreshPeerProcedure[]::new));
        setNextStateAfterRefreshBegin();
        return Flow.HAS_MORE_STATE;
      case REPLAY_REMOTE_WAL_IN_PEER:
        replayRemoteWAL();
        setNextState(
          PeerSyncReplicationStateTransitionState.TRANSIT_PEER_NEW_SYNC_REPLICATION_STATE);
        return Flow.HAS_MORE_STATE;
      case REMOVE_ALL_REPLICATION_QUEUES_IN_PEER:
        try {
          env.getReplicationPeerManager().removeAllQueues(peerId);
        } catch (ReplicationException e) {
          LOG.warn("Failed to remove all replication queues peer {} when starting transiting" +
            " sync replication peer state from {} to {}, retry", peerId, fromState, toState, e);
          throw new ProcedureYieldException();
        }
        setNextState(fromState.equals(SyncReplicationState.ACTIVE)
          ? PeerSyncReplicationStateTransitionState.REOPEN_ALL_REGIONS_IN_PEER
          : PeerSyncReplicationStateTransitionState.TRANSIT_PEER_NEW_SYNC_REPLICATION_STATE);
        return Flow.HAS_MORE_STATE;
      case REOPEN_ALL_REGIONS_IN_PEER:
        try {
          addChildProcedure(
            env.getAssignmentManager().createReopenProcedures(getRegionsToReopen(env)));
        } catch (IOException e) {
          LOG.warn("Failed to schedule region reopen for peer {} when starting transiting sync " +
            "replication peer state from {} to {}, retry", peerId, fromState, toState, e);
          throw new ProcedureYieldException();
        }
        setNextState(
          PeerSyncReplicationStateTransitionState.TRANSIT_PEER_NEW_SYNC_REPLICATION_STATE);
        return Flow.HAS_MORE_STATE;
      case TRANSIT_PEER_NEW_SYNC_REPLICATION_STATE:
        try {
          env.getReplicationPeerManager().transitPeerSyncReplicationState(peerId, toState);
        } catch (ReplicationException e) {
          LOG.warn("Failed to update peer storage for peer {} when ending transiting sync " +
            "replication peer state from {} to {}, retry", peerId, fromState, toState, e);
          throw new ProcedureYieldException();
        }
        setNextState(
          PeerSyncReplicationStateTransitionState.REFRESH_PEER_SYNC_REPLICATION_STATE_ON_RS_END);
        return Flow.HAS_MORE_STATE;
      case REFRESH_PEER_SYNC_REPLICATION_STATE_ON_RS_END:
        addChildProcedure(env.getMasterServices().getServerManager().getOnlineServersList().stream()
          .map(sn -> new RefreshPeerProcedure(peerId, getPeerOperationType(), sn, 1))
          .toArray(RefreshPeerProcedure[]::new));
        setNextStateAfterRefreshEnd();
        return Flow.HAS_MORE_STATE;
      case SYNC_REPLICATION_SET_PEER_ENABLED:
        try {
          env.getReplicationPeerManager().enablePeer(peerId);
        } catch (ReplicationException e) {
          LOG.warn("Failed to set peer enabled for peer {} when transiting sync replication peer " +
            "state from {} to {}, retry", peerId, fromState, toState, e);
          throw new ProcedureYieldException();
        }
        setNextState(
          PeerSyncReplicationStateTransitionState.SYNC_REPLICATION_ENABLE_PEER_REFRESH_PEER_ON_RS);
        return Flow.HAS_MORE_STATE;
      case SYNC_REPLICATION_ENABLE_PEER_REFRESH_PEER_ON_RS:
        refreshPeer(env, PeerOperationType.ENABLE);
        setNextState(PeerSyncReplicationStateTransitionState.CREATE_DIR_FOR_REMOTE_WAL);
        return Flow.HAS_MORE_STATE;
      case CREATE_DIR_FOR_REMOTE_WAL:
        try {
          createDirForRemoteWAL(env);
        } catch (IOException e) {
          LOG.warn("Failed to create remote wal dir for peer {} when transiting sync replication " +
            "peer state from {} to {}, retry", peerId, fromState, toState, e);
          throw new ProcedureYieldException();
        }
        setNextState(
          PeerSyncReplicationStateTransitionState.POST_PEER_SYNC_REPLICATION_STATE_TRANSITION);
        return Flow.HAS_MORE_STATE;
      case POST_PEER_SYNC_REPLICATION_STATE_TRANSITION:
        try {
          postTransit(env);
        } catch (IOException e) {
          LOG.warn(
            "Failed to call post CP hook for peer {} when transiting sync replication " +
              "peer state from {} to {}, ignore since the procedure has already done",
            peerId, fromState, toState, e);
        }
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }
}
