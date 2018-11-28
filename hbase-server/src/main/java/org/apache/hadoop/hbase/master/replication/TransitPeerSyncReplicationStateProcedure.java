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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ReopenTableRegionsProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

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

  protected SyncReplicationState fromState;

  private SyncReplicationState toState;

  private boolean enabled;

  private boolean serial;

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

  @VisibleForTesting
  protected void preTransit(MasterProcedureEnv env) throws IOException {
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
    serial = desc.getPeerConfig().isSerial();
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

  @VisibleForTesting
  protected void reopenRegions(MasterProcedureEnv env) {
    addChildProcedure(
      env.getReplicationPeerManager().getPeerConfig(peerId).get().getTableCFsMap().keySet().stream()
        .map(ReopenTableRegionsProcedure::new).toArray(ReopenTableRegionsProcedure[]::new));
  }

  @VisibleForTesting
  protected void createDirForRemoteWAL(MasterProcedureEnv env) throws IOException {
    MasterFileSystem mfs = env.getMasterFileSystem();
    Path remoteWALDir = new Path(mfs.getWALRootDir(), ReplicationUtils.REMOTE_WAL_DIR_NAME);
    Path remoteWALDirForPeer = ReplicationUtils.getPeerRemoteWALDir(remoteWALDir, peerId);
    FileSystem walFs = mfs.getWALFileSystem();
    if (walFs.exists(remoteWALDirForPeer)) {
      LOG.warn("Wal dir {} already exists, usually this should not happen, continue anyway",
        remoteWALDirForPeer);
    } else if (!walFs.mkdirs(remoteWALDirForPeer)) {
      throw new IOException("Failed to create remote wal dir " + remoteWALDirForPeer);
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
      // for serial peer, we need to reopen all the regions and then update the last pushed sequence
      // id, before replaying any remote wals, so that the serial replication will not be stuck, and
      // also guarantee the order when replicating the remote wal back.
      setNextState(serial ? PeerSyncReplicationStateTransitionState.REOPEN_ALL_REGIONS_IN_PEER
        : PeerSyncReplicationStateTransitionState.REPLAY_REMOTE_WAL_IN_PEER);
    }
  }

  private void setNextStateAfterRefreshEnd() {
    if (toState == SyncReplicationState.STANDBY) {
      setNextState(
        enabled ? PeerSyncReplicationStateTransitionState.SYNC_REPLICATION_SET_PEER_ENABLED
          : PeerSyncReplicationStateTransitionState.CREATE_DIR_FOR_REMOTE_WAL);
    } else if (fromState == SyncReplicationState.STANDBY) {
      assert toState.equals(SyncReplicationState.DOWNGRADE_ACTIVE);
      setNextState(serial && enabled
        ? PeerSyncReplicationStateTransitionState.SYNC_REPLICATION_SET_PEER_ENABLED
        : PeerSyncReplicationStateTransitionState.POST_PEER_SYNC_REPLICATION_STATE_TRANSITION);
    } else {
      setNextState(
        PeerSyncReplicationStateTransitionState.POST_PEER_SYNC_REPLICATION_STATE_TRANSITION);
    }
  }

  private void replayRemoteWAL(boolean serial) {
    addChildProcedure(new RecoverStandbyProcedure(peerId, serial));
  }

  @VisibleForTesting
  protected void setPeerNewSyncReplicationState(MasterProcedureEnv env)
      throws ReplicationException {
    if (toState.equals(SyncReplicationState.STANDBY) ||
      (fromState.equals(SyncReplicationState.STANDBY) && serial) && enabled) {
      // Disable the peer if we are going to transit to STANDBY state, as we need to remove
      // all the pending replication files. If we do not disable the peer and delete the wal
      // queues on zk directly, RS will get NoNode exception when updating the wal position
      // and crash.
      // Disable the peer if we are going to transit from STANDBY to DOWNGRADE_ACTIVE, and the
      // replication is serial, as we need to update the lastPushedSequence id after we reopen all
      // the regions, and for performance reason here we will update in batch, without using CAS, if
      // we are still replicating at RS side, we may accidentally update the last pushed sequence id
      // to a less value and cause the replication to be stuck.
      env.getReplicationPeerManager().disablePeer(peerId);
    }
    env.getReplicationPeerManager().setPeerNewSyncReplicationState(peerId, toState);
  }

  @VisibleForTesting
  protected void removeAllReplicationQueues(MasterProcedureEnv env) throws ReplicationException {
    env.getReplicationPeerManager().removeAllQueues(peerId);
  }

  @VisibleForTesting
  protected void transitPeerSyncReplicationState(MasterProcedureEnv env)
      throws ReplicationException {
    env.getReplicationPeerManager().transitPeerSyncReplicationState(peerId, toState);
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env,
      PeerSyncReplicationStateTransitionState state) throws ProcedureSuspendedException {
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
          setPeerNewSyncReplicationState(env);
        } catch (ReplicationException e) {
          long backoff = ProcedureUtil.getBackoffTimeMs(attempts);
          LOG.warn(
            "Failed to update peer storage for peer {} when starting transiting sync " +
              "replication peer state from {} to {}, sleep {} secs and retry",
            peerId, fromState, toState, backoff / 1000, e);
          throw suspend(backoff);
        }
        attempts = 0;
        setNextState(
          PeerSyncReplicationStateTransitionState.REFRESH_PEER_SYNC_REPLICATION_STATE_ON_RS_BEGIN);
        return Flow.HAS_MORE_STATE;
      case REFRESH_PEER_SYNC_REPLICATION_STATE_ON_RS_BEGIN:
        addChildProcedure(env.getMasterServices().getServerManager().getOnlineServersList().stream()
          .map(sn -> new RefreshPeerProcedure(peerId, getPeerOperationType(), sn, 0))
          .toArray(RefreshPeerProcedure[]::new));
        setNextStateAfterRefreshBegin();
        return Flow.HAS_MORE_STATE;
      case REOPEN_ALL_REGIONS_IN_PEER:
        reopenRegions(env);
        if (fromState.equals(SyncReplicationState.STANDBY)) {
          assert serial;
          setNextState(
            PeerSyncReplicationStateTransitionState.SYNC_REPLICATION_UPDATE_LAST_PUSHED_SEQ_ID_FOR_SERIAL_PEER);
        } else {
          setNextState(
            PeerSyncReplicationStateTransitionState.TRANSIT_PEER_NEW_SYNC_REPLICATION_STATE);
        }
        return Flow.HAS_MORE_STATE;
      case SYNC_REPLICATION_UPDATE_LAST_PUSHED_SEQ_ID_FOR_SERIAL_PEER:
        try {
          setLastPushedSequenceId(env, env.getReplicationPeerManager().getPeerConfig(peerId).get());
        } catch (Exception e) {
          long backoff = ProcedureUtil.getBackoffTimeMs(attempts);
          LOG.warn(
            "Failed to update last pushed sequence id for peer {} when transiting sync " +
              "replication peer state from {} to {}, sleep {} secs and retry",
            peerId, fromState, toState, backoff / 1000, e);
          throw suspend(backoff);
        }
        setNextState(PeerSyncReplicationStateTransitionState.REPLAY_REMOTE_WAL_IN_PEER);
        return Flow.HAS_MORE_STATE;
      case REPLAY_REMOTE_WAL_IN_PEER:
        replayRemoteWAL(env.getReplicationPeerManager().getPeerConfig(peerId).get().isSerial());
        setNextState(
          PeerSyncReplicationStateTransitionState.TRANSIT_PEER_NEW_SYNC_REPLICATION_STATE);
        return Flow.HAS_MORE_STATE;
      case REMOVE_ALL_REPLICATION_QUEUES_IN_PEER:
        try {
          removeAllReplicationQueues(env);
        } catch (ReplicationException e) {
          long backoff = ProcedureUtil.getBackoffTimeMs(attempts);
          LOG.warn(
            "Failed to remove all replication queues peer {} when starting transiting" +
              " sync replication peer state from {} to {}, sleep {} secs and retry",
            peerId, fromState, toState, backoff / 1000, e);
          throw suspend(backoff);
        }
        attempts = 0;
        setNextState(fromState.equals(SyncReplicationState.ACTIVE)
          ? PeerSyncReplicationStateTransitionState.REOPEN_ALL_REGIONS_IN_PEER
          : PeerSyncReplicationStateTransitionState.TRANSIT_PEER_NEW_SYNC_REPLICATION_STATE);
        return Flow.HAS_MORE_STATE;
      case TRANSIT_PEER_NEW_SYNC_REPLICATION_STATE:
        try {
          transitPeerSyncReplicationState(env);
        } catch (ReplicationException e) {
          long backoff = ProcedureUtil.getBackoffTimeMs(attempts);
          LOG.warn(
            "Failed to update peer storage for peer {} when ending transiting sync " +
              "replication peer state from {} to {}, sleep {} secs and retry",
            peerId, fromState, toState, backoff / 1000, e);
          throw suspend(backoff);
        }
        attempts = 0;
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
          enablePeer(env);
        } catch (ReplicationException e) {
          long backoff = ProcedureUtil.getBackoffTimeMs(attempts);
          LOG.warn(
            "Failed to set peer enabled for peer {} when transiting sync replication peer " +
              "state from {} to {}, sleep {} secs and retry",
            peerId, fromState, toState, backoff / 1000, e);
          throw suspend(backoff);
        }
        attempts = 0;
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
          long backoff = ProcedureUtil.getBackoffTimeMs(attempts);
          LOG.warn(
            "Failed to create remote wal dir for peer {} when transiting sync replication " +
              "peer state from {} to {}, sleep {} secs and retry",
            peerId, fromState, toState, backoff / 1000, e);
          throw suspend(backoff);
        }
        attempts = 0;
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
