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
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RecoverStandbyState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RecoverStandbyStateData;

/**
 * The procedure for replaying all the remote wals for transitting a sync replication peer from
 * STANDBY to DOWNGRADE_ACTIVE.
 */
@InterfaceAudience.Private
public class RecoverStandbyProcedure extends AbstractPeerNoLockProcedure<RecoverStandbyState> {

  private static final Logger LOG = LoggerFactory.getLogger(RecoverStandbyProcedure.class);

  private boolean serial;

  public RecoverStandbyProcedure() {
  }

  public RecoverStandbyProcedure(String peerId, boolean serial) {
    super(peerId);
    this.serial = serial;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, RecoverStandbyState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    SyncReplicationReplayWALManager syncReplicationReplayWALManager =
      env.getMasterServices().getSyncReplicationReplayWALManager();
    switch (state) {
      case RENAME_SYNC_REPLICATION_WALS_DIR:
        try {
          syncReplicationReplayWALManager.renameToPeerReplayWALDir(peerId);
        } catch (IOException e) {
          LOG.warn("Failed to rename remote wal dir for peer id={}", peerId, e);
          setFailure("master-recover-standby", e);
          return Flow.NO_MORE_STATE;
        }
        setNextState(RecoverStandbyState.REGISTER_PEER_TO_WORKER_STORAGE);
        return Flow.HAS_MORE_STATE;
      case REGISTER_PEER_TO_WORKER_STORAGE:
        syncReplicationReplayWALManager.registerPeer(peerId);
        setNextState(RecoverStandbyState.DISPATCH_WALS);
        return Flow.HAS_MORE_STATE;
      case DISPATCH_WALS:
        dispathWals(syncReplicationReplayWALManager);
        setNextState(RecoverStandbyState.UNREGISTER_PEER_FROM_WORKER_STORAGE);
        return Flow.HAS_MORE_STATE;
      case UNREGISTER_PEER_FROM_WORKER_STORAGE:
        syncReplicationReplayWALManager.unregisterPeer(peerId);
        setNextState(RecoverStandbyState.SNAPSHOT_SYNC_REPLICATION_WALS_DIR);
        return Flow.HAS_MORE_STATE;
      case SNAPSHOT_SYNC_REPLICATION_WALS_DIR:
        try {
          syncReplicationReplayWALManager.renameToPeerSnapshotWALDir(peerId);
        } catch (IOException e) {
          LOG.warn("Failed to cleanup replay wals dir for peer id={}, , retry", peerId, e);
          throw new ProcedureYieldException();
        }
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  // TODO: dispatch wals by region server when serial is true and sort wals
  private void dispathWals(SyncReplicationReplayWALManager syncReplicationReplayWALManager)
      throws ProcedureYieldException {
    try {
      List<Path> wals = syncReplicationReplayWALManager.getReplayWALsAndCleanUpUnusedFiles(peerId);
      addChildProcedure(wals.stream()
        .map(wal -> new SyncReplicationReplayWALProcedure(peerId,
          Arrays.asList(syncReplicationReplayWALManager.removeWALRootPath(wal))))
        .toArray(SyncReplicationReplayWALProcedure[]::new));
    } catch (IOException e) {
      LOG.warn("Failed to get replay wals for peer id={}, , retry", peerId, e);
      throw new ProcedureYieldException();
    }
  }

  @Override
  protected RecoverStandbyState getState(int stateId) {
    return RecoverStandbyState.forNumber(stateId);
  }

  @Override
  protected int getStateId(RecoverStandbyState state) {
    return state.getNumber();
  }

  @Override
  protected RecoverStandbyState getInitialState() {
    return RecoverStandbyState.RENAME_SYNC_REPLICATION_WALS_DIR;
  }

  @Override
  public PeerOperationType getPeerOperationType() {
    return PeerOperationType.RECOVER_STANDBY;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    serializer.serialize(RecoverStandbyStateData.newBuilder().setSerial(serial).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    RecoverStandbyStateData data = serializer.deserialize(RecoverStandbyStateData.class);
    serial = data.getSerial();
  }

  @Override
  protected void afterReplay(MasterProcedureEnv env) {
    // For these two states, we need to register the peer to the replay manager, as the state are
    // only kept in memory and will be lost after restarting. And in
    // SyncReplicationReplayWALProcedure.afterReplay we will reconstruct the used workers.
    switch (getCurrentState()) {
      case DISPATCH_WALS:
      case UNREGISTER_PEER_FROM_WORKER_STORAGE:
        env.getMasterServices().getSyncReplicationReplayWALManager().registerPeer(peerId);
        break;
      default:
        break;
    }
  }
}