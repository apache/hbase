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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RecoverStandbyState;

@InterfaceAudience.Private
public class RecoverStandbyProcedure extends AbstractPeerProcedure<RecoverStandbyState> {

  private static final Logger LOG = LoggerFactory.getLogger(RecoverStandbyProcedure.class);

  public RecoverStandbyProcedure() {
  }

  public RecoverStandbyProcedure(String peerId) {
    super(peerId);
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, RecoverStandbyState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    ReplaySyncReplicationWALManager replaySyncReplicationWALManager =
        env.getMasterServices().getReplaySyncReplicationWALManager();
    switch (state) {
      case RENAME_SYNC_REPLICATION_WALS_DIR:
        try {
          replaySyncReplicationWALManager.renameToPeerReplayWALDir(peerId);
        } catch (IOException e) {
          LOG.warn("Failed to rename remote wal dir for peer id={}", peerId, e);
          setFailure("master-recover-standby", e);
          return Flow.NO_MORE_STATE;
        }
        setNextState(RecoverStandbyState.INIT_WORKERS);
        return Flow.HAS_MORE_STATE;
      case INIT_WORKERS:
        replaySyncReplicationWALManager.initPeerWorkers(peerId);
        setNextState(RecoverStandbyState.DISPATCH_TASKS);
        return Flow.HAS_MORE_STATE;
      case DISPATCH_TASKS:
        addChildProcedure(getReplayWALs(replaySyncReplicationWALManager).stream()
            .map(wal -> new ReplaySyncReplicationWALProcedure(peerId,
                replaySyncReplicationWALManager.removeWALRootPath(wal)))
            .toArray(ReplaySyncReplicationWALProcedure[]::new));
        setNextState(RecoverStandbyState.SNAPSHOT_SYNC_REPLICATION_WALS_DIR);
        return Flow.HAS_MORE_STATE;
      case SNAPSHOT_SYNC_REPLICATION_WALS_DIR:
        try {
          replaySyncReplicationWALManager.renameToPeerSnapshotWALDir(peerId);
        } catch (IOException e) {
          LOG.warn("Failed to cleanup replay wals dir for peer id={}, , retry", peerId, e);
          throw new ProcedureYieldException();
        }
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  private List<Path> getReplayWALs(ReplaySyncReplicationWALManager replaySyncReplicationWALManager)
      throws ProcedureYieldException {
    try {
      return replaySyncReplicationWALManager.getReplayWALsAndCleanUpUnusedFiles(peerId);
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
}