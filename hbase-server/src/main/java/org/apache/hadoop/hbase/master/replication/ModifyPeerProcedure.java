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
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.TableStateManager.TableStateNotFoundException;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerModificationState;

/**
 * The base class for all replication peer related procedure except sync replication state
 * transition.
 */
@InterfaceAudience.Private
public abstract class ModifyPeerProcedure extends AbstractPeerProcedure<PeerModificationState> {

  private static final Logger LOG = LoggerFactory.getLogger(ModifyPeerProcedure.class);

  private static final int SET_LAST_SEQ_ID_BATCH_SIZE = 1000;

  protected ModifyPeerProcedure() {
  }

  protected ModifyPeerProcedure(String peerId) {
    super(peerId);
  }

  /**
   * Called before we start the actual processing. The implementation should call the pre CP hook,
   * and also the pre-check for the peer modification.
   * <p>
   * If an IOException is thrown then we will give up and mark the procedure as failed directly. If
   * all checks passes then the procedure can not be rolled back any more.
   */
  protected abstract void prePeerModification(MasterProcedureEnv env)
      throws IOException, ReplicationException;

  protected abstract void updatePeerStorage(MasterProcedureEnv env) throws ReplicationException;

  /**
   * Called before we finish the procedure. The implementation can do some logging work, and also
   * call the coprocessor hook if any.
   * <p>
   * Notice that, since we have already done the actual work, throwing {@code IOException} here will
   * not fail this procedure, we will just ignore it and finish the procedure as suceeded. If
   * {@code ReplicationException} is thrown we will retry since this usually means we fails to
   * update the peer storage.
   */
  protected abstract void postPeerModification(MasterProcedureEnv env)
      throws IOException, ReplicationException;

  private void releaseLatch() {
    ProcedurePrepareLatch.releaseLatch(latch, this);
  }

  /**
   * Implementation class can override this method. The default return value is false which means we
   * will jump to POST_PEER_MODIFICATION and finish the procedure. If returns true, we will jump to
   * SERIAL_PEER_REOPEN_REGIONS.
   */
  protected boolean reopenRegionsAfterRefresh() {
    return false;
  }

  /**
   * The implementation class should override this method if the procedure may enter the serial
   * related states.
   */
  protected boolean enablePeerBeforeFinish() {
    throw new UnsupportedOperationException();
  }

  private void refreshPeer(MasterProcedureEnv env, PeerOperationType type) {
    addChildProcedure(env.getMasterServices().getServerManager().getOnlineServersList().stream()
      .map(sn -> new RefreshPeerProcedure(peerId, type, sn))
      .toArray(RefreshPeerProcedure[]::new));
  }

  protected ReplicationPeerConfig getOldPeerConfig() {
    return null;
  }

  protected ReplicationPeerConfig getNewPeerConfig() {
    throw new UnsupportedOperationException();
  }

  private Stream<TableDescriptor> getTables(MasterProcedureEnv env) throws IOException {
    ReplicationPeerConfig peerConfig = getNewPeerConfig();
    Stream<TableDescriptor> stream = env.getMasterServices().getTableDescriptors().getAll().values()
      .stream().filter(TableDescriptor::hasGlobalReplicationScope)
      .filter(td -> ReplicationUtils.contains(peerConfig, td.getTableName()));
    ReplicationPeerConfig oldPeerConfig = getOldPeerConfig();
    if (oldPeerConfig != null && oldPeerConfig.isSerial()) {
      stream = stream.filter(td -> !ReplicationUtils.contains(oldPeerConfig, td.getTableName()));
    }
    return stream;
  }

  private void reopenRegions(MasterProcedureEnv env) throws IOException {
    Stream<TableDescriptor> stream = getTables(env);
    TableStateManager tsm = env.getMasterServices().getTableStateManager();
    stream.filter(td -> {
      try {
        return tsm.getTableState(td.getTableName()).isEnabled();
      } catch (TableStateNotFoundException e) {
        return false;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }).forEach(td -> {
      try {
        addChildProcedure(env.getAssignmentManager().createReopenProcedures(
          env.getAssignmentManager().getRegionStates().getRegionsOfTable(td.getTableName())));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
  }

  private void addToMap(Map<String, Long> lastSeqIds, String encodedRegionName, long barrier,
      ReplicationQueueStorage queueStorage) throws ReplicationException {
    if (barrier >= 0) {
      lastSeqIds.put(encodedRegionName, barrier);
      if (lastSeqIds.size() >= SET_LAST_SEQ_ID_BATCH_SIZE) {
        queueStorage.setLastSequenceIds(peerId, lastSeqIds);
        lastSeqIds.clear();
      }
    }
  }

  private void setLastSequenceIdForSerialPeer(MasterProcedureEnv env)
      throws IOException, ReplicationException {
    Stream<TableDescriptor> stream = getTables(env);
    TableStateManager tsm = env.getMasterServices().getTableStateManager();
    ReplicationQueueStorage queueStorage = env.getReplicationPeerManager().getQueueStorage();
    Connection conn = env.getMasterServices().getConnection();
    RegionStates regionStates = env.getAssignmentManager().getRegionStates();
    MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    Map<String, Long> lastSeqIds = new HashMap<String, Long>();
    stream.forEach(td -> {
      try {
        if (tsm.getTableState(td.getTableName()).isEnabled()) {
          for (Pair<String, Long> name2Barrier : MetaTableAccessor
            .getTableEncodedRegionNameAndLastBarrier(conn, td.getTableName())) {
            addToMap(lastSeqIds, name2Barrier.getFirst(), name2Barrier.getSecond().longValue() - 1,
              queueStorage);
          }
        } else {
          for (RegionInfo region : regionStates.getRegionsOfTable(td.getTableName(), true)) {
            long maxSequenceId =
              WALSplitter.getMaxRegionSequenceId(mfs.getFileSystem(), mfs.getRegionDir(region));
            addToMap(lastSeqIds, region.getEncodedName(), maxSequenceId, queueStorage);
          }
        }
      } catch (IOException | ReplicationException e) {
        throw new RuntimeException(e);
      }
    });
    if (!lastSeqIds.isEmpty()) {
      queueStorage.setLastSequenceIds(peerId, lastSeqIds);
    }
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
        refreshPeer(env, getPeerOperationType());
        setNextState(reopenRegionsAfterRefresh() ? PeerModificationState.SERIAL_PEER_REOPEN_REGIONS
          : PeerModificationState.POST_PEER_MODIFICATION);
        return Flow.HAS_MORE_STATE;
      case SERIAL_PEER_REOPEN_REGIONS:
        try {
          reopenRegions(env);
        } catch (Exception e) {
          LOG.warn("{} reopen regions for peer {} failed, retry", getClass().getName(), peerId, e);
          throw new ProcedureYieldException();
        }
        setNextState(PeerModificationState.SERIAL_PEER_UPDATE_LAST_PUSHED_SEQ_ID);
        return Flow.HAS_MORE_STATE;
      case SERIAL_PEER_UPDATE_LAST_PUSHED_SEQ_ID:
        try {
          setLastSequenceIdForSerialPeer(env);
        } catch (Exception e) {
          LOG.warn("{} set last sequence id for peer {} failed, retry", getClass().getName(),
            peerId, e);
          throw new ProcedureYieldException();
        }
        setNextState(enablePeerBeforeFinish() ? PeerModificationState.SERIAL_PEER_SET_PEER_ENABLED
          : PeerModificationState.POST_PEER_MODIFICATION);
        return Flow.HAS_MORE_STATE;
      case SERIAL_PEER_SET_PEER_ENABLED:
        try {
          env.getReplicationPeerManager().enablePeer(peerId);
        } catch (ReplicationException e) {
          LOG.warn("{} enable peer before finish for peer {} failed, retry", getClass().getName(),
            peerId, e);
          throw new ProcedureYieldException();
        }
        setNextState(PeerModificationState.SERIAL_PEER_ENABLE_PEER_REFRESH_PEER_ON_RS);
        return Flow.HAS_MORE_STATE;
      case SERIAL_PEER_ENABLE_PEER_REFRESH_PEER_ON_RS:
        refreshPeer(env, PeerOperationType.ENABLE);
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

  @Override
  protected void rollbackState(MasterProcedureEnv env, PeerModificationState state)
      throws IOException, InterruptedException {
    if (state == PeerModificationState.PRE_PEER_MODIFICATION) {
      // actually the peer related operations has no rollback, but if we haven't done any
      // modifications on the peer storage yet, we can just return.
      return;
    }
    throw new UnsupportedOperationException();
  }

  @Override
  protected PeerModificationState getState(int stateId) {
    return PeerModificationState.forNumber(stateId);
  }

  @Override
  protected int getStateId(PeerModificationState state) {
    return state.getNumber();
  }

  @Override
  protected PeerModificationState getInitialState() {
    return PeerModificationState.PRE_PEER_MODIFICATION;
  }
}
