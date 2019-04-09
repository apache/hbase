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
import java.io.InterruptedIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.hadoop.hbase.master.TableStateManager.TableStateNotFoundException;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.master.procedure.ReopenTableRegionsProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerModificationState;

/**
 * The base class for all replication peer related procedure except sync replication state
 * transition.
 */
@InterfaceAudience.Private
public abstract class ModifyPeerProcedure extends AbstractPeerProcedure<PeerModificationState> {

  private static final Logger LOG = LoggerFactory.getLogger(ModifyPeerProcedure.class);

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
      throws IOException, ReplicationException, InterruptedException;

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

  protected void releaseLatch(MasterProcedureEnv env) {
    ProcedurePrepareLatch.releaseLatch(latch, this);
  }

  /**
   * Implementation class can override this method. By default we will jump to
   * POST_PEER_MODIFICATION and finish the procedure.
   */
  protected PeerModificationState nextStateAfterRefresh() {
    return PeerModificationState.POST_PEER_MODIFICATION;
  }

  /**
   * The implementation class should override this method if the procedure may enter the serial
   * related states.
   */
  protected boolean enablePeerBeforeFinish() {
    throw new UnsupportedOperationException();
  }

  protected ReplicationPeerConfig getOldPeerConfig() {
    return null;
  }

  protected ReplicationPeerConfig getNewPeerConfig() {
    throw new UnsupportedOperationException();
  }

  protected void updateLastPushedSequenceIdForSerialPeer(MasterProcedureEnv env)
      throws IOException, ReplicationException {
    throw new UnsupportedOperationException();
  }

  // If the table is in enabling state, we need to wait until it is enabled and then reopen all its
  // regions.
  private boolean needReopen(TableStateManager tsm, TableName tn) throws IOException {
    for (;;) {
      try {
        TableState state = tsm.getTableState(tn);
        if (state.isEnabled()) {
          return true;
        }
        if (!state.isEnabling()) {
          return false;
        }
        Thread.sleep(SLEEP_INTERVAL_MS);
      } catch (TableStateNotFoundException e) {
        return false;
      } catch (InterruptedException e) {
        throw (IOException) new InterruptedIOException(e.getMessage()).initCause(e);
      }
    }
  }

  // will be override in test to simulate error
  @VisibleForTesting
  protected void reopenRegions(MasterProcedureEnv env) throws IOException {
    ReplicationPeerConfig peerConfig = getNewPeerConfig();
    ReplicationPeerConfig oldPeerConfig = getOldPeerConfig();
    TableStateManager tsm = env.getMasterServices().getTableStateManager();
    for (TableDescriptor td : env.getMasterServices().getTableDescriptors().getAll().values()) {
      if (!td.hasGlobalReplicationScope()) {
        continue;
      }
      TableName tn = td.getTableName();
      if (!ReplicationUtils.contains(peerConfig, tn)) {
        continue;
      }
      if (oldPeerConfig != null && oldPeerConfig.isSerial() &&
        ReplicationUtils.contains(oldPeerConfig, tn)) {
        continue;
      }
      if (needReopen(tsm, tn)) {
        addChildProcedure(new ReopenTableRegionsProcedure(tn));
      }
    }
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, PeerModificationState state)
      throws ProcedureSuspendedException, InterruptedException {
    switch (state) {
      case PRE_PEER_MODIFICATION:
        try {
          prePeerModification(env);
        } catch (IOException e) {
          LOG.warn("{} failed to call pre CP hook or the pre check is failed for peer {}, " +
            "mark the procedure as failure and give up", getClass().getName(), peerId, e);
          setFailure("master-" + getPeerOperationType().name().toLowerCase() + "-peer", e);
          releaseLatch(env);
          return Flow.NO_MORE_STATE;
        } catch (ReplicationException e) {
          long backoff = ProcedureUtil.getBackoffTimeMs(attempts);
          LOG.warn("{} failed to call prePeerModification for peer {}, sleep {} secs",
            getClass().getName(), peerId, backoff / 1000, e);
          throw suspend(backoff);
        }
        attempts = 0;
        setNextState(PeerModificationState.UPDATE_PEER_STORAGE);
        return Flow.HAS_MORE_STATE;
      case UPDATE_PEER_STORAGE:
        try {
          updatePeerStorage(env);
        } catch (ReplicationException e) {
          long backoff = ProcedureUtil.getBackoffTimeMs(attempts);
          LOG.warn("{} update peer storage for peer {} failed, sleep {} secs", getClass().getName(),
            peerId, backoff / 1000, e);
          throw suspend(backoff);
        }
        attempts = 0;
        setNextState(PeerModificationState.REFRESH_PEER_ON_RS);
        return Flow.HAS_MORE_STATE;
      case REFRESH_PEER_ON_RS:
        refreshPeer(env, getPeerOperationType());
        setNextState(nextStateAfterRefresh());
        return Flow.HAS_MORE_STATE;
      case SERIAL_PEER_REOPEN_REGIONS:
        try {
          reopenRegions(env);
        } catch (Exception e) {
          long backoff = ProcedureUtil.getBackoffTimeMs(attempts);
          LOG.warn("{} reopen regions for peer {} failed,  sleep {} secs", getClass().getName(),
            peerId, backoff / 1000, e);
          throw suspend(backoff);
        }
        attempts = 0;
        setNextState(PeerModificationState.SERIAL_PEER_UPDATE_LAST_PUSHED_SEQ_ID);
        return Flow.HAS_MORE_STATE;
      case SERIAL_PEER_UPDATE_LAST_PUSHED_SEQ_ID:
        try {
          updateLastPushedSequenceIdForSerialPeer(env);
        } catch (Exception e) {
          long backoff = ProcedureUtil.getBackoffTimeMs(attempts);
          LOG.warn("{} set last sequence id for peer {} failed,  sleep {} secs",
            getClass().getName(), peerId, backoff / 1000, e);
          throw suspend(backoff);
        }
        attempts = 0;
        setNextState(enablePeerBeforeFinish() ? PeerModificationState.SERIAL_PEER_SET_PEER_ENABLED
          : PeerModificationState.POST_PEER_MODIFICATION);
        return Flow.HAS_MORE_STATE;
      case SERIAL_PEER_SET_PEER_ENABLED:
        try {
          enablePeer(env);
        } catch (ReplicationException e) {
          long backoff = ProcedureUtil.getBackoffTimeMs(attempts);
          LOG.warn("{} enable peer before finish for peer {} failed,  sleep {} secs",
            getClass().getName(), peerId, backoff / 1000, e);
          throw suspend(backoff);
        }
        attempts = 0;
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
          long backoff = ProcedureUtil.getBackoffTimeMs(attempts);
          LOG.warn("{} failed to call postPeerModification for peer {},  sleep {} secs",
            getClass().getName(), peerId, backoff / 1000, e);
          throw suspend(backoff);
        } catch (IOException e) {
          LOG.warn("{} failed to call post CP hook for peer {}, " +
            "ignore since the procedure has already done", getClass().getName(), peerId, e);
        }
        releaseLatch(env);
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
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
