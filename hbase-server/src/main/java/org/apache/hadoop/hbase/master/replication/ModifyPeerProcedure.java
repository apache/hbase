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
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.PeerProcedureInterface;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ModifyPeerStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerModificationState;

/**
 * The base class for all replication peer related procedure.
 */
@InterfaceAudience.Private
public abstract class ModifyPeerProcedure
    extends StateMachineProcedure<MasterProcedureEnv, PeerModificationState>
    implements PeerProcedureInterface {

  private static final Logger LOG = LoggerFactory.getLogger(ModifyPeerProcedure.class);

  protected String peerId;

  private volatile boolean locked;

  // used to keep compatible with old client where we can only returns after updateStorage.
  protected ProcedurePrepareLatch latch;

  protected ModifyPeerProcedure() {
  }

  protected ModifyPeerProcedure(String peerId) {
    this.peerId = peerId;
    this.latch = ProcedurePrepareLatch.createLatch(2, 0);
  }

  public ProcedurePrepareLatch getLatch() {
    return latch;
  }

  @Override
  public String getPeerId() {
    return peerId;
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
        addChildProcedure(env.getMasterServices().getServerManager().getOnlineServersList().stream()
            .map(sn -> new RefreshPeerProcedure(peerId, getPeerOperationType(), sn))
            .toArray(RefreshPeerProcedure[]::new));
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
  protected LockState acquireLock(MasterProcedureEnv env) {
    if (env.getProcedureScheduler().waitPeerExclusiveLock(this, peerId)) {
      return  LockState.LOCK_EVENT_WAIT;
    }
    locked = true;
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(MasterProcedureEnv env) {
    locked = false;
    env.getProcedureScheduler().wakePeerExclusiveLock(this, peerId);
  }

  @Override
  protected boolean holdLock(MasterProcedureEnv env) {
    return true;
  }

  @Override
  protected boolean hasLock(MasterProcedureEnv env) {
    return locked;
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

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    serializer.serialize(ModifyPeerStateData.newBuilder().setPeerId(peerId).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    peerId = serializer.deserialize(ModifyPeerStateData.class).getPeerId();
  }
}
