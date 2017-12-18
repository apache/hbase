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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.PeerProcedureInterface;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ModifyPeerStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerModificationState;

/**
 * The base class for all replication peer related procedure.
 */
@InterfaceAudience.Private
public abstract class ModifyPeerProcedure
    extends StateMachineProcedure<MasterProcedureEnv, PeerModificationState>
    implements PeerProcedureInterface {

  private static final Log LOG = LogFactory.getLog(ModifyPeerProcedure.class);

  protected String peerId;

  // used to keep compatible with old client where we can only returns after updateStorage.
  protected ProcedurePrepareLatch latch;

  protected ModifyPeerProcedure() {
  }

  protected ModifyPeerProcedure(String peerId) {
    this.peerId = peerId;
    // TODO: temporarily set a 4.0 here to always wait for the procedure exection completed. Change
    // to 3.0 or 2.0 after the client modification is done.
    this.latch = ProcedurePrepareLatch.createLatch(4, 0);
  }

  public ProcedurePrepareLatch getLatch() {
    return latch;
  }

  @Override
  public String getPeerId() {
    return peerId;
  }

  /**
   * Called before we start the actual processing. If an exception is thrown then we will give up
   * and mark the procedure as failed directly.
   */
  protected abstract void prePeerModification(MasterProcedureEnv env) throws IOException;

  /**
   * We will give up and mark the procedure as failure if {@link IllegalArgumentException} is
   * thrown, for other type of Exception we will retry.
   */
  protected abstract void updatePeerStorage(MasterProcedureEnv env)
      throws IllegalArgumentException, Exception;

  /**
   * Called before we finish the procedure. The implementation can do some logging work, and also
   * call the coprocessor hook if any.
   * <p>
   * Notice that, since we have already done the actual work, throwing exception here will not fail
   * this procedure, we will just ignore it and finish the procedure as suceeded.
   */
  protected abstract void postPeerModification(MasterProcedureEnv env) throws IOException;

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
          LOG.warn(getClass().getName() + " failed to call prePeerModification for peer " + peerId +
            ", mark the procedure as failure and give up", e);
          setFailure("prePeerModification", e);
          releaseLatch();
          return Flow.NO_MORE_STATE;
        }
        setNextState(PeerModificationState.UPDATE_PEER_STORAGE);
        return Flow.HAS_MORE_STATE;
      case UPDATE_PEER_STORAGE:
        try {
          updatePeerStorage(env);
        } catch (IllegalArgumentException e) {
          setFailure("master-" + getPeerOperationType().name().toLowerCase() + "-peer",
            new DoNotRetryIOException(e));
          releaseLatch();
          return Flow.NO_MORE_STATE;
        } catch (Exception e) {
          LOG.warn(
            getClass().getName() + " update peer storage for peer " + peerId + " failed, retry", e);
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
        } catch (IOException e) {
          LOG.warn(getClass().getName() + " failed to call prePeerModification for peer " + peerId +
            ", ignore since the procedure has already done", e);
        }
        releaseLatch();
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  @Override
  protected LockState acquireLock(MasterProcedureEnv env) {
    return env.getProcedureScheduler().waitPeerExclusiveLock(this, peerId)
      ? LockState.LOCK_EVENT_WAIT
      : LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(MasterProcedureEnv env) {
    env.getProcedureScheduler().wakePeerExclusiveLock(this, peerId);
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, PeerModificationState state)
      throws IOException, InterruptedException {
    if (state == PeerModificationState.PRE_PEER_MODIFICATION ||
      state == PeerModificationState.UPDATE_PEER_STORAGE) {
      // actually the peer related operations has no rollback, but if we haven't done any
      // modifications on the peer storage, we can just return.
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
