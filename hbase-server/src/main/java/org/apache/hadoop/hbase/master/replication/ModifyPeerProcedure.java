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
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.PeerProcedureInterface;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerModificationState;

@InterfaceAudience.Private
public abstract class ModifyPeerProcedure
    extends StateMachineProcedure<MasterProcedureEnv, PeerModificationState>
    implements PeerProcedureInterface {

  private static final Log LOG = LogFactory.getLog(ModifyPeerProcedure.class);

  protected String peerId;

  protected ModifyPeerProcedure() {
  }

  protected ModifyPeerProcedure(String peerId) {
    this.peerId = peerId;
  }

  @Override
  public String getPeerId() {
    return peerId;
  }

  /**
   * Return {@code false} means that the operation is invalid and we should give up, otherwise
   * {@code true}.
   * <p>
   * You need to call {@link #setFailure(String, Throwable)} to give the detail failure information.
   */
  protected abstract boolean updatePeerStorage() throws IOException;

  protected void postPeerModification() {
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, PeerModificationState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    switch (state) {
      case UPDATE_PEER_STORAGE:
        try {
          if (!updatePeerStorage()) {
            assert isFailed() : "setFailure is not called";
            return Flow.NO_MORE_STATE;
          }
        } catch (IOException e) {
          LOG.warn("update peer storage failed, retry", e);
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
        postPeerModification();
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
    return PeerModificationState.UPDATE_PEER_STORAGE;
  }
}
