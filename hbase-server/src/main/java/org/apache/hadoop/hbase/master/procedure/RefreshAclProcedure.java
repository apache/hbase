/*
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
package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.security.access.ACLManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AclEntryUpdate;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshAclState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshAclStateData;

/**
 * Broadcasts one ACL change set to all registered region servers, as one
 * {@link RefreshAclRemoteProcedure} child per server. At most one instance of this procedure runs
 * at a time, see {@link ACLManager}: updates that arrive while it runs are merged into the next
 * one.
 * <p>
 * A change set is persisted in the procedure store before it is dispatched, so it survives a master
 * failover and is replayed at-least-once; stale replays are dropped by the timestamp watermark on
 * the servers.
 * </p>
 */
@InterfaceAudience.Private
public class RefreshAclProcedure extends StateMachineProcedure<MasterProcedureEnv, RefreshAclState>
  implements GlobalProcedureInterface {

  private static final Logger LOG = LoggerFactory.getLogger(RefreshAclProcedure.class);

  private List<AclEntryUpdate> updates;
  private boolean full;
  private long ts;

  /** For the single-flight incremental push: drains {@link ACLManager} at the first step. */
  public RefreshAclProcedure() {
  }

  /** For the full round pushed when a master becomes active. */
  public RefreshAclProcedure(List<AclEntryUpdate> updates, boolean full, long ts) {
    this.updates = updates;
    this.full = full;
    this.ts = ts;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, RefreshAclState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    switch (state) {
      case REFRESH_ACL_DRAIN_PENDING: {
        if (updates == null) {
          ACLManager.ChangeSet changeSet = env.getMasterServices().getACLManager().drainPending();
          if (changeSet == null) {
            // nothing to push, e.g. drained by a previous instance after a failover recovery
            return Flow.NO_MORE_STATE;
          }
          updates = changeSet.getUpdates();
          full = changeSet.isFull();
          ts = changeSet.getTs();
        }
        setNextState(RefreshAclState.REFRESH_ACL_DISPATCH);
        return Flow.HAS_MORE_STATE;
      }
      case REFRESH_ACL_DISPATCH: {
        RefreshAclRemoteProcedure[] subProcedures =
          env.getMasterServices().getServerManager().getOnlineServersList().stream()
            .map(sn -> new RefreshAclRemoteProcedure(sn, updates, full, ts))
            .toArray(RefreshAclRemoteProcedure[]::new);
        if (subProcedures.length > 0) {
          addChildProcedure(subProcedures);
        }
        setNextState(RefreshAclState.REFRESH_ACL_WAIT);
        return Flow.HAS_MORE_STATE;
      }
      case REFRESH_ACL_WAIT:
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  @Override
  protected void completionCleanup(MasterProcedureEnv env) {
    // release the single-flight gate and submit the next push when updates piled up meanwhile
    env.getMasterServices().getACLManager().onPushComplete();
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, RefreshAclState state)
    throws IOException, InterruptedException {
  }

  @Override
  protected RefreshAclState getState(int stateId) {
    return RefreshAclState.forNumber(stateId);
  }

  @Override
  protected int getStateId(RefreshAclState state) {
    return state.getNumber();
  }

  @Override
  protected RefreshAclState getInitialState() {
    return RefreshAclState.REFRESH_ACL_DRAIN_PENDING;
  }

  @Override
  protected RefreshAclState getCurrentState() {
    return super.getCurrentState();
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    RefreshAclStateData.Builder builder = RefreshAclStateData.newBuilder().setFull(full).setTs(ts);
    if (updates != null) {
      builder.addAllUpdates(updates);
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    RefreshAclStateData data = serializer.deserialize(RefreshAclStateData.class);
    // an empty list means the drain step has not run yet (recovered before it), re-drain then
    updates = (data.getUpdatesCount() == 0 && !data.getFull()) ? null : data.getUpdatesList();
    full = data.getFull();
    ts = data.getTs();
  }

  @Override
  public String getGlobalId() {
    return getClass().getSimpleName();
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" ts=");
    sb.append(ts);
    sb.append(", full=");
    sb.append(full);
    sb.append(", updates=");
    sb.append(updates != null ? updates.size() : 0);
  }
}
