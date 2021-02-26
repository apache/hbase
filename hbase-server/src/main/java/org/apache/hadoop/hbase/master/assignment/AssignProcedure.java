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
package org.apache.hadoop.hbase.master.assignment;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AssignRegionStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionTransitionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Leave here only for checking if we can successfully start the master.
 * @deprecated Do not use any more.
 * @see TransitRegionStateProcedure
 */
@Deprecated
@InterfaceAudience.Private
public class AssignProcedure extends RegionTransitionProcedure {

  private boolean forceNewPlan = false;

  protected volatile ServerName targetServer;

  public AssignProcedure() {
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_ASSIGN;
  }

  @Override
  protected boolean isRollbackSupported(final RegionTransitionState state) {
    switch (state) {
      case REGION_TRANSITION_QUEUE:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    final AssignRegionStateData.Builder state =
      AssignRegionStateData.newBuilder().setTransitionState(getTransitionState())
        .setRegionInfo(ProtobufUtil.toRegionInfo(getRegionInfo()));
    if (forceNewPlan) {
      state.setForceNewPlan(true);
    }
    if (this.targetServer != null) {
      state.setTargetServer(ProtobufUtil.toServerName(this.targetServer));
    }
    if (getAttempt() > 0) {
      state.setAttempt(getAttempt());
    }
    serializer.serialize(state.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    final AssignRegionStateData state = serializer.deserialize(AssignRegionStateData.class);
    setTransitionState(state.getTransitionState());
    setRegionInfo(ProtobufUtil.toRegionInfo(state.getRegionInfo()));
    forceNewPlan = state.getForceNewPlan();
    if (state.hasTargetServer()) {
      this.targetServer = ProtobufUtil.toServerName(state.getTargetServer());
    }
    if (state.hasAttempt()) {
      setAttempt(state.getAttempt());
    }
  }

  @Override
  protected boolean startTransition(final MasterProcedureEnv env, final RegionStateNode regionNode)
      throws IOException {
    return true;
  }

  @Override
  protected boolean updateTransition(final MasterProcedureEnv env, final RegionStateNode regionNode)
      throws IOException, ProcedureSuspendedException {
    return true;
  }

  @Override
  protected void finishTransition(final MasterProcedureEnv env, final RegionStateNode regionNode)
      throws IOException {
  }

  @Override
  protected void reportTransition(final MasterProcedureEnv env, final RegionStateNode regionNode,
      final TransitionCode code, final long openSeqNum) throws UnexpectedStateException {
  }

  @Override
  public Optional<RemoteOperation> remoteCallBuild(final MasterProcedureEnv env,
      final ServerName serverName) {
    return Optional.empty();
  }

  @Override
  protected boolean remoteCallFailed(final MasterProcedureEnv env, final RegionStateNode regionNode,
      final IOException exception) {
    return true;
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    super.toStringClassDetails(sb);
    if (this.targetServer != null) sb.append(", target=").append(this.targetServer);
  }

  @Override
  protected ProcedureMetrics getProcedureMetrics(MasterProcedureEnv env) {
    return env.getAssignmentManager().getAssignmentManagerMetrics().getAssignProcMetrics();
  }

  @Override
  public void setProcId(long procId) {
    super.setProcId(procId);
  }
}
