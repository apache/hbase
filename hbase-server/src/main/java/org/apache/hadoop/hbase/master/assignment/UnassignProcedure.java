/**
 *
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
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionTransitionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.UnassignRegionStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * Leave here only for checking if we can successfully start the master.
 * @deprecated Do not use any more.
 * @see TransitRegionStateProcedure
 */
@Deprecated
@InterfaceAudience.Private
public class UnassignProcedure extends RegionTransitionProcedure {

  protected volatile ServerName hostingServer;

  protected volatile ServerName destinationServer;

  private boolean removeAfterUnassigning;

  public UnassignProcedure() {
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_UNASSIGN;
  }

  @Override
  protected boolean isRollbackSupported(final RegionTransitionState state) {
    switch (state) {
      case REGION_TRANSITION_QUEUE:
      case REGION_TRANSITION_DISPATCH:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    UnassignRegionStateData.Builder state =
      UnassignRegionStateData.newBuilder().setTransitionState(getTransitionState())
        .setHostingServer(ProtobufUtil.toServerName(this.hostingServer))
        .setRegionInfo(ProtobufUtil.toRegionInfo(getRegionInfo()));
    if (this.destinationServer != null) {
      state.setDestinationServer(ProtobufUtil.toServerName(destinationServer));
    }
    if (removeAfterUnassigning) {
      state.setRemoveAfterUnassigning(true);
    }
    if (getAttempt() > 0) {
      state.setAttempt(getAttempt());
    }
    serializer.serialize(state.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    final UnassignRegionStateData state = serializer.deserialize(UnassignRegionStateData.class);
    setTransitionState(state.getTransitionState());
    setRegionInfo(ProtobufUtil.toRegionInfo(state.getRegionInfo()));
    this.hostingServer = ProtobufUtil.toServerName(state.getHostingServer());
    if (state.hasDestinationServer()) {
      this.destinationServer = ProtobufUtil.toServerName(state.getDestinationServer());
    }
    removeAfterUnassigning = state.getRemoveAfterUnassigning();
    if (state.hasAttempt()) {
      setAttempt(state.getAttempt());
    }
  }

  @Override
  protected boolean startTransition(final MasterProcedureEnv env,
      final RegionStateNode regionNode) {
    // nothing to do here. we skip the step in the constructor
    // by jumping to REGION_TRANSITION_DISPATCH
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean updateTransition(final MasterProcedureEnv env, final RegionStateNode regionNode)
      throws IOException {
    return true;
  }

  @Override
  protected void finishTransition(final MasterProcedureEnv env, final RegionStateNode regionNode)
      throws IOException {
  }

  @Override
  public Optional<RemoteOperation> remoteCallBuild(final MasterProcedureEnv env,
      final ServerName serverName) {
    return Optional.empty();
  }

  @Override
  protected void reportTransition(final MasterProcedureEnv env, final RegionStateNode regionNode,
      final TransitionCode code, final long seqId) throws UnexpectedStateException {
  }

  /**
   * @return If true, we will re-wake up this procedure; if false, the procedure stays suspended.
   */
  @Override
  protected boolean remoteCallFailed(final MasterProcedureEnv env, final RegionStateNode regionNode,
      final IOException exception) {
    return true;
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    super.toStringClassDetails(sb);
    sb.append(", server=").append(this.hostingServer);
  }

  @Override
  protected ProcedureMetrics getProcedureMetrics(MasterProcedureEnv env) {
    return env.getAssignmentManager().getAssignmentManagerMetrics().getUnassignProcMetrics();
  }
}
