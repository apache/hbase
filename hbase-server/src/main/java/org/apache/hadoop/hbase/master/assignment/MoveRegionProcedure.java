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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.procedure.AbstractStateMachineRegionProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MoveRegionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MoveRegionStateData;

/**
 * Leave here only for checking if we can successfully start the master.
 * @deprecated Do not use any more.
 * @see TransitRegionStateProcedure
 */
@Deprecated
@InterfaceAudience.Private
public class MoveRegionProcedure extends AbstractStateMachineRegionProcedure<MoveRegionState> {
  private RegionPlan plan;

  public MoveRegionProcedure() {
    super();
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final MoveRegionState state)
      throws InterruptedException {
    return Flow.NO_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final MoveRegionState state)
      throws IOException {
    // no-op
  }

  @Override
  public boolean abort(final MasterProcedureEnv env) {
    return false;
  }

  @Override
  public void toStringClassDetails(final StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" ");
    sb.append(plan);
  }

  @Override
  protected MoveRegionState getInitialState() {
    return MoveRegionState.MOVE_REGION_UNASSIGN;
  }

  @Override
  protected int getStateId(final MoveRegionState state) {
    return state.getNumber();
  }

  @Override
  protected MoveRegionState getState(final int stateId) {
    return MoveRegionState.forNumber(stateId);
  }

  @Override
  public TableName getTableName() {
    return plan.getRegionInfo().getTable();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_EDIT;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.serializeStateData(serializer);

    final MoveRegionStateData.Builder state = MoveRegionStateData.newBuilder()
        // No need to serialize the RegionInfo. The super class has the region.
        .setSourceServer(ProtobufUtil.toServerName(plan.getSource()));
    if (plan.getDestination() != null) {
      state.setDestinationServer(ProtobufUtil.toServerName(plan.getDestination()));
    }

    serializer.serialize(state.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.deserializeStateData(serializer);

    final MoveRegionStateData state = serializer.deserialize(MoveRegionStateData.class);
    final RegionInfo regionInfo = getRegion(); // Get it from super class deserialization.
    final ServerName sourceServer = ProtobufUtil.toServerName(state.getSourceServer());
    final ServerName destinationServer = state.hasDestinationServer() ?
        ProtobufUtil.toServerName(state.getDestinationServer()) : null;
    this.plan = new RegionPlan(regionInfo, sourceServer, destinationServer);
  }
}
