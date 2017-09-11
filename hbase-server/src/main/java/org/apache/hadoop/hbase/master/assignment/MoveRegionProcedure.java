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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.procedure.AbstractStateMachineRegionProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MoveRegionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MoveRegionStateData;

/**
 * Procedure that implements a RegionPlan.
 * It first runs an unassign subprocedure followed
 * by an assign subprocedure. It takes a lock on the region being moved.
 * It holds the lock for the life of the procedure.
 */
@InterfaceAudience.Private
public class MoveRegionProcedure extends AbstractStateMachineRegionProcedure<MoveRegionState> {
  private static final Log LOG = LogFactory.getLog(MoveRegionProcedure.class);
  private RegionPlan plan;

  public MoveRegionProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    super();
  }

  public MoveRegionProcedure(final MasterProcedureEnv env, final RegionPlan plan) {
    super(env, plan.getRegionInfo());
    this.plan = plan;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final MoveRegionState state)
      throws InterruptedException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }
    switch (state) {
      case MOVE_REGION_UNASSIGN:
        addChildProcedure(new UnassignProcedure(plan.getRegionInfo(), plan.getSource(), true));
        setNextState(MoveRegionState.MOVE_REGION_ASSIGN);
        break;
      case MOVE_REGION_ASSIGN:
        AssignProcedure assignProcedure = plan.getDestination() == null ?
            new AssignProcedure(plan.getRegionInfo(), true) :
            new AssignProcedure(plan.getRegionInfo(), plan.getDestination());
        addChildProcedure(assignProcedure);
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
    return Flow.HAS_MORE_STATE;
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
    return MoveRegionState.valueOf(stateId);
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
        // No need to serialize the HRegionInfo. The super class has the region.
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
    final HRegionInfo regionInfo = getRegion(); // Get it from super class deserialization.
    final ServerName sourceServer = ProtobufUtil.toServerName(state.getSourceServer());
    final ServerName destinationServer = state.hasDestinationServer() ?
        ProtobufUtil.toServerName(state.getDestinationServer()) : null;
    this.plan = new RegionPlan(regionInfo, sourceServer, destinationServer);
  }
}
