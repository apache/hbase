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
import java.util.function.Consumer;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.CloseExcessRegionReplicasProcedureState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.CloseExcessRegionReplicasProcedureStateData;

/**
 * Procedure for close excess region replicas.
 */
@InterfaceAudience.Private
public class CloseExcessRegionReplicasProcedure
  extends AbstractCloseTableRegionsProcedure<CloseExcessRegionReplicasProcedureState> {

  private int newReplicaCount;

  public CloseExcessRegionReplicasProcedure() {
  }

  public CloseExcessRegionReplicasProcedure(TableName tableName, int newReplicaCount) {
    super(tableName);
    this.newReplicaCount = newReplicaCount;
  }

  @Override
  protected CloseExcessRegionReplicasProcedureState getState(int stateId) {
    return CloseExcessRegionReplicasProcedureState.forNumber(stateId);
  }

  @Override
  protected int getStateId(CloseExcessRegionReplicasProcedureState state) {
    return state.getNumber();
  }

  @Override
  protected CloseExcessRegionReplicasProcedureState getInitialState() {
    return CloseExcessRegionReplicasProcedureState.CLOSE_EXCESS_REGION_REPLICAS_SCHEDULE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    CloseExcessRegionReplicasProcedureStateData data = CloseExcessRegionReplicasProcedureStateData
      .newBuilder().setTableName(ProtobufUtil.toProtoTableName(tableName))
      .setNewReplicaCount(newReplicaCount).build();
    serializer.serialize(data);
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    CloseExcessRegionReplicasProcedureStateData data =
      serializer.deserialize(CloseExcessRegionReplicasProcedureStateData.class);
    tableName = ProtobufUtil.toTableName(data.getTableName());
    newReplicaCount = data.getNewReplicaCount();
  }

  @Override
  protected CloseExcessRegionReplicasProcedureState getConfirmState() {
    return CloseExcessRegionReplicasProcedureState.CLOSE_EXCESS_REGION_REPLICAS_CONFIRM;
  }

  @Override
  protected int submitUnassignProcedure(MasterProcedureEnv env,
    Consumer<TransitRegionStateProcedure> submit) {
    return env.getAssignmentManager()
      .submitUnassignProcedureForClosingExcessRegionReplicas(tableName, newReplicaCount, submit);
  }

  @Override
  protected int numberOfUnclosedRegions(MasterProcedureEnv env) {
    return env.getAssignmentManager().numberOfUnclosedExcessRegionReplicas(tableName,
      newReplicaCount);
  }

}
