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
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.CloseTableRegionsProcedureState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.CloseTableRegionsProcedureStateData;

/**
 * Procedure for closing all regions for a table.
 */
@InterfaceAudience.Private
public class CloseTableRegionsProcedure
  extends AbstractCloseTableRegionsProcedure<CloseTableRegionsProcedureState> {

  public CloseTableRegionsProcedure() {
  }

  public CloseTableRegionsProcedure(TableName tableName) {
    super(tableName);
  }

  @Override
  protected int submitUnassignProcedure(MasterProcedureEnv env,
    Consumer<TransitRegionStateProcedure> submit) {
    return env.getAssignmentManager().submitUnassignProcedureForDisablingTable(tableName, submit);
  }

  @Override
  protected int numberOfUnclosedRegions(MasterProcedureEnv env) {
    return env.getAssignmentManager().numberOfUnclosedRegionsForDisabling(tableName);
  }

  @Override
  protected CloseTableRegionsProcedureState getState(int stateId) {
    return CloseTableRegionsProcedureState.forNumber(stateId);
  }

  @Override
  protected int getStateId(CloseTableRegionsProcedureState state) {
    return state.getNumber();
  }

  @Override
  protected CloseTableRegionsProcedureState getInitialState() {
    return CloseTableRegionsProcedureState.CLOSE_TABLE_REGIONS_SCHEDULE;
  }

  @Override
  protected CloseTableRegionsProcedureState getConfirmState() {
    return CloseTableRegionsProcedureState.CLOSE_TABLE_REGIONS_CONFIRM;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    CloseTableRegionsProcedureStateData data = CloseTableRegionsProcedureStateData.newBuilder()
      .setTableName(ProtobufUtil.toProtoTableName(tableName)).build();
    serializer.serialize(data);
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    CloseTableRegionsProcedureStateData data =
      serializer.deserialize(CloseTableRegionsProcedureStateData.class);
    tableName = ProtobufUtil.toTableName(data.getTableName());
  }
}
