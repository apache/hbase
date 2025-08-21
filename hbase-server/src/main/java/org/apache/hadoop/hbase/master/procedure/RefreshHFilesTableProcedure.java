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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshHFilesTableProcedureState;

@InterfaceAudience.Private
public class RefreshHFilesTableProcedure
  extends AbstractStateMachineTableProcedure<RefreshHFilesTableProcedureState> {
  private static final Logger LOG = LoggerFactory.getLogger(RefreshHFilesTableProcedure.class);

  private TableName tableName;
  private String namespaceName;

  public RefreshHFilesTableProcedure() {
    super();
  }

  public RefreshHFilesTableProcedure(MasterProcedureEnv env) {
    super(env);
  }

  public RefreshHFilesTableProcedure(MasterProcedureEnv env, TableName tableName) {
    super(env);
    this.tableName = tableName;
  }

  public RefreshHFilesTableProcedure(MasterProcedureEnv env, String namespaceName) {
    super(env);
    this.namespaceName = namespaceName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REFRESH_HFILES;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    MasterProcedureProtos.RefreshHFilesTableProcedureStateData.Builder builder = MasterProcedureProtos.RefreshHFilesTableProcedureStateData.newBuilder();
    if (tableName != null && namespaceName == null){
      builder.setTableName(ProtobufUtil.toProtoTableName(tableName));
    }
    else if(tableName == null && namespaceName != null){
      builder.setNamespaceName(namespaceName);
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    MasterProcedureProtos.RefreshHFilesTableProcedureStateData
      data = serializer.deserialize(MasterProcedureProtos.RefreshHFilesTableProcedureStateData.class);
    if (data.hasTableName() && !data.hasNamespaceName()){
      this.tableName = ProtobufUtil.toTableName(data.getTableName());
    }
    else if(!data.hasTableName() && data.hasNamespaceName()){
      this.namespaceName = data.getNamespaceName();
    }
  }

  @Override
  public TableName getTableName() {
    if (tableName != null && namespaceName == null){
      return tableName;
    }
      return DUMMY_NAMESPACE_TABLE_NAME;
  }

  @Override
  protected RefreshHFilesTableProcedureState getInitialState() {
    return RefreshHFilesTableProcedureState.REFRESH_HFILES_PREPARE;
  }

  @Override
  protected int getStateId(RefreshHFilesTableProcedureState state) {
    return state.getNumber();
  }

  @Override
  protected RefreshHFilesTableProcedureState getState(int stateId) {
    return RefreshHFilesTableProcedureState.forNumber(stateId);
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env,
    RefreshHFilesTableProcedureState state)
    throws IOException, InterruptedException {
    // Refresh HFiles is idempotent operation hence rollback is not needed
    LOG.trace("Rollback not implemented for RefreshHFilesTableProcedure state: {}",
      state);
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env,
    RefreshHFilesTableProcedureState state) {
    LOG.info("Executing RefreshHFilesTableProcedureState state: {}",
      state);

    try {
      return switch (state) {
        case REFRESH_HFILES_PREPARE -> prepare(env);
        case REFRESH_HFILES_REFRESH_REGION -> refreshHFiles(env);
        case REFRESH_HFILES_FINISH -> finish();
        default -> throw new UnsupportedOperationException(
          "Unhandled state: " + state);
      };
    } catch (Exception ex) {
      LOG.error("Error in RefreshHFilesTableProcedure state {}", state,
        ex);
      setFailure("RefreshHFilesTableProcedure", ex);
      return Flow.NO_MORE_STATE;
    }
  }

  private Flow prepare(final MasterProcedureEnv env) {
    // TODO Check if table exists otherwise send exception.
    // TODO verify if region is alive or not
    setNextState(RefreshHFilesTableProcedureState.REFRESH_HFILES_REFRESH_REGION);
    return Flow.HAS_MORE_STATE;
  }

  private void refreshHFilesForTable(final MasterProcedureEnv env, TableName tableName){
    addChildProcedure(env.getAssignmentManager().getTableRegions(tableName, true).stream()
      .map(r -> new RefreshHFilesRegionProcedure(r)).toArray(RefreshHFilesRegionProcedure[]::new));
  }

  private Flow refreshHFiles(final MasterProcedureEnv env) throws IOException {
    if (tableName != null && namespaceName == null){
      refreshHFilesForTable(env, tableName);
    }
    else if(tableName == null && namespaceName != null){
      final List<TableName> tables = env.getMasterServices().listTableNamesByNamespace(namespaceName);
      for (TableName table : tables){
        refreshHFilesForTable(env, table);
      }
    }
    else{
      final List<TableName> tables = env.getMasterServices().getTableDescriptors().getAll().values().stream().map(TableDescriptor::getTableName).toList();
      for (TableName table : tables){
        if(!table.isSystemTable()){
          refreshHFilesForTable(env, table);
        }
      }
    }

    setNextState(RefreshHFilesTableProcedureState.REFRESH_HFILES_FINISH);
    return Flow.HAS_MORE_STATE;
  }

  private Flow finish() {
    return Flow.NO_MORE_STATE;
  }
}
