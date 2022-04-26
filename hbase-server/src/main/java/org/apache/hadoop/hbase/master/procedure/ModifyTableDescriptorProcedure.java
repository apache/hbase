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
import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ModifyTableDescriptorState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ModifyTableDescriptorStateData;

/**
 * The procedure will only update the table descriptor without reopening all the regions.
 * <p/>
 * It is usually used for migrating when upgrading, where we need to add something into the table
 * descriptor, such as the rs group information.
 */
@InterfaceAudience.Private
public abstract class ModifyTableDescriptorProcedure
  extends AbstractStateMachineTableProcedure<ModifyTableDescriptorState> {

  private static final Logger LOG = LoggerFactory.getLogger(ModifyTableDescriptorProcedure.class);

  private TableName tableName;

  private TableDescriptor modifiedTableDescriptor;

  protected ModifyTableDescriptorProcedure() {
  }

  protected ModifyTableDescriptorProcedure(MasterProcedureEnv env, TableName tableName) {
    super(env);
    this.tableName = Objects.requireNonNull(tableName);
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.EDIT;
  }

  /**
   * Sub class should implement this method to modify the table descriptor, such as storing the rs
   * group information.
   * <p/>
   * Since the migrating is asynchronouns, it is possible that users have already changed the rs
   * group for a table, in this case we do not need to modify the table descriptor any more, then
   * you could just return {@link Optional#empty()}.
   */
  protected abstract Optional<TableDescriptor> modify(MasterProcedureEnv env,
    TableDescriptor current) throws IOException;

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, ModifyTableDescriptorState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    try {
      switch (state) {
        case MODIFY_TABLE_DESCRIPTOR_PREPARE:
          TableDescriptor current = env.getMasterServices().getTableDescriptors().get(tableName);
          if (current == null) {
            LOG.info("Table {} does not exist, skip modifying", tableName);
            return Flow.NO_MORE_STATE;
          }
          Optional<TableDescriptor> modified = modify(env, current);
          if (modified.isPresent()) {
            modifiedTableDescriptor = modified.get();
            setNextState(ModifyTableDescriptorState.MODIFY_TABLE_DESCRIPTOR_UPDATE);
            return Flow.HAS_MORE_STATE;
          } else {
            // do not need to modify
            return Flow.NO_MORE_STATE;
          }
        case MODIFY_TABLE_DESCRIPTOR_UPDATE:
          env.getMasterServices().getTableDescriptors().update(modifiedTableDescriptor);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      if (isRollbackSupported(state)) {
        setFailure("master-modify-table-descriptor", e);
      } else {
        LOG.warn("Retriable error trying to modify table descriptor={} (in state={})",
          getTableName(), state, e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected boolean holdLock(MasterProcedureEnv env) {
    // here we want to make sure that our modification result will not be overwrite by other MTPs,
    // so we set holdLock to true. Since we do not need to schedule any sub procedures, especially
    // no remote procedures, so it is OK for us a hold the lock all the time, it will not hurt the
    // availability too much.
    return true;
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, ModifyTableDescriptorState state)
    throws IOException, InterruptedException {
    if (state == ModifyTableDescriptorState.MODIFY_TABLE_DESCRIPTOR_PREPARE) {
      return;
    }
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected boolean isRollbackSupported(ModifyTableDescriptorState state) {
    return state == ModifyTableDescriptorState.MODIFY_TABLE_DESCRIPTOR_PREPARE;
  }

  @Override
  protected ModifyTableDescriptorState getState(int stateId) {
    return ModifyTableDescriptorState.forNumber(stateId);
  }

  @Override
  protected int getStateId(ModifyTableDescriptorState state) {
    return state.getNumber();
  }

  @Override
  protected ModifyTableDescriptorState getInitialState() {
    return ModifyTableDescriptorState.MODIFY_TABLE_DESCRIPTOR_PREPARE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    ModifyTableDescriptorStateData.Builder builder = ModifyTableDescriptorStateData.newBuilder()
      .setTableName(ProtobufUtil.toProtoTableName(tableName));
    if (modifiedTableDescriptor != null) {
      builder.setModifiedTableSchema(ProtobufUtil.toTableSchema(modifiedTableDescriptor));
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    ModifyTableDescriptorStateData data =
      serializer.deserialize(ModifyTableDescriptorStateData.class);
    tableName = ProtobufUtil.toTableName(data.getTableName());
    if (data.hasModifiedTableSchema()) {
      modifiedTableDescriptor = ProtobufUtil.toTableDescriptor(data.getModifiedTableSchema());
    }
  }
}
