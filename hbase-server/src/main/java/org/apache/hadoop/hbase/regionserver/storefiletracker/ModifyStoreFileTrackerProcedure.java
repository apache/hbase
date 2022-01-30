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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.procedure.AbstractStateMachineTableProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ModifyTableProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ModifyStoreFileTrackerState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ModifyStoreFileTrackerStateData;

/**
 * This procedure is used to change the store file tracker implementation.
 * <p/>
 * Typically we need to schedule two {@link ModifyTableProcedure} (or three if the table is already
 * in {@code MIGRATION} but the {@code dstSFT} is not what we expected) to do this, so we introduce
 * this procedure to simplify the work of our users.
 */
@InterfaceAudience.Private
public abstract class ModifyStoreFileTrackerProcedure
  extends AbstractStateMachineTableProcedure<ModifyStoreFileTrackerState> {

  private static final Logger LOG = LoggerFactory.getLogger(ModifyStoreFileTrackerProcedure.class);

  private TableName tableName;

  private String dstSFT;

  protected ModifyStoreFileTrackerProcedure() {
  }

  protected ModifyStoreFileTrackerProcedure(MasterProcedureEnv env, TableName tableName,
    String dstSFT) throws HBaseIOException {
    super(env);
    checkDstSFT(dstSFT);
    this.tableName = tableName;
    this.dstSFT = dstSFT;
    preflightChecks(env, true);
  }

  private void checkDstSFT(String dstSFT) throws DoNotRetryIOException {
    if (MigrationStoreFileTracker.class
      .isAssignableFrom(StoreFileTrackerFactory.getTrackerClass(dstSFT))) {
      throw new DoNotRetryIOException("Do not need to transfer to " + dstSFT);
    }
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.EDIT;
  }

  private enum StoreFileTrackerState {
    NEED_FINISH_PREVIOUS_MIGRATION_FIRST, NEED_START_MIGRATION, NEED_FINISH_MIGRATION,
    ALREADY_FINISHED
  }

  private StoreFileTrackerState checkState(Configuration conf, String dstSFT) {
    // there are 4 possible conditions:
    // 1. The table or family has already made use of the dstSFT. In this way we just finish the
    // procedure.
    // 2. The table or family is not using the dstSFT but also not using migration SFT,
    // then we just go to the MODIFY_STORE_FILE_TRACKER_MIGRATION state.
    // 3. The table or family has already been using migration SFT and the dst SFT is what we
    // expect, just go to MODIFY_STORE_FILE_TRACKER_FINISH.
    // 4. The table or family is using migration SFT and the dst SFT is not what we
    // expect, then need to schedule a MTP to change it to the dst SFT of the current migration
    // SFT first, and then go to MODIFY_STORE_FILE_TRACKER_MIGRATION.
    Class<? extends StoreFileTracker> clazz = StoreFileTrackerFactory.getTrackerClass(conf);
    Class<? extends StoreFileTracker> dstSFTClass = StoreFileTrackerFactory.getTrackerClass(dstSFT);
    if (clazz.equals(dstSFTClass)) {
      return StoreFileTrackerState.ALREADY_FINISHED;
    }
    if (!MigrationStoreFileTracker.class.isAssignableFrom(clazz)) {
      return StoreFileTrackerState.NEED_START_MIGRATION;
    }
    Class<? extends StoreFileTracker> currentDstSFT = StoreFileTrackerFactory
      .getStoreFileTrackerClassForMigration(conf, MigrationStoreFileTracker.DST_IMPL);
    if (currentDstSFT.equals(dstSFTClass)) {
      return StoreFileTrackerState.NEED_FINISH_MIGRATION;
    } else {
      return StoreFileTrackerState.NEED_FINISH_PREVIOUS_MIGRATION_FIRST;
    }
  }

  private final String getRestoreSFT(Configuration conf) {
    Class<? extends StoreFileTracker> currentDstSFT = StoreFileTrackerFactory
      .getStoreFileTrackerClassForMigration(conf, MigrationStoreFileTracker.DST_IMPL);
    return StoreFileTrackerFactory.getStoreFileTrackerName(currentDstSFT);
  }

  protected abstract void preCheck(TableDescriptor current) throws IOException;

  protected abstract Configuration createConf(Configuration conf, TableDescriptor current);

  protected abstract TableDescriptor createRestoreTableDescriptor(TableDescriptor current,
    String restoreSFT);

  private Flow preCheckAndTryRestoreSFT(MasterProcedureEnv env) throws IOException {
    // Checks whether the table exists
    if (!env.getMasterServices().getTableDescriptors().exists(getTableName())) {
      throw new TableNotFoundException(getTableName());
    }
    if (!isTableEnabled(env)) {
      throw new TableNotEnabledException(tableName);
    }
    TableDescriptor current = env.getMasterServices().getTableDescriptors().get(tableName);
    preCheck(current);
    Configuration conf = createConf(env.getMasterConfiguration(), current);
    StoreFileTrackerState state = checkState(conf, dstSFT);
    switch (state) {
      case NEED_FINISH_PREVIOUS_MIGRATION_FIRST:
        TableDescriptor td = createRestoreTableDescriptor(current, getRestoreSFT(conf));
        addChildProcedure(new ModifyTableProcedure(env, td));
        setNextState(
          ModifyStoreFileTrackerState.MODIFY_STORE_FILE_TRACKER_FINISH_PREVIOUS_MIGRATION);
        return Flow.HAS_MORE_STATE;
      case NEED_START_MIGRATION:
        setNextState(ModifyStoreFileTrackerState.MODIFY_STORE_FILE_TRACKER_START_MIGRATION);
        return Flow.HAS_MORE_STATE;
      case NEED_FINISH_MIGRATION:
        setNextState(ModifyStoreFileTrackerState.MODIFY_STORE_FILE_TRACKER_FINISH_MIGRATION);
        return Flow.HAS_MORE_STATE;
      case ALREADY_FINISHED:
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  protected abstract TableDescriptor createMigrationTableDescriptor(Configuration conf,
    TableDescriptor current);

  protected final void migrate(Configuration conf, BiConsumer<String, String> setValue) {
    setValue.accept(StoreFileTrackerFactory.TRACKER_IMPL,
      StoreFileTrackerFactory.Trackers.MIGRATION.name());
    setValue.accept(MigrationStoreFileTracker.SRC_IMPL,
      StoreFileTrackerFactory.getStoreFileTrackerName(conf));
    setValue.accept(MigrationStoreFileTracker.DST_IMPL, dstSFT);
  }

  protected abstract TableDescriptor createFinishTableDescriptor(TableDescriptor current);

  protected final void finish(BiConsumer<String, String> setValue, Consumer<String> removeValue) {
    setValue.accept(StoreFileTrackerFactory.TRACKER_IMPL, dstSFT);
    removeValue.accept(MigrationStoreFileTracker.SRC_IMPL);
    removeValue.accept(MigrationStoreFileTracker.DST_IMPL);
  }

  private void migrate(MasterProcedureEnv env) throws IOException {
    TableDescriptor current = env.getMasterServices().getTableDescriptors().get(tableName);
    TableDescriptor td = createMigrationTableDescriptor(env.getMasterConfiguration(), current);
    addChildProcedure(new ModifyTableProcedure(env, td));
    setNextState(ModifyStoreFileTrackerState.MODIFY_STORE_FILE_TRACKER_FINISH_MIGRATION);
  }

  private void finish(MasterProcedureEnv env) throws IOException {
    TableDescriptor current = env.getMasterServices().getTableDescriptors().get(tableName);
    TableDescriptor td = createFinishTableDescriptor(current);
    addChildProcedure(new ModifyTableProcedure(env, td));
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, ModifyStoreFileTrackerState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    try {
      switch (state) {
        case MODIFY_STORE_FILE_TRACKER_FINISH_PREVIOUS_MIGRATION:
          return preCheckAndTryRestoreSFT(env);
        case MODIFY_STORE_FILE_TRACKER_START_MIGRATION:
          migrate(env);
          return Flow.HAS_MORE_STATE;
        case MODIFY_STORE_FILE_TRACKER_FINISH_MIGRATION:
          finish(env);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      if (isRollbackSupported(state)) {
        setFailure("master-modify-SFT", e);
      } else {
        LOG.warn("Retriable error trying to modify SFT for table={} (in state={})", getTableName(),
          state, e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, ModifyStoreFileTrackerState state)
    throws IOException, InterruptedException {
    if (isRollbackSupported(state)) {
      return;
    }
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected ModifyStoreFileTrackerState getState(int stateId) {
    return ModifyStoreFileTrackerState.forNumber(stateId);
  }

  @Override
  protected int getStateId(ModifyStoreFileTrackerState state) {
    return state.getNumber();
  }

  @Override
  protected ModifyStoreFileTrackerState getInitialState() {
    return ModifyStoreFileTrackerState.MODIFY_STORE_FILE_TRACKER_FINISH_PREVIOUS_MIGRATION;
  }

  @Override
  protected boolean isRollbackSupported(ModifyStoreFileTrackerState state) {
    return state == ModifyStoreFileTrackerState.MODIFY_STORE_FILE_TRACKER_FINISH_PREVIOUS_MIGRATION;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    serializer.serialize(ModifyStoreFileTrackerStateData.newBuilder()
      .setTableName(ProtobufUtil.toProtoTableName(tableName)).setDstSft(dstSFT).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    ModifyStoreFileTrackerStateData data =
      serializer.deserialize(ModifyStoreFileTrackerStateData.class);
    this.tableName = ProtobufUtil.toTableName(data.getTableName());
    this.dstSFT = data.getDstSft();
  }
}
