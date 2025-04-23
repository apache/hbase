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
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.procedure.flush.MasterFlushTableProcedureManager;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.FlushTableProcedureStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.FlushTableState;

@InterfaceAudience.Private
public class FlushTableProcedure extends AbstractStateMachineTableProcedure<FlushTableState> {
  private static final Logger LOG = LoggerFactory.getLogger(FlushTableProcedure.class);

  private TableName tableName;

  private List<byte[]> columnFamilies;

  public FlushTableProcedure() {
    super();
  }

  public FlushTableProcedure(MasterProcedureEnv env, TableName tableName) {
    this(env, tableName, null);
  }

  public FlushTableProcedure(MasterProcedureEnv env, TableName tableName,
    List<byte[]> columnFamilies) {
    super(env);
    this.tableName = tableName;
    this.columnFamilies = columnFamilies;
  }

  @Override
  protected LockState acquireLock(MasterProcedureEnv env) {
    // Here we don't acquire table lock because the flush operation and other operations (like
    // split or merge) are not mutually exclusive. Region will flush memstore when being closed.
    // It's safe even if we don't have lock. However, currently we are limited by the scheduling
    // mechanism of the procedure scheduler and have to acquire table shared lock here. See
    // HBASE-27905 for details.
    if (env.getProcedureScheduler().waitTableSharedLock(this, getTableName())) {
      return LockState.LOCK_EVENT_WAIT;
    }
    return LockState.LOCK_ACQUIRED;
  }

  @Override
  protected void releaseLock(MasterProcedureEnv env) {
    env.getProcedureScheduler().wakeTableSharedLock(this, getTableName());
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, FlushTableState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    LOG.info("{} execute state={}", this, state);

    try {
      switch (state) {
        case FLUSH_TABLE_PREPARE:
          preflightChecks(env, true);
          setNextState(FlushTableState.FLUSH_TABLE_FLUSH_REGIONS);
          return Flow.HAS_MORE_STATE;
        case FLUSH_TABLE_FLUSH_REGIONS:
          addChildProcedure(createFlushRegionProcedures(env));
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (Exception e) {
      if (e instanceof DoNotRetryIOException) {
        // for example, TableNotFoundException or TableNotEnabledException
        setFailure("master-flush-table", e);
        LOG.warn("Unrecoverable error trying to flush " + getTableName() + " state=" + state, e);
      } else {
        LOG.warn("Retriable error trying to flush " + getTableName() + " state=" + state, e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void preflightChecks(MasterProcedureEnv env, Boolean enabled) throws HBaseIOException {
    super.preflightChecks(env, enabled);
    if (columnFamilies == null) {
      return;
    }
    MasterServices master = env.getMasterServices();
    try {
      TableDescriptor tableDescriptor = master.getTableDescriptors().get(tableName);
      List<String> noSuchFamilies = columnFamilies.stream()
        .filter(cf -> !tableDescriptor.hasColumnFamily(cf)).map(Bytes::toString).toList();
      if (!noSuchFamilies.isEmpty()) {
        throw new NoSuchColumnFamilyException("Column families " + noSuchFamilies
          + " don't exist in table " + tableName.getNameAsString());
      }
    } catch (IOException ioe) {
      if (ioe instanceof HBaseIOException) {
        throw (HBaseIOException) ioe;
      }
      throw new HBaseIOException(ioe);
    }
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, FlushTableState state)
    throws IOException, InterruptedException {
    // nothing to rollback
  }

  @Override
  protected FlushTableState getState(int stateId) {
    return FlushTableState.forNumber(stateId);
  }

  @Override
  protected int getStateId(FlushTableState state) {
    return state.getNumber();
  }

  @Override
  protected FlushTableState getInitialState() {
    return FlushTableState.FLUSH_TABLE_PREPARE;
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.FLUSH;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    FlushTableProcedureStateData.Builder builder = FlushTableProcedureStateData.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName(tableName));
    if (columnFamilies != null) {
      for (byte[] columnFamily : columnFamilies) {
        if (columnFamily != null && columnFamily.length > 0) {
          builder.addColumnFamily(UnsafeByteOperations.unsafeWrap(columnFamily));
        }
      }
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    FlushTableProcedureStateData data = serializer.deserialize(FlushTableProcedureStateData.class);
    this.tableName = ProtobufUtil.toTableName(data.getTableName());
    if (data.getColumnFamilyCount() > 0) {
      this.columnFamilies = data.getColumnFamilyList().stream().filter(cf -> !cf.isEmpty())
        .map(ByteString::toByteArray).collect(Collectors.toList());
    }
  }

  private FlushRegionProcedure[] createFlushRegionProcedures(MasterProcedureEnv env) {
    return env.getAssignmentManager().getTableRegions(getTableName(), true).stream()
      .filter(r -> RegionReplicaUtil.isDefaultReplica(r))
      .map(r -> new FlushRegionProcedure(r, columnFamilies)).toArray(FlushRegionProcedure[]::new);
  }

  @Override
  public void toStringClassDetails(StringBuilder builder) {
    builder.append(getClass().getName()).append(", id=").append(getProcId()).append(", table=")
      .append(tableName);
    if (columnFamilies != null) {
      builder.append(", columnFamilies=[")
        .append(Strings.JOINER
          .join(columnFamilies.stream().map(Bytes::toString).collect(Collectors.toList())))
        .append("]");
    }
  }

  @Override
  protected void afterReplay(MasterProcedureEnv env) {
    if (
      !env.getMasterConfiguration().getBoolean(
        MasterFlushTableProcedureManager.FLUSH_PROCEDURE_ENABLED,
        MasterFlushTableProcedureManager.FLUSH_PROCEDURE_ENABLED_DEFAULT)
    ) {
      setFailure("master-flush-table", new HBaseIOException("FlushTableProcedureV2 is DISABLED"));
    }
  }
}
