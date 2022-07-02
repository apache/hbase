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
import java.util.Optional;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.hadoop.hbase.regionserver.FlushRegionCallable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.FlushRegionParameter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.FlushRegionProcedureStateData;

@InterfaceAudience.Private
public class FlushRegionProcedure extends IdempotentRegionRemoteProcedureBase {
  private static final Logger LOG = LoggerFactory.getLogger(FlushRegionProcedure.class);

  private byte[] columnFamily;

  public FlushRegionProcedure() {
  }

  public FlushRegionProcedure(RegionInfo region) {
    this(region, null);
  }

  public FlushRegionProcedure(RegionInfo region, byte[] columnFamily) {
    super(region);
    this.columnFamily = columnFamily;
  }

  @Override
  protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
    throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    RegionStateNode regionNode =
      env.getAssignmentManager().getRegionStates().getRegionStateNode(region);
    if (!regionNode.isInState(State.OPEN) || regionNode.isInTransition()) {
      LOG.info("State of region {} is not OPEN or in transition. Skip {} ...", region, this);
      return null;
    }
    return super.execute(env);
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    FlushRegionProcedureStateData.Builder builder = FlushRegionProcedureStateData.newBuilder();
    builder.setRegion(ProtobufUtil.toRegionInfo(region));
    if (columnFamily != null) {
      builder.setColumnFamily(UnsafeByteOperations.unsafeWrap(columnFamily));
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    FlushRegionProcedureStateData data =
      serializer.deserialize(FlushRegionProcedureStateData.class);
    this.region = ProtobufUtil.toRegionInfo(data.getRegion());
    if (data.hasColumnFamily()) {
      this.columnFamily = data.getColumnFamily().toByteArray();
    }
  }

  @Override
  public Optional<RemoteOperation> remoteCallBuild(MasterProcedureEnv env, ServerName serverName) {
    FlushRegionParameter.Builder builder = FlushRegionParameter.newBuilder();
    builder.setRegion(ProtobufUtil.toRegionInfo(region));
    if (columnFamily != null) {
      builder.setColumnFamily(UnsafeByteOperations.unsafeWrap(columnFamily));
    }
    return Optional.of(new RSProcedureDispatcher.ServerOperation(this, getProcId(),
      FlushRegionCallable.class, builder.build().toByteArray()));
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.FLUSH;
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
    return env.waitInitialized(this);
  }
}
