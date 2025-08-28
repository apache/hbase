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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.procedure2.FailedRemoteDispatchException;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.regionserver.RefreshHFilesCallable;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

@InterfaceAudience.Private
public class RefreshHFilesRegionProcedure extends Procedure<MasterProcedureEnv>
  implements TableProcedureInterface,
  RemoteProcedureDispatcher.RemoteProcedure<MasterProcedureEnv, ServerName> {
  private static final Logger LOG = LoggerFactory.getLogger(RefreshHFilesRegionProcedure.class);
  private RegionInfo region;
  private ProcedureEvent<?> event;
  private boolean dispatched;
  private boolean succ;
  private RetryCounter retryCounter;

  public RefreshHFilesRegionProcedure() {
  }

  public RefreshHFilesRegionProcedure(RegionInfo region) {
    this.region = region;
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    MasterProcedureProtos.RefreshHFilesRegionProcedureStateData data =
      serializer.deserialize(MasterProcedureProtos.RefreshHFilesRegionProcedureStateData.class);
    this.region = ProtobufUtil.toRegionInfo(data.getRegion());
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    MasterProcedureProtos.RefreshHFilesRegionProcedureStateData.Builder builder =
      MasterProcedureProtos.RefreshHFilesRegionProcedureStateData.newBuilder();
    builder.setRegion(ProtobufUtil.toRegionInfo(region));
    serializer.serialize(builder.build());
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    return false;
  }

  @Override
  protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  private void setTimeoutForSuspend(MasterProcedureEnv env, String reason) {
    if (retryCounter == null) {
      retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
    }
    long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
    LOG.warn("{} can not run currently because {}, wait {} ms to retry", this, reason, backoff);
    setTimeout(Math.toIntExact(backoff));
    setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
    skipPersistence();
  }

  @Override
  protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
    throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    if (dispatched) {
      if (succ) {
        return null;
      }
      dispatched = false;
    }

    RegionStates regionStates = env.getAssignmentManager().getRegionStates();
    RegionStateNode regionNode = regionStates.getRegionStateNode(region);

    if (regionNode.getProcedure() != null) {
      setTimeoutForSuspend(env, String.format("region %s has a TRSP attached %s",
        region.getRegionNameAsString(), regionNode.getProcedure()));
      throw new ProcedureSuspendedException();
    }

    if (!regionNode.isInState(RegionState.State.OPEN)) {
      LOG.info("State of region {} is not OPEN. Skip {} ...", region, this);
      setTimeoutForSuspend(env, String.format("region state of %s is %s",
        region.getRegionNameAsString(), regionNode.getState()));
      return null;
    }

    ServerName targetServer = regionNode.getRegionLocation();
    if (targetServer == null) {
      setTimeoutForSuspend(env,
        String.format("target server of region %s is null", region.getRegionNameAsString()));
      throw new ProcedureSuspendedException();
    }

    try {
      env.getRemoteDispatcher().addOperationToNode(targetServer, this);
      dispatched = true;
      event = new ProcedureEvent<>(this);
      event.suspendIfNotReady(this);
      throw new ProcedureSuspendedException();
    } catch (FailedRemoteDispatchException e) {
      setTimeoutForSuspend(env, "Failed send request to " + targetServer);
      throw new ProcedureSuspendedException();
    }
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REFRESH_HFILES;
  }

  @Override
  public TableName getTableName() {
    return region.getTable();
  }

  @Override
  public void remoteOperationFailed(MasterProcedureEnv env, RemoteProcedureException error) {
    complete(env, error);
  }

  @Override
  public void remoteOperationCompleted(MasterProcedureEnv env) {
    complete(env, null);
  }

  @Override
  public void remoteCallFailed(MasterProcedureEnv env, ServerName serverName, IOException e) {
    complete(env, e);
  }

  private void complete(MasterProcedureEnv env, Throwable error) {
    if (isFinished()) {
      LOG.info("This procedure {} is already finished, skip the rest processes", this.getProcId());
      return;
    }
    if (event == null) {
      LOG.warn("procedure event for {} is null, maybe the procedure is created when recovery",
        getProcId());
      return;
    }
    if (error == null) {
      succ = true;
    }
    event.wake(env.getProcedureScheduler());
    event = null;
  }

  @Override
  public Optional<RemoteProcedureDispatcher.RemoteOperation> remoteCallBuild(MasterProcedureEnv env,
    ServerName serverName) {
    MasterProcedureProtos.RefreshHFilesRegionParameter.Builder builder =
      MasterProcedureProtos.RefreshHFilesRegionParameter.newBuilder();
    builder.setRegion(ProtobufUtil.toRegionInfo(region));
    return Optional
      .of(new RSProcedureDispatcher.ServerOperation(this, getProcId(), RefreshHFilesCallable.class,
        builder.build().toByteArray(), env.getMasterServices().getMasterActiveTime()));
  }
}
