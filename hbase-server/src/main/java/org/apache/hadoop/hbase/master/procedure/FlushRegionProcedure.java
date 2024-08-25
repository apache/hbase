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
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.master.assignment.ServerState;
import org.apache.hadoop.hbase.procedure2.FailedRemoteDispatchException;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteProcedure;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.regionserver.FlushRegionCallable;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.FlushRegionParameter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.FlushRegionProcedureStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

@InterfaceAudience.Private
public class FlushRegionProcedure extends Procedure<MasterProcedureEnv>
  implements TableProcedureInterface, RemoteProcedure<MasterProcedureEnv, ServerName> {
  private static final Logger LOG = LoggerFactory.getLogger(FlushRegionProcedure.class);

  private RegionInfo region;
  private List<byte[]> columnFamilies;
  private ProcedureEvent<?> event;
  private boolean dispatched;
  private boolean succ;
  private RetryCounter retryCounter;

  public FlushRegionProcedure() {
  }

  public FlushRegionProcedure(RegionInfo region) {
    this(region, null);
  }

  public FlushRegionProcedure(RegionInfo region, List<byte[]> columnFamilies) {
    this.region = region;
    this.columnFamilies = columnFamilies;
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
    if (regionNode == null) {
      LOG.debug("Region {} is not in region states, it is very likely that it has been cleared by"
        + " other procedures such as merge or split, so skip {}. See HBASE-28226", region, this);
      return null;
    }
    regionNode.lock();
    try {
      if (!regionNode.isInState(State.OPEN) || regionNode.isInTransition()) {
        LOG.info("State of region {} is not OPEN or in transition. Skip {} ...", region, this);
        return null;
      }
      ServerName targetServer = regionNode.getRegionLocation();
      if (targetServer == null) {
        setTimeoutForSuspend(env,
          String.format("target server of region %s is null", region.getRegionNameAsString()));
        throw new ProcedureSuspendedException();
      }
      ServerState serverState = regionStates.getServerNode(targetServer).getState();
      if (serverState != ServerState.ONLINE) {
        setTimeoutForSuspend(env, String.format("target server of region %s %s is in state %s",
          region.getRegionNameAsString(), targetServer, serverState));
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
    } finally {
      regionNode.unlock();
    }
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  @Override
  protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    return false;
  }

  @Override
  public void remoteCallFailed(MasterProcedureEnv env, ServerName serverName, IOException e) {
    complete(env, e);
  }

  @Override
  public void remoteOperationCompleted(MasterProcedureEnv env) {
    complete(env, null);
  }

  @Override
  public void remoteOperationFailed(MasterProcedureEnv env, RemoteProcedureException error) {
    complete(env, error);
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
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    FlushRegionProcedureStateData.Builder builder = FlushRegionProcedureStateData.newBuilder();
    builder.setRegion(ProtobufUtil.toRegionInfo(region));
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
    FlushRegionProcedureStateData data =
      serializer.deserialize(FlushRegionProcedureStateData.class);
    this.region = ProtobufUtil.toRegionInfo(data.getRegion());
    if (data.getColumnFamilyCount() > 0) {
      this.columnFamilies = data.getColumnFamilyList().stream().filter(cf -> !cf.isEmpty())
        .map(ByteString::toByteArray).collect(Collectors.toList());
    }
  }

  @Override
  public Optional<RemoteOperation> remoteCallBuild(MasterProcedureEnv env, ServerName serverName) {
    FlushRegionParameter.Builder builder = FlushRegionParameter.newBuilder();
    builder.setRegion(ProtobufUtil.toRegionInfo(region));
    if (columnFamilies != null) {
      for (byte[] columnFamily : columnFamilies) {
        if (columnFamily != null && columnFamily.length > 0) {
          builder.addColumnFamily(UnsafeByteOperations.unsafeWrap(columnFamily));
        }
      }
    }
    return Optional
      .of(new RSProcedureDispatcher.ServerOperation(this, getProcId(), FlushRegionCallable.class,
        builder.build().toByteArray(), env.getMasterServices().getMasterActiveTime()));
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.FLUSH;
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
    return env.waitInitialized(this);
  }

  @Override
  public TableName getTableName() {
    return region.getTable();
  }
}
