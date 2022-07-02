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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.master.assignment.ServerState;
import org.apache.hadoop.hbase.procedure2.FailedRemoteDispatchException;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteProcedure;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * The base class for the remote procedures for normal operations, like flush or snapshot. Normal
 * operations do not change the region state. This is the difference between
 * {@link org.apache.hadoop.hbase.master.assignment.RegionRemoteProcedureBase} and
 * {@link IdempotentRegionRemoteProcedureBase}. It requires that the state of the region must be
 * OPEN. If region is in transition state, the procedure will suspend and retry later.
 */
@InterfaceAudience.Private
public abstract class IdempotentRegionRemoteProcedureBase extends Procedure<MasterProcedureEnv>
  implements TableProcedureInterface, RemoteProcedure<MasterProcedureEnv, ServerName> {
  private static final Logger LOG =
    LoggerFactory.getLogger(IdempotentRegionRemoteProcedureBase.class);

  protected RegionInfo region;

  private ProcedureEvent<?> event;
  private boolean dispatched;
  private boolean succ;
  private RetryCounter retryCounter;

  public IdempotentRegionRemoteProcedureBase() {
  }

  public IdempotentRegionRemoteProcedureBase(RegionInfo region) {
    this.region = region;
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
    regionNode.lock();
    try {
      if (regionNode.isInTransition()) {
        setTimeoutForSuspend(env, String.format("region %s has a TRSP attached %s",
          region.getRegionNameAsString(), regionNode.getProcedure()));
        throw new ProcedureSuspendedException();
      }
      if (!regionNode.isInState(RegionState.State.OPEN)) {
        setTimeoutForSuspend(env, String.format("region state of %s is %s",
          region.getRegionNameAsString(), regionNode.getState()));
        throw new ProcedureSuspendedException();
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

  @Override
  public TableName getTableName() {
    return region.getTable();
  }
}
