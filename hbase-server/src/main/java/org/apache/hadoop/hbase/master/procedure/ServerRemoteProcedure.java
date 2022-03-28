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
package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.procedure2.FailedRemoteDispatchException;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
/**
 * The base class for Procedures that run {@link java.util.concurrent.Callable}s on a (remote)
 * RegionServer; e.g. asking a RegionServer to split a WAL file as a sub-procedure of
 * the ServerCrashProcedure recovery process.
 *
 * <p>To implement a new Procedure type, extend this class and override remoteCallBuild() and
 * complete(). The dispatch and callback will be handled for you here, internally.
 *
 * <p>The Procedure works as follows. It uses {@link RSProcedureDispatcher}, the same system
 * used dispatching Region OPEN and CLOSE RPCs, to pass a Callable to a RegionServer. Examples
 * include {@link org.apache.hadoop.hbase.regionserver.SplitWALCallable} and
 * {@link org.apache.hadoop.hbase.replication.regionserver.RefreshPeerCallable}. Rather than
 * assign/unassign, the Master calls #executeProcedures against the remote RegionServer wrapping
 * a Callable in a {@link ExecuteProceduresRequest}. Upon successful dispatch,
 * the Procedure then suspends itself on the Master-side and relinqushes its executor worker.
 * On receipt, the RegionServer submits the Callable to its executor service. When the Callable
 * completes, it adds itself to a queue on the RegionServer side for processing by a background
 * thread, the {@link RemoteProcedureResultReporter}. It picks up the completed Callable from the
 * queue and RPCs the master at #reportProcedureDone with the procedure id and whether success or
 * failure. The master calls complete() setting success or failure state and then reschedules the
 * suspended Procedure so it can finish.
 *
 * <p>Here are some details on operation:
 * <p>If adding the operation to the dispatcher fails, addOperationToNode will throw
 * FailedRemoteDispatchException, and this Procedure will return 'null'. The Procedure Executor
 * will then mark this procedure as 'complete' (though we failed to dispatch our task). In this
 * case, the upper layer of this procedure must have a way to check if this Procedure really
 * succeeded or not and have appropriate handling.
 *
 * <p>If sending the operation to remote RS failed, dispatcher will call remoteCallFailed() to
 * handle this which calls remoteOperationDone with the exception. If the targetServer crashed but
 * this procedure has no response, than dispatcher will call remoteOperationFailed() which also
 * calls remoteOperationDone with the exception. If the operation is successful, then
 * remoteOperationCompleted will be called and actually calls the remoteOperationDone without
 * exception.
 *
 * In remoteOperationDone, we'll check if the procedure is already get wake up by others. Then
 * developer could implement complete() based on their own purpose.
 *
 * But basic logic is that if operation succeed, set succ to true and do the clean work.
 *
 * If operation failed and require to resend it to the same server, leave the succ as false.
 *
 * If operation failed and require to resend it to another server, set succ to true and upper layer
 * should be able to find out this operation not work and send a operation to another server.
 */
public abstract class ServerRemoteProcedure extends Procedure<MasterProcedureEnv>
    implements RemoteProcedureDispatcher.RemoteProcedure<MasterProcedureEnv, ServerName> {
  protected static final Logger LOG = LoggerFactory.getLogger(ServerRemoteProcedure.class);
  protected ProcedureEvent<?> event;
  protected ServerName targetServer;
  protected boolean dispatched;
  protected boolean succ;

  protected abstract void complete(MasterProcedureEnv env, Throwable error);

  @Override
  protected synchronized Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    if (dispatched) {
      if (succ) {
        return null;
      }
      dispatched = false;
    }
    try {
      env.getRemoteDispatcher().addOperationToNode(targetServer, this);
    } catch (FailedRemoteDispatchException frde) {
      LOG.warn("Can not send remote operation {} to {}, this operation will "
          + "be retried to send to another server",
        this.getProcId(), targetServer);
      return null;
    }
    dispatched = true;
    event = new ProcedureEvent<>(this);
    event.suspendIfNotReady(this);
    throw new ProcedureSuspendedException();
  }

  @Override
  protected synchronized void completionCleanup(MasterProcedureEnv env) {
    env.getRemoteDispatcher().removeCompletedOperation(targetServer, this);
  }

  @Override
  public synchronized void remoteCallFailed(MasterProcedureEnv env, ServerName serverName,
      IOException exception) {
    remoteOperationDone(env, exception);
  }

  @Override
  public synchronized void remoteOperationCompleted(MasterProcedureEnv env) {
    remoteOperationDone(env, null);
  }

  @Override
  public synchronized void remoteOperationFailed(MasterProcedureEnv env,
      RemoteProcedureException error) {
    remoteOperationDone(env, error);
  }

  synchronized void remoteOperationDone(MasterProcedureEnv env, Throwable error) {
    if (this.isFinished()) {
      LOG.info("This procedure {} is already finished, skip the rest processes", this.getProcId());
      return;
    }
    if (event == null) {
      LOG.warn("procedure event for {} is null, maybe the procedure is created when recovery",
          getProcId());
      return;
    }
    complete(env, error);
    event.wake(env.getProcedureScheduler());
    event = null;
  }
}
