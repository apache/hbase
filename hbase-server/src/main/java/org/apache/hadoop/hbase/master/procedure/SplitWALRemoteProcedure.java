/**
 *
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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.procedure2.NoNodeDispatchException;
import org.apache.hadoop.hbase.procedure2.NoServerDispatchException;
import org.apache.hadoop.hbase.procedure2.NullTargetServerDispatchException;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.regionserver.SplitWALCallable;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
/**
 * A remote procedure which is used to send split WAL request to region server.
 * it will return null if the task is succeed or return a DoNotRetryIOException
 * {@link SplitWALProcedure} will help handle the situation that encounter
 * DoNotRetryIOException. Otherwise it will retry until succeed.
 */
@InterfaceAudience.Private
public class SplitWALRemoteProcedure extends Procedure<MasterProcedureEnv>
    implements RemoteProcedureDispatcher.RemoteProcedure<MasterProcedureEnv, ServerName>,
    ServerProcedureInterface {
  private static final Logger LOG = LoggerFactory.getLogger(SplitWALRemoteProcedure.class);
  private String walPath;
  private ServerName worker;
  private ServerName crashedServer;
  private boolean dispatched;
  private ProcedureEvent<?> event;
  private boolean success = false;

  public SplitWALRemoteProcedure() {
  }

  public SplitWALRemoteProcedure(ServerName worker, ServerName crashedServer, String wal) {
    this.worker = worker;
    this.crashedServer = crashedServer;
    this.walPath = wal;
  }

  @Override
  protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    if (dispatched) {
      if (success) {
        return null;
      }
      dispatched = false;
    }
    try {
      env.getRemoteDispatcher().addOperationToNode(worker, this);
    } catch (NoNodeDispatchException | NullTargetServerDispatchException
        | NoServerDispatchException e) {
      // When send to a wrong target server, it need construct a new SplitWALRemoteProcedure.
      // Thus return null for this procedure and let SplitWALProcedure to handle this.
      LOG.warn("dispatch WAL {} to {} failed, will retry on another server", walPath, worker, e);
      return null;
    }
    dispatched = true;
    event = new ProcedureEvent<>(this);
    event.suspendIfNotReady(this);
    throw new ProcedureSuspendedException();
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
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    MasterProcedureProtos.SplitWALRemoteData.Builder builder =
        MasterProcedureProtos.SplitWALRemoteData.newBuilder();
    builder.setWalPath(walPath).setWorker(ProtobufUtil.toServerName(worker))
        .setCrashedServer(ProtobufUtil.toServerName(crashedServer));
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    MasterProcedureProtos.SplitWALRemoteData data =
        serializer.deserialize(MasterProcedureProtos.SplitWALRemoteData.class);
    walPath = data.getWalPath();
    worker = ProtobufUtil.toServerName(data.getWorker());
    crashedServer = ProtobufUtil.toServerName(data.getCrashedServer());
  }

  @Override
  public RemoteProcedureDispatcher.RemoteOperation remoteCallBuild(MasterProcedureEnv env,
      ServerName serverName) {
    return new RSProcedureDispatcher.ServerOperation(this, getProcId(), SplitWALCallable.class,
        MasterProcedureProtos.SplitWALParameter.newBuilder().setWalPath(walPath).build()
            .toByteArray());
  }

  @Override
  public void remoteCallFailed(MasterProcedureEnv env, ServerName serverName,
      IOException exception) {
    complete(env, exception);
  }

  @Override
  public void remoteOperationCompleted(MasterProcedureEnv env) {
    complete(env, null);
  }

  private void complete(MasterProcedureEnv env, Throwable error) {
    if (event == null) {
      LOG.warn("procedure event for {} is null, maybe the procedure is created when recovery",
        getProcId());
      return;
    }
    if (error == null) {
      LOG.info("split WAL {} on {} succeeded", walPath, worker);
      try {
        env.getMasterServices().getSplitWALManager().deleteSplitWAL(walPath);
      } catch (IOException e){
        LOG.warn("remove WAL {} failed, ignore...", walPath, e);
      }
      success = true;
    } else {
      if (error instanceof DoNotRetryIOException) {
        LOG.warn("WAL split task of {} send to a wrong server {}, will retry on another server",
          walPath, worker, error);
        success = true;
      } else {
        LOG.warn("split WAL {} failed, retry...", walPath, error);
        success = false;
      }

    }
    event.wake(env.getProcedureScheduler());
    event = null;
  }

  @Override
  public void remoteOperationFailed(MasterProcedureEnv env, RemoteProcedureException error) {
    complete(env, error);
  }

  public String getWAL() {
    return this.walPath;
  }

  @Override
  public ServerName getServerName() {
    // return the crashed server is to use the queue of root ServerCrashProcedure
    return this.crashedServer;
  }

  @Override
  public boolean hasMetaTableRegion() {
    return AbstractFSWALProvider.isMetaFile(new Path(walPath));
  }

  @Override
  public ServerOperationType getServerOperationType() {
    return ServerOperationType.SPLIT_WAL_REMOTE;
  }
}
