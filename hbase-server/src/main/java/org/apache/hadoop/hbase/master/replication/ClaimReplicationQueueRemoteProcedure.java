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
package org.apache.hadoop.hbase.master.replication;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher.ServerOperation;
import org.apache.hadoop.hbase.master.procedure.ServerProcedureInterface;
import org.apache.hadoop.hbase.master.procedure.ServerRemoteProcedure;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteProcedure;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.replication.regionserver.ClaimReplicationQueueCallable;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSyncUp;
import org.apache.hadoop.hbase.util.ForeignExceptionUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ErrorHandlingProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ClaimReplicationQueueRemoteParameter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ClaimReplicationQueueRemoteStateData;

@InterfaceAudience.Private
public class ClaimReplicationQueueRemoteProcedure extends ServerRemoteProcedure
  implements ServerProcedureInterface, RemoteProcedure<MasterProcedureEnv, ServerName> {

  private static final Logger LOG =
    LoggerFactory.getLogger(ClaimReplicationQueueRemoteProcedure.class);

  private ReplicationQueueId queueId;

  public ClaimReplicationQueueRemoteProcedure() {
  }

  public ClaimReplicationQueueRemoteProcedure(ReplicationQueueId queueId, ServerName targetServer) {
    this.queueId = queueId;
    this.targetServer = targetServer;
  }

  // check whether ReplicationSyncUp has already done the work for us, if so, we should skip
  // claiming the replication queues and deleting them instead.
  private boolean shouldSkip(MasterProcedureEnv env) throws IOException {
    MasterFileSystem mfs = env.getMasterFileSystem();
    Path syncUpDir = new Path(mfs.getRootDir(), ReplicationSyncUp.INFO_DIR);
    return mfs.getFileSystem().exists(new Path(syncUpDir, getServerName().getServerName()));
  }

  @Override
  protected synchronized Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
    throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    try {
      if (shouldSkip(env)) {
        LOG.info("Skip claiming {} because replication sync up has already done it for us",
          getServerName());
        return null;
      }
    } catch (IOException e) {
      LOG.warn("failed to check whether we should skip claiming {} due to replication sync up",
        getServerName(), e);
      // just finish the procedure here, as the AssignReplicationQueuesProcedure will reschedule
      return null;
    }
    return super.execute(env);
  }

  @Override
  public Optional<RemoteOperation> remoteCallBuild(MasterProcedureEnv env, ServerName remote) {
    assert targetServer.equals(remote);
    ClaimReplicationQueueRemoteParameter.Builder builder = ClaimReplicationQueueRemoteParameter
      .newBuilder().setCrashedServer(ProtobufUtil.toServerName(queueId.getServerName()))
      .setQueue(queueId.getPeerId());
    queueId.getSourceServerName()
      .ifPresent(sourceServer -> builder.setSourceServer(ProtobufUtil.toServerName(sourceServer)));
    return Optional.of(new ServerOperation(this, getProcId(), ClaimReplicationQueueCallable.class,
      builder.build().toByteArray(), env.getMasterServices().getMasterActiveTime()));
  }

  @Override
  public ServerName getServerName() {
    // return crashed server here, as we are going to recover its replication queues so we should
    // use its scheduler queue instead of the one for the target server.
    return queueId.getServerName();
  }

  @Override
  public boolean hasMetaTableRegion() {
    return false;
  }

  @Override
  public ServerOperationType getServerOperationType() {
    return ServerOperationType.CLAIM_REPLICATION_QUEUE_REMOTE;
  }

  @Override
  protected boolean complete(MasterProcedureEnv env, Throwable error) {
    if (error != null) {
      LOG.warn("Failed to claim replication queue {} on server {} ", queueId, targetServer, error);
      return false;
    } else {
      return true;
    }
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
  protected boolean waitInitialized(MasterProcedureEnv env) {
    return env.waitInitialized(this);
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    ClaimReplicationQueueRemoteStateData.Builder builder = ClaimReplicationQueueRemoteStateData
      .newBuilder().setCrashedServer(ProtobufUtil.toServerName(queueId.getServerName()))
      .setQueue(queueId.getPeerId()).setTargetServer(ProtobufUtil.toServerName(targetServer))
      .setState(state);
    if (this.remoteError != null) {
      ErrorHandlingProtos.ForeignExceptionMessage fem =
        ForeignExceptionUtil.toProtoForeignException(remoteError);
      builder.setError(fem);
    }
    queueId.getSourceServerName()
      .ifPresent(sourceServer -> builder.setSourceServer(ProtobufUtil.toServerName(sourceServer)));
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    ClaimReplicationQueueRemoteStateData data =
      serializer.deserialize(ClaimReplicationQueueRemoteStateData.class);
    targetServer = ProtobufUtil.toServerName(data.getTargetServer());
    ServerName crashedServer = ProtobufUtil.toServerName(data.getCrashedServer());
    String queue = data.getQueue();
    state = data.getState();
    if (data.hasSourceServer()) {
      queueId = new ReplicationQueueId(crashedServer, queue,
        ProtobufUtil.toServerName(data.getSourceServer()));
    } else {
      queueId = new ReplicationQueueId(crashedServer, queue);
    }
    if (data.hasError()) {
      this.remoteError = ForeignExceptionUtil.toException(data.getError());
    }
  }
}
