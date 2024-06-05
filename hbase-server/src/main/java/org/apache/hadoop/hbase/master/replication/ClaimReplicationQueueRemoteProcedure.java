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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher.ServerOperation;
import org.apache.hadoop.hbase.master.procedure.ServerProcedureInterface;
import org.apache.hadoop.hbase.master.procedure.ServerRemoteProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteProcedure;
import org.apache.hadoop.hbase.replication.regionserver.ClaimReplicationQueueCallable;
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

  private ServerName crashedServer;

  private String queue;

  public ClaimReplicationQueueRemoteProcedure() {
  }

  public ClaimReplicationQueueRemoteProcedure(ServerName crashedServer, String queue,
    ServerName targetServer) {
    this.crashedServer = crashedServer;
    this.queue = queue;
    this.targetServer = targetServer;
  }

  @Override
  public Optional<RemoteOperation> remoteCallBuild(MasterProcedureEnv env, ServerName remote) {
    assert targetServer.equals(remote);
    return Optional.of(new ServerOperation(this, getProcId(), ClaimReplicationQueueCallable.class,
      ClaimReplicationQueueRemoteParameter.newBuilder()
        .setCrashedServer(ProtobufUtil.toServerName(crashedServer)).setQueue(queue).build()
        .toByteArray()));
  }

  @Override
  public ServerName getServerName() {
    // return crashed server here, as we are going to recover its replication queues so we should
    // use its scheduler queue instead of the one for the target server.
    return crashedServer;
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
      LOG.warn("Failed to claim replication queue {} of crashed server on server {} ", queue,
        crashedServer, targetServer, error);
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
      .newBuilder().setCrashedServer(ProtobufUtil.toServerName(crashedServer)).setQueue(queue)
      .setTargetServer(ProtobufUtil.toServerName(targetServer)).setState(state);
    if (this.remoteError != null) {
      ErrorHandlingProtos.ForeignExceptionMessage fem =
        ForeignExceptionUtil.toProtoForeignException(remoteError);
      builder.setError(fem);
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    ClaimReplicationQueueRemoteStateData data =
      serializer.deserialize(ClaimReplicationQueueRemoteStateData.class);
    crashedServer = ProtobufUtil.toServerName(data.getCrashedServer());
    queue = data.getQueue();
    targetServer = ProtobufUtil.toServerName(data.getTargetServer());
    state = data.getState();
    if (data.hasError()) {
      this.remoteError = ForeignExceptionUtil.toException(data.getError());
    }
  }
}
