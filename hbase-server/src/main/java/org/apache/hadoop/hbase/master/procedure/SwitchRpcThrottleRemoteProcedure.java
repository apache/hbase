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
import java.util.Optional;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher;
import org.apache.hadoop.hbase.replication.regionserver.SwitchRpcThrottleRemoteCallable;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SwitchRpcThrottleRemoteStateData;

/**
 * The procedure to switch rpc throttle on region server
 */
@InterfaceAudience.Private
public class SwitchRpcThrottleRemoteProcedure extends ServerRemoteProcedure
    implements ServerProcedureInterface {

  private static final Logger LOG = LoggerFactory.getLogger(SwitchRpcThrottleRemoteProcedure.class);
  private boolean rpcThrottleEnabled;

  public SwitchRpcThrottleRemoteProcedure() {
  }

  public SwitchRpcThrottleRemoteProcedure(ServerName serverName, boolean rpcThrottleEnabled) {
    this.targetServer = serverName;
    this.rpcThrottleEnabled = rpcThrottleEnabled;
  }

  @Override
  protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    return false;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    SwitchRpcThrottleRemoteStateData.newBuilder()
        .setTargetServer(ProtobufUtil.toServerName(targetServer))
        .setRpcThrottleEnabled(rpcThrottleEnabled).build();
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    SwitchRpcThrottleRemoteStateData data =
        serializer.deserialize(SwitchRpcThrottleRemoteStateData.class);
    targetServer = ProtobufUtil.toServerName(data.getTargetServer());
    rpcThrottleEnabled = data.getRpcThrottleEnabled();
  }

  @Override
  public Optional<RemoteProcedureDispatcher.RemoteOperation> remoteCallBuild(
      MasterProcedureEnv masterProcedureEnv, ServerName remote) {
    assert targetServer.equals(remote);
    return Optional.of(new RSProcedureDispatcher.ServerOperation(this, getProcId(),
        SwitchRpcThrottleRemoteCallable.class, SwitchRpcThrottleRemoteStateData.newBuilder()
        .setTargetServer(ProtobufUtil.toServerName(remote))
        .setRpcThrottleEnabled(rpcThrottleEnabled).build().toByteArray()));
  }

  @Override
  public ServerName getServerName() {
    return targetServer;
  }

  @Override
  public boolean hasMetaTableRegion() {
    return false;
  }

  @Override
  public ServerOperationType getServerOperationType() {
    return ServerOperationType.SWITCH_RPC_THROTTLE;
  }

  @Override
  protected void complete(MasterProcedureEnv env, Throwable error) {
    if (error != null) {
      LOG.warn("Failed to switch rpc throttle to {} on server {}", rpcThrottleEnabled, targetServer,
        error);
      this.succ = false;
    } else {
      this.succ = true;
    }
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" server=");
    sb.append(targetServer);
    sb.append(", rpcThrottleEnabled=");
    sb.append(rpcThrottleEnabled);
  }
}
