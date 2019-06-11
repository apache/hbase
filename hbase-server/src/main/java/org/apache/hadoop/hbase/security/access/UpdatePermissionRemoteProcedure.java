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
package org.apache.hadoop.hbase.security.access;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.procedure.AclProcedureInterface;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher;
import org.apache.hadoop.hbase.master.procedure.ServerRemoteProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.UpdatePermissionRemoteStateData;

@InterfaceAudience.Private
public class UpdatePermissionRemoteProcedure extends ServerRemoteProcedure
    implements AclProcedureInterface {
  private static final Logger LOG = LoggerFactory.getLogger(UpdatePermissionRemoteProcedure.class);
  private String entry;

  public UpdatePermissionRemoteProcedure() {
  }

  public UpdatePermissionRemoteProcedure(ServerName serverName, String entry) {
    this.targetServer = serverName;
    this.entry = entry;
  }

  @Override
  protected void complete(MasterProcedureEnv env, Throwable error) {
    if (error != null) {
      LOG.warn("Failed to update permissions for entry {} on server {}", entry, targetServer,
        error);
      this.succ = false;
    } else {
      this.succ = true;
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
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    UpdatePermissionRemoteStateData data = UpdatePermissionRemoteStateData.newBuilder()
        .setTargetServer(ProtobufUtil.toServerName(targetServer)).setEntry(entry).build();
    serializer.serialize(data);
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    UpdatePermissionRemoteStateData data =
        serializer.deserialize(UpdatePermissionRemoteStateData.class);
    targetServer = ProtobufUtil.toServerName(data.getTargetServer());
    entry = data.getEntry();
  }

  @Override
  public Optional<RemoteOperation> remoteCallBuild(MasterProcedureEnv env, ServerName serverName) {
    assert targetServer.equals(serverName);
    return Optional.of(new RSProcedureDispatcher.ServerOperation(this, getProcId(),
        UpdatePermissionRemoteCallable.class,
        UpdatePermissionRemoteStateData.newBuilder()
            .setTargetServer(ProtobufUtil.toServerName(serverName)).setEntry(entry).build()
            .toByteArray()));
  }

  @Override
  protected void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName()).append(" server=").append(targetServer).append(", entry=")
        .append(entry);
  }

  @Override
  public String getAclEntry() {
    return entry;
  }

  @Override
  public AclOperationType getAclOperationType() {
    return AclOperationType.REMOTE;
  }
}
