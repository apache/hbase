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
package org.apache.hadoop.hbase.backup.master;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.backup.regionserver.LogRollCallable;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher.ServerOperation;
import org.apache.hadoop.hbase.master.procedure.ServerProcedureInterface;
import org.apache.hadoop.hbase.master.procedure.ServerRemoteProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.BackupProtos.RSLogRollParameter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.BackupProtos.RSLogRollRemoteData;

/**
 * The remote procedure to perform WAL rolling on the specific RegionServer without retrying.
 */
@InterfaceAudience.Private
public class RSLogRollRemoteProcedure extends ServerRemoteProcedure
  implements ServerProcedureInterface {

  private String backupRoot;

  public RSLogRollRemoteProcedure() {
  }

  public RSLogRollRemoteProcedure(ServerName targetServer, String backupRoot) {
    this.targetServer = targetServer;
    this.backupRoot = backupRoot;
  }

  @Override
  protected void rollback(MasterProcedureEnv env) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    return false;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    serializer.serialize(RSLogRollRemoteData.newBuilder().setBackupRoot(backupRoot)
      .setTargetServer(ProtobufUtil.toServerName(targetServer)).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    RSLogRollRemoteData data = serializer.deserialize(RSLogRollRemoteData.class);
    this.backupRoot = data.getBackupRoot();
    this.targetServer = ProtobufUtil.toServerName(data.getTargetServer());
  }

  @Override
  public Optional<RemoteOperation> remoteCallBuild(MasterProcedureEnv env, ServerName serverName) {
    return Optional.of(new ServerOperation(this, getProcId(), LogRollCallable.class,
      RSLogRollParameter.newBuilder().setBackupRoot(backupRoot).build().toByteArray()));
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
    return ServerOperationType.LOG_ROLL;
  }

  @Override
  protected void complete(MasterProcedureEnv env, Throwable error) {
    // do not retry. just returns.
    if (error != null) {
      LOG.warn("Failed to roll wal for {}", targetServer, error);
    }
    succ = true;
  }

  @Override
  protected void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName()).append(" targetServer=").append(targetServer)
      .append(", backupRoot=").append(backupRoot);
  }
}
