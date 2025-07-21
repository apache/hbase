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
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher;
import org.apache.hadoop.hbase.regionserver.ReloadQuotasCallable;
import org.apache.hadoop.hbase.util.ForeignExceptionUtil;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Throwables;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ErrorHandlingProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;

@InterfaceAudience.Private
public class ReloadQuotasProcedure extends ServerRemoteProcedure
  implements ServerProcedureInterface {

  public ReloadQuotasProcedure() {
  }

  public ReloadQuotasProcedure(ServerName targetServer) {
    this.targetServer = targetServer;
  }

  @Override
  protected boolean complete(MasterProcedureEnv env, Throwable error) {
    if (error != null && containsCause(error, ClassNotFoundException.class)) {
      LOG.warn(
        "Failed to reload quotas on server {}, but will allow this procedure to complete. The RegionServer may be on an older version of HBase that does not support ReloadQuotasProcedure.",
        targetServer);
      return true;
    } else if (error != null) {
      LOG.error("Failed to reload quotas on server {}", targetServer, error);
      return false;
    } else {
      return true;
    }
  }

  /** visibility for testing */
  boolean containsCause(Throwable t, Class<? extends Throwable> causeClass) {
    return Throwables.getCausalChain(t).stream().anyMatch(causeClass::isInstance);
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
    MasterProcedureProtos.ReloadQuotasProcedureStateData.Builder builder =
      MasterProcedureProtos.ReloadQuotasProcedureStateData.newBuilder();
    if (this.remoteError != null) {
      ErrorHandlingProtos.ForeignExceptionMessage fem =
        ForeignExceptionUtil.toProtoForeignException(remoteError);
      builder.setError(fem);
    }
    serializer.serialize(builder.setTargetServer(ProtobufUtil.toServerName(targetServer)).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    MasterProcedureProtos.ReloadQuotasProcedureStateData data =
      serializer.deserialize(MasterProcedureProtos.ReloadQuotasProcedureStateData.class);
    this.targetServer = ProtobufUtil.toServerName(data.getTargetServer());
    if (data.hasError()) {
      this.remoteError = ForeignExceptionUtil.toException(data.getError());
    }
  }

  @Override
  public Optional<RemoteProcedureDispatcher.RemoteOperation> remoteCallBuild(MasterProcedureEnv env,
    ServerName serverName) {
    if (!serverName.equals(targetServer)) {
      throw new IllegalArgumentException(
        "ReloadQuotasProcedure#remoteCallBuild called with unexpected serverName: " + serverName
          + " != " + targetServer);
    }
    return Optional.of(new RSProcedureDispatcher.ServerOperation(this, getProcId(),
      ReloadQuotasCallable.class, new byte[] {}, env.getMasterServices().getMasterActiveTime()));
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
    return ServerOperationType.RELOAD_QUOTAS;
  }
}
