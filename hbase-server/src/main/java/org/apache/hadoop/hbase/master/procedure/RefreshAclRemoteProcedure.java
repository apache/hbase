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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher;
import org.apache.hadoop.hbase.security.access.RefreshAclRemoteCallable;
import org.apache.hadoop.hbase.util.ForeignExceptionUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ErrorHandlingProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AclEntryUpdate;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshAclRemoteStateData;

/**
 * Delivers one ACL change set to one region server, as a child task of {@link RefreshAclProcedure}.
 * Retried against the same server for as long as it is alive; when the server disappears the task
 * is dropped and the startup load / periodic reload of the restarted server picks the change up
 * from the {@code hbase:acl} table.
 */
@InterfaceAudience.Private
public class RefreshAclRemoteProcedure extends ServerRemoteProcedure
  implements ServerProcedureInterface {

  private static final Logger LOG = LoggerFactory.getLogger(RefreshAclRemoteProcedure.class);

  private List<AclEntryUpdate> updates;
  private boolean full;
  private long ts;

  public RefreshAclRemoteProcedure() {
  }

  public RefreshAclRemoteProcedure(ServerName serverName, List<AclEntryUpdate> updates,
    boolean full, long ts) {
    this.targetServer = serverName;
    this.updates = updates;
    this.full = full;
    this.ts = ts;
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
    RefreshAclRemoteStateData.Builder builder = RefreshAclRemoteStateData.newBuilder();
    builder.setTargetServer(ProtobufUtil.toServerName(targetServer)).setFull(full).setTs(ts)
      .setState(state).addAllUpdates(updates);
    if (this.remoteError != null) {
      ErrorHandlingProtos.ForeignExceptionMessage fem =
        ForeignExceptionUtil.toProtoForeignException(remoteError);
      builder.setError(fem);
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    RefreshAclRemoteStateData data = serializer.deserialize(RefreshAclRemoteStateData.class);
    targetServer = ProtobufUtil.toServerName(data.getTargetServer());
    updates = data.getUpdatesList();
    full = data.getFull();
    ts = data.getTs();
    state = data.getState();
    if (data.hasError()) {
      this.remoteError = ForeignExceptionUtil.toException(data.getError());
    }
  }

  @Override
  public Optional<RemoteProcedureDispatcher.RemoteOperation>
    remoteCallBuild(MasterProcedureEnv masterProcedureEnv, ServerName remote) {
    assert targetServer.equals(remote);
    return Optional.of(
      new RSProcedureDispatcher.ServerOperation(this, getProcId(), RefreshAclRemoteCallable.class,
        RefreshAclRemoteStateData.newBuilder().setTargetServer(ProtobufUtil.toServerName(remote))
          .setFull(full).setTs(ts).addAllUpdates(updates).build().toByteArray(),
        masterProcedureEnv.getMasterServices().getMasterActiveTime()));
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
    return ServerOperationType.REFRESH_ACL;
  }

  @Override
  protected boolean complete(MasterProcedureEnv env, Throwable error) {
    if (error != null) {
      // keep retrying while the server is alive; if it dies the dispatch fails and this task is
      // dropped, the restarted server reloads the full content from hbase:acl
      LOG.warn("Failed to push ACL update ts={} to server {}, will retry", ts, targetServer, error);
      return false;
    }
    return true;
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" server=");
    sb.append(targetServer);
    sb.append(", ts=");
    sb.append(ts);
    sb.append(", full=");
    sb.append(full);
    sb.append(", updates=");
    sb.append(updates != null ? updates.size() : 0);
  }
}
