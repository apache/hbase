/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher;
import org.apache.hadoop.hbase.regionserver.SnapshotVerifyCallable;
import org.apache.hadoop.hbase.snapshot.CorruptedSnapshotException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SnapshotVerifyParameter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SnapshotVerifyProcedureStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

/**
 *  A remote procedure which is used to send verify snapshot request to region server.
 */
@InterfaceAudience.Private
public class SnapshotVerifyProcedure
    extends ServerRemoteProcedure implements ServerProcedureInterface {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotProcedure.class);
  private SnapshotDescription snapshot;
  private List<RegionInfo> regions;
  private int expectedNumRegion;

  public SnapshotVerifyProcedure() {}

  public SnapshotVerifyProcedure(SnapshotDescription snapshot, List<RegionInfo> regions,
      ServerName targetServer, int expectedNumRegion) {
    this.targetServer = targetServer;
    this.snapshot = snapshot;
    this.regions = regions;
    this.expectedNumRegion = expectedNumRegion;
  }

  @Override
  protected void complete(MasterProcedureEnv env, Throwable error) {
    if (error != null) {
      Throwable realError = error.getCause();
      if (realError instanceof CorruptedSnapshotException) {
        getParent(env).ifPresent(p -> p.reportSnapshotCorrupted(
          this, (CorruptedSnapshotException) realError));
        this.succ = true;
      } else {
        if (realError instanceof DoNotRetryIOException) {
          newTargetServer(env).ifPresent(s -> {
            LOG.warn("Failed send request to {}, try new target server {}",
              targetServer, s, realError);
            this.targetServer = s;
            this.dispatched = false;
          });
        }
        this.succ = false;
      }
    } else {
      this.succ = true;
    }
  }

  @Override
  protected synchronized Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    // Regardless of success or failure, ServerRemoteProcedure returns and leaves the parent
    // procedure to find out and handle failures. In this case, SnapshotProcedure doesn't
    // care which region server the task is assigned to, so we push down the choice of
    // new target server to SnapshotVerifyProcedure.
    Procedure<MasterProcedureEnv>[] res = super.execute(env);
    if (res == null) {
      if (succ) {
        // remote task has finished
        return null;
      } else {
        // can not send request to remote server, we will try another server
        newTargetServer(env).ifPresent(s -> {
          LOG.warn("retry {} on new target server {}", this, s);
          this.targetServer = s;
          dispatched = false;
        });
        throw new ProcedureYieldException();
      }
    } else {
      return res;
    }
  }

  @Override
  protected void rollback(MasterProcedureEnv env) {
    // nothing to rollback
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    return false;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    SnapshotVerifyProcedureStateData.Builder builder =
      SnapshotVerifyProcedureStateData.newBuilder();
    builder.setTargetServer(ProtobufUtil.toServerName(targetServer));
    builder.setSnapshot(snapshot);
    for (RegionInfo region : regions) {
      builder.addRegion(ProtobufUtil.toRegionInfo(region));
    }
    builder.setExpectedNumRegion(expectedNumRegion);
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    SnapshotVerifyProcedureStateData data =
      serializer.deserialize(SnapshotVerifyProcedureStateData.class);
    this.targetServer = ProtobufUtil.toServerName(data.getTargetServer());
    this.snapshot = data.getSnapshot();
    this.regions = data.getRegionList().stream()
      .map(ProtobufUtil::toRegionInfo).collect(Collectors.toList());
    this.expectedNumRegion = data.getExpectedNumRegion();
  }

  @Override
  public Optional<RemoteProcedureDispatcher.RemoteOperation> remoteCallBuild(
      MasterProcedureEnv env, ServerName serverName) {
    SnapshotVerifyParameter.Builder builder = SnapshotVerifyParameter.newBuilder();
    builder.setSnapshot(snapshot);
    for (RegionInfo region : regions) {
      builder.addRegion(ProtobufUtil.toRegionInfo(region));
    }
    builder.setExpectedNumRegion(expectedNumRegion);
    return Optional.of(new RSProcedureDispatcher.ServerOperation(this, getProcId(),
      SnapshotVerifyCallable.class, builder.build().toByteArray()));
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
    return ServerOperationType.VERIFY_SNAPSHOT;
  }

  private Optional<SnapshotProcedure> getParent(MasterProcedureEnv env) {
    SnapshotProcedure parent = env.getMasterServices().getMasterProcedureExecutor()
      .getProcedure(SnapshotProcedure.class, getParentProcId());
    if (parent == null) {
      LOG.warn("Parent procedure of {} does not exist. Is it created without "
        + "a SnapshotProcedure as parent?", this);
      return Optional.empty();
    } else {
      return Optional.of(parent);
    }
  }

  private Optional<ServerName> newTargetServer(MasterProcedureEnv env) {
    List<ServerName> onlineServers =
      env.getMasterServices().getServerManager().getOnlineServersList();
    onlineServers.remove(targetServer);
    Collections.shuffle(onlineServers);
    if (onlineServers.isEmpty()) {
      LOG.warn("There is no online server, maybe the cluster is shutting down.");
      return Optional.empty();
    } else {
      return Optional.of(onlineServers.get(0));
    }
  }

  @Override
  protected void toStringClassDetails(StringBuilder builder) {
    builder.append(getClass().getSimpleName())
      .append(", snapshot=").append(snapshot.getName())
      .append(", targetServer=").append(targetServer);
  }
}
