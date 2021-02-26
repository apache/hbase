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
package org.apache.hadoop.hbase.master.replication;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.PeerProcedureInterface;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher.ServerOperation;
import org.apache.hadoop.hbase.master.procedure.ServerRemoteProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteProcedure;
import org.apache.hadoop.hbase.replication.regionserver.RefreshPeerCallable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerModificationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshPeerParameter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshPeerStateData;

@InterfaceAudience.Private
public class RefreshPeerProcedure extends ServerRemoteProcedure
    implements PeerProcedureInterface, RemoteProcedure<MasterProcedureEnv, ServerName> {

  private static final Logger LOG = LoggerFactory.getLogger(RefreshPeerProcedure.class);

  private String peerId;
  private PeerOperationType type;

  public RefreshPeerProcedure() {
  }

  public RefreshPeerProcedure(String peerId, PeerOperationType type, ServerName targetServer) {
    this.peerId = peerId;
    this.type = type;
    this.targetServer = targetServer;
  }

  @Override
  public String getPeerId() {
    return peerId;
  }

  @Override
  public PeerOperationType getPeerOperationType() {
    return PeerOperationType.REFRESH;
  }

  private static PeerModificationType toPeerModificationType(PeerOperationType type) {
    switch (type) {
      case ADD:
        return PeerModificationType.ADD_PEER;
      case REMOVE:
        return PeerModificationType.REMOVE_PEER;
      case ENABLE:
        return PeerModificationType.ENABLE_PEER;
      case DISABLE:
        return PeerModificationType.DISABLE_PEER;
      case UPDATE_CONFIG:
        return PeerModificationType.UPDATE_PEER_CONFIG;
      default:
        throw new IllegalArgumentException("Unknown type: " + type);
    }
  }

  private static PeerOperationType toPeerOperationType(PeerModificationType type) {
    switch (type) {
      case ADD_PEER:
        return PeerOperationType.ADD;
      case REMOVE_PEER:
        return PeerOperationType.REMOVE;
      case ENABLE_PEER:
        return PeerOperationType.ENABLE;
      case DISABLE_PEER:
        return PeerOperationType.DISABLE;
      case UPDATE_PEER_CONFIG:
        return PeerOperationType.UPDATE_CONFIG;
      default:
        throw new IllegalArgumentException("Unknown type: " + type);
    }
  }

  @Override
  public Optional<RemoteOperation> remoteCallBuild(MasterProcedureEnv env, ServerName remote) {
    assert targetServer.equals(remote);
    return Optional.of(new ServerOperation(this, getProcId(), RefreshPeerCallable.class,
        RefreshPeerParameter.newBuilder().setPeerId(peerId).setType(toPeerModificationType(type))
            .setTargetServer(ProtobufUtil.toServerName(remote)).build().toByteArray()));
  }

  @Override
  protected void complete(MasterProcedureEnv env, Throwable error) {
    if (error != null) {
      LOG.warn("Refresh peer {} for {} on {} failed", peerId, type, targetServer, error);
      this.succ = false;
    } else {
      LOG.info("Refresh peer {} for {} on {} suceeded", peerId, type, targetServer);
      this.succ = true;
    }
  }

  @Override
  protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    // TODO: no correctness problem if we just ignore this, implement later.
    return false;
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
    return env.waitInitialized(this);
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    serializer.serialize(
      RefreshPeerStateData.newBuilder().setPeerId(peerId).setType(toPeerModificationType(type))
          .setTargetServer(ProtobufUtil.toServerName(targetServer)).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    RefreshPeerStateData data = serializer.deserialize(RefreshPeerStateData.class);
    peerId = data.getPeerId();
    type = toPeerOperationType(data.getType());
    targetServer = ProtobufUtil.toServerName(data.getTargetServer());
  }
}
