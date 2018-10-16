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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.PeerProcedureInterface;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher.ServerOperation;
import org.apache.hadoop.hbase.procedure2.FailedRemoteDispatchException;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteProcedure;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.replication.regionserver.RefreshPeerCallable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerModificationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshPeerParameter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshPeerStateData;

@InterfaceAudience.Private
public class RefreshPeerProcedure extends Procedure<MasterProcedureEnv>
    implements PeerProcedureInterface, RemoteProcedure<MasterProcedureEnv, ServerName> {

  private static final Logger LOG = LoggerFactory.getLogger(RefreshPeerProcedure.class);

  private String peerId;

  private PeerOperationType type;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "IS2_INCONSISTENT_SYNC",
      justification = "Will never change after construction")
  private ServerName targetServer;

  private int stage;

  private boolean dispatched;

  private ProcedureEvent<?> event;

  private boolean succ;

  public RefreshPeerProcedure() {
  }

  public RefreshPeerProcedure(String peerId, PeerOperationType type, ServerName targetServer) {
    this(peerId, type, targetServer, 0);
  }

  public RefreshPeerProcedure(String peerId, PeerOperationType type, ServerName targetServer,
      int stage) {
    this.peerId = peerId;
    this.type = type;
    this.targetServer = targetServer;
    this.stage = stage;
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
      case TRANSIT_SYNC_REPLICATION_STATE:
        return PeerModificationType.TRANSIT_SYNC_REPLICATION_STATE;
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
      case TRANSIT_SYNC_REPLICATION_STATE:
        return PeerOperationType.TRANSIT_SYNC_REPLICATION_STATE;
      default:
        throw new IllegalArgumentException("Unknown type: " + type);
    }
  }

  @Override
  public RemoteOperation remoteCallBuild(MasterProcedureEnv env, ServerName remote) {
    assert targetServer.equals(remote);
    return new ServerOperation(this, getProcId(), RefreshPeerCallable.class,
        RefreshPeerParameter.newBuilder().setPeerId(peerId).setType(toPeerModificationType(type))
            .setTargetServer(ProtobufUtil.toServerName(remote)).setStage(stage).build()
            .toByteArray());
  }

  private void complete(MasterProcedureEnv env, Throwable error) {
    if (event == null) {
      LOG.warn("procedure event for {} is null, maybe the procedure is created when recovery",
        getProcId());
      return;
    }
    if (error != null) {
      LOG.warn("Refresh peer {} for {} on {} failed", peerId, type, targetServer, error);
      this.succ = false;
    } else {
      LOG.info("Refresh peer {} for {} on {} suceeded", peerId, type, targetServer);
      this.succ = true;
    }

    event.wake(env.getProcedureScheduler());
    event = null;
  }

  @Override
  public synchronized void remoteCallFailed(MasterProcedureEnv env, ServerName remote,
      IOException exception) {
    complete(env, exception);
  }

  @Override
  public synchronized void remoteOperationCompleted(MasterProcedureEnv env) {
    complete(env, null);
  }

  @Override
  public synchronized void remoteOperationFailed(MasterProcedureEnv env,
      RemoteProcedureException error) {
    complete(env, error);
  }

  @Override
  protected synchronized Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    if (dispatched) {
      if (succ) {
        return null;
      }
      // retry
      dispatched = false;
    }
    try {
      env.getRemoteDispatcher().addOperationToNode(targetServer, this);
    } catch (FailedRemoteDispatchException frde) {
      LOG.info("Can not add remote operation for refreshing peer {} for {} to {}, " +
        "this is usually because the server is already dead, " +
        "give up and mark the procedure as complete", peerId, type, targetServer, frde);
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
    // TODO: no correctness problem if we just ignore this, implement later.
    return false;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    serializer.serialize(
      RefreshPeerStateData.newBuilder().setPeerId(peerId).setType(toPeerModificationType(type))
          .setTargetServer(ProtobufUtil.toServerName(targetServer)).setStage(stage).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    RefreshPeerStateData data = serializer.deserialize(RefreshPeerStateData.class);
    peerId = data.getPeerId();
    type = toPeerOperationType(data.getType());
    targetServer = ProtobufUtil.toServerName(data.getTargetServer());
    stage = data.getStage();
  }
}
