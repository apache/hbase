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
import java.util.ArrayList;
import java.util.List;

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
import org.apache.hadoop.hbase.replication.regionserver.ReplaySyncReplicationWALCallable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ReplaySyncReplicationWALParameter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SyncReplicationReplayWALRemoteStateData;

@InterfaceAudience.Private
public class SyncReplicationReplayWALRemoteProcedure extends Procedure<MasterProcedureEnv>
    implements RemoteProcedure<MasterProcedureEnv, ServerName>, PeerProcedureInterface {

  private static final Logger LOG =
      LoggerFactory.getLogger(SyncReplicationReplayWALRemoteProcedure.class);

  private String peerId;

  private ServerName targetServer;

  private List<String> wals;

  private boolean dispatched;

  private ProcedureEvent<?> event;

  private boolean succ;

  public SyncReplicationReplayWALRemoteProcedure() {
  }

  public SyncReplicationReplayWALRemoteProcedure(String peerId, List<String> wals,
      ServerName targetServer) {
    this.peerId = peerId;
    this.wals = wals;
    this.targetServer = targetServer;
  }

  @Override
  public RemoteOperation remoteCallBuild(MasterProcedureEnv env, ServerName remote) {
    ReplaySyncReplicationWALParameter.Builder builder =
        ReplaySyncReplicationWALParameter.newBuilder();
    builder.setPeerId(peerId);
    wals.stream().forEach(builder::addWal);
    return new ServerOperation(this, getProcId(), ReplaySyncReplicationWALCallable.class,
        builder.build().toByteArray());
  }

  @Override
  public void remoteCallFailed(MasterProcedureEnv env, ServerName remote, IOException exception) {
    complete(env, exception);
  }

  @Override
  public void remoteOperationCompleted(MasterProcedureEnv env) {
    complete(env, null);
  }

  @Override
  public void remoteOperationFailed(MasterProcedureEnv env, RemoteProcedureException error) {
    complete(env, error);
  }

  private void complete(MasterProcedureEnv env, Throwable error) {
    if (event == null) {
      LOG.warn("procedure event for {} is null, maybe the procedure is created when recovery",
        getProcId());
      return;
    }
    if (error != null) {
      LOG.warn("Replay wals {} on {} failed for peer id={}", wals, targetServer, peerId, error);
      this.succ = false;
    } else {
      truncateWALs(env);
      LOG.info("Replay wals {} on {} succeed for peer id={}", wals, targetServer, peerId);
      this.succ = true;
    }
    event.wake(env.getProcedureScheduler());
    event = null;
  }

  /**
   * Only truncate wals one by one when task succeed. The parent procedure will check the first
   * wal length to know whether this task succeed.
   */
  private void truncateWALs(MasterProcedureEnv env) {
    String firstWal = wals.get(0);
    try {
      env.getMasterServices().getSyncReplicationReplayWALManager().finishReplayWAL(firstWal);
    } catch (IOException e) {
      // As it is idempotent to rerun this task. Just ignore this exception and return.
      LOG.warn("Failed to truncate wal {} for peer id={}", firstWal, peerId, e);
      return;
    }
    for (int i = 1; i < wals.size(); i++) {
      String wal = wals.get(i);
      try {
        env.getMasterServices().getSyncReplicationReplayWALManager().finishReplayWAL(wal);
      } catch (IOException e1) {
        try {
          // retry
          env.getMasterServices().getSyncReplicationReplayWALManager().finishReplayWAL(wal);
        } catch (IOException e2) {
          // As the parent procedure only check the first wal length. Just ignore this exception.
          LOG.warn("Failed to truncate wal {} for peer id={}", wal, peerId, e2);
        }
      }
    }
  }

  @Override
  protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    if (dispatched) {
      if (succ) {
        return null;
      }
      // retry
      dispatched = false;
    }

    // Dispatch task to target server
    try {
      env.getRemoteDispatcher().addOperationToNode(targetServer, this);
    } catch (FailedRemoteDispatchException e) {
      LOG.warn(
          "Can not add remote operation for replay wals {} on {} for peer id={}, "
              + "this usually because the server is already dead, retry",
          wals, targetServer, peerId);
      throw new ProcedureYieldException();
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
    return false;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    SyncReplicationReplayWALRemoteStateData.Builder builder =
        SyncReplicationReplayWALRemoteStateData.newBuilder().setPeerId(peerId)
            .setTargetServer(ProtobufUtil.toServerName(targetServer));
    wals.stream().forEach(builder::addWal);
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    SyncReplicationReplayWALRemoteStateData data =
        serializer.deserialize(SyncReplicationReplayWALRemoteStateData.class);
    peerId = data.getPeerId();
    wals = new ArrayList<>();
    data.getWalList().forEach(wals::add);
    targetServer = ProtobufUtil.toServerName(data.getTargetServer());
  }

  @Override
  public String getPeerId() {
    return peerId;
  }

  @Override
  public PeerOperationType getPeerOperationType() {
    return PeerOperationType.SYNC_REPLICATION_REPLAY_WAL_REMOTE;
  }
}