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
import java.util.concurrent.TimeUnit;

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
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ReplaySyncReplicationWALStateData;

@InterfaceAudience.Private
public class ReplaySyncReplicationWALProcedure extends Procedure<MasterProcedureEnv>
    implements RemoteProcedure<MasterProcedureEnv, ServerName>, PeerProcedureInterface {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReplaySyncReplicationWALProcedure.class);

  private static final long DEFAULT_WAIT_AVAILABLE_SERVER_TIMEOUT = 10000;

  private String peerId;

  private ServerName targetServer = null;

  private String wal;

  private boolean dispatched;

  private ProcedureEvent<?> event;

  private boolean succ;

  public ReplaySyncReplicationWALProcedure() {
  }

  public ReplaySyncReplicationWALProcedure(String peerId, String wal) {
    this.peerId = peerId;
    this.wal = wal;
  }

  @Override
  public RemoteOperation remoteCallBuild(MasterProcedureEnv env, ServerName remote) {
    return new ServerOperation(this, getProcId(), ReplaySyncReplicationWALCallable.class,
        ReplaySyncReplicationWALParameter.newBuilder().setPeerId(peerId).setWal(wal).build()
            .toByteArray());
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
    ReplaySyncReplicationWALManager replaySyncReplicationWALManager =
        env.getMasterServices().getReplaySyncReplicationWALManager();
    if (error != null) {
      LOG.warn("Replay sync replication wal {} on {} failed for peer id={}", wal, targetServer,
        peerId, error);
      this.succ = false;
    } else {
      LOG.warn("Replay sync replication wal {} on {} suceeded for peer id={}", wal, targetServer,
        peerId);
      this.succ = true;
      replaySyncReplicationWALManager.addAvailServer(peerId, targetServer);
    }
    event.wake(env.getProcedureScheduler());
    event = null;
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

    // Try poll a available server
    if (targetServer == null) {
      targetServer = env.getMasterServices().getReplaySyncReplicationWALManager()
          .getAvailServer(peerId, DEFAULT_WAIT_AVAILABLE_SERVER_TIMEOUT, TimeUnit.MILLISECONDS);
      if (targetServer == null) {
        LOG.info("No available server to replay wal {} for peer id={}, retry", wal, peerId);
        throw new ProcedureYieldException();
      }
    }

    // Dispatch task to target server
    try {
      env.getRemoteDispatcher().addOperationToNode(targetServer, this);
    } catch (FailedRemoteDispatchException e) {
      LOG.info(
        "Can not add remote operation for replay wal {} on {} for peer id={}, " +
          "this usually because the server is already dead, " + "retry",
        wal, targetServer, peerId, e);
      targetServer = null;
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
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    ReplaySyncReplicationWALStateData.Builder builder =
        ReplaySyncReplicationWALStateData.newBuilder().setPeerId(peerId).setWal(wal);
    if (targetServer != null) {
      builder.setTargetServer(ProtobufUtil.toServerName(targetServer));
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    ReplaySyncReplicationWALStateData data =
        serializer.deserialize(ReplaySyncReplicationWALStateData.class);
    peerId = data.getPeerId();
    wal = data.getWal();
    if (data.hasTargetServer()) {
      targetServer = ProtobufUtil.toServerName(data.getTargetServer());
    }
  }

  @Override
  public String getPeerId() {
    return peerId;
  }

  @Override
  public PeerOperationType getPeerOperationType() {
    return PeerOperationType.REPLAY_SYNC_REPLICATION_WAL;
  }
}