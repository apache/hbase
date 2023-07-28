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
package org.apache.hadoop.hbase.master.replication;

import java.io.IOException;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AddPeerStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerModificationState;

/**
 * The procedure for adding a new replication peer.
 */
@InterfaceAudience.Private
public class AddPeerProcedure extends ModifyPeerProcedure {

  private static final Logger LOG = LoggerFactory.getLogger(AddPeerProcedure.class);

  private ReplicationPeerConfig peerConfig;

  private boolean enabled;

  private boolean cleanerDisabled;

  public AddPeerProcedure() {
  }

  public AddPeerProcedure(String peerId, ReplicationPeerConfig peerConfig, boolean enabled) {
    super(peerId);
    this.peerConfig = peerConfig;
    this.enabled = enabled;
  }

  @Override
  public PeerOperationType getPeerOperationType() {
    return PeerOperationType.ADD;
  }

  @Override
  protected PeerModificationState nextStateAfterRefresh() {
    return peerConfig.isSerial()
      ? PeerModificationState.SERIAL_PEER_REOPEN_REGIONS
      : super.nextStateAfterRefresh();
  }

  @Override
  protected void updateLastPushedSequenceIdForSerialPeer(MasterProcedureEnv env)
    throws IOException, ReplicationException {
    setLastPushedSequenceId(env, peerConfig);
  }

  @Override
  protected boolean enablePeerBeforeFinish() {
    return enabled;
  }

  @Override
  protected ReplicationPeerConfig getNewPeerConfig() {
    return peerConfig;
  }

  @Override
  protected void releaseLatch(MasterProcedureEnv env) {
    if (cleanerDisabled) {
      env.getMasterServices().getReplicationLogCleanerBarrier().enable();
    }
    if (peerConfig.isSyncReplication()) {
      env.getMasterServices().getSyncReplicationPeerLock().release();
    }
    super.releaseLatch(env);
  }

  @Override
  protected void prePeerModification(MasterProcedureEnv env)
    throws IOException, ReplicationException, ProcedureSuspendedException {
    if (!env.getMasterServices().getReplicationLogCleanerBarrier().disable()) {
      throw suspend(env.getMasterConfiguration(),
        backoff -> LOG.warn("LogCleaner is run at the same time when adding peer {}, sleep {} secs",
          peerId, backoff / 1000));
    }
    cleanerDisabled = true;
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preAddReplicationPeer(peerId, peerConfig);
    }
    if (peerConfig.isSyncReplication()) {
      if (!env.getMasterServices().getSyncReplicationPeerLock().tryAcquire()) {
        throw suspend(env.getMasterConfiguration(),
          backoff -> LOG.warn(
            "Can not acquire sync replication peer lock for peer {}, sleep {} secs", peerId,
            backoff / 1000));
      }
    }
    env.getReplicationPeerManager().preAddPeer(peerId, peerConfig);
  }

  @Override
  protected void updatePeerStorage(MasterProcedureEnv env) throws ReplicationException {
    env.getReplicationPeerManager().addPeer(peerId, peerConfig,
      peerConfig.isSerial() ? false : enabled);
  }

  @Override
  protected void postPeerModification(MasterProcedureEnv env)
    throws IOException, ReplicationException {
    LOG.info("Successfully added {} peer {}, config {}", enabled ? "ENABLED" : "DISABLED", peerId,
      peerConfig);
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      env.getMasterCoprocessorHost().postAddReplicationPeer(peerId, peerConfig);
    }
  }

  @Override
  protected void afterReplay(MasterProcedureEnv env) {
    if (getCurrentState() == getInitialState()) {
      // do not need to disable log cleaner or acquire lock if we are in the initial state, later
      // when executing the procedure we will try to disable and acquire.
      return;
    }
    if (!env.getMasterServices().getReplicationLogCleanerBarrier().disable()) {
      throw new IllegalStateException("can not disable log cleaner, this should not happen");
    }
    cleanerDisabled = true;
    if (peerConfig.isSyncReplication()) {
      if (!env.getMasterServices().getSyncReplicationPeerLock().tryAcquire()) {
        throw new IllegalStateException(
          "Can not acquire sync replication peer lock for peer " + peerId);
      }
    }
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    serializer.serialize(AddPeerStateData.newBuilder()
      .setPeerConfig(ReplicationPeerConfigUtil.convert(peerConfig)).setEnabled(enabled).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    AddPeerStateData data = serializer.deserialize(AddPeerStateData.class);
    peerConfig = ReplicationPeerConfigUtil.convert(data.getPeerConfig());
    enabled = data.getEnabled();
  }
}
