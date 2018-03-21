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
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.UpdatePeerConfigStateData;

/**
 * The procedure for updating the config for a replication peer.
 */
@InterfaceAudience.Private
public class UpdatePeerConfigProcedure extends ModifyPeerProcedure {

  private static final Logger LOG = LoggerFactory.getLogger(UpdatePeerConfigProcedure.class);

  private ReplicationPeerConfig peerConfig;

  private ReplicationPeerConfig oldPeerConfig;

  private boolean enabled;

  public UpdatePeerConfigProcedure() {
  }

  public UpdatePeerConfigProcedure(String peerId, ReplicationPeerConfig peerConfig) {
    super(peerId);
    this.peerConfig = peerConfig;
  }

  @Override
  public PeerOperationType getPeerOperationType() {
    return PeerOperationType.UPDATE_CONFIG;
  }

  @Override
  protected boolean reopenRegionsAfterRefresh() {
    // If we remove some tables from the peer config then we do not need to enter the extra states
    // for serial replication. Could try to optimize later since it is not easy to determine this...
    return peerConfig.isSerial() && (!oldPeerConfig.isSerial() ||
      !ReplicationUtils.isNamespacesAndTableCFsEqual(peerConfig, oldPeerConfig));
  }

  @Override
  protected boolean enablePeerBeforeFinish() {
    // do not need to test reopenRegionsAfterRefresh since we can only enter here if
    // reopenRegionsAfterRefresh returns true.
    return enabled;
  }

  @Override
  protected ReplicationPeerConfig getOldPeerConfig() {
    return oldPeerConfig;
  }

  @Override
  protected ReplicationPeerConfig getNewPeerConfig() {
    return peerConfig;
  }

  @Override
  protected void prePeerModification(MasterProcedureEnv env) throws IOException {
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preUpdateReplicationPeerConfig(peerId, peerConfig);
    }
    ReplicationPeerDescription desc =
      env.getReplicationPeerManager().preUpdatePeerConfig(peerId, peerConfig);
    oldPeerConfig = desc.getPeerConfig();
    enabled = desc.isEnabled();
  }

  @Override
  protected void updatePeerStorage(MasterProcedureEnv env) throws ReplicationException {
    env.getReplicationPeerManager().updatePeerConfig(peerId, peerConfig);
    if (enabled && reopenRegionsAfterRefresh()) {
      env.getReplicationPeerManager().disablePeer(peerId);
    }
  }

  @Override
  protected void postPeerModification(MasterProcedureEnv env)
      throws IOException, ReplicationException {
    LOG.info("Successfully updated peer config of {} to {}", peerId, peerConfig);
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postUpdateReplicationPeerConfig(peerId, peerConfig);
    }
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    UpdatePeerConfigStateData.Builder builder = UpdatePeerConfigStateData.newBuilder()
      .setPeerConfig(ReplicationPeerConfigUtil.convert(peerConfig));
    if (oldPeerConfig != null) {
      builder.setOldPeerConfig(ReplicationPeerConfigUtil.convert(oldPeerConfig));
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    UpdatePeerConfigStateData data = serializer.deserialize(UpdatePeerConfigStateData.class);
    peerConfig = ReplicationPeerConfigUtil.convert(data.getPeerConfig());
    if (data.hasOldPeerConfig()) {
      oldPeerConfig = ReplicationPeerConfigUtil.convert(data.getOldPeerConfig());
    } else {
      oldPeerConfig = null;
    }
  }
}
