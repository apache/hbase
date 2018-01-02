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
  protected void prePeerModification(MasterProcedureEnv env) throws IOException {
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preUpdateReplicationPeerConfig(peerId, peerConfig);
    }
    env.getReplicationPeerManager().preUpdatePeerConfig(peerId, peerConfig);
  }

  @Override
  protected void updatePeerStorage(MasterProcedureEnv env) throws ReplicationException {
    env.getReplicationPeerManager().updatePeerConfig(peerId, peerConfig);
  }

  @Override
  protected void postPeerModification(MasterProcedureEnv env) throws IOException {
    LOG.info("Successfully updated peer config of {} to {}", peerId, peerConfig);
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postUpdateReplicationPeerConfig(peerId, peerConfig);
    }
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    serializer.serialize(UpdatePeerConfigStateData.newBuilder()
        .setPeerConfig(ReplicationPeerConfigUtil.convert(peerConfig)).build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    peerConfig = ReplicationPeerConfigUtil
        .convert(serializer.deserialize(UpdatePeerConfigStateData.class).getPeerConfig());
  }
}
