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

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RemovePeerStateData;

/**
 * The procedure for removing a replication peer.
 */
@InterfaceAudience.Private
public class RemovePeerProcedure extends ModifyPeerProcedure {

  private static final Logger LOG = LoggerFactory.getLogger(RemovePeerProcedure.class);

  private ReplicationPeerConfig peerConfig;

  public RemovePeerProcedure() {
  }

  public RemovePeerProcedure(String peerId) {
    super(peerId);
  }

  @Override
  public PeerOperationType getPeerOperationType() {
    return PeerOperationType.REMOVE;
  }

  @Override
  protected void prePeerModification(MasterProcedureEnv env) throws IOException {
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preRemoveReplicationPeer(peerId);
    }
    peerConfig = env.getReplicationPeerManager().preRemovePeer(peerId);
  }

  @Override
  protected void updatePeerStorage(MasterProcedureEnv env) throws ReplicationException {
    env.getReplicationPeerManager().removePeer(peerId);
  }

  private void removeRemoteWALs(MasterProcedureEnv env) throws IOException {
    ReplaySyncReplicationWALManager remoteWALManager =
        env.getMasterServices().getReplaySyncReplicationWALManager();
    remoteWALManager.removePeerRemoteWALs(peerId);
    remoteWALManager.removePeerReplayWALDir(peerId);
  }

  @Override
  protected void postPeerModification(MasterProcedureEnv env)
      throws IOException, ReplicationException {
    if (peerConfig.isSyncReplication()) {
      removeRemoteWALs(env);
    }
    env.getReplicationPeerManager().removeAllQueuesAndHFileRefs(peerId);
    if (peerConfig.isSerial()) {
      env.getReplicationPeerManager().removeAllLastPushedSeqIds(peerId);
    }
    LOG.info("Successfully removed peer {}", peerId);
    MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.postRemoveReplicationPeer(peerId);
    }
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    RemovePeerStateData.Builder builder = RemovePeerStateData.newBuilder();
    if (peerConfig != null) {
      builder.setPeerConfig(ReplicationPeerConfigUtil.convert(peerConfig));
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    RemovePeerStateData data = serializer.deserialize(RemovePeerStateData.class);
    if (data.hasPeerConfig()) {
      this.peerConfig = ReplicationPeerConfigUtil.convert(data.getPeerConfig());
    }
  }
}
