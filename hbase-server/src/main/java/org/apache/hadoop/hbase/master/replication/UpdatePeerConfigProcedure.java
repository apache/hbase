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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerModificationState;
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

  private void addToList(List<String> encodedRegionNames, String encodedRegionName,
      ReplicationQueueStorage queueStorage) throws ReplicationException {
    encodedRegionNames.add(encodedRegionName);
    if (encodedRegionNames.size() >= UPDATE_LAST_SEQ_ID_BATCH_SIZE) {
      queueStorage.removeLastSequenceIds(peerId, encodedRegionNames);
      encodedRegionNames.clear();
    }
  }

  @Override
  protected PeerModificationState nextStateAfterRefresh() {
    if (peerConfig.isSerial()) {
      if (oldPeerConfig.isSerial()) {
        // both serial, then if the ns/table-cfs configs are not changed, just go with the normal
        // way, otherwise we need to reopen the regions for the newly added tables.
        return ReplicationUtils.isNamespacesAndTableCFsEqual(peerConfig, oldPeerConfig)
          ? super.nextStateAfterRefresh()
          : PeerModificationState.SERIAL_PEER_REOPEN_REGIONS;
      } else {
        // we change the peer to serial, need to reopen all regions
        return PeerModificationState.SERIAL_PEER_REOPEN_REGIONS;
      }
    } else {
      if (oldPeerConfig.isSerial()) {
        // we remove the serial flag for peer, then we do not need to reopen all regions, but we
        // need to remove the last pushed sequence ids.
        return PeerModificationState.SERIAL_PEER_UPDATE_LAST_PUSHED_SEQ_ID;
      } else {
        // not serial for both, just go with the normal way.
        return super.nextStateAfterRefresh();
      }
    }
  }

  @Override
  protected void updateLastPushedSequenceIdForSerialPeer(MasterProcedureEnv env)
      throws IOException, ReplicationException {
    if (!oldPeerConfig.isSerial()) {
      assert peerConfig.isSerial();
      // change to serial
      setLastPushedSequenceId(env, peerConfig);
      return;
    }
    if (!peerConfig.isSerial()) {
      // remove the serial flag
      env.getReplicationPeerManager().removeAllLastPushedSeqIds(peerId);
      return;
    }
    // enter here means peerConfig and oldPeerConfig are both serial, let's find out the diffs and
    // process them
    ReplicationQueueStorage queueStorage = env.getReplicationPeerManager().getQueueStorage();
    Connection conn = env.getMasterServices().getConnection();
    Map<String, Long> lastSeqIds = new HashMap<String, Long>();
    List<String> encodedRegionNames = new ArrayList<>();
    for (TableDescriptor td : env.getMasterServices().getTableDescriptors().getAll().values()) {
      if (!td.hasGlobalReplicationScope()) {
        continue;
      }
      TableName tn = td.getTableName();
      if (oldPeerConfig.needToReplicate(tn)) {
        if (!peerConfig.needToReplicate(tn)) {
          // removed from peer config
          for (String encodedRegionName : MetaTableAccessor
            .getTableEncodedRegionNamesForSerialReplication(conn, tn)) {
            addToList(encodedRegionNames, encodedRegionName, queueStorage);
          }
        }
      } else if (peerConfig.needToReplicate(tn)) {
        // newly added to peer config
        setLastPushedSequenceIdForTable(env, tn, lastSeqIds);
      }
    }
    if (!encodedRegionNames.isEmpty()) {
      queueStorage.removeLastSequenceIds(peerId, encodedRegionNames);
    }
    if (!lastSeqIds.isEmpty()) {
      queueStorage.setLastSequenceIds(peerId, lastSeqIds);
    }
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
    // if we need to jump to the special states for serial peers, then we need to disable the peer
    // first if it is not disabled yet.
    if (enabled && nextStateAfterRefresh() != super.nextStateAfterRefresh()) {
      env.getReplicationPeerManager().disablePeer(peerId);
    }
  }

  @Override
  protected void postPeerModification(MasterProcedureEnv env)
      throws IOException, ReplicationException {
    if (oldPeerConfig.isSerial() && !peerConfig.isSerial()) {
      env.getReplicationPeerManager().removeAllLastPushedSeqIds(peerId);
    }
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
    builder.setEnabled(enabled);
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
    enabled = data.getEnabled();
  }
}
