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
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.ZKUtilOp;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos;

/**
 * ZK based replication peer storage.
 */
@InterfaceAudience.Private
public class ZKReplicationPeerStorage extends ZKReplicationStorageBase
    implements ReplicationPeerStorage {

  public static final String PEERS_ZNODE = "zookeeper.znode.replication.peers";
  public static final String PEERS_ZNODE_DEFAULT = "peers";

  public static final String PEERS_STATE_ZNODE = "zookeeper.znode.replication.peers.state";
  public static final String PEERS_STATE_ZNODE_DEFAULT = "peer-state";

  public static final byte[] ENABLED_ZNODE_BYTES =
    toByteArray(ReplicationProtos.ReplicationState.State.ENABLED);
  public static final byte[] DISABLED_ZNODE_BYTES =
    toByteArray(ReplicationProtos.ReplicationState.State.DISABLED);

  public static final String SYNC_REPLICATION_STATE_ZNODE = "sync-rep-state";

  public static final String NEW_SYNC_REPLICATION_STATE_ZNODE = "new-sync-rep-state";

  public static final byte[] NONE_STATE_ZNODE_BYTES =
    SyncReplicationState.toByteArray(SyncReplicationState.NONE);

  /**
   * The name of the znode that contains the replication status of a remote slave (i.e. peer)
   * cluster.
   */
  private final String peerStateNodeName;

  /**
   * The name of the znode that contains a list of all remote slave (i.e. peer) clusters.
   */
  private final String peersZNode;

  public ZKReplicationPeerStorage(ZKWatcher zookeeper, Configuration conf) {
    super(zookeeper, conf);
    this.peerStateNodeName = conf.get(PEERS_STATE_ZNODE, PEERS_STATE_ZNODE_DEFAULT);
    String peersZNodeName = conf.get(PEERS_ZNODE, PEERS_ZNODE_DEFAULT);
    this.peersZNode = ZNodePaths.joinZNode(replicationZNode, peersZNodeName);
  }

  @VisibleForTesting
  public String getPeerStateNode(String peerId) {
    return ZNodePaths.joinZNode(getPeerNode(peerId), peerStateNodeName);
  }

  @VisibleForTesting
  public String getPeerNode(String peerId) {
    return ZNodePaths.joinZNode(peersZNode, peerId);
  }

  @VisibleForTesting
  public String getSyncReplicationStateNode(String peerId) {
    return ZNodePaths.joinZNode(getPeerNode(peerId), SYNC_REPLICATION_STATE_ZNODE);
  }

  private String getNewSyncReplicationStateNode(String peerId) {
    return ZNodePaths.joinZNode(getPeerNode(peerId), NEW_SYNC_REPLICATION_STATE_ZNODE);
  }

  @Override
  public void addPeer(String peerId, ReplicationPeerConfig peerConfig, boolean enabled,
      SyncReplicationState syncReplicationState) throws ReplicationException {
    List<ZKUtilOp> multiOps = Arrays.asList(
      ZKUtilOp.createAndFailSilent(getPeerNode(peerId),
        ReplicationPeerConfigUtil.toByteArray(peerConfig)),
      ZKUtilOp.createAndFailSilent(getPeerStateNode(peerId),
        enabled ? ENABLED_ZNODE_BYTES : DISABLED_ZNODE_BYTES),
      ZKUtilOp.createAndFailSilent(getSyncReplicationStateNode(peerId),
        SyncReplicationState.toByteArray(syncReplicationState)),
      ZKUtilOp.createAndFailSilent(getNewSyncReplicationStateNode(peerId), NONE_STATE_ZNODE_BYTES));
    try {
      ZKUtil.createWithParents(zookeeper, peersZNode);
      ZKUtil.multiOrSequential(zookeeper, multiOps, false);
    } catch (KeeperException e) {
      throw new ReplicationException(
        "Could not add peer with id=" + peerId + ", peerConfig=>" + peerConfig + ", state=" +
          (enabled ? "ENABLED" : "DISABLED") + ", syncReplicationState=" + syncReplicationState,
        e);
    }
  }

  @Override
  public void removePeer(String peerId) throws ReplicationException {
    try {
      ZKUtil.deleteNodeRecursively(zookeeper, getPeerNode(peerId));
    } catch (KeeperException e) {
      throw new ReplicationException("Could not remove peer with id=" + peerId, e);
    }
  }

  @Override
  public void setPeerState(String peerId, boolean enabled) throws ReplicationException {
    byte[] stateBytes = enabled ? ENABLED_ZNODE_BYTES : DISABLED_ZNODE_BYTES;
    try {
      ZKUtil.setData(zookeeper, getPeerStateNode(peerId), stateBytes);
    } catch (KeeperException e) {
      throw new ReplicationException("Unable to change state of the peer with id=" + peerId, e);
    }
  }

  @Override
  public void updatePeerConfig(String peerId, ReplicationPeerConfig peerConfig)
      throws ReplicationException {
    try {
      ZKUtil.setData(this.zookeeper, getPeerNode(peerId),
        ReplicationPeerConfigUtil.toByteArray(peerConfig));
    } catch (KeeperException e) {
      throw new ReplicationException(
        "There was a problem trying to save changes to the " + "replication peer " + peerId, e);
    }
  }

  @Override
  public List<String> listPeerIds() throws ReplicationException {
    try {
      List<String> children = ZKUtil.listChildrenNoWatch(zookeeper, peersZNode);
      return children != null ? children : Collections.emptyList();
    } catch (KeeperException e) {
      throw new ReplicationException("Cannot get the list of peers", e);
    }
  }

  @Override
  public boolean isPeerEnabled(String peerId) throws ReplicationException {
    try {
      return Arrays.equals(ENABLED_ZNODE_BYTES,
        ZKUtil.getData(zookeeper, getPeerStateNode(peerId)));
    } catch (KeeperException | InterruptedException e) {
      throw new ReplicationException("Unable to get status of the peer with id=" + peerId, e);
    }
  }

  @Override
  public ReplicationPeerConfig getPeerConfig(String peerId) throws ReplicationException {
    byte[] data;
    try {
      data = ZKUtil.getData(zookeeper, getPeerNode(peerId));
    } catch (KeeperException | InterruptedException e) {
      throw new ReplicationException("Error getting configuration for peer with id=" + peerId, e);
    }
    if (data == null || data.length == 0) {
      throw new ReplicationException(
        "Replication peer config data shouldn't be empty, peerId=" + peerId);
    }
    try {
      return ReplicationPeerConfigUtil.parsePeerFrom(data);
    } catch (DeserializationException e) {
      throw new ReplicationException(
        "Failed to parse replication peer config for peer with id=" + peerId, e);
    }
  }

  @Override
  public void setPeerNewSyncReplicationState(String peerId, SyncReplicationState state)
      throws ReplicationException {
    try {
      ZKUtil.createSetData(zookeeper, getNewSyncReplicationStateNode(peerId),
        SyncReplicationState.toByteArray(state));
    } catch (KeeperException e) {
      throw new ReplicationException(
        "Unable to set the new sync replication state for peer with id=" + peerId, e);
    }
  }

  @Override
  public void transitPeerSyncReplicationState(String peerId) throws ReplicationException {
    String newStateNode = getNewSyncReplicationStateNode(peerId);
    try {
      byte[] data = ZKUtil.getData(zookeeper, newStateNode);
      ZKUtil.multiOrSequential(zookeeper,
        Arrays.asList(ZKUtilOp.setData(newStateNode, NONE_STATE_ZNODE_BYTES),
          ZKUtilOp.setData(getSyncReplicationStateNode(peerId), data)),
        false);
    } catch (KeeperException | InterruptedException e) {
      throw new ReplicationException(
        "Error transiting sync replication state for peer with id=" + peerId, e);
    }
  }

  private SyncReplicationState getSyncReplicationState(String peerId, String path)
      throws ReplicationException {
    try {
      byte[] data = ZKUtil.getData(zookeeper, path);
      return SyncReplicationState.parseFrom(data);
    } catch (KeeperException | InterruptedException | IOException e) {
      throw new ReplicationException(
        "Error getting sync replication state of path " + path + " for peer with id=" + peerId, e);
    }
  }

  @Override
  public SyncReplicationState getPeerNewSyncReplicationState(String peerId)
      throws ReplicationException {
    return getSyncReplicationState(peerId, getNewSyncReplicationStateNode(peerId));
  }

  @Override
  public SyncReplicationState getPeerSyncReplicationState(String peerId)
      throws ReplicationException {
    return getSyncReplicationState(peerId, getSyncReplicationStateNode(peerId));
  }
}
