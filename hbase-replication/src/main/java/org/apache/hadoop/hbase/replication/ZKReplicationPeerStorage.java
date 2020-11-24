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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.ZKUtilOp;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

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

  public String getPeerStateNode(String peerId) {
    return ZNodePaths.joinZNode(getPeerNode(peerId), peerStateNodeName);
  }

  public String getPeerNode(String peerId) {
    return ZNodePaths.joinZNode(peersZNode, peerId);
  }

  @Override
  public void addPeer(String peerId, ReplicationPeerConfig peerConfig, boolean enabled)
      throws ReplicationException {
    try {
      ZKUtil.createWithParents(zookeeper, peersZNode);
      ZKUtil.multiOrSequential(zookeeper,
        Arrays.asList(
          ZKUtilOp.createAndFailSilent(getPeerNode(peerId),
            ReplicationPeerConfigUtil.toByteArray(peerConfig)),
          ZKUtilOp.createAndFailSilent(getPeerStateNode(peerId),
            enabled ? ENABLED_ZNODE_BYTES : DISABLED_ZNODE_BYTES)),
        false);
    } catch (KeeperException e) {
      throw new ReplicationException("Could not add peer with id=" + peerId + ", peerConfif=>"
          + peerConfig + ", state=" + (enabled ? "ENABLED" : "DISABLED"), e);
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
}
