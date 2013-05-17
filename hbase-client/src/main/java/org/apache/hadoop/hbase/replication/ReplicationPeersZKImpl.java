/*
 *
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * This class provides an implementation of the ReplicationPeers interface using Zookeeper. The
 * peers znode contains a list of all peer replication clusters and the current replication state of
 * those clusters. It has one child peer znode for each peer cluster. The peer znode is named with
 * the cluster id provided by the user in the HBase shell. The value of the peer znode contains the
 * peers cluster key provided by the user in the HBase Shell. The cluster key contains a list of
 * zookeeper quorum peers, the client port for the zookeeper quorum, and the base znode for HBase.
 * For example:
 *
 *  /hbase/replication/peers/1 [Value: zk1.host.com,zk2.host.com,zk3.host.com:2181:/hbase]
 *  /hbase/replication/peers/2 [Value: zk5.host.com,zk6.host.com,zk7.host.com:2181:/hbase]
 *
 * Each of these peer znodes has a child znode that indicates whether or not replication is enabled
 * on that peer cluster. These peer-state znodes do not have child znodes and simply contain a
 * boolean value (i.e. ENABLED or DISABLED). This value is read/maintained by the
 * ReplicationPeer.PeerStateTracker class. For example:
 *
 * /hbase/replication/peers/1/peer-state [Value: ENABLED]
 */
public class ReplicationPeersZKImpl extends ReplicationStateZKBase implements ReplicationPeers {

  // Map of peer clusters keyed by their id
  private Map<String, ReplicationPeer> peerClusters;

  private static final Log LOG = LogFactory.getLog(ReplicationPeersZKImpl.class);

  public ReplicationPeersZKImpl(final ZooKeeperWatcher zk, final Configuration conf,
      Abortable abortable) {
    super(zk, conf, abortable);
    this.peerClusters = new HashMap<String, ReplicationPeer>();
  }

  @Override
  public void init() throws IOException, KeeperException {
    ZKUtil.createWithParents(this.zookeeper, this.peersZNode);
    connectExistingPeers();
  }

  @Override
  public void addPeer(String id, String clusterKey) throws IOException {
    try {
      if (peerExists(id)) {
        throw new IllegalArgumentException("Cannot add existing peer");
      }
      ZKUtil.createWithParents(this.zookeeper, this.peersZNode);
      ZKUtil.createAndWatch(this.zookeeper, ZKUtil.joinZNode(this.peersZNode, id),
        toByteArray(clusterKey));
      // There is a race b/w PeerWatcher and ReplicationZookeeper#add method to create the
      // peer-state znode. This happens while adding a peer.
      // The peer state data is set as "ENABLED" by default.
      ZKUtil.createNodeIfNotExistsAndWatch(this.zookeeper, getPeerStateNode(id),
        ENABLED_ZNODE_BYTES);
      // A peer is enabled by default
    } catch (KeeperException e) {
      throw new IOException("Unable to add peer", e);
    }
  }

  @Override
  public void removePeer(String id) throws IOException {
    try {
      if (!peerExists(id)) {
        throw new IllegalArgumentException("Cannot remove inexisting peer");
      }
      ZKUtil.deleteNodeRecursively(this.zookeeper, ZKUtil.joinZNode(this.peersZNode, id));
    } catch (KeeperException e) {
      throw new IOException("Unable to remove a peer", e);
    }
  }

  @Override
  public void enablePeer(String id) throws IOException {
    changePeerState(id, ZooKeeperProtos.ReplicationState.State.ENABLED);
    LOG.info("peer " + id + " is enabled");
  }

  @Override
  public void disablePeer(String id) throws IOException {
    changePeerState(id, ZooKeeperProtos.ReplicationState.State.DISABLED);
    LOG.info("peer " + id + " is disabled");
  }

  @Override
  public boolean getStatusOfConnectedPeer(String id) {
    if (!this.peerClusters.containsKey(id)) {
      throw new IllegalArgumentException("peer " + id + " is not connected");
    }
    return this.peerClusters.get(id).getPeerEnabled().get();
  }

  @Override
  public boolean connectToPeer(String peerId) throws IOException, KeeperException {
    if (peerClusters == null) {
      return false;
    }
    if (this.peerClusters.containsKey(peerId)) {
      return false;
    }
    ReplicationPeer peer = getPeer(peerId);
    if (peer == null) {
      return false;
    }
    this.peerClusters.put(peerId, peer);
    LOG.info("Added new peer cluster " + peer.getClusterKey());
    return true;
  }

  @Override
  public void disconnectFromPeer(String peerId) {
    ReplicationPeer rp = this.peerClusters.get(peerId);
    if (rp != null) {
      rp.getZkw().close();
      this.peerClusters.remove(peerId);
    }
  }

  @Override
  public Map<String, String> getAllPeerClusterKeys() {
    Map<String, String> peers = new TreeMap<String, String>();
    List<String> ids = null;
    try {
      ids = ZKUtil.listChildrenNoWatch(this.zookeeper, this.peersZNode);
      for (String id : ids) {
        byte[] bytes = ZKUtil.getData(this.zookeeper, ZKUtil.joinZNode(this.peersZNode, id));
        String clusterKey = null;
        try {
          clusterKey = parsePeerFrom(bytes);
        } catch (DeserializationException de) {
          LOG.warn("Failed parse of clusterid=" + id + " znode content, continuing.");
          continue;
        }
        peers.put(id, clusterKey);
      }
    } catch (KeeperException e) {
      this.abortable.abort("Cannot get the list of peers ", e);
    }
    return peers;
  }

  @Override
  public List<ServerName> getRegionServersOfConnectedPeer(String peerId) {
    if (this.peerClusters.size() == 0) {
      return Collections.emptyList();
    }
    ReplicationPeer peer = this.peerClusters.get(peerId);
    if (peer == null) {
      return Collections.emptyList();
    }
    List<ServerName> addresses;
    try {
      addresses = fetchSlavesAddresses(peer.getZkw());
    } catch (KeeperException ke) {
      reconnectPeer(ke, peer);
      addresses = Collections.emptyList();
    }
    peer.setRegionServers(addresses);
    return peer.getRegionServers();
  }

  @Override
  public UUID getPeerUUID(String peerId) {
    ReplicationPeer peer = this.peerClusters.get(peerId);
    if (peer == null) {
      return null;
    }
    UUID peerUUID = null;
    try {
      peerUUID = ZKClusterId.getUUIDForCluster(peer.getZkw());
    } catch (KeeperException ke) {
      reconnectPeer(ke, peer);
    }
    return peerUUID;
  }

  @Override
  public Set<String> getConnectedPeers() {
    return this.peerClusters.keySet();
  }

  @Override
  public Configuration getPeerConf(String peerId) throws KeeperException {
    String znode = ZKUtil.joinZNode(this.peersZNode, peerId);
    byte[] data = ZKUtil.getData(this.zookeeper, znode);
    if (data == null) {
      LOG.error("Could not get configuration for peer because it doesn't exist. peerId=" + peerId);
      return null;
    }
    String otherClusterKey = "";
    try {
      otherClusterKey = parsePeerFrom(data);
    } catch (DeserializationException e) {
      LOG.warn("Failed to parse cluster key from peerId=" + peerId
          + ", specifically the content from the following znode: " + znode);
      return null;
    }

    Configuration otherConf = new Configuration(this.conf);
    try {
      ZKUtil.applyClusterKeyToConf(otherConf, otherClusterKey);
    } catch (IOException e) {
      LOG.error("Can't get peer configuration for peerId=" + peerId + " because:", e);
      return null;
    }
    return otherConf;
  }

  /**
   * List all registered peer clusters and set a watch on their znodes.
   */
  @Override
  public List<String> getAllPeerIds() {
    List<String> ids = null;
    try {
      ids = ZKUtil.listChildrenAndWatchThem(this.zookeeper, this.peersZNode);
    } catch (KeeperException e) {
      this.abortable.abort("Cannot get the list of peers ", e);
    }
    return ids;
  }

  /**
   * A private method used during initialization. This method attempts to connect to all registered
   * peer clusters. This method does not set a watch on the peer cluster znodes.
   * @throws IOException
   * @throws KeeperException
   */
  private void connectExistingPeers() throws IOException, KeeperException {
    List<String> znodes = ZKUtil.listChildrenNoWatch(this.zookeeper, this.peersZNode);
    if (znodes != null) {
      for (String z : znodes) {
        connectToPeer(z);
      }
    }
  }

  /**
   * A private method used to re-establish a zookeeper session with a peer cluster.
   * @param ke
   * @param peer
   */
  private void reconnectPeer(KeeperException ke, ReplicationPeer peer) {
    if (ke instanceof ConnectionLossException || ke instanceof SessionExpiredException) {
      LOG.warn("Lost the ZooKeeper connection for peer " + peer.getClusterKey(), ke);
      try {
        peer.reloadZkWatcher();
      } catch (IOException io) {
        LOG.warn("Creation of ZookeeperWatcher failed for peer " + peer.getClusterKey(), io);
      }
    }
  }

  /**
   * Get the list of all the region servers from the specified peer
   * @param zkw zk connection to use
   * @return list of region server addresses or an empty list if the slave is unavailable
   */
  private static List<ServerName> fetchSlavesAddresses(ZooKeeperWatcher zkw)
      throws KeeperException {
    List<String> children = ZKUtil.listChildrenNoWatch(zkw, zkw.rsZNode);
    if (children == null) {
      return Collections.emptyList();
    }
    List<ServerName> addresses = new ArrayList<ServerName>(children.size());
    for (String child : children) {
      addresses.add(ServerName.parseServerName(child));
    }
    return addresses;
  }

  private String getPeerStateNode(String id) {
    return ZKUtil.joinZNode(this.peersZNode, ZKUtil.joinZNode(id, this.peerStateNodeName));
  }

  /**
   * Update the state znode of a peer cluster.
   * @param id
   * @param state
   * @throws IOException
   */
  private void changePeerState(String id, ZooKeeperProtos.ReplicationState.State state)
      throws IOException {
    try {
      if (!peerExists(id)) {
        throw new IllegalArgumentException("peer " + id + " is not registered");
      }
      String peerStateZNode = getPeerStateNode(id);
      byte[] stateBytes =
          (state == ZooKeeperProtos.ReplicationState.State.ENABLED) ? ENABLED_ZNODE_BYTES
              : DISABLED_ZNODE_BYTES;
      if (ZKUtil.checkExists(this.zookeeper, peerStateZNode) != -1) {
        ZKUtil.setData(this.zookeeper, peerStateZNode, stateBytes);
      } else {
        ZKUtil.createAndWatch(this.zookeeper, peerStateZNode, stateBytes);
      }
      LOG.info("state of the peer " + id + " changed to " + state.name());
    } catch (KeeperException e) {
      throw new IOException("Unable to change state of the peer " + id, e);
    }
  }

  /**
   * Helper method to connect to a peer
   * @param peerId peer's identifier
   * @return object representing the peer
   * @throws IOException
   * @throws KeeperException
   */
  private ReplicationPeer getPeer(String peerId) throws IOException, KeeperException {
    Configuration peerConf = getPeerConf(peerId);
    if (peerConf == null) {
      return null;
    }
    if (this.ourClusterKey.equals(ZKUtil.getZooKeeperClusterKey(peerConf))) {
      LOG.debug("Not connecting to " + peerId + " because it's us");
      return null;
    }

    ReplicationPeer peer =
        new ReplicationPeer(peerConf, peerId, ZKUtil.getZooKeeperClusterKey(peerConf));
    peer.startStateTracker(this.zookeeper, this.getPeerStateNode(peerId));
    return peer;
  }

  /**
   * @param bytes Content of a peer znode.
   * @return ClusterKey parsed from the passed bytes.
   * @throws DeserializationException
   */
  private static String parsePeerFrom(final byte[] bytes) throws DeserializationException {
    if (ProtobufUtil.isPBMagicPrefix(bytes)) {
      int pblen = ProtobufUtil.lengthOfPBMagic();
      ZooKeeperProtos.ReplicationPeer.Builder builder =
          ZooKeeperProtos.ReplicationPeer.newBuilder();
      ZooKeeperProtos.ReplicationPeer peer;
      try {
        peer = builder.mergeFrom(bytes, pblen, bytes.length - pblen).build();
      } catch (InvalidProtocolBufferException e) {
        throw new DeserializationException(e);
      }
      return peer.getClusterkey();
    } else {
      if (bytes.length > 0) {
        return Bytes.toString(bytes);
      }
      return "";
    }
  }

  /**
   * @param clusterKey
   * @return Serialized protobuf of <code>clusterKey</code> with pb magic prefix prepended suitable
   *         for use as content of a this.peersZNode; i.e. the content of PEER_ID znode under
   *         /hbase/replication/peers/PEER_ID
   */
  private static byte[] toByteArray(final String clusterKey) {
    byte[] bytes =
        ZooKeeperProtos.ReplicationPeer.newBuilder().setClusterkey(clusterKey).build()
            .toByteArray();
    return ProtobufUtil.prependPBMagic(bytes);
  }
}