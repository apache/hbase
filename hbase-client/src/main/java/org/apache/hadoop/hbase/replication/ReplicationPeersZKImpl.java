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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
import org.apache.hadoop.hbase.zookeeper.ZKUtil.ZKUtilOp;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.AuthFailedException;
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
 *
 * Each of these peer znodes has a child znode that indicates which data will be replicated
 * to the peer cluster. These peer-tableCFs znodes do not have child znodes and only have a
 * table/cf list config. This value is read/maintained by the ReplicationPeer.TableCFsTracker
 * class. For example:
 *
 * /hbase/replication/peers/1/tableCFs [Value: "table1; table2:cf1,cf3; table3:cfx,cfy"]
 */
public class ReplicationPeersZKImpl extends ReplicationStateZKBase implements ReplicationPeers {

  // Map of peer clusters keyed by their id
  private Map<String, ReplicationPeer> peerClusters;
  private final String tableCFsNodeName;

  private static final Log LOG = LogFactory.getLog(ReplicationPeersZKImpl.class);

  public ReplicationPeersZKImpl(final ZooKeeperWatcher zk, final Configuration conf,
      Abortable abortable) {
    super(zk, conf, abortable);
    this.tableCFsNodeName = conf.get("zookeeper.znode.replication.peers.tableCFs", "tableCFs");
    this.peerClusters = new ConcurrentHashMap<String, ReplicationPeer>();
  }

  @Override
  public void init() throws ReplicationException {
    try {
      if (ZKUtil.checkExists(this.zookeeper, this.peersZNode) < 0) {
        ZKUtil.createWithParents(this.zookeeper, this.peersZNode);
      }
    } catch (KeeperException e) {
      throw new ReplicationException("Could not initialize replication peers", e);
    }
    connectExistingPeers();
  }

  @Override
  public void addPeer(String id, String clusterKey) throws ReplicationException {
    addPeer(id, clusterKey, null);
  }

  @Override
  public void addPeer(String id, String clusterKey, String tableCFs) throws ReplicationException {
    try {
      if (peerExists(id)) {
        throw new IllegalArgumentException("Cannot add a peer with id=" + id
            + " because that id already exists.");
      }
      
      if(id.contains("-")){
        throw new IllegalArgumentException("Found invalid peer name:" + id);
      }
      
      ZKUtil.createWithParents(this.zookeeper, this.peersZNode);
      List<ZKUtilOp> listOfOps = new ArrayList<ZKUtil.ZKUtilOp>();
      ZKUtilOp op1 =
          ZKUtilOp.createAndFailSilent(ZKUtil.joinZNode(this.peersZNode, id),
            toByteArray(clusterKey));
      // There is a race (if hbase.zookeeper.useMulti is false)
      // b/w PeerWatcher and ReplicationZookeeper#add method to create the
      // peer-state znode. This happens while adding a peer
      // The peer state data is set as "ENABLED" by default.
      ZKUtilOp op2 = ZKUtilOp.createAndFailSilent(getPeerStateNode(id), ENABLED_ZNODE_BYTES);
      String tableCFsStr = (tableCFs == null) ? "" : tableCFs;
      ZKUtilOp op3 = ZKUtilOp.createAndFailSilent(getTableCFsNode(id), Bytes.toBytes(tableCFsStr));
      listOfOps.add(op1);
      listOfOps.add(op2);
      listOfOps.add(op3);
      ZKUtil.multiOrSequential(this.zookeeper, listOfOps, false);
    } catch (KeeperException e) {
      throw new ReplicationException("Could not add peer with id=" + id + ", clusterKey="
          + clusterKey, e);
    }
  }

  @Override
  public void removePeer(String id) throws ReplicationException {
    try {
      if (!peerExists(id)) {
        throw new IllegalArgumentException("Cannot remove peer with id=" + id
            + " because that id does not exist.");
      }
      ZKUtil.deleteNodeRecursively(this.zookeeper, ZKUtil.joinZNode(this.peersZNode, id));
    } catch (KeeperException e) {
      throw new ReplicationException("Could not remove peer with id=" + id, e);
    }
  }

  @Override
  public void enablePeer(String id) throws ReplicationException {
    changePeerState(id, ZooKeeperProtos.ReplicationState.State.ENABLED);
    LOG.info("peer " + id + " is enabled");
  }

  @Override
  public void disablePeer(String id) throws ReplicationException {
    changePeerState(id, ZooKeeperProtos.ReplicationState.State.DISABLED);
    LOG.info("peer " + id + " is disabled");
  }

  @Override
  public String getPeerTableCFsConfig(String id) throws ReplicationException {
    try {
      if (!peerExists(id)) {
        throw new IllegalArgumentException("peer " + id + " doesn't exist");
      }
      try {
        return Bytes.toString(ZKUtil.getData(this.zookeeper, getTableCFsNode(id)));
      } catch (Exception e) {
        throw new ReplicationException(e);
      }
    } catch (KeeperException e) {
      throw new ReplicationException("Unable to get tableCFs of the peer with id=" + id, e);
    }
  }

  @Override
  public void setPeerTableCFsConfig(String id, String tableCFsStr) throws ReplicationException {
    try {
      if (!peerExists(id)) {
        throw new IllegalArgumentException("Cannot set peer tableCFs because id=" + id
            + " does not exist.");
      }
      String tableCFsZKNode = getTableCFsNode(id);
      byte[] tableCFs = Bytes.toBytes(tableCFsStr);
      if (ZKUtil.checkExists(this.zookeeper, tableCFsZKNode) != -1) {
        ZKUtil.setData(this.zookeeper, tableCFsZKNode, tableCFs);
      } else {
        ZKUtil.createAndWatch(this.zookeeper, tableCFsZKNode, tableCFs);
      }
      LOG.info("Peer tableCFs with id= " + id + " is now " + tableCFsStr);
    } catch (KeeperException e) {
      throw new ReplicationException("Unable to change tableCFs of the peer with id=" + id, e);
    }
  }

  @Override
  public Map<String, List<String>> getTableCFs(String id) throws IllegalArgumentException {
    ReplicationPeer replicationPeer = this.peerClusters.get(id);
    if (replicationPeer == null) {
      throw new IllegalArgumentException("Peer with id= " + id + " is not connected");
    }
    return replicationPeer.getTableCFs();
  }

  @Override
  public boolean getStatusOfConnectedPeer(String id) {
    ReplicationPeer replicationPeer = this.peerClusters.get(id);
    if (replicationPeer == null) {
      throw new IllegalArgumentException("Peer with id= " + id + " is not connected");
    } 
    return replicationPeer.getPeerEnabled().get();
  }

  @Override
  public boolean getStatusOfPeerFromBackingStore(String id) throws ReplicationException {
    try {
      if (!peerExists(id)) {
        throw new IllegalArgumentException("peer " + id + " doesn't exist");
      }
      String peerStateZNode = getPeerStateNode(id);
      try {
        return ReplicationPeer.isStateEnabled(ZKUtil.getData(this.zookeeper, peerStateZNode));
      } catch (KeeperException e) {
        throw new ReplicationException(e);
      } catch (DeserializationException e) {
        throw new ReplicationException(e);
      }
    } catch (KeeperException e) {
      throw new ReplicationException("Unable to get status of the peer with id=" + id +
          " from backing store", e);
    }
  }

  @Override
  public boolean connectToPeer(String peerId) throws ReplicationException {
    if (peerClusters == null) {
      return false;
    }
    if (this.peerClusters.containsKey(peerId)) {
      return false;
    }
    ReplicationPeer peer = null;
    try {
      peer = getPeer(peerId);
    } catch (Exception e) {
      throw new ReplicationException("Error connecting to peer with id=" + peerId, e);
    }
    if (peer == null) {
      return false;
    }
    ReplicationPeer previous =
      ((ConcurrentMap<String, ReplicationPeer>) peerClusters).putIfAbsent(peerId, peer);
    if (previous == null) {
      LOG.info("Added new peer cluster=" + peer.getClusterKey());
    } else {
      LOG.info("Peer already present, " + previous.getClusterKey() + ", new cluster=" +
        peer.getClusterKey());
    }
    return true;
  }

  @Override
  public void disconnectFromPeer(String peerId) {
    ReplicationPeer rp = this.peerClusters.get(peerId);
    if (rp != null) {
      rp.getZkw().close();
      ((ConcurrentMap<String, ReplicationPeer>) peerClusters).remove(peerId, rp);
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
    // Synchronize peer cluster connection attempts to avoid races and rate
    // limit connections when multiple replication sources try to connect to
    // the peer cluster. If the peer cluster is down we can get out of control
    // over time.
    synchronized (peer) {
      List<ServerName> addresses;
      try {
        addresses = fetchSlavesAddresses(peer.getZkw());
      } 
      catch (KeeperException ke) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Fetch salves addresses failed.", ke);
        }
        reconnectPeer(ke, peer);
        addresses = Collections.emptyList();
      }
      peer.setRegionServers(addresses);
    }
    
    return peer.getRegionServers();
  }

  @Override
  public UUID getPeerUUID(String peerId) {
    ReplicationPeer peer = this.peerClusters.get(peerId);
    if (peer == null) {
      return null;
    }
    UUID peerUUID = null;
    // Synchronize peer cluster connection attempts to avoid races and rate
    // limit connections when multiple replication sources try to connect to
    // the peer cluster. If the peer cluster is down we can get out of control
    // over time.
    synchronized (peer) {
      try {
        peerUUID = ZKClusterId.getUUIDForCluster(peer.getZkw());
      } catch (KeeperException ke) {
        reconnectPeer(ke, peer);
      }
    }
    return peerUUID;
  }

  @Override
  public Set<String> getConnectedPeers() {
    return this.peerClusters.keySet();
  }

  @Override
  public Configuration getPeerConf(String peerId) throws ReplicationException {
    String znode = ZKUtil.joinZNode(this.peersZNode, peerId);
    byte[] data = null;
    try {
      data = ZKUtil.getData(this.zookeeper, znode);
    } catch (KeeperException e) {
      throw new ReplicationException("Error getting configuration for peer with id="
          + peerId, e);
    }
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

  @Override
  public long getTimestampOfLastChangeToPeer(String peerId) {
    ReplicationPeer peer = this.peerClusters.get(peerId);
    if (peer == null) {
      throw new IllegalArgumentException("Unknown peer id: " + peerId);
    }
    return peer.getLastRegionserverUpdate();
  }

  /**
   * A private method used during initialization. This method attempts to connect to all registered
   * peer clusters. This method does not set a watch on the peer cluster znodes.
   */
  private void connectExistingPeers() throws ReplicationException {
    List<String> znodes = null;
    try {
      znodes = ZKUtil.listChildrenNoWatch(this.zookeeper, this.peersZNode);
    } catch (KeeperException e) {
      throw new ReplicationException("Error getting the list of peer clusters.", e);
    }
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
    if (ke instanceof ConnectionLossException || ke instanceof SessionExpiredException
        || ke instanceof AuthFailedException) {
      LOG.warn("Lost the ZooKeeper connection for peer " + peer.getClusterKey(), ke);
      try {
        peer.reloadZkWatcher();
        peer.getZkw().registerListener(new PeerRegionServerListener(peer));
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
    List<String> children = ZKUtil.listChildrenAndWatchForNewChildren(zkw, zkw.rsZNode);
    if (children == null) {
      return Collections.emptyList();
    }
    List<ServerName> addresses = new ArrayList<ServerName>(children.size());
    for (String child : children) {
      addresses.add(ServerName.parseServerName(child));
    }
    return addresses;
  }

  private String getTableCFsNode(String id) {
    return ZKUtil.joinZNode(this.peersZNode, ZKUtil.joinZNode(id, this.tableCFsNodeName));
  }

  private String getPeerStateNode(String id) {
    return ZKUtil.joinZNode(this.peersZNode, ZKUtil.joinZNode(id, this.peerStateNodeName));
  }

  /**
   * Update the state znode of a peer cluster.
   * @param id
   * @param state
   */
  private void changePeerState(String id, ZooKeeperProtos.ReplicationState.State state)
      throws ReplicationException {
    try {
      if (!peerExists(id)) {
        throw new IllegalArgumentException("Cannot enable/disable peer because id=" + id
            + " does not exist.");
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
      LOG.info("Peer with id= " + id + " is now " + state.name());
    } catch (KeeperException e) {
      throw new ReplicationException("Unable to change state of the peer with id=" + id, e);
    }
  }

  /**
   * Helper method to connect to a peer
   * @param peerId peer's identifier
   * @return object representing the peer
   * @throws ReplicationException
   */
  private ReplicationPeer getPeer(String peerId) throws ReplicationException {
    Configuration peerConf = getPeerConf(peerId);
    if (peerConf == null) {
      return null;
    }
    if (this.ourClusterKey.equals(ZKUtil.getZooKeeperClusterKey(peerConf))) {
      LOG.debug("Not connecting to " + peerId + " because it's us");
      return null;
    }

    ReplicationPeer peer =
        new ReplicationPeer(peerConf, peerId);
    try {
      peer.startStateTracker(this.zookeeper, this.getPeerStateNode(peerId));
    } catch (KeeperException e) {
      throw new ReplicationException("Error starting the peer state tracker for peerId=" +
          peerId, e);
    }

    try {
      peer.startTableCFsTracker(this.zookeeper, this.getTableCFsNode(peerId));
    } catch (KeeperException e) {
      throw new ReplicationException("Error starting the peer tableCFs tracker for peerId=" +
          peerId, e);
    }

    peer.getZkw().registerListener(new PeerRegionServerListener(peer));
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

  /**
   * Tracks changes to the list of region servers in a peer's cluster.
   */
  public static class PeerRegionServerListener extends ZooKeeperListener {

    private ReplicationPeer peer;
    private String regionServerListNode;

    public PeerRegionServerListener(ReplicationPeer replicationPeer) {
      super(replicationPeer.getZkw());
      this.peer = replicationPeer;
      this.regionServerListNode = peer.getZkw().rsZNode;
    }

    public PeerRegionServerListener(String regionServerListNode, ZooKeeperWatcher zkw) {
      super(zkw);
      this.regionServerListNode = regionServerListNode;
    }

    @Override
    public synchronized void nodeChildrenChanged(String path) {
      if (path.equals(regionServerListNode)) {
        try {
          LOG.info("Detected change to peer regionservers, fetching updated list");
          peer.setRegionServers(fetchSlavesAddresses(peer.getZkw()));
        } catch (KeeperException e) {
          LOG.fatal("Error reading slave addresses", e);
        }
      }
    }

  }
}
