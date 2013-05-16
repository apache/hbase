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

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class serves as a helper for all things related to zookeeper in
 * replication.
 * <p/>
 * The layout looks something like this under zookeeper.znode.parent for the
 * master cluster:
 * <p/>
 *
 * <pre>
 * replication/
 *  state      {contains true or false}
 *  clusterId  {contains a byte}
 *  peers/
 *    1/   {contains a full cluster address}
 *      peer-state  {contains ENABLED or DISABLED}
 *    2/
 *    ...
 *  rs/ {lists all RS that replicate}
 *    startcode1/ {lists all peer clusters}
 *      1/ {lists hlogs to process}
 *        10.10.1.76%3A53488.123456789 {contains nothing or a position}
 *        10.10.1.76%3A53488.123456790
 *        ...
 *      2/
 *      ...
 *    startcode2/
 *    ...
 * </pre>
 */
@InterfaceAudience.Private
public class ReplicationZookeeper extends ReplicationStateZKBase implements Closeable {
  private static final Log LOG =
    LogFactory.getLog(ReplicationZookeeper.class);

  // Our handle on zookeeper
  private final ZooKeeperWatcher zookeeper;
  // Map of peer clusters keyed by their id
  private Map<String, ReplicationPeer> peerClusters;
  // Path to the root replication znode
  private String replicationZNode;
  // Path to the peer clusters znode
  private String peersZNode;
  // Path to the znode that contains all RS that replicates
  private String rsZNode;
  // Path to this region server's name under rsZNode
  private String rsServerNameZnode;
  // Name node if the replicationState znode
  private String replicationStateNodeName;
  // Name of zk node which stores peer state. The peer-state znode is under a
  // peers' id node; e.g. /hbase/replication/peers/PEER_ID/peer-state
  private String peerStateNodeName;
  private final Configuration conf;
  // The key to our own cluster
  private String ourClusterKey;
  // Abortable
  private Abortable abortable;
  private final ReplicationStateInterface replicationState;
  private final ReplicationQueues replicationQueues;

  /**
   * ZNode content if enabled state.
   */
  // Public so it can be seen by test code.
  public static final byte[] ENABLED_ZNODE_BYTES =
      toByteArray(ZooKeeperProtos.ReplicationState.State.ENABLED);

  /**
   * ZNode content if disabled state.
   */
  static final byte[] DISABLED_ZNODE_BYTES =
      toByteArray(ZooKeeperProtos.ReplicationState.State.DISABLED);

  /**
   * Constructor used by clients of replication (like master and HBase clients)
   * @param conf  conf to use
   * @param zk    zk connection to use
   * @throws IOException
   */
  public ReplicationZookeeper(final Abortable abortable, final Configuration conf,
      final ZooKeeperWatcher zk) throws KeeperException {
    super(zk, conf, abortable);
    this.conf = conf;
    this.zookeeper = zk;
    setZNodes(abortable);
    this.replicationState = new ReplicationStateImpl(this.zookeeper, conf, abortable);
    // TODO This interface is no longer used by anyone using this constructor. When this class goes
    // away, we will no longer have this null initialization business
    this.replicationQueues = null;
  }

  /**
   * Constructor used by region servers, connects to the peer cluster right away.
   *
   * @param server
   * @param replicating    atomic boolean to start/stop replication
   * @throws IOException
   * @throws KeeperException
   */
  public ReplicationZookeeper(final Server server, final AtomicBoolean replicating)
  throws IOException, KeeperException {
    super(server.getZooKeeper(), server.getConfiguration(), server);
    this.abortable = server;
    this.zookeeper = server.getZooKeeper();
    this.conf = server.getConfiguration();
    setZNodes(server);

    this.replicationState = new ReplicationStateImpl(this.zookeeper, conf, server, replicating);
    this.peerClusters = new HashMap<String, ReplicationPeer>();
    ZKUtil.createWithParents(this.zookeeper,
        ZKUtil.joinZNode(this.replicationZNode, this.replicationStateNodeName));
    this.rsServerNameZnode = ZKUtil.joinZNode(rsZNode, server.getServerName().toString());
    ZKUtil.createWithParents(this.zookeeper, this.rsServerNameZnode);
    this.replicationQueues = new ReplicationQueuesZKImpl(this.zookeeper, this.conf, server);
    this.replicationQueues.init(server.getServerName().toString());
    connectExistingPeers();
  }

  private void setZNodes(Abortable abortable) throws KeeperException {
    String replicationZNodeName = conf.get("zookeeper.znode.replication", "replication");
    String peersZNodeName = conf.get("zookeeper.znode.replication.peers", "peers");
    this.peerStateNodeName = conf.get("zookeeper.znode.replication.peers.state", "peer-state");
    this.replicationStateNodeName = conf.get("zookeeper.znode.replication.state", "state");
    String rsZNodeName = conf.get("zookeeper.znode.replication.rs", "rs");
    this.ourClusterKey = ZKUtil.getZooKeeperClusterKey(this.conf);
    this.replicationZNode = ZKUtil.joinZNode(this.zookeeper.baseZNode, replicationZNodeName);
    this.peersZNode = ZKUtil.joinZNode(replicationZNode, peersZNodeName);
    ZKUtil.createWithParents(this.zookeeper, this.peersZNode);
    this.rsZNode = ZKUtil.joinZNode(replicationZNode, rsZNodeName);
    ZKUtil.createWithParents(this.zookeeper, this.rsZNode);
  }

  private void connectExistingPeers() throws IOException, KeeperException {
    List<String> znodes = ZKUtil.listChildrenNoWatch(this.zookeeper, this.peersZNode);
    if (znodes != null) {
      for (String z : znodes) {
        connectToPeer(z);
      }
    }
  }

  /**
   * List this cluster's peers' IDs
   * @return list of all peers' identifiers
   */
  public List<String> listPeersIdsAndWatch() {
    List<String> ids = null;
    try {
      ids = ZKUtil.listChildrenAndWatchThem(this.zookeeper, this.peersZNode);
    } catch (KeeperException e) {
      this.abortable.abort("Cannot get the list of peers ", e);
    }
    return ids;
  }

  /**
   * Map of this cluster's peers for display.
   * @return A map of peer ids to peer cluster keys
   */
  public Map<String,String> listPeers() {
    Map<String,String> peers = new TreeMap<String,String>();
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

  /**
   * Returns all region servers from given peer
   *
   * @param peerClusterId (byte) the cluster to interrogate
   * @return addresses of all region servers
   */
  public List<ServerName> getSlavesAddresses(String peerClusterId) {
    if (this.peerClusters.size() == 0) {
      return Collections.emptyList();
    }
    ReplicationPeer peer = this.peerClusters.get(peerClusterId);
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

  /**
   * Get the list of all the region servers from the specified peer
   * @param zkw zk connection to use
   * @return list of region server addresses or an empty list if the slave
   * is unavailable
   */
  private List<ServerName> fetchSlavesAddresses(ZooKeeperWatcher zkw)
    throws KeeperException {
    return listChildrenAndGetAsServerNames(zkw, zkw.rsZNode);
  }

  /**
   * Lists the children of the specified znode, retrieving the data of each
   * child as a server address.
   *
   * Used to list the currently online regionservers and their addresses.
   *
   * Sets no watches at all, this method is best effort.
   *
   * Returns an empty list if the node has no children.  Returns null if the
   * parent node itself does not exist.
   *
   * @param zkw zookeeper reference
   * @param znode node to get children of as addresses
   * @return list of data of children of specified znode, empty if no children,
   *         null if parent does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static List<ServerName> listChildrenAndGetAsServerNames(
      ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    List<String> children = ZKUtil.listChildrenNoWatch(zkw, znode);
    if(children == null) {
      return Collections.emptyList();
    }
    List<ServerName> addresses = new ArrayList<ServerName>(children.size());
    for (String child : children) {
      addresses.add(ServerName.parseServerName(child));
    }
    return addresses;
  }

  /**
   * This method connects this cluster to another one and registers it
   * in this region server's replication znode
   * @param peerId id of the peer cluster
   * @throws KeeperException 
   */
  public boolean connectToPeer(String peerId)
      throws IOException, KeeperException {
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
    ZKUtil.createWithParents(this.zookeeper, ZKUtil.joinZNode(
        this.rsServerNameZnode, peerId));
    LOG.info("Added new peer cluster " + peer.getClusterKey());
    return true;
  }

  /**
   * Helper method to connect to a peer
   * @param peerId peer's identifier
   * @return object representing the peer
   * @throws IOException
   * @throws KeeperException
   */
  public ReplicationPeer getPeer(String peerId) throws IOException, KeeperException{
    String znode = ZKUtil.joinZNode(this.peersZNode, peerId);
    byte [] data = ZKUtil.getData(this.zookeeper, znode);
    String otherClusterKey = "";
    try {
      otherClusterKey = parsePeerFrom(data);
    } catch (DeserializationException e) {
      LOG.warn("Failed parse of cluster key from peerId=" + peerId
          + ", specifically the content from the following znode: " + znode);
    }
    if (this.ourClusterKey.equals(otherClusterKey)) {
      LOG.debug("Not connecting to " + peerId + " because it's us");
      return null;
    }
    // Construct the connection to the new peer
    Configuration otherConf = new Configuration(this.conf);
    try {
      ZKUtil.applyClusterKeyToConf(otherConf, otherClusterKey);
    } catch (IOException e) {
      LOG.error("Can't get peer because:", e);
      return null;
    }

    ReplicationPeer peer = new ReplicationPeer(otherConf, peerId,
        otherClusterKey);
    peer.startStateTracker(this.zookeeper, this.getPeerStateNode(peerId));
    return peer;
  }

  /**
   * Remove the peer from zookeeper. which will trigger the watchers on every
   * region server and close their sources
   * @param id
   * @throws IllegalArgumentException Thrown when the peer doesn't exist
   */
  public void removePeer(String id) throws IOException {
    try {
      if (!peerExists(id)) {
        throw new IllegalArgumentException("Cannot remove inexisting peer");
      }
      ZKUtil.deleteNodeRecursively(this.zookeeper,
          ZKUtil.joinZNode(this.peersZNode, id));
    } catch (KeeperException e) {
      throw new IOException("Unable to remove a peer", e);
    }
  }

  /**
   * Add a new peer to this cluster
   * @param id peer's identifier
   * @param clusterKey ZK ensemble's addresses, client port and root znode
   * @throws IllegalArgumentException Thrown when the peer doesn't exist
   * @throws IllegalStateException Thrown when a peer already exists, since
   *         multi-slave isn't supported yet.
   */
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

  /**
   * @param clusterKey
   * @return Serialized protobuf of <code>clusterKey</code> with pb magic prefix
   *         prepended suitable for use as content of a this.peersZNode; i.e.
   *         the content of PEER_ID znode under /hbase/replication/peers/PEER_ID
   */
  static byte[] toByteArray(final String clusterKey) {
    byte[] bytes = ZooKeeperProtos.ReplicationPeer.newBuilder().setClusterkey(clusterKey).build()
        .toByteArray();
    return ProtobufUtil.prependPBMagic(bytes);
  }

  /**
   * @param state
   * @return Serialized protobuf of <code>state</code> with pb magic prefix
   *         prepended suitable for use as content of either the cluster state
   *         znode -- whether or not we should be replicating kept in
   *         /hbase/replication/state -- or as content of a peer-state znode
   *         under a peer cluster id as in
   *         /hbase/replication/peers/PEER_ID/peer-state.
   */
  static byte[] toByteArray(final ZooKeeperProtos.ReplicationState.State state) {
    byte[] bytes = ZooKeeperProtos.ReplicationState.newBuilder().setState(state).build()
        .toByteArray();
    return ProtobufUtil.prependPBMagic(bytes);
  }

  /**
   * @param position
   * @return Serialized protobuf of <code>position</code> with pb magic prefix
   *         prepended suitable for use as content of an hlog position in a
   *         replication queue.
   */
  public static byte[] positionToByteArray(
      final long position) {
    return ZKUtil.positionToByteArray(position);
  }

  /**
   * @param lockOwner
   * @return Serialized protobuf of <code>lockOwner</code> with pb magic prefix
   *         prepended suitable for use as content of an replication lock during
   *         region server fail over.
   */
  static byte[] lockToByteArray(
      final String lockOwner) {
    byte[] bytes = ZooKeeperProtos.ReplicationLock.newBuilder().setLockOwner(lockOwner).build()
        .toByteArray();
    return ProtobufUtil.prependPBMagic(bytes);
  }

  /**
   * @param bytes Content of a peer znode.
   * @return ClusterKey parsed from the passed bytes.
   * @throws DeserializationException
   */
  static String parsePeerFrom(final byte[] bytes) throws DeserializationException {
    if (ProtobufUtil.isPBMagicPrefix(bytes)) {
      int pblen = ProtobufUtil.lengthOfPBMagic();
      ZooKeeperProtos.ReplicationPeer.Builder builder = ZooKeeperProtos.ReplicationPeer
          .newBuilder();
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
   * @param bytes Content of a state znode.
   * @return State parsed from the passed bytes.
   * @throws DeserializationException
   */
  static ZooKeeperProtos.ReplicationState.State parseStateFrom(final byte[] bytes)
      throws DeserializationException {
    ProtobufUtil.expectPBMagicPrefix(bytes);
    int pblen = ProtobufUtil.lengthOfPBMagic();
    ZooKeeperProtos.ReplicationState.Builder builder = ZooKeeperProtos.ReplicationState
        .newBuilder();
    ZooKeeperProtos.ReplicationState state;
    try {
      state = builder.mergeFrom(bytes, pblen, bytes.length - pblen).build();
      return state.getState();
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
  }

  /**
   * @param bytes - Content of a HLog position znode.
   * @return long - The current HLog position.
   * @throws DeserializationException
   */
  public static long parseHLogPositionFrom(
      final byte[] bytes) throws DeserializationException {
    return ZKUtil.parseHLogPositionFrom(bytes);
  }

  /**
   * @param bytes - Content of a lock znode.
   * @return String - The owner of the lock.
   * @throws DeserializationException
   */
  static String parseLockOwnerFrom(
      final byte[] bytes) throws DeserializationException {
    if (ProtobufUtil.isPBMagicPrefix(bytes)) {
      int pblen = ProtobufUtil.lengthOfPBMagic();
      ZooKeeperProtos.ReplicationLock.Builder builder = ZooKeeperProtos.ReplicationLock
          .newBuilder();
      ZooKeeperProtos.ReplicationLock lock;
      try {
        lock = builder.mergeFrom(bytes, pblen, bytes.length - pblen).build();
      } catch (InvalidProtocolBufferException e) {
        throw new DeserializationException(e);
      }
      return lock.getLockOwner();
    } else {
      if (bytes.length > 0) {
        return Bytes.toString(bytes);
      }
      return "";
    }
  }

  /**
   * Enable replication to the peer
   *
   * @param id peer's identifier
   * @throws IllegalArgumentException
   *           Thrown when the peer doesn't exist
   */
  public void enablePeer(String id) throws IOException {
    changePeerState(id, ZooKeeperProtos.ReplicationState.State.ENABLED);
    LOG.info("peer " + id + " is enabled");
  }

  /**
   * Disable replication to the peer
   *
   * @param id peer's identifier
   * @throws IllegalArgumentException
   *           Thrown when the peer doesn't exist
   */
  public void disablePeer(String id) throws IOException {
    changePeerState(id, ZooKeeperProtos.ReplicationState.State.DISABLED);
    LOG.info("peer " + id + " is disabled");
  }

  private void changePeerState(String id, ZooKeeperProtos.ReplicationState.State state)
      throws IOException {
    try {
      if (!peerExists(id)) {
        throw new IllegalArgumentException("peer " + id + " is not registered");
      }
      String peerStateZNode = getPeerStateNode(id);
      byte[] stateBytes = (state == ZooKeeperProtos.ReplicationState.State.ENABLED) ? ENABLED_ZNODE_BYTES
          : DISABLED_ZNODE_BYTES;
      if (ZKUtil.checkExists(this.zookeeper, peerStateZNode) != -1) {
        ZKUtil.setData(this.zookeeper, peerStateZNode, stateBytes);
      } else {
        ZKUtil.createAndWatch(zookeeper, peerStateZNode, stateBytes);
      }
      LOG.info("state of the peer " + id + " changed to " + state.name());
    } catch (KeeperException e) {
      throw new IOException("Unable to change state of the peer " + id, e);
    }
  }

  /**
   * Check whether the peer is enabled or not. This method checks the atomic
   * boolean of ReplicationPeer locally.
   *
   * @param id peer identifier
   * @return true if the peer is enabled, otherwise false
   * @throws IllegalArgumentException
   *           Thrown when the peer doesn't exist
   */
  public boolean getPeerEnabled(String id) {
    if (!this.peerClusters.containsKey(id)) {
      throw new IllegalArgumentException("peer " + id + " is not registered");
    }
    return this.peerClusters.get(id).getPeerEnabled().get();
  }

  private String getPeerStateNode(String id) {
    return ZKUtil.joinZNode(this.peersZNode, ZKUtil.joinZNode(id, this.peerStateNodeName));
  }

  /**
   * Get the replication status of this cluster. If the state znode doesn't exist it will also
   * create it and set it true.
   * @return returns true when it's enabled, else false
   * @throws KeeperException
   */
  public boolean getReplication() throws KeeperException {
    return this.replicationState.getState();
  }

  /**
   * Set the new replication state for this cluster
   * @param newState
   * @throws KeeperException
   */
  public void setReplication(boolean newState) throws KeeperException {
    this.replicationState.setState(newState);
  }

  /**
   * Add a new log to the list of hlogs in zookeeper
   * @param filename name of the hlog's znode
   * @param peerId name of the cluster's znode
   */
  public void addLogToList(String filename, String peerId) throws KeeperException {
    this.replicationQueues.addLog(peerId, filename);
  }

  /**
   * Remove a log from the list of hlogs in zookeeper
   * @param filename name of the hlog's znode
   * @param clusterId name of the cluster's znode
   */
  public void removeLogFromList(String filename, String clusterId) {
    this.replicationQueues.removeLog(clusterId, filename);
  }

  /**
   * Set the current position of the specified cluster in the current hlog
   * @param filename filename name of the hlog's znode
   * @param clusterId clusterId name of the cluster's znode
   * @param position the position in the file
   */
  public void writeReplicationStatus(String filename, String clusterId, long position) {
    this.replicationQueues.setLogPosition(clusterId, filename, position);
  }

  /**
   * Get a list of all the other region servers in this cluster
   * and set a watch
   * @return a list of server nanes
   */
  public List<String> getRegisteredRegionServers() {
    List<String> result = null;
    try {
      result = ZKUtil.listChildrenAndWatchThem(
          this.zookeeper, this.zookeeper.rsZNode);
    } catch (KeeperException e) {
      this.abortable.abort("Get list of registered region servers", e);
    }
    return result;
  }


  /**
   * Take ownership for the set of queues belonging to a dead region server.
   * @param regionserver the id of the dead region server
   * @return A SortedMap of the queues that have been claimed, including a SortedSet of HLogs in
   *         each queue.
   */
  public SortedMap<String, SortedSet<String>> claimQueues(String regionserver) {
    return this.replicationQueues.claimQueues(regionserver);
  }

  /**
   * Delete a complete queue of hlogs
   * @param peerZnode znode of the peer cluster queue of hlogs to delete
   */
  public void deleteSource(String peerZnode, boolean closeConnection) {
    this.replicationQueues.removeQueue(peerZnode);
    if (closeConnection) {
      this.peerClusters.get(peerZnode).getZkw().close();
      this.peerClusters.remove(peerZnode);
    }
  }

  /**
   * Delete this cluster's queues
   */
  public void deleteOwnRSZNode() {
    this.replicationQueues.removeAllQueues();
  }

  /**
   * Get the position of the specified hlog in the specified peer znode
   * @param peerId znode of the peer cluster
   * @param hlog name of the hlog
   * @return the position in that hlog
   * @throws KeeperException
   */
  public long getHLogRepPosition(String peerId, String hlog) throws KeeperException {
    return this.replicationQueues.getLogPosition(peerId, hlog);
  }

  /**
   * Returns the UUID of the provided peer id. Should a connection loss or session
   * expiration happen, the ZK handler will be reopened once and if it still doesn't
   * work then it will bail and return null.
   * @param peerId the peer's ID that will be converted into a UUID
   * @return a UUID or null if there's a ZK connection issue
   */
  public UUID getPeerUUID(String peerId) {
    ReplicationPeer peer = getPeerClusters().get(peerId);
    UUID peerUUID = null;
    try {
      peerUUID = getUUIDForCluster(peer.getZkw());
    } catch (KeeperException ke) {
      reconnectPeer(ke, peer);
    }
    return peerUUID;
  }

  /**
   * Get the UUID for the provided ZK watcher. Doesn't handle any ZK exceptions
   * @param zkw watcher connected to an ensemble
   * @return the UUID read from zookeeper
   * @throws KeeperException
   */
  public UUID getUUIDForCluster(ZooKeeperWatcher zkw) throws KeeperException {
    return UUID.fromString(ZKClusterId.readClusterIdZNode(zkw));
  }

  private void reconnectPeer(KeeperException ke, ReplicationPeer peer) {
    if (ke instanceof ConnectionLossException
      || ke instanceof SessionExpiredException) {
      LOG.warn(
        "Lost the ZooKeeper connection for peer " + peer.getClusterKey(),
        ke);
      try {
        peer.reloadZkWatcher();
      } catch(IOException io) {
        LOG.warn(
          "Creation of ZookeeperWatcher failed for peer "
            + peer.getClusterKey(), io);
      }
    }
  }

  public void registerRegionServerListener(ZooKeeperListener listener) {
    this.zookeeper.registerListener(listener);
  }

  /**
   * Get a map of all peer clusters
   * @return map of peer cluster keyed by id
   */
  public Map<String, ReplicationPeer> getPeerClusters() {
    return this.peerClusters;
  }

  /**
   * Determine if a ZK path points to a peer node.
   * @param path path to be checked
   * @return true if the path points to a peer node, otherwise false
   */
  public boolean isPeerPath(String path) {
    return path.split("/").length == peersZNode.split("/").length + 1;
  }

  /**
   * Extracts the znode name of a peer cluster from a ZK path
   * @param fullPath Path to extract the id from
   * @return the id or an empty string if path is invalid
   */
  public static String getZNodeName(String fullPath) {
    String[] parts = fullPath.split("/");
    return parts.length > 0 ? parts[parts.length-1] : "";
  }

  /**
   * Get this cluster's zk connection
   * @return zk connection
   */
  public ZooKeeperWatcher getZookeeperWatcher() {
    return this.zookeeper;
  }


  /**
   * Get the full path to the peers' znode
   * @return path to peers in zk
   */
  public String getPeersZNode() {
    return peersZNode;
  }

  @Override
  public void close() throws IOException {
    if (replicationState != null) replicationState.close();
  }

  /**
   * Utility method to ensure an ENABLED znode is in place; if not present, we
   * create it.
   * @param zookeeper
   * @param path Path to znode to check
   * @return True if we created the znode.
   * @throws NodeExistsException
   * @throws KeeperException
   */
  static boolean ensurePeerEnabled(final ZooKeeperWatcher zookeeper, final String path)
      throws NodeExistsException, KeeperException {
    if (ZKUtil.checkExists(zookeeper, path) == -1) {
      // There is a race b/w PeerWatcher and ReplicationZookeeper#add method to create the
      // peer-state znode. This happens while adding a peer.
      // The peer state data is set as "ENABLED" by default.
      ZKUtil.createNodeIfNotExistsAndWatch(zookeeper, path, ENABLED_ZNODE_BYTES);
      return true;
    }
    return false;
  }

  /**
   * @param bytes
   * @return True if the passed in <code>bytes</code> are those of a pb
   *         serialized ENABLED state.
   * @throws DeserializationException
   */
  static boolean isStateEnabled(final byte[] bytes) throws DeserializationException {
    ZooKeeperProtos.ReplicationState.State state = parseStateFrom(bytes);
    return ZooKeeperProtos.ReplicationState.State.ENABLED == state;
  }
}
