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
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.ZKUtilOp;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.AuthFailedException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;

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
public class ReplicationZookeeper {
  private static final Log LOG =
    LogFactory.getLog(ReplicationZookeeper.class);
  // Name of znode we use to lock when failover
  private final static String RS_LOCK_ZNODE = "lock";

  // Values of znode which stores state of a peer
  public static enum PeerState {
    ENABLED, DISABLED
  };

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
  // Name of zk node which stores peer state
  private String peerStateNodeName;
  private final Configuration conf;
  // Is this cluster replicating at the moment?
  private AtomicBoolean replicating;
  // The key to our own cluster
  private String ourClusterKey;
  // Abortable
  private Abortable abortable;
  private ReplicationStatusTracker statusTracker;

  /**
   * Constructor used by clients of replication (like master and HBase clients)
   * @param conf  conf to use
   * @param zk    zk connection to use
   * @throws IOException
   */
  public ReplicationZookeeper(final Abortable abortable, final Configuration conf,
                              final ZooKeeperWatcher zk)
    throws KeeperException {

    this.conf = conf;
    this.zookeeper = zk;
    this.replicating = new AtomicBoolean();
    setZNodes(abortable);
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
    this.abortable = server;
    this.zookeeper = server.getZooKeeper();
    this.conf = server.getConfiguration();
    this.replicating = replicating;
    setZNodes(server);

    this.peerClusters = new HashMap<String, ReplicationPeer>();
    ZKUtil.createWithParents(this.zookeeper,
        ZKUtil.joinZNode(this.replicationZNode, this.replicationStateNodeName));
    this.rsServerNameZnode = ZKUtil.joinZNode(rsZNode, server.getServerName().toString());
    ZKUtil.createWithParents(this.zookeeper, this.rsServerNameZnode);
    connectExistingPeers();
  }

  private void setZNodes(Abortable abortable) throws KeeperException {
    String replicationZNodeName =
        conf.get("zookeeper.znode.replication", "replication");
    String peersZNodeName =
        conf.get("zookeeper.znode.replication.peers", "peers");
    this.peerStateNodeName = conf.get(
        "zookeeper.znode.replication.peers.state", "peer-state");
    this.replicationStateNodeName =
        conf.get("zookeeper.znode.replication.state", "state");
    String rsZNodeName =
        conf.get("zookeeper.znode.replication.rs", "rs");
    this.ourClusterKey = ZKUtil.getZooKeeperClusterKey(this.conf);
    this.replicationZNode =
      ZKUtil.joinZNode(this.zookeeper.baseZNode, replicationZNodeName);
    this.peersZNode = ZKUtil.joinZNode(replicationZNode, peersZNodeName);
    ZKUtil.createWithParents(this.zookeeper, this.peersZNode);
    this.rsZNode = ZKUtil.joinZNode(replicationZNode, rsZNodeName);
    ZKUtil.createWithParents(this.zookeeper, this.rsZNode);

    // Set a tracker on replicationStateNodeNode
    this.statusTracker =
        new ReplicationStatusTracker(this.zookeeper, abortable);
    statusTracker.start();
    readReplicationStateZnode();
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
        peers.put(id, Bytes.toString(ZKUtil.getData(this.zookeeper,
            ZKUtil.joinZNode(this.peersZNode, id))));
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
    // Synchronize peer cluster connection attempts to avoid races and rate
    // limit connections when multiple replication sources try to connect to
    // the peer cluster. If the peer cluster is down we can get out of control
    // over time.
    synchronized (peer) {
      List<ServerName> addresses;
      try {
        addresses = fetchSlavesAddresses(peer.getZkw());
      } catch (KeeperException ke) {
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
    String otherClusterKey = Bytes.toString(data);
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
   * Set the new replication state for this cluster
   * @param newState
   */
  public void setReplicating(boolean newState) throws KeeperException {
    ZKUtil.createWithParents(this.zookeeper,
        ZKUtil.joinZNode(this.replicationZNode, this.replicationStateNodeName));
    ZKUtil.setData(this.zookeeper,
        ZKUtil.joinZNode(this.replicationZNode, this.replicationStateNodeName),
        Bytes.toBytes(Boolean.toString(newState)));
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
        Bytes.toBytes(clusterKey));
      // There is a race b/w PeerWatcher and ReplicationZookeeper#add method to create the
      // peer-state znode. This happens while adding a peer.
      // The peer state data is set as "ENABLED" by default.
      ZKUtil.createNodeIfNotExistsAndWatch(this.zookeeper, getPeerStateNode(id),
        Bytes.toBytes(PeerState.ENABLED.name()));
    } catch (KeeperException e) {
      throw new IOException("Unable to add peer", e);
    }
  }

  private boolean peerExists(String id) throws KeeperException {
    return ZKUtil.checkExists(this.zookeeper,
          ZKUtil.joinZNode(this.peersZNode, id)) >= 0;
  }

  /**
   * Enable replication to the peer
   *
   * @param id peer's identifier
   * @throws IllegalArgumentException
   *           Thrown when the peer doesn't exist
   */
  public void enablePeer(String id) throws IOException {
    changePeerState(id, PeerState.ENABLED);
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
    changePeerState(id, PeerState.DISABLED);
    LOG.info("peer " + id + " is disabled");
  }

  private void changePeerState(String id, PeerState state) throws IOException {
    try {
      if (!peerExists(id)) {
        throw new IllegalArgumentException("peer " + id + " is not registered");
      }
      String peerStateZNode = getPeerStateNode(id);
      if (ZKUtil.checkExists(this.zookeeper, peerStateZNode) != -1) {
        ZKUtil.setData(this.zookeeper, peerStateZNode,
          Bytes.toBytes(state.name()));
      } else {
        ZKUtil.createAndWatch(zookeeper, peerStateZNode,
            Bytes.toBytes(state.name()));
      }
      LOG.info("state of the peer " + id + " changed to " + state.name());
    } catch (KeeperException e) {
      throw new IOException("Unable to change state of the peer " + id, e);
    }
  }

  /**
   * Get state of the peer. This method checks the state by connecting to ZK.
   *
   * @param id peer's identifier
   * @return current state of the peer
   */
  public PeerState getPeerState(String id) throws KeeperException {
    byte[] peerStateBytes = ZKUtil
        .getData(this.zookeeper, getPeerStateNode(id));
    return PeerState.valueOf(Bytes.toString(peerStateBytes));
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
    return ZKUtil.joinZNode(this.peersZNode,
        ZKUtil.joinZNode(id, this.peerStateNodeName));
  }

  /**
   * This reads the state znode for replication and sets the atomic boolean
   */
  private void readReplicationStateZnode() {
    try {
      this.replicating.set(getReplication());
      LOG.info("Replication is now " + (this.replicating.get()?
        "started" : "stopped"));
    } catch (KeeperException e) {
      this.abortable.abort("Failed getting data on from " + getRepStateNode(), e);
    }
  }

  /**
   * Get the replication status of this cluster. If the state znode doesn't
   * exist it will also create it and set it true.
   * @return returns true when it's enabled, else false
   * @throws KeeperException
   */
  public boolean getReplication() throws KeeperException {
    byte [] data = this.statusTracker.getData(false);
    if (data == null || data.length == 0) {
      setReplicating(true);
      return true;
    }
    return Boolean.parseBoolean(Bytes.toString(data));
  }

  private String getRepStateNode() {
    return ZKUtil.joinZNode(this.replicationZNode, this.replicationStateNodeName);
  }

  /**
   * Add a new log to the list of hlogs in zookeeper
   * @param filename name of the hlog's znode
   * @param peerId name of the cluster's znode
   */
  public void addLogToList(String filename, String peerId)
    throws KeeperException {
    String znode = ZKUtil.joinZNode(this.rsServerNameZnode, peerId);
    znode = ZKUtil.joinZNode(znode, filename);
    ZKUtil.createWithParents(this.zookeeper, znode);
  }

  /**
   * Remove a log from the list of hlogs in zookeeper
   * @param filename name of the hlog's znode
   * @param clusterId name of the cluster's znode
   */
  public void removeLogFromList(String filename, String clusterId) {
    try {
      String znode = ZKUtil.joinZNode(rsServerNameZnode, clusterId);
      znode = ZKUtil.joinZNode(znode, filename);
      ZKUtil.deleteNode(this.zookeeper, znode);
    } catch (KeeperException e) {
      this.abortable.abort("Failed remove from list", e);
    }
  }

  /**
   * Set the current position of the specified cluster in the current hlog
   * @param filename filename name of the hlog's znode
   * @param clusterId clusterId name of the cluster's znode
   * @param position the position in the file
   * @throws IOException
   */
  public void writeReplicationStatus(String filename, String clusterId,
      long position) {
    try {
      String znode = ZKUtil.joinZNode(this.rsServerNameZnode, clusterId);
      znode = ZKUtil.joinZNode(znode, filename);
      // Why serialize String of Long and note Long as bytes?
      ZKUtil.setData(this.zookeeper, znode,
        Bytes.toBytes(Long.toString(position)));
    } catch (KeeperException e) {
      this.abortable.abort("Writing replication status", e);
    }
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
   * Get the list of the replicators that have queues, they can be alive, dead
   * or simply from a previous run
   * @return a list of server names
   */
  public List<String> getListOfReplicators() {
    List<String> result = null;
    try {
      result = ZKUtil.listChildrenNoWatch(this.zookeeper, rsZNode);
    } catch (KeeperException e) {
      this.abortable.abort("Get list of replicators", e);
    }
    return result;
  }

  /**
   * Get the list of peer clusters for the specified server names
   * @param rs server names of the rs
   * @return a list of peer cluster
   */
  public List<String> getListPeersForRS(String rs) {
    String znode = ZKUtil.joinZNode(rsZNode, rs);
    List<String> result = null;
    try {
      result = ZKUtil.listChildrenNoWatch(this.zookeeper, znode);
    } catch (KeeperException e) {
      this.abortable.abort("Get list of peers for rs", e);
    }
    return result;
  }

  /**
   * Get the list of hlogs for the specified region server and peer cluster
   * @param rs server names of the rs
   * @param id peer cluster
   * @return a list of hlogs
   */
  public List<String> getListHLogsForPeerForRS(String rs, String id) {
    String znode = ZKUtil.joinZNode(rsZNode, rs);
    znode = ZKUtil.joinZNode(znode, id);
    List<String> result = null;
    try {
      result = ZKUtil.listChildrenNoWatch(this.zookeeper, znode);
    } catch (KeeperException e) {
      this.abortable.abort("Get list of hlogs for peer", e);
    }
    return result;
  }

  /**
   * Checks if the provided znode is the same as this region server's
   * @param znode to check
   * @return if this is this rs's znode
   */
  public boolean isThisOurZnode(String znode) {
    String otherRs = ZKUtil.joinZNode(this.rsZNode, znode);
    return otherRs.equals(rsServerNameZnode);
  }

  /**
   * Try to set a lock in another server's znode.
   * @param znode the server names of the other server
   * @return true if the lock was acquired, false in every other cases
   */
  public boolean lockOtherRS(String znode) {
    try {
      String parent = ZKUtil.joinZNode(this.rsZNode, znode);
      String p = ZKUtil.joinZNode(parent, RS_LOCK_ZNODE);
      ZKUtil.createAndWatch(this.zookeeper, p, Bytes.toBytes(rsServerNameZnode));
    } catch (KeeperException e) {
      // This exception will pop up if the znode under which we're trying to
      // create the lock is already deleted by another region server, meaning
      // that the transfer already occurred.
      // NoNode => transfer is done and znodes are already deleted
      // NodeExists => lock znode already created by another RS
      if (e instanceof KeeperException.NoNodeException ||
          e instanceof KeeperException.NodeExistsException) {
        LOG.info("Won't transfer the queue," +
            " another RS took care of it because of: " + e.getMessage());
      } else {
        LOG.info("Failed lock other rs", e);
      }
      return false;
    }
    return true;
  }

  /**
   * It "atomically" copies all the hlogs queues from another region server and returns them all
   * sorted per peer cluster (appended with the dead server's znode).
   * @param znode
   * @return HLog queues sorted per peer cluster
   */
  public SortedMap<String, SortedSet<String>> copyQueuesFromRSUsingMulti(String znode) {
    SortedMap<String, SortedSet<String>> queues = new TreeMap<String, SortedSet<String>>();
    String deadRSZnodePath = ZKUtil.joinZNode(rsZNode, znode);// hbase/replication/rs/deadrs
    List<String> peerIdsToProcess = null;
    List<ZKUtilOp> listOfOps = new ArrayList<ZKUtil.ZKUtilOp>();
    try {
      peerIdsToProcess = ZKUtil.listChildrenNoWatch(this.zookeeper, deadRSZnodePath);
      if (peerIdsToProcess == null) return queues; // node already processed
      for (String peerId : peerIdsToProcess) {
        String newPeerId = peerId + "-" + znode;
        String newPeerZnode = ZKUtil.joinZNode(this.rsServerNameZnode, newPeerId);
        // check the logs queue for the old peer cluster
        String oldClusterZnode = ZKUtil.joinZNode(deadRSZnodePath, peerId);
        List<String> hlogs = ZKUtil.listChildrenNoWatch(this.zookeeper, oldClusterZnode);
        if (hlogs == null || hlogs.size() == 0) {
          listOfOps.add(ZKUtilOp.deleteNodeFailSilent(oldClusterZnode));
          continue; // empty log queue.
        }
        // create the new cluster znode
        SortedSet<String> logQueue = new TreeSet<String>();
        queues.put(newPeerId, logQueue);
        ZKUtilOp op = ZKUtilOp.createAndFailSilent(newPeerZnode, HConstants.EMPTY_BYTE_ARRAY);
        listOfOps.add(op);
        // get the offset of the logs and set it to new znodes
        for (String hlog : hlogs) {
          String oldHlogZnode = ZKUtil.joinZNode(oldClusterZnode, hlog);
          byte[] logOffset = ZKUtil.getData(this.zookeeper, oldHlogZnode);
          LOG.debug("Creating " + hlog + " with data " + Bytes.toString(logOffset));
          String newLogZnode = ZKUtil.joinZNode(newPeerZnode, hlog);
          listOfOps.add(ZKUtilOp.createAndFailSilent(newLogZnode, logOffset));
          // add ops for deleting
          listOfOps.add(ZKUtilOp.deleteNodeFailSilent(oldHlogZnode));
          logQueue.add(hlog);
        }
        // add delete op for peer
        listOfOps.add(ZKUtilOp.deleteNodeFailSilent(oldClusterZnode));
      }
      // add delete op for dead rs
      listOfOps.add(ZKUtilOp.deleteNodeFailSilent(deadRSZnodePath));
      LOG.debug(" The multi list size is: " + listOfOps.size());
      ZKUtil.multiOrSequential(this.zookeeper, listOfOps, false);
      LOG.info("Atomically moved the dead regionserver logs. ");
    } catch (KeeperException e) {
      // Multi call failed; it looks like some other regionserver took away the logs.
      LOG.warn("Got exception in copyQueuesFromRSUsingMulti: ", e);
      queues.clear();
    }
    return queues;
  }

  /**
   * This methods copies all the hlogs queues from another region server
   * and returns them all sorted per peer cluster (appended with the dead
   * server's znode)
   * @param znode server names to copy
   * @return all hlogs for all peers of that cluster, null if an error occurred
   */
  public SortedMap<String, SortedSet<String>> copyQueuesFromRS(String znode) {
    // TODO this method isn't atomic enough, we could start copying and then
    // TODO fail for some reason and we would end up with znodes we don't want.
    SortedMap<String,SortedSet<String>> queues =
        new TreeMap<String,SortedSet<String>>();
    try {
      String nodePath = ZKUtil.joinZNode(rsZNode, znode);
      List<String> clusters =
        ZKUtil.listChildrenNoWatch(this.zookeeper, nodePath);
      // We have a lock znode in there, it will count as one.
      if (clusters == null || clusters.size() <= 1) {
        return queues;
      }
      // The lock isn't a peer cluster, remove it
      clusters.remove(RS_LOCK_ZNODE);
      for (String cluster : clusters) {
        // We add the name of the recovered RS to the new znode, we can even
        // do that for queues that were recovered 10 times giving a znode like
        // number-startcode-number-otherstartcode-number-anotherstartcode-etc
        String newCluster = cluster+"-"+znode;
        String newClusterZnode = ZKUtil.joinZNode(rsServerNameZnode, newCluster);
        String clusterPath = ZKUtil.joinZNode(nodePath, cluster);
        List<String> hlogs = ZKUtil.listChildrenNoWatch(this.zookeeper, clusterPath);
        // That region server didn't have anything to replicate for this cluster
        if (hlogs == null || hlogs.size() == 0) {
          continue;
        }
        ZKUtil.createNodeIfNotExistsAndWatch(this.zookeeper, newClusterZnode,
            HConstants.EMPTY_BYTE_ARRAY);
        SortedSet<String> logQueue = new TreeSet<String>();
        queues.put(newCluster, logQueue);
        for (String hlog : hlogs) {
          String z = ZKUtil.joinZNode(clusterPath, hlog);
          byte [] position = ZKUtil.getData(this.zookeeper, z);
          LOG.debug("Creating " + hlog + " with data " + Bytes.toString(position));
          String child = ZKUtil.joinZNode(newClusterZnode, hlog);
          ZKUtil.createAndWatch(this.zookeeper, child, position);
          logQueue.add(hlog);
        }
      }
    } catch (KeeperException e) {
      this.abortable.abort("Copy queues from rs", e);
    }
    return queues;
  }

  /**
   * Delete a complete queue of hlogs
   * @param peerZnode znode of the peer cluster queue of hlogs to delete
   */
  public void deleteSource(String peerZnode, boolean closeConnection) {
    try {
      ZKUtil.deleteNodeRecursively(this.zookeeper,
          ZKUtil.joinZNode(rsServerNameZnode, peerZnode));
      if (closeConnection) {
        this.peerClusters.get(peerZnode).getZkw().close();
        this.peerClusters.remove(peerZnode);
      }
    } catch (KeeperException e) {
      this.abortable.abort("Failed delete of " + peerZnode, e);
    }
  }

  /**
   * Recursive deletion of all znodes in specified rs' znode
   * @param znode
   */
  public void deleteRsQueues(String znode) {
    String fullpath = ZKUtil.joinZNode(rsZNode, znode);
    try {
      List<String> clusters =
        ZKUtil.listChildrenNoWatch(this.zookeeper, fullpath);
      for (String cluster : clusters) {
        // We'll delete it later
        if (cluster.equals(RS_LOCK_ZNODE)) {
          continue;
        }
        String fullClusterPath = ZKUtil.joinZNode(fullpath, cluster);
        ZKUtil.deleteNodeRecursively(this.zookeeper, fullClusterPath);
      }
      // Finish cleaning up
      ZKUtil.deleteNodeRecursively(this.zookeeper, fullpath);
    } catch (KeeperException e) {
      if (e instanceof KeeperException.NoNodeException ||
          e instanceof KeeperException.NotEmptyException) {
        // Testing a special case where another region server was able to
        // create a lock just after we deleted it, but then was also able to
        // delete the RS znode before us or its lock znode is still there.
        if (e.getPath().equals(fullpath)) {
          return;
        }
      }
      this.abortable.abort("Failed delete of " + znode, e);
    }
  }

  /**
   * Delete this cluster's queues
   */
  public void deleteOwnRSZNode() {
    try {
      ZKUtil.deleteNodeRecursively(this.zookeeper,
          this.rsServerNameZnode);
    } catch (KeeperException e) {
      // if the znode is already expired, don't bother going further
      if (e instanceof KeeperException.SessionExpiredException) {
        return;
      }
      this.abortable.abort("Failed delete of " + this.rsServerNameZnode, e);
    }
  }

  /**
   * Get the position of the specified hlog in the specified peer znode
   * @param peerId znode of the peer cluster
   * @param hlog name of the hlog
   * @return the position in that hlog
   * @throws KeeperException 
   */
  public long getHLogRepPosition(String peerId, String hlog)
  throws KeeperException {
    String clusterZnode = ZKUtil.joinZNode(rsServerNameZnode, peerId);
    String znode = ZKUtil.joinZNode(clusterZnode, hlog);
    String data = Bytes.toString(ZKUtil.getData(this.zookeeper, znode));
    return data == null || data.length() == 0 ? 0 : Long.parseLong(data);
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
    // Synchronize peer cluster connection attempts to avoid races and rate
    // limit connections when multiple replication sources try to connect to
    // the peer cluster. If the peer cluster is down we can get out of control
    // over time.
    synchronized (peer) {
      try {
        peerUUID = getUUIDForCluster(peer.getZkw());
      } catch (KeeperException ke) {
        reconnectPeer(ke, peer);
      }
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
    return UUID.fromString(ClusterId.readClusterIdZNode(zkw));
  }

  private void reconnectPeer(KeeperException ke, ReplicationPeer peer) {
    if (ke instanceof ConnectionLossException || ke instanceof SessionExpiredException
        || ke instanceof AuthFailedException) {
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

  /**
   * Tracker for status of the replication
   */
  public class ReplicationStatusTracker extends ZooKeeperNodeTracker {
    public ReplicationStatusTracker(ZooKeeperWatcher watcher,
        Abortable abortable) {
      super(watcher, getRepStateNode(), abortable);
    }

    @Override
    public synchronized void nodeDataChanged(String path) {
      if (path.equals(node)) {
        super.nodeDataChanged(path);
        readReplicationStateZnode();
      }
    }
  }
}
