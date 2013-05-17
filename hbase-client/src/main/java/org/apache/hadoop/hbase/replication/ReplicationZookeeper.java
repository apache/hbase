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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
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
  private static final Log LOG = LogFactory.getLog(ReplicationZookeeper.class);

  // Our handle on zookeeper
  private final ZooKeeperWatcher zookeeper;
  private String peersZNode;
  private final Configuration conf;
  // Abortable
  private Abortable abortable;
  private final ReplicationStateInterface replicationState;
  private final ReplicationPeers replicationPeers;
  private final ReplicationQueues replicationQueues;

  /**
   * Constructor used by clients of replication (like master and HBase clients)
   * @param conf  conf to use
   * @param zk    zk connection to use
   * @throws IOException
   */
  public ReplicationZookeeper(final Abortable abortable, final Configuration conf,
      final ZooKeeperWatcher zk) throws KeeperException, IOException {
    super(zk, conf, abortable);
    this.conf = conf;
    this.zookeeper = zk;
    setZNodes(abortable);
    this.replicationState = new ReplicationStateImpl(this.zookeeper, conf, abortable);
    this.replicationState.init();
    // TODO This interface is no longer used by anyone using this constructor. When this class goes
    // away, we will no longer have this null initialization business
    this.replicationQueues = null;
    this.replicationPeers = new ReplicationPeersZKImpl(this.zookeeper, this.conf, abortable);
    this.replicationPeers.init();
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
    this.replicationState.init();
    this.replicationQueues = new ReplicationQueuesZKImpl(this.zookeeper, this.conf, server);
    this.replicationQueues.init(server.getServerName().toString());
    this.replicationPeers = new ReplicationPeersZKImpl(this.zookeeper, this.conf, server);
    this.replicationPeers.init();
  }

  private void setZNodes(Abortable abortable) throws KeeperException {
    String replicationZNodeName = conf.get("zookeeper.znode.replication", "replication");
    String peersZNodeName = conf.get("zookeeper.znode.replication.peers", "peers");
    String replicationZNode = ZKUtil.joinZNode(this.zookeeper.baseZNode, replicationZNodeName);
    this.peersZNode = ZKUtil.joinZNode(replicationZNode, peersZNodeName);
  }

  /**
   * List this cluster's peers' IDs
   * @return list of all peers' identifiers
   */
  public List<String> listPeersIdsAndWatch() {
    return this.replicationPeers.getAllPeerIds();
  }

  /**
   * Map of this cluster's peers for display.
   * @return A map of peer ids to peer cluster keys
   */
  public Map<String, String> listPeers() {
    return this.replicationPeers.getAllPeerClusterKeys();
  }

  /**
   * Returns all region servers from given peer
   *
   * @param peerClusterId (byte) the cluster to interrogate
   * @return addresses of all region servers
   */
  public List<ServerName> getSlavesAddresses(String peerClusterId) {
    return this.replicationPeers.getRegionServersOfConnectedPeer(peerClusterId);
  }

  /**
   * This method connects this cluster to another one and registers it
   * in this region server's replication znode
   * @param peerId id of the peer cluster
   * @throws KeeperException
   */
  public boolean connectToPeer(String peerId) throws IOException, KeeperException {
    return this.replicationPeers.connectToPeer(peerId);
  }

  /**
   * Remove the peer from zookeeper. which will trigger the watchers on every
   * region server and close their sources
   * @param id
   * @throws IllegalArgumentException Thrown when the peer doesn't exist
   */
  public void removePeer(String id) throws IOException {
    this.replicationPeers.removePeer(id);
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
    this.replicationPeers.addPeer(id, clusterKey);
  }

  /**
   * Enable replication to the peer
   *
   * @param id peer's identifier
   * @throws IllegalArgumentException
   *           Thrown when the peer doesn't exist
   */
  public void enablePeer(String id) throws IOException {
    this.replicationPeers.enablePeer(id);
  }

  /**
   * Disable replication to the peer
   *
   * @param id peer's identifier
   * @throws IllegalArgumentException
   *           Thrown when the peer doesn't exist
   */
  public void disablePeer(String id) throws IOException {
    this.replicationPeers.disablePeer(id);
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
    return this.replicationPeers.getStatusOfConnectedPeer(id);
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
      this.replicationPeers.disconnectFromPeer(peerZnode);
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
    return this.replicationPeers.getPeerUUID(peerId);
  }

  public void registerRegionServerListener(ZooKeeperListener listener) {
    this.zookeeper.registerListener(listener);
  }

  /**
   * Get a map of all peer clusters
   * @return map of peer cluster keyed by id
   */
  public Set<String> getPeerClusters() {
    return this.replicationPeers.getConnectedPeers();
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
}
