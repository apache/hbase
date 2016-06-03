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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.replication.ReplicationSerDeHelper;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.replication.ReplicationPeer.PeerState;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.ZKUtilOp;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;

/**
 * This class provides an implementation of the ReplicationPeers interface using ZooKeeper. The
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
@InterfaceAudience.Private
public class ReplicationPeersZKImpl extends ReplicationStateZKBase implements ReplicationPeers {

  // Map of peer clusters keyed by their id
  private Map<String, ReplicationPeerZKImpl> peerClusters;
  private final ReplicationQueuesClient queuesClient;
  private Abortable abortable;

  private static final Log LOG = LogFactory.getLog(ReplicationPeersZKImpl.class);

  public ReplicationPeersZKImpl(final ZooKeeperWatcher zk, final Configuration conf,
      final ReplicationQueuesClient queuesClient, Abortable abortable) {
    super(zk, conf, abortable);
    this.abortable = abortable;
    this.peerClusters = new ConcurrentHashMap<String, ReplicationPeerZKImpl>();
    this.queuesClient = queuesClient;
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
    addExistingPeers();
  }

  @Override
  public void addPeer(String id, ReplicationPeerConfig peerConfig)
      throws ReplicationException {
    try {
      if (peerExists(id)) {
        throw new IllegalArgumentException("Cannot add a peer with id=" + id
            + " because that id already exists.");
      }

      if(id.contains("-")){
        throw new IllegalArgumentException("Found invalid peer name:" + id);
      }

      if (peerConfig.getClusterKey() != null) {
        try {
          ZKConfig.validateClusterKey(peerConfig.getClusterKey());
        } catch (IOException ioe) {
          throw new IllegalArgumentException(ioe.getMessage());
        }
      }

      checkQueuesDeleted(id);

      ZKUtil.createWithParents(this.zookeeper, this.peersZNode);

      // Irrespective of bulk load hfile replication is enabled or not we add peerId node to
      // hfile-refs node -- HBASE-15397
      try {
        String peerId = ZKUtil.joinZNode(this.hfileRefsZNode, id);
        LOG.info("Adding peer " + peerId + " to hfile reference queue.");
        ZKUtil.createWithParents(this.zookeeper, peerId);
      } catch (KeeperException e) {
        throw new ReplicationException("Failed to add peer with id=" + id
            + ", node under hfile references node.", e);
      }

      List<ZKUtilOp> listOfOps = new ArrayList<ZKUtil.ZKUtilOp>();
      ZKUtilOp op1 = ZKUtilOp.createAndFailSilent(getPeerNode(id),
        ReplicationSerDeHelper.toByteArray(peerConfig));
      // There is a race (if hbase.zookeeper.useMulti is false)
      // b/w PeerWatcher and ReplicationZookeeper#add method to create the
      // peer-state znode. This happens while adding a peer
      // The peer state data is set as "ENABLED" by default.
      ZKUtilOp op2 = ZKUtilOp.createAndFailSilent(getPeerStateNode(id), ENABLED_ZNODE_BYTES);
      listOfOps.add(op1);
      listOfOps.add(op2);
      ZKUtil.multiOrSequential(this.zookeeper, listOfOps, false);
      // A peer is enabled by default
    } catch (KeeperException e) {
      throw new ReplicationException("Could not add peer with id=" + id
          + ", peerConfif=>" + peerConfig, e);
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
      // Delete peerId node from hfile-refs node irrespective of whether bulk loaded hfile
      // replication is enabled or not

      String peerId = ZKUtil.joinZNode(this.hfileRefsZNode, id);
      try {
        LOG.info("Removing peer " + peerId + " from hfile reference queue.");
        ZKUtil.deleteNodeRecursively(this.zookeeper, peerId);
      } catch (NoNodeException e) {
        LOG.info("Did not find node " + peerId + " to delete.", e);
      }
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
  public Map<TableName, List<String>> getPeerTableCFsConfig(String id) throws ReplicationException {
    try {
      if (!peerExists(id)) {
        throw new IllegalArgumentException("peer " + id + " doesn't exist");
      }
      try {
        ReplicationPeerConfig rpc = getReplicationPeerConfig(id);
        if (rpc == null) {
          throw new ReplicationException("Unable to get tableCFs of the peer with id=" + id);
        }
        return rpc.getTableCFsMap();
      } catch (Exception e) {
        throw new ReplicationException(e);
      }
    } catch (KeeperException e) {
      throw new ReplicationException("Unable to get tableCFs of the peer with id=" + id, e);
    }
  }

  @Override
  public void setPeerTableCFsConfig(String id,
                                    Map<TableName, ? extends Collection<String>>  tableCFs)
      throws ReplicationException {
    try {
      if (!peerExists(id)) {
        throw new IllegalArgumentException("Cannot set peer tableCFs because id=" + id
            + " does not exist.");
      }
      ReplicationPeerConfig rpc = getReplicationPeerConfig(id);
      if (rpc == null) {
        throw new ReplicationException("Unable to get tableCFs of the peer with id=" + id);
      }
      rpc.setTableCFsMap(tableCFs);
      ZKUtil.setData(this.zookeeper, getPeerNode(id),
          ReplicationSerDeHelper.toByteArray(rpc));
      LOG.info("Peer tableCFs with id= " + id + " is now " +
        ReplicationSerDeHelper.convertToString(tableCFs));
    } catch (KeeperException e) {
      throw new ReplicationException("Unable to change tableCFs of the peer with id=" + id, e);
    }
  }

  @Override
  public boolean getStatusOfPeer(String id) {
    ReplicationPeer replicationPeer = this.peerClusters.get(id);
    if (replicationPeer == null) {
      throw new IllegalArgumentException("Peer with id= " + id + " is not connected");
    }
    return replicationPeer.getPeerState() == PeerState.ENABLED;
  }

  @Override
  public boolean getStatusOfPeerFromBackingStore(String id) throws ReplicationException {
    try {
      if (!peerExists(id)) {
        throw new IllegalArgumentException("peer " + id + " doesn't exist");
      }
      String peerStateZNode = getPeerStateNode(id);
      try {
        return ReplicationPeerZKImpl.isStateEnabled(ZKUtil.getData(this.zookeeper, peerStateZNode));
      } catch (KeeperException e) {
        throw new ReplicationException(e);
      } catch (DeserializationException e) {
        throw new ReplicationException(e);
      }
    } catch (KeeperException e) {
      throw new ReplicationException("Unable to get status of the peer with id=" + id +
          " from backing store", e);
    } catch (InterruptedException e) {
      throw new ReplicationException(e);
    }
  }

  @Override
  public Map<String, ReplicationPeerConfig> getAllPeerConfigs() {
    Map<String, ReplicationPeerConfig> peers = new TreeMap<String, ReplicationPeerConfig>();
    List<String> ids = null;
    try {
      ids = ZKUtil.listChildrenNoWatch(this.zookeeper, this.peersZNode);
      for (String id : ids) {
        ReplicationPeerConfig peerConfig = getReplicationPeerConfig(id);
        if (peerConfig == null) {
          LOG.warn("Failed to get replication peer configuration of clusterid=" + id
            + " znode content, continuing.");
          continue;
        }
        peers.put(id, peerConfig);
      }
    } catch (KeeperException e) {
      this.abortable.abort("Cannot get the list of peers ", e);
    } catch (ReplicationException e) {
      this.abortable.abort("Cannot get the list of peers ", e);
    }
    return peers;
  }

  @Override
  public ReplicationPeer getPeer(String peerId) {
    return peerClusters.get(peerId);
  }

  @Override
  public Set<String> getPeerIds() {
    return peerClusters.keySet(); // this is not thread-safe
  }

  /**
   * Returns a ReplicationPeerConfig from the znode or null for the given peerId.
   */
  @Override
  public ReplicationPeerConfig getReplicationPeerConfig(String peerId)
      throws ReplicationException {
    String znode = getPeerNode(peerId);
    byte[] data = null;
    try {
      data = ZKUtil.getData(this.zookeeper, znode);
    } catch (InterruptedException e) {
      LOG.warn("Could not get configuration for peer because the thread " +
          "was interrupted. peerId=" + peerId);
      Thread.currentThread().interrupt();
      return null;
    } catch (KeeperException e) {
      throw new ReplicationException("Error getting configuration for peer with id="
          + peerId, e);
    }
    if (data == null) {
      LOG.error("Could not get configuration for peer because it doesn't exist. peerId=" + peerId);
      return null;
    }

    try {
      return ReplicationSerDeHelper.parsePeerFrom(data);
    } catch (DeserializationException e) {
      LOG.warn("Failed to parse cluster key from peerId=" + peerId
          + ", specifically the content from the following znode: " + znode);
      return null;
    }
  }

  @Override
  public Pair<ReplicationPeerConfig, Configuration> getPeerConf(String peerId)
      throws ReplicationException {
    ReplicationPeerConfig peerConfig = getReplicationPeerConfig(peerId);

    if (peerConfig == null) {
      return null;
    }

    Configuration otherConf;
    try {
      otherConf = HBaseConfiguration.createClusterConf(this.conf, peerConfig.getClusterKey());
    } catch (IOException e) {
      LOG.error("Can't get peer configuration for peerId=" + peerId + " because:", e);
      return null;
    }

    if (!peerConfig.getConfiguration().isEmpty()) {
      CompoundConfiguration compound = new CompoundConfiguration();
      compound.add(otherConf);
      compound.addStringMap(peerConfig.getConfiguration());
      return new Pair<ReplicationPeerConfig, Configuration>(peerConfig, compound);
    }

    return new Pair<ReplicationPeerConfig, Configuration>(peerConfig, otherConf);
  }

  @Override
  public void updatePeerConfig(String id, ReplicationPeerConfig newConfig)
      throws ReplicationException {
    ReplicationPeer peer = getPeer(id);
    if (peer == null){
      throw new ReplicationException("Could not find peer Id " + id);
    }
    ReplicationPeerConfig existingConfig = peer.getPeerConfig();
    if (newConfig.getClusterKey() != null && !newConfig.getClusterKey().isEmpty() &&
        !newConfig.getClusterKey().equals(existingConfig.getClusterKey())){
      throw new ReplicationException("Changing the cluster key on an existing peer is not allowed."
          + " Existing key '" + existingConfig.getClusterKey() + "' does not match new key '"
          + newConfig.getClusterKey() +
      "'");
    }
    String existingEndpointImpl = existingConfig.getReplicationEndpointImpl();
    if (newConfig.getReplicationEndpointImpl() != null &&
        !newConfig.getReplicationEndpointImpl().isEmpty() &&
        !newConfig.getReplicationEndpointImpl().equals(existingEndpointImpl)){
      throw new ReplicationException("Changing the replication endpoint implementation class " +
          "on an existing peer is not allowed. Existing class '"
          + existingConfig.getReplicationEndpointImpl()
          + "' does not match new class '" + newConfig.getReplicationEndpointImpl() + "'");
    }
    //Update existingConfig's peer config and peer data with the new values, but don't touch config
    // or data that weren't explicitly changed
    existingConfig.getConfiguration().putAll(newConfig.getConfiguration());
    existingConfig.getPeerData().putAll(newConfig.getPeerData());

    try {
      ZKUtil.setData(this.zookeeper, getPeerNode(id),
          ReplicationSerDeHelper.toByteArray(existingConfig));
    }
    catch(KeeperException ke){
      throw new ReplicationException("There was a problem trying to save changes to the " +
          "replication peer " + id, ke);
    }
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
   * A private method used during initialization. This method attempts to add all registered
   * peer clusters. This method does not set a watch on the peer cluster znodes.
   */
  private void addExistingPeers() throws ReplicationException {
    List<String> znodes = null;
    try {
      znodes = ZKUtil.listChildrenNoWatch(this.zookeeper, this.peersZNode);
    } catch (KeeperException e) {
      throw new ReplicationException("Error getting the list of peer clusters.", e);
    }
    if (znodes != null) {
      for (String z : znodes) {
        createAndAddPeer(z);
      }
    }
  }

  @Override
  public boolean peerAdded(String peerId) throws ReplicationException {
    return createAndAddPeer(peerId);
  }

  @Override
  public void peerRemoved(String peerId) {
    ReplicationPeer rp = this.peerClusters.get(peerId);
    if (rp != null) {
      ((ConcurrentMap<String, ReplicationPeerZKImpl>) peerClusters).remove(peerId, rp);
    }
  }

  /**
   * Attempt to connect to a new remote slave cluster.
   * @param peerId a short that identifies the cluster
   * @return true if a new connection was made, false if no new connection was made.
   */
  public boolean createAndAddPeer(String peerId) throws ReplicationException {
    if (peerClusters == null) {
      return false;
    }
    if (this.peerClusters.containsKey(peerId)) {
      return false;
    }

    ReplicationPeerZKImpl peer = null;
    try {
      peer = createPeer(peerId);
    } catch (Exception e) {
      throw new ReplicationException("Error adding peer with id=" + peerId, e);
    }
    if (peer == null) {
      return false;
    }
    ReplicationPeerZKImpl previous =
      ((ConcurrentMap<String, ReplicationPeerZKImpl>) peerClusters).putIfAbsent(peerId, peer);
    if (previous == null) {
      LOG.info("Added new peer cluster=" + peer.getPeerConfig().getClusterKey());
    } else {
      LOG.info("Peer already present, " + previous.getPeerConfig().getClusterKey() +
        ", new cluster=" + peer.getPeerConfig().getClusterKey());
    }
    return true;
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
  private ReplicationPeerZKImpl createPeer(String peerId) throws ReplicationException {
    Pair<ReplicationPeerConfig, Configuration> pair = getPeerConf(peerId);
    if (pair == null) {
      return null;
    }
    Configuration peerConf = pair.getSecond();

    ReplicationPeerZKImpl peer = new ReplicationPeerZKImpl(zookeeper,
        peerConf, peerId, pair.getFirst(), abortable);
    try {
      peer.startStateTracker(this.getPeerStateNode(peerId));
    } catch (KeeperException e) {
      throw new ReplicationException("Error starting the peer state tracker for peerId=" +
          peerId, e);
    }

    try {
      peer.startPeerConfigTracker(this.getPeerNode(peerId));
    } catch (KeeperException e) {
      throw new ReplicationException("Error starting the peer tableCFs tracker for peerId=" +
          peerId, e);
    }

    return peer;
  }

  private void checkQueuesDeleted(String peerId) throws ReplicationException {
    if (queuesClient == null) return;
    try {
      List<String> replicators = queuesClient.getListOfReplicators();
      for (String replicator : replicators) {
        List<String> queueIds = queuesClient.getAllQueues(replicator);
        for (String queueId : queueIds) {
          ReplicationQueueInfo queueInfo = new ReplicationQueueInfo(queueId);
          if (queueInfo.getPeerId().equals(peerId)) {
            throw new ReplicationException("undeleted queue for peerId: " + peerId
                + ", replicator: " + replicator + ", queueId: " + queueId);
          }
        }
      }
      // Check for hfile-refs queue
      if (-1 != ZKUtil.checkExists(zookeeper, hfileRefsZNode)
          && queuesClient.getAllPeersFromHFileRefsQueue().contains(peerId)) {
        throw new ReplicationException("Undeleted queue for peerId: " + peerId
            + ", found in hfile-refs node path " + hfileRefsZNode);
      }
    } catch (KeeperException e) {
      throw new ReplicationException("Could not check queues deleted with id=" + peerId, e);
    }
  }
}
