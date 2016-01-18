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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.AuthFailedException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;

/**
 * A {@link BaseReplicationEndpoint} for replication endpoints whose
 * target cluster is an HBase cluster.
 */
@InterfaceAudience.Private
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="MT_CORRECTNESS",
  justification="Thinks zkw needs to be synchronized access but should be fine as is.")
public abstract class HBaseReplicationEndpoint extends BaseReplicationEndpoint
  implements Abortable {

  private static final Log LOG = LogFactory.getLog(HBaseReplicationEndpoint.class);

  private ZooKeeperWatcher zkw = null; // FindBugs: MT_CORRECTNESS

  private List<ServerName> regionServers = new ArrayList<ServerName>(0);
  private long lastRegionServerUpdate;

  protected void disconnect() {
    if (zkw != null) {
      zkw.close();
    }
  }

  /**
   * A private method used to re-establish a zookeeper session with a peer cluster.
   * @param ke
   */
  protected void reconnect(KeeperException ke) {
    if (ke instanceof ConnectionLossException || ke instanceof SessionExpiredException
        || ke instanceof AuthFailedException) {
      String clusterKey = ctx.getPeerConfig().getClusterKey();
      LOG.warn("Lost the ZooKeeper connection for peer " + clusterKey, ke);
      try {
        reloadZkWatcher();
      } catch (IOException io) {
        LOG.warn("Creation of ZookeeperWatcher failed for peer " + clusterKey, io);
      }
    }
  }

  @Override
  protected void doStart() {
    try {
      reloadZkWatcher();
      notifyStarted();
    } catch (IOException e) {
      notifyFailed(e);
    }
  }

  @Override
  protected void doStop() {
    disconnect();
    notifyStopped();
  }

  @Override
  // Synchronize peer cluster connection attempts to avoid races and rate
  // limit connections when multiple replication sources try to connect to
  // the peer cluster. If the peer cluster is down we can get out of control
  // over time.
  public synchronized UUID getPeerUUID() {
    UUID peerUUID = null;
    try {
      peerUUID = ZKClusterId.getUUIDForCluster(zkw);
    } catch (KeeperException ke) {
      reconnect(ke);
    }
    return peerUUID;
  }

  /**
   * Get the ZK connection to this peer
   * @return zk connection
   */
  protected ZooKeeperWatcher getZkw() {
    return zkw;
  }

  /**
   * Closes the current ZKW (if not null) and creates a new one
   * @throws IOException If anything goes wrong connecting
   */
  void reloadZkWatcher() throws IOException {
    if (zkw != null) zkw.close();
    zkw = new ZooKeeperWatcher(ctx.getConfiguration(),
        "connection to cluster: " + ctx.getPeerId(), this);
    getZkw().registerListener(new PeerRegionServerListener(this));
  }

  @Override
  public void abort(String why, Throwable e) {
    LOG.fatal("The HBaseReplicationEndpoint corresponding to peer " + ctx.getPeerId()
        + " was aborted for the following reason(s):" + why, e);
  }

  @Override
  public boolean isAborted() {
    // Currently this is never "Aborted", we just log when the abort method is called.
    return false;
  }

  /**
   * Get the list of all the region servers from the specified peer
   * @param zkw zk connection to use
   * @return list of region server addresses or an empty list if the slave is unavailable
   */
  protected static List<ServerName> fetchSlavesAddresses(ZooKeeperWatcher zkw)
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

  /**
   * Get a list of all the addresses of all the region servers
   * for this peer cluster
   * @return list of addresses
   */
  // Synchronize peer cluster connection attempts to avoid races and rate
  // limit connections when multiple replication sources try to connect to
  // the peer cluster. If the peer cluster is down we can get out of control
  // over time.
  public synchronized List<ServerName> getRegionServers() {
    try {
      setRegionServers(fetchSlavesAddresses(this.getZkw()));
    } catch (KeeperException ke) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Fetch slaves addresses failed", ke);
      }
      reconnect(ke);
    }
    return regionServers;
  }

  /**
   * Set the list of region servers for that peer
   * @param regionServers list of addresses for the region servers
   */
  public synchronized void setRegionServers(List<ServerName> regionServers) {
    this.regionServers = regionServers;
    lastRegionServerUpdate = System.currentTimeMillis();
  }

  /**
   * Get the timestamp at which the last change occurred to the list of region servers to replicate
   * to.
   * @return The System.currentTimeMillis at the last time the list of peer region servers changed.
   */
  public long getLastRegionServerUpdate() {
    return lastRegionServerUpdate;
  }

  /**
   * Tracks changes to the list of region servers in a peer's cluster.
   */
  public static class PeerRegionServerListener extends ZooKeeperListener {

    private final HBaseReplicationEndpoint replicationEndpoint;
    private final String regionServerListNode;

    public PeerRegionServerListener(HBaseReplicationEndpoint replicationPeer) {
      super(replicationPeer.getZkw());
      this.replicationEndpoint = replicationPeer;
      this.regionServerListNode = replicationEndpoint.getZkw().rsZNode;
    }

    @Override
    public synchronized void nodeChildrenChanged(String path) {
      if (path.equals(regionServerListNode)) {
        try {
          LOG.info("Detected change to peer region servers, fetching updated list");
          replicationEndpoint.setRegionServers(fetchSlavesAddresses(replicationEndpoint.getZkw()));
        } catch (KeeperException e) {
          LOG.fatal("Error reading slave addresses", e);
        }
      }
    }
  }
}
