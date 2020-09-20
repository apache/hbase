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

import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_RPC_SHORTOPERATION_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.ClusterConnectionFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.AuthFailedException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListReplicationSinkServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListReplicationSinkServersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;

/**
 * A {@link BaseReplicationEndpoint} for replication endpoints whose
 * target cluster is an HBase cluster.
 */
@InterfaceAudience.Private
public abstract class HBaseReplicationEndpoint extends BaseReplicationEndpoint
  implements Abortable {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseReplicationEndpoint.class);

  public static final String FETCH_SERVERS_USE_ZK_CONF_KEY =
      "hbase.replication.fetch.servers.usezk";

  public static final String FETCH_SERVERS_INTERVAL_CONF_KEY =
      "hbase.replication.fetch.servers.interval";
  public static final int DEFAULT_FETCH_SERVERS_INTERVAL = 10 * 60 * 1000; // 10 mins

  private ZKWatcher zkw = null;

  private List<ServerName> regionServers = new ArrayList<>(0);
  private long lastRegionServerUpdate;
  private AsyncClusterConnection peerConnection;
  private boolean fetchServersUseZk = false;
  private FetchServersChore fetchServersChore;
  private int shortOperationTimeout;

  protected synchronized void disconnect() {
    if (zkw != null) {
      zkw.close();
    }
    if (fetchServersChore != null) {
      ChoreService choreService = ctx.getServer().getChoreService();
      if (null != choreService) {
        choreService.cancelChore(fetchServersChore);
      }
    }
    if (peerConnection != null) {
      try {
        peerConnection.close();
      } catch (IOException e) {
        LOG.warn("Attempt to close peerConnection failed.", e);
      }
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
  public void start() {
    startAsync();
  }

  @Override
  public void stop() {
    stopAsync();
  }

  @Override
  protected synchronized void doStart() {
    this.shortOperationTimeout = ctx.getLocalConfiguration().getInt(
        HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY, DEFAULT_HBASE_RPC_SHORTOPERATION_TIMEOUT);
    try {
      if (ctx.getLocalConfiguration().getBoolean(FETCH_SERVERS_USE_ZK_CONF_KEY, false)) {
        fetchServersUseZk = true;
      } else {
        try {
          if (ReplicationUtils.isPeerClusterSupportReplicationOffload(getPeerConnection())) {
            fetchServersChore = new FetchServersChore(ctx.getServer(), this);
            ctx.getServer().getChoreService().scheduleChore(fetchServersChore);
            fetchServersUseZk = false;
          } else {
            fetchServersUseZk = true;
          }
        } catch (Throwable t) {
          fetchServersUseZk = true;
          LOG.warn("Peer {} try to fetch servers by admin failed. Using zk impl.",
              ctx.getPeerId(), t);
        }
      }
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
  protected synchronized ZKWatcher getZkw() {
    return zkw;
  }

  /**
   * Closes the current ZKW (if not null) and creates a new one
   * @throws IOException If anything goes wrong connecting
   */
  synchronized void reloadZkWatcher() throws IOException {
    if (zkw != null) {
      zkw.close();
    }
    zkw = new ZKWatcher(ctx.getConfiguration(),
        "connection to cluster: " + ctx.getPeerId(), this);
    if (fetchServersUseZk) {
      getZkw().registerListener(new PeerRegionServerListener(this));
    }
  }

  @Override
  public void abort(String why, Throwable e) {
    LOG.error("The HBaseReplicationEndpoint corresponding to peer " + ctx.getPeerId()
        + " was aborted for the following reason(s):" + why, e);
  }

  @Override
  public boolean isAborted() {
    // Currently this is never "Aborted", we just log when the abort method is called.
    return false;
  }

  /**
   * Get the connection to peer cluster
   * @return connection to peer cluster
   * @throws IOException If anything goes wrong connecting
   */
  protected synchronized AsyncClusterConnection getPeerConnection() throws IOException {
    if (peerConnection == null) {
      Configuration conf = ctx.getConfiguration();
      peerConnection = ClusterConnectionFactory.createAsyncClusterConnection(conf, null,
          UserProvider.instantiate(conf).getCurrent());
    }
    return peerConnection;
  }

  /**
   * Get the list of all the servers that are responsible for replication sink
   * from the specified peer master
   * @return list of server addresses or an empty list if the slave is unavailable
   */
  protected List<ServerName> fetchSlavesAddresses() throws IOException {
    AsyncClusterConnection peerConn = getPeerConnection();
    ServerName master = FutureUtils.get(peerConn.getAdmin().getMaster());
    MasterService.BlockingInterface masterStub = MasterService.newBlockingStub(
        peerConn.getRpcClient()
            .createBlockingRpcChannel(master, User.getCurrent(), shortOperationTimeout));
    try {
      ListReplicationSinkServersResponse resp = masterStub.listReplicationSinkServers(null,
              ListReplicationSinkServersRequest.newBuilder().build());
      return ProtobufUtil.toServerNameList(resp.getServerNameList());
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * Get the list of all the region servers from the specified peer
   * @return list of region server addresses or an empty list if the slave is unavailable
   */
  protected List<ServerName> fetchSlavesAddressesByZK() throws KeeperException {
    ZKWatcher zk = getZkw();
    List<String> children = ZKUtil.listChildrenAndWatchForNewChildren(zk,
        zk.getZNodePaths().rsZNode);
    if (children == null) {
      return Collections.emptyList();
    }
    List<ServerName> addresses = new ArrayList<>(children.size());
    for (String child : children) {
      addresses.add(ServerName.parseServerName(child));
    }
    return addresses;
  }

  /**
   * Get a list of all the addresses of all the available servers that are responsible for
   * replication sink for this peer cluster, or an empty list if no servers available at peer
   * cluster.
   * @return list of addresses
   */
  // Synchronize peer cluster connection attempts to avoid races and rate
  // limit connections when multiple replication sources try to connect to
  // the peer cluster. If the peer cluster is down we can get out of control
  // over time.
  public synchronized List<ServerName> getRegionServers() {
    if (fetchServersUseZk) {
      try {
        setRegionServers(fetchSlavesAddressesByZK());
      } catch (KeeperException ke) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Fetch slaves addresses failed", ke);
        }
        reconnect(ke);
      }
    } else {
      try {
        setRegionServers(fetchSlavesAddresses());
      } catch (IOException e) {
        LOG.warn("Fetch slaves addresses failed", e);
      }
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
  public static class PeerRegionServerListener extends ZKListener {

    private final HBaseReplicationEndpoint replicationEndpoint;
    private final String regionServerListNode;

    public PeerRegionServerListener(HBaseReplicationEndpoint replicationPeer) {
      super(replicationPeer.getZkw());
      this.replicationEndpoint = replicationPeer;
      this.regionServerListNode = replicationEndpoint.getZkw().getZNodePaths().rsZNode;
    }

    @Override
    public synchronized void nodeChildrenChanged(String path) {
      if (path.equals(regionServerListNode)) {
        try {
          LOG.info("Detected change to peer region servers, fetching updated list");
          replicationEndpoint.setRegionServers(replicationEndpoint.fetchSlavesAddressesByZK());
        } catch (KeeperException e) {
          LOG.error("Error reading slave addresses", e);
        }
      }
    }
  }

  /**
   * Chore that will fetch the list of servers from peer master.
   */
  public static class FetchServersChore extends ScheduledChore {

    private HBaseReplicationEndpoint endpoint;

    public FetchServersChore(Server server, HBaseReplicationEndpoint endpoint) {
      super("Peer-" + endpoint.ctx.getPeerId() + "-FetchServersChore", server,
          server.getConfiguration().getInt(FETCH_SERVERS_INTERVAL_CONF_KEY,
              DEFAULT_FETCH_SERVERS_INTERVAL));
      this.endpoint = endpoint;
    }

    @Override
    protected void chore() {
      try {
        endpoint.setRegionServers(endpoint.fetchSlavesAddresses());
      } catch (Throwable t) {
        LOG.error("Peer {} fetches servers failed", endpoint.ctx.getPeerId(), t);
      }
    }
  }
}
