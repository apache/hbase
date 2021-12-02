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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.AsyncRegionServerAdmin;
import org.apache.hadoop.hbase.client.ClusterConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.rsgroup.RSGroupBasedLoadBalancer;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.AuthFailedException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

/**
 * A {@link BaseReplicationEndpoint} for replication endpoints whose
 * target cluster is an HBase cluster.
 */
@InterfaceAudience.Private
public abstract class HBaseReplicationEndpoint extends BaseReplicationEndpoint
  implements Abortable {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseReplicationEndpoint.class);

  private ZKWatcher zkw = null;
  private final Object zkwLock = new Object();

  protected Configuration conf;

  private AsyncClusterConnection conn;

  /**
   * Default maximum number of times a replication sink can be reported as bad before
   * it will no longer be provided as a sink for replication without the pool of
   * replication sinks being refreshed.
   */
  public static final int DEFAULT_BAD_SINK_THRESHOLD = 3;

  /**
   * Default ratio of the total number of peer cluster region servers to consider
   * replicating to.
   */
  public static final float DEFAULT_REPLICATION_SOURCE_RATIO = 0.5f;

  static final float DEFAULT_REPLICATION_SOURCE_GROUP_RATIO = 1f;

  // Ratio of total number of potential peer region servers to be used
  private float ratio;

  private float groupRatio;

  // Maximum number of times a sink can be reported as bad before the pool of
  // replication sinks is refreshed
  private int badSinkThreshold;
  // Count of "bad replication sink" reports per peer sink
  private Map<ServerName, Integer> badReportCounts;

  private List<ServerName> sinkServers = new ArrayList<>(0);

  private static ThreadLocal<AtomicBoolean> threadLocal = new ThreadLocal<AtomicBoolean>() {
    @Override
    protected AtomicBoolean initialValue() {
      return new AtomicBoolean(false);
    }
  };

  public  boolean getIsGroup() {
    return threadLocal.get().get();
  }

  /*
   * Some implementations of HBaseInterClusterReplicationEndpoint may require instantiate different
   * Connection implementations, or initialize it in a different way, so defining createConnection
   * as protected for possible overridings.
   */
  protected AsyncClusterConnection createConnection(Configuration conf) throws IOException {
    return ClusterConnectionFactory.createAsyncClusterConnection(conf,
      null, User.getCurrent());
  }

  @Override
  public void init(Context context) throws IOException {
    super.init(context);
    this.conf = HBaseConfiguration.create(ctx.getConfiguration());
    this.ratio =
      ctx.getConfiguration().getFloat("replication.source.ratio", DEFAULT_REPLICATION_SOURCE_RATIO);
    this.groupRatio = conf.getFloat("replication.source.group.ratio",
      DEFAULT_REPLICATION_SOURCE_GROUP_RATIO);
    this.badSinkThreshold =
      ctx.getConfiguration().getInt("replication.bad.sink.threshold", DEFAULT_BAD_SINK_THRESHOLD);
    this.badReportCounts = Maps.newHashMap();
  }

  protected void disconnect() {
    synchronized (zkwLock) {
      if (zkw != null) {
        zkw.close();
      }
    }
    if (this.conn != null) {
      try {
        this.conn.close();
        this.conn = null;
      } catch (IOException e) {
        LOG.warn("{} Failed to close the connection", ctx.getPeerId());
      }
    }
  }

  /**
   * A private method used to re-establish a zookeeper session with a peer cluster.
   * @param ke
   */
  private void reconnect(KeeperException ke) {
    if (ke instanceof ConnectionLossException || ke instanceof SessionExpiredException
        || ke instanceof AuthFailedException) {
      String clusterKey = ctx.getPeerConfig().getClusterKey();
      LOG.warn("Lost the ZooKeeper connection for peer {}", clusterKey, ke);
      try {
        reloadZkWatcher();
      } catch (IOException io) {
        LOG.warn("Creation of ZookeeperWatcher failed for peer {}", clusterKey, io);
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
  protected void doStart() {
    try {
      reloadZkWatcher();
      connectPeerCluster();
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
  public UUID getPeerUUID() {
    UUID peerUUID = null;
    try {
      synchronized (zkwLock) {
        peerUUID = ZKClusterId.getUUIDForCluster(zkw);
      }
    } catch (KeeperException ke) {
      reconnect(ke);
    }
    return peerUUID;
  }

  /**
   * Closes the current ZKW (if not null) and creates a new one
   * @throws IOException If anything goes wrong connecting
   */
  private void reloadZkWatcher() throws IOException {
    synchronized (zkwLock) {
      if (zkw != null) {
        zkw.close();
      }
      zkw = new ZKWatcher(ctx.getConfiguration(),
          "connection to cluster: " + ctx.getPeerId(), this);
      zkw.registerListener(new PeerRegionServerListener(this));
    }
  }

  private void connectPeerCluster() throws IOException {
    try {
      conn = createConnection(this.conf);
    } catch (IOException ioe) {
      LOG.warn("{} Failed to create connection for peer cluster", ctx.getPeerId(), ioe);
      throw ioe;
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
   * Get the list of all the region servers from the specified peer
   *
   * @return list of region server addresses or an empty list if the slave is unavailable
   */
  protected List<ServerName> fetchSlavesAddresses() {
    return fetchSlavesAddresses(hostServerName);
  }

  private List<ServerName> fetchSlavesAddresses(ServerName hostServerName) {
    List<String> children = null;
    try {
      synchronized (zkwLock) {
        children = ZKUtil.listChildrenAndWatchForNewChildren(zkw, zkw.getZNodePaths().rsZNode);
      }
    } catch (KeeperException ke) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Fetch slaves addresses failed", ke);
      }
      reconnect(ke);
    }
    if (children == null) {
      return Collections.emptyList();
    }

    Configuration conf = HBaseConfiguration.create();

    /** if use other balancer, return all regionservers */
    if (!conf.get(HConstants.HBASE_MASTER_LOADBALANCER_CLASS)
      .equals(RSGroupBasedLoadBalancer.class.getName())
      || hostServerName == null) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Use replication random choose policy...");
      }
      return parseServerNameFromList(children);
    } else {
      /** if use rsgroup balancer,
       * just return regionservers belong to the same rsgroup or default rsgroup */
      if(LOG.isDebugEnabled()) {
        LOG.debug("Use replication rsgroup choose policy...");
      }
      Map<String, String> serverNameHostPortMapping = new HashMap<>();
      for (String serverName : children) {
        String mappingKey =
          serverName.split(",")[0] + ServerName.SERVERNAME_SEPARATOR + serverName.split(",")[1];
        serverNameHostPortMapping.put(mappingKey, serverName);
      }

      String groupName = null;
      RSGroupInfo rsGroupInfo = null;
      try {
        rsGroupInfo = getRSGroupInfoOfServer(conn.toConnection(), hostServerName.getAddress());
      }catch (IOException e) {
        e.printStackTrace();
        LOG.error("rsGroupInfo error!", e);
      }
      if (rsGroupInfo != null) {
        groupName = rsGroupInfo.getName();
      }
      try {
        List<ServerName> serverList =
          getGroupServerListFromTargetZkCluster(groupName, zkw, serverNameHostPortMapping);
        if (serverList.size() > 0) {
          // if target cluster open group balancer, serverList must has server(s)
          LOG.debug("group list > 0");
          threadLocal.get().getAndSet(true);
          return serverList;
        }
        else {
          // if not, choose sinkers from all regionservers
          LOG.debug("target group list <= 0");
          return parseServerNameFromList(children);
        }
      }catch (IOException | KeeperException e) {
        LOG.error("Get server list from target zk error", e);
        return Collections.emptyList();
      }
    }
  }

  protected List<ServerName> parseServerNameFromList(List<String> children) {
    if (children == null) {
      return Collections.emptyList();
    }
    StringBuffer sb = new StringBuffer();
    List<ServerName> addresses = new ArrayList<>(children.size());
    for (String child : children) {
      addresses.add(ServerName.parseServerName(child));
      sb.append(ServerName.parseServerName(child)).append("/");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "Find " + addresses.size() + " child znodes from target cluster zk. " + sb.toString());
    }
    return addresses;
  }

  protected List<ServerName> getGroupServerListFromTargetZkCluster(String groupName,
    ZKWatcher zkw, Map<String, String> serverNameHostPortMapping)
    throws KeeperException, IOException {
    /** get group info from slave cluster zk */
    List<String> groupInfos = ZKUtil.listChildrenAndWatchForNewChildren(
        zkw, ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "rsgroup"));
    /** if target cluster have same name group */
    if(groupInfos == null){
      if(LOG.isDebugEnabled()){
        LOG.debug("groupInfos == null");
      }
      return Collections.emptyList();
    }else{
      if (groupInfos.size() > 0) {
        if (groupInfos.contains(groupName)) {
          return getServerListFromWithRSGroupName(groupName, zkw, serverNameHostPortMapping);
        } else if (!groupInfos.contains(groupName)) {
          /** if target cluster does not have same name group, return a empty list */
          return Collections.emptyList();
        }
      } else {
        /** if target cluster does not use group balancer, return a empty list */
        return Collections.emptyList();
      }
    }

    return Collections.emptyList();
  }

  protected List<ServerName> getServerListFromWithRSGroupName(
    String groupName, ZKWatcher zkw, Map<String, String> serverNameHostPortMapping)
    throws IOException {
    List<ServerName> serverList = new ArrayList<>();
    RSGroupInfo detail = retrieveGroupInfo(
      zkw, ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "rsgroup"), groupName);
    // choose server from rsZNode children which also in same group with local mashine
    for (Address serverInfo : detail.getServers()) {
      String serverPrefix =
        serverInfo.getHostname() + ServerName.SERVERNAME_SEPARATOR + serverInfo.getPort();
      if (serverNameHostPortMapping.containsKey(serverPrefix)) {
        ServerName sn = ServerName.parseServerName(serverNameHostPortMapping.get(serverPrefix));
        if(LOG.isDebugEnabled()) {
          LOG.debug("Match server in " + groupName + " success " + serverPrefix + "/" + sn);
        }
        serverList.add(sn);
      }
    }
    return serverList;
  }

  protected synchronized void chooseSinks() {
    List<ServerName> slaveAddresses = fetchSlavesAddresses();
    if (slaveAddresses.isEmpty()) {
      LOG.warn("No sinks available at peer. Will not be able to replicate");
    }
    Collections.shuffle(slaveAddresses, ThreadLocalRandom.current());
    float actualRatio=ratio;
    if(getIsGroup() && conf.get(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, LoadBalancerFactory
      .getDefaultLoadBalancerClass().getName()).equals(RSGroupBasedLoadBalancer.class.getName())
      && hostServerName != null) {
      actualRatio=groupRatio;
    }

    int numSinks = (int) Math.ceil(slaveAddresses.size() * actualRatio);
    this.sinkServers = slaveAddresses.subList(0, numSinks);
    StringBuffer sb = new StringBuffer();
    sb.append("choose sinker(s) of target cluster ");
    for(ServerName sn : sinkServers){
      sb.append(sn.getServerName()).append("/");
    }
    LOG.debug(sb.toString());
    badReportCounts.clear();
  }

  protected synchronized int getNumSinks() {
    return sinkServers.size();
  }

  /**
   * Get a randomly-chosen replication sink to replicate to.
   * @return a replication sink to replicate to
   */
  protected synchronized SinkPeer getReplicationSink() throws IOException {
    if (sinkServers.isEmpty()) {
      LOG.info("Current list of sinks is out of date or empty, updating");
      chooseSinks();
    }
    if (sinkServers.isEmpty()) {
      throw new IOException("No replication sinks are available");
    }
    ServerName serverName =
      sinkServers.get(ThreadLocalRandom.current().nextInt(sinkServers.size()));
    return new SinkPeer(serverName, conn.getRegionServerAdmin(serverName));
  }

  /**
   * Report a {@code SinkPeer} as being bad (i.e. an attempt to replicate to it
   * failed). If a single SinkPeer is reported as bad more than
   * replication.bad.sink.threshold times, it will be removed
   * from the pool of potential replication targets.
   *
   * @param sinkPeer The SinkPeer that had a failed replication attempt on it
   */
  protected synchronized void reportBadSink(SinkPeer sinkPeer) {
    ServerName serverName = sinkPeer.getServerName();
    int badReportCount = badReportCounts.compute(serverName, (k, v) -> v == null ? 1 : v + 1);
    if (badReportCount > badSinkThreshold) {
      this.sinkServers.remove(serverName);
      if (sinkServers.isEmpty()) {
        chooseSinks();
      }
    }
  }

  /**
   * Report that a {@code SinkPeer} successfully replicated a chunk of data.
   *
   * @param sinkPeer
   *          The SinkPeer that had a failed replication attempt on it
   */
  protected synchronized void reportSinkSuccess(SinkPeer sinkPeer) {
    badReportCounts.remove(sinkPeer.getServerName());
  }

  List<ServerName> getSinkServers() {
    return sinkServers;
  }

  public RSGroupInfo getRSGroupInfoOfServer(Connection connection, Address hostAndPort)
    throws IOException {
    Admin rsGroupAdmin =
      connection.getAdmin();
    RSGroupInfo rsGroupInfo = null;
    try {
      rsGroupInfo = rsGroupAdmin.getRSGroup(hostAndPort);
    }catch (Exception e){
      LOG.error("failed to fetch the RSGroupInfo!",e);
    }
    finally {
      if(rsGroupAdmin != null) {
        rsGroupAdmin.close();
      }
    }
    return rsGroupInfo;
  }

  public RSGroupInfo retrieveGroupInfo(ZKWatcher watcher, String groupBasePath,
    String groupName) throws IOException {
    ByteArrayInputStream bis = null;
    try {
      String groupInfoPath = ZNodePaths.joinZNode(groupBasePath, groupName);
      LOG.debug("---groupInfoPath: " + groupInfoPath);
      if (-1 != ZKUtil.checkExists(watcher, groupInfoPath)) {
        byte[] data = ZKUtil.getData(watcher, groupInfoPath);
        if (data.length > 0) {
          ProtobufUtil.expectPBMagicPrefix(data);
          bis = new ByteArrayInputStream(data, ProtobufUtil.lengthOfPBMagic(), data.length);
        }
      }
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Failed to read groupZNode", e);
    } catch (DeserializationException e) {
      throw new IOException("Failed to read groupZNode", e);
    }
    return ProtobufUtil.toGroupInfo(RSGroupProtos.RSGroupInfo.parseFrom(bis));
  }

  /**
   * Tracks changes to the list of region servers in a peer's cluster.
   */
  public static class PeerRegionServerListener extends ZKListener {

    private final HBaseReplicationEndpoint replicationEndpoint;
    private final String regionServerListNode;

    public PeerRegionServerListener(HBaseReplicationEndpoint endpoint) {
      super(endpoint.zkw);
      this.replicationEndpoint = endpoint;
      this.regionServerListNode = endpoint.zkw.getZNodePaths().rsZNode;
    }

    @Override
    public synchronized void nodeChildrenChanged(String path) {
      if (path.equals(regionServerListNode)) {
        LOG.info("Detected change to peer region servers, fetching updated list");
        replicationEndpoint.chooseSinks();
      }
    }
  }

  /**
   * Wraps a replication region server sink to provide the ability to identify it.
   */
  public static class SinkPeer {
    private ServerName serverName;
    private AsyncRegionServerAdmin regionServer;

    public SinkPeer(ServerName serverName, AsyncRegionServerAdmin regionServer) {
      this.serverName = serverName;
      this.regionServer = regionServer;
    }

    ServerName getServerName() {
      return serverName;
    }

    public AsyncRegionServerAdmin getRegionServer() {
      return regionServer;
    }
  }
}
