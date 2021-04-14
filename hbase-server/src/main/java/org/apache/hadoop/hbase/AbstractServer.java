/*
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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.ClusterConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterRpcServicesVersionWrapper;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.zookeeper.ClusterStatusTracker;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;

import org.apache.hadoop.hbase.shaded.protobuf.generated.CompactionServerStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos;

@InterfaceAudience.Private
public abstract class AbstractServer extends Thread implements Server {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractServer.class);
  protected Configuration conf;
  // A sleeper that sleeps for msgInterval.
  protected Sleeper sleeper;
  protected int msgInterval;
  /**
   * This servers startcode.
   */
  protected long startcode;
  /**
   * Unique identifier for the cluster we are a part of.
   */
  protected String clusterId;
  protected UserProvider userProvider;
  protected int shortOperationTimeout;
  // zookeeper connection and watcher
  protected  ZKWatcher zooKeeper;
  /**
   * The asynchronous cluster connection to be shared by services.
   */
  protected AsyncClusterConnection asyncClusterConnection;

  /**
   * The server name the Master sees us as. Its made from the hostname the master passes us, port,
   * and server startcode. Gets set after registration against Master.
   */
  private ServerName serverName;
  /**
   * True if this RegionServer is coming up in a cluster where there is no Master; means it needs to
   * just come up and make do without a Master to talk to: e.g. in test or HRegionServer is doing
   * other than its usual duties: e.g. as an hollowed-out host whose only purpose is as a
   * Replication-stream sink; see HBASE-18846 for more. TODO: can this replace
   * {@link org.apache.hadoop.hbase.regionserver.HRegionServer#TEST_SKIP_REPORTING_TRANSITION} ?
   */
  protected final boolean masterless;
  private static final String MASTERLESS_CONFIG_NAME = "hbase.masterless";

  // RPC client. Used to make the stub above that does region server status checking.
  protected RpcClient rpcClient;

  // Set when a report to the master comes back with a message asking us to
  // shutdown. Also set by call to stop when debugging or running unit tests
  // of AbstractServer in isolation.
  protected volatile boolean stopped = false;

  // master address tracker
  protected MasterAddressTracker masterAddressTracker;
  // Cluster Status Tracker
  protected ClusterStatusTracker clusterStatusTracker;
  /**
   * Setup our cluster connection if not already initialized.
   */
  protected final synchronized void setupClusterConnection() throws IOException {
    if (asyncClusterConnection == null) {
      Configuration conf = cleanupConfiguration();
      InetSocketAddress localAddress = new InetSocketAddress(getRpcService().isa.getAddress(), 0);
      User user = userProvider.getCurrent();
      asyncClusterConnection =
          ClusterConnectionFactory.createAsyncClusterConnection(conf, localAddress, user);
    }
  }

  @Override
  public ServerName getServerName() {
    return this.serverName;
  }

  public void setServerName(ServerName serverName) {
    this.serverName = serverName;
  }

  public int getMsgInterval() {
    return msgInterval;
  }

  public String getClusterId() {
    return this.clusterId;
  }

  /**
   * Bring up connection to zk ensemble and then wait until a master for this cluster and then after
   * that, wait until cluster 'up' flag has been set. This is the order in which master does things.
   * <p>
   * Finally open long-living server short-circuit connection.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE",
    justification = "cluster Id znode read would give us correct response")
  protected void initializeZooKeeper() throws IOException, InterruptedException {
    // Nothing to do in here if no Master in the mix.
    if (this.masterless) {
      return;
    }
    // If we are HMaster then the cluster id should have already been set.
    if (clusterId == null) {
      try {
        clusterId = ZKClusterId.readClusterIdZNode(this.zooKeeper);
        if (clusterId == null) {
          this.abort("Cluster ID has not been set");
        }
        LOG.info("ClusterId : " + clusterId);
      } catch (KeeperException e) {
        this.abort("Failed to retrieve Cluster ID", e);
      }
    }
  }

  public AbstractServer(final Configuration conf, String processName) throws IOException {
    super(processName); // thread name
    this.startcode = System.currentTimeMillis();
    this.conf = conf;
    this.masterless = conf.getBoolean(MASTERLESS_CONFIG_NAME, false);
    this.userProvider = UserProvider.instantiate(conf);
    this.shortOperationTimeout = conf.getInt(HConstants.HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY,
      HConstants.DEFAULT_HBASE_RPC_SHORTOPERATION_TIMEOUT);
    Superusers.initialize(conf);
  }

  @Override
  public Connection getConnection() {
    return getAsyncConnection().toConnection();
  }

  @Override
  public Connection createConnection(Configuration conf) throws IOException {
    return null;
  }

  @Override
  public synchronized AsyncClusterConnection getAsyncClusterConnection() {
    return asyncClusterConnection;
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public ZKWatcher getZooKeeper() {
    return zooKeeper;
  }

  /**
   * @see HRegionServer#abort(String, Throwable)
   */
  public void abort(String reason) {
    abort(reason, null);
  }


  protected boolean canCreateBaseZNode() {
    return this.masterless;
  }

  private Configuration cleanupConfiguration() {
    Configuration conf = this.conf;
    // We use ZKConnectionRegistry for all the internal communication, primarily for these reasons:
    // - Decouples RS and master life cycles. RegionServers can continue be up independent of
    //   masters' availability.
    // - Configuration management for region servers (cluster internal) is much simpler when adding
    //   new masters or removing existing masters, since only clients' config needs to be updated.
    // - We need to retain ZKConnectionRegistry for replication use anyway, so we just extend it for
    //   other internal connections too.
    conf.set(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
      HConstants.ZK_CONNECTION_REGISTRY_CLASS);
    if (conf.get(HConstants.CLIENT_ZOOKEEPER_QUORUM) != null) {
      // Use server ZK cluster for server-issued connections, so we clone
      // the conf and unset the client ZK related properties
      conf = new Configuration(this.conf);
      conf.unset(HConstants.CLIENT_ZOOKEEPER_QUORUM);
    }
    return conf;
  }
  /**
   * All initialization needed before we go register with Master.<br>
   * Do bare minimum. Do bulk of initializations AFTER we've connected to the Master.<br>
   * In here we just put up the RpcServer, setup Connection, and ZooKeeper.
   */
  protected void preRegistrationInitialization() {
    try {
      initializeZooKeeper();
      setupClusterConnection();
      // Setup RPC client for master communication
      this.rpcClient = asyncClusterConnection.getRpcClient();
    } catch (Throwable t) {
      // Call stop if error or process will stick around for ever since server
      // puts up non-daemon threads.
      getRpcService().stop();
      abort("Initialization of Server failed.  Hence aborting Server.", t);
    }
  }

  protected static boolean sleepInterrupted(long millis) {
    boolean interrupted = false;
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while sleeping");
      interrupted = true;
    }
    return interrupted;
  }

  /**
   * @return True if we should break loop because cluster is going down or this server has been
   *         stopped or hdfs has gone bad.
   */
  protected boolean keepLooping() {
    return !this.stopped && isClusterUp();
  }

  /**
   * @return True if the cluster is up.
   */
  public boolean isClusterUp() {
    return this.masterless ||
      (this.clusterStatusTracker != null && this.clusterStatusTracker.isClusterUp());
  }

  /**
   * Get the current master from ZooKeeper and open the RPC connection to it. To get a fresh
   * connection, the current rssStub must be null. Method will block until a master is available.
   * You can break from this block by requesting the server stop.
   * @param refresh If true then master address will be read from ZK, otherwise use cached data
   * @return master + port, or null if server has been stopped
   */
  @InterfaceAudience.Private
  protected synchronized <T extends org.apache.hbase.thirdparty.com.google.protobuf.Service> Object
      createMasterStub(Class<T> tClass, boolean refresh) {
    ServerName sn;
    long previousLogTime = 0;
    boolean interrupted = false;
    try {
      while (keepLooping()) {
        sn = this.masterAddressTracker.getMasterAddress(refresh);
        if (sn == null) {
          if (!keepLooping()) {
            // give up with no connection.
            LOG.debug("No master found and cluster is stopped; bailing out");
            return null;
          }
          if (System.currentTimeMillis() > (previousLogTime + 1000)) {
            LOG.debug("No master found; retry");
            previousLogTime = System.currentTimeMillis();
          }
          refresh = true; // let's try pull it from ZK directly
          if (sleepInterrupted(200)) {
            interrupted = true;
          }
          continue;
        }

        // If we are on the active master, use the shortcut
        if (this instanceof HMaster && sn.equals(getServerName())) {
          // Wrap the shortcut in a class providing our version to the calls where it's relevant.
          // Normally, RpcServer-based threadlocals do that.
          if (tClass.getName()
              .equals(RegionServerStatusProtos.RegionServerStatusService.class.getName())) {
            return new MasterRpcServicesVersionWrapper(((HMaster) this).getMasterRpcServices());
          }
          if (tClass.getName()
              .equals(CompactionServerStatusProtos.CompactionServerStatusService.class.getName())) {
            return ((HMaster) this).getMasterRpcServices();
          }
        }
        try {
          BlockingRpcChannel channel = this.rpcClient.createBlockingRpcChannel(sn,
            userProvider.getCurrent(), shortOperationTimeout);
          try {
            Method newBlockingStubMethod =
                tClass.getMethod("newBlockingStub", BlockingRpcChannel.class);
            return newBlockingStubMethod.invoke(null, channel);
          } catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException
              | NoSuchMethodException ite) {
            LOG.error("Unable to create class {}, master is {}", tClass.getName(), sn, ite);
            return null;
          }
        } catch (IOException e) {
          if (System.currentTimeMillis() > (previousLogTime + 1000)) {
            e = e instanceof RemoteException ? ((RemoteException) e).unwrapRemoteException() : e;
            if (e instanceof ServerNotRunningYetException) {
              LOG.info("Master {} isn't available yet, retrying", sn);
            } else {
              LOG.warn("Unable to connect to master {} . Retrying. Error was:", sn, e);
            }
            previousLogTime = System.currentTimeMillis();
          }
          if (sleepInterrupted(200)) {
            interrupted = true;
          }
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    return null;
  }

  /**
   * @return true if a stop has been requested.
   */
  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  protected abstract AbstractRpcServices getRpcService();
}
