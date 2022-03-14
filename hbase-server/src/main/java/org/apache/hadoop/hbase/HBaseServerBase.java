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
package org.apache.hadoop.hbase;

import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_SPLIT_COORDINATED_BY_ZK;
import static org.apache.hadoop.hbase.HConstants.HBASE_SPLIT_WAL_COORDINATED_BY_ZK;

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import java.lang.management.MemoryType;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.servlet.http.HttpServlet;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.ClusterConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ConnectionRegistryEndpoint;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.coordination.ZkCoordinatedStateManager;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.http.InfoServer;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.namequeues.NamedQueueRecorder;
import org.apache.hadoop.hbase.regionserver.ChunkCreator;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.ZKPermissionWatcher;
import org.apache.hadoop.hbase.unsafe.HBasePlatformDependent;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.NettyEventLoopGroupConfig;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.zookeeper.ClusterStatusTracker;
import org.apache.hadoop.hbase.zookeeper.ZKAuthentication;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for hbase services, such as master or region server.
 */
@InterfaceAudience.Private
public abstract class HBaseServerBase<R extends HBaseRpcServicesBase<?>> extends Thread
  implements Server, ConfigurationObserver, ConnectionRegistryEndpoint {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseServerBase.class);

  protected final Configuration conf;

  // Go down hard. Used if file system becomes unavailable and also in
  // debugging and unit tests.
  protected final AtomicBoolean abortRequested = new AtomicBoolean(false);

  // Set when a report to the master comes back with a message asking us to
  // shutdown. Also set by call to stop when debugging or running unit tests
  // of HRegionServer in isolation.
  protected volatile boolean stopped = false;

  /**
   * This servers startcode.
   */
  protected final long startcode;

  protected final UserProvider userProvider;

  // zookeeper connection and watcher
  protected final ZKWatcher zooKeeper;

  /**
   * The server name the Master sees us as. Its made from the hostname the master passes us, port,
   * and server startcode. Gets set after registration against Master.
   */
  protected ServerName serverName;

  protected final R rpcServices;

  /**
   * hostname specified by hostname config
   */
  protected final String useThisHostnameInstead;

  /**
   * Provide online slow log responses from ringbuffer
   */
  protected final NamedQueueRecorder namedQueueRecorder;

  /**
   * Configuration manager is used to register/deregister and notify the configuration observers
   * when the regionserver is notified that there was a change in the on disk configs.
   */
  protected final ConfigurationManager configurationManager;

  /**
   * ChoreService used to schedule tasks that we want to run periodically
   */
  protected final ChoreService choreService;

  // Instance of the hbase executor executorService.
  protected final ExecutorService executorService;

  // Cluster Status Tracker
  protected final ClusterStatusTracker clusterStatusTracker;

  protected final CoordinatedStateManager csm;

  // Info server. Default access so can be used by unit tests. REGIONSERVER
  // is name of the webapp and the attribute name used stuffing this instance
  // into web context.
  protected InfoServer infoServer;

  protected HFileSystem dataFs;

  protected HFileSystem walFs;

  protected Path dataRootDir;

  protected Path walRootDir;

  protected final int msgInterval;

  // A sleeper that sleeps for msgInterval.
  protected final Sleeper sleeper;

  /**
   * Go here to get table descriptors.
   */
  protected TableDescriptors tableDescriptors;

  /**
   * The asynchronous cluster connection to be shared by services.
   */
  protected AsyncClusterConnection asyncClusterConnection;

  /**
   * Cache for the meta region replica's locations. Also tracks their changes to avoid stale cache
   * entries. Used for serving ClientMetaService.
   */
  protected final MetaRegionLocationCache metaRegionLocationCache;

  protected final NettyEventLoopGroupConfig eventLoopGroupConfig;

  /**
   * If running on Windows, do windows-specific setup.
   */
  private static void setupWindows(final Configuration conf, ConfigurationManager cm) {
    if (!SystemUtils.IS_OS_WINDOWS) {
      HBasePlatformDependent.handle("HUP", (number, name) -> {
        conf.reloadConfiguration();
        cm.notifyAllObservers(conf);
      });
    }
  }

  /**
   * Setup our cluster connection if not already initialized.
   */
  protected final synchronized void setupClusterConnection() throws IOException {
    if (asyncClusterConnection == null) {
      InetSocketAddress localAddress =
        new InetSocketAddress(rpcServices.getSocketAddress().getAddress(), 0);
      User user = userProvider.getCurrent();
      asyncClusterConnection =
        ClusterConnectionFactory.createAsyncClusterConnection(this, conf, localAddress, user);
    }
  }

  protected final void initializeFileSystem() throws IOException {
    // Get fs instance used by this RS. Do we use checksum verification in the hbase? If hbase
    // checksum verification enabled, then automatically switch off hdfs checksum verification.
    boolean useHBaseChecksum = conf.getBoolean(HConstants.HBASE_CHECKSUM_VERIFICATION, true);
    String walDirUri = CommonFSUtils.getDirUri(this.conf,
      new Path(conf.get(CommonFSUtils.HBASE_WAL_DIR, conf.get(HConstants.HBASE_DIR))));
    // set WAL's uri
    if (walDirUri != null) {
      CommonFSUtils.setFsDefault(this.conf, walDirUri);
    }
    // init the WALFs
    this.walFs = new HFileSystem(this.conf, useHBaseChecksum);
    this.walRootDir = CommonFSUtils.getWALRootDir(this.conf);
    // Set 'fs.defaultFS' to match the filesystem on hbase.rootdir else
    // underlying hadoop hdfs accessors will be going against wrong filesystem
    // (unless all is set to defaults).
    String rootDirUri =
      CommonFSUtils.getDirUri(this.conf, new Path(conf.get(HConstants.HBASE_DIR)));
    if (rootDirUri != null) {
      CommonFSUtils.setFsDefault(this.conf, rootDirUri);
    }
    // init the filesystem
    this.dataFs = new HFileSystem(this.conf, useHBaseChecksum);
    this.dataRootDir = CommonFSUtils.getRootDir(this.conf);
    this.tableDescriptors = new FSTableDescriptors(this.dataFs, this.dataRootDir,
      !canUpdateTableDescriptor(), cacheTableDescriptor());
  }

  public HBaseServerBase(Configuration conf, String name)
    throws ZooKeeperConnectionException, IOException {
    super(name); // thread name
    this.conf = conf;
    this.eventLoopGroupConfig =
      NettyEventLoopGroupConfig.setup(conf, getClass().getSimpleName() + "-EventLoopGroup");
    this.startcode = EnvironmentEdgeManager.currentTime();
    this.userProvider = UserProvider.instantiate(conf);
    this.msgInterval = conf.getInt("hbase.regionserver.msginterval", 3 * 1000);
    this.sleeper = new Sleeper(this.msgInterval, this);
    this.namedQueueRecorder = createNamedQueueRecord();
    this.rpcServices = createRpcServices();
    useThisHostnameInstead = getUseThisHostnameInstead(conf);
    InetSocketAddress addr = rpcServices.getSocketAddress();
    String hostName = StringUtils.isBlank(useThisHostnameInstead) ? addr.getHostName() :
      this.useThisHostnameInstead;
    serverName = ServerName.valueOf(hostName, addr.getPort(), this.startcode);
    // login the zookeeper client principal (if using security)
    ZKAuthentication.loginClient(this.conf, HConstants.ZK_CLIENT_KEYTAB_FILE,
      HConstants.ZK_CLIENT_KERBEROS_PRINCIPAL, hostName);
    // login the server principal (if using secure Hadoop)
    login(userProvider, hostName);
    // init superusers and add the server principal (if using security)
    // or process owner as default super user.
    Superusers.initialize(conf);
    zooKeeper =
      new ZKWatcher(conf, getProcessName() + ":" + addr.getPort(), this, canCreateBaseZNode());

    this.configurationManager = new ConfigurationManager();
    setupWindows(conf, configurationManager);

    initializeFileSystem();

    this.choreService = new ChoreService(getName(), true);
    this.executorService = new ExecutorService(getName());

    this.metaRegionLocationCache = new MetaRegionLocationCache(zooKeeper);

    if (clusterMode()) {
      if (conf.getBoolean(HBASE_SPLIT_WAL_COORDINATED_BY_ZK,
        DEFAULT_HBASE_SPLIT_COORDINATED_BY_ZK)) {
        csm = new ZkCoordinatedStateManager(this);
      } else {
        csm = null;
      }
      clusterStatusTracker = new ClusterStatusTracker(zooKeeper, this);
      clusterStatusTracker.start();
    } else {
      csm = null;
      clusterStatusTracker = null;
    }
    putUpWebUI();
  }

  /**
   * Puts up the webui.
   */
  private void putUpWebUI() throws IOException {
    int port =
      this.conf.getInt(HConstants.REGIONSERVER_INFO_PORT, HConstants.DEFAULT_REGIONSERVER_INFOPORT);
    String addr = this.conf.get("hbase.regionserver.info.bindAddress", "0.0.0.0");

    if (this instanceof HMaster) {
      port = conf.getInt(HConstants.MASTER_INFO_PORT, HConstants.DEFAULT_MASTER_INFOPORT);
      addr = this.conf.get("hbase.master.info.bindAddress", "0.0.0.0");
    }
    // -1 is for disabling info server
    if (port < 0) {
      return;
    }

    if (!Addressing.isLocalAddress(InetAddress.getByName(addr))) {
      String msg = "Failed to start http info server. Address " + addr +
        " does not belong to this host. Correct configuration parameter: " +
        "hbase.regionserver.info.bindAddress";
      LOG.error(msg);
      throw new IOException(msg);
    }
    // check if auto port bind enabled
    boolean auto = this.conf.getBoolean(HConstants.REGIONSERVER_INFO_PORT_AUTO, false);
    while (true) {
      try {
        this.infoServer = new InfoServer(getProcessName(), addr, port, false, this.conf);
        infoServer.addPrivilegedServlet("dump", "/dump", getDumpServlet());
        configureInfoServer(infoServer);
        this.infoServer.start();
        break;
      } catch (BindException e) {
        if (!auto) {
          // auto bind disabled throw BindException
          LOG.error("Failed binding http info server to port: " + port);
          throw e;
        }
        // auto bind enabled, try to use another port
        LOG.info("Failed binding http info server to port: " + port);
        port++;
        LOG.info("Retry starting http info server with port: " + port);
      }
    }
    port = this.infoServer.getPort();
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, port);
    int masterInfoPort =
      conf.getInt(HConstants.MASTER_INFO_PORT, HConstants.DEFAULT_MASTER_INFOPORT);
    conf.setInt("hbase.master.info.port.orig", masterInfoPort);
    conf.setInt(HConstants.MASTER_INFO_PORT, port);
  }

  /**
   * Sets the abort state if not already set.
   * @return True if abortRequested set to True successfully, false if an abort is already in
   *         progress.
   */
  protected final boolean setAbortRequested() {
    return abortRequested.compareAndSet(false, true);
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }

  @Override
  public boolean isAborted() {
    return abortRequested.get();
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public AsyncClusterConnection getAsyncClusterConnection() {
    return asyncClusterConnection;
  }

  @Override
  public ZKWatcher getZooKeeper() {
    return zooKeeper;
  }

  protected final void shutdownChore(ScheduledChore chore) {
    if (chore != null) {
      chore.shutdown();
    }
  }

  protected final void initializeMemStoreChunkCreator(HeapMemoryManager hMemManager) {
    if (MemStoreLAB.isEnabled(conf)) {
      // MSLAB is enabled. So initialize MemStoreChunkPool
      // By this time, the MemstoreFlusher is already initialized. We can get the global limits from
      // it.
      Pair<Long, MemoryType> pair = MemorySizeUtil.getGlobalMemStoreSize(conf);
      long globalMemStoreSize = pair.getFirst();
      boolean offheap = pair.getSecond() == MemoryType.NON_HEAP;
      // When off heap memstore in use, take full area for chunk pool.
      float poolSizePercentage = offheap ? 1.0F :
        conf.getFloat(MemStoreLAB.CHUNK_POOL_MAXSIZE_KEY, MemStoreLAB.POOL_MAX_SIZE_DEFAULT);
      float initialCountPercentage = conf.getFloat(MemStoreLAB.CHUNK_POOL_INITIALSIZE_KEY,
        MemStoreLAB.POOL_INITIAL_SIZE_DEFAULT);
      int chunkSize = conf.getInt(MemStoreLAB.CHUNK_SIZE_KEY, MemStoreLAB.CHUNK_SIZE_DEFAULT);
      float indexChunkSizePercent = conf.getFloat(MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_KEY,
        MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
      // init the chunkCreator
      ChunkCreator.initialize(chunkSize, offheap, globalMemStoreSize, poolSizePercentage,
        initialCountPercentage, hMemManager, indexChunkSizePercent);
    }
  }

  protected abstract void stopChores();

  protected final void stopChoreService() {
    // clean up the scheduled chores
    if (choreService != null) {
      LOG.info("Shutdown chores and chore service");
      stopChores();
      // cancel the remaining scheduled chores (in case we missed out any)
      // TODO: cancel will not cleanup the chores, so we need make sure we do not miss any
      choreService.shutdown();
    }
  }

  protected final void stopExecutorService() {
    if (executorService != null) {
      LOG.info("Shutdown executor service");
      executorService.shutdown();
    }
  }

  protected final void closeClusterConnection() {
    if (asyncClusterConnection != null) {
      LOG.info("Close async cluster connection");
      try {
        this.asyncClusterConnection.close();
      } catch (IOException e) {
        // Although the {@link Closeable} interface throws an {@link
        // IOException}, in reality, the implementation would never do that.
        LOG.warn("Attempt to close server's AsyncClusterConnection failed.", e);
      }
    }
  }

  protected final void stopInfoServer() {
    if (this.infoServer != null) {
      LOG.info("Stop info server");
      try {
        this.infoServer.stop();
      } catch (Exception e) {
        LOG.error("Failed to stop infoServer", e);
      }
    }
  }

  protected final void closeZooKeeper() {
    if (this.zooKeeper != null) {
      LOG.info("Close zookeeper");
      this.zooKeeper.close();
    }
  }

  @Override
  public ServerName getServerName() {
    return serverName;
  }

  @Override
  public ChoreService getChoreService() {
    return choreService;
  }

  /**
   * @return Return table descriptors implementation.
   */
  public TableDescriptors getTableDescriptors() {
    return this.tableDescriptors;
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  public AccessChecker getAccessChecker() {
    return rpcServices.getAccessChecker();
  }

  public ZKPermissionWatcher getZKPermissionWatcher() {
    return rpcServices.getZkPermissionWatcher();
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
    return csm;
  }

  @Override
  public Connection createConnection(Configuration conf) throws IOException {
    User user = UserProvider.instantiate(conf).getCurrent();
    return ConnectionFactory.createConnection(conf, null, user);
  }

  /**
   * @return Return the rootDir.
   */
  public Path getDataRootDir() {
    return dataRootDir;
  }

  @Override
  public FileSystem getFileSystem() {
    return dataFs;
  }

  /**
   * @return Return the walRootDir.
   */
  public Path getWALRootDir() {
    return walRootDir;
  }

  /**
   * @return Return the walFs.
   */
  public FileSystem getWALFileSystem() {
    return walFs;
  }

  /**
   * @return True if the cluster is up.
   */
  public boolean isClusterUp() {
    return !clusterMode() || this.clusterStatusTracker.isClusterUp();
  }

  /**
   * @return time stamp in millis of when this server was started
   */
  public long getStartcode() {
    return this.startcode;
  }

  public InfoServer getInfoServer() {
    return infoServer;
  }

  public int getMsgInterval() {
    return msgInterval;
  }

  /**
   * get NamedQueue Provider to add different logs to ringbuffer
   * @return NamedQueueRecorder
   */
  public NamedQueueRecorder getNamedQueueRecorder() {
    return this.namedQueueRecorder;
  }

  public RpcServerInterface getRpcServer() {
    return rpcServices.getRpcServer();
  }

  public NettyEventLoopGroupConfig getEventLoopGroupConfig() {
    return eventLoopGroupConfig;
  }

  public R getRpcServices() {
    return rpcServices;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*/src/test/.*")
  public MetaRegionLocationCache getMetaRegionLocationCache() {
    return this.metaRegionLocationCache;
  }

  /**
   * Reload the configuration from disk.
   */
  public void updateConfiguration() {
    LOG.info("Reloading the configuration from disk.");
    // Reload the configuration from disk.
    conf.reloadConfiguration();
    configurationManager.notifyAllObservers(conf);
  }

  @Override
  public String toString() {
    return getServerName().toString();
  }

  protected abstract boolean canCreateBaseZNode();

  protected abstract String getProcessName();

  protected abstract R createRpcServices() throws IOException;

  protected abstract String getUseThisHostnameInstead(Configuration conf) throws IOException;

  protected abstract void login(UserProvider user, String host) throws IOException;

  protected abstract NamedQueueRecorder createNamedQueueRecord();

  protected abstract void configureInfoServer(InfoServer infoServer);

  protected abstract Class<? extends HttpServlet> getDumpServlet();

  protected abstract boolean canUpdateTableDescriptor();

  protected abstract boolean cacheTableDescriptor();

  protected abstract boolean clusterMode();
}
