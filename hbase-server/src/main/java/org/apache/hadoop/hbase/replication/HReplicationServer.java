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

import static org.apache.hadoop.hbase.master.ReplicationServerManager.REPLICATION_SERVER_REFRESH_PERIOD;
import static org.apache.hadoop.hbase.master.ReplicationServerManager.REPLICATION_SERVER_REFRESH_PERIOD_DEFAULT;

import java.io.IOException;
import java.lang.management.MemoryUsage;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.ClusterConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.regionserver.ReplicationService;
import org.apache.hadoop.hbase.regionserver.ReplicationSinkService;
import org.apache.hadoop.hbase.replication.regionserver.MetricsReplicationGlobalSourceSource;
import org.apache.hadoop.hbase.replication.regionserver.MetricsReplicationSourceFactory;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.apache.hadoop.hbase.replication.regionserver.RecoveredReplicationSource;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationLoad;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceFactory;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceInterface;
import org.apache.hadoop.hbase.security.SecurityConstants;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationServerStatusProtos.ReplicationServerReportRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationServerStatusProtos.ReplicationServerStatusService;

/**
 * HReplicationServer which is responsible to all replication stuff. It checks in with
 * the HMaster. There are many HReplicationServers in a single HBase deployment.
 */
@InterfaceAudience.Private
@SuppressWarnings({ "deprecation"})
public class HReplicationServer extends Thread implements Server, ReplicationSourceController  {

  private static final Logger LOG = LoggerFactory.getLogger(HReplicationServer.class);

  /** replication server process name */
  public static final String REPLICATION_SERVER = "replicationserver";

  /**
   * This servers start code.
   */
  private final long startCode;

  private volatile boolean stopped = false;

  // Go down hard. Used if file system becomes unavailable and also in
  // debugging and unit tests.
  private AtomicBoolean abortRequested;

  // flag set after we're done setting up server threads
  private final AtomicBoolean online = new AtomicBoolean(false);

  private final int msgInterval;
  // A sleeper that sleeps for msgInterval.
  private final Sleeper sleeper;

  /**
   * The server name the Master sees us as.  Its made from the hostname the
   * master passes us, port, and server start code. Gets set after registration
   * against Master.
   */
  private ServerName serverName;

  private final Configuration conf;

  // zookeeper connection and watcher
  private final ZKWatcher zooKeeper;

  private final UUID clusterId;

  private final int shortOperationTimeout;

  private HFileSystem walFs;
  private Path walRootDir;

  /**
   * ChoreService used to schedule tasks that we want to run periodically
   */
  private ChoreService choreService;

  // master address tracker
  private final MasterAddressTracker masterAddressTracker;

  private ServerName masterServerName;
  // Stub to do region server status calls against the master.
  private volatile ReplicationServerStatusService.BlockingInterface rssStub;

  /**
   * The asynchronous cluster connection to be shared by services.
   */
  private AsyncClusterConnection asyncClusterConnection;

  private UserProvider userProvider;

  final ReplicationServerRpcServices rpcServices;

  // Total buffer size on this RegionServer for holding batched edits to be shipped.
  private final long totalBufferLimit;
  private AtomicLong totalBufferUsed = new AtomicLong();

  private final MetricsReplicationGlobalSourceSource globalMetrics;
  private final Map<String, MetricsSource> sourceMetrics = new HashMap<>();
  private final ConcurrentMap<String, ReplicationSourceInterface> sources =
    new ConcurrentHashMap<>();

  private final ZKReplicationQueueStorage zkQueueStorage;
  private final ReplicationPeers replicationPeers;

  // RPC client. Used to make the stub above that does region server status checking.
  private RpcClient rpcClient;

  private ReplicationSinkService replicationSinkService;

  private final int statsPeriodInSecond;

  public HReplicationServer(final Configuration conf) throws Exception {
    TraceUtil.initTracer(conf);
    try {
      this.startCode = System.currentTimeMillis();
      this.conf = conf;

      this.abortRequested = new AtomicBoolean(false);

      this.rpcServices = createRpcServices();

      String hostName = this.rpcServices.isa.getHostName();
      serverName = ServerName.valueOf(hostName, this.rpcServices.isa.getPort(), this.startCode);

      this.userProvider = UserProvider.instantiate(conf);
      // login the zookeeper client principal (if using security)
      ZKUtil.loginClient(this.conf, HConstants.ZK_CLIENT_KEYTAB_FILE,
        HConstants.ZK_CLIENT_KERBEROS_PRINCIPAL, hostName);
      // login the server principal (if using secure Hadoop)
      this.userProvider.login(SecurityConstants.REGIONSERVER_KRB_KEYTAB_FILE,
        SecurityConstants.REGIONSERVER_KRB_PRINCIPAL, hostName);
      // init superusers and add the server principal (if using security)
      // or process owner as default super user.
      Superusers.initialize(conf);

      this.msgInterval = conf.getInt("hbase.replication.server.msginterval", 3 * 1000);
      this.sleeper = new Sleeper(this.msgInterval, this);

      this.shortOperationTimeout = conf.getInt(HConstants.HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY,
          HConstants.DEFAULT_HBASE_RPC_SHORTOPERATION_TIMEOUT);
      this.totalBufferLimit = conf.getLong(HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_KEY,
        HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_DFAULT);
      this.statsPeriodInSecond = conf.getInt("replication.stats.thread.period.seconds", 5 * 60);
      this.globalMetrics =
        CompatibilitySingletonFactory.getInstance(MetricsReplicationSourceFactory.class)
          .getGlobalSource();

      initializeFileSystem();

      // Open connection to zookeeper and set primary watcher
      zooKeeper =
        new ZKWatcher(conf, getProcessName() + ":" + rpcServices.isa.getPort(), this, false);
      masterAddressTracker = new MasterAddressTracker(getZooKeeper(), this);
      masterAddressTracker.start();

      this.zkQueueStorage = (ZKReplicationQueueStorage) ReplicationStorageFactory
        .getReplicationQueueStorage(zooKeeper, conf);
      this.replicationPeers =
        ReplicationFactory.getReplicationPeers(zooKeeper, this.conf);
      this.replicationPeers.init();
      this.clusterId = ZKClusterId.getUUIDForCluster(zooKeeper);
      this.rpcServices.start(zooKeeper);
      this.choreService = new ChoreService(getName(), true);
    } catch (Throwable t) {
      // Make sure we log the exception. HReplicationServer is often started via reflection and the
      // cause of failed startup is lost.
      LOG.error("Failed construction ReplicationServer", t);
      throw t;
    }
  }

  private void initializeFileSystem() throws IOException {
    // Get fs instance used by this RS. Do we use checksum verification in the hbase? If hbase
    // checksum verification enabled, then automatically switch off hdfs checksum verification.
    boolean useHBaseChecksum = conf.getBoolean(HConstants.HBASE_CHECKSUM_VERIFICATION, true);
    CommonFSUtils.setFsDefault(this.conf, CommonFSUtils.getWALRootDir(this.conf));
    this.walFs = new HFileSystem(this.conf, useHBaseChecksum);
    this.walRootDir = CommonFSUtils.getWALRootDir(this.conf);
  }

  public String getProcessName() {
    return REPLICATION_SERVER;
  }

  @Override
  public void run() {
    if (isStopped()) {
      LOG.info("Skipping run; stopped");
      return;
    }
    try {
      // Do pre-registration initializations; zookeeper, lease threads, etc.
      preRegistrationInitialization();
    } catch (Throwable e) {
      abort("Fatal exception during initialization", e);
    }

    try {
      setupReplication();
      startReplicationService();

      online.set(true);

      int refreshPeriod = conf.getInt(REPLICATION_SERVER_REFRESH_PERIOD,
        REPLICATION_SERVER_REFRESH_PERIOD_DEFAULT);
      long lastReportedTime = System.currentTimeMillis();
      // The main run loop.
      while (!isStopped()) {
        long now = System.currentTimeMillis();
        if ((now - lastReportedTime) >= msgInterval) {
          if (tryReplicationServerReport(lastReportedTime, now)) {
            lastReportedTime = now;
          }
        }
        if ((now - lastReportedTime) >= refreshPeriod) {
          stop("Connection loss with master");
        }
        if (!isStopped() && !isAborted()) {
          this.sleeper.sleep();
        }
      }

      stopServiceThreads();

      if (this.rpcServices != null) {
        this.rpcServices.stop();
      }
    } catch (Throwable t) {
      abort(t.getMessage(), t);
    }

    if (this.asyncClusterConnection != null) {
      try {
        this.asyncClusterConnection.close();
      } catch (IOException e) {
        // Although the {@link Closeable} interface throws an {@link
        // IOException}, in reality, the implementation would never do that.
        LOG.warn("Attempt to close server's AsyncClusterConnection failed.", e);
      }
    }
    if (rssStub != null) {
      rssStub = null;
    }
    if (rpcClient != null) {
      this.rpcClient.close();
    }

    if (this.zooKeeper != null) {
      this.zooKeeper.close();
    }
    LOG.info("Exiting; stopping=" + this.serverName + "; zookeeper connection closed.");
  }

  private Configuration cleanupConfiguration() {
    Configuration conf = this.conf;
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
  private void preRegistrationInitialization() {
    try {
      setupClusterConnection();
      // Setup RPC client for master communication
      this.rpcClient = asyncClusterConnection.getRpcClient();
    } catch (Throwable t) {
      // Call stop if error or process will stick around for ever since server
      // puts up non-daemon threads.
      this.rpcServices.stop();
      abort("Initialization of ReplicationServer failed. Hence aborting ReplicationServer.", t);
    }
  }

  /**
   * Setup our cluster connection if not already initialized.
   */
  protected final synchronized void setupClusterConnection() throws IOException {
    if (asyncClusterConnection == null) {
      Configuration conf = cleanupConfiguration();
      InetSocketAddress localAddress = new InetSocketAddress(this.rpcServices.isa.getAddress(), 0);
      User user = userProvider.getCurrent();
      asyncClusterConnection =
          ClusterConnectionFactory.createAsyncClusterConnection(conf, localAddress, user);
    }
  }

  /**
   * Wait on all threads to finish. Presumption is that all closes and stops
   * have already been called.
   */
  protected void stopServiceThreads() {
    if (this.replicationSinkService != null) {
      this.replicationSinkService.stopReplicationService();
    }
    if (this.choreService != null) {
      this.choreService.shutdown();
    }
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public ZKWatcher getZooKeeper() {
    return zooKeeper;
  }

  @Override
  public Connection getConnection() {
    return getAsyncConnection().toConnection();
  }

  @Override
  public Connection createConnection(Configuration conf) throws IOException {
    throw new DoNotRetryIOException(new UnsupportedOperationException("This's ReplicationServer."));
  }

  @Override
  public AsyncClusterConnection getAsyncClusterConnection() {
    return this.asyncClusterConnection;
  }

  @Override
  public ServerName getServerName() {
    return serverName;
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
    return null;
  }

  @Override
  public ChoreService getChoreService() {
    return choreService;
  }

  @Override
  public void abort(String why, Throwable cause) {
    if (!setAbortRequested()) {
      // Abort already in progress, ignore the new request.
      LOG.debug(
          "Abort already in progress. Ignoring the current request with reason: {}", why);
      return;
    }
    String msg = "***** ABORTING replication server " + this + ": " + why + " *****";
    if (cause != null) {
      LOG.error(HBaseMarkers.FATAL, msg, cause);
    } else {
      LOG.error(HBaseMarkers.FATAL, msg);
    }
    stop(why);
  }

  @Override
  public boolean isAborted() {
    return abortRequested.get();
  }

  @Override
  public void stop(final String msg) {
    if (!this.stopped) {
      LOG.info("***** STOPPING region server '" + this + "' *****");
      this.stopped = true;
      LOG.info("STOPPED: " + msg);
      // Wakes run() if it is sleeping
      sleeper.skipSleepCycle();
    }
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  /**
   * Setup WAL log and replication if enabled. Replication setup is done in here because it wants to
   * be hooked up to WAL.
   */
  private void setupReplication() throws IOException {
    // Instantiate replication if replication enabled. Pass it the log directories.
    createNewReplicationInstance(conf, this);
  }

  /**
   * Load the replication executorService objects, if any
   */
  private static void createNewReplicationInstance(Configuration conf, HReplicationServer server)
      throws IOException {
    // read in the name of the sink replication class from the config file.
    String sinkClassname = conf.get(HConstants.REPLICATION_SINK_SERVICE_CLASSNAME,
        HConstants.REPLICATION_SINK_SERVICE_CLASSNAME_DEFAULT);

    server.replicationSinkService = newReplicationInstance(sinkClassname,
        ReplicationSinkService.class, conf, server);
  }

  private static <T extends ReplicationService> T newReplicationInstance(String classname,
      Class<T> xface, Configuration conf, HReplicationServer server) throws IOException {
    final Class<? extends T> clazz;
    try {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      clazz = Class.forName(classname, true, classLoader).asSubclass(xface);
    } catch (java.lang.ClassNotFoundException nfe) {
      throw new IOException("Could not find class for " + classname);
    }
    T service = ReflectionUtils.newInstance(clazz, conf);
    service.initialize(server, null, null, null, null);
    return service;
  }

  /**
   * Start up replication source and sink handlers.
   */
  private void startReplicationService() throws IOException {
    if (this.replicationSinkService != null) {
      this.replicationSinkService.startReplicationService();
    }
    this.choreService.scheduleChore(
      new ReplicationStatisticsChore("ReplicationSourceStatistics", this,
        (int) TimeUnit.SECONDS.toMillis(statsPeriodInSecond)));
  }

  /**
   * @return Return the object that implements the replication sink executorService.
   */
  public ReplicationSinkService getReplicationSinkService() {
    return replicationSinkService;
  }

  /**
   * Report the status of the server. A server is online once all the startup is
   * completed (setting up filesystem, starting executorService threads, etc.). This
   * method is designed mostly to be useful in tests.
   *
   * @return true if online, false if not.
   */
  public boolean isOnline() {
    return online.get();
  }

  protected ReplicationServerRpcServices createRpcServices() throws IOException {
    return new ReplicationServerRpcServices(this);
  }

  /**
   * Sets the abort state if not already set.
   * @return True if abortRequested set to True successfully, false if an abort is already in
   * progress.
   */
  protected boolean setAbortRequested() {
    return abortRequested.compareAndSet(false, true);
  }

  /**
   * @return True if report to master success.
   * @throws YouAreDeadException
   */
  private boolean tryReplicationServerReport(long reportStartTime, long reportEndTime)
    throws YouAreDeadException {
    ServerName newMasterServerName = masterAddressTracker.getMasterAddress(true);
    if (newMasterServerName == null) {
      LOG.warn("No master found. Will retry report next time.");
      return false;
    }
    if (masterServerName == null || !ServerName
      .isSameAddress(masterServerName, newMasterServerName)) {
      ReplicationServerStatusService.BlockingInterface intRssStub =
        createReplicationServerStatusStub(newMasterServerName);
      if (intRssStub == null) {
        return false;
      }
      masterServerName = newMasterServerName;
      rssStub = intRssStub;
    }

    try {
      rssStub.replicationServerReport(null, buildReportRequest(reportStartTime, reportEndTime));
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof YouAreDeadException) {
        throw (YouAreDeadException) ioe;
      }
      LOG.warn("Failed to report to master {}", masterServerName, ioe);
      return false;
    }
    return true;
  }

  private ReplicationServerStatusService.BlockingInterface createReplicationServerStatusStub(
    ServerName sm) {
    try {
      BlockingRpcChannel channel = this.rpcClient
        .createBlockingRpcChannel(sm, userProvider.getCurrent(), shortOperationTimeout);
      return ReplicationServerStatusService.newBlockingStub(channel);
    } catch (IOException e) {
      e = e instanceof RemoteException ? ((RemoteException) e).unwrapRemoteException() : e;
      if (e instanceof ServerNotRunningYetException) {
        LOG.info("Master {} isn't available yet. Will retry report next time.", sm);
      } else {
        LOG.warn("Unable to connect to master {}. Will retry report next time.", sm, e);
      }
    }
    return null;
  }

  private ReplicationServerReportRequest buildReportRequest(long reportStartTime,
    long reportEndTime) {
    ReplicationServerReportRequest.Builder request = ReplicationServerReportRequest
      .newBuilder();
    request.setServer(ProtobufUtil.toServerName(this.serverName));

    long usedMemory = -1L;
    long maxMemory = -1L;
    final MemoryUsage usage = MemorySizeUtil.safeGetHeapMemoryUsage();
    if (usage != null) {
      usedMemory = usage.getUsed();
      maxMemory = usage.getMax();
    }

    ClusterStatusProtos.ServerLoad.Builder serverLoad = ClusterStatusProtos.ServerLoad.newBuilder();
    serverLoad.setTotalNumberOfRequests(rpcServices.requestCount.sum());
    serverLoad.setUsedHeapMB((int) (usedMemory / 1024 / 1024));
    serverLoad.setMaxHeapMB((int) (maxMemory / 1024 / 1024));

    serverLoad.setReportStartTime(reportStartTime);
    serverLoad.setReportEndTime(reportEndTime);

    // for the replicationLoad purpose. Only need to get from one executorService
    // either source or sink will get the same info
    ReplicationSinkService sinks = getReplicationSinkService();

    if (sinks != null) {
      // always refresh first to get the latest value
      ReplicationLoad rLoad = sinks.refreshAndGetReplicationLoad();
      if (rLoad != null) {
        serverLoad.setReplLoadSink(rLoad.getReplicationLoadSink());
      }
    }

    request.setLoad(serverLoad.build());
    for (ReplicationSourceInterface source : this.sources.values()) {
      request.addQueueNode(
        zkQueueStorage.getQueueNode(source.getServerWALsBelongTo(), source.getQueueId()));
    }
    return request.build();
  }

  @Override
  public long getTotalBufferLimit() {
    return this.totalBufferLimit;
  }

  @Override
  public AtomicLong getTotalBufferUsed() {
    return this.totalBufferUsed;
  }

  @Override
  public MetricsReplicationGlobalSourceSource getGlobalMetrics() {
    return this.globalMetrics;
  }

  @Override
  public void finishRecoveredSource(RecoveredReplicationSource src) {
    this.sources.remove(src.getQueueId());
    this.sourceMetrics.remove(src.getQueueId());
    deleteQueue(src.getQueueId());
    LOG.info("Finished recovering queue {} with the following stats: {}", src.getQueueId(),
      src.getStats());
  }

  public void startReplicationSource(ServerName owner, String queueId)
    throws IOException, ReplicationException {
    ReplicationQueueInfo queueInfo = new ReplicationQueueInfo(owner, queueId);
    String peerId = queueInfo.getPeerId();
    this.replicationPeers.addPeer(peerId);
    Path walDir =
      new Path(walRootDir, AbstractFSWALProvider.getWALDirectoryName(owner.toString()));
    MetricsSource metrics = new MetricsSource(queueId);

    ReplicationSourceInterface src = ReplicationSourceFactory.create(conf, queueId);
    // init replication source
    src.init(conf, walFs, walDir, this, zkQueueStorage, replicationPeers.getPeer(peerId), this,
      queueInfo, clusterId, p -> OptionalLong.empty(), metrics);
    zkQueueStorage.getWALsInQueue(owner, queueId)
      .forEach(walName -> src.enqueueLog(new Path(walDir, walName)));
    src.startup();
    sources.put(queueId, src);
    sourceMetrics.put(queueId, metrics);
  }

  /**
   * Delete a complete queue of wals associated with a replication source
   * @param queueId the id of replication queue to delete
   */
  private void deleteQueue(String queueId) {
    abortWhenFail(() -> this.zkQueueStorage.removeQueue(getServerName(), queueId));
  }

  @FunctionalInterface
  private interface ReplicationQueueOperation {
    void exec() throws ReplicationException;
  }

  private void abortWhenFail(ReplicationQueueOperation op) {
    try {
      op.exec();
    } catch (ReplicationException e) {
      abort("Failed to operate on replication queue", e);
    }
  }

  /**
   * Get a string representation of all the sources' metrics
   */
  private String getStats() {
    StringBuilder stats = new StringBuilder();
    // Print stats that apply across all Replication Sources
    stats.append("Global stats: ");
    stats.append("WAL Edits Buffer Used=").append(getTotalBufferUsed().get()).append("B, Limit=")
      .append(getTotalBufferLimit()).append("B\n");
    for (ReplicationSourceInterface source : this.sources.values()) {
      if (source.isRecovered()) {
        stats.append("Recovered source for cluster/machine(s) " + source.getPeerId() + ": ");
      } else {
        stats.append("Normal source for cluster " + source.getPeerId() + ": ");
      }
      stats.append(source.getStats() + "\n");
    }
    return stats.toString();
  }

  private final class ReplicationStatisticsChore extends ScheduledChore {

    ReplicationStatisticsChore(String name, Stoppable stopper, int period) {
      super(name, stopper, period);
    }

    @Override
    protected void chore() {
      printStats(getStats());
    }

    private void printStats(String stats) {
      if (!stats.isEmpty()) {
        LOG.info(stats);
      }
    }
  }
}
