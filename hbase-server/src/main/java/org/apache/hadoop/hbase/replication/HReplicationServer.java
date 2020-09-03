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
import java.lang.management.MemoryUsage;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.ClusterConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.regionserver.ReplicationService;
import org.apache.hadoop.hbase.regionserver.ReplicationSinkService;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationLoad;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationServerStatusProtos.ReplicationServerStatusService;

/**
 * HReplicationServer which is responsible to all replication stuff. It checks in with
 * the HMaster. There are many HReplicationServers in a single HBase deployment.
 */
@InterfaceAudience.Private
@SuppressWarnings({ "deprecation"})
public class HReplicationServer extends Thread implements Server {

  private static final Logger LOG = LoggerFactory.getLogger(HReplicationServer.class);

  /** replication server process name */
  public static final String REPLICATION_SERVER = "replicationserver";

  /**
   * This servers start code.
   */
  protected final long startCode;

  private volatile boolean stopped = false;

  // Go down hard. Used if file system becomes unavailable and also in
  // debugging and unit tests.
  private AtomicBoolean abortRequested;

  // flag set after we're done setting up server threads
  final AtomicBoolean online = new AtomicBoolean(false);

  /**
   * The server name the Master sees us as.  Its made from the hostname the
   * master passes us, port, and server start code. Gets set after registration
   * against Master.
   */
  private ServerName serverName;

  protected final Configuration conf;

  private ReplicationSinkService replicationSinkService;

  final int msgInterval;
  // A sleeper that sleeps for msgInterval.
  protected final Sleeper sleeper;

  private final int shortOperationTimeout;

  // zookeeper connection and watcher
  protected final ZKWatcher zooKeeper;

  // master address tracker
  private final MasterAddressTracker masterAddressTracker;

  /**
   * The asynchronous cluster connection to be shared by services.
   */
  protected AsyncClusterConnection asyncClusterConnection;

  private UserProvider userProvider;

  protected final ReplicationServerRpcServices rpcServices;

  // Stub to do region server status calls against the master.
  private volatile ReplicationServerStatusService.BlockingInterface rssStub;

  // RPC client. Used to make the stub above that does region server status checking.
  private RpcClient rpcClient;

  public HReplicationServer(final Configuration conf) throws IOException {
    TraceUtil.initTracer(conf);
    try {
      this.startCode = System.currentTimeMillis();
      this.conf = conf;

      this.abortRequested = new AtomicBoolean(false);

      this.rpcServices = createRpcServices();

      String hostName = this.rpcServices.isa.getHostName();
      serverName = ServerName.valueOf(hostName, this.rpcServices.isa.getPort(), this.startCode);

      this.userProvider = UserProvider.instantiate(conf);

      this.msgInterval = conf.getInt("hbase.replicationserver.msginterval", 3 * 1000);
      this.sleeper = new Sleeper(this.msgInterval, this);

      this.shortOperationTimeout = conf.getInt(HConstants.HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY,
          HConstants.DEFAULT_HBASE_RPC_SHORTOPERATION_TIMEOUT);

      // Some unit tests don't need a cluster, so no zookeeper at all
      if (!conf.getBoolean("hbase.testing.nocluster", false)) {
        // Open connection to zookeeper and set primary watcher
        zooKeeper = new ZKWatcher(conf, getProcessName() + ":" +
            rpcServices.isa.getPort(), this, false);
        masterAddressTracker = new MasterAddressTracker(getZooKeeper(), this);
        masterAddressTracker.start();
      } else {
        zooKeeper = null;
        masterAddressTracker = null;
      }
      this.rpcServices.start(zooKeeper);
    } catch (Throwable t) {
      // Make sure we log the exception. HReplicationServer is often started via reflection and the
      // cause of failed startup is lost.
      LOG.error("Failed construction ReplicationServer", t);
      throw t;
    }
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

      long lastMsg = System.currentTimeMillis();
      // The main run loop.
      while (!isStopped()) {
        long now = System.currentTimeMillis();
        if ((now - lastMsg) >= msgInterval) {
          tryReplicationServerReport(lastMsg, now);
          lastMsg = System.currentTimeMillis();
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
  private synchronized void preRegistrationInitialization() {
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
  public synchronized AsyncClusterConnection getAsyncClusterConnection() {
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
    return null;
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
        HConstants.REPLICATION_SERVICE_CLASSNAME_DEFAULT);

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

  protected void tryReplicationServerReport(long reportStartTime, long reportEndTime)
      throws IOException {
    ReplicationServerStatusService.BlockingInterface rss = rssStub;
    if (rss == null) {
      createReplicationServerStatusStub(true);
      rss = rssStub;
      if (rss == null) {
        return;
      }
    }
    ClusterStatusProtos.ServerLoad sl = buildServerLoad(reportStartTime, reportEndTime);
    try {
      RegionServerReportRequest.Builder request = RegionServerReportRequest
          .newBuilder();
      request.setServer(ProtobufUtil.toServerName(this.serverName));
      request.setLoad(sl);
      rss.replicationServerReport(null, request.build());
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof YouAreDeadException) {
        // This will be caught and handled as a fatal error in run()
        throw ioe;
      }
      if (rssStub == rss) {
        rssStub = null;
      }
      // Couldn't connect to the master, get location from zk and reconnect
      // Method blocks until new master is found or we are stopped
      createReplicationServerStatusStub(true);
    }
  }

  private ClusterStatusProtos.ServerLoad buildServerLoad(long reportStartTime, long reportEndTime) {
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
    return serverLoad.build();
  }

  /**
   * Get the current master from ZooKeeper and open the RPC connection to it. To get a fresh
   * connection, the current rssStub must be null. Method will block until a master is available.
   * You can break from this block by requesting the server stop.
   * @param refresh If true then master address will be read from ZK, otherwise use cached data
   * @return master + port, or null if server has been stopped
   */
  protected synchronized ServerName createReplicationServerStatusStub(boolean refresh) {
    if (rssStub != null) {
      return masterAddressTracker.getMasterAddress();
    }
    ServerName sn = null;
    long previousLogTime = 0;
    ReplicationServerStatusService.BlockingInterface intRssStub = null;
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

        try {
          BlockingRpcChannel channel =
              this.rpcClient.createBlockingRpcChannel(sn, userProvider.getCurrent(),
                  shortOperationTimeout);
          intRssStub = ReplicationServerStatusService.newBlockingStub(channel);
          break;
        } catch (IOException e) {
          if (System.currentTimeMillis() > (previousLogTime + 1000)) {
            e = e instanceof RemoteException ?
                ((RemoteException)e).unwrapRemoteException() : e;
            if (e instanceof ServerNotRunningYetException) {
              LOG.info("Master isn't available yet, retrying");
            } else {
              LOG.warn("Unable to connect to master. Retrying. Error was:", e);
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
    this.rssStub = intRssStub;
    return sn;
  }

  /**
   * @return True if we should break loop because cluster is going down or
   *   this server has been stopped or hdfs has gone bad.
   */
  private boolean keepLooping() {
    return !this.stopped;
  }

  private static boolean sleepInterrupted(long millis) {
    boolean interrupted = false;
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while sleeping");
      interrupted = true;
    }
    return interrupted;
  }
}
