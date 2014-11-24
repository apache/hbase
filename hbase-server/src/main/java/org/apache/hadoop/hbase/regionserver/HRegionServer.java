/**
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Constructor;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.net.InetAddress;

import javax.management.ObjectName;
import javax.servlet.http.HttpServlet;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.ClockOutOfSyncException;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.CoordinatedStateManagerFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HealthCheckChore;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.ZNodeClearer;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.coordination.BaseCoordinatedStateManager;
import org.apache.hadoop.hbase.coordination.SplitLogWorkerCoordination;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.exceptions.RegionOpeningException;
import org.apache.hadoop.hbase.exceptions.UnknownProtocolException;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.http.InfoServer;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.procedure.RegionServerProcedureManagerHost;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceCall;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos.RegionLoad;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.Coprocessor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionServerInfo;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStatusService;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.ReportRSFatalErrorRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;
import org.apache.hadoop.hbase.quotas.RegionServerQuotaManager;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.handler.CloseMetaHandler;
import org.apache.hadoop.hbase.regionserver.handler.CloseRegionHandler;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogFactory;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.trace.SpanReceiverHost;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CompressionTest;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.JvmPauseMonitor;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.ClusterStatusTracker;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.RecoveringRegionWatcher;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

/**
 * HRegionServer makes a set of HRegions available to clients. It checks in with
 * the HMaster. There are many HRegionServers in a single HBase deployment.
 */
@InterfaceAudience.Private
@SuppressWarnings("deprecation")
public class HRegionServer extends HasThread implements
    RegionServerServices, LastSequenceId {

  public static final Log LOG = LogFactory.getLog(HRegionServer.class);

  /**
   * For testing only!  Set to true to skip notifying region assignment to master .
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="MS_SHOULD_BE_FINAL")
  public static boolean TEST_SKIP_REPORTING_TRANSITION = false;

  /*
   * Strings to be used in forming the exception message for
   * RegionsAlreadyInTransitionException.
   */
  protected static final String OPEN = "OPEN";
  protected static final String CLOSE = "CLOSE";

  //RegionName vs current action in progress
  //true - if open region action in progress
  //false - if close region action in progress
  protected final ConcurrentMap<byte[], Boolean> regionsInTransitionInRS =
    new ConcurrentSkipListMap<byte[], Boolean>(Bytes.BYTES_COMPARATOR);

  // Cache flushing
  protected MemStoreFlusher cacheFlusher;

  protected HeapMemoryManager hMemManager;

  /*
   * Short-circuit (ie. bypassing RPC layer) HConnection to this Server
   * to be used internally for miscellaneous needs. Initialized at the server startup
   * and closed when server shuts down. Clients must never close it explicitly.
   */
  protected HConnection shortCircuitConnection;

  /*
   * Long-living meta table locator, which is created when the server is started and stopped
   * when server shuts down. References to this locator shall be used to perform according
   * operations in EventHandlers. Primary reason for this decision is to make it mockable
   * for tests.
   */
  protected MetaTableLocator metaTableLocator;

  // Watch if a region is out of recovering state from ZooKeeper
  @SuppressWarnings("unused")
  private RecoveringRegionWatcher recoveringRegionWatcher;

  /**
   * Go here to get table descriptors.
   */
  protected TableDescriptors tableDescriptors;

  // Replication services. If no replication, this handler will be null.
  protected ReplicationSourceService replicationSourceHandler;
  protected ReplicationSinkService replicationSinkHandler;

  // Compactions
  public CompactSplitThread compactSplitThread;

  /**
   * Map of regions currently being served by this region server. Key is the
   * encoded region name.  All access should be synchronized.
   */
  protected final Map<String, HRegion> onlineRegions =
    new ConcurrentHashMap<String, HRegion>();

  /**
   * Map of encoded region names to the DataNode locations they should be hosted on
   * We store the value as InetSocketAddress since this is used only in HDFS
   * API (create() that takes favored nodes as hints for placing file blocks).
   * We could have used ServerName here as the value class, but we'd need to
   * convert it to InetSocketAddress at some point before the HDFS API call, and
   * it seems a bit weird to store ServerName since ServerName refers to RegionServers
   * and here we really mean DataNode locations.
   */
  protected final Map<String, InetSocketAddress[]> regionFavoredNodesMap =
      new ConcurrentHashMap<String, InetSocketAddress[]>();

  /**
   * Set of regions currently being in recovering state which means it can accept writes(edits from
   * previous failed region server) but not reads. A recovering region is also an online region.
   */
  protected final Map<String, HRegion> recoveringRegions = Collections
      .synchronizedMap(new HashMap<String, HRegion>());

  // Leases
  protected Leases leases;

  // Instance of the hbase executor service.
  protected ExecutorService service;

  // If false, the file system has become unavailable
  protected volatile boolean fsOk;
  protected HFileSystem fs;

  // Set when a report to the master comes back with a message asking us to
  // shutdown. Also set by call to stop when debugging or running unit tests
  // of HRegionServer in isolation.
  private volatile boolean stopped = false;

  // Go down hard. Used if file system becomes unavailable and also in
  // debugging and unit tests.
  private volatile boolean abortRequested;

  ConcurrentMap<String, Integer> rowlocks = new ConcurrentHashMap<String, Integer>();

  // A state before we go into stopped state.  At this stage we're closing user
  // space regions.
  private boolean stopping = false;

  private volatile boolean killed = false;

  protected final Configuration conf;

  private Path rootDir;

  protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  final int numRetries;
  protected final int threadWakeFrequency;
  protected final int msgInterval;

  protected final int numRegionsToReport;

  // Stub to do region server status calls against the master.
  private volatile RegionServerStatusService.BlockingInterface rssStub;
  // RPC client. Used to make the stub above that does region server status checking.
  RpcClient rpcClient;

  private UncaughtExceptionHandler uncaughtExceptionHandler;

  // Info server. Default access so can be used by unit tests. REGIONSERVER
  // is name of the webapp and the attribute name used stuffing this instance
  // into web context.
  protected InfoServer infoServer;
  private JvmPauseMonitor pauseMonitor;

  /** region server process name */
  public static final String REGIONSERVER = "regionserver";

  MetricsRegionServer metricsRegionServer;
  private SpanReceiverHost spanReceiverHost;

  /*
   * Check for compactions requests.
   */
  Chore compactionChecker;

  /*
   * Check for flushes
   */
  Chore periodicFlusher;

  // HLog and HLog roller. log is protected rather than private to avoid
  // eclipse warning when accessed by inner classes
  protected volatile HLog hlog;
  // The meta updates are written to a different hlog. If this
  // regionserver holds meta regions, then this field will be non-null.
  protected volatile HLog hlogForMeta;

  LogRoller hlogRoller;
  LogRoller metaHLogRoller;

  // flag set after we're done setting up server threads
  final AtomicBoolean online = new AtomicBoolean(false);

  // zookeeper connection and watcher
  protected ZooKeeperWatcher zooKeeper;

  // master address tracker
  private MasterAddressTracker masterAddressTracker;

  // Cluster Status Tracker
  protected ClusterStatusTracker clusterStatusTracker;

  // Log Splitting Worker
  private SplitLogWorker splitLogWorker;

  // A sleeper that sleeps for msgInterval.
  protected final Sleeper sleeper;

  private final int operationTimeout;

  private final RegionServerAccounting regionServerAccounting;

  // Cache configuration and block cache reference
  protected CacheConfig cacheConfig;

  /** The health check chore. */
  private HealthCheckChore healthCheckChore;

  /** The nonce manager chore. */
  private Chore nonceManagerChore;

  private Map<String, Service> coprocessorServiceHandlers = Maps.newHashMap();

  /**
   * The server name the Master sees us as.  Its made from the hostname the
   * master passes us, port, and server startcode. Gets set after registration
   * against  Master.
   */
  protected ServerName serverName;

  /**
   * This servers startcode.
   */
  protected final long startcode;

  /**
   * Unique identifier for the cluster we are a part of.
   */
  private String clusterId;

  /**
   * MX Bean for RegionServerInfo
   */
  private ObjectName mxBean = null;

  /**
   * Chore to clean periodically the moved region list
   */
  private MovedRegionsCleaner movedRegionsCleaner;

  // chore for refreshing store files for secondary regions
  private StorefileRefresherChore storefileRefresher;

  private RegionServerCoprocessorHost rsHost;

  private RegionServerProcedureManagerHost rspmHost;

  private RegionServerQuotaManager rsQuotaManager;

  // Table level lock manager for locking for region operations
  protected TableLockManager tableLockManager;

  /**
   * Nonce manager. Nonces are used to make operations like increment and append idempotent
   * in the case where client doesn't receive the response from a successful operation and
   * retries. We track the successful ops for some time via a nonce sent by client and handle
   * duplicate operations (currently, by failing them; in future we might use MVCC to return
   * result). Nonces are also recovered from WAL during, recovery; however, the caveats (from
   * HBASE-3787) are:
   * - WAL recovery is optimized, and under high load we won't read nearly nonce-timeout worth
   *   of past records. If we don't read the records, we don't read and recover the nonces.
   *   Some WALs within nonce-timeout at recovery may not even be present due to rolling/cleanup.
   * - There's no WAL recovery during normal region move, so nonces will not be transfered.
   * We can have separate additional "Nonce WAL". It will just contain bunch of numbers and
   * won't be flushed on main path - because WAL itself also contains nonces, if we only flush
   * it before memstore flush, for a given nonce we will either see it in the WAL (if it was
   * never flushed to disk, it will be part of recovery), or we'll see it as part of the nonce
   * log (or both occasionally, which doesn't matter). Nonce log file can be deleted after the
   * latest nonce in it expired. It can also be recovered during move.
   */
  final ServerNonceManager nonceManager;

  private UserProvider userProvider;

  protected final RSRpcServices rpcServices;

  protected BaseCoordinatedStateManager csm;

  /**
   * Configuration manager is used to register/deregister and notify the configuration observers
   * when the regionserver is notified that there was a change in the on disk configs.
   */
  private final ConfigurationManager configurationManager;

  /**
   * Starts a HRegionServer at the default location.
   * @param conf
   * @throws IOException
   * @throws InterruptedException
   */
  public HRegionServer(Configuration conf) throws IOException, InterruptedException {
    this(conf, CoordinatedStateManagerFactory.getCoordinatedStateManager(conf));
  }

  /**
   * Starts a HRegionServer at the default location
   * @param conf
   * @param csm implementation of CoordinatedStateManager to be used
   * @throws IOException
   */
  public HRegionServer(Configuration conf, CoordinatedStateManager csm)
      throws IOException {
    this.fsOk = true;
    this.conf = conf;
    checkCodecs(this.conf);
    this.userProvider = UserProvider.instantiate(conf);
    FSUtils.setupShortCircuitRead(this.conf);

    // Config'ed params
    this.numRetries = this.conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    this.threadWakeFrequency = conf.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.msgInterval = conf.getInt("hbase.regionserver.msginterval", 3 * 1000);

    this.sleeper = new Sleeper(this.msgInterval, this);

    boolean isNoncesEnabled = conf.getBoolean(HConstants.HBASE_RS_NONCES_ENABLED, true);
    this.nonceManager = isNoncesEnabled ? new ServerNonceManager(this.conf) : null;

    this.numRegionsToReport = conf.getInt(
      "hbase.regionserver.numregionstoreport", 10);

    this.operationTimeout = conf.getInt(
      HConstants.HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY,
      HConstants.DEFAULT_HBASE_RPC_SHORTOPERATION_TIMEOUT);

    this.abortRequested = false;
    this.stopped = false;

    rpcServices = createRpcServices();
    this.startcode = System.currentTimeMillis();
    String hostName = rpcServices.isa.getHostName();
    serverName = ServerName.valueOf(hostName, rpcServices.isa.getPort(), startcode);

    // login the zookeeper client principal (if using security)
    ZKUtil.loginClient(this.conf, "hbase.zookeeper.client.keytab.file",
      "hbase.zookeeper.client.kerberos.principal", hostName);
    // login the server principal (if using secure Hadoop)
    login(userProvider, hostName);

    regionServerAccounting = new RegionServerAccounting();
    uncaughtExceptionHandler = new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        abort("Uncaught exception in service thread " + t.getName(), e);
      }
    };

    // Set 'fs.defaultFS' to match the filesystem on hbase.rootdir else
    // underlying hadoop hdfs accessors will be going against wrong filesystem
    // (unless all is set to defaults).
    FSUtils.setFsDefault(this.conf, FSUtils.getRootDir(this.conf));
    // Get fs instance used by this RS.  Do we use checksum verification in the hbase? If hbase
    // checksum verification enabled, then automatically switch off hdfs checksum verification.
    boolean useHBaseChecksum = conf.getBoolean(HConstants.HBASE_CHECKSUM_VERIFICATION, true);
    this.fs = new HFileSystem(this.conf, useHBaseChecksum);
    this.rootDir = FSUtils.getRootDir(this.conf);
    this.tableDescriptors = new FSTableDescriptors(this.conf,
      this.fs, this.rootDir, !canUpdateTableDescriptor());

    service = new ExecutorService(getServerName().toShortString());
    spanReceiverHost = SpanReceiverHost.getInstance(getConfiguration());

    // Some unit tests don't need a cluster, so no zookeeper at all
    if (!conf.getBoolean("hbase.testing.nocluster", false)) {
      // Open connection to zookeeper and set primary watcher
      zooKeeper = new ZooKeeperWatcher(conf, getProcessName() + ":" +
        rpcServices.isa.getPort(), this, canCreateBaseZNode());

      this.csm = (BaseCoordinatedStateManager) csm;
      this.csm.initialize(this);
      this.csm.start();

      tableLockManager = TableLockManager.createTableLockManager(
        conf, zooKeeper, serverName);

      masterAddressTracker = new MasterAddressTracker(getZooKeeper(), this);
      masterAddressTracker.start();

      clusterStatusTracker = new ClusterStatusTracker(zooKeeper, this);
      clusterStatusTracker.start();
    }
    this.configurationManager = new ConfigurationManager();

    rpcServices.start();
    putUpWebUI();
  }

  protected void login(UserProvider user, String host) throws IOException {
    user.login("hbase.regionserver.keytab.file",
      "hbase.regionserver.kerberos.principal", host);
  }

  protected void waitForMasterActive(){
  }

  protected String getProcessName() {
    return REGIONSERVER;
  }

  protected boolean canCreateBaseZNode() {
    return false;
  }

  protected boolean canUpdateTableDescriptor() {
    return false;
  }

  protected RSRpcServices createRpcServices() throws IOException {
    return new RSRpcServices(this);
  }

  protected void configureInfoServer() {
    infoServer.addServlet("rs-status", "/rs-status", RSStatusServlet.class);
    infoServer.setAttribute(REGIONSERVER, this);
  }

  protected Class<? extends HttpServlet> getDumpServlet() {
    return RSDumpServlet.class;
  }

  protected void doMetrics() {
  }

  @Override
  public boolean registerService(Service instance) {
    /*
     * No stacking of instances is allowed for a single service name
     */
    Descriptors.ServiceDescriptor serviceDesc = instance.getDescriptorForType();
    if (coprocessorServiceHandlers.containsKey(serviceDesc.getFullName())) {
      LOG.error("Coprocessor service " + serviceDesc.getFullName()
          + " already registered, rejecting request from " + instance);
      return false;
    }

    coprocessorServiceHandlers.put(serviceDesc.getFullName(), instance);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Registered regionserver coprocessor service: service=" + serviceDesc.getFullName());
    }
    return true;
  }

  /**
   * Create wrapped short-circuit connection to this server.
   * In its own method so can intercept and mock it over in tests.
   * @throws IOException
   */
  protected HConnection createShortCircuitConnection() throws IOException {
    return ConnectionUtils.createShortCircuitHConnection(
      HConnectionManager.getConnection(conf), serverName, rpcServices, rpcServices);
  }
  
  /**
   * Run test on configured codecs to make sure supporting libs are in place.
   * @param c
   * @throws IOException
   */
  private static void checkCodecs(final Configuration c) throws IOException {
    // check to see if the codec list is available:
    String [] codecs = c.getStrings("hbase.regionserver.codecs", (String[])null);
    if (codecs == null) return;
    for (String codec : codecs) {
      if (!CompressionTest.testCompression(codec)) {
        throw new IOException("Compression codec " + codec +
          " not supported, aborting RS construction");
      }
    }
  }

  public String getClusterId() {
    return this.clusterId;
  }

  /**
   * All initialization needed before we go register with Master.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  private void preRegistrationInitialization(){
    try {
      synchronized (this) {
        if (shortCircuitConnection == null) {
          shortCircuitConnection = createShortCircuitConnection();
          metaTableLocator = new MetaTableLocator();
        }
      }

      // Health checker thread.
      if (isHealthCheckerConfigured()) {
        int sleepTime = this.conf.getInt(HConstants.HEALTH_CHORE_WAKE_FREQ,
          HConstants.DEFAULT_THREAD_WAKE_FREQUENCY);
        healthCheckChore = new HealthCheckChore(sleepTime, this, getConfiguration());
      }
      this.pauseMonitor = new JvmPauseMonitor(conf);
      pauseMonitor.start();

      initializeZooKeeper();
      if (!isStopped() && !isAborted()) {
        initializeThreads();
      }
    } catch (Throwable t) {
      // Call stop if error or process will stick around for ever since server
      // puts up non-daemon threads.
      this.rpcServices.stop();
      abort("Initialization of RS failed.  Hence aborting RS.", t);
    }
  }

  /**
   * Bring up connection to zk ensemble and then wait until a master for this
   * cluster and then after that, wait until cluster 'up' flag has been set.
   * This is the order in which master does things.
   * Finally open long-living server short-circuit connection.
   * @throws IOException
   * @throws InterruptedException
   */
  private void initializeZooKeeper() throws IOException, InterruptedException {
    // Create the master address tracker, register with zk, and start it.  Then
    // block until a master is available.  No point in starting up if no master
    // running.
    blockAndCheckIfStopped(this.masterAddressTracker);

    // Wait on cluster being up.  Master will set this flag up in zookeeper
    // when ready.
    blockAndCheckIfStopped(this.clusterStatusTracker);

    // Retrieve clusterId
    // Since cluster status is now up
    // ID should have already been set by HMaster
    try {
      clusterId = ZKClusterId.readClusterIdZNode(this.zooKeeper);
      if (clusterId == null) {
        this.abort("Cluster ID has not been set");
      }
      LOG.info("ClusterId : "+clusterId);
    } catch (KeeperException e) {
      this.abort("Failed to retrieve Cluster ID",e);
    }

    // In case colocated master, wait here till it's active.
    // So backup masters won't start as regionservers.
    // This is to avoid showing backup masters as regionservers
    // in master web UI, or assigning any region to them.
    waitForMasterActive();
    if (isStopped() || isAborted()) {
      return; // No need for further initialization
    }

    // watch for snapshots and other procedures
    try {
      rspmHost = new RegionServerProcedureManagerHost();
      rspmHost.loadProcedures(conf);
      rspmHost.initialize(this);
    } catch (KeeperException e) {
      this.abort("Failed to reach zk cluster when creating procedure handler.", e);
    }
    // register watcher for recovering regions
    this.recoveringRegionWatcher = new RecoveringRegionWatcher(this.zooKeeper, this);
  }

  /**
   * Utilty method to wait indefinitely on a znode availability while checking
   * if the region server is shut down
   * @param tracker znode tracker to use
   * @throws IOException any IO exception, plus if the RS is stopped
   * @throws InterruptedException
   */
  private void blockAndCheckIfStopped(ZooKeeperNodeTracker tracker)
      throws IOException, InterruptedException {
    while (tracker.blockUntilAvailable(this.msgInterval, false) == null) {
      if (this.stopped) {
        throw new IOException("Received the shutdown message while waiting.");
      }
    }
  }

  /**
   * @return False if cluster shutdown in progress
   */
  private boolean isClusterUp() {
    return clusterStatusTracker != null && clusterStatusTracker.isClusterUp();
  }

  private void initializeThreads() throws IOException {
    // Cache flushing thread.
    this.cacheFlusher = new MemStoreFlusher(conf, this);

    // Compaction thread
    this.compactSplitThread = new CompactSplitThread(this);

    // Background thread to check for compactions; needed if region has not gotten updates
    // in a while. It will take care of not checking too frequently on store-by-store basis.
    this.compactionChecker = new CompactionChecker(this, this.threadWakeFrequency, this);
    this.periodicFlusher = new PeriodicMemstoreFlusher(this.threadWakeFrequency, this);
    this.leases = new Leases(this.threadWakeFrequency);

    // Create the thread to clean the moved regions list
    movedRegionsCleaner = MovedRegionsCleaner.createAndStart(this);

    if (this.nonceManager != null) {
      // Create the chore that cleans up nonces.
      nonceManagerChore = this.nonceManager.createCleanupChore(this);
    }

    // Setup the Quota Manager
    rsQuotaManager = new RegionServerQuotaManager(this);

    // Setup RPC client for master communication
    rpcClient = new RpcClient(conf, clusterId, new InetSocketAddress(
      rpcServices.isa.getAddress(), 0));

    int storefileRefreshPeriod = conf.getInt(StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD
      , StorefileRefresherChore.DEFAULT_REGIONSERVER_STOREFILE_REFRESH_PERIOD);
    if (storefileRefreshPeriod > 0) {
      this.storefileRefresher = new StorefileRefresherChore(storefileRefreshPeriod, this, this);
    }
    registerConfigurationObservers();
  }

  private void registerConfigurationObservers() {
    // Registering the compactSplitThread object with the ConfigurationManager.
    configurationManager.registerObserver(this.compactSplitThread);
  }

  /**
   * The HRegionServer sticks in this loop until closed.
   */
  @Override
  public void run() {
    try {
      // Do pre-registration initializations; zookeeper, lease threads, etc.
      preRegistrationInitialization();
    } catch (Throwable e) {
      abort("Fatal exception during initialization", e);
    }

    try {
      if (!isStopped() && !isAborted()) {
        ShutdownHook.install(conf, fs, this, Thread.currentThread());
        // Set our ephemeral znode up in zookeeper now we have a name.
        createMyEphemeralNode();
        // Initialize the RegionServerCoprocessorHost now that our ephemeral
        // node was created, in case any coprocessors want to use ZooKeeper
        this.rsHost = new RegionServerCoprocessorHost(this, this.conf);
      }

      // Try and register with the Master; tell it we are here.  Break if
      // server is stopped or the clusterup flag is down or hdfs went wacky.
      while (keepLooping()) {
        RegionServerStartupResponse w = reportForDuty();
        if (w == null) {
          LOG.warn("reportForDuty failed; sleeping and then retrying.");
          this.sleeper.sleep();
        } else {
          handleReportForDutyResponse(w);
          break;
        }
      }

      if (!isStopped() && isHealthy()){
        // start the snapshot handler and other procedure handlers,
        // since the server is ready to run
        rspmHost.start();

        // Start the Quota Manager
        rsQuotaManager.start(getRpcServer().getScheduler());
      }

      // We registered with the Master.  Go into run mode.
      long lastMsg = System.currentTimeMillis();
      long oldRequestCount = -1;
      // The main run loop.
      while (!isStopped() && isHealthy()) {
        if (!isClusterUp()) {
          if (isOnlineRegionsEmpty()) {
            stop("Exiting; cluster shutdown set and not carrying any regions");
          } else if (!this.stopping) {
            this.stopping = true;
            LOG.info("Closing user regions");
            closeUserRegions(this.abortRequested);
          } else if (this.stopping) {
            boolean allUserRegionsOffline = areAllUserRegionsOffline();
            if (allUserRegionsOffline) {
              // Set stopped if no more write requests tp meta tables
              // since last time we went around the loop.  Any open
              // meta regions will be closed on our way out.
              if (oldRequestCount == getWriteRequestCount()) {
                stop("Stopped; only catalog regions remaining online");
                break;
              }
              oldRequestCount = getWriteRequestCount();
            } else {
              // Make sure all regions have been closed -- some regions may
              // have not got it because we were splitting at the time of
              // the call to closeUserRegions.
              closeUserRegions(this.abortRequested);
            }
            LOG.debug("Waiting on " + getOnlineRegionsAsPrintableString());
          }
        }
        long now = System.currentTimeMillis();
        if ((now - lastMsg) >= msgInterval) {
          tryRegionServerReport(lastMsg, now);
          lastMsg = System.currentTimeMillis();
          doMetrics();
        }
        if (!isStopped() && !isAborted()) {
          this.sleeper.sleep();
        }
      } // for
    } catch (Throwable t) {
      if (!rpcServices.checkOOME(t)) {
        String prefix = t instanceof YouAreDeadException? "": "Unhandled: ";
        abort(prefix + t.getMessage(), t);
      }
    }
    // Run shutdown.
    if (mxBean != null) {
      MBeanUtil.unregisterMBean(mxBean);
      mxBean = null;
    }
    if (this.leases != null) this.leases.closeAfterLeasesExpire();
    if (this.splitLogWorker != null) {
      splitLogWorker.stop();
    }
    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (Exception e) {
        LOG.error("Failed to stop infoServer", e);
      }
    }
    // Send cache a shutdown.
    if (cacheConfig != null && cacheConfig.isBlockCacheEnabled()) {
      cacheConfig.getBlockCache().shutdown();
    }

    if (movedRegionsCleaner != null) {
      movedRegionsCleaner.stop("Region Server stopping");
    }

    // Send interrupts to wake up threads if sleeping so they notice shutdown.
    // TODO: Should we check they are alive? If OOME could have exited already
    if(this.hMemManager != null) this.hMemManager.stop();
    if (this.cacheFlusher != null) this.cacheFlusher.interruptIfNecessary();
    if (this.compactSplitThread != null) this.compactSplitThread.interruptIfNecessary();
    if (this.compactionChecker != null)
      this.compactionChecker.interrupt();
    if (this.healthCheckChore != null) {
      this.healthCheckChore.interrupt();
    }
    if (this.nonceManagerChore != null) {
      this.nonceManagerChore.interrupt();
    }
    if (this.storefileRefresher != null) {
      this.storefileRefresher.interrupt();
    }

    // Stop the quota manager
    if (rsQuotaManager != null) {
      rsQuotaManager.stop();
    }

    // Stop the snapshot and other procedure handlers, forcefully killing all running tasks
    if (rspmHost != null) {
      rspmHost.stop(this.abortRequested || this.killed);
    }

    if (this.killed) {
      // Just skip out w/o closing regions.  Used when testing.
    } else if (abortRequested) {
      if (this.fsOk) {
        closeUserRegions(abortRequested); // Don't leave any open file handles
      }
      LOG.info("aborting server " + this.serverName);
    } else {
      closeUserRegions(abortRequested);
      LOG.info("stopping server " + this.serverName);
    }

    // so callers waiting for meta without timeout can stop
    if (this.metaTableLocator != null) this.metaTableLocator.stop();
    if (this.shortCircuitConnection != null && !shortCircuitConnection.isClosed()) {
      try {
        this.shortCircuitConnection.close();
      } catch (IOException e) {
        // Although the {@link Closeable} interface throws an {@link
        // IOException}, in reality, the implementation would never do that.
        LOG.error("Attempt to close server's short circuit HConnection failed.", e);
      }
    }

    // Closing the compactSplit thread before closing meta regions
    if (!this.killed && containsMetaTableRegions()) {
      if (!abortRequested || this.fsOk) {
        if (this.compactSplitThread != null) {
          this.compactSplitThread.join();
          this.compactSplitThread = null;
        }
        closeMetaTableRegions(abortRequested);
      }
    }

    if (!this.killed && this.fsOk) {
      waitOnAllRegionsToClose(abortRequested);
      LOG.info("stopping server " + this.serverName +
        "; all regions closed.");
    }

    //fsOk flag may be changed when closing regions throws exception.
    if (this.fsOk) {
      closeWAL(!abortRequested);
    }

    // Make sure the proxy is down.
    if (this.rssStub != null) {
      this.rssStub = null;
    }
    if (this.rpcClient != null) {
      this.rpcClient.stop();
    }
    if (this.leases != null) {
      this.leases.close();
    }
    if (this.pauseMonitor != null) {
      this.pauseMonitor.stop();
    }

    if (!killed) {
      stopServiceThreads();
    }

    if (this.rpcServices != null) {
      this.rpcServices.stop();
    }

    try {
      deleteMyEphemeralNode();
    } catch (KeeperException.NoNodeException nn) {
    } catch (KeeperException e) {
      LOG.warn("Failed deleting my ephemeral node", e);
    }
    // We may have failed to delete the znode at the previous step, but
    //  we delete the file anyway: a second attempt to delete the znode is likely to fail again.
    ZNodeClearer.deleteMyEphemeralNodeOnDisk();

    if (this.zooKeeper != null) {
      this.zooKeeper.close();
    }
    LOG.info("stopping server " + this.serverName +
      "; zookeeper connection closed.");

    LOG.info(Thread.currentThread().getName() + " exiting");
  }

  private boolean containsMetaTableRegions() {
    return onlineRegions.containsKey(HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
  }

  private boolean areAllUserRegionsOffline() {
    if (getNumberOfOnlineRegions() > 2) return false;
    boolean allUserRegionsOffline = true;
    for (Map.Entry<String, HRegion> e: this.onlineRegions.entrySet()) {
      if (!e.getValue().getRegionInfo().isMetaTable()) {
        allUserRegionsOffline = false;
        break;
      }
    }
    return allUserRegionsOffline;
  }

  /**
   * @return Current write count for all online regions.
   */
  private long getWriteRequestCount() {
    int writeCount = 0;
    for (Map.Entry<String, HRegion> e: this.onlineRegions.entrySet()) {
      writeCount += e.getValue().getWriteRequestsCount();
    }
    return writeCount;
  }

  @VisibleForTesting
  protected void tryRegionServerReport(long reportStartTime, long reportEndTime)
  throws IOException {
    RegionServerStatusService.BlockingInterface rss = rssStub;
    if (rss == null) {
      // the current server could be stopping.
      return;
    }
    ClusterStatusProtos.ServerLoad sl = buildServerLoad(reportStartTime, reportEndTime);
    try {
      RegionServerReportRequest.Builder request = RegionServerReportRequest.newBuilder();
      ServerName sn = ServerName.parseVersionedServerName(
        this.serverName.getVersionedBytes());
      request.setServer(ProtobufUtil.toServerName(sn));
      request.setLoad(sl);
      rss.regionServerReport(null, request.build());
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
      createRegionServerStatusStub();
    }
  }

  ClusterStatusProtos.ServerLoad buildServerLoad(long reportStartTime, long reportEndTime) {
    // We're getting the MetricsRegionServerWrapper here because the wrapper computes requests
    // per second, and other metrics  As long as metrics are part of ServerLoad it's best to use
    // the wrapper to compute those numbers in one place.
    // In the long term most of these should be moved off of ServerLoad and the heart beat.
    // Instead they should be stored in an HBase table so that external visibility into HBase is
    // improved; Additionally the load balancer will be able to take advantage of a more complete
    // history.
    MetricsRegionServerWrapper regionServerWrapper = this.metricsRegionServer.getRegionServerWrapper();
    Collection<HRegion> regions = getOnlineRegionsLocalContext();
    MemoryUsage memory =
      ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();

    ClusterStatusProtos.ServerLoad.Builder serverLoad =
      ClusterStatusProtos.ServerLoad.newBuilder();
    serverLoad.setNumberOfRequests((int) regionServerWrapper.getRequestsPerSecond());
    serverLoad.setTotalNumberOfRequests((int) regionServerWrapper.getTotalRequestCount());
    serverLoad.setUsedHeapMB((int)(memory.getUsed() / 1024 / 1024));
    serverLoad.setMaxHeapMB((int) (memory.getMax() / 1024 / 1024));
    Set<String> coprocessors = this.hlog.getCoprocessorHost().getCoprocessors();
    for (String coprocessor : coprocessors) {
      serverLoad.addCoprocessors(
        Coprocessor.newBuilder().setName(coprocessor).build());
    }
    RegionLoad.Builder regionLoadBldr = RegionLoad.newBuilder();
    RegionSpecifier.Builder regionSpecifier = RegionSpecifier.newBuilder();
    for (HRegion region : regions) {
      serverLoad.addRegionLoads(createRegionLoad(region, regionLoadBldr, regionSpecifier));
    }
    serverLoad.setReportStartTime(reportStartTime);
    serverLoad.setReportEndTime(reportEndTime);
    if (this.infoServer != null) {
      serverLoad.setInfoServerPort(this.infoServer.getPort());
    } else {
      serverLoad.setInfoServerPort(-1);
    }
    return serverLoad.build();
  }

  String getOnlineRegionsAsPrintableString() {
    StringBuilder sb = new StringBuilder();
    for (HRegion r: this.onlineRegions.values()) {
      if (sb.length() > 0) sb.append(", ");
      sb.append(r.getRegionInfo().getEncodedName());
    }
    return sb.toString();
  }

  /**
   * Wait on regions close.
   */
  private void waitOnAllRegionsToClose(final boolean abort) {
    // Wait till all regions are closed before going out.
    int lastCount = -1;
    long previousLogTime = 0;
    Set<String> closedRegions = new HashSet<String>();
    boolean interrupted = false;
    try {
      while (!isOnlineRegionsEmpty()) {
        int count = getNumberOfOnlineRegions();
        // Only print a message if the count of regions has changed.
        if (count != lastCount) {
          // Log every second at most
          if (System.currentTimeMillis() > (previousLogTime + 1000)) {
            previousLogTime = System.currentTimeMillis();
            lastCount = count;
            LOG.info("Waiting on " + count + " regions to close");
            // Only print out regions still closing if a small number else will
            // swamp the log.
            if (count < 10 && LOG.isDebugEnabled()) {
              LOG.debug(this.onlineRegions);
            }
          }
        }
        // Ensure all user regions have been sent a close. Use this to
        // protect against the case where an open comes in after we start the
        // iterator of onlineRegions to close all user regions.
        for (Map.Entry<String, HRegion> e : this.onlineRegions.entrySet()) {
          HRegionInfo hri = e.getValue().getRegionInfo();
          if (!this.regionsInTransitionInRS.containsKey(hri.getEncodedNameAsBytes())
              && !closedRegions.contains(hri.getEncodedName())) {
            closedRegions.add(hri.getEncodedName());
            // Don't update zk with this close transition; pass false.
            closeRegionIgnoreErrors(hri, abort);
              }
        }
        // No regions in RIT, we could stop waiting now.
        if (this.regionsInTransitionInRS.isEmpty()) {
          if (!isOnlineRegionsEmpty()) {
            LOG.info("We were exiting though online regions are not empty," +
                " because some regions failed closing");
          }
          break;
        }
        if (sleep(200)) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private boolean sleep(long millis) {
    boolean interrupted = false;
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while sleeping");
      interrupted = true;
    }
    return interrupted;
  }

  private void closeWAL(final boolean delete) {
    if (this.hlogForMeta != null) {
      // All hlogs (meta and non-meta) are in the same directory. Don't call
      // closeAndDelete here since that would delete all hlogs not just the
      // meta ones. We will just 'close' the hlog for meta here, and leave
      // the directory cleanup to the follow-on closeAndDelete call.
      try {
        this.hlogForMeta.close();
      } catch (Throwable e) {
        e = e instanceof RemoteException ? ((RemoteException) e).unwrapRemoteException() : e;
        LOG.error("Metalog close and delete failed", e);
      }
    }
    if (this.hlog != null) {
      try {
        if (delete) {
          hlog.closeAndDelete();
        } else {
          hlog.close();
        }
      } catch (Throwable e) {
        e = e instanceof RemoteException ? ((RemoteException) e).unwrapRemoteException() : e;
        LOG.error("Close and delete failed", e);
      }
    }
  }

  /*
   * Run init. Sets up hlog and starts up all server threads.
   *
   * @param c Extra configuration.
   */
  protected void handleReportForDutyResponse(final RegionServerStartupResponse c)
  throws IOException {
    try {
      for (NameStringPair e : c.getMapEntriesList()) {
        String key = e.getName();
        // The hostname the master sees us as.
        if (key.equals(HConstants.KEY_FOR_HOSTNAME_SEEN_BY_MASTER)) {
          String hostnameFromMasterPOV = e.getValue();
          this.serverName = ServerName.valueOf(hostnameFromMasterPOV,
            rpcServices.isa.getPort(), this.startcode);
          if (!hostnameFromMasterPOV.equals(rpcServices.isa.getHostName())) {
            LOG.info("Master passed us a different hostname to use; was=" +
              rpcServices.isa.getHostName() + ", but now=" + hostnameFromMasterPOV);
          }
          continue;
        }
        String value = e.getValue();
        if (LOG.isDebugEnabled()) {
          LOG.info("Config from master: " + key + "=" + value);
        }
        this.conf.set(key, value);
      }

      // hack! Maps DFSClient => RegionServer for logs.  HDFS made this
      // config param for task trackers, but we can piggyback off of it.
      if (this.conf.get("mapreduce.task.attempt.id") == null) {
        this.conf.set("mapreduce.task.attempt.id", "hb_rs_" +
          this.serverName.toString());
      }

      // Save it in a file, this will allow to see if we crash
      ZNodeClearer.writeMyEphemeralNodeOnDisk(getMyEphemeralNodePath());

      this.cacheConfig = new CacheConfig(conf);
      this.hlog = setupWALAndReplication();
      // Init in here rather than in constructor after thread name has been set
      this.metricsRegionServer = new MetricsRegionServer(new MetricsRegionServerWrapperImpl(this));

      startServiceThreads();
      startHeapMemoryManager();
      LOG.info("Serving as " + this.serverName +
        ", RpcServer on " + rpcServices.isa +
        ", sessionid=0x" +
        Long.toHexString(this.zooKeeper.getRecoverableZooKeeper().getSessionId()));

      // Wake up anyone waiting for this server to online
      synchronized (online) {
        online.set(true);
        online.notifyAll();
      }
    } catch (Throwable e) {
      stop("Failed initialization");
      throw convertThrowableToIOE(cleanup(e, "Failed init"),
          "Region server startup failed");
    } finally {
      sleeper.skipSleepCycle();
    }
  }

  private void startHeapMemoryManager() {
    this.hMemManager = HeapMemoryManager.create(this.conf, this.cacheFlusher, this);
    if (this.hMemManager != null) {
      this.hMemManager.start();
    }
  }

  private void createMyEphemeralNode() throws KeeperException, IOException {
    RegionServerInfo.Builder rsInfo = RegionServerInfo.newBuilder();
    rsInfo.setInfoPort(infoServer != null ? infoServer.getPort() : -1);
    byte[] data = ProtobufUtil.prependPBMagic(rsInfo.build().toByteArray());
    ZKUtil.createEphemeralNodeAndWatch(this.zooKeeper,
      getMyEphemeralNodePath(), data);
  }

  private void deleteMyEphemeralNode() throws KeeperException {
    ZKUtil.deleteNode(this.zooKeeper, getMyEphemeralNodePath());
  }

  @Override
  public RegionServerAccounting getRegionServerAccounting() {
    return regionServerAccounting;
  }

  @Override
  public TableLockManager getTableLockManager() {
    return tableLockManager;
  }

  /*
   * @param r Region to get RegionLoad for.
   * @param regionLoadBldr the RegionLoad.Builder, can be null
   * @param regionSpecifier the RegionSpecifier.Builder, can be null
   * @return RegionLoad instance.
   *
   * @throws IOException
   */
  private RegionLoad createRegionLoad(final HRegion r, RegionLoad.Builder regionLoadBldr,
      RegionSpecifier.Builder regionSpecifier) {
    byte[] name = r.getRegionName();
    int stores = 0;
    int storefiles = 0;
    int storeUncompressedSizeMB = 0;
    int storefileSizeMB = 0;
    int memstoreSizeMB = (int) (r.memstoreSize.get() / 1024 / 1024);
    int storefileIndexSizeMB = 0;
    int rootIndexSizeKB = 0;
    int totalStaticIndexSizeKB = 0;
    int totalStaticBloomSizeKB = 0;
    long totalCompactingKVs = 0;
    long currentCompactedKVs = 0;
    synchronized (r.stores) {
      stores += r.stores.size();
      for (Store store : r.stores.values()) {
        storefiles += store.getStorefilesCount();
        storeUncompressedSizeMB += (int) (store.getStoreSizeUncompressed()
            / 1024 / 1024);
        storefileSizeMB += (int) (store.getStorefilesSize() / 1024 / 1024);
        storefileIndexSizeMB += (int) (store.getStorefilesIndexSize() / 1024 / 1024);
        CompactionProgress progress = store.getCompactionProgress();
        if (progress != null) {
          totalCompactingKVs += progress.totalCompactingKVs;
          currentCompactedKVs += progress.currentCompactedKVs;
        }

        rootIndexSizeKB +=
            (int) (store.getStorefilesIndexSize() / 1024);

        totalStaticIndexSizeKB +=
          (int) (store.getTotalStaticIndexSize() / 1024);

        totalStaticBloomSizeKB +=
          (int) (store.getTotalStaticBloomSize() / 1024);
      }
    }
    if (regionLoadBldr == null) {
      regionLoadBldr = RegionLoad.newBuilder();
    }
    if (regionSpecifier == null) {
      regionSpecifier = RegionSpecifier.newBuilder();
    }
    regionSpecifier.setType(RegionSpecifierType.REGION_NAME);
    regionSpecifier.setValue(ByteStringer.wrap(name));
    regionLoadBldr.setRegionSpecifier(regionSpecifier.build())
      .setStores(stores)
      .setStorefiles(storefiles)
      .setStoreUncompressedSizeMB(storeUncompressedSizeMB)
      .setStorefileSizeMB(storefileSizeMB)
      .setMemstoreSizeMB(memstoreSizeMB)
      .setStorefileIndexSizeMB(storefileIndexSizeMB)
      .setRootIndexSizeKB(rootIndexSizeKB)
      .setTotalStaticIndexSizeKB(totalStaticIndexSizeKB)
      .setTotalStaticBloomSizeKB(totalStaticBloomSizeKB)
      .setReadRequestsCount(r.readRequestsCount.get())
      .setWriteRequestsCount(r.writeRequestsCount.get())
      .setTotalCompactingKVs(totalCompactingKVs)
      .setCurrentCompactedKVs(currentCompactedKVs)
      .setCompleteSequenceId(r.lastFlushSeqId);

    return regionLoadBldr.build();
  }

  /**
   * @param encodedRegionName
   * @return An instance of RegionLoad.
   */
  public RegionLoad createRegionLoad(final String encodedRegionName) {
    HRegion r = null;
    r = this.onlineRegions.get(encodedRegionName);
    return r != null ? createRegionLoad(r, null, null) : null;
  }

  /*
   * Inner class that runs on a long period checking if regions need compaction.
   */
  private static class CompactionChecker extends Chore {
    private final HRegionServer instance;
    private final int majorCompactPriority;
    private final static int DEFAULT_PRIORITY = Integer.MAX_VALUE;
    private long iteration = 0;

    CompactionChecker(final HRegionServer h, final int sleepTime,
        final Stoppable stopper) {
      super("CompactionChecker", sleepTime, h);
      this.instance = h;
      LOG.info(this.getName() + " runs every " + StringUtils.formatTime(sleepTime));

      /* MajorCompactPriority is configurable.
       * If not set, the compaction will use default priority.
       */
      this.majorCompactPriority = this.instance.conf.
        getInt("hbase.regionserver.compactionChecker.majorCompactPriority",
        DEFAULT_PRIORITY);
    }

    @Override
    protected void chore() {
      for (HRegion r : this.instance.onlineRegions.values()) {
        if (r == null)
          continue;
        for (Store s : r.getStores().values()) {
          try {
            long multiplier = s.getCompactionCheckMultiplier();
            assert multiplier > 0;
            if (iteration % multiplier != 0) continue;
            if (s.needsCompaction()) {
              // Queue a compaction. Will recognize if major is needed.
              this.instance.compactSplitThread.requestSystemCompaction(r, s, getName()
                  + " requests compaction");
            } else if (s.isMajorCompaction()) {
              if (majorCompactPriority == DEFAULT_PRIORITY
                  || majorCompactPriority > r.getCompactPriority()) {
                this.instance.compactSplitThread.requestCompaction(r, s, getName()
                    + " requests major compaction; use default priority", null);
              } else {
                this.instance.compactSplitThread.requestCompaction(r, s, getName()
                    + " requests major compaction; use configured priority",
                  this.majorCompactPriority, null);
              }
            }
          } catch (IOException e) {
            LOG.warn("Failed major compaction check on " + r, e);
          }
        }
      }
      iteration = (iteration == Long.MAX_VALUE) ? 0 : (iteration + 1);
    }
  }

  class PeriodicMemstoreFlusher extends Chore {
    final HRegionServer server;
    final static int RANGE_OF_DELAY = 20000; //millisec
    final static int MIN_DELAY_TIME = 3000; //millisec
    public PeriodicMemstoreFlusher(int cacheFlushInterval, final HRegionServer server) {
      super(server.getServerName() + "-MemstoreFlusherChore", cacheFlushInterval, server);
      this.server = server;
    }

    @Override
    protected void chore() {
      for (HRegion r : this.server.onlineRegions.values()) {
        if (r == null)
          continue;
        if (r.shouldFlush()) {
          FlushRequester requester = server.getFlushRequester();
          if (requester != null) {
            long randomDelay = RandomUtils.nextInt(RANGE_OF_DELAY) + MIN_DELAY_TIME;
            LOG.info(getName() + " requesting flush for region " + r.getRegionNameAsString() +
                " after a delay of " + randomDelay);
            //Throttle the flushes by putting a delay. If we don't throttle, and there
            //is a balanced write-load on the regions in a table, we might end up
            //overwhelming the filesystem with too many flushes at once.
            requester.requestDelayedFlush(r, randomDelay);
          }
        }
      }
    }
  }

  /**
   * Report the status of the server. A server is online once all the startup is
   * completed (setting up filesystem, starting service threads, etc.). This
   * method is designed mostly to be useful in tests.
   *
   * @return true if online, false if not.
   */
  public boolean isOnline() {
    return online.get();
  }

  /**
   * Setup WAL log and replication if enabled.
   * Replication setup is done in here because it wants to be hooked up to WAL.
   * @return A WAL instance.
   * @throws IOException
   */
  private HLog setupWALAndReplication() throws IOException {
    final Path oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    final String logName
      = HLogUtil.getHLogDirectoryName(this.serverName.toString());

    Path logdir = new Path(rootDir, logName);
    if (LOG.isDebugEnabled()) LOG.debug("logdir=" + logdir);
    if (this.fs.exists(logdir)) {
      throw new RegionServerRunningException("Region server has already " +
        "created directory at " + this.serverName.toString());
    }

    // Instantiate replication manager if replication enabled.  Pass it the
    // log directories.
    createNewReplicationInstance(conf, this, this.fs, logdir, oldLogDir);

    return instantiateHLog(rootDir, logName);
  }

  private HLog getMetaWAL() throws IOException {
    if (this.hlogForMeta != null) return this.hlogForMeta;
    final String logName = HLogUtil.getHLogDirectoryName(this.serverName.toString());
    Path logdir = new Path(rootDir, logName);
    if (LOG.isDebugEnabled()) LOG.debug("logdir=" + logdir);
    this.hlogForMeta = HLogFactory.createMetaHLog(this.fs.getBackingFs(), rootDir, logName,
      this.conf, getMetaWALActionListeners(), this.serverName.toString());
    return this.hlogForMeta;
  }

  /**
   * Called by {@link #setupWALAndReplication()} creating WAL instance.
   * @param rootdir
   * @param logName
   * @return WAL instance.
   * @throws IOException
   */
  protected HLog instantiateHLog(Path rootdir, String logName) throws IOException {
    return HLogFactory.createHLog(this.fs.getBackingFs(), rootdir, logName, this.conf,
      getWALActionListeners(), this.serverName.toString());
  }

  /**
   * Called by {@link #instantiateHLog(Path, String)} setting up WAL instance.
   * Add any {@link WALActionsListener}s you want inserted before WAL startup.
   * @return List of WALActionsListener that will be passed in to
   * {@link org.apache.hadoop.hbase.regionserver.wal.FSHLog} on construction.
   */
  protected List<WALActionsListener> getWALActionListeners() {
    List<WALActionsListener> listeners = new ArrayList<WALActionsListener>();
    // Log roller.
    this.hlogRoller = new LogRoller(this, this);
    listeners.add(this.hlogRoller);
    if (this.replicationSourceHandler != null &&
        this.replicationSourceHandler.getWALActionsListener() != null) {
      // Replication handler is an implementation of WALActionsListener.
      listeners.add(this.replicationSourceHandler.getWALActionsListener());
    }
    return listeners;
  }

  protected List<WALActionsListener> getMetaWALActionListeners() {
    List<WALActionsListener> listeners = new ArrayList<WALActionsListener>();
    // Using a tmp log roller to ensure metaLogRoller is alive once it is not
    // null
    MetaLogRoller tmpLogRoller = new MetaLogRoller(this, this);
    String n = Thread.currentThread().getName();
    Threads.setDaemonThreadRunning(tmpLogRoller.getThread(),
        n + "-MetaLogRoller", uncaughtExceptionHandler);
    this.metaHLogRoller = tmpLogRoller;
    tmpLogRoller = null;
    listeners.add(this.metaHLogRoller);
    return listeners;
  }

  protected LogRoller getLogRoller() {
    return hlogRoller;
  }

  public MetricsRegionServer getRegionServerMetrics() {
    return this.metricsRegionServer;
  }

  /**
   * @return Master address tracker instance.
   */
  public MasterAddressTracker getMasterAddressTracker() {
    return this.masterAddressTracker;
  }

  /*
   * Start maintenance Threads, Server, Worker and lease checker threads.
   * Install an UncaughtExceptionHandler that calls abort of RegionServer if we
   * get an unhandled exception. We cannot set the handler on all threads.
   * Server's internal Listener thread is off limits. For Server, if an OOME, it
   * waits a while then retries. Meantime, a flush or a compaction that tries to
   * run should trigger same critical condition and the shutdown will run. On
   * its way out, this server will shut down Server. Leases are sort of
   * inbetween. It has an internal thread that while it inherits from Chore, it
   * keeps its own internal stop mechanism so needs to be stopped by this
   * hosting server. Worker logs the exception and exits.
   */
  private void startServiceThreads() throws IOException {
    // Start executor services
    this.service.startExecutorService(ExecutorType.RS_OPEN_REGION,
      conf.getInt("hbase.regionserver.executor.openregion.threads", 3));
    this.service.startExecutorService(ExecutorType.RS_OPEN_META,
      conf.getInt("hbase.regionserver.executor.openmeta.threads", 1));
    this.service.startExecutorService(ExecutorType.RS_CLOSE_REGION,
      conf.getInt("hbase.regionserver.executor.closeregion.threads", 3));
    this.service.startExecutorService(ExecutorType.RS_CLOSE_META,
      conf.getInt("hbase.regionserver.executor.closemeta.threads", 1));
    if (conf.getBoolean(StoreScanner.STORESCANNER_PARALLEL_SEEK_ENABLE, false)) {
      this.service.startExecutorService(ExecutorType.RS_PARALLEL_SEEK,
        conf.getInt("hbase.storescanner.parallel.seek.threads", 10));
    }
    this.service.startExecutorService(ExecutorType.RS_LOG_REPLAY_OPS, conf.getInt(
       "hbase.regionserver.wal.max.splitters", SplitLogWorkerCoordination.DEFAULT_MAX_SPLITTERS));

    Threads.setDaemonThreadRunning(this.hlogRoller.getThread(), getName() + ".logRoller",
        uncaughtExceptionHandler);
    this.cacheFlusher.start(uncaughtExceptionHandler);
    Threads.setDaemonThreadRunning(this.compactionChecker.getThread(), getName() +
      ".compactionChecker", uncaughtExceptionHandler);
    Threads.setDaemonThreadRunning(this.periodicFlusher.getThread(), getName() +
        ".periodicFlusher", uncaughtExceptionHandler);
    if (this.healthCheckChore != null) {
      Threads.setDaemonThreadRunning(this.healthCheckChore.getThread(), getName() + ".healthChecker",
            uncaughtExceptionHandler);
    }
    if (this.nonceManagerChore != null) {
      Threads.setDaemonThreadRunning(this.nonceManagerChore.getThread(), getName() + ".nonceCleaner",
            uncaughtExceptionHandler);
    }
    if (this.storefileRefresher != null) {
      Threads.setDaemonThreadRunning(this.storefileRefresher.getThread(), getName() + ".storefileRefresher",
            uncaughtExceptionHandler);
    }

    // Leases is not a Thread. Internally it runs a daemon thread. If it gets
    // an unhandled exception, it will just exit.
    this.leases.setName(getName() + ".leaseChecker");
    this.leases.start();

    if (this.replicationSourceHandler == this.replicationSinkHandler &&
        this.replicationSourceHandler != null) {
      this.replicationSourceHandler.startReplicationService();
    } else {
      if (this.replicationSourceHandler != null) {
        this.replicationSourceHandler.startReplicationService();
      }
      if (this.replicationSinkHandler != null) {
        this.replicationSinkHandler.startReplicationService();
      }
    }

    // Create the log splitting worker and start it
    // set a smaller retries to fast fail otherwise splitlogworker could be blocked for
    // quite a while inside HConnection layer. The worker won't be available for other
    // tasks even after current task is preempted after a split task times out.
    Configuration sinkConf = HBaseConfiguration.create(conf);
    sinkConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      conf.getInt("hbase.log.replay.retries.number", 8)); // 8 retries take about 23 seconds
    sinkConf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
      conf.getInt("hbase.log.replay.rpc.timeout", 30000)); // default 30 seconds
    sinkConf.setInt("hbase.client.serverside.retries.multiplier", 1);
    this.splitLogWorker = new SplitLogWorker(this, sinkConf, this, this);
    splitLogWorker.start();
  }

  /**
   * Puts up the webui.
   * @return Returns final port -- maybe different from what we started with.
   * @throws IOException
   */
  private int putUpWebUI() throws IOException {
    int port = this.conf.getInt(HConstants.REGIONSERVER_INFO_PORT,
      HConstants.DEFAULT_REGIONSERVER_INFOPORT);
    // -1 is for disabling info server
    if (port < 0) return port;
    String addr = this.conf.get("hbase.regionserver.info.bindAddress", "0.0.0.0");
    if (!Addressing.isLocalAddress(InetAddress.getByName(addr))) {
      String msg =
          "Failed to start http info server. Address " + addr
              + " does not belong to this host. Correct configuration parameter: "
              + "hbase.regionserver.info.bindAddress";
      LOG.error(msg);
      throw new IOException(msg);
    }
    // check if auto port bind enabled
    boolean auto = this.conf.getBoolean(HConstants.REGIONSERVER_INFO_PORT_AUTO,
        false);
    while (true) {
      try {
        this.infoServer = new InfoServer(getProcessName(), addr, port, false, this.conf);
        infoServer.addServlet("dump", "/dump", getDumpServlet());
        configureInfoServer();
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
      }
    }
    port = this.infoServer.getPort();
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, port);
    int masterInfoPort = conf.getInt(HConstants.MASTER_INFO_PORT,
      HConstants.DEFAULT_MASTER_INFOPORT);
    conf.setInt("hbase.master.info.port.orig", masterInfoPort);
    conf.setInt(HConstants.MASTER_INFO_PORT, port);
    return port;
  }

  /*
   * Verify that server is healthy
   */
  private boolean isHealthy() {
    if (!fsOk) {
      // File system problem
      return false;
    }
    // Verify that all threads are alive
    if (!(leases.isAlive()
        && cacheFlusher.isAlive() && hlogRoller.isAlive()
        && this.compactionChecker.isAlive()
        && this.periodicFlusher.isAlive())) {
      stop("One or more threads are no longer alive -- stop");
      return false;
    }
    if (metaHLogRoller != null && !metaHLogRoller.isAlive()) {
      stop("Meta HLog roller thread is no longer alive -- stop");
      return false;
    }
    return true;
  }

  public HLog getWAL() {
    try {
      return getWAL(null);
    } catch (IOException e) {
      LOG.warn("getWAL threw exception " + e);
      return null;
    }
  }

  @Override
  public HLog getWAL(HRegionInfo regionInfo) throws IOException {
    //TODO: at some point this should delegate to the HLogFactory
    //currently, we don't care about the region as much as we care about the
    //table.. (hence checking the tablename below)
    //_ROOT_ and hbase:meta regions have separate WAL.
    if (regionInfo != null && regionInfo.isMetaTable()) {
      return getMetaWAL();
    }
    return this.hlog;
  }

  @Override
  public HConnection getShortCircuitConnection() {
    return this.shortCircuitConnection;
  }

  @Override
  public MetaTableLocator getMetaTableLocator() {
    return this.metaTableLocator;
  }

  @Override
  public void stop(final String msg) {
    if (!this.stopped) {
      try {
        if (this.rsHost != null) {
          this.rsHost.preStop(msg);
        }
        this.stopped = true;
        LOG.info("STOPPED: " + msg);
        // Wakes run() if it is sleeping
        sleeper.skipSleepCycle();
      } catch (IOException exp) {
        LOG.warn("The region server did not stop", exp);
      }
    }
  }

  public void waitForServerOnline(){
    while (!isStopped() && !isOnline()) {
      synchronized (online) {
        try {
          online.wait(msgInterval);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  @Override
  public void postOpenDeployTasks(final HRegion r)
  throws KeeperException, IOException {
    rpcServices.checkOpen();
    LOG.info("Post open deploy tasks for " + r.getRegionNameAsString());
    // Do checks to see if we need to compact (references or too many files)
    for (Store s : r.getStores().values()) {
      if (s.hasReferences() || s.needsCompaction()) {
       this.compactSplitThread.requestSystemCompaction(r, s, "Opening Region");
      }
    }
    long openSeqNum = r.getOpenSeqNum();
    if (openSeqNum == HConstants.NO_SEQNUM) {
      // If we opened a region, we should have read some sequence number from it.
      LOG.error("No sequence number found when opening " + r.getRegionNameAsString());
      openSeqNum = 0;
    }

    // Update flushed sequence id of a recovering region in ZK
    updateRecoveringRegionLastFlushedSequenceId(r);

    // Notify master
    if (!reportRegionStateTransition(
        TransitionCode.OPENED, openSeqNum, r.getRegionInfo())) {
      throw new IOException("Failed to report opened region to master: "
        + r.getRegionNameAsString());
    }

    LOG.debug("Finished post open deploy task for " + r.getRegionNameAsString());
  }

  @Override
  public boolean reportRegionStateTransition(TransitionCode code, HRegionInfo... hris) {
    return reportRegionStateTransition(code, HConstants.NO_SEQNUM, hris);
  }

  @Override
  public boolean reportRegionStateTransition(
      TransitionCode code, long openSeqNum, HRegionInfo... hris) {
    if (TEST_SKIP_REPORTING_TRANSITION) {
      // This is for testing only in case there is no master
      // to handle the region transition report at all.
      if (code == TransitionCode.OPENED) {
        Preconditions.checkArgument(hris != null && hris.length == 1);
        if (hris[0].isMetaRegion()) {
          try {
            MetaTableLocator.setMetaLocation(getZooKeeper(), serverName, State.OPEN);
          } catch (KeeperException e) {
            LOG.info("Failed to update meta location", e);
            return false;
          }
        } else {
          try {
            MetaTableAccessor.updateRegionLocation(shortCircuitConnection,
              hris[0], serverName, openSeqNum);
          } catch (IOException e) {
            LOG.info("Failed to update meta", e);
            return false;
          }
        }
      }
      return true;
    }

    ReportRegionStateTransitionRequest.Builder builder =
      ReportRegionStateTransitionRequest.newBuilder();
    builder.setServer(ProtobufUtil.toServerName(serverName));
    RegionStateTransition.Builder transition = builder.addTransitionBuilder();
    transition.setTransitionCode(code);
    if (code == TransitionCode.OPENED && openSeqNum >= 0) {
      transition.setOpenSeqNum(openSeqNum);
    }
    for (HRegionInfo hri: hris) {
      transition.addRegionInfo(HRegionInfo.convert(hri));
    }
    ReportRegionStateTransitionRequest request = builder.build();
    while (keepLooping()) {
      RegionServerStatusService.BlockingInterface rss = rssStub;
      try {
        if (rss == null) {
          createRegionServerStatusStub();
          continue;
        }
        ReportRegionStateTransitionResponse response =
          rss.reportRegionStateTransition(null, request);
        if (response.hasErrorMessage()) {
          LOG.info("Failed to transition " + hris[0]
            + " to " + code + ": " + response.getErrorMessage());
          return false;
        }
        return true;
      } catch (ServiceException se) {
        IOException ioe = ProtobufUtil.getRemoteException(se);
        LOG.info("Failed to report region transition, will retry", ioe);
        if (rssStub == rss) {
          rssStub = null;
        }
      }
    }
    return false;
  }

  @Override
  public RpcServerInterface getRpcServer() {
    return rpcServices.rpcServer;
  }

  @VisibleForTesting
  public RSRpcServices getRSRpcServices() {
    return rpcServices;
  }

  /**
   * Cause the server to exit without closing the regions it is serving, the log
   * it is using and without notifying the master. Used unit testing and on
   * catastrophic events such as HDFS is yanked out from under hbase or we OOME.
   *
   * @param reason
   *          the reason we are aborting
   * @param cause
   *          the exception that caused the abort, or null
   */
  @Override
  public void abort(String reason, Throwable cause) {
    String msg = "ABORTING region server " + this + ": " + reason;
    if (cause != null) {
      LOG.fatal(msg, cause);
    } else {
      LOG.fatal(msg);
    }
    this.abortRequested = true;
    // HBASE-4014: show list of coprocessors that were loaded to help debug
    // regionserver crashes.Note that we're implicitly using
    // java.util.HashSet's toString() method to print the coprocessor names.
    LOG.fatal("RegionServer abort: loaded coprocessors are: " +
        CoprocessorHost.getLoadedCoprocessors());
    // Do our best to report our abort to the master, but this may not work
    try {
      if (cause != null) {
        msg += "\nCause:\n" + StringUtils.stringifyException(cause);
      }
      // Report to the master but only if we have already registered with the master.
      if (rssStub != null && this.serverName != null) {
        ReportRSFatalErrorRequest.Builder builder =
          ReportRSFatalErrorRequest.newBuilder();
        ServerName sn =
          ServerName.parseVersionedServerName(this.serverName.getVersionedBytes());
        builder.setServer(ProtobufUtil.toServerName(sn));
        builder.setErrorMessage(msg);
        rssStub.reportRSFatalError(null, builder.build());
      }
    } catch (Throwable t) {
      LOG.warn("Unable to report fatal error to master", t);
    }
    stop(reason);
  }

  /**
   * @see HRegionServer#abort(String, Throwable)
   */
  public void abort(String reason) {
    abort(reason, null);
  }

  @Override
  public boolean isAborted() {
    return this.abortRequested;
  }

  /*
   * Simulate a kill -9 of this server. Exits w/o closing regions or cleaninup
   * logs but it does close socket in case want to bring up server on old
   * hostname+port immediately.
   */
  protected void kill() {
    this.killed = true;
    abort("Simulated kill");
  }

  /**
   * Wait on all threads to finish. Presumption is that all closes and stops
   * have already been called.
   */
  protected void stopServiceThreads() {
    if (this.nonceManagerChore != null) {
      Threads.shutdown(this.nonceManagerChore.getThread());
    }
    if (this.compactionChecker != null) {
      Threads.shutdown(this.compactionChecker.getThread());
    }
    if (this.periodicFlusher != null) {
      Threads.shutdown(this.periodicFlusher.getThread());
    }
    if (this.cacheFlusher != null) {
      this.cacheFlusher.join();
    }
    if (this.healthCheckChore != null) {
      Threads.shutdown(this.healthCheckChore.getThread());
    }
    if (this.spanReceiverHost != null) {
      this.spanReceiverHost.closeReceivers();
    }
    if (this.hlogRoller != null) {
      Threads.shutdown(this.hlogRoller.getThread());
    }
    if (this.metaHLogRoller != null) {
      Threads.shutdown(this.metaHLogRoller.getThread());
    }
    if (this.compactSplitThread != null) {
      this.compactSplitThread.join();
    }
    if (this.service != null) this.service.shutdown();
    if (this.replicationSourceHandler != null &&
        this.replicationSourceHandler == this.replicationSinkHandler) {
      this.replicationSourceHandler.stopReplicationService();
    } else {
      if (this.replicationSourceHandler != null) {
        this.replicationSourceHandler.stopReplicationService();
      }
      if (this.replicationSinkHandler != null) {
        this.replicationSinkHandler.stopReplicationService();
      }
    }
    if (this.storefileRefresher != null) {
      Threads.shutdown(this.storefileRefresher.getThread());
    }
  }

  /**
   * @return Return the object that implements the replication
   * source service.
   */
  ReplicationSourceService getReplicationSourceService() {
    return replicationSourceHandler;
  }

  /**
   * @return Return the object that implements the replication
   * sink service.
   */
  ReplicationSinkService getReplicationSinkService() {
    return replicationSinkHandler;
  }

  /**
   * Get the current master from ZooKeeper and open the RPC connection to it.
   *
   * Method will block until a master is available. You can break from this
   * block by requesting the server stop.
   *
   * @return master + port, or null if server has been stopped
   */
  private synchronized ServerName createRegionServerStatusStub() {
    if (rssStub != null) {
      return masterAddressTracker.getMasterAddress();
    }
    ServerName sn = null;
    long previousLogTime = 0;
    boolean refresh = false; // for the first time, use cached data
    RegionServerStatusService.BlockingInterface intf = null;
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
          if (sleep(200)) {
            interrupted = true;
          }
          continue;
        }

        // If we are on the active master, use the shortcut
        if (this instanceof HMaster && sn.equals(getServerName())) {
          intf = ((HMaster)this).getMasterRpcServices();
          break;
        }
        try {
          BlockingRpcChannel channel =
            this.rpcClient.createBlockingRpcChannel(sn, userProvider.getCurrent(), operationTimeout);
          intf = RegionServerStatusService.newBlockingStub(channel);
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
          if (sleep(200)) {
            interrupted = true;
          }
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    rssStub = intf;
    return sn;
  }

  /**
   * @return True if we should break loop because cluster is going down or
   * this server has been stopped or hdfs has gone bad.
   */
  private boolean keepLooping() {
    return !this.stopped && isClusterUp();
  }

  /*
   * Let the master know we're here Run initialization using parameters passed
   * us by the master.
   * @return A Map of key/value configurations we got from the Master else
   * null if we failed to register.
   * @throws IOException
   */
  private RegionServerStartupResponse reportForDuty() throws IOException {
    ServerName masterServerName = createRegionServerStatusStub();
    if (masterServerName == null) return null;
    RegionServerStartupResponse result = null;
    try {
      rpcServices.requestCount.set(0);
      LOG.info("reportForDuty to master=" + masterServerName + " with port="
        + rpcServices.isa.getPort() + ", startcode=" + this.startcode);
      long now = EnvironmentEdgeManager.currentTime();
      int port = rpcServices.isa.getPort();
      RegionServerStartupRequest.Builder request = RegionServerStartupRequest.newBuilder();
      request.setPort(port);
      request.setServerStartCode(this.startcode);
      request.setServerCurrentTime(now);
      result = this.rssStub.regionServerStartup(null, request.build());
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof ClockOutOfSyncException) {
        LOG.fatal("Master rejected startup because clock is out of sync", ioe);
        // Re-throw IOE will cause RS to abort
        throw ioe;
      } else if (ioe instanceof ServerNotRunningYetException) {
        LOG.debug("Master is not running yet");
      } else {
        LOG.warn("error telling master we are up", se);
      }
    }
    return result;
  }

  @Override
  public long getLastSequenceId(byte[] region) {
    Long lastFlushedSequenceId = -1l;
    try {
      GetLastFlushedSequenceIdRequest req = RequestConverter
          .buildGetLastFlushedSequenceIdRequest(region);
      RegionServerStatusService.BlockingInterface rss = rssStub;
      if (rss == null) { // Try to connect one more time
        createRegionServerStatusStub();
        rss = rssStub;
        if (rss == null) {
          // Still no luck, we tried
          LOG.warn("Unable to connect to the master to check "
            + "the last flushed sequence id");
          return -1l;
        }
      }
      lastFlushedSequenceId = rss.getLastFlushedSequenceId(null, req)
          .getLastFlushedSequenceId();
    } catch (ServiceException e) {
      lastFlushedSequenceId = -1l;
      LOG.warn("Unable to connect to the master to check "
        + "the last flushed sequence id", e);
    }
    return lastFlushedSequenceId;
  }

  /**
   * Closes all regions.  Called on our way out.
   * Assumes that its not possible for new regions to be added to onlineRegions
   * while this method runs.
   */
  protected void closeAllRegions(final boolean abort) {
    closeUserRegions(abort);
    closeMetaTableRegions(abort);
  }

  /**
   * Close meta region if we carry it
   * @param abort Whether we're running an abort.
   */
  void closeMetaTableRegions(final boolean abort) {
    HRegion meta = null;
    this.lock.writeLock().lock();
    try {
      for (Map.Entry<String, HRegion> e: onlineRegions.entrySet()) {
        HRegionInfo hri = e.getValue().getRegionInfo();
        if (hri.isMetaRegion()) {
          meta = e.getValue();
        }
        if (meta != null) break;
      }
    } finally {
      this.lock.writeLock().unlock();
    }
    if (meta != null) closeRegionIgnoreErrors(meta.getRegionInfo(), abort);
  }

  /**
   * Schedule closes on all user regions.
   * Should be safe calling multiple times because it wont' close regions
   * that are already closed or that are closing.
   * @param abort Whether we're running an abort.
   */
  void closeUserRegions(final boolean abort) {
    this.lock.writeLock().lock();
    try {
      for (Map.Entry<String, HRegion> e: this.onlineRegions.entrySet()) {
        HRegion r = e.getValue();
        if (!r.getRegionInfo().isMetaTable() && r.isAvailable()) {
          // Don't update zk with this close transition; pass false.
          closeRegionIgnoreErrors(r.getRegionInfo(), abort);
        }
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  /** @return the info server */
  public InfoServer getInfoServer() {
    return infoServer;
  }

  /**
   * @return true if a stop has been requested.
   */
  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  @Override
  public boolean isStopping() {
    return this.stopping;
  }

  @Override
  public Map<String, HRegion> getRecoveringRegions() {
    return this.recoveringRegions;
  }

  /**
   *
   * @return the configuration
   */
  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  /** @return the write lock for the server */
  ReentrantReadWriteLock.WriteLock getWriteLock() {
    return lock.writeLock();
  }

  public int getNumberOfOnlineRegions() {
    return this.onlineRegions.size();
  }

  boolean isOnlineRegionsEmpty() {
    return this.onlineRegions.isEmpty();
  }

  /**
   * For tests, web ui and metrics.
   * This method will only work if HRegionServer is in the same JVM as client;
   * HRegion cannot be serialized to cross an rpc.
   */
  public Collection<HRegion> getOnlineRegionsLocalContext() {
    Collection<HRegion> regions = this.onlineRegions.values();
    return Collections.unmodifiableCollection(regions);
  }

  @Override
  public void addToOnlineRegions(HRegion region) {
    this.onlineRegions.put(region.getRegionInfo().getEncodedName(), region);
    configurationManager.registerObserver(region);
  }

  /**
   * @return A new Map of online regions sorted by region size with the first entry being the
   * biggest.  If two regions are the same size, then the last one found wins; i.e. this method
   * may NOT return all regions.
   */
  SortedMap<Long, HRegion> getCopyOfOnlineRegionsSortedBySize() {
    // we'll sort the regions in reverse
    SortedMap<Long, HRegion> sortedRegions = new TreeMap<Long, HRegion>(
        new Comparator<Long>() {
          @Override
          public int compare(Long a, Long b) {
            return -1 * a.compareTo(b);
          }
        });
    // Copy over all regions. Regions are sorted by size with biggest first.
    for (HRegion region : this.onlineRegions.values()) {
      sortedRegions.put(region.memstoreSize.get(), region);
    }
    return sortedRegions;
  }

  /**
   * @return time stamp in millis of when this region server was started
   */
  public long getStartcode() {
    return this.startcode;
  }

  /** @return reference to FlushRequester */
  @Override
  public FlushRequester getFlushRequester() {
    return this.cacheFlusher;
  }

  /**
   * Get the top N most loaded regions this server is serving so we can tell the
   * master which regions it can reallocate if we're overloaded. TODO: actually
   * calculate which regions are most loaded. (Right now, we're just grabbing
   * the first N regions being served regardless of load.)
   */
  protected HRegionInfo[] getMostLoadedRegions() {
    ArrayList<HRegionInfo> regions = new ArrayList<HRegionInfo>();
    for (HRegion r : onlineRegions.values()) {
      if (!r.isAvailable()) {
        continue;
      }
      if (regions.size() < numRegionsToReport) {
        regions.add(r.getRegionInfo());
      } else {
        break;
      }
    }
    return regions.toArray(new HRegionInfo[regions.size()]);
  }

  @Override
  public Leases getLeases() {
    return leases;
  }

  /**
   * @return Return the rootDir.
   */
  protected Path getRootDir() {
    return rootDir;
  }

  /**
   * @return Return the fs.
   */
  @Override
  public FileSystem getFileSystem() {
    return fs;
  }

  @Override
  public String toString() {
    return getServerName().toString();
  }

  /**
   * Interval at which threads should run
   *
   * @return the interval
   */
  public int getThreadWakeFrequency() {
    return threadWakeFrequency;
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return zooKeeper;
  }

  @Override
  public BaseCoordinatedStateManager getCoordinatedStateManager() {
    return csm;
  }

  @Override
  public ServerName getServerName() {
    return serverName;
  }

  @Override
  public CompactionRequestor getCompactionRequester() {
    return this.compactSplitThread;
  }

  public RegionServerCoprocessorHost getRegionServerCoprocessorHost(){
    return this.rsHost;
  }

  @Override
  public ConcurrentMap<byte[], Boolean> getRegionsInTransitionInRS() {
    return this.regionsInTransitionInRS;
  }

  @Override
  public ExecutorService getExecutorService() {
    return service;
  }

  @Override
  public RegionServerQuotaManager getRegionServerQuotaManager() {
    return rsQuotaManager;
  }

  //
  // Main program and support routines
  //

  /**
   * Load the replication service objects, if any
   */
  static private void createNewReplicationInstance(Configuration conf,
    HRegionServer server, FileSystem fs, Path logDir, Path oldLogDir) throws IOException{

    // If replication is not enabled, then return immediately.
    if (!conf.getBoolean(HConstants.REPLICATION_ENABLE_KEY,
        HConstants.REPLICATION_ENABLE_DEFAULT)) {
      return;
    }

    // read in the name of the source replication class from the config file.
    String sourceClassname = conf.get(HConstants.REPLICATION_SOURCE_SERVICE_CLASSNAME,
                               HConstants.REPLICATION_SERVICE_CLASSNAME_DEFAULT);

    // read in the name of the sink replication class from the config file.
    String sinkClassname = conf.get(HConstants.REPLICATION_SINK_SERVICE_CLASSNAME,
                             HConstants.REPLICATION_SERVICE_CLASSNAME_DEFAULT);

    // If both the sink and the source class names are the same, then instantiate
    // only one object.
    if (sourceClassname.equals(sinkClassname)) {
      server.replicationSourceHandler = (ReplicationSourceService)
                                         newReplicationInstance(sourceClassname,
                                         conf, server, fs, logDir, oldLogDir);
      server.replicationSinkHandler = (ReplicationSinkService)
                                         server.replicationSourceHandler;
    } else {
      server.replicationSourceHandler = (ReplicationSourceService)
                                         newReplicationInstance(sourceClassname,
                                         conf, server, fs, logDir, oldLogDir);
      server.replicationSinkHandler = (ReplicationSinkService)
                                         newReplicationInstance(sinkClassname,
                                         conf, server, fs, logDir, oldLogDir);
    }
  }

  static private ReplicationService newReplicationInstance(String classname,
    Configuration conf, HRegionServer server, FileSystem fs, Path logDir,
    Path oldLogDir) throws IOException{

    Class<?> clazz = null;
    try {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      clazz = Class.forName(classname, true, classLoader);
    } catch (java.lang.ClassNotFoundException nfe) {
      throw new IOException("Could not find class for " + classname);
    }

    // create an instance of the replication object.
    ReplicationService service = (ReplicationService)
                              ReflectionUtils.newInstance(clazz, conf);
    service.initialize(server, fs, logDir, oldLogDir);
    return service;
  }

  /**
   * Utility for constructing an instance of the passed HRegionServer class.
   *
   * @param regionServerClass
   * @param conf2
   * @return HRegionServer instance.
   */
  public static HRegionServer constructRegionServer(
      Class<? extends HRegionServer> regionServerClass,
      final Configuration conf2, CoordinatedStateManager cp) {
    try {
      Constructor<? extends HRegionServer> c = regionServerClass
          .getConstructor(Configuration.class, CoordinatedStateManager.class);
      return c.newInstance(conf2, cp);
    } catch (Exception e) {
      throw new RuntimeException("Failed construction of " + "Regionserver: "
          + regionServerClass.toString(), e);
    }
  }

  /**
   * @see org.apache.hadoop.hbase.regionserver.HRegionServerCommandLine
   */
  public static void main(String[] args) throws Exception {
	VersionInfo.logVersion();
    Configuration conf = HBaseConfiguration.create();
    @SuppressWarnings("unchecked")
    Class<? extends HRegionServer> regionServerClass = (Class<? extends HRegionServer>) conf
        .getClass(HConstants.REGION_SERVER_IMPL, HRegionServer.class);

    new HRegionServerCommandLine(regionServerClass).doMain(args);
  }

  /**
   * Gets the online regions of the specified table.
   * This method looks at the in-memory onlineRegions.  It does not go to <code>hbase:meta</code>.
   * Only returns <em>online</em> regions.  If a region on this table has been
   * closed during a disable, etc., it will not be included in the returned list.
   * So, the returned list may not necessarily be ALL regions in this table, its
   * all the ONLINE regions in the table.
   * @param tableName
   * @return Online regions from <code>tableName</code>
   */
  @Override
  public List<HRegion> getOnlineRegions(TableName tableName) {
     List<HRegion> tableRegions = new ArrayList<HRegion>();
     synchronized (this.onlineRegions) {
       for (HRegion region: this.onlineRegions.values()) {
         HRegionInfo regionInfo = region.getRegionInfo();
         if(regionInfo.getTable().equals(tableName)) {
           tableRegions.add(region);
         }
       }
     }
     return tableRegions;
   }

  /**
   * Gets the online tables in this RS.
   * This method looks at the in-memory onlineRegions.
   * @return all the online tables in this RS
   */
  @Override
  public Set<TableName> getOnlineTables() {
    Set<TableName> tables = new HashSet<TableName>();
    synchronized (this.onlineRegions) {
      for (HRegion region: this.onlineRegions.values()) {
        tables.add(region.getTableDesc().getTableName());
      }
    }
    return tables;
  }

  // used by org/apache/hbase/tmpl/regionserver/RSStatusTmpl.jamon (HBASE-4070).
  public String[] getRegionServerCoprocessors() {
    TreeSet<String> coprocessors = new TreeSet<String>(
        this.hlog.getCoprocessorHost().getCoprocessors());
    Collection<HRegion> regions = getOnlineRegionsLocalContext();
    for (HRegion region: regions) {
      coprocessors.addAll(region.getCoprocessorHost().getCoprocessors());
    }
    return coprocessors.toArray(new String[coprocessors.size()]);
  }

  /**
   * Try to close the region, logs a warning on failure but continues.
   * @param region Region to close
   */
  private void closeRegionIgnoreErrors(HRegionInfo region, final boolean abort) {
    try {
      if (!closeRegion(region.getEncodedName(), abort, null)) {
        LOG.warn("Failed to close " + region.getRegionNameAsString() +
            " - ignoring and continuing");
      }
    } catch (IOException e) {
      LOG.warn("Failed to close " + region.getRegionNameAsString() +
          " - ignoring and continuing", e);
    }
  }

  /**
   * Close asynchronously a region, can be called from the master or internally by the regionserver
   * when stopping. If called from the master, the region will update the znode status.
   *
   * <p>
   * If an opening was in progress, this method will cancel it, but will not start a new close. The
   * coprocessors are not called in this case. A NotServingRegionException exception is thrown.
   * </p>

   * <p>
   *   If a close was in progress, this new request will be ignored, and an exception thrown.
   * </p>
   *
   * @param encodedName Region to close
   * @param abort True if we are aborting
   * @return True if closed a region.
   * @throws NotServingRegionException if the region is not online
   */
  protected boolean closeRegion(String encodedName, final boolean abort, final ServerName sn)
      throws NotServingRegionException {
    //Check for permissions to close.
    HRegion actualRegion = this.getFromOnlineRegions(encodedName);
    if ((actualRegion != null) && (actualRegion.getCoprocessorHost() != null)) {
      try {
        actualRegion.getCoprocessorHost().preClose(false);
      } catch (IOException exp) {
        LOG.warn("Unable to close region: the coprocessor launched an error ", exp);
        return false;
      }
    }

    final Boolean previous = this.regionsInTransitionInRS.putIfAbsent(encodedName.getBytes(),
        Boolean.FALSE);

    if (Boolean.TRUE.equals(previous)) {
      LOG.info("Received CLOSE for the region:" + encodedName + " , which we are already " +
          "trying to OPEN. Cancelling OPENING.");
      if (!regionsInTransitionInRS.replace(encodedName.getBytes(), previous, Boolean.FALSE)){
        // The replace failed. That should be an exceptional case, but theoretically it can happen.
        // We're going to try to do a standard close then.
        LOG.warn("The opening for region " + encodedName + " was done before we could cancel it." +
            " Doing a standard close now");
        return closeRegion(encodedName, abort, sn);
      }
      // Let's get the region from the online region list again
      actualRegion = this.getFromOnlineRegions(encodedName);
      if (actualRegion == null) { // If already online, we still need to close it.
        LOG.info("The opening previously in progress has been cancelled by a CLOSE request.");
        // The master deletes the znode when it receives this exception.
        throw new NotServingRegionException("The region " + encodedName +
          " was opening but not yet served. Opening is cancelled.");
      }
    } else if (Boolean.FALSE.equals(previous)) {
      LOG.info("Received CLOSE for the region: " + encodedName +
        ", which we are already trying to CLOSE, but not completed yet");
      return true;
    }

    if (actualRegion == null) {
      LOG.debug("Received CLOSE for a region which is not online, and we're not opening.");
      this.regionsInTransitionInRS.remove(encodedName.getBytes());
      // The master deletes the znode when it receives this exception.
      throw new NotServingRegionException("The region " + encodedName +
          " is not online, and is not opening.");
    }

    CloseRegionHandler crh;
    final HRegionInfo hri = actualRegion.getRegionInfo();
    if (hri.isMetaRegion()) {
      crh = new CloseMetaHandler(this, this, hri, abort);
    } else {
      crh = new CloseRegionHandler(this, this, hri, abort, sn);
    }
    this.service.submit(crh);
    return true;
  }

   /**
   * @param regionName
   * @return HRegion for the passed binary <code>regionName</code> or null if
   *         named region is not member of the online regions.
   */
  public HRegion getOnlineRegion(final byte[] regionName) {
    String encodedRegionName = HRegionInfo.encodeRegionName(regionName);
    return this.onlineRegions.get(encodedRegionName);
  }

  public InetSocketAddress[] getRegionBlockLocations(final String encodedRegionName) {
    return this.regionFavoredNodesMap.get(encodedRegionName);
  }

  @Override
  public HRegion getFromOnlineRegions(final String encodedRegionName) {
    return this.onlineRegions.get(encodedRegionName);
  }


  @Override
  public boolean removeFromOnlineRegions(final HRegion r, ServerName destination) {
    HRegion toReturn = this.onlineRegions.remove(r.getRegionInfo().getEncodedName());

    if (destination != null) {
      HLog wal = getWAL();
      long closeSeqNum = wal.getEarliestMemstoreSeqNum(r.getRegionInfo().getEncodedNameAsBytes());
      if (closeSeqNum == HConstants.NO_SEQNUM) {
        // No edits in WAL for this region; get the sequence number when the region was opened.
        closeSeqNum = r.getOpenSeqNum();
        if (closeSeqNum == HConstants.NO_SEQNUM) {
          closeSeqNum = 0;
        }
      }
      addToMovedRegions(r.getRegionInfo().getEncodedName(), destination, closeSeqNum);
    }
    this.regionFavoredNodesMap.remove(r.getRegionInfo().getEncodedName());
    return toReturn != null;
  }

  /**
   * Protected utility method for safely obtaining an HRegion handle.
   *
   * @param regionName
   *          Name of online {@link HRegion} to return
   * @return {@link HRegion} for <code>regionName</code>
   * @throws NotServingRegionException
   */
  protected HRegion getRegion(final byte[] regionName)
      throws NotServingRegionException {
    String encodedRegionName = HRegionInfo.encodeRegionName(regionName);
    return getRegionByEncodedName(regionName, encodedRegionName);
  }

  public HRegion getRegionByEncodedName(String encodedRegionName)
      throws NotServingRegionException {
    return getRegionByEncodedName(null, encodedRegionName);
  }

  protected HRegion getRegionByEncodedName(byte[] regionName, String encodedRegionName)
    throws NotServingRegionException {
    HRegion region = this.onlineRegions.get(encodedRegionName);
    if (region == null) {
      MovedRegionInfo moveInfo = getMovedRegion(encodedRegionName);
      if (moveInfo != null) {
        throw new RegionMovedException(moveInfo.getServerName(), moveInfo.getSeqNum());
      }
      Boolean isOpening = this.regionsInTransitionInRS.get(Bytes.toBytes(encodedRegionName));
      String regionNameStr = regionName == null?
        encodedRegionName: Bytes.toStringBinary(regionName);
      if (isOpening != null && isOpening.booleanValue()) {
        throw new RegionOpeningException("Region " + regionNameStr +
          " is opening on " + this.serverName);
      }
      throw new NotServingRegionException("Region " + regionNameStr +
        " is not online on " + this.serverName);
    }
    return region;
  }

  /*
   * Cleanup after Throwable caught invoking method. Converts <code>t</code> to
   * IOE if it isn't already.
   *
   * @param t Throwable
   *
   * @param msg Message to log in error. Can be null.
   *
   * @return Throwable converted to an IOE; methods can only let out IOEs.
   */
  private Throwable cleanup(final Throwable t, final String msg) {
    // Don't log as error if NSRE; NSRE is 'normal' operation.
    if (t instanceof NotServingRegionException) {
      LOG.debug("NotServingRegionException; " + t.getMessage());
      return t;
    }
    Throwable e = t instanceof RemoteException ? ((RemoteException) t).unwrapRemoteException() : t;
    if (msg == null) {
      LOG.error("", e);
    } else {
      LOG.error(msg, e);
    }
    if (!rpcServices.checkOOME(t)) {
      checkFileSystem();
    }
    return t;
  }

  /*
   * @param t
   *
   * @param msg Message to put in new IOE if passed <code>t</code> is not an IOE
   *
   * @return Make <code>t</code> an IOE if it isn't already.
   */
  protected IOException convertThrowableToIOE(final Throwable t, final String msg) {
    return (t instanceof IOException ? (IOException) t : msg == null
        || msg.length() == 0 ? new IOException(t) : new IOException(msg, t));
  }

  /**
   * Checks to see if the file system is still accessible. If not, sets
   * abortRequested and stopRequested
   *
   * @return false if file system is not available
   */
  public boolean checkFileSystem() {
    if (this.fsOk && this.fs != null) {
      try {
        FSUtils.checkFileSystemAvailable(this.fs);
      } catch (IOException e) {
        abort("File System not available", e);
        this.fsOk = false;
      }
    }
    return this.fsOk;
  }

  @Override
  public void updateRegionFavoredNodesMapping(String encodedRegionName,
      List<org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ServerName> favoredNodes) {
    InetSocketAddress[] addr = new InetSocketAddress[favoredNodes.size()];
    // Refer to the comment on the declaration of regionFavoredNodesMap on why
    // it is a map of region name to InetSocketAddress[]
    for (int i = 0; i < favoredNodes.size(); i++) {
      addr[i] = InetSocketAddress.createUnresolved(favoredNodes.get(i).getHostName(),
          favoredNodes.get(i).getPort());
    }
    regionFavoredNodesMap.put(encodedRegionName, addr);
  }

  /**
   * Return the favored nodes for a region given its encoded name. Look at the
   * comment around {@link #regionFavoredNodesMap} on why it is InetSocketAddress[]
   * @param encodedRegionName
   * @return array of favored locations
   */
  @Override
  public InetSocketAddress[] getFavoredNodesForRegion(String encodedRegionName) {
    return regionFavoredNodesMap.get(encodedRegionName);
  }

  @Override
  public ServerNonceManager getNonceManager() {
    return this.nonceManager;
  }

  private static class MovedRegionInfo {
    private final ServerName serverName;
    private final long seqNum;
    private final long ts;

    public MovedRegionInfo(ServerName serverName, long closeSeqNum) {
      this.serverName = serverName;
      this.seqNum = closeSeqNum;
      ts = EnvironmentEdgeManager.currentTime();
     }

    public ServerName getServerName() {
      return serverName;
    }

    public long getSeqNum() {
      return seqNum;
    }

    public long getMoveTime() {
      return ts;
    }
  }

  // This map will contains all the regions that we closed for a move.
  //  We add the time it was moved as we don't want to keep too old information
  protected Map<String, MovedRegionInfo> movedRegions =
      new ConcurrentHashMap<String, MovedRegionInfo>(3000);

  // We need a timeout. If not there is a risk of giving a wrong information: this would double
  //  the number of network calls instead of reducing them.
  private static final int TIMEOUT_REGION_MOVED = (2 * 60 * 1000);

  protected void addToMovedRegions(String encodedName, ServerName destination, long closeSeqNum) {
    if (ServerName.isSameHostnameAndPort(destination, this.getServerName())) {
      LOG.warn("Not adding moved region record: " + encodedName + " to self.");
      return;
    }
    LOG.info("Adding moved region record: "
      + encodedName + " to " + destination + " as of " + closeSeqNum);
    movedRegions.put(encodedName, new MovedRegionInfo(destination, closeSeqNum));
  }

  void removeFromMovedRegions(String encodedName) {
    movedRegions.remove(encodedName);
  }

  private MovedRegionInfo getMovedRegion(final String encodedRegionName) {
    MovedRegionInfo dest = movedRegions.get(encodedRegionName);

    long now = EnvironmentEdgeManager.currentTime();
    if (dest != null) {
      if (dest.getMoveTime() > (now - TIMEOUT_REGION_MOVED)) {
        return dest;
      } else {
        movedRegions.remove(encodedRegionName);
      }
    }

    return null;
  }

  /**
   * Remove the expired entries from the moved regions list.
   */
  protected void cleanMovedRegions() {
    final long cutOff = System.currentTimeMillis() - TIMEOUT_REGION_MOVED;
    Iterator<Entry<String, MovedRegionInfo>> it = movedRegions.entrySet().iterator();

    while (it.hasNext()){
      Map.Entry<String, MovedRegionInfo> e = it.next();
      if (e.getValue().getMoveTime() < cutOff) {
        it.remove();
      }
    }
  }

  /**
   * Creates a Chore thread to clean the moved region cache.
   */
  protected static class MovedRegionsCleaner extends Chore implements Stoppable {
    private HRegionServer regionServer;
    Stoppable stoppable;

    private MovedRegionsCleaner(
      HRegionServer regionServer, Stoppable stoppable){
      super("MovedRegionsCleaner for region "+regionServer, TIMEOUT_REGION_MOVED, stoppable);
      this.regionServer = regionServer;
      this.stoppable = stoppable;
    }

    static MovedRegionsCleaner createAndStart(HRegionServer rs){
      Stoppable stoppable = new Stoppable() {
        private volatile boolean isStopped = false;
        @Override public void stop(String why) { isStopped = true;}
        @Override public boolean isStopped() {return isStopped;}
      };

      return new MovedRegionsCleaner(rs, stoppable);
    }

    @Override
    protected void chore() {
      regionServer.cleanMovedRegions();
    }

    @Override
    public void stop(String why) {
      stoppable.stop(why);
    }

    @Override
    public boolean isStopped() {
      return stoppable.isStopped();
    }
  }

  private String getMyEphemeralNodePath() {
    return ZKUtil.joinZNode(this.zooKeeper.rsZNode, getServerName().toString());
  }

  private boolean isHealthCheckerConfigured() {
    String healthScriptLocation = this.conf.get(HConstants.HEALTH_SCRIPT_LOC);
    return org.apache.commons.lang.StringUtils.isNotBlank(healthScriptLocation);
  }

  /**
   * @return the underlying {@link CompactSplitThread} for the servers
   */
  public CompactSplitThread getCompactSplitThread() {
    return this.compactSplitThread;
  }

  /**
   * A helper function to store the last flushed sequence Id with the previous failed RS for a
   * recovering region. The Id is used to skip wal edits which are flushed. Since the flushed
   * sequence id is only valid for each RS, we associate the Id with corresponding failed RS.
   * @throws KeeperException
   * @throws IOException
   */
  private void updateRecoveringRegionLastFlushedSequenceId(HRegion r) throws KeeperException,
      IOException {
    if (!r.isRecovering()) {
      // return immdiately for non-recovering regions
      return;
    }

    HRegionInfo region = r.getRegionInfo();
    ZooKeeperWatcher zkw = getZooKeeper();
    String previousRSName = this.getLastFailedRSFromZK(region.getEncodedName());
    Map<byte[], Long> maxSeqIdInStores = r.getMaxStoreSeqIdForLogReplay();
    long minSeqIdForLogReplay = -1;
    for (Long storeSeqIdForReplay : maxSeqIdInStores.values()) {
      if (minSeqIdForLogReplay == -1 || storeSeqIdForReplay < minSeqIdForLogReplay) {
        minSeqIdForLogReplay = storeSeqIdForReplay;
      }
    }

    try {
      long lastRecordedFlushedSequenceId = -1;
      String nodePath = ZKUtil.joinZNode(this.zooKeeper.recoveringRegionsZNode,
        region.getEncodedName());
      // recovering-region level
      byte[] data;
      try {
        data = ZKUtil.getData(zkw, nodePath);
      } catch (InterruptedException e) {
        throw new InterruptedIOException();
      }
      if (data != null) {
      lastRecordedFlushedSequenceId = ZKSplitLog.parseLastFlushedSequenceIdFrom(data);
      }
      if (data == null || lastRecordedFlushedSequenceId < minSeqIdForLogReplay) {
        ZKUtil.setData(zkw, nodePath, ZKUtil.positionToByteArray(minSeqIdForLogReplay));
      }
      if (previousRSName != null) {
        // one level deeper for the failed RS
        nodePath = ZKUtil.joinZNode(nodePath, previousRSName);
        ZKUtil.setData(zkw, nodePath,
          ZKUtil.regionSequenceIdsToByteArray(minSeqIdForLogReplay, maxSeqIdInStores));
        LOG.debug("Update last flushed sequence id of region " + region.getEncodedName() + " for "
            + previousRSName);
      } else {
        LOG.warn("Can't find failed region server for recovering region " +
          region.getEncodedName());
      }
    } catch (NoNodeException ignore) {
      LOG.debug("Region " + region.getEncodedName() +
        " must have completed recovery because its recovery znode has been removed", ignore);
    }
  }

  /**
   * Return the last failed RS name under /hbase/recovering-regions/encodedRegionName
   * @param encodedRegionName
   * @throws KeeperException
   */
  private String getLastFailedRSFromZK(String encodedRegionName) throws KeeperException {
    String result = null;
    long maxZxid = 0;
    ZooKeeperWatcher zkw = this.getZooKeeper();
    String nodePath = ZKUtil.joinZNode(zkw.recoveringRegionsZNode, encodedRegionName);
    List<String> failedServers = ZKUtil.listChildrenNoWatch(zkw, nodePath);
    if (failedServers == null || failedServers.isEmpty()) {
      return result;
    }
    for (String failedServer : failedServers) {
      String rsPath = ZKUtil.joinZNode(nodePath, failedServer);
      Stat stat = new Stat();
      ZKUtil.getDataNoWatch(zkw, rsPath, stat);
      if (maxZxid < stat.getCzxid()) {
        maxZxid = stat.getCzxid();
        result = failedServer;
      }
    }
    return result;
  }
  
  public CoprocessorServiceResponse execRegionServerService(final RpcController controller,
      final CoprocessorServiceRequest serviceRequest) throws ServiceException {
    try {
      ServerRpcController execController = new ServerRpcController();
      CoprocessorServiceCall call = serviceRequest.getCall();
      String serviceName = call.getServiceName();
      String methodName = call.getMethodName();
      if (!coprocessorServiceHandlers.containsKey(serviceName)) {
        throw new UnknownProtocolException(null,
            "No registered coprocessor service found for name " + serviceName);
      }
      Service service = coprocessorServiceHandlers.get(serviceName);
      Descriptors.ServiceDescriptor serviceDesc = service.getDescriptorForType();
      Descriptors.MethodDescriptor methodDesc = serviceDesc.findMethodByName(methodName);
      if (methodDesc == null) {
        throw new UnknownProtocolException(service.getClass(), "Unknown method " + methodName
            + " called on service " + serviceName);
      }
      Message request =
          service.getRequestPrototype(methodDesc).newBuilderForType().mergeFrom(call.getRequest())
              .build();
      final Message.Builder responseBuilder =
          service.getResponsePrototype(methodDesc).newBuilderForType();
      service.callMethod(methodDesc, controller, request, new RpcCallback<Message>() {
        @Override
        public void run(Message message) {
          if (message != null) {
            responseBuilder.mergeFrom(message);
          }
        }
      });
      Message execResult = responseBuilder.build();
      if (execController.getFailedOn() != null) {
        throw execController.getFailedOn();
      }
      ClientProtos.CoprocessorServiceResponse.Builder builder =
          ClientProtos.CoprocessorServiceResponse.newBuilder();
      builder.setRegion(RequestConverter.buildRegionSpecifier(RegionSpecifierType.REGION_NAME,
        HConstants.EMPTY_BYTE_ARRAY));
      builder.setValue(builder.getValueBuilder().setName(execResult.getClass().getName())
          .setValue(execResult.toByteString()));
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }
  
  /**
   * @return The cache config instance used by the regionserver.
   */
  public CacheConfig getCacheConfig() {
    return this.cacheConfig;
  }

  /**
   * @return : Returns the ConfigurationManager object for testing purposes.
   */
  protected ConfigurationManager getConfigurationManager() {
    return configurationManager;
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
}
