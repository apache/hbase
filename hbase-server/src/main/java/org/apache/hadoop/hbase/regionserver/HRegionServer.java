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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Constructor;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import javax.management.MalformedObjectNameException;
import javax.servlet.http.HttpServlet;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ClockOutOfSyncException;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HealthCheckChore;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.ZNodeClearer;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.locking.EntityLock;
import org.apache.hadoop.hbase.client.locking.LockServiceClient;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.coordination.SplitLogWorkerCoordination;
import org.apache.hadoop.hbase.coordination.ZkCoordinatedStateManager;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.exceptions.RegionOpeningException;
import org.apache.hadoop.hbase.exceptions.UnknownProtocolException;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.http.InfoServer;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.NettyRpcClientConfigHelper;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.mob.MobCacheConfig;
import org.apache.hadoop.hbase.procedure.RegionServerProcedureManagerHost;
import org.apache.hadoop.hbase.procedure2.RSProcedureCallable;
import org.apache.hadoop.hbase.quotas.FileSystemUtilizationChore;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.quotas.RegionServerRpcQuotaManager;
import org.apache.hadoop.hbase.quotas.RegionServerSpaceQuotaManager;
import org.apache.hadoop.hbase.quotas.RegionSize;
import org.apache.hadoop.hbase.quotas.RegionSizeStore;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequester;
import org.apache.hadoop.hbase.regionserver.handler.CloseMetaHandler;
import org.apache.hadoop.hbase.regionserver.handler.CloseRegionHandler;
import org.apache.hadoop.hbase.regionserver.handler.RSProcedureHandler;
import org.apache.hadoop.hbase.regionserver.handler.RegionReplicaFlushHandler;
import org.apache.hadoop.hbase.regionserver.throttle.FlushThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationLoad;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationObserver;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.trace.SpanReceiverHost;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CompressionTest;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.JvmPauseMonitor;
import org.apache.hadoop.hbase.util.NettyEventLoopGroupConfig;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.NettyAsyncFSWALConfigHelper;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.hbase.zookeeper.ClusterStatusTracker;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.base.Throwables;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceCall;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionLoad;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.Coprocessor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.Coprocessor.Builder;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionServerInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos.LockService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStatusService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionSpaceUse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionSpaceUseReportRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportProcedureDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRSFatalErrorRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;

/**
 * HRegionServer makes a set of HRegions available to clients. It checks in with
 * the HMaster. There are many HRegionServers in a single HBase deployment.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@SuppressWarnings({ "deprecation"})
public class HRegionServer extends HasThread implements
    RegionServerServices, LastSequenceId, ConfigurationObserver {
  // Time to pause if master says 'please hold'. Make configurable if needed.
  private static final int INIT_PAUSE_TIME_MS = 1000;

  private static final Logger LOG = LoggerFactory.getLogger(HRegionServer.class);

  /**
   * For testing only!  Set to true to skip notifying region assignment to master .
   */
  @VisibleForTesting
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="MS_SHOULD_BE_FINAL")
  public static boolean TEST_SKIP_REPORTING_TRANSITION = false;

  //RegionName vs current action in progress
  //true - if open region action in progress
  //false - if close region action in progress
  protected final ConcurrentMap<byte[], Boolean> regionsInTransitionInRS =
    new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);

  // Cache flushing
  protected MemStoreFlusher cacheFlusher;

  protected HeapMemoryManager hMemManager;

  /**
   * Cluster connection to be shared by services.
   * Initialized at server startup and closed when server shuts down.
   * Clients must never close it explicitly.
   * Clients hosted by this Server should make use of this clusterConnection rather than create
   * their own; if they create their own, there is no way for the hosting server to shutdown
   * ongoing client RPCs.
   */
  protected ClusterConnection clusterConnection;

  /*
   * Long-living meta table locator, which is created when the server is started and stopped
   * when server shuts down. References to this locator shall be used to perform according
   * operations in EventHandlers. Primary reason for this decision is to make it mockable
   * for tests.
   */
  protected MetaTableLocator metaTableLocator;

  /**
   * Go here to get table descriptors.
   */
  protected TableDescriptors tableDescriptors;

  // Replication services. If no replication, this handler will be null.
  protected ReplicationSourceService replicationSourceHandler;
  protected ReplicationSinkService replicationSinkHandler;

  // Compactions
  public CompactSplit compactSplitThread;

  /**
   * Map of regions currently being served by this region server. Key is the
   * encoded region name.  All access should be synchronized.
   */
  protected final Map<String, HRegion> onlineRegions = new ConcurrentHashMap<>();

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
      new ConcurrentHashMap<>();

  // Leases
  protected Leases leases;

  // Instance of the hbase executor executorService.
  protected ExecutorService executorService;

  // If false, the file system has become unavailable
  protected volatile boolean fsOk;
  protected HFileSystem fs;
  protected HFileSystem walFs;

  // Set when a report to the master comes back with a message asking us to
  // shutdown. Also set by call to stop when debugging or running unit tests
  // of HRegionServer in isolation.
  private volatile boolean stopped = false;

  // Go down hard. Used if file system becomes unavailable and also in
  // debugging and unit tests.
  private volatile boolean abortRequested;

  ConcurrentMap<String, Integer> rowlocks = new ConcurrentHashMap<>();

  // A state before we go into stopped state.  At this stage we're closing user
  // space regions.
  private boolean stopping = false;

  volatile boolean killed = false;

  private volatile boolean shutDown = false;

  protected final Configuration conf;

  private Path rootDir;
  private Path walRootDir;

  protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  final int numRetries;
  protected final int threadWakeFrequency;
  protected final int msgInterval;

  protected final int numRegionsToReport;

  // Stub to do region server status calls against the master.
  private volatile RegionServerStatusService.BlockingInterface rssStub;
  private volatile LockService.BlockingInterface lockStub;
  // RPC client. Used to make the stub above that does region server status checking.
  RpcClient rpcClient;

  private RpcRetryingCallerFactory rpcRetryingCallerFactory;
  private RpcControllerFactory rpcControllerFactory;

  private UncaughtExceptionHandler uncaughtExceptionHandler;

  // Info server. Default access so can be used by unit tests. REGIONSERVER
  // is name of the webapp and the attribute name used stuffing this instance
  // into web context.
  protected InfoServer infoServer;
  private JvmPauseMonitor pauseMonitor;

  /** region server process name */
  public static final String REGIONSERVER = "regionserver";

  MetricsRegionServer metricsRegionServer;
  MetricsTable metricsTable;
  private SpanReceiverHost spanReceiverHost;

  /**
   * ChoreService used to schedule tasks that we want to run periodically
   */
  private ChoreService choreService;

  /*
   * Check for compactions requests.
   */
  ScheduledChore compactionChecker;

  /*
   * Check for flushes
   */
  ScheduledChore periodicFlusher;

  protected volatile WALFactory walFactory;

  // WAL roller. log is protected rather than private to avoid
  // eclipse warning when accessed by inner classes
  protected LogRoller walRoller;

  // A thread which calls reportProcedureDone
  private RemoteProcedureResultReporter procedureResultReporter;

  // flag set after we're done setting up server threads
  final AtomicBoolean online = new AtomicBoolean(false);

  // zookeeper connection and watcher
  protected final ZKWatcher zooKeeper;

  // master address tracker
  private final MasterAddressTracker masterAddressTracker;

  // Cluster Status Tracker
  protected final ClusterStatusTracker clusterStatusTracker;

  // Log Splitting Worker
  private SplitLogWorker splitLogWorker;

  // A sleeper that sleeps for msgInterval.
  protected final Sleeper sleeper;

  private final int operationTimeout;
  private final int shortOperationTimeout;

  private final RegionServerAccounting regionServerAccounting;

  // Cache configuration and block cache reference
  protected CacheConfig cacheConfig;
  // Cache configuration for mob
  final MobCacheConfig mobCacheConfig;

  /** The health check chore. */
  private HealthCheckChore healthCheckChore;

  /** The nonce manager chore. */
  private ScheduledChore nonceManagerChore;

  private Map<String, com.google.protobuf.Service> coprocessorServiceHandlers = Maps.newHashMap();

  /**
   * The server name the Master sees us as.  Its made from the hostname the
   * master passes us, port, and server startcode. Gets set after registration
   * against  Master.
   */
  protected ServerName serverName;

  /*
   * hostname specified by hostname config
   */
  protected String useThisHostnameInstead;

  // key to the config parameter of server hostname
  // the specification of server hostname is optional. The hostname should be resolvable from
  // both master and region server
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
  final static String RS_HOSTNAME_KEY = "hbase.regionserver.hostname";
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
  protected final static String MASTER_HOSTNAME_KEY = "hbase.master.hostname";

  // HBASE-18226: This config and hbase.regionserver.hostname are mutually exclusive.
  // Exception will be thrown if both are used.
  final static String RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY =
    "hbase.regionserver.hostname.disable.master.reversedns";

  /**
   * This servers startcode.
   */
  protected final long startcode;

  /**
   * Unique identifier for the cluster we are a part of.
   */
  protected String clusterId;

  /**
   * Chore to clean periodically the moved region list
   */
  private MovedRegionsCleaner movedRegionsCleaner;

  // chore for refreshing store files for secondary regions
  private StorefileRefresherChore storefileRefresher;

  private RegionServerCoprocessorHost rsHost;

  private RegionServerProcedureManagerHost rspmHost;

  private RegionServerRpcQuotaManager rsQuotaManager;
  private RegionServerSpaceQuotaManager rsSpaceQuotaManager;

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

  protected CoordinatedStateManager csm;

  /**
   * Configuration manager is used to register/deregister and notify the configuration observers
   * when the regionserver is notified that there was a change in the on disk configs.
   */
  protected final ConfigurationManager configurationManager;

  @VisibleForTesting
  CompactedHFilesDischarger compactedFileDischarger;

  private volatile ThroughputController flushThroughputController;

  protected SecureBulkLoadManager secureBulkLoadManager;

  protected FileSystemUtilizationChore fsUtilizationChore;

  private final NettyEventLoopGroupConfig eventLoopGroupConfig;

  /**
   * True if this RegionServer is coming up in a cluster where there is no Master;
   * means it needs to just come up and make do without a Master to talk to: e.g. in test or
   * HRegionServer is doing other than its usual duties: e.g. as an hollowed-out host whose only
   * purpose is as a Replication-stream sink; see HBASE-18846 for more.
   */
  private final boolean masterless;
  static final String MASTERLESS_CONFIG_NAME = "hbase.masterless";

  /**
   * Starts a HRegionServer at the default location
   */
  // Don't start any services or managers in here in the Constructor.
  // Defer till after we register with the Master as much as possible. See #startServices.
  public HRegionServer(Configuration conf) throws IOException {
    super("RegionServer");  // thread name
    TraceUtil.initTracer(conf);
    try {
      this.startcode = System.currentTimeMillis();
      this.conf = conf;
      this.fsOk = true;
      this.masterless = conf.getBoolean(MASTERLESS_CONFIG_NAME, false);
      this.eventLoopGroupConfig = setupNetty(this.conf);
      MemorySizeUtil.checkForClusterFreeHeapMemoryLimit(this.conf);
      HFile.checkHFileVersion(this.conf);
      checkCodecs(this.conf);
      this.userProvider = UserProvider.instantiate(conf);
      FSUtils.setupShortCircuitRead(this.conf);
      decorateRegionServerConfiguration(this.conf);

      // Disable usage of meta replicas in the regionserver
      this.conf.setBoolean(HConstants.USE_META_REPLICAS, false);
      // Config'ed params
      this.numRetries = this.conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
          HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
      this.threadWakeFrequency = conf.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
      this.msgInterval = conf.getInt("hbase.regionserver.msginterval", 3 * 1000);

      this.sleeper = new Sleeper(this.msgInterval, this);

      boolean isNoncesEnabled = conf.getBoolean(HConstants.HBASE_RS_NONCES_ENABLED, true);
      this.nonceManager = isNoncesEnabled ? new ServerNonceManager(this.conf) : null;

      this.numRegionsToReport = conf.getInt("hbase.regionserver.numregionstoreport", 10);

      this.operationTimeout = conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
          HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);

      this.shortOperationTimeout = conf.getInt(HConstants.HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY,
          HConstants.DEFAULT_HBASE_RPC_SHORTOPERATION_TIMEOUT);

      this.abortRequested = false;
      this.stopped = false;

      rpcServices = createRpcServices();
      useThisHostnameInstead = getUseThisHostnameInstead(conf);
      String hostName =
          StringUtils.isBlank(useThisHostnameInstead) ? this.rpcServices.isa.getHostName()
              : this.useThisHostnameInstead;
      serverName = ServerName.valueOf(hostName, this.rpcServices.isa.getPort(), this.startcode);

      rpcControllerFactory = RpcControllerFactory.instantiate(this.conf);
      rpcRetryingCallerFactory = RpcRetryingCallerFactory.instantiate(this.conf);

      // login the zookeeper client principal (if using security)
      ZKUtil.loginClient(this.conf, HConstants.ZK_CLIENT_KEYTAB_FILE,
          HConstants.ZK_CLIENT_KERBEROS_PRINCIPAL, hostName);
      // login the server principal (if using secure Hadoop)
      login(userProvider, hostName);
      // init superusers and add the server principal (if using security)
      // or process owner as default super user.
      Superusers.initialize(conf);

      regionServerAccounting = new RegionServerAccounting(conf);
      cacheConfig = new CacheConfig(conf);
      mobCacheConfig = new MobCacheConfig(conf);
      uncaughtExceptionHandler = new UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          abort("Uncaught exception in executorService thread " + t.getName(), e);
        }
      };

      initializeFileSystem();
      spanReceiverHost = SpanReceiverHost.getInstance(getConfiguration());

      this.configurationManager = new ConfigurationManager();
      setupWindows(getConfiguration(), getConfigurationManager());

      // Some unit tests don't need a cluster, so no zookeeper at all
      if (!conf.getBoolean("hbase.testing.nocluster", false)) {
        // Open connection to zookeeper and set primary watcher
        zooKeeper = new ZKWatcher(conf, getProcessName() + ":" +
          rpcServices.isa.getPort(), this, canCreateBaseZNode());
        // If no master in cluster, skip trying to track one or look for a cluster status.
        if (!this.masterless) {
          this.csm = new ZkCoordinatedStateManager(this);

          masterAddressTracker = new MasterAddressTracker(getZooKeeper(), this);
          masterAddressTracker.start();

          clusterStatusTracker = new ClusterStatusTracker(zooKeeper, this);
          clusterStatusTracker.start();
        } else {
          masterAddressTracker = null;
          clusterStatusTracker = null;
        }
      } else {
        zooKeeper = null;
        masterAddressTracker = null;
        clusterStatusTracker = null;
      }
      this.rpcServices.start(zooKeeper);
      // This violates 'no starting stuff in Constructor' but Master depends on the below chore
      // and executor being created and takes a different startup route. Lots of overlap between HRS
      // and M (An M IS A HRS now). Need to refactor so less duplication between M and its super
      // Master expects Constructor to put up web servers. Ugh.
      // class HRS. TODO.
      this.choreService = new ChoreService(getName(), true);
      this.executorService = new ExecutorService(getName());
      putUpWebUI();
    } catch (Throwable t) {
      // Make sure we log the exception. HRegionServer is often started via reflection and the
      // cause of failed startup is lost.
      LOG.error("Failed construction RegionServer", t);
      throw t;
    }
  }

  // HMaster should override this method to load the specific config for master
  protected String getUseThisHostnameInstead(Configuration conf) throws IOException {
    String hostname = conf.get(RS_HOSTNAME_KEY);
    if (conf.getBoolean(RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY, false)) {
      if (!StringUtils.isBlank(hostname)) {
        String msg = RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY + " and " + RS_HOSTNAME_KEY +
          " are mutually exclusive. Do not set " + RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY +
          " to true while " + RS_HOSTNAME_KEY + " is used";
        throw new IOException(msg);
      } else {
        return rpcServices.isa.getHostName();
      }
    } else {
      return hostname;
    }
  }

  /**
   * If running on Windows, do windows-specific setup.
   */
  private static void setupWindows(final Configuration conf, ConfigurationManager cm) {
    if (!SystemUtils.IS_OS_WINDOWS) {
      Signal.handle(new Signal("HUP"), new SignalHandler() {
        @Override
        public void handle(Signal signal) {
          conf.reloadConfiguration();
          cm.notifyAllObservers(conf);
        }
      });
    }
  }

  private static NettyEventLoopGroupConfig setupNetty(Configuration conf) {
    // Initialize netty event loop group at start as we may use it for rpc server, rpc client & WAL.
    NettyEventLoopGroupConfig nelgc =
      new NettyEventLoopGroupConfig(conf, "RS-EventLoopGroup");
    NettyRpcClientConfigHelper.setEventLoopConfig(conf, nelgc.group(), nelgc.clientChannelClass());
    NettyAsyncFSWALConfigHelper.setEventLoopConfig(conf, nelgc.group(), nelgc.clientChannelClass());
    return nelgc;
  }

  private void initializeFileSystem() throws IOException {
    // Get fs instance used by this RS.  Do we use checksum verification in the hbase? If hbase
    // checksum verification enabled, then automatically switch off hdfs checksum verification.
    boolean useHBaseChecksum = conf.getBoolean(HConstants.HBASE_CHECKSUM_VERIFICATION, true);
    FSUtils.setFsDefault(this.conf, FSUtils.getWALRootDir(this.conf));
    this.walFs = new HFileSystem(this.conf, useHBaseChecksum);
    this.walRootDir = FSUtils.getWALRootDir(this.conf);
    // Set 'fs.defaultFS' to match the filesystem on hbase.rootdir else
    // underlying hadoop hdfs accessors will be going against wrong filesystem
    // (unless all is set to defaults).
    FSUtils.setFsDefault(this.conf, FSUtils.getRootDir(this.conf));
    this.fs = new HFileSystem(this.conf, useHBaseChecksum);
    this.rootDir = FSUtils.getRootDir(this.conf);
    this.tableDescriptors = getFsTableDescriptors();
  }

  protected TableDescriptors getFsTableDescriptors() throws IOException {
    return new FSTableDescriptors(this.conf,
      this.fs, this.rootDir, !canUpdateTableDescriptor(), false, getMetaTableObserver());
  }

  protected Function<TableDescriptorBuilder, TableDescriptorBuilder> getMetaTableObserver() {
    return null;
  }

  protected void login(UserProvider user, String host) throws IOException {
    user.login("hbase.regionserver.keytab.file",
      "hbase.regionserver.kerberos.principal", host);
  }

  protected void waitForMasterActive() {
  }

  protected String getProcessName() {
    return REGIONSERVER;
  }

  protected boolean canCreateBaseZNode() {
    return this.masterless;
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

  @Override
  public boolean registerService(com.google.protobuf.Service instance) {
    /*
     * No stacking of instances is allowed for a single executorService name
     */
    com.google.protobuf.Descriptors.ServiceDescriptor serviceDesc =
        instance.getDescriptorForType();
    String serviceName = CoprocessorRpcUtils.getServiceName(serviceDesc);
    if (coprocessorServiceHandlers.containsKey(serviceName)) {
      LOG.error("Coprocessor executorService " + serviceName
          + " already registered, rejecting request from " + instance);
      return false;
    }

    coprocessorServiceHandlers.put(serviceName, instance);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Registered regionserver coprocessor executorService: executorService=" + serviceName);
    }
    return true;
  }

  /**
   * Create a 'smarter' Connection, one that is capable of by-passing RPC if the request is to
   * the local server; i.e. a short-circuit Connection. Safe to use going to local or remote
   * server. Create this instance in a method can be intercepted and mocked in tests.
   * @throws IOException
   */
  @VisibleForTesting
  protected ClusterConnection createClusterConnection() throws IOException {
    Configuration conf = this.conf;
    if (conf.get(HConstants.CLIENT_ZOOKEEPER_QUORUM) != null) {
      // Use server ZK cluster for server-issued connections, so we clone
      // the conf and unset the client ZK related properties
      conf = new Configuration(this.conf);
      conf.unset(HConstants.CLIENT_ZOOKEEPER_QUORUM);
    }
    // Create a cluster connection that when appropriate, can short-circuit and go directly to the
    // local server if the request is to the local server bypassing RPC. Can be used for both local
    // and remote invocations.
    return ConnectionUtils.createShortCircuitConnection(conf, null, userProvider.getCurrent(),
      serverName, rpcServices, rpcServices);
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
   * Setup our cluster connection if not already initialized.
   * @throws IOException
   */
  protected synchronized void setupClusterConnection() throws IOException {
    if (clusterConnection == null) {
      clusterConnection = createClusterConnection();
      metaTableLocator = new MetaTableLocator();
    }
  }

  /**
   * All initialization needed before we go register with Master.<br>
   * Do bare minimum. Do bulk of initializations AFTER we've connected to the Master.<br>
   * In here we just put up the RpcServer, setup Connection, and ZooKeeper.
   */
  private void preRegistrationInitialization() {
    try {
      initializeZooKeeper();
      setupClusterConnection();
      // Setup RPC client for master communication
      this.rpcClient = RpcClientFactory.createClient(conf, clusterId, new InetSocketAddress(
          this.rpcServices.isa.getAddress(), 0), clusterConnection.getConnectionMetrics());
    } catch (Throwable t) {
      // Call stop if error or process will stick around for ever since server
      // puts up non-daemon threads.
      this.rpcServices.stop();
      abort("Initialization of RS failed.  Hence aborting RS.", t);
    }
  }

  /**
   * Bring up connection to zk ensemble and then wait until a master for this cluster and then after
   * that, wait until cluster 'up' flag has been set. This is the order in which master does things.
   * <p>
   * Finally open long-living server short-circuit connection.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="RV_RETURN_VALUE_IGNORED_BAD_PRACTICE",
    justification="cluster Id znode read would give us correct response")
  private void initializeZooKeeper() throws IOException, InterruptedException {
    // Nothing to do in here if no Master in the mix.
    if (this.masterless) {
      return;
    }

    // Create the master address tracker, register with zk, and start it.  Then
    // block until a master is available.  No point in starting up if no master
    // running.
    blockAndCheckIfStopped(this.masterAddressTracker);

    // Wait on cluster being up.  Master will set this flag up in zookeeper
    // when ready.
    blockAndCheckIfStopped(this.clusterStatusTracker);

    // If we are HMaster then the cluster id should have already been set.
    if (clusterId == null) {
      // Retrieve clusterId
      // Since cluster status is now up
      // ID should have already been set by HMaster
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
      this.abort("Failed to reach coordination cluster when creating procedure handler.", e);
    }
  }

  /**
   * Utilty method to wait indefinitely on a znode availability while checking
   * if the region server is shut down
   * @param tracker znode tracker to use
   * @throws IOException any IO exception, plus if the RS is stopped
   * @throws InterruptedException
   */
  private void blockAndCheckIfStopped(ZKNodeTracker tracker)
      throws IOException, InterruptedException {
    while (tracker.blockUntilAvailable(this.msgInterval, false) == null) {
      if (this.stopped) {
        throw new IOException("Received the shutdown message while waiting.");
      }
    }
  }

  /**
   * @return True if the cluster is up.
   */
  @Override
  public boolean isClusterUp() {
    return this.masterless ||
        (this.clusterStatusTracker != null && this.clusterStatusTracker.isClusterUp());
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
        // Initialize the RegionServerCoprocessorHost now that our ephemeral
        // node was created, in case any coprocessors want to use ZooKeeper
        this.rsHost = new RegionServerCoprocessorHost(this, this.conf);
      }

      // Try and register with the Master; tell it we are here.  Break if
      // server is stopped or the clusterup flag is down or hdfs went wacky.
      // Once registered successfully, go ahead and start up all Services.
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

      if (!isStopped() && isHealthy()) {
        // start the snapshot handler and other procedure handlers,
        // since the server is ready to run
        if (this.rspmHost != null) {
          this.rspmHost.start();
        }
        // Start the Quota Manager
        if (this.rsQuotaManager != null) {
          rsQuotaManager.start(getRpcServer().getScheduler());
        }
        if (this.rsSpaceQuotaManager != null) {
          this.rsSpaceQuotaManager.start();
        }
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
    if (this.leases != null) {
      this.leases.closeAfterLeasesExpire();
    }
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
    mobCacheConfig.getMobFileCache().shutdown();

    if (movedRegionsCleaner != null) {
      movedRegionsCleaner.stop("Region Server stopping");
    }

    // Send interrupts to wake up threads if sleeping so they notice shutdown.
    // TODO: Should we check they are alive? If OOME could have exited already
    if (this.hMemManager != null) this.hMemManager.stop();
    if (this.cacheFlusher != null) this.cacheFlusher.interruptIfNecessary();
    if (this.compactSplitThread != null) this.compactSplitThread.interruptIfNecessary();
    if (this.compactionChecker != null) this.compactionChecker.cancel(true);
    if (this.healthCheckChore != null) this.healthCheckChore.cancel(true);
    if (this.nonceManagerChore != null) this.nonceManagerChore.cancel(true);
    if (this.storefileRefresher != null) this.storefileRefresher.cancel(true);
    sendShutdownInterrupt();

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
    if (this.clusterConnection != null && !clusterConnection.isClosed()) {
      try {
        this.clusterConnection.close();
      } catch (IOException e) {
        // Although the {@link Closeable} interface throws an {@link
        // IOException}, in reality, the implementation would never do that.
        LOG.warn("Attempt to close server's short circuit ClusterConnection failed.", e);
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
      LOG.info("stopping server " + this.serverName + "; all regions closed.");
    }

    // Stop the quota manager
    if (rsQuotaManager != null) {
      rsQuotaManager.stop();
    }
    if (rsSpaceQuotaManager != null) {
      rsSpaceQuotaManager.stop();
      rsSpaceQuotaManager = null;
    }

    //fsOk flag may be changed when closing regions throws exception.
    if (this.fsOk) {
      shutdownWAL(!abortRequested);
    }

    // Make sure the proxy is down.
    if (this.rssStub != null) {
      this.rssStub = null;
    }
    if (this.lockStub != null) {
      this.lockStub = null;
    }
    if (this.rpcClient != null) {
      this.rpcClient.close();
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
    this.shutDown = true;
    LOG.info("Exiting; stopping=" + this.serverName + "; zookeeper connection closed.");
  }

  private boolean containsMetaTableRegions() {
    return onlineRegions.containsKey(RegionInfoBuilder.FIRST_META_REGIONINFO.getEncodedName());
  }

  private boolean areAllUserRegionsOffline() {
    if (getNumberOfOnlineRegions() > 2) return false;
    boolean allUserRegionsOffline = true;
    for (Map.Entry<String, HRegion> e: this.onlineRegions.entrySet()) {
      if (!e.getValue().getRegionInfo().isMetaRegion()) {
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
    long writeCount = 0;
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
      request.setServer(ProtobufUtil.toServerName(this.serverName));
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
      createRegionServerStatusStub(true);
    }
  }

  /**
   * Reports the given map of Regions and their size on the filesystem to the active Master.
   *
   * @param regionSizeStore The store containing region sizes
   * @return false if FileSystemUtilizationChore should pause reporting to master. true otherwise
   */
  public boolean reportRegionSizesForQuotas(RegionSizeStore regionSizeStore) {
    RegionServerStatusService.BlockingInterface rss = rssStub;
    if (rss == null) {
      // the current server could be stopping.
      LOG.trace("Skipping Region size report to HMaster as stub is null");
      return true;
    }
    try {
      buildReportAndSend(rss, regionSizeStore);
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof PleaseHoldException) {
        LOG.trace("Failed to report region sizes to Master because it is initializing."
            + " This will be retried.", ioe);
        // The Master is coming up. Will retry the report later. Avoid re-creating the stub.
        return true;
      }
      if (rssStub == rss) {
        rssStub = null;
      }
      createRegionServerStatusStub(true);
      if (ioe instanceof DoNotRetryIOException) {
        DoNotRetryIOException doNotRetryEx = (DoNotRetryIOException) ioe;
        if (doNotRetryEx.getCause() != null) {
          Throwable t = doNotRetryEx.getCause();
          if (t instanceof UnsupportedOperationException) {
            LOG.debug("master doesn't support ReportRegionSpaceUse, pause before retrying");
            return false;
          }
        }
      }
      LOG.debug("Failed to report region sizes to Master. This will be retried.", ioe);
    }
    return true;
  }

  /**
   * Builds the region size report and sends it to the master. Upon successful sending of the
   * report, the region sizes that were sent are marked as sent.
   *
   * @param rss The stub to send to the Master
   * @param regionSizeStore The store containing region sizes
   */
  void buildReportAndSend(RegionServerStatusService.BlockingInterface rss,
      RegionSizeStore regionSizeStore) throws ServiceException {
    RegionSpaceUseReportRequest request =
        buildRegionSpaceUseReportRequest(Objects.requireNonNull(regionSizeStore));
    rss.reportRegionSpaceUse(null, request);
    // Record the number of size reports sent
    if (metricsRegionServer != null) {
      metricsRegionServer.incrementNumRegionSizeReportsSent(regionSizeStore.size());
    }
  }

  /**
   * Builds a {@link RegionSpaceUseReportRequest} protobuf message from the region size map.
   *
   * @param regionSizeStore The size in bytes of regions
   * @return The corresponding protocol buffer message.
   */
  RegionSpaceUseReportRequest buildRegionSpaceUseReportRequest(RegionSizeStore regionSizes) {
    RegionSpaceUseReportRequest.Builder request = RegionSpaceUseReportRequest.newBuilder();
    for (Entry<RegionInfo, RegionSize> entry : regionSizes) {
      request.addSpaceUse(convertRegionSize(entry.getKey(), entry.getValue().getSize()));
    }
    return request.build();
  }

  /**
   * Converts a pair of {@link RegionInfo} and {@code long} into a {@link RegionSpaceUse}
   * protobuf message.
   *
   * @param regionInfo The RegionInfo
   * @param sizeInBytes The size in bytes of the Region
   * @return The protocol buffer
   */
  RegionSpaceUse convertRegionSize(RegionInfo regionInfo, Long sizeInBytes) {
    return RegionSpaceUse.newBuilder()
        .setRegionInfo(ProtobufUtil.toRegionInfo(Objects.requireNonNull(regionInfo)))
        .setRegionSize(Objects.requireNonNull(sizeInBytes))
        .build();
  }

  ClusterStatusProtos.ServerLoad buildServerLoad(long reportStartTime, long reportEndTime)
      throws IOException {
    // We're getting the MetricsRegionServerWrapper here because the wrapper computes requests
    // per second, and other metrics  As long as metrics are part of ServerLoad it's best to use
    // the wrapper to compute those numbers in one place.
    // In the long term most of these should be moved off of ServerLoad and the heart beat.
    // Instead they should be stored in an HBase table so that external visibility into HBase is
    // improved; Additionally the load balancer will be able to take advantage of a more complete
    // history.
    MetricsRegionServerWrapper regionServerWrapper = metricsRegionServer.getRegionServerWrapper();
    Collection<HRegion> regions = getOnlineRegionsLocalContext();
    long usedMemory = -1L;
    long maxMemory = -1L;
    final MemoryUsage usage = MemorySizeUtil.safeGetHeapMemoryUsage();
    if (usage != null) {
      usedMemory = usage.getUsed();
      maxMemory = usage.getMax();
    }

    ClusterStatusProtos.ServerLoad.Builder serverLoad = ClusterStatusProtos.ServerLoad.newBuilder();
    serverLoad.setNumberOfRequests((int) regionServerWrapper.getRequestsPerSecond());
    serverLoad.setTotalNumberOfRequests(regionServerWrapper.getTotalRequestCount());
    serverLoad.setUsedHeapMB((int)(usedMemory / 1024 / 1024));
    serverLoad.setMaxHeapMB((int) (maxMemory / 1024 / 1024));
    Set<String> coprocessors = getWAL(null).getCoprocessorHost().getCoprocessors();
    Builder coprocessorBuilder = Coprocessor.newBuilder();
    for (String coprocessor : coprocessors) {
      serverLoad.addCoprocessors(coprocessorBuilder.setName(coprocessor).build());
    }
    RegionLoad.Builder regionLoadBldr = RegionLoad.newBuilder();
    RegionSpecifier.Builder regionSpecifier = RegionSpecifier.newBuilder();
    for (HRegion region : regions) {
      if (region.getCoprocessorHost() != null) {
        Set<String> regionCoprocessors = region.getCoprocessorHost().getCoprocessors();
        Iterator<String> iterator = regionCoprocessors.iterator();
        while (iterator.hasNext()) {
          serverLoad.addCoprocessors(coprocessorBuilder.setName(iterator.next()).build());
        }
      }
      serverLoad.addRegionLoads(createRegionLoad(region, regionLoadBldr, regionSpecifier));
      for (String coprocessor : getWAL(region.getRegionInfo()).getCoprocessorHost()
          .getCoprocessors()) {
        serverLoad.addCoprocessors(coprocessorBuilder.setName(coprocessor).build());
      }
    }
    serverLoad.setReportStartTime(reportStartTime);
    serverLoad.setReportEndTime(reportEndTime);
    if (this.infoServer != null) {
      serverLoad.setInfoServerPort(this.infoServer.getPort());
    } else {
      serverLoad.setInfoServerPort(-1);
    }

    // for the replicationLoad purpose. Only need to get from one executorService
    // either source or sink will get the same info
    ReplicationSourceService rsources = getReplicationSourceService();

    if (rsources != null) {
      // always refresh first to get the latest value
      ReplicationLoad rLoad = rsources.refreshAndGetReplicationLoad();
      if (rLoad != null) {
        serverLoad.setReplLoadSink(rLoad.getReplicationLoadSink());
        for (ClusterStatusProtos.ReplicationLoadSource rLS : rLoad.getReplicationLoadSourceList()) {
          serverLoad.addReplLoadSource(rLS);
        }
      }
    }

    return serverLoad.build();
  }

  String getOnlineRegionsAsPrintableString() {
    StringBuilder sb = new StringBuilder();
    for (Region r: this.onlineRegions.values()) {
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
    Set<String> closedRegions = new HashSet<>();
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
              LOG.debug("Online Regions=" + this.onlineRegions);
            }
          }
        }
        // Ensure all user regions have been sent a close. Use this to
        // protect against the case where an open comes in after we start the
        // iterator of onlineRegions to close all user regions.
        for (Map.Entry<String, HRegion> e : this.onlineRegions.entrySet()) {
          RegionInfo hri = e.getValue().getRegionInfo();
          if (!this.regionsInTransitionInRS.containsKey(hri.getEncodedNameAsBytes()) &&
              !closedRegions.contains(hri.getEncodedName())) {
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

  private void shutdownWAL(final boolean close) {
    if (this.walFactory != null) {
      try {
        if (close) {
          walFactory.close();
        } else {
          walFactory.shutdown();
        }
      } catch (Throwable e) {
        e = e instanceof RemoteException ? ((RemoteException) e).unwrapRemoteException() : e;
        LOG.error("Shutdown / close of WAL failed: " + e);
        LOG.debug("Shutdown / close exception details:", e);
      }
    }
  }

  /*
   * Run init. Sets up wal and starts up all server threads.
   *
   * @param c Extra configuration.
   */
  protected void handleReportForDutyResponse(final RegionServerStartupResponse c)
  throws IOException {
    try {
      boolean updateRootDir = false;
      for (NameStringPair e : c.getMapEntriesList()) {
        String key = e.getName();
        // The hostname the master sees us as.
        if (key.equals(HConstants.KEY_FOR_HOSTNAME_SEEN_BY_MASTER)) {
          String hostnameFromMasterPOV = e.getValue();
          this.serverName = ServerName.valueOf(hostnameFromMasterPOV, rpcServices.isa.getPort(),
              this.startcode);
          if (!StringUtils.isBlank(useThisHostnameInstead) &&
              !hostnameFromMasterPOV.equals(useThisHostnameInstead)) {
            String msg = "Master passed us a different hostname to use; was=" +
                this.useThisHostnameInstead + ", but now=" + hostnameFromMasterPOV;
            LOG.error(msg);
            throw new IOException(msg);
          }
          if (StringUtils.isBlank(useThisHostnameInstead) &&
              !hostnameFromMasterPOV.equals(rpcServices.isa.getHostName())) {
            String msg = "Master passed us a different hostname to use; was=" +
                rpcServices.isa.getHostName() + ", but now=" + hostnameFromMasterPOV;
            LOG.error(msg);
          }
          continue;
        }

        String value = e.getValue();
        if (key.equals(HConstants.HBASE_DIR)) {
          if (value != null && !value.equals(conf.get(HConstants.HBASE_DIR))) {
            updateRootDir = true;
          }
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("Config from master: " + key + "=" + value);
        }
        this.conf.set(key, value);
      }
      // Set our ephemeral znode up in zookeeper now we have a name.
      createMyEphemeralNode();

      if (updateRootDir) {
        // initialize file system by the config fs.defaultFS and hbase.rootdir from master
        initializeFileSystem();
      }

      // hack! Maps DFSClient => RegionServer for logs.  HDFS made this
      // config param for task trackers, but we can piggyback off of it.
      if (this.conf.get("mapreduce.task.attempt.id") == null) {
        this.conf.set("mapreduce.task.attempt.id", "hb_rs_" + this.serverName.toString());
      }

      // Save it in a file, this will allow to see if we crash
      ZNodeClearer.writeMyEphemeralNodeOnDisk(getMyEphemeralNodePath());

      // This call sets up an initialized replication and WAL. Later we start it up.
      setupWALAndReplication();
      // Init in here rather than in constructor after thread name has been set
      this.metricsRegionServer = new MetricsRegionServer(
          new MetricsRegionServerWrapperImpl(this), conf);
      this.metricsTable = new MetricsTable(new MetricsTableWrapperAggregateImpl(this));
      // Now that we have a metrics source, start the pause monitor
      this.pauseMonitor = new JvmPauseMonitor(conf, getMetrics().getMetricsSource());
      pauseMonitor.start();

      // There is a rare case where we do NOT want services to start. Check config.
      if (getConfiguration().getBoolean("hbase.regionserver.workers", true)) {
        startServices();
      }
      // In here we start up the replication Service. Above we initialized it. TODO. Reconcile.
      // or make sense of it.
      startReplicationService();


      // Set up ZK
      LOG.info("Serving as " + this.serverName + ", RpcServer on " + rpcServices.isa +
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

  protected void initializeMemStoreChunkCreator() {
    if (MemStoreLAB.isEnabled(conf)) {
      // MSLAB is enabled. So initialize MemStoreChunkPool
      // By this time, the MemstoreFlusher is already initialized. We can get the global limits from
      // it.
      Pair<Long, MemoryType> pair = MemorySizeUtil.getGlobalMemStoreSize(conf);
      long globalMemStoreSize = pair.getFirst();
      boolean offheap = this.regionServerAccounting.isOffheap();
      // When off heap memstore in use, take full area for chunk pool.
      float poolSizePercentage = offheap? 1.0F:
          conf.getFloat(MemStoreLAB.CHUNK_POOL_MAXSIZE_KEY, MemStoreLAB.POOL_MAX_SIZE_DEFAULT);
      float initialCountPercentage = conf.getFloat(MemStoreLAB.CHUNK_POOL_INITIALSIZE_KEY,
          MemStoreLAB.POOL_INITIAL_SIZE_DEFAULT);
      int chunkSize = conf.getInt(MemStoreLAB.CHUNK_SIZE_KEY, MemStoreLAB.CHUNK_SIZE_DEFAULT);
      // init the chunkCreator
      ChunkCreator chunkCreator =
          ChunkCreator.initialize(chunkSize, offheap, globalMemStoreSize, poolSizePercentage,
      initialCountPercentage, this.hMemManager);
    }
  }

  private void startHeapMemoryManager() {
    this.hMemManager = HeapMemoryManager.create(this.conf, this.cacheFlusher, this,
        this.regionServerAccounting);
    if (this.hMemManager != null) {
      this.hMemManager.start(getChoreService());
    }
  }

  private void createMyEphemeralNode() throws KeeperException, IOException {
    RegionServerInfo.Builder rsInfo = RegionServerInfo.newBuilder();
    rsInfo.setInfoPort(infoServer != null ? infoServer.getPort() : -1);
    rsInfo.setVersionInfo(ProtobufUtil.getVersionInfo());
    byte[] data = ProtobufUtil.prependPBMagic(rsInfo.build().toByteArray());
    ZKUtil.createEphemeralNodeAndWatch(this.zooKeeper, getMyEphemeralNodePath(), data);
  }

  private void deleteMyEphemeralNode() throws KeeperException {
    ZKUtil.deleteNode(this.zooKeeper, getMyEphemeralNodePath());
  }

  @Override
  public RegionServerAccounting getRegionServerAccounting() {
    return regionServerAccounting;
  }

  /*
   * @param r Region to get RegionLoad for.
   * @param regionLoadBldr the RegionLoad.Builder, can be null
   * @param regionSpecifier the RegionSpecifier.Builder, can be null
   * @return RegionLoad instance.
   *
   * @throws IOException
   */
  RegionLoad createRegionLoad(final HRegion r, RegionLoad.Builder regionLoadBldr,
      RegionSpecifier.Builder regionSpecifier) throws IOException {
    byte[] name = r.getRegionInfo().getRegionName();
    int stores = 0;
    int storefiles = 0;
    int storeUncompressedSizeMB = 0;
    int storefileSizeMB = 0;
    int memstoreSizeMB = (int) (r.getMemStoreDataSize() / 1024 / 1024);
    long storefileIndexSizeKB = 0;
    int rootLevelIndexSizeKB = 0;
    int totalStaticIndexSizeKB = 0;
    int totalStaticBloomSizeKB = 0;
    long totalCompactingKVs = 0;
    long currentCompactedKVs = 0;
    List<HStore> storeList = r.getStores();
    stores += storeList.size();
    for (HStore store : storeList) {
      storefiles += store.getStorefilesCount();
      storeUncompressedSizeMB += (int) (store.getStoreSizeUncompressed() / 1024 / 1024);
      storefileSizeMB += (int) (store.getStorefilesSize() / 1024 / 1024);
      //TODO: storefileIndexSizeKB is same with rootLevelIndexSizeKB?
      storefileIndexSizeKB += store.getStorefilesRootLevelIndexSize() / 1024;
      CompactionProgress progress = store.getCompactionProgress();
      if (progress != null) {
        totalCompactingKVs += progress.getTotalCompactingKVs();
        currentCompactedKVs += progress.currentCompactedKVs;
      }
      rootLevelIndexSizeKB += (int) (store.getStorefilesRootLevelIndexSize() / 1024);
      totalStaticIndexSizeKB += (int) (store.getTotalStaticIndexSize() / 1024);
      totalStaticBloomSizeKB += (int) (store.getTotalStaticBloomSize() / 1024);
    }

    float dataLocality =
        r.getHDFSBlocksDistribution().getBlockLocalityIndex(serverName.getHostname());
    if (regionLoadBldr == null) {
      regionLoadBldr = RegionLoad.newBuilder();
    }
    if (regionSpecifier == null) {
      regionSpecifier = RegionSpecifier.newBuilder();
    }
    regionSpecifier.setType(RegionSpecifierType.REGION_NAME);
    regionSpecifier.setValue(UnsafeByteOperations.unsafeWrap(name));
    regionLoadBldr.setRegionSpecifier(regionSpecifier.build())
      .setStores(stores)
      .setStorefiles(storefiles)
      .setStoreUncompressedSizeMB(storeUncompressedSizeMB)
      .setStorefileSizeMB(storefileSizeMB)
      .setMemStoreSizeMB(memstoreSizeMB)
      .setStorefileIndexSizeKB(storefileIndexSizeKB)
      .setRootIndexSizeKB(rootLevelIndexSizeKB)
      .setTotalStaticIndexSizeKB(totalStaticIndexSizeKB)
      .setTotalStaticBloomSizeKB(totalStaticBloomSizeKB)
      .setReadRequestsCount(r.getReadRequestsCount())
      .setCpRequestsCount(r.getCpRequestsCount())
      .setFilteredReadRequestsCount(r.getFilteredReadRequestsCount())
      .setWriteRequestsCount(r.getWriteRequestsCount())
      .setTotalCompactingKVs(totalCompactingKVs)
      .setCurrentCompactedKVs(currentCompactedKVs)
      .setDataLocality(dataLocality)
      .setLastMajorCompactionTs(r.getOldestHfileTs(true));
    r.setCompleteSequenceId(regionLoadBldr);

    return regionLoadBldr.build();
  }

  /**
   * @param encodedRegionName
   * @return An instance of RegionLoad.
   */
  public RegionLoad createRegionLoad(final String encodedRegionName) throws IOException {
    HRegion r = onlineRegions.get(encodedRegionName);
    return r != null ? createRegionLoad(r, null, null) : null;
  }

  /*
   * Inner class that runs on a long period checking if regions need compaction.
   */
  private static class CompactionChecker extends ScheduledChore {
    private final HRegionServer instance;
    private final int majorCompactPriority;
    private final static int DEFAULT_PRIORITY = Integer.MAX_VALUE;
    //Iteration is 1-based rather than 0-based so we don't check for compaction
    // immediately upon region server startup
    private long iteration = 1;

    CompactionChecker(final HRegionServer h, final int sleepTime, final Stoppable stopper) {
      super("CompactionChecker", stopper, sleepTime);
      this.instance = h;
      LOG.info(this.getName() + " runs every " + Duration.ofMillis(sleepTime));

      /* MajorCompactPriority is configurable.
       * If not set, the compaction will use default priority.
       */
      this.majorCompactPriority = this.instance.conf.
          getInt("hbase.regionserver.compactionChecker.majorCompactPriority",
              DEFAULT_PRIORITY);
    }

    @Override
    protected void chore() {
      for (Region r : this.instance.onlineRegions.values()) {
        if (r == null) {
          continue;
        }
        HRegion hr = (HRegion) r;
        for (HStore s : hr.stores.values()) {
          try {
            long multiplier = s.getCompactionCheckMultiplier();
            assert multiplier > 0;
            if (iteration % multiplier != 0) {
              continue;
            }
            if (s.needsCompaction()) {
              // Queue a compaction. Will recognize if major is needed.
              this.instance.compactSplitThread.requestSystemCompaction(hr, s,
                getName() + " requests compaction");
            } else if (s.shouldPerformMajorCompaction()) {
              s.triggerMajorCompaction();
              if (majorCompactPriority == DEFAULT_PRIORITY ||
                  majorCompactPriority > hr.getCompactPriority()) {
                this.instance.compactSplitThread.requestCompaction(hr, s,
                    getName() + " requests major compaction; use default priority",
                    Store.NO_PRIORITY,
                CompactionLifeCycleTracker.DUMMY, null);
              } else {
                this.instance.compactSplitThread.requestCompaction(hr, s,
                    getName() + " requests major compaction; use configured priority",
                    this.majorCompactPriority, CompactionLifeCycleTracker.DUMMY, null);
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

  static class PeriodicMemStoreFlusher extends ScheduledChore {
    final HRegionServer server;
    final static int RANGE_OF_DELAY = 5 * 60 * 1000; // 5 min in milliseconds
    final static int MIN_DELAY_TIME = 0; // millisec
    public PeriodicMemStoreFlusher(int cacheFlushInterval, final HRegionServer server) {
      super("MemstoreFlusherChore", server, cacheFlushInterval);
      this.server = server;
    }

    @Override
    protected void chore() {
      final StringBuilder whyFlush = new StringBuilder();
      for (HRegion r : this.server.onlineRegions.values()) {
        if (r == null) continue;
        if (r.shouldFlush(whyFlush)) {
          FlushRequester requester = server.getFlushRequester();
          if (requester != null) {
            long randomDelay = (long) RandomUtils.nextInt(0, RANGE_OF_DELAY) + MIN_DELAY_TIME;
            LOG.info(getName() + " requesting flush of " +
              r.getRegionInfo().getRegionNameAsString() + " because " +
              whyFlush.toString() +
              " after random delay " + randomDelay + "ms");
            //Throttle the flushes by putting a delay. If we don't throttle, and there
            //is a balanced write-load on the regions in a table, we might end up
            //overwhelming the filesystem with too many flushes at once.
            requester.requestDelayedFlush(r, randomDelay, false);
          }
        }
      }
    }
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

  /**
   * Setup WAL log and replication if enabled. Replication setup is done in here because it wants to
   * be hooked up to WAL.
   */
  private void setupWALAndReplication() throws IOException {
    boolean isMasterNoTableOrSystemTableOnly = this instanceof HMaster &&
      (!LoadBalancer.isTablesOnMaster(conf) || LoadBalancer.isSystemTablesOnlyOnMaster(conf));
    WALFactory factory =
        new WALFactory(conf, serverName.toString(), !isMasterNoTableOrSystemTableOnly);
    if (!isMasterNoTableOrSystemTableOnly) {
      // TODO Replication make assumptions here based on the default filesystem impl
      Path oldLogDir = new Path(walRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
      String logName = AbstractFSWALProvider.getWALDirectoryName(this.serverName.toString());

      Path logDir = new Path(walRootDir, logName);
      LOG.debug("logDir={}", logDir);
      if (this.walFs.exists(logDir)) {
        throw new RegionServerRunningException(
            "Region server has already created directory at " + this.serverName.toString());
      }
      // Always create wal directory as now we need this when master restarts to find out the live
      // region servers.
      if (!this.walFs.mkdirs(logDir)) {
        throw new IOException("Can not create wal directory " + logDir);
      }
      // Instantiate replication if replication enabled. Pass it the log directories.
      createNewReplicationInstance(conf, this, this.walFs, logDir, oldLogDir,
        factory.getWALProvider());
    }
    this.walFactory = factory;
  }

  /**
   * Start up replication source and sink handlers.
   * @throws IOException
   */
  private void startReplicationService() throws IOException {
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
   * Start all threads we need to run. This is called after we've successfully
   * registered with the Master.
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
  private void startServices() throws IOException {
    if (!isStopped() && !isAborted()) {
      initializeThreads();
    }
    this.secureBulkLoadManager = new SecureBulkLoadManager(this.conf, clusterConnection);
    this.secureBulkLoadManager.start();

    // Health checker thread.
    if (isHealthCheckerConfigured()) {
      int sleepTime = this.conf.getInt(HConstants.HEALTH_CHORE_WAKE_FREQ,
      HConstants.DEFAULT_THREAD_WAKE_FREQUENCY);
      healthCheckChore = new HealthCheckChore(sleepTime, this, getConfiguration());
    }

    this.walRoller = new LogRoller(this, this);
    this.flushThroughputController = FlushThroughputControllerFactory.create(this, conf);
    this.procedureResultReporter = new RemoteProcedureResultReporter(this);

    // Create the CompactedFileDischarger chore executorService. This chore helps to
    // remove the compacted files
    // that will no longer be used in reads.
    // Default is 2 mins. The default value for TTLCleaner is 5 mins so we set this to
    // 2 mins so that compacted files can be archived before the TTLCleaner runs
    int cleanerInterval =
    conf.getInt("hbase.hfile.compaction.discharger.interval", 2 * 60 * 1000);
    this.compactedFileDischarger =
    new CompactedHFilesDischarger(cleanerInterval, this, this);
    choreService.scheduleChore(compactedFileDischarger);

    // Start executor services
    this.executorService.startExecutorService(ExecutorType.RS_OPEN_REGION,
        conf.getInt("hbase.regionserver.executor.openregion.threads", 3));
    this.executorService.startExecutorService(ExecutorType.RS_OPEN_META,
        conf.getInt("hbase.regionserver.executor.openmeta.threads", 1));
    this.executorService.startExecutorService(ExecutorType.RS_OPEN_PRIORITY_REGION,
        conf.getInt("hbase.regionserver.executor.openpriorityregion.threads", 3));
    this.executorService.startExecutorService(ExecutorType.RS_CLOSE_REGION,
        conf.getInt("hbase.regionserver.executor.closeregion.threads", 3));
    this.executorService.startExecutorService(ExecutorType.RS_CLOSE_META,
        conf.getInt("hbase.regionserver.executor.closemeta.threads", 1));
    if (conf.getBoolean(StoreScanner.STORESCANNER_PARALLEL_SEEK_ENABLE, false)) {
      this.executorService.startExecutorService(ExecutorType.RS_PARALLEL_SEEK,
          conf.getInt("hbase.storescanner.parallel.seek.threads", 10));
    }
    this.executorService.startExecutorService(ExecutorType.RS_LOG_REPLAY_OPS, conf.getInt(
    "hbase.regionserver.wal.max.splitters", SplitLogWorkerCoordination.DEFAULT_MAX_SPLITTERS));
    // Start the threads for compacted files discharger
    this.executorService.startExecutorService(ExecutorType.RS_COMPACTED_FILES_DISCHARGER,
        conf.getInt(CompactionConfiguration.HBASE_HFILE_COMPACTION_DISCHARGER_THREAD_COUNT, 10));
    if (ServerRegionReplicaUtil.isRegionReplicaWaitForPrimaryFlushEnabled(conf)) {
      this.executorService.startExecutorService(ExecutorType.RS_REGION_REPLICA_FLUSH_OPS,
          conf.getInt("hbase.regionserver.region.replica.flusher.threads",
              conf.getInt("hbase.regionserver.executor.openregion.threads", 3)));
    }
    this.executorService.startExecutorService(ExecutorType.RS_REFRESH_PEER,
      conf.getInt("hbase.regionserver.executor.refresh.peer.threads", 2));
    this.executorService.startExecutorService(ExecutorType.RS_REPLAY_SYNC_REPLICATION_WAL,
      conf.getInt("hbase.regionserver.executor.replay.sync.replication.wal.threads", 1));

    Threads.setDaemonThreadRunning(this.walRoller.getThread(), getName() + ".logRoller",
    uncaughtExceptionHandler);
    this.cacheFlusher.start(uncaughtExceptionHandler);
    Threads.setDaemonThreadRunning(this.procedureResultReporter,
      getName() + ".procedureResultReporter", uncaughtExceptionHandler);

    if (this.compactionChecker != null) choreService.scheduleChore(compactionChecker);
    if (this.periodicFlusher != null) choreService.scheduleChore(periodicFlusher);
    if (this.healthCheckChore != null) choreService.scheduleChore(healthCheckChore);
    if (this.nonceManagerChore != null) choreService.scheduleChore(nonceManagerChore);
    if (this.storefileRefresher != null) choreService.scheduleChore(storefileRefresher);
    if (this.movedRegionsCleaner != null) choreService.scheduleChore(movedRegionsCleaner);
    if (this.fsUtilizationChore != null) choreService.scheduleChore(fsUtilizationChore);

    // Leases is not a Thread. Internally it runs a daemon thread. If it gets
    // an unhandled exception, it will just exit.
    Threads.setDaemonThreadRunning(this.leases.getThread(), getName() + ".leaseChecker",
    uncaughtExceptionHandler);

    // Create the log splitting worker and start it
    // set a smaller retries to fast fail otherwise splitlogworker could be blocked for
    // quite a while inside Connection layer. The worker won't be available for other
    // tasks even after current task is preempted after a split task times out.
    Configuration sinkConf = HBaseConfiguration.create(conf);
    sinkConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        conf.getInt("hbase.log.replay.retries.number", 8)); // 8 retries take about 23 seconds
    sinkConf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
        conf.getInt("hbase.log.replay.rpc.timeout", 30000)); // default 30 seconds
    sinkConf.setInt(HConstants.HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER, 1);
    if (this.csm != null) {
      // SplitLogWorker needs csm. If none, don't start this.
      this.splitLogWorker = new SplitLogWorker(this, sinkConf, this,
          this, walFactory);
      splitLogWorker.start();
    } else {
      LOG.warn("SplitLogWorker Service NOT started; CoordinatedStateManager is null");
    }

    // Memstore services.
    startHeapMemoryManager();
    // Call it after starting HeapMemoryManager.
    initializeMemStoreChunkCreator();
  }

  private void initializeThreads() throws IOException {
    // Cache flushing thread.
    this.cacheFlusher = new MemStoreFlusher(conf, this);

    // Compaction thread
    this.compactSplitThread = new CompactSplit(this);

    // Background thread to check for compactions; needed if region has not gotten updates
    // in a while. It will take care of not checking too frequently on store-by-store basis.
    this.compactionChecker = new CompactionChecker(this, this.threadWakeFrequency, this);
    this.periodicFlusher = new PeriodicMemStoreFlusher(this.threadWakeFrequency, this);
    this.leases = new Leases(this.threadWakeFrequency);

    // Create the thread to clean the moved regions list
    movedRegionsCleaner = MovedRegionsCleaner.create(this);

    if (this.nonceManager != null) {
      // Create the scheduled chore that cleans up nonces.
      nonceManagerChore = this.nonceManager.createCleanupScheduledChore(this);
    }

    // Setup the Quota Manager
    rsQuotaManager = new RegionServerRpcQuotaManager(this);
    rsSpaceQuotaManager = new RegionServerSpaceQuotaManager(this);

    if (QuotaUtil.isQuotaEnabled(conf)) {
      this.fsUtilizationChore = new FileSystemUtilizationChore(this);
    }


    boolean onlyMetaRefresh = false;
    int storefileRefreshPeriod = conf.getInt(
        StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD,
        StorefileRefresherChore.DEFAULT_REGIONSERVER_STOREFILE_REFRESH_PERIOD);
    if (storefileRefreshPeriod == 0) {
      storefileRefreshPeriod = conf.getInt(
          StorefileRefresherChore.REGIONSERVER_META_STOREFILE_REFRESH_PERIOD,
          StorefileRefresherChore.DEFAULT_REGIONSERVER_STOREFILE_REFRESH_PERIOD);
      onlyMetaRefresh = true;
    }
    if (storefileRefreshPeriod > 0) {
      this.storefileRefresher = new StorefileRefresherChore(storefileRefreshPeriod,
          onlyMetaRefresh, this, this);
    }
    registerConfigurationObservers();
  }

  private void registerConfigurationObservers() {
    // Registering the compactSplitThread object with the ConfigurationManager.
    configurationManager.registerObserver(this.compactSplitThread);
    configurationManager.registerObserver(this.rpcServices);
    configurationManager.registerObserver(this);
  }

  /**
   * Puts up the webui.
   * @return Returns final port -- maybe different from what we started with.
   * @throws IOException
   */
  private int putUpWebUI() throws IOException {
    int port = this.conf.getInt(HConstants.REGIONSERVER_INFO_PORT,
      HConstants.DEFAULT_REGIONSERVER_INFOPORT);
    String addr = this.conf.get("hbase.regionserver.info.bindAddress", "0.0.0.0");

    if(this instanceof HMaster) {
      port = conf.getInt(HConstants.MASTER_INFO_PORT,
          HConstants.DEFAULT_MASTER_INFOPORT);
      addr = this.conf.get("hbase.master.info.bindAddress", "0.0.0.0");
    }
    // -1 is for disabling info server
    if (port < 0) return port;

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
    boolean healthy = (this.leases == null || this.leases.isAlive())
        && (this.cacheFlusher == null || this.cacheFlusher.isAlive())
        && (this.walRoller == null || this.walRoller.isAlive())
        && (this.compactionChecker == null || this.compactionChecker.isScheduled())
        && (this.periodicFlusher == null || this.periodicFlusher.isScheduled());
    if (!healthy) {
      stop("One or more threads are no longer alive -- stop");
    }
    return healthy;
  }

  @Override
  public List<WAL> getWALs() throws IOException {
    return walFactory.getWALs();
  }

  @Override
  public WAL getWAL(RegionInfo regionInfo) throws IOException {
    WAL wal = walFactory.getWAL(regionInfo);
    if (this.walRoller != null) {
      this.walRoller.addWAL(wal);
    }
    return wal;
  }

  public LogRoller getWalRoller() {
    return walRoller;
  }

  @Override
  public Connection getConnection() {
    return getClusterConnection();
  }

  @Override
  public ClusterConnection getClusterConnection() {
    return this.clusterConnection;
  }

  @Override
  public MetaTableLocator getMetaTableLocator() {
    return this.metaTableLocator;
  }

  @Override
  public void stop(final String msg) {
    stop(msg, false, RpcServer.getRequestUser().orElse(null));
  }

  /**
   * Stops the regionserver.
   * @param msg Status message
   * @param force True if this is a regionserver abort
   * @param user The user executing the stop request, or null if no user is associated
   */
  public void stop(final String msg, final boolean force, final User user) {
    if (!this.stopped) {
      LOG.info("***** STOPPING region server '" + this + "' *****");
      if (this.rsHost != null) {
        // when forced via abort don't allow CPs to override
        try {
          this.rsHost.preStop(msg, user);
        } catch (IOException ioe) {
          if (!force) {
            LOG.warn("The region server did not stop", ioe);
            return;
          }
          LOG.warn("Skipping coprocessor exception on preStop() due to forced shutdown", ioe);
        }
      }
      this.stopped = true;
      LOG.info("STOPPED: " + msg);
      // Wakes run() if it is sleeping
      sleeper.skipSleepCycle();
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
  public void postOpenDeployTasks(final PostOpenDeployContext context)
      throws KeeperException, IOException {
    HRegion r = context.getRegion();
    long masterSystemTime = context.getMasterSystemTime();
    rpcServices.checkOpen();
    LOG.info("Post open deploy tasks for " + r.getRegionInfo().getRegionNameAsString());
    // Do checks to see if we need to compact (references or too many files)
    for (HStore s : r.stores.values()) {
      if (s.hasReferences() || s.needsCompaction()) {
        this.compactSplitThread.requestSystemCompaction(r, s, "Opening Region");
      }
    }
    long openSeqNum = r.getOpenSeqNum();
    if (openSeqNum == HConstants.NO_SEQNUM) {
      // If we opened a region, we should have read some sequence number from it.
      LOG.error("No sequence number found when opening " +
        r.getRegionInfo().getRegionNameAsString());
      openSeqNum = 0;
    }

    // Notify master
    if (!reportRegionStateTransition(new RegionStateTransitionContext(
        TransitionCode.OPENED, openSeqNum, masterSystemTime, r.getRegionInfo()))) {
      throw new IOException("Failed to report opened region to master: "
        + r.getRegionInfo().getRegionNameAsString());
    }

    triggerFlushInPrimaryRegion(r);

    LOG.debug("Finished post open deploy task for " + r.getRegionInfo().getRegionNameAsString());
  }

  @Override
  public boolean reportRegionStateTransition(final RegionStateTransitionContext context) {
    TransitionCode code = context.getCode();
    long openSeqNum = context.getOpenSeqNum();
    long masterSystemTime = context.getMasterSystemTime();
    RegionInfo[] hris = context.getHris();

    if (TEST_SKIP_REPORTING_TRANSITION) {
      // This is for testing only in case there is no master
      // to handle the region transition report at all.
      if (code == TransitionCode.OPENED) {
        Preconditions.checkArgument(hris != null && hris.length == 1);
        if (hris[0].isMetaRegion()) {
          try {
            MetaTableLocator.setMetaLocation(getZooKeeper(), serverName,
                hris[0].getReplicaId(),State.OPEN);
          } catch (KeeperException e) {
            LOG.info("Failed to update meta location", e);
            return false;
          }
        } else {
          try {
            MetaTableAccessor.updateRegionLocation(clusterConnection,
              hris[0], serverName, openSeqNum, masterSystemTime);
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
    for (RegionInfo hri: hris) {
      transition.addRegionInfo(ProtobufUtil.toRegionInfo(hri));
    }
    ReportRegionStateTransitionRequest request = builder.build();
    int tries = 0;
    long pauseTime = INIT_PAUSE_TIME_MS;
    // Keep looping till we get an error. We want to send reports even though server is going down.
    // Only go down if clusterConnection is null. It is set to null almost as last thing as the
    // HRegionServer does down.
    while (this.clusterConnection != null && !this.clusterConnection.isClosed()) {
      RegionServerStatusService.BlockingInterface rss = rssStub;
      try {
        if (rss == null) {
          createRegionServerStatusStub();
          continue;
        }
        ReportRegionStateTransitionResponse response =
          rss.reportRegionStateTransition(null, request);
        if (response.hasErrorMessage()) {
          LOG.info("TRANSITION FAILED " + request + ": " + response.getErrorMessage());
          break;
        }
        // Log if we had to retry else don't log unless TRACE. We want to
        // know if were successful after an attempt showed in logs as failed.
        if (tries > 0 || LOG.isTraceEnabled()) {
          LOG.info("TRANSITION REPORTED " + request);
        }
        // NOTE: Return mid-method!!!
        return true;
      } catch (ServiceException se) {
        IOException ioe = ProtobufUtil.getRemoteException(se);
        boolean pause = ioe instanceof ServerNotRunningYetException ||
            ioe instanceof PleaseHoldException;
        if (pause) {
          // Do backoff else we flood the Master with requests.
          pauseTime = ConnectionUtils.getPauseTime(INIT_PAUSE_TIME_MS, tries);
        } else {
          pauseTime = INIT_PAUSE_TIME_MS; // Reset.
        }
        LOG.info("Failed report transition " +
          TextFormat.shortDebugString(request) + "; retry (#" + tries + ")" +
            (pause?
                " after " + pauseTime + "ms delay (Master is coming online...).":
                " immediately."),
            ioe);
        if (pause) Threads.sleep(pauseTime);
        tries++;
        if (rssStub == rss) {
          rssStub = null;
        }
      }
    }
    return false;
  }

  /**
   * Trigger a flush in the primary region replica if this region is a secondary replica. Does not
   * block this thread. See RegionReplicaFlushHandler for details.
   */
  void triggerFlushInPrimaryRegion(final HRegion region) {
    if (ServerRegionReplicaUtil.isDefaultReplica(region.getRegionInfo())) {
      return;
    }
    if (!ServerRegionReplicaUtil.isRegionReplicaReplicationEnabled(region.conf) ||
        !ServerRegionReplicaUtil.isRegionReplicaWaitForPrimaryFlushEnabled(
          region.conf)) {
      region.setReadsEnabled(true);
      return;
    }

    region.setReadsEnabled(false); // disable reads before marking the region as opened.
    // RegionReplicaFlushHandler might reset this.

    // submit it to be handled by one of the handlers so that we do not block OpenRegionHandler
    if (this.executorService != null) {
      this.executorService.submit(new RegionReplicaFlushHandler(this, clusterConnection,
          rpcRetryingCallerFactory, rpcControllerFactory, operationTimeout, region));
    }
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
    String msg = "***** ABORTING region server " + this + ": " + reason + " *****";
    if (cause != null) {
      LOG.error(HBaseMarkers.FATAL, msg, cause);
    } else {
      LOG.error(HBaseMarkers.FATAL, msg);
    }
    this.abortRequested = true;
    // HBASE-4014: show list of coprocessors that were loaded to help debug
    // regionserver crashes.Note that we're implicitly using
    // java.util.HashSet's toString() method to print the coprocessor names.
    LOG.error(HBaseMarkers.FATAL, "RegionServer abort: loaded coprocessors are: " +
        CoprocessorHost.getLoadedCoprocessors());
    // Try and dump metrics if abort -- might give clue as to how fatal came about....
    try {
      LOG.info("Dump of metrics as JSON on abort: " + DumpRegionServerMetrics.dumpMetrics());
    } catch (MalformedObjectNameException | IOException e) {
      LOG.warn("Failed dumping metrics", e);
    }

    // Do our best to report our abort to the master, but this may not work
    try {
      if (cause != null) {
        msg += "\nCause:\n" + Throwables.getStackTraceAsString(cause);
      }
      // Report to the master but only if we have already registered with the master.
      if (rssStub != null && this.serverName != null) {
        ReportRSFatalErrorRequest.Builder builder =
          ReportRSFatalErrorRequest.newBuilder();
        builder.setServer(ProtobufUtil.toServerName(this.serverName));
        builder.setErrorMessage(msg);
        rssStub.reportRSFatalError(null, builder.build());
      }
    } catch (Throwable t) {
      LOG.warn("Unable to report fatal error to master", t);
    }
    // shutdown should be run as the internal user
    stop(reason, true, null);
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
  @VisibleForTesting
  protected void kill() {
    this.killed = true;
    abort("Simulated kill");
  }

  /**
   * Called on stop/abort before closing the cluster connection and meta locator.
   */
  protected void sendShutdownInterrupt() {
  }

  /**
   * Wait on all threads to finish. Presumption is that all closes and stops
   * have already been called.
   */
  protected void stopServiceThreads() {
    // clean up the scheduled chores
    if (this.choreService != null) choreService.shutdown();
    if (this.nonceManagerChore != null) nonceManagerChore.cancel(true);
    if (this.compactionChecker != null) compactionChecker.cancel(true);
    if (this.periodicFlusher != null) periodicFlusher.cancel(true);
    if (this.healthCheckChore != null) healthCheckChore.cancel(true);
    if (this.storefileRefresher != null) storefileRefresher.cancel(true);
    if (this.movedRegionsCleaner != null) movedRegionsCleaner.cancel(true);
    if (this.fsUtilizationChore != null) fsUtilizationChore.cancel(true);

    if (this.cacheFlusher != null) {
      this.cacheFlusher.join();
    }

    if (this.spanReceiverHost != null) {
      this.spanReceiverHost.closeReceivers();
    }
    if (this.walRoller != null) {
      this.walRoller.close();
    }
    if (this.compactSplitThread != null) {
      this.compactSplitThread.join();
    }
    if (this.executorService != null) this.executorService.shutdown();
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
  }

  /**
   * @return Return the object that implements the replication
   * source executorService.
   */
  @Override
  public ReplicationSourceService getReplicationSourceService() {
    return replicationSourceHandler;
  }

  /**
   * @return Return the object that implements the replication
   * sink executorService.
   */
  ReplicationSinkService getReplicationSinkService() {
    return replicationSinkHandler;
  }

  /**
   * Get the current master from ZooKeeper and open the RPC connection to it.
   * To get a fresh connection, the current rssStub must be null.
   * Method will block until a master is available. You can break from this
   * block by requesting the server stop.
   *
   * @return master + port, or null if server has been stopped
   */
  @VisibleForTesting
  protected synchronized ServerName createRegionServerStatusStub() {
    // Create RS stub without refreshing the master node from ZK, use cached data
    return createRegionServerStatusStub(false);
  }

  /**
   * Get the current master from ZooKeeper and open the RPC connection to it. To get a fresh
   * connection, the current rssStub must be null. Method will block until a master is available.
   * You can break from this block by requesting the server stop.
   * @param refresh If true then master address will be read from ZK, otherwise use cached data
   * @return master + port, or null if server has been stopped
   */
  @VisibleForTesting
  protected synchronized ServerName createRegionServerStatusStub(boolean refresh) {
    if (rssStub != null) {
      return masterAddressTracker.getMasterAddress();
    }
    ServerName sn = null;
    long previousLogTime = 0;
    RegionServerStatusService.BlockingInterface intRssStub = null;
    LockService.BlockingInterface intLockStub = null;
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
          intRssStub = ((HMaster)this).getMasterRpcServices();
          intLockStub = ((HMaster)this).getMasterRpcServices();
          break;
        }
        try {
          BlockingRpcChannel channel =
            this.rpcClient.createBlockingRpcChannel(sn, userProvider.getCurrent(),
              shortOperationTimeout);
          intRssStub = RegionServerStatusService.newBlockingStub(channel);
          intLockStub = LockService.newBlockingStub(channel);
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
    this.rssStub = intRssStub;
    this.lockStub = intLockStub;
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
    if (this.masterless) return RegionServerStartupResponse.getDefaultInstance();
    ServerName masterServerName = createRegionServerStatusStub(true);
    if (masterServerName == null) return null;
    RegionServerStartupResponse result = null;
    try {
      rpcServices.requestCount.reset();
      rpcServices.rpcGetRequestCount.reset();
      rpcServices.rpcScanRequestCount.reset();
      rpcServices.rpcMultiRequestCount.reset();
      rpcServices.rpcMutateRequestCount.reset();
      LOG.info("reportForDuty to master=" + masterServerName + " with port="
        + rpcServices.isa.getPort() + ", startcode=" + this.startcode);
      long now = EnvironmentEdgeManager.currentTime();
      int port = rpcServices.isa.getPort();
      RegionServerStartupRequest.Builder request = RegionServerStartupRequest.newBuilder();
      if (!StringUtils.isBlank(useThisHostnameInstead)) {
        request.setUseThisHostnameInstead(useThisHostnameInstead);
      }
      request.setPort(port);
      request.setServerStartCode(this.startcode);
      request.setServerCurrentTime(now);
      result = this.rssStub.regionServerStartup(null, request.build());
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof ClockOutOfSyncException) {
        LOG.error(HBaseMarkers.FATAL, "Master rejected startup because clock is out of sync",
            ioe);
        // Re-throw IOE will cause RS to abort
        throw ioe;
      } else if (ioe instanceof ServerNotRunningYetException) {
        LOG.debug("Master is not running yet");
      } else {
        LOG.warn("error telling master we are up", se);
      }
      rssStub = null;
    }
    return result;
  }

  @Override
  public RegionStoreSequenceIds getLastSequenceId(byte[] encodedRegionName) {
    try {
      GetLastFlushedSequenceIdRequest req =
          RequestConverter.buildGetLastFlushedSequenceIdRequest(encodedRegionName);
      RegionServerStatusService.BlockingInterface rss = rssStub;
      if (rss == null) { // Try to connect one more time
        createRegionServerStatusStub();
        rss = rssStub;
        if (rss == null) {
          // Still no luck, we tried
          LOG.warn("Unable to connect to the master to check " + "the last flushed sequence id");
          return RegionStoreSequenceIds.newBuilder().setLastFlushedSequenceId(HConstants.NO_SEQNUM)
              .build();
        }
      }
      GetLastFlushedSequenceIdResponse resp = rss.getLastFlushedSequenceId(null, req);
      return RegionStoreSequenceIds.newBuilder()
          .setLastFlushedSequenceId(resp.getLastFlushedSequenceId())
          .addAllStoreSequenceId(resp.getStoreLastFlushedSequenceIdList()).build();
    } catch (ServiceException e) {
      LOG.warn("Unable to connect to the master to check the last flushed sequence id", e);
      return RegionStoreSequenceIds.newBuilder().setLastFlushedSequenceId(HConstants.NO_SEQNUM)
          .build();
    }
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
        RegionInfo hri = e.getValue().getRegionInfo();
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
        if (!r.getRegionInfo().isMetaRegion() && r.isAvailable()) {
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
  public void addRegion(HRegion region) {
    this.onlineRegions.put(region.getRegionInfo().getEncodedName(), region);
    configurationManager.registerObserver(region);
  }

  private void addRegion(SortedMap<Long, Collection<HRegion>> sortedRegions, HRegion region,
      long size) {
    if (!sortedRegions.containsKey(size)) {
      sortedRegions.put(size, new ArrayList<>());
    }
    sortedRegions.get(size).add(region);
  }
  /**
   * @return A new Map of online regions sorted by region off-heap size with the first entry being
   *   the biggest.
   */
  SortedMap<Long, Collection<HRegion>> getCopyOfOnlineRegionsSortedByOffHeapSize() {
    // we'll sort the regions in reverse
    SortedMap<Long, Collection<HRegion>> sortedRegions = new TreeMap<>(
        new Comparator<Long>() {
          @Override
          public int compare(Long a, Long b) {
            return -1 * a.compareTo(b);
          }
        });
    // Copy over all regions. Regions are sorted by size with biggest first.
    for (HRegion region : this.onlineRegions.values()) {
      addRegion(sortedRegions, region, region.getMemStoreOffHeapSize());
    }
    return sortedRegions;
  }

  /**
   * @return A new Map of online regions sorted by region heap size with the first entry being the
   *   biggest.
   */
  SortedMap<Long, Collection<HRegion>> getCopyOfOnlineRegionsSortedByOnHeapSize() {
    // we'll sort the regions in reverse
    SortedMap<Long, Collection<HRegion>> sortedRegions = new TreeMap<>(
        new Comparator<Long>() {
          @Override
          public int compare(Long a, Long b) {
            return -1 * a.compareTo(b);
          }
        });
    // Copy over all regions. Regions are sorted by size with biggest first.
    for (HRegion region : this.onlineRegions.values()) {
      addRegion(sortedRegions, region, region.getMemStoreHeapSize());
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

  @Override
  public CompactionRequester getCompactionRequestor() {
    return this.compactSplitThread;
  }

  /**
   * Get the top N most loaded regions this server is serving so we can tell the
   * master which regions it can reallocate if we're overloaded. TODO: actually
   * calculate which regions are most loaded. (Right now, we're just grabbing
   * the first N regions being served regardless of load.)
   */
  protected RegionInfo[] getMostLoadedRegions() {
    ArrayList<RegionInfo> regions = new ArrayList<>();
    for (Region r : onlineRegions.values()) {
      if (!r.isAvailable()) {
        continue;
      }
      if (regions.size() < numRegionsToReport) {
        regions.add(r.getRegionInfo());
      } else {
        break;
      }
    }
    return regions.toArray(new RegionInfo[regions.size()]);
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
  public ZKWatcher getZooKeeper() {
    return zooKeeper;
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
    return csm;
  }

  @Override
  public ServerName getServerName() {
    return serverName;
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
    return executorService;
  }

  @Override
  public ChoreService getChoreService() {
    return choreService;
  }

  @Override
  public RegionServerRpcQuotaManager getRegionServerRpcQuotaManager() {
    return rsQuotaManager;
  }

  //
  // Main program and support routines
  //
  /**
   * Load the replication executorService objects, if any
   */
  private static void createNewReplicationInstance(Configuration conf, HRegionServer server,
      FileSystem walFs, Path walDir, Path oldWALDir, WALProvider walProvider) throws IOException {
    // read in the name of the source replication class from the config file.
    String sourceClassname = conf.get(HConstants.REPLICATION_SOURCE_SERVICE_CLASSNAME,
      HConstants.REPLICATION_SERVICE_CLASSNAME_DEFAULT);

    // read in the name of the sink replication class from the config file.
    String sinkClassname = conf.get(HConstants.REPLICATION_SINK_SERVICE_CLASSNAME,
      HConstants.REPLICATION_SERVICE_CLASSNAME_DEFAULT);

    // If both the sink and the source class names are the same, then instantiate
    // only one object.
    if (sourceClassname.equals(sinkClassname)) {
      server.replicationSourceHandler = newReplicationInstance(sourceClassname,
        ReplicationSourceService.class, conf, server, walFs, walDir, oldWALDir, walProvider);
      server.replicationSinkHandler = (ReplicationSinkService) server.replicationSourceHandler;
    } else {
      server.replicationSourceHandler = newReplicationInstance(sourceClassname,
        ReplicationSourceService.class, conf, server, walFs, walDir, oldWALDir, walProvider);
      server.replicationSinkHandler = newReplicationInstance(sinkClassname,
        ReplicationSinkService.class, conf, server, walFs, walDir, oldWALDir, walProvider);
    }
  }

  private static <T extends ReplicationService> T newReplicationInstance(String classname,
      Class<T> xface, Configuration conf, HRegionServer server, FileSystem walFs, Path logDir,
      Path oldLogDir, WALProvider walProvider) throws IOException {
    Class<? extends T> clazz = null;
    try {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      clazz = Class.forName(classname, true, classLoader).asSubclass(xface);
    } catch (java.lang.ClassNotFoundException nfe) {
      throw new IOException("Could not find class for " + classname);
    }
    T service = ReflectionUtils.newInstance(clazz, conf);
    service.initialize(server, walFs, logDir, oldLogDir, walProvider);
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
      final Configuration conf2) {
    try {
      Constructor<? extends HRegionServer> c = regionServerClass
          .getConstructor(Configuration.class);
      return c.newInstance(conf2);
    } catch (Exception e) {
      throw new RuntimeException("Failed construction of " + "Regionserver: "
          + regionServerClass.toString(), e);
    }
  }

  /**
   * @see org.apache.hadoop.hbase.regionserver.HRegionServerCommandLine
   */
  public static void main(String[] args) throws Exception {
    LOG.info("STARTING executorService " + HRegionServer.class.getSimpleName());
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
  public List<HRegion> getRegions(TableName tableName) {
     List<HRegion> tableRegions = new ArrayList<>();
     synchronized (this.onlineRegions) {
       for (HRegion region: this.onlineRegions.values()) {
         RegionInfo regionInfo = region.getRegionInfo();
         if(regionInfo.getTable().equals(tableName)) {
           tableRegions.add(region);
         }
       }
     }
     return tableRegions;
   }

  @Override
  public List<HRegion> getRegions() {
    List<HRegion> allRegions = new ArrayList<>();
    synchronized (this.onlineRegions) {
      // Return a clone copy of the onlineRegions
      allRegions.addAll(onlineRegions.values());
    }
    return allRegions;
  }

  /**
   * Gets the online tables in this RS.
   * This method looks at the in-memory onlineRegions.
   * @return all the online tables in this RS
   */
  public Set<TableName> getOnlineTables() {
    Set<TableName> tables = new HashSet<>();
    synchronized (this.onlineRegions) {
      for (Region region: this.onlineRegions.values()) {
        tables.add(region.getTableDescriptor().getTableName());
      }
    }
    return tables;
  }

  // used by org/apache/hbase/tmpl/regionserver/RSStatusTmpl.jamon (HBASE-4070).
  public String[] getRegionServerCoprocessors() {
    TreeSet<String> coprocessors = new TreeSet<>();
    try {
      coprocessors.addAll(getWAL(null).getCoprocessorHost().getCoprocessors());
    } catch (IOException exception) {
      LOG.warn("Exception attempting to fetch wal coprocessor information for the common wal; " +
          "skipping.");
      LOG.debug("Exception details for failure to fetch wal coprocessor information.", exception);
    }
    Collection<HRegion> regions = getOnlineRegionsLocalContext();
    for (HRegion region: regions) {
      coprocessors.addAll(region.getCoprocessorHost().getCoprocessors());
      try {
        coprocessors.addAll(getWAL(region.getRegionInfo()).getCoprocessorHost().getCoprocessors());
      } catch (IOException exception) {
        LOG.warn("Exception attempting to fetch wal coprocessor information for region " + region +
            "; skipping.");
        LOG.debug("Exception details for failure to fetch wal coprocessor information.", exception);
      }
    }
    coprocessors.addAll(rsHost.getCoprocessors());
    return coprocessors.toArray(new String[coprocessors.size()]);
  }

  /**
   * Try to close the region, logs a warning on failure but continues.
   * @param region Region to close
   */
  private void closeRegionIgnoreErrors(RegionInfo region, final boolean abort) {
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
    HRegion actualRegion = this.getRegion(encodedName);
    // Can be null if we're calling close on a region that's not online
    if ((actualRegion != null) && (actualRegion.getCoprocessorHost() != null)) {
      try {
        actualRegion.getCoprocessorHost().preClose(false);
      } catch (IOException exp) {
        LOG.warn("Unable to close region: the coprocessor launched an error ", exp);
        return false;
      }
    }

    final Boolean previous = this.regionsInTransitionInRS.putIfAbsent(Bytes.toBytes(encodedName),
        Boolean.FALSE);

    if (Boolean.TRUE.equals(previous)) {
      LOG.info("Received CLOSE for the region:" + encodedName + " , which we are already " +
          "trying to OPEN. Cancelling OPENING.");
      if (!regionsInTransitionInRS.replace(Bytes.toBytes(encodedName), previous, Boolean.FALSE)) {
        // The replace failed. That should be an exceptional case, but theoretically it can happen.
        // We're going to try to do a standard close then.
        LOG.warn("The opening for region " + encodedName + " was done before we could cancel it." +
            " Doing a standard close now");
        return closeRegion(encodedName, abort, sn);
      }
      // Let's get the region from the online region list again
      actualRegion = this.getRegion(encodedName);
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
      this.regionsInTransitionInRS.remove(Bytes.toBytes(encodedName));
      // The master deletes the znode when it receives this exception.
      throw new NotServingRegionException("The region " + encodedName +
          " is not online, and is not opening.");
    }

    CloseRegionHandler crh;
    final RegionInfo hri = actualRegion.getRegionInfo();
    if (hri.isMetaRegion()) {
      crh = new CloseMetaHandler(this, this, hri, abort);
    } else {
      crh = new CloseRegionHandler(this, this, hri, abort, sn);
    }
    this.executorService.submit(crh);
    return true;
  }

  /**
   * Close and offline the region for split or merge
   *
   * @param regionEncodedName the name of the region(s) to close
   * @return true if closed the region successfully.
   * @throws IOException
  */
  protected boolean closeAndOfflineRegionForSplitOrMerge(final List<String> regionEncodedName)
      throws IOException {
    for (int i = 0; i < regionEncodedName.size(); ++i) {
      HRegion regionToClose = this.getRegion(regionEncodedName.get(i));
      if (regionToClose != null) {
        Map<byte[], List<HStoreFile>> hstoreFiles = null;
        Exception exceptionToThrow = null;
        try {
          hstoreFiles = regionToClose.close(false);
        } catch (Exception e) {
          exceptionToThrow = e;
        }
        if (exceptionToThrow == null && hstoreFiles == null) {
          // The region was closed by someone else
          exceptionToThrow =
            new IOException("Failed to close region: already closed by another thread");
        }
        if (exceptionToThrow != null) {
          if (exceptionToThrow instanceof IOException) {
            throw (IOException) exceptionToThrow;
          }
          throw new IOException(exceptionToThrow);
        }
        // Offline the region
        this.removeRegion(regionToClose, null);
      }
    }
    return true;
  }

   /**
   * @param regionName
   * @return HRegion for the passed binary <code>regionName</code> or null if
   *         named region is not member of the online regions.
   */
  public HRegion getOnlineRegion(final byte[] regionName) {
    String encodedRegionName = RegionInfo.encodeRegionName(regionName);
    return this.onlineRegions.get(encodedRegionName);
  }

  public InetSocketAddress[] getRegionBlockLocations(final String encodedRegionName) {
    return this.regionFavoredNodesMap.get(encodedRegionName);
  }

  @Override
  public HRegion getRegion(final String encodedRegionName) {
    return this.onlineRegions.get(encodedRegionName);
  }


  @Override
  public boolean removeRegion(final HRegion r, ServerName destination) {
    HRegion toReturn = this.onlineRegions.remove(r.getRegionInfo().getEncodedName());
    if (destination != null) {
      long closeSeqNum = r.getMaxFlushedSeqId();
      if (closeSeqNum == HConstants.NO_SEQNUM) {
        // No edits in WAL for this region; get the sequence number when the region was opened.
        closeSeqNum = r.getOpenSeqNum();
        if (closeSeqNum == HConstants.NO_SEQNUM) closeSeqNum = 0;
      }
      addToMovedRegions(r.getRegionInfo().getEncodedName(), destination, closeSeqNum);
    }
    this.regionFavoredNodesMap.remove(r.getRegionInfo().getEncodedName());
    return toReturn != null;
  }

  /**
   * Protected Utility method for safely obtaining an HRegion handle.
   *
   * @param regionName
   *          Name of online {@link HRegion} to return
   * @return {@link HRegion} for <code>regionName</code>
   * @throws NotServingRegionException
   */
  protected HRegion getRegion(final byte[] regionName)
      throws NotServingRegionException {
    String encodedRegionName = RegionInfo.encodeRegionName(regionName);
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
      throw new NotServingRegionException("" + regionNameStr +
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
      List<org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ServerName> favoredNodes) {
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
  // We add the time it was moved as we don't want to keep too old information
  protected Map<String, MovedRegionInfo> movedRegions = new ConcurrentHashMap<>(3000);

  // We need a timeout. If not there is a risk of giving a wrong information: this would double
  //  the number of network calls instead of reducing them.
  private static final int TIMEOUT_REGION_MOVED = (2 * 60 * 1000);

  protected void addToMovedRegions(String encodedName, ServerName destination, long closeSeqNum) {
    if (ServerName.isSameAddress(destination, this.getServerName())) {
      LOG.warn("Not adding moved region record: " + encodedName + " to self.");
      return;
    }
    LOG.info("Adding " + encodedName + " move to " + destination + " record at close sequenceid=" +
        closeSeqNum);
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

  /*
   * Use this to allow tests to override and schedule more frequently.
   */

  protected int movedRegionCleanerPeriod() {
        return TIMEOUT_REGION_MOVED;
  }

  /**
   * Creates a Chore thread to clean the moved region cache.
   */
  protected final static class MovedRegionsCleaner extends ScheduledChore implements Stoppable {
    private HRegionServer regionServer;
    Stoppable stoppable;

    private MovedRegionsCleaner(
      HRegionServer regionServer, Stoppable stoppable){
      super("MovedRegionsCleaner for region " + regionServer, stoppable,
          regionServer.movedRegionCleanerPeriod());
      this.regionServer = regionServer;
      this.stoppable = stoppable;
    }

    static MovedRegionsCleaner create(HRegionServer rs){
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
    return ZNodePaths.joinZNode(this.zooKeeper.getZNodePaths().rsZNode, getServerName().toString());
  }

  private boolean isHealthCheckerConfigured() {
    String healthScriptLocation = this.conf.get(HConstants.HEALTH_SCRIPT_LOC);
    return org.apache.commons.lang3.StringUtils.isNotBlank(healthScriptLocation);
  }

  /**
   * @return the underlying {@link CompactSplit} for the servers
   */
  public CompactSplit getCompactSplitThread() {
    return this.compactSplitThread;
  }

  public CoprocessorServiceResponse execRegionServerService(
      @SuppressWarnings("UnusedParameters") final RpcController controller,
      final CoprocessorServiceRequest serviceRequest) throws ServiceException {
    try {
      ServerRpcController serviceController = new ServerRpcController();
      CoprocessorServiceCall call = serviceRequest.getCall();
      String serviceName = call.getServiceName();
      com.google.protobuf.Service service = coprocessorServiceHandlers.get(serviceName);
      if (service == null) {
        throw new UnknownProtocolException(null, "No registered coprocessor executorService found for " +
            serviceName);
      }
      com.google.protobuf.Descriptors.ServiceDescriptor serviceDesc =
          service.getDescriptorForType();

      String methodName = call.getMethodName();
      com.google.protobuf.Descriptors.MethodDescriptor methodDesc =
          serviceDesc.findMethodByName(methodName);
      if (methodDesc == null) {
        throw new UnknownProtocolException(service.getClass(), "Unknown method " + methodName +
            " called on executorService " + serviceName);
      }

      com.google.protobuf.Message request =
          CoprocessorRpcUtils.getRequest(service, methodDesc, call.getRequest());
      final com.google.protobuf.Message.Builder responseBuilder =
          service.getResponsePrototype(methodDesc).newBuilderForType();
      service.callMethod(methodDesc, serviceController, request,
          new com.google.protobuf.RpcCallback<com.google.protobuf.Message>() {
        @Override
        public void run(com.google.protobuf.Message message) {
          if (message != null) {
            responseBuilder.mergeFrom(message);
          }
        }
      });
      IOException exception = CoprocessorRpcUtils.getControllerException(serviceController);
      if (exception != null) {
        throw exception;
      }
      return CoprocessorRpcUtils.getResponse(responseBuilder.build(), HConstants.EMPTY_BYTE_ARRAY);
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
   * @return Return table descriptors implementation.
   */
  public TableDescriptors getTableDescriptors() {
    return this.tableDescriptors;
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

  public CacheEvictionStats clearRegionBlockCache(Region region) {
    BlockCache blockCache = this.getCacheConfig().getBlockCache();
    long evictedBlocks = 0;

    for(Store store : region.getStores()) {
      for(StoreFile hFile : store.getStorefiles()) {
        evictedBlocks += blockCache.evictBlocksByHfileName(hFile.getPath().getName());
      }
    }

    return CacheEvictionStats.builder()
        .withEvictedBlocks(evictedBlocks)
        .build();
  }

  @Override
  public double getCompactionPressure() {
    double max = 0;
    for (Region region : onlineRegions.values()) {
      for (Store store : region.getStores()) {
        double normCount = store.getCompactionPressure();
        if (normCount > max) {
          max = normCount;
        }
      }
    }
    return max;
  }

  @Override
  public HeapMemoryManager getHeapMemoryManager() {
    return hMemManager;
  }

  /**
   * For testing
   * @return whether all wal roll request finished for this regionserver
   */
  @VisibleForTesting
  public boolean walRollRequestFinished() {
    return this.walRoller.walRollFinished();
  }

  @Override
  public ThroughputController getFlushThroughputController() {
    return flushThroughputController;
  }

  @Override
  public double getFlushPressure() {
    if (getRegionServerAccounting() == null || cacheFlusher == null) {
      // return 0 during RS initialization
      return 0.0;
    }
    return getRegionServerAccounting().getFlushPressure();
  }

  @Override
  public void onConfigurationChange(Configuration newConf) {
    ThroughputController old = this.flushThroughputController;
    if (old != null) {
      old.stop("configuration change");
    }
    this.flushThroughputController = FlushThroughputControllerFactory.create(this, newConf);
  }

  @Override
  public MetricsRegionServer getMetrics() {
    return metricsRegionServer;
  }

  @Override
  public SecureBulkLoadManager getSecureBulkLoadManager() {
    return this.secureBulkLoadManager;
  }

  @Override
  public EntityLock regionLock(List<RegionInfo> regionInfos, String description,
      Abortable abort) throws IOException {
    return new LockServiceClient(conf, lockStub, clusterConnection.getNonceGenerator())
      .regionLock(regionInfos, description, abort);
  }

  @Override
  public void unassign(byte[] regionName) throws IOException {
    clusterConnection.getAdmin().unassign(regionName, false);
  }

  @Override
  public RegionServerSpaceQuotaManager getRegionServerSpaceQuotaManager() {
    return this.rsSpaceQuotaManager;
  }

  @Override
  public boolean reportFileArchivalForQuotas(TableName tableName,
      Collection<Entry<String, Long>> archivedFiles) {
    RegionServerStatusService.BlockingInterface rss = rssStub;
    if (rss == null || rsSpaceQuotaManager == null) {
      // the current server could be stopping.
      LOG.trace("Skipping file archival reporting to HMaster as stub is null");
      return false;
    }
    try {
      RegionServerStatusProtos.FileArchiveNotificationRequest request =
          rsSpaceQuotaManager.buildFileArchiveRequest(tableName, archivedFiles);
      rss.reportFileArchival(null, request);
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof PleaseHoldException) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Failed to report file archival(s) to Master because it is initializing."
              + " This will be retried.", ioe);
        }
        // The Master is coming up. Will retry the report later. Avoid re-creating the stub.
        return false;
      }
      if (rssStub == rss) {
        rssStub = null;
      }
      // re-create the stub if we failed to report the archival
      createRegionServerStatusStub(true);
      LOG.debug("Failed to report file archival(s) to Master. This will be retried.", ioe);
      return false;
    }
    return true;
  }

  public NettyEventLoopGroupConfig getEventLoopGroupConfig() {
    return eventLoopGroupConfig;
  }

  @Override
  public Connection createConnection(Configuration conf) throws IOException {
    User user = UserProvider.instantiate(conf).getCurrent();
    return ConnectionUtils.createShortCircuitConnection(conf, null, user, this.serverName,
        this.rpcServices, this.rpcServices);
  }

  public void executeProcedure(long procId, RSProcedureCallable callable) {
    executorService.submit(new RSProcedureHandler(this, procId, callable));
  }

  public void remoteProcedureComplete(long procId, Throwable error) {
    procedureResultReporter.complete(procId, error);
  }

  void reportProcedureDone(ReportProcedureDoneRequest request) throws IOException {
    RegionServerStatusService.BlockingInterface rss = rssStub;
    for (;;) {
      rss = rssStub;
      if (rss != null) {
        break;
      }
      createRegionServerStatusStub();
    }
    try {
      rss.reportProcedureDone(null, request);
    } catch (ServiceException se) {
      if (rssStub == rss) {
        rssStub = null;
      }
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * This method modifies the region server's configuration in order to inject replication-related
   * features
   * @param conf region server configurations
   */
  static void decorateRegionServerConfiguration(Configuration conf) {
    if (ReplicationUtils.isReplicationForBulkLoadDataEnabled(conf)) {
      String plugins = conf.get(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY, "");
      String rsCoprocessorClass = ReplicationObserver.class.getCanonicalName();
      if (!plugins.contains(rsCoprocessorClass)) {
        conf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
          plugins + "," + rsCoprocessorClass);
      }
    }
  }

  public boolean isShutDown() {
    return shutDown;
  }
}
