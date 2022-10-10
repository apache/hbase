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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_SPLIT_COORDINATED_BY_ZK;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_SPLIT_WAL_MAX_SPLITTER;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_SLOW_LOG_SYS_TABLE_CHORE_DURATION;
import static org.apache.hadoop.hbase.HConstants.HBASE_SPLIT_WAL_COORDINATED_BY_ZK;
import static org.apache.hadoop.hbase.HConstants.HBASE_SPLIT_WAL_MAX_SPLITTER;
import static org.apache.hadoop.hbase.master.waleventtracker.WALEventTrackerTableCreator.WAL_EVENT_TRACKER_ENABLED_DEFAULT;
import static org.apache.hadoop.hbase.master.waleventtracker.WALEventTrackerTableCreator.WAL_EVENT_TRACKER_ENABLED_KEY;
import static org.apache.hadoop.hbase.namequeues.NamedQueueServiceChore.NAMED_QUEUE_CHORE_DURATION_DEFAULT;
import static org.apache.hadoop.hbase.namequeues.NamedQueueServiceChore.NAMED_QUEUE_CHORE_DURATION_KEY;
import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_CHORE_DURATION_DEFAULT;
import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_CHORE_DURATION_KEY;
import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_ENABLED_DEFAULT;
import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_ENABLED_KEY;
import static org.apache.hadoop.hbase.util.DNS.UNSAFE_RS_HOSTNAME_KEY;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Constructor;
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
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import javax.management.MalformedObjectNameException;
import javax.servlet.http.HttpServlet;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.CacheEvictionStats;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.ClockOutOfSyncException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ExecutorStatusChore;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HBaseServerBase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HealthCheckChore;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.ZNodeClearer;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.locking.EntityLock;
import org.apache.hadoop.hbase.client.locking.LockServiceClient;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.exceptions.RegionOpeningException;
import org.apache.hadoop.hbase.exceptions.UnknownProtocolException;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.http.InfoServer;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheFactory;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.mob.MobFileCache;
import org.apache.hadoop.hbase.mob.RSMobFileCleanerChore;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.namequeues.NamedQueueRecorder;
import org.apache.hadoop.hbase.namequeues.NamedQueueServiceChore;
import org.apache.hadoop.hbase.net.Address;
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
import org.apache.hadoop.hbase.regionserver.http.RSDumpServlet;
import org.apache.hadoop.hbase.regionserver.http.RSStatusServlet;
import org.apache.hadoop.hbase.regionserver.regionreplication.RegionReplicationBufferManager;
import org.apache.hadoop.hbase.regionserver.throttle.FlushThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALEventTrackerListener;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationLoad;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceInterface;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationStatus;
import org.apache.hadoop.hbase.security.SecurityConstants;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CompressionTest;
import org.apache.hadoop.hbase.util.CoprocessorConfigurationUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.JvmPauseMonitor;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.base.Throwables;
import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.ServiceDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.UserLoad;
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
 * HRegionServer makes a set of HRegions available to clients. It checks in with the HMaster. There
 * are many HRegionServers in a single HBase deployment.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@SuppressWarnings({ "deprecation" })
public class HRegionServer extends HBaseServerBase<RSRpcServices>
  implements RegionServerServices, LastSequenceId {

  private static final Logger LOG = LoggerFactory.getLogger(HRegionServer.class);

  /**
   * For testing only! Set to true to skip notifying region assignment to master .
   */
  @InterfaceAudience.Private
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "MS_SHOULD_BE_FINAL")
  public static boolean TEST_SKIP_REPORTING_TRANSITION = false;

  /**
   * A map from RegionName to current action in progress. Boolean value indicates: true - if open
   * region action in progress false - if close region action in progress
   */
  private final ConcurrentMap<byte[], Boolean> regionsInTransitionInRS =
    new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);

  /**
   * Used to cache the open/close region procedures which already submitted. See
   * {@link #submitRegionProcedure(long)}.
   */
  private final ConcurrentMap<Long, Long> submittedRegionProcedures = new ConcurrentHashMap<>();
  /**
   * Used to cache the open/close region procedures which already executed. See
   * {@link #submitRegionProcedure(long)}.
   */
  private final Cache<Long, Long> executedRegionProcedures =
    CacheBuilder.newBuilder().expireAfterAccess(600, TimeUnit.SECONDS).build();

  /**
   * Used to cache the moved-out regions
   */
  private final Cache<String, MovedRegionInfo> movedRegionInfoCache = CacheBuilder.newBuilder()
    .expireAfterWrite(movedRegionCacheExpiredTime(), TimeUnit.MILLISECONDS).build();

  private MemStoreFlusher cacheFlusher;

  private HeapMemoryManager hMemManager;

  // Replication services. If no replication, this handler will be null.
  private ReplicationSourceService replicationSourceHandler;
  private ReplicationSinkService replicationSinkHandler;
  private boolean sameReplicationSourceAndSink;

  // Compactions
  private CompactSplit compactSplitThread;

  /**
   * Map of regions currently being served by this region server. Key is the encoded region name.
   * All access should be synchronized.
   */
  private final Map<String, HRegion> onlineRegions = new ConcurrentHashMap<>();
  /**
   * Lock for gating access to {@link #onlineRegions}. TODO: If this map is gated by a lock, does it
   * need to be a ConcurrentHashMap?
   */
  private final ReentrantReadWriteLock onlineRegionsLock = new ReentrantReadWriteLock();

  /**
   * Map of encoded region names to the DataNode locations they should be hosted on We store the
   * value as Address since InetSocketAddress is required by the HDFS API (create() that takes
   * favored nodes as hints for placing file blocks). We could have used ServerName here as the
   * value class, but we'd need to convert it to InetSocketAddress at some point before the HDFS API
   * call, and it seems a bit weird to store ServerName since ServerName refers to RegionServers and
   * here we really mean DataNode locations. We don't store it as InetSocketAddress here because the
   * conversion on demand from Address to InetSocketAddress will guarantee the resolution results
   * will be fresh when we need it.
   */
  private final Map<String, Address[]> regionFavoredNodesMap = new ConcurrentHashMap<>();

  private LeaseManager leaseManager;

  private volatile boolean dataFsOk;

  static final String ABORT_TIMEOUT = "hbase.regionserver.abort.timeout";
  // Default abort timeout is 1200 seconds for safe
  private static final long DEFAULT_ABORT_TIMEOUT = 1200000;
  // Will run this task when abort timeout
  static final String ABORT_TIMEOUT_TASK = "hbase.regionserver.abort.timeout.task";

  // A state before we go into stopped state. At this stage we're closing user
  // space regions.
  private boolean stopping = false;
  private volatile boolean killed = false;

  private final int threadWakeFrequency;

  private static final String PERIOD_COMPACTION = "hbase.regionserver.compaction.check.period";
  private final int compactionCheckFrequency;
  private static final String PERIOD_FLUSH = "hbase.regionserver.flush.check.period";
  private final int flushCheckFrequency;

  // Stub to do region server status calls against the master.
  private volatile RegionServerStatusService.BlockingInterface rssStub;
  private volatile LockService.BlockingInterface lockStub;
  // RPC client. Used to make the stub above that does region server status checking.
  private RpcClient rpcClient;

  private UncaughtExceptionHandler uncaughtExceptionHandler;

  private JvmPauseMonitor pauseMonitor;

  private RSSnapshotVerifier rsSnapshotVerifier;

  /** region server process name */
  public static final String REGIONSERVER = "regionserver";

  private MetricsRegionServer metricsRegionServer;
  MetricsRegionServerWrapperImpl metricsRegionServerImpl;

  /**
   * Check for compactions requests.
   */
  private ScheduledChore compactionChecker;

  /**
   * Check for flushes
   */
  private ScheduledChore periodicFlusher;

  private volatile WALFactory walFactory;

  private LogRoller walRoller;

  // A thread which calls reportProcedureDone
  private RemoteProcedureResultReporter procedureResultReporter;

  // flag set after we're done setting up server threads
  final AtomicBoolean online = new AtomicBoolean(false);

  // master address tracker
  private final MasterAddressTracker masterAddressTracker;

  // Log Splitting Worker
  private SplitLogWorker splitLogWorker;

  private final int shortOperationTimeout;

  // Time to pause if master says 'please hold'
  private final long retryPauseTime;

  private final RegionServerAccounting regionServerAccounting;

  private NamedQueueServiceChore namedQueueServiceChore = null;

  // Block cache
  private BlockCache blockCache;
  // The cache for mob files
  private MobFileCache mobFileCache;

  /** The health check chore. */
  private HealthCheckChore healthCheckChore;

  /** The Executor status collect chore. */
  private ExecutorStatusChore executorStatusChore;

  /** The nonce manager chore. */
  private ScheduledChore nonceManagerChore;

  private Map<String, Service> coprocessorServiceHandlers = Maps.newHashMap();

  /**
   * @deprecated since 2.4.0 and will be removed in 4.0.0. Use
   *             {@link HRegionServer#UNSAFE_RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY} instead.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-24667">HBASE-24667</a>
   */
  @Deprecated
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
  final static String RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY =
    "hbase.regionserver.hostname.disable.master.reversedns";

  /**
   * HBASE-18226: This config and hbase.unsafe.regionserver.hostname are mutually exclusive.
   * Exception will be thrown if both are used.
   */
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
  final static String UNSAFE_RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY =
    "hbase.unsafe.regionserver.hostname.disable.master.reversedns";

  /**
   * Unique identifier for the cluster we are a part of.
   */
  private String clusterId;

  // chore for refreshing store files for secondary regions
  private StorefileRefresherChore storefileRefresher;

  private volatile RegionServerCoprocessorHost rsHost;

  private RegionServerProcedureManagerHost rspmHost;

  private RegionServerRpcQuotaManager rsQuotaManager;
  private RegionServerSpaceQuotaManager rsSpaceQuotaManager;

  /**
   * Nonce manager. Nonces are used to make operations like increment and append idempotent in the
   * case where client doesn't receive the response from a successful operation and retries. We
   * track the successful ops for some time via a nonce sent by client and handle duplicate
   * operations (currently, by failing them; in future we might use MVCC to return result). Nonces
   * are also recovered from WAL during, recovery; however, the caveats (from HBASE-3787) are: - WAL
   * recovery is optimized, and under high load we won't read nearly nonce-timeout worth of past
   * records. If we don't read the records, we don't read and recover the nonces. Some WALs within
   * nonce-timeout at recovery may not even be present due to rolling/cleanup. - There's no WAL
   * recovery during normal region move, so nonces will not be transfered. We can have separate
   * additional "Nonce WAL". It will just contain bunch of numbers and won't be flushed on main path
   * - because WAL itself also contains nonces, if we only flush it before memstore flush, for a
   * given nonce we will either see it in the WAL (if it was never flushed to disk, it will be part
   * of recovery), or we'll see it as part of the nonce log (or both occasionally, which doesn't
   * matter). Nonce log file can be deleted after the latest nonce in it expired. It can also be
   * recovered during move.
   */
  final ServerNonceManager nonceManager;

  private BrokenStoreFileCleaner brokenStoreFileCleaner;

  private RSMobFileCleanerChore rsMobFileCleanerChore;

  @InterfaceAudience.Private
  CompactedHFilesDischarger compactedFileDischarger;

  private volatile ThroughputController flushThroughputController;

  private SecureBulkLoadManager secureBulkLoadManager;

  private FileSystemUtilizationChore fsUtilizationChore;

  private BootstrapNodeManager bootstrapNodeManager;

  /**
   * True if this RegionServer is coming up in a cluster where there is no Master; means it needs to
   * just come up and make do without a Master to talk to: e.g. in test or HRegionServer is doing
   * other than its usual duties: e.g. as an hollowed-out host whose only purpose is as a
   * Replication-stream sink; see HBASE-18846 for more. TODO: can this replace
   * {@link #TEST_SKIP_REPORTING_TRANSITION} ?
   */
  private final boolean masterless;
  private static final String MASTERLESS_CONFIG_NAME = "hbase.masterless";

  /** regionserver codec list **/
  private static final String REGIONSERVER_CODEC = "hbase.regionserver.codecs";

  // A timer to shutdown the process if abort takes too long
  private Timer abortMonitor;

  private RegionReplicationBufferManager regionReplicationBufferManager;

  /*
   * Chore that creates replication marker rows.
   */
  private ReplicationMarkerChore replicationMarkerChore;

  /**
   * Starts a HRegionServer at the default location.
   * <p/>
   * Don't start any services or managers in here in the Constructor. Defer till after we register
   * with the Master as much as possible. See {@link #startServices}.
   */
  public HRegionServer(final Configuration conf) throws IOException {
    super(conf, "RegionServer"); // thread name
    final Span span = TraceUtil.createSpan("HRegionServer.cxtor");
    try (Scope ignored = span.makeCurrent()) {
      this.dataFsOk = true;
      this.masterless = !clusterMode();
      MemorySizeUtil.checkForClusterFreeHeapMemoryLimit(this.conf);
      HFile.checkHFileVersion(this.conf);
      checkCodecs(this.conf);
      FSUtils.setupShortCircuitRead(this.conf);

      // Disable usage of meta replicas in the regionserver
      this.conf.setBoolean(HConstants.USE_META_REPLICAS, false);
      // Config'ed params
      this.threadWakeFrequency = conf.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
      this.compactionCheckFrequency = conf.getInt(PERIOD_COMPACTION, this.threadWakeFrequency);
      this.flushCheckFrequency = conf.getInt(PERIOD_FLUSH, this.threadWakeFrequency);

      boolean isNoncesEnabled = conf.getBoolean(HConstants.HBASE_RS_NONCES_ENABLED, true);
      this.nonceManager = isNoncesEnabled ? new ServerNonceManager(this.conf) : null;

      this.shortOperationTimeout = conf.getInt(HConstants.HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY,
        HConstants.DEFAULT_HBASE_RPC_SHORTOPERATION_TIMEOUT);

      this.retryPauseTime = conf.getLong(HConstants.HBASE_RPC_SHORTOPERATION_RETRY_PAUSE_TIME,
        HConstants.DEFAULT_HBASE_RPC_SHORTOPERATION_RETRY_PAUSE_TIME);

      regionServerAccounting = new RegionServerAccounting(conf);

      blockCache = BlockCacheFactory.createBlockCache(conf);
      mobFileCache = new MobFileCache(conf);

      rsSnapshotVerifier = new RSSnapshotVerifier(conf);

      uncaughtExceptionHandler =
        (t, e) -> abort("Uncaught exception in executorService thread " + t.getName(), e);

      // If no master in cluster, skip trying to track one or look for a cluster status.
      if (!this.masterless) {
        masterAddressTracker = new MasterAddressTracker(getZooKeeper(), this);
        masterAddressTracker.start();
      } else {
        masterAddressTracker = null;
      }
      this.rpcServices.start(zooKeeper);
      span.setStatus(StatusCode.OK);
    } catch (Throwable t) {
      // Make sure we log the exception. HRegionServer is often started via reflection and the
      // cause of failed startup is lost.
      TraceUtil.setError(span, t);
      LOG.error("Failed construction RegionServer", t);
      throw t;
    } finally {
      span.end();
    }
  }

  // HMaster should override this method to load the specific config for master
  @Override
  protected String getUseThisHostnameInstead(Configuration conf) throws IOException {
    String hostname = conf.get(UNSAFE_RS_HOSTNAME_KEY);
    if (conf.getBoolean(UNSAFE_RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY, false)) {
      if (!StringUtils.isBlank(hostname)) {
        String msg = UNSAFE_RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY + " and "
          + UNSAFE_RS_HOSTNAME_KEY + " are mutually exclusive. Do not set "
          + UNSAFE_RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY + " to true while "
          + UNSAFE_RS_HOSTNAME_KEY + " is used";
        throw new IOException(msg);
      } else {
        return rpcServices.getSocketAddress().getHostName();
      }
    } else {
      return hostname;
    }
  }

  @Override
  protected void login(UserProvider user, String host) throws IOException {
    user.login(SecurityConstants.REGIONSERVER_KRB_KEYTAB_FILE,
      SecurityConstants.REGIONSERVER_KRB_PRINCIPAL, host);
  }

  @Override
  protected String getProcessName() {
    return REGIONSERVER;
  }

  @Override
  protected boolean canCreateBaseZNode() {
    return !clusterMode();
  }

  @Override
  protected boolean canUpdateTableDescriptor() {
    return false;
  }

  @Override
  protected boolean cacheTableDescriptor() {
    return false;
  }

  protected RSRpcServices createRpcServices() throws IOException {
    return new RSRpcServices(this);
  }

  @Override
  protected void configureInfoServer(InfoServer infoServer) {
    infoServer.addUnprivilegedServlet("rs-status", "/rs-status", RSStatusServlet.class);
    infoServer.setAttribute(REGIONSERVER, this);
  }

  @Override
  protected Class<? extends HttpServlet> getDumpServlet() {
    return RSDumpServlet.class;
  }

  /**
   * Used by {@link RSDumpServlet} to generate debugging information.
   */
  public void dumpRowLocks(final PrintWriter out) {
    StringBuilder sb = new StringBuilder();
    for (HRegion region : getRegions()) {
      if (region.getLockedRows().size() > 0) {
        for (HRegion.RowLockContext rowLockContext : region.getLockedRows().values()) {
          sb.setLength(0);
          sb.append(region.getTableDescriptor().getTableName()).append(",")
            .append(region.getRegionInfo().getEncodedName()).append(",");
          sb.append(rowLockContext.toString());
          out.println(sb);
        }
      }
    }
  }

  @Override
  public boolean registerService(Service instance) {
    // No stacking of instances is allowed for a single executorService name
    ServiceDescriptor serviceDesc = instance.getDescriptorForType();
    String serviceName = CoprocessorRpcUtils.getServiceName(serviceDesc);
    if (coprocessorServiceHandlers.containsKey(serviceName)) {
      LOG.error("Coprocessor executorService " + serviceName
        + " already registered, rejecting request from " + instance);
      return false;
    }

    coprocessorServiceHandlers.put(serviceName, instance);
    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "Registered regionserver coprocessor executorService: executorService=" + serviceName);
    }
    return true;
  }

  /**
   * Run test on configured codecs to make sure supporting libs are in place.
   */
  private static void checkCodecs(final Configuration c) throws IOException {
    // check to see if the codec list is available:
    String[] codecs = c.getStrings(REGIONSERVER_CODEC, (String[]) null);
    if (codecs == null) {
      return;
    }
    for (String codec : codecs) {
      if (!CompressionTest.testCompression(codec)) {
        throw new IOException(
          "Compression codec " + codec + " not supported, aborting RS construction");
      }
    }
  }

  public String getClusterId() {
    return this.clusterId;
  }

  /**
   * All initialization needed before we go register with Master.<br>
   * Do bare minimum. Do bulk of initializations AFTER we've connected to the Master.<br>
   * In here we just put up the RpcServer, setup Connection, and ZooKeeper.
   */
  private void preRegistrationInitialization() {
    final Span span = TraceUtil.createSpan("HRegionServer.preRegistrationInitialization");
    try (Scope ignored = span.makeCurrent()) {
      initializeZooKeeper();
      setupClusterConnection();
      bootstrapNodeManager = new BootstrapNodeManager(asyncClusterConnection, masterAddressTracker);
      regionReplicationBufferManager = new RegionReplicationBufferManager(this);
      // Setup RPC client for master communication
      this.rpcClient = asyncClusterConnection.getRpcClient();
      span.setStatus(StatusCode.OK);
    } catch (Throwable t) {
      // Call stop if error or process will stick around for ever since server
      // puts up non-daemon threads.
      TraceUtil.setError(span, t);
      this.rpcServices.stop();
      abort("Initialization of RS failed.  Hence aborting RS.", t);
    } finally {
      span.end();
    }
  }

  /**
   * Bring up connection to zk ensemble and then wait until a master for this cluster and then after
   * that, wait until cluster 'up' flag has been set. This is the order in which master does things.
   * <p>
   * Finally open long-living server short-circuit connection.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE",
      justification = "cluster Id znode read would give us correct response")
  private void initializeZooKeeper() throws IOException, InterruptedException {
    // Nothing to do in here if no Master in the mix.
    if (this.masterless) {
      return;
    }

    // Create the master address tracker, register with zk, and start it. Then
    // block until a master is available. No point in starting up if no master
    // running.
    blockAndCheckIfStopped(this.masterAddressTracker);

    // Wait on cluster being up. Master will set this flag up in zookeeper
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
   * Utilty method to wait indefinitely on a znode availability while checking if the region server
   * is shut down
   * @param tracker znode tracker to use
   * @throws IOException          any IO exception, plus if the RS is stopped
   * @throws InterruptedException if the waiting thread is interrupted
   */
  private void blockAndCheckIfStopped(ZKNodeTracker tracker)
    throws IOException, InterruptedException {
    while (tracker.blockUntilAvailable(this.msgInterval, false) == null) {
      if (this.stopped) {
        throw new IOException("Received the shutdown message while waiting.");
      }
    }
  }

  /** Returns True if the cluster is up. */
  @Override
  public boolean isClusterUp() {
    return this.masterless
      || (this.clusterStatusTracker != null && this.clusterStatusTracker.isClusterUp());
  }

  private void initializeReplicationMarkerChore() {
    boolean replicationMarkerEnabled =
      conf.getBoolean(REPLICATION_MARKER_ENABLED_KEY, REPLICATION_MARKER_ENABLED_DEFAULT);
    // If replication or replication marker is not enabled then return immediately.
    if (replicationMarkerEnabled) {
      int period = conf.getInt(REPLICATION_MARKER_CHORE_DURATION_KEY,
        REPLICATION_MARKER_CHORE_DURATION_DEFAULT);
      replicationMarkerChore = new ReplicationMarkerChore(this, this, period, conf);
    }
  }

  /**
   * The HRegionServer sticks in this loop until closed.
   */
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
      if (!isStopped() && !isAborted()) {
        installShutdownHook();
        // Initialize the RegionServerCoprocessorHost now that our ephemeral
        // node was created, in case any coprocessors want to use ZooKeeper
        this.rsHost = new RegionServerCoprocessorHost(this, this.conf);

        // Try and register with the Master; tell it we are here. Break if server is stopped or
        // the clusterup flag is down or hdfs went wacky. Once registered successfully, go ahead and
        // start up all Services. Use RetryCounter to get backoff in case Master is struggling to
        // come up.
        LOG.debug("About to register with Master.");
        TraceUtil.trace(() -> {
          RetryCounterFactory rcf =
            new RetryCounterFactory(Integer.MAX_VALUE, this.sleeper.getPeriod(), 1000 * 60 * 5);
          RetryCounter rc = rcf.create();
          while (keepLooping()) {
            RegionServerStartupResponse w = reportForDuty();
            if (w == null) {
              long sleepTime = rc.getBackoffTimeAndIncrementAttempts();
              LOG.warn("reportForDuty failed; sleeping {} ms and then retrying.", sleepTime);
              this.sleeper.sleep(sleepTime);
            } else {
              handleReportForDutyResponse(w);
              break;
            }
          }
        }, "HRegionServer.registerWithMaster");
      }

      if (!isStopped() && isHealthy()) {
        TraceUtil.trace(() -> {
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
        }, "HRegionServer.startup");
      }

      // We registered with the Master. Go into run mode.
      long lastMsg = EnvironmentEdgeManager.currentTime();
      long oldRequestCount = -1;
      // The main run loop.
      while (!isStopped() && isHealthy()) {
        if (!isClusterUp()) {
          if (onlineRegions.isEmpty()) {
            stop("Exiting; cluster shutdown set and not carrying any regions");
          } else if (!this.stopping) {
            this.stopping = true;
            LOG.info("Closing user regions");
            closeUserRegions(isAborted());
          } else {
            boolean allUserRegionsOffline = areAllUserRegionsOffline();
            if (allUserRegionsOffline) {
              // Set stopped if no more write requests tp meta tables
              // since last time we went around the loop. Any open
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
              closeUserRegions(this.abortRequested.get());
            }
            LOG.debug("Waiting on " + getOnlineRegionsAsPrintableString());
          }
        }
        long now = EnvironmentEdgeManager.currentTime();
        if ((now - lastMsg) >= msgInterval) {
          tryRegionServerReport(lastMsg, now);
          lastMsg = EnvironmentEdgeManager.currentTime();
        }
        if (!isStopped() && !isAborted()) {
          this.sleeper.sleep();
        }
      } // for
    } catch (Throwable t) {
      if (!rpcServices.checkOOME(t)) {
        String prefix = t instanceof YouAreDeadException ? "" : "Unhandled: ";
        abort(prefix + t.getMessage(), t);
      }
    }

    final Span span = TraceUtil.createSpan("HRegionServer exiting main loop");
    try (Scope ignored = span.makeCurrent()) {
      if (this.leaseManager != null) {
        this.leaseManager.closeAfterLeasesExpire();
      }
      if (this.splitLogWorker != null) {
        splitLogWorker.stop();
      }
      stopInfoServer();
      // Send cache a shutdown.
      if (blockCache != null) {
        blockCache.shutdown();
      }
      if (mobFileCache != null) {
        mobFileCache.shutdown();
      }

      // Send interrupts to wake up threads if sleeping so they notice shutdown.
      // TODO: Should we check they are alive? If OOME could have exited already
      if (this.hMemManager != null) {
        this.hMemManager.stop();
      }
      if (this.cacheFlusher != null) {
        this.cacheFlusher.interruptIfNecessary();
      }
      if (this.compactSplitThread != null) {
        this.compactSplitThread.interruptIfNecessary();
      }

      // Stop the snapshot and other procedure handlers, forcefully killing all running tasks
      if (rspmHost != null) {
        rspmHost.stop(this.abortRequested.get() || this.killed);
      }

      if (this.killed) {
        // Just skip out w/o closing regions. Used when testing.
      } else if (abortRequested.get()) {
        if (this.dataFsOk) {
          closeUserRegions(abortRequested.get()); // Don't leave any open file handles
        }
        LOG.info("aborting server " + this.serverName);
      } else {
        closeUserRegions(abortRequested.get());
        LOG.info("stopping server " + this.serverName);
      }
      regionReplicationBufferManager.stop();
      closeClusterConnection();
      // Closing the compactSplit thread before closing meta regions
      if (!this.killed && containsMetaTableRegions()) {
        if (!abortRequested.get() || this.dataFsOk) {
          if (this.compactSplitThread != null) {
            this.compactSplitThread.join();
            this.compactSplitThread = null;
          }
          closeMetaTableRegions(abortRequested.get());
        }
      }

      if (!this.killed && this.dataFsOk) {
        waitOnAllRegionsToClose(abortRequested.get());
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

      // flag may be changed when closing regions throws exception.
      if (this.dataFsOk) {
        shutdownWAL(!abortRequested.get());
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
      if (this.leaseManager != null) {
        this.leaseManager.close();
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
        // pass
      } catch (KeeperException e) {
        LOG.warn("Failed deleting my ephemeral node", e);
      }
      // We may have failed to delete the znode at the previous step, but
      // we delete the file anyway: a second attempt to delete the znode is likely to fail again.
      ZNodeClearer.deleteMyEphemeralNodeOnDisk();

      closeZooKeeper();
      closeTableDescriptors();
      LOG.info("Exiting; stopping=" + this.serverName + "; zookeeper connection closed.");
      span.setStatus(StatusCode.OK);
    } finally {
      span.end();
    }
  }

  private boolean containsMetaTableRegions() {
    return onlineRegions.containsKey(RegionInfoBuilder.FIRST_META_REGIONINFO.getEncodedName());
  }

  private boolean areAllUserRegionsOffline() {
    if (getNumberOfOnlineRegions() > 2) {
      return false;
    }
    boolean allUserRegionsOffline = true;
    for (Map.Entry<String, HRegion> e : this.onlineRegions.entrySet()) {
      if (!e.getValue().getRegionInfo().isMetaRegion()) {
        allUserRegionsOffline = false;
        break;
      }
    }
    return allUserRegionsOffline;
  }

  /** Returns Current write count for all online regions. */
  private long getWriteRequestCount() {
    long writeCount = 0;
    for (Map.Entry<String, HRegion> e : this.onlineRegions.entrySet()) {
      writeCount += e.getValue().getWriteRequestsCount();
    }
    return writeCount;
  }

  @InterfaceAudience.Private
  protected void tryRegionServerReport(long reportStartTime, long reportEndTime)
    throws IOException {
    RegionServerStatusService.BlockingInterface rss = rssStub;
    if (rss == null) {
      // the current server could be stopping.
      return;
    }
    ClusterStatusProtos.ServerLoad sl = buildServerLoad(reportStartTime, reportEndTime);
    final Span span = TraceUtil.createSpan("HRegionServer.tryRegionServerReport");
    try (Scope ignored = span.makeCurrent()) {
      RegionServerReportRequest.Builder request = RegionServerReportRequest.newBuilder();
      request.setServer(ProtobufUtil.toServerName(this.serverName));
      request.setLoad(sl);
      rss.regionServerReport(null, request.build());
      span.setStatus(StatusCode.OK);
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof YouAreDeadException) {
        // This will be caught and handled as a fatal error in run()
        TraceUtil.setError(span, ioe);
        throw ioe;
      }
      if (rssStub == rss) {
        rssStub = null;
      }
      TraceUtil.setError(span, se);
      // Couldn't connect to the master, get location from zk and reconnect
      // Method blocks until new master is found or we are stopped
      createRegionServerStatusStub(true);
    } finally {
      span.end();
    }
  }

  /**
   * Reports the given map of Regions and their size on the filesystem to the active Master.
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
   * @param rss             The stub to send to the Master
   * @param regionSizeStore The store containing region sizes
   */
  private void buildReportAndSend(RegionServerStatusService.BlockingInterface rss,
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
   * @param regionSizes The size in bytes of regions
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
   * Converts a pair of {@link RegionInfo} and {@code long} into a {@link RegionSpaceUse} protobuf
   * message.
   * @param regionInfo  The RegionInfo
   * @param sizeInBytes The size in bytes of the Region
   * @return The protocol buffer
   */
  RegionSpaceUse convertRegionSize(RegionInfo regionInfo, Long sizeInBytes) {
    return RegionSpaceUse.newBuilder()
      .setRegionInfo(ProtobufUtil.toRegionInfo(Objects.requireNonNull(regionInfo)))
      .setRegionSize(Objects.requireNonNull(sizeInBytes)).build();
  }

  private ClusterStatusProtos.ServerLoad buildServerLoad(long reportStartTime, long reportEndTime)
    throws IOException {
    // We're getting the MetricsRegionServerWrapper here because the wrapper computes requests
    // per second, and other metrics As long as metrics are part of ServerLoad it's best to use
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
    serverLoad.setUsedHeapMB((int) (usedMemory / 1024 / 1024));
    serverLoad.setMaxHeapMB((int) (maxMemory / 1024 / 1024));
    serverLoad.setReadRequestsCount(this.metricsRegionServerImpl.getReadRequestsCount());
    serverLoad.setWriteRequestsCount(this.metricsRegionServerImpl.getWriteRequestsCount());
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
        for (String regionCoprocessor : regionCoprocessors) {
          serverLoad.addCoprocessors(coprocessorBuilder.setName(regionCoprocessor).build());
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
    MetricsUserAggregateSource userSource =
      metricsRegionServer.getMetricsUserAggregate().getSource();
    if (userSource != null) {
      Map<String, MetricsUserSource> userMetricMap = userSource.getUserSources();
      for (Entry<String, MetricsUserSource> entry : userMetricMap.entrySet()) {
        serverLoad.addUserLoads(createUserLoad(entry.getKey(), entry.getValue()));
      }
    }

    if (sameReplicationSourceAndSink && replicationSourceHandler != null) {
      // always refresh first to get the latest value
      ReplicationLoad rLoad = replicationSourceHandler.refreshAndGetReplicationLoad();
      if (rLoad != null) {
        serverLoad.setReplLoadSink(rLoad.getReplicationLoadSink());
        for (ClusterStatusProtos.ReplicationLoadSource rLS : rLoad
          .getReplicationLoadSourceEntries()) {
          serverLoad.addReplLoadSource(rLS);
        }
      }
    } else {
      if (replicationSourceHandler != null) {
        ReplicationLoad rLoad = replicationSourceHandler.refreshAndGetReplicationLoad();
        if (rLoad != null) {
          for (ClusterStatusProtos.ReplicationLoadSource rLS : rLoad
            .getReplicationLoadSourceEntries()) {
            serverLoad.addReplLoadSource(rLS);
          }
        }
      }
      if (replicationSinkHandler != null) {
        ReplicationLoad rLoad = replicationSinkHandler.refreshAndGetReplicationLoad();
        if (rLoad != null) {
          serverLoad.setReplLoadSink(rLoad.getReplicationLoadSink());
        }
      }
    }

    TaskMonitor.get().getTasks().forEach(task -> serverLoad.addTasks(ClusterStatusProtos.ServerTask
      .newBuilder().setDescription(task.getDescription())
      .setStatus(task.getStatus() != null ? task.getStatus() : "")
      .setState(ClusterStatusProtos.ServerTask.State.valueOf(task.getState().name()))
      .setStartTime(task.getStartTime()).setCompletionTime(task.getCompletionTimestamp()).build()));

    return serverLoad.build();
  }

  private String getOnlineRegionsAsPrintableString() {
    StringBuilder sb = new StringBuilder();
    for (Region r : this.onlineRegions.values()) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
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
      while (!onlineRegions.isEmpty()) {
        int count = getNumberOfOnlineRegions();
        // Only print a message if the count of regions has changed.
        if (count != lastCount) {
          // Log every second at most
          if (EnvironmentEdgeManager.currentTime() > (previousLogTime + 1000)) {
            previousLogTime = EnvironmentEdgeManager.currentTime();
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
          if (
            !this.regionsInTransitionInRS.containsKey(hri.getEncodedNameAsBytes())
              && !closedRegions.contains(hri.getEncodedName())
          ) {
            closedRegions.add(hri.getEncodedName());
            // Don't update zk with this close transition; pass false.
            closeRegionIgnoreErrors(hri, abort);
          }
        }
        // No regions in RIT, we could stop waiting now.
        if (this.regionsInTransitionInRS.isEmpty()) {
          if (!onlineRegions.isEmpty()) {
            LOG.info("We were exiting though online regions are not empty,"
              + " because some regions failed closing");
          }
          break;
        } else {
          LOG.debug("Waiting on {}", this.regionsInTransitionInRS.keySet().stream()
            .map(e -> Bytes.toString(e)).collect(Collectors.joining(", ")));
        }
        if (sleepInterrupted(200)) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
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

  /**
   * Run init. Sets up wal and starts up all server threads.
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
          this.serverName = ServerName.valueOf(hostnameFromMasterPOV,
            rpcServices.getSocketAddress().getPort(), this.startcode);
          if (
            !StringUtils.isBlank(useThisHostnameInstead)
              && !hostnameFromMasterPOV.equals(useThisHostnameInstead)
          ) {
            String msg = "Master passed us a different hostname to use; was="
              + this.useThisHostnameInstead + ", but now=" + hostnameFromMasterPOV;
            LOG.error(msg);
            throw new IOException(msg);
          }
          if (
            StringUtils.isBlank(useThisHostnameInstead)
              && !hostnameFromMasterPOV.equals(rpcServices.getSocketAddress().getHostName())
          ) {
            String msg = "Master passed us a different hostname to use; was="
              + rpcServices.getSocketAddress().getHostName() + ", but now=" + hostnameFromMasterPOV;
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

      // hack! Maps DFSClient => RegionServer for logs. HDFS made this
      // config param for task trackers, but we can piggyback off of it.
      if (this.conf.get("mapreduce.task.attempt.id") == null) {
        this.conf.set("mapreduce.task.attempt.id", "hb_rs_" + this.serverName.toString());
      }

      // Save it in a file, this will allow to see if we crash
      ZNodeClearer.writeMyEphemeralNodeOnDisk(getMyEphemeralNodePath());

      // This call sets up an initialized replication and WAL. Later we start it up.
      setupWALAndReplication();
      // Init in here rather than in constructor after thread name has been set
      final MetricsTable metricsTable =
        new MetricsTable(new MetricsTableWrapperAggregateImpl(this));
      this.metricsRegionServerImpl = new MetricsRegionServerWrapperImpl(this);
      this.metricsRegionServer =
        new MetricsRegionServer(metricsRegionServerImpl, conf, metricsTable);
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
      LOG.info("Serving as " + this.serverName + ", RpcServer on " + rpcServices.getSocketAddress()
        + ", sessionid=0x"
        + Long.toHexString(this.zooKeeper.getRecoverableZooKeeper().getSessionId()));

      // Wake up anyone waiting for this server to online
      synchronized (online) {
        online.set(true);
        online.notifyAll();
      }
    } catch (Throwable e) {
      stop("Failed initialization");
      throw convertThrowableToIOE(cleanup(e, "Failed init"), "Region server startup failed");
    } finally {
      sleeper.skipSleepCycle();
    }
  }

  private void startHeapMemoryManager() {
    if (this.blockCache != null) {
      this.hMemManager =
        new HeapMemoryManager(this.blockCache, this.cacheFlusher, this, regionServerAccounting);
      this.hMemManager.start(getChoreService());
    }
  }

  private void createMyEphemeralNode() throws KeeperException {
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

  // Round the size with KB or MB.
  // A trick here is that if the sizeInBytes is less than sizeUnit, we will round the size to 1
  // instead of 0 if it is not 0, to avoid some schedulers think the region has no data. See
  // HBASE-26340 for more details on why this is important.
  private static int roundSize(long sizeInByte, int sizeUnit) {
    if (sizeInByte == 0) {
      return 0;
    } else if (sizeInByte < sizeUnit) {
      return 1;
    } else {
      return (int) Math.min(sizeInByte / sizeUnit, Integer.MAX_VALUE);
    }
  }

  /**
   * @param r               Region to get RegionLoad for.
   * @param regionLoadBldr  the RegionLoad.Builder, can be null
   * @param regionSpecifier the RegionSpecifier.Builder, can be null
   * @return RegionLoad instance.
   */
  RegionLoad createRegionLoad(final HRegion r, RegionLoad.Builder regionLoadBldr,
    RegionSpecifier.Builder regionSpecifier) throws IOException {
    byte[] name = r.getRegionInfo().getRegionName();
    int stores = 0;
    int storefiles = 0;
    int storeRefCount = 0;
    int maxCompactedStoreFileRefCount = 0;
    long storeUncompressedSize = 0L;
    long storefileSize = 0L;
    long storefileIndexSize = 0L;
    long rootLevelIndexSize = 0L;
    long totalStaticIndexSize = 0L;
    long totalStaticBloomSize = 0L;
    long totalCompactingKVs = 0L;
    long currentCompactedKVs = 0L;
    List<HStore> storeList = r.getStores();
    stores += storeList.size();
    for (HStore store : storeList) {
      storefiles += store.getStorefilesCount();
      int currentStoreRefCount = store.getStoreRefCount();
      storeRefCount += currentStoreRefCount;
      int currentMaxCompactedStoreFileRefCount = store.getMaxCompactedStoreFileRefCount();
      maxCompactedStoreFileRefCount =
        Math.max(maxCompactedStoreFileRefCount, currentMaxCompactedStoreFileRefCount);
      storeUncompressedSize += store.getStoreSizeUncompressed();
      storefileSize += store.getStorefilesSize();
      // TODO: storefileIndexSizeKB is same with rootLevelIndexSizeKB?
      storefileIndexSize += store.getStorefilesRootLevelIndexSize();
      CompactionProgress progress = store.getCompactionProgress();
      if (progress != null) {
        totalCompactingKVs += progress.getTotalCompactingKVs();
        currentCompactedKVs += progress.currentCompactedKVs;
      }
      rootLevelIndexSize += store.getStorefilesRootLevelIndexSize();
      totalStaticIndexSize += store.getTotalStaticIndexSize();
      totalStaticBloomSize += store.getTotalStaticBloomSize();
    }

    int unitMB = 1024 * 1024;
    int unitKB = 1024;

    int memstoreSizeMB = roundSize(r.getMemStoreDataSize(), unitMB);
    int storeUncompressedSizeMB = roundSize(storeUncompressedSize, unitMB);
    int storefileSizeMB = roundSize(storefileSize, unitMB);
    int storefileIndexSizeKB = roundSize(storefileIndexSize, unitKB);
    int rootLevelIndexSizeKB = roundSize(rootLevelIndexSize, unitKB);
    int totalStaticIndexSizeKB = roundSize(totalStaticIndexSize, unitKB);
    int totalStaticBloomSizeKB = roundSize(totalStaticBloomSize, unitKB);

    HDFSBlocksDistribution hdfsBd = r.getHDFSBlocksDistribution();
    float dataLocality = hdfsBd.getBlockLocalityIndex(serverName.getHostname());
    float dataLocalityForSsd = hdfsBd.getBlockLocalityIndexForSsd(serverName.getHostname());
    long blocksTotalWeight = hdfsBd.getUniqueBlocksTotalWeight();
    long blocksLocalWeight = hdfsBd.getBlocksLocalWeight(serverName.getHostname());
    long blocksLocalWithSsdWeight = hdfsBd.getBlocksLocalWithSsdWeight(serverName.getHostname());
    if (regionLoadBldr == null) {
      regionLoadBldr = RegionLoad.newBuilder();
    }
    if (regionSpecifier == null) {
      regionSpecifier = RegionSpecifier.newBuilder();
    }

    regionSpecifier.setType(RegionSpecifierType.REGION_NAME);
    regionSpecifier.setValue(UnsafeByteOperations.unsafeWrap(name));
    regionLoadBldr.setRegionSpecifier(regionSpecifier.build()).setStores(stores)
      .setStorefiles(storefiles).setStoreRefCount(storeRefCount)
      .setMaxCompactedStoreFileRefCount(maxCompactedStoreFileRefCount)
      .setStoreUncompressedSizeMB(storeUncompressedSizeMB).setStorefileSizeMB(storefileSizeMB)
      .setMemStoreSizeMB(memstoreSizeMB).setStorefileIndexSizeKB(storefileIndexSizeKB)
      .setRootIndexSizeKB(rootLevelIndexSizeKB).setTotalStaticIndexSizeKB(totalStaticIndexSizeKB)
      .setTotalStaticBloomSizeKB(totalStaticBloomSizeKB)
      .setReadRequestsCount(r.getReadRequestsCount()).setCpRequestsCount(r.getCpRequestsCount())
      .setFilteredReadRequestsCount(r.getFilteredReadRequestsCount())
      .setWriteRequestsCount(r.getWriteRequestsCount()).setTotalCompactingKVs(totalCompactingKVs)
      .setCurrentCompactedKVs(currentCompactedKVs).setDataLocality(dataLocality)
      .setDataLocalityForSsd(dataLocalityForSsd).setBlocksLocalWeight(blocksLocalWeight)
      .setBlocksLocalWithSsdWeight(blocksLocalWithSsdWeight).setBlocksTotalWeight(blocksTotalWeight)
      .setCompactionState(ProtobufUtil.createCompactionStateForRegionLoad(r.getCompactionState()))
      .setLastMajorCompactionTs(r.getOldestHfileTs(true));
    r.setCompleteSequenceId(regionLoadBldr);
    return regionLoadBldr.build();
  }

  private UserLoad createUserLoad(String user, MetricsUserSource userSource) {
    UserLoad.Builder userLoadBldr = UserLoad.newBuilder();
    userLoadBldr.setUserName(user);
    userSource.getClientMetrics().values().stream()
      .map(clientMetrics -> ClusterStatusProtos.ClientMetrics.newBuilder()
        .setHostName(clientMetrics.getHostName())
        .setWriteRequestsCount(clientMetrics.getWriteRequestsCount())
        .setFilteredRequestsCount(clientMetrics.getFilteredReadRequests())
        .setReadRequestsCount(clientMetrics.getReadRequestsCount()).build())
      .forEach(userLoadBldr::addClientMetrics);
    return userLoadBldr.build();
  }

  public RegionLoad createRegionLoad(final String encodedRegionName) throws IOException {
    HRegion r = onlineRegions.get(encodedRegionName);
    return r != null ? createRegionLoad(r, null, null) : null;
  }

  /**
   * Inner class that runs on a long period checking if regions need compaction.
   */
  private static class CompactionChecker extends ScheduledChore {
    private final HRegionServer instance;
    private final int majorCompactPriority;
    private final static int DEFAULT_PRIORITY = Integer.MAX_VALUE;
    // Iteration is 1-based rather than 0-based so we don't check for compaction
    // immediately upon region server startup
    private long iteration = 1;

    CompactionChecker(final HRegionServer h, final int sleepTime, final Stoppable stopper) {
      super("CompactionChecker", stopper, sleepTime);
      this.instance = h;
      LOG.info(this.getName() + " runs every " + Duration.ofMillis(sleepTime));

      /*
       * MajorCompactPriority is configurable. If not set, the compaction will use default priority.
       */
      this.majorCompactPriority = this.instance.conf
        .getInt("hbase.regionserver.compactionChecker.majorCompactPriority", DEFAULT_PRIORITY);
    }

    @Override
    protected void chore() {
      for (Region r : this.instance.onlineRegions.values()) {
        // Skip compaction if region is read only
        if (r == null || r.isReadOnly()) {
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
              if (
                majorCompactPriority == DEFAULT_PRIORITY
                  || majorCompactPriority > hr.getCompactPriority()
              ) {
                this.instance.compactSplitThread.requestCompaction(hr, s,
                  getName() + " requests major compaction; use default priority", Store.NO_PRIORITY,
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

  private static class PeriodicMemStoreFlusher extends ScheduledChore {
    private final HRegionServer server;
    private final static int RANGE_OF_DELAY = 5 * 60; // 5 min in seconds
    private final static int MIN_DELAY_TIME = 0; // millisec
    private final long rangeOfDelayMs;

    PeriodicMemStoreFlusher(int cacheFlushInterval, final HRegionServer server) {
      super("MemstoreFlusherChore", server, cacheFlushInterval);
      this.server = server;

      final long configuredRangeOfDelay = server.getConfiguration()
        .getInt("hbase.regionserver.periodicmemstoreflusher.rangeofdelayseconds", RANGE_OF_DELAY);
      this.rangeOfDelayMs = TimeUnit.SECONDS.toMillis(configuredRangeOfDelay);
    }

    @Override
    protected void chore() {
      final StringBuilder whyFlush = new StringBuilder();
      for (HRegion r : this.server.onlineRegions.values()) {
        if (r == null) {
          continue;
        }
        if (r.shouldFlush(whyFlush)) {
          FlushRequester requester = server.getFlushRequester();
          if (requester != null) {
            long delay = ThreadLocalRandom.current().nextLong(rangeOfDelayMs) + MIN_DELAY_TIME;
            // Throttle the flushes by putting a delay. If we don't throttle, and there
            // is a balanced write-load on the regions in a table, we might end up
            // overwhelming the filesystem with too many flushes at once.
            if (requester.requestDelayedFlush(r, delay)) {
              LOG.info("{} requesting flush of {} because {} after random delay {} ms", getName(),
                r.getRegionInfo().getRegionNameAsString(), whyFlush.toString(), delay);
            }
          }
        }
      }
    }
  }

  /**
   * Report the status of the server. A server is online once all the startup is completed (setting
   * up filesystem, starting executorService threads, etc.). This method is designed mostly to be
   * useful in tests.
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
    WALFactory factory = new WALFactory(conf, serverName.toString(), this, true);
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
    createNewReplicationInstance(conf, this, this.walFs, logDir, oldLogDir, factory);

    WALActionsListener walEventListener = getWALEventTrackerListener(conf);
    if (walEventListener != null && factory.getWALProvider() != null) {
      factory.getWALProvider().addWALActionsListener(walEventListener);
    }
    this.walFactory = factory;
  }

  private WALActionsListener getWALEventTrackerListener(Configuration conf) {
    if (conf.getBoolean(WAL_EVENT_TRACKER_ENABLED_KEY, WAL_EVENT_TRACKER_ENABLED_DEFAULT)) {
      WALEventTrackerListener listener =
        new WALEventTrackerListener(conf, getNamedQueueRecorder(), getServerName());
      return listener;
    }
    return null;
  }

  /**
   * Start up replication source and sink handlers.
   */
  private void startReplicationService() throws IOException {
    if (sameReplicationSourceAndSink && this.replicationSourceHandler != null) {
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

  /** Returns Master address tracker instance. */
  public MasterAddressTracker getMasterAddressTracker() {
    return this.masterAddressTracker;
  }

  /**
   * Start maintenance Threads, Server, Worker and lease checker threads. Start all threads we need
   * to run. This is called after we've successfully registered with the Master. Install an
   * UncaughtExceptionHandler that calls abort of RegionServer if we get an unhandled exception. We
   * cannot set the handler on all threads. Server's internal Listener thread is off limits. For
   * Server, if an OOME, it waits a while then retries. Meantime, a flush or a compaction that tries
   * to run should trigger same critical condition and the shutdown will run. On its way out, this
   * server will shut down Server. Leases are sort of inbetween. It has an internal thread that
   * while it inherits from Chore, it keeps its own internal stop mechanism so needs to be stopped
   * by this hosting server. Worker logs the exception and exits.
   */
  private void startServices() throws IOException {
    if (!isStopped() && !isAborted()) {
      initializeThreads();
    }
    this.secureBulkLoadManager = new SecureBulkLoadManager(this.conf, asyncClusterConnection);
    this.secureBulkLoadManager.start();

    // Health checker thread.
    if (isHealthCheckerConfigured()) {
      int sleepTime = this.conf.getInt(HConstants.HEALTH_CHORE_WAKE_FREQ,
        HConstants.DEFAULT_THREAD_WAKE_FREQUENCY);
      healthCheckChore = new HealthCheckChore(sleepTime, this, getConfiguration());
    }
    // Executor status collect thread.
    if (
      this.conf.getBoolean(HConstants.EXECUTOR_STATUS_COLLECT_ENABLED,
        HConstants.DEFAULT_EXECUTOR_STATUS_COLLECT_ENABLED)
    ) {
      int sleepTime =
        this.conf.getInt(ExecutorStatusChore.WAKE_FREQ, ExecutorStatusChore.DEFAULT_WAKE_FREQ);
      executorStatusChore = new ExecutorStatusChore(sleepTime, this, this.getExecutorService(),
        this.metricsRegionServer.getMetricsSource());
    }

    this.walRoller = new LogRoller(this);
    this.flushThroughputController = FlushThroughputControllerFactory.create(this, conf);
    this.procedureResultReporter = new RemoteProcedureResultReporter(this);

    // Create the CompactedFileDischarger chore executorService. This chore helps to
    // remove the compacted files that will no longer be used in reads.
    // Default is 2 mins. The default value for TTLCleaner is 5 mins so we set this to
    // 2 mins so that compacted files can be archived before the TTLCleaner runs
    int cleanerInterval = conf.getInt("hbase.hfile.compaction.discharger.interval", 2 * 60 * 1000);
    this.compactedFileDischarger = new CompactedHFilesDischarger(cleanerInterval, this, this);
    choreService.scheduleChore(compactedFileDischarger);

    // Start executor services
    final int openRegionThreads = conf.getInt("hbase.regionserver.executor.openregion.threads", 3);
    executorService.startExecutorService(executorService.new ExecutorConfig()
      .setExecutorType(ExecutorType.RS_OPEN_REGION).setCorePoolSize(openRegionThreads));
    final int openMetaThreads = conf.getInt("hbase.regionserver.executor.openmeta.threads", 1);
    executorService.startExecutorService(executorService.new ExecutorConfig()
      .setExecutorType(ExecutorType.RS_OPEN_META).setCorePoolSize(openMetaThreads));
    final int openPriorityRegionThreads =
      conf.getInt("hbase.regionserver.executor.openpriorityregion.threads", 3);
    executorService.startExecutorService(
      executorService.new ExecutorConfig().setExecutorType(ExecutorType.RS_OPEN_PRIORITY_REGION)
        .setCorePoolSize(openPriorityRegionThreads));
    final int closeRegionThreads =
      conf.getInt("hbase.regionserver.executor.closeregion.threads", 3);
    executorService.startExecutorService(executorService.new ExecutorConfig()
      .setExecutorType(ExecutorType.RS_CLOSE_REGION).setCorePoolSize(closeRegionThreads));
    final int closeMetaThreads = conf.getInt("hbase.regionserver.executor.closemeta.threads", 1);
    executorService.startExecutorService(executorService.new ExecutorConfig()
      .setExecutorType(ExecutorType.RS_CLOSE_META).setCorePoolSize(closeMetaThreads));
    if (conf.getBoolean(StoreScanner.STORESCANNER_PARALLEL_SEEK_ENABLE, false)) {
      final int storeScannerParallelSeekThreads =
        conf.getInt("hbase.storescanner.parallel.seek.threads", 10);
      executorService.startExecutorService(
        executorService.new ExecutorConfig().setExecutorType(ExecutorType.RS_PARALLEL_SEEK)
          .setCorePoolSize(storeScannerParallelSeekThreads).setAllowCoreThreadTimeout(true));
    }
    final int logReplayOpsThreads =
      conf.getInt(HBASE_SPLIT_WAL_MAX_SPLITTER, DEFAULT_HBASE_SPLIT_WAL_MAX_SPLITTER);
    executorService.startExecutorService(
      executorService.new ExecutorConfig().setExecutorType(ExecutorType.RS_LOG_REPLAY_OPS)
        .setCorePoolSize(logReplayOpsThreads).setAllowCoreThreadTimeout(true));
    // Start the threads for compacted files discharger
    final int compactionDischargerThreads =
      conf.getInt(CompactionConfiguration.HBASE_HFILE_COMPACTION_DISCHARGER_THREAD_COUNT, 10);
    executorService.startExecutorService(executorService.new ExecutorConfig()
      .setExecutorType(ExecutorType.RS_COMPACTED_FILES_DISCHARGER)
      .setCorePoolSize(compactionDischargerThreads));
    if (ServerRegionReplicaUtil.isRegionReplicaWaitForPrimaryFlushEnabled(conf)) {
      final int regionReplicaFlushThreads =
        conf.getInt("hbase.regionserver.region.replica.flusher.threads",
          conf.getInt("hbase.regionserver.executor.openregion.threads", 3));
      executorService.startExecutorService(executorService.new ExecutorConfig()
        .setExecutorType(ExecutorType.RS_REGION_REPLICA_FLUSH_OPS)
        .setCorePoolSize(regionReplicaFlushThreads));
    }
    final int refreshPeerThreads =
      conf.getInt("hbase.regionserver.executor.refresh.peer.threads", 2);
    executorService.startExecutorService(executorService.new ExecutorConfig()
      .setExecutorType(ExecutorType.RS_REFRESH_PEER).setCorePoolSize(refreshPeerThreads));
    final int replaySyncReplicationWALThreads =
      conf.getInt("hbase.regionserver.executor.replay.sync.replication.wal.threads", 1);
    executorService.startExecutorService(executorService.new ExecutorConfig()
      .setExecutorType(ExecutorType.RS_REPLAY_SYNC_REPLICATION_WAL)
      .setCorePoolSize(replaySyncReplicationWALThreads));
    final int switchRpcThrottleThreads =
      conf.getInt("hbase.regionserver.executor.switch.rpc.throttle.threads", 1);
    executorService.startExecutorService(
      executorService.new ExecutorConfig().setExecutorType(ExecutorType.RS_SWITCH_RPC_THROTTLE)
        .setCorePoolSize(switchRpcThrottleThreads));
    final int claimReplicationQueueThreads =
      conf.getInt("hbase.regionserver.executor.claim.replication.queue.threads", 1);
    executorService.startExecutorService(
      executorService.new ExecutorConfig().setExecutorType(ExecutorType.RS_CLAIM_REPLICATION_QUEUE)
        .setCorePoolSize(claimReplicationQueueThreads));
    final int rsSnapshotOperationThreads =
      conf.getInt("hbase.regionserver.executor.snapshot.operations.threads", 3);
    executorService.startExecutorService(
      executorService.new ExecutorConfig().setExecutorType(ExecutorType.RS_SNAPSHOT_OPERATIONS)
        .setCorePoolSize(rsSnapshotOperationThreads));

    Threads.setDaemonThreadRunning(this.walRoller, getName() + ".logRoller",
      uncaughtExceptionHandler);
    if (this.cacheFlusher != null) {
      this.cacheFlusher.start(uncaughtExceptionHandler);
    }
    Threads.setDaemonThreadRunning(this.procedureResultReporter,
      getName() + ".procedureResultReporter", uncaughtExceptionHandler);

    if (this.compactionChecker != null) {
      choreService.scheduleChore(compactionChecker);
    }
    if (this.periodicFlusher != null) {
      choreService.scheduleChore(periodicFlusher);
    }
    if (this.healthCheckChore != null) {
      choreService.scheduleChore(healthCheckChore);
    }
    if (this.executorStatusChore != null) {
      choreService.scheduleChore(executorStatusChore);
    }
    if (this.nonceManagerChore != null) {
      choreService.scheduleChore(nonceManagerChore);
    }
    if (this.storefileRefresher != null) {
      choreService.scheduleChore(storefileRefresher);
    }
    if (this.fsUtilizationChore != null) {
      choreService.scheduleChore(fsUtilizationChore);
    }
    if (this.namedQueueServiceChore != null) {
      choreService.scheduleChore(namedQueueServiceChore);
    }
    if (this.brokenStoreFileCleaner != null) {
      choreService.scheduleChore(brokenStoreFileCleaner);
    }
    if (this.rsMobFileCleanerChore != null) {
      choreService.scheduleChore(rsMobFileCleanerChore);
    }
    if (replicationMarkerChore != null) {
      LOG.info("Starting replication marker chore");
      choreService.scheduleChore(replicationMarkerChore);
    }

    // Leases is not a Thread. Internally it runs a daemon thread. If it gets
    // an unhandled exception, it will just exit.
    Threads.setDaemonThreadRunning(this.leaseManager, getName() + ".leaseChecker",
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
    if (
      this.csm != null
        && conf.getBoolean(HBASE_SPLIT_WAL_COORDINATED_BY_ZK, DEFAULT_HBASE_SPLIT_COORDINATED_BY_ZK)
    ) {
      // SplitLogWorker needs csm. If none, don't start this.
      this.splitLogWorker = new SplitLogWorker(sinkConf, this, this, walFactory);
      splitLogWorker.start();
      LOG.debug("SplitLogWorker started");
    }

    // Memstore services.
    startHeapMemoryManager();
    // Call it after starting HeapMemoryManager.
    initializeMemStoreChunkCreator(hMemManager);
  }

  private void initializeThreads() {
    // Cache flushing thread.
    this.cacheFlusher = new MemStoreFlusher(conf, this);

    // Compaction thread
    this.compactSplitThread = new CompactSplit(this);

    // Background thread to check for compactions; needed if region has not gotten updates
    // in a while. It will take care of not checking too frequently on store-by-store basis.
    this.compactionChecker = new CompactionChecker(this, this.compactionCheckFrequency, this);
    this.periodicFlusher = new PeriodicMemStoreFlusher(this.flushCheckFrequency, this);
    this.leaseManager = new LeaseManager(this.threadWakeFrequency);

    final boolean isSlowLogTableEnabled = conf.getBoolean(HConstants.SLOW_LOG_SYS_TABLE_ENABLED_KEY,
      HConstants.DEFAULT_SLOW_LOG_SYS_TABLE_ENABLED_KEY);
    final boolean walEventTrackerEnabled =
      conf.getBoolean(WAL_EVENT_TRACKER_ENABLED_KEY, WAL_EVENT_TRACKER_ENABLED_DEFAULT);

    if (isSlowLogTableEnabled || walEventTrackerEnabled) {
      // default chore duration: 10 min
      // After <version number>, we will remove hbase.slowlog.systable.chore.duration conf property
      final int slowLogChoreDuration = conf.getInt(HConstants.SLOW_LOG_SYS_TABLE_CHORE_DURATION_KEY,
        DEFAULT_SLOW_LOG_SYS_TABLE_CHORE_DURATION);

      final int namedQueueChoreDuration =
        conf.getInt(NAMED_QUEUE_CHORE_DURATION_KEY, NAMED_QUEUE_CHORE_DURATION_DEFAULT);
      // Considering min of slowLogChoreDuration and namedQueueChoreDuration
      int choreDuration = Math.min(slowLogChoreDuration, namedQueueChoreDuration);

      namedQueueServiceChore = new NamedQueueServiceChore(this, choreDuration,
        this.namedQueueRecorder, this.getConnection());
    }

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
    int storefileRefreshPeriod =
      conf.getInt(StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD,
        StorefileRefresherChore.DEFAULT_REGIONSERVER_STOREFILE_REFRESH_PERIOD);
    if (storefileRefreshPeriod == 0) {
      storefileRefreshPeriod =
        conf.getInt(StorefileRefresherChore.REGIONSERVER_META_STOREFILE_REFRESH_PERIOD,
          StorefileRefresherChore.DEFAULT_REGIONSERVER_STOREFILE_REFRESH_PERIOD);
      onlyMetaRefresh = true;
    }
    if (storefileRefreshPeriod > 0) {
      this.storefileRefresher =
        new StorefileRefresherChore(storefileRefreshPeriod, onlyMetaRefresh, this, this);
    }

    int brokenStoreFileCleanerPeriod =
      conf.getInt(BrokenStoreFileCleaner.BROKEN_STOREFILE_CLEANER_PERIOD,
        BrokenStoreFileCleaner.DEFAULT_BROKEN_STOREFILE_CLEANER_PERIOD);
    int brokenStoreFileCleanerDelay =
      conf.getInt(BrokenStoreFileCleaner.BROKEN_STOREFILE_CLEANER_DELAY,
        BrokenStoreFileCleaner.DEFAULT_BROKEN_STOREFILE_CLEANER_DELAY);
    double brokenStoreFileCleanerDelayJitter =
      conf.getDouble(BrokenStoreFileCleaner.BROKEN_STOREFILE_CLEANER_DELAY_JITTER,
        BrokenStoreFileCleaner.DEFAULT_BROKEN_STOREFILE_CLEANER_DELAY_JITTER);
    double jitterRate =
      (ThreadLocalRandom.current().nextDouble() - 0.5D) * brokenStoreFileCleanerDelayJitter;
    long jitterValue = Math.round(brokenStoreFileCleanerDelay * jitterRate);
    this.brokenStoreFileCleaner =
      new BrokenStoreFileCleaner((int) (brokenStoreFileCleanerDelay + jitterValue),
        brokenStoreFileCleanerPeriod, this, conf, this);

    this.rsMobFileCleanerChore = new RSMobFileCleanerChore(this);

    registerConfigurationObservers();
    initializeReplicationMarkerChore();
  }

  private void registerConfigurationObservers() {
    // Registering the compactSplitThread object with the ConfigurationManager.
    configurationManager.registerObserver(this.compactSplitThread);
    configurationManager.registerObserver(this.rpcServices);
    configurationManager.registerObserver(this);
  }

  /*
   * Verify that server is healthy
   */
  private boolean isHealthy() {
    if (!dataFsOk) {
      // File system problem
      return false;
    }
    // Verify that all threads are alive
    boolean healthy = (this.leaseManager == null || this.leaseManager.isAlive())
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
  public List<WAL> getWALs() {
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

  WALFactory getWalFactory() {
    return walFactory;
  }

  @Override
  public void stop(final String msg) {
    stop(msg, false, RpcServer.getRequestUser().orElse(null));
  }

  /**
   * Stops the regionserver.
   * @param msg   Status message
   * @param force True if this is a regionserver abort
   * @param user  The user executing the stop request, or null if no user is associated
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

  public void waitForServerOnline() {
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
  public void postOpenDeployTasks(final PostOpenDeployContext context) throws IOException {
    HRegion r = context.getRegion();
    long openProcId = context.getOpenProcId();
    long masterSystemTime = context.getMasterSystemTime();
    rpcServices.checkOpen();
    LOG.info("Post open deploy tasks for {}, pid={}, masterSystemTime={}",
      r.getRegionInfo().getRegionNameAsString(), openProcId, masterSystemTime);
    // Do checks to see if we need to compact (references or too many files)
    // Skip compaction check if region is read only
    if (!r.isReadOnly()) {
      for (HStore s : r.stores.values()) {
        if (s.hasReferences() || s.needsCompaction()) {
          this.compactSplitThread.requestSystemCompaction(r, s, "Opening Region");
        }
      }
    }
    long openSeqNum = r.getOpenSeqNum();
    if (openSeqNum == HConstants.NO_SEQNUM) {
      // If we opened a region, we should have read some sequence number from it.
      LOG.error(
        "No sequence number found when opening " + r.getRegionInfo().getRegionNameAsString());
      openSeqNum = 0;
    }

    // Notify master
    if (
      !reportRegionStateTransition(new RegionStateTransitionContext(TransitionCode.OPENED,
        openSeqNum, openProcId, masterSystemTime, r.getRegionInfo()))
    ) {
      throw new IOException(
        "Failed to report opened region to master: " + r.getRegionInfo().getRegionNameAsString());
    }

    triggerFlushInPrimaryRegion(r);

    LOG.debug("Finished post open deploy task for " + r.getRegionInfo().getRegionNameAsString());
  }

  /**
   * Helper method for use in tests. Skip the region transition report when there's no master around
   * to receive it.
   */
  private boolean skipReportingTransition(final RegionStateTransitionContext context) {
    final TransitionCode code = context.getCode();
    final long openSeqNum = context.getOpenSeqNum();
    long masterSystemTime = context.getMasterSystemTime();
    final RegionInfo[] hris = context.getHris();

    if (code == TransitionCode.OPENED) {
      Preconditions.checkArgument(hris != null && hris.length == 1);
      if (hris[0].isMetaRegion()) {
        LOG.warn(
          "meta table location is stored in master local store, so we can not skip reporting");
        return false;
      } else {
        try {
          MetaTableAccessor.updateRegionLocation(asyncClusterConnection.toConnection(), hris[0],
            serverName, openSeqNum, masterSystemTime);
        } catch (IOException e) {
          LOG.info("Failed to update meta", e);
          return false;
        }
      }
    }
    return true;
  }

  private ReportRegionStateTransitionRequest
    createReportRegionStateTransitionRequest(final RegionStateTransitionContext context) {
    final TransitionCode code = context.getCode();
    final long openSeqNum = context.getOpenSeqNum();
    final RegionInfo[] hris = context.getHris();
    final long[] procIds = context.getProcIds();

    ReportRegionStateTransitionRequest.Builder builder =
      ReportRegionStateTransitionRequest.newBuilder();
    builder.setServer(ProtobufUtil.toServerName(serverName));
    RegionStateTransition.Builder transition = builder.addTransitionBuilder();
    transition.setTransitionCode(code);
    if (code == TransitionCode.OPENED && openSeqNum >= 0) {
      transition.setOpenSeqNum(openSeqNum);
    }
    for (RegionInfo hri : hris) {
      transition.addRegionInfo(ProtobufUtil.toRegionInfo(hri));
    }
    for (long procId : procIds) {
      transition.addProcId(procId);
    }

    return builder.build();
  }

  @Override
  public boolean reportRegionStateTransition(final RegionStateTransitionContext context) {
    if (TEST_SKIP_REPORTING_TRANSITION) {
      return skipReportingTransition(context);
    }
    final ReportRegionStateTransitionRequest request =
      createReportRegionStateTransitionRequest(context);

    int tries = 0;
    long pauseTime = this.retryPauseTime;
    // Keep looping till we get an error. We want to send reports even though server is going down.
    // Only go down if clusterConnection is null. It is set to null almost as last thing as the
    // HRegionServer does down.
    while (this.asyncClusterConnection != null && !this.asyncClusterConnection.isClosed()) {
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
        boolean pause = ioe instanceof ServerNotRunningYetException
          || ioe instanceof PleaseHoldException || ioe instanceof CallQueueTooBigException;
        if (pause) {
          // Do backoff else we flood the Master with requests.
          pauseTime = ConnectionUtils.getPauseTime(this.retryPauseTime, tries);
        } else {
          pauseTime = this.retryPauseTime; // Reset.
        }
        LOG.info("Failed report transition " + TextFormat.shortDebugString(request) + "; retry (#"
          + tries + ")"
          + (pause
            ? " after " + pauseTime + "ms delay (Master is coming online...)."
            : " immediately."),
          ioe);
        if (pause) {
          Threads.sleep(pauseTime);
        }
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
  private void triggerFlushInPrimaryRegion(final HRegion region) {
    if (ServerRegionReplicaUtil.isDefaultReplica(region.getRegionInfo())) {
      return;
    }
    TableName tn = region.getTableDescriptor().getTableName();
    if (
      !ServerRegionReplicaUtil.isRegionReplicaReplicationEnabled(region.conf, tn)
        || !ServerRegionReplicaUtil.isRegionReplicaWaitForPrimaryFlushEnabled(region.conf) ||
        // If the memstore replication not setup, we do not have to wait for observing a flush event
        // from primary before starting to serve reads, because gaps from replication is not
        // applicable,this logic is from
        // TableDescriptorBuilder.ModifyableTableDescriptor.setRegionMemStoreReplication by
        // HBASE-13063
        !region.getTableDescriptor().hasRegionMemStoreReplication()
    ) {
      region.setReadsEnabled(true);
      return;
    }

    region.setReadsEnabled(false); // disable reads before marking the region as opened.
    // RegionReplicaFlushHandler might reset this.

    // Submit it to be handled by one of the handlers so that we do not block OpenRegionHandler
    if (this.executorService != null) {
      this.executorService.submit(new RegionReplicaFlushHandler(this, region));
    } else {
      LOG.info("Executor is null; not running flush of primary region replica for {}",
        region.getRegionInfo());
    }
  }

  @InterfaceAudience.Private
  public RSRpcServices getRSRpcServices() {
    return rpcServices;
  }

  /**
   * Cause the server to exit without closing the regions it is serving, the log it is using and
   * without notifying the master. Used unit testing and on catastrophic events such as HDFS is
   * yanked out from under hbase or we OOME. the reason we are aborting the exception that caused
   * the abort, or null
   */
  @Override
  public void abort(String reason, Throwable cause) {
    if (!setAbortRequested()) {
      // Abort already in progress, ignore the new request.
      LOG.debug("Abort already in progress. Ignoring the current request with reason: {}", reason);
      return;
    }
    String msg = "***** ABORTING region server " + this + ": " + reason + " *****";
    if (cause != null) {
      LOG.error(HBaseMarkers.FATAL, msg, cause);
    } else {
      LOG.error(HBaseMarkers.FATAL, msg);
    }
    // HBASE-4014: show list of coprocessors that were loaded to help debug
    // regionserver crashes.Note that we're implicitly using
    // java.util.HashSet's toString() method to print the coprocessor names.
    LOG.error(HBaseMarkers.FATAL,
      "RegionServer abort: loaded coprocessors are: " + CoprocessorHost.getLoadedCoprocessors());
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
      RegionServerStatusService.BlockingInterface rss = rssStub;
      if (rss != null && this.serverName != null) {
        ReportRSFatalErrorRequest.Builder builder = ReportRSFatalErrorRequest.newBuilder();
        builder.setServer(ProtobufUtil.toServerName(this.serverName));
        builder.setErrorMessage(msg);
        rss.reportRSFatalError(null, builder.build());
      }
    } catch (Throwable t) {
      LOG.warn("Unable to report fatal error to master", t);
    }

    scheduleAbortTimer();
    // shutdown should be run as the internal user
    stop(reason, true, null);
  }

  /*
   * Simulate a kill -9 of this server. Exits w/o closing regions or cleaninup logs but it does
   * close socket in case want to bring up server on old hostname+port immediately.
   */
  @InterfaceAudience.Private
  protected void kill() {
    this.killed = true;
    abort("Simulated kill");
  }

  // Limits the time spent in the shutdown process.
  private void scheduleAbortTimer() {
    if (this.abortMonitor == null) {
      this.abortMonitor = new Timer("Abort regionserver monitor", true);
      TimerTask abortTimeoutTask = null;
      try {
        Constructor<? extends TimerTask> timerTaskCtor =
          Class.forName(conf.get(ABORT_TIMEOUT_TASK, SystemExitWhenAbortTimeout.class.getName()))
            .asSubclass(TimerTask.class).getDeclaredConstructor();
        timerTaskCtor.setAccessible(true);
        abortTimeoutTask = timerTaskCtor.newInstance();
      } catch (Exception e) {
        LOG.warn("Initialize abort timeout task failed", e);
      }
      if (abortTimeoutTask != null) {
        abortMonitor.schedule(abortTimeoutTask, conf.getLong(ABORT_TIMEOUT, DEFAULT_ABORT_TIMEOUT));
      }
    }
  }

  /**
   * Wait on all threads to finish. Presumption is that all closes and stops have already been
   * called.
   */
  protected void stopServiceThreads() {
    // clean up the scheduled chores
    stopChoreService();
    if (bootstrapNodeManager != null) {
      bootstrapNodeManager.stop();
    }
    if (this.cacheFlusher != null) {
      this.cacheFlusher.join();
    }
    if (this.walRoller != null) {
      this.walRoller.close();
    }
    if (this.compactSplitThread != null) {
      this.compactSplitThread.join();
    }
    stopExecutorService();
    if (sameReplicationSourceAndSink && this.replicationSourceHandler != null) {
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

  /** Returns Return the object that implements the replication source executorService. */
  @Override
  public ReplicationSourceService getReplicationSourceService() {
    return replicationSourceHandler;
  }

  /** Returns Return the object that implements the replication sink executorService. */
  public ReplicationSinkService getReplicationSinkService() {
    return replicationSinkHandler;
  }

  /**
   * Get the current master from ZooKeeper and open the RPC connection to it. To get a fresh
   * connection, the current rssStub must be null. Method will block until a master is available.
   * You can break from this block by requesting the server stop.
   * @return master + port, or null if server has been stopped
   */
  private synchronized ServerName createRegionServerStatusStub() {
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
  @InterfaceAudience.Private
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
          if (EnvironmentEdgeManager.currentTime() > (previousLogTime + 1000)) {
            LOG.debug("No master found; retry");
            previousLogTime = EnvironmentEdgeManager.currentTime();
          }
          refresh = true; // let's try pull it from ZK directly
          if (sleepInterrupted(200)) {
            interrupted = true;
          }
          continue;
        }
        try {
          BlockingRpcChannel channel = this.rpcClient.createBlockingRpcChannel(sn,
            userProvider.getCurrent(), shortOperationTimeout);
          intRssStub = RegionServerStatusService.newBlockingStub(channel);
          intLockStub = LockService.newBlockingStub(channel);
          break;
        } catch (IOException e) {
          if (EnvironmentEdgeManager.currentTime() > (previousLogTime + 1000)) {
            e = e instanceof RemoteException ? ((RemoteException) e).unwrapRemoteException() : e;
            if (e instanceof ServerNotRunningYetException) {
              LOG.info("Master isn't available yet, retrying");
            } else {
              LOG.warn("Unable to connect to master. Retrying. Error was:", e);
            }
            previousLogTime = EnvironmentEdgeManager.currentTime();
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
    this.lockStub = intLockStub;
    return sn;
  }

  /**
   * @return True if we should break loop because cluster is going down or this server has been
   *         stopped or hdfs has gone bad.
   */
  private boolean keepLooping() {
    return !this.stopped && isClusterUp();
  }

  /*
   * Let the master know we're here Run initialization using parameters passed us by the master.
   * @return A Map of key/value configurations we got from the Master else null if we failed to
   * register.
   */
  private RegionServerStartupResponse reportForDuty() throws IOException {
    if (this.masterless) {
      return RegionServerStartupResponse.getDefaultInstance();
    }
    ServerName masterServerName = createRegionServerStatusStub(true);
    RegionServerStatusService.BlockingInterface rss = rssStub;
    if (masterServerName == null || rss == null) {
      return null;
    }
    RegionServerStartupResponse result = null;
    try {
      rpcServices.requestCount.reset();
      rpcServices.rpcGetRequestCount.reset();
      rpcServices.rpcScanRequestCount.reset();
      rpcServices.rpcFullScanRequestCount.reset();
      rpcServices.rpcMultiRequestCount.reset();
      rpcServices.rpcMutateRequestCount.reset();
      LOG.info("reportForDuty to master=" + masterServerName + " with port="
        + rpcServices.getSocketAddress().getPort() + ", startcode=" + this.startcode);
      long now = EnvironmentEdgeManager.currentTime();
      int port = rpcServices.getSocketAddress().getPort();
      RegionServerStartupRequest.Builder request = RegionServerStartupRequest.newBuilder();
      if (!StringUtils.isBlank(useThisHostnameInstead)) {
        request.setUseThisHostnameInstead(useThisHostnameInstead);
      }
      request.setPort(port);
      request.setServerStartCode(this.startcode);
      request.setServerCurrentTime(now);
      result = rss.regionServerStartup(null, request.build());
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof ClockOutOfSyncException) {
        LOG.error(HBaseMarkers.FATAL, "Master rejected startup because clock is out of sync", ioe);
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
   * Close meta region if we carry it
   * @param abort Whether we're running an abort.
   */
  private void closeMetaTableRegions(final boolean abort) {
    HRegion meta = null;
    this.onlineRegionsLock.writeLock().lock();
    try {
      for (Map.Entry<String, HRegion> e : onlineRegions.entrySet()) {
        RegionInfo hri = e.getValue().getRegionInfo();
        if (hri.isMetaRegion()) {
          meta = e.getValue();
        }
        if (meta != null) {
          break;
        }
      }
    } finally {
      this.onlineRegionsLock.writeLock().unlock();
    }
    if (meta != null) {
      closeRegionIgnoreErrors(meta.getRegionInfo(), abort);
    }
  }

  /**
   * Schedule closes on all user regions. Should be safe calling multiple times because it wont'
   * close regions that are already closed or that are closing.
   * @param abort Whether we're running an abort.
   */
  private void closeUserRegions(final boolean abort) {
    this.onlineRegionsLock.writeLock().lock();
    try {
      for (Map.Entry<String, HRegion> e : this.onlineRegions.entrySet()) {
        HRegion r = e.getValue();
        if (!r.getRegionInfo().isMetaRegion() && r.isAvailable()) {
          // Don't update zk with this close transition; pass false.
          closeRegionIgnoreErrors(r.getRegionInfo(), abort);
        }
      }
    } finally {
      this.onlineRegionsLock.writeLock().unlock();
    }
  }

  protected Map<String, HRegion> getOnlineRegions() {
    return this.onlineRegions;
  }

  public int getNumberOfOnlineRegions() {
    return this.onlineRegions.size();
  }

  /**
   * For tests, web ui and metrics. This method will only work if HRegionServer is in the same JVM
   * as client; HRegion cannot be serialized to cross an rpc.
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
   *         the biggest.
   */
  SortedMap<Long, Collection<HRegion>> getCopyOfOnlineRegionsSortedByOffHeapSize() {
    // we'll sort the regions in reverse
    SortedMap<Long, Collection<HRegion>> sortedRegions = new TreeMap<>(Comparator.reverseOrder());
    // Copy over all regions. Regions are sorted by size with biggest first.
    for (HRegion region : this.onlineRegions.values()) {
      addRegion(sortedRegions, region, region.getMemStoreOffHeapSize());
    }
    return sortedRegions;
  }

  /**
   * @return A new Map of online regions sorted by region heap size with the first entry being the
   *         biggest.
   */
  SortedMap<Long, Collection<HRegion>> getCopyOfOnlineRegionsSortedByOnHeapSize() {
    // we'll sort the regions in reverse
    SortedMap<Long, Collection<HRegion>> sortedRegions = new TreeMap<>(Comparator.reverseOrder());
    // Copy over all regions. Regions are sorted by size with biggest first.
    for (HRegion region : this.onlineRegions.values()) {
      addRegion(sortedRegions, region, region.getMemStoreHeapSize());
    }
    return sortedRegions;
  }

  /** Returns reference to FlushRequester */
  @Override
  public FlushRequester getFlushRequester() {
    return this.cacheFlusher;
  }

  @Override
  public CompactionRequester getCompactionRequestor() {
    return this.compactSplitThread;
  }

  @Override
  public LeaseManager getLeaseManager() {
    return leaseManager;
  }

  /** Returns {@code true} when the data file system is available, {@code false} otherwise. */
  boolean isDataFileSystemOk() {
    return this.dataFsOk;
  }

  public RegionServerCoprocessorHost getRegionServerCoprocessorHost() {
    return this.rsHost;
  }

  @Override
  public ConcurrentMap<byte[], Boolean> getRegionsInTransitionInRS() {
    return this.regionsInTransitionInRS;
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
    FileSystem walFs, Path walDir, Path oldWALDir, WALFactory walFactory) throws IOException {
    // read in the name of the source replication class from the config file.
    String sourceClassname = conf.get(HConstants.REPLICATION_SOURCE_SERVICE_CLASSNAME,
      HConstants.REPLICATION_SERVICE_CLASSNAME_DEFAULT);

    // read in the name of the sink replication class from the config file.
    String sinkClassname = conf.get(HConstants.REPLICATION_SINK_SERVICE_CLASSNAME,
      HConstants.REPLICATION_SINK_SERVICE_CLASSNAME_DEFAULT);

    // If both the sink and the source class names are the same, then instantiate
    // only one object.
    if (sourceClassname.equals(sinkClassname)) {
      server.replicationSourceHandler = newReplicationInstance(sourceClassname,
        ReplicationSourceService.class, conf, server, walFs, walDir, oldWALDir, walFactory);
      server.replicationSinkHandler = (ReplicationSinkService) server.replicationSourceHandler;
      server.sameReplicationSourceAndSink = true;
    } else {
      server.replicationSourceHandler = newReplicationInstance(sourceClassname,
        ReplicationSourceService.class, conf, server, walFs, walDir, oldWALDir, walFactory);
      server.replicationSinkHandler = newReplicationInstance(sinkClassname,
        ReplicationSinkService.class, conf, server, walFs, walDir, oldWALDir, walFactory);
      server.sameReplicationSourceAndSink = false;
    }
  }

  private static <T extends ReplicationService> T newReplicationInstance(String classname,
    Class<T> xface, Configuration conf, HRegionServer server, FileSystem walFs, Path logDir,
    Path oldLogDir, WALFactory walFactory) throws IOException {
    final Class<? extends T> clazz;
    try {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      clazz = Class.forName(classname, true, classLoader).asSubclass(xface);
    } catch (java.lang.ClassNotFoundException nfe) {
      throw new IOException("Could not find class for " + classname);
    }
    T service = ReflectionUtils.newInstance(clazz, conf);
    service.initialize(server, walFs, logDir, oldLogDir, walFactory);
    return service;
  }

  public Map<String, ReplicationStatus> getWalGroupsReplicationStatus() {
    Map<String, ReplicationStatus> walGroupsReplicationStatus = new TreeMap<>();
    if (!this.isOnline()) {
      return walGroupsReplicationStatus;
    }
    List<ReplicationSourceInterface> allSources = new ArrayList<>();
    allSources.addAll(replicationSourceHandler.getReplicationManager().getSources());
    allSources.addAll(replicationSourceHandler.getReplicationManager().getOldSources());
    for (ReplicationSourceInterface source : allSources) {
      walGroupsReplicationStatus.putAll(source.getWalGroupStatus());
    }
    return walGroupsReplicationStatus;
  }

  /**
   * Utility for constructing an instance of the passed HRegionServer class.
   */
  static HRegionServer constructRegionServer(final Class<? extends HRegionServer> regionServerClass,
    final Configuration conf) {
    try {
      Constructor<? extends HRegionServer> c =
        regionServerClass.getConstructor(Configuration.class);
      return c.newInstance(conf);
    } catch (Exception e) {
      throw new RuntimeException(
        "Failed construction of " + "Regionserver: " + regionServerClass.toString(), e);
    }
  }

  /**
   * @see org.apache.hadoop.hbase.regionserver.HRegionServerCommandLine
   */
  public static void main(String[] args) {
    LOG.info("STARTING executorService " + HRegionServer.class.getSimpleName());
    VersionInfo.logVersion();
    Configuration conf = HBaseConfiguration.create();
    @SuppressWarnings("unchecked")
    Class<? extends HRegionServer> regionServerClass = (Class<? extends HRegionServer>) conf
      .getClass(HConstants.REGION_SERVER_IMPL, HRegionServer.class);

    new HRegionServerCommandLine(regionServerClass).doMain(args);
  }

  /**
   * Gets the online regions of the specified table. This method looks at the in-memory
   * onlineRegions. It does not go to <code>hbase:meta</code>. Only returns <em>online</em> regions.
   * If a region on this table has been closed during a disable, etc., it will not be included in
   * the returned list. So, the returned list may not necessarily be ALL regions in this table, its
   * all the ONLINE regions in the table.
   * @param tableName table to limit the scope of the query
   * @return Online regions from <code>tableName</code>
   */
  @Override
  public List<HRegion> getRegions(TableName tableName) {
    List<HRegion> tableRegions = new ArrayList<>();
    synchronized (this.onlineRegions) {
      for (HRegion region : this.onlineRegions.values()) {
        RegionInfo regionInfo = region.getRegionInfo();
        if (regionInfo.getTable().equals(tableName)) {
          tableRegions.add(region);
        }
      }
    }
    return tableRegions;
  }

  @Override
  public List<HRegion> getRegions() {
    List<HRegion> allRegions;
    synchronized (this.onlineRegions) {
      // Return a clone copy of the onlineRegions
      allRegions = new ArrayList<>(onlineRegions.values());
    }
    return allRegions;
  }

  /**
   * Gets the online tables in this RS. This method looks at the in-memory onlineRegions.
   * @return all the online tables in this RS
   */
  public Set<TableName> getOnlineTables() {
    Set<TableName> tables = new HashSet<>();
    synchronized (this.onlineRegions) {
      for (Region region : this.onlineRegions.values()) {
        tables.add(region.getTableDescriptor().getTableName());
      }
    }
    return tables;
  }

  public String[] getRegionServerCoprocessors() {
    TreeSet<String> coprocessors = new TreeSet<>();
    try {
      coprocessors.addAll(getWAL(null).getCoprocessorHost().getCoprocessors());
    } catch (IOException exception) {
      LOG.warn("Exception attempting to fetch wal coprocessor information for the common wal; "
        + "skipping.");
      LOG.debug("Exception details for failure to fetch wal coprocessor information.", exception);
    }
    Collection<HRegion> regions = getOnlineRegionsLocalContext();
    for (HRegion region : regions) {
      coprocessors.addAll(region.getCoprocessorHost().getCoprocessors());
      try {
        coprocessors.addAll(getWAL(region.getRegionInfo()).getCoprocessorHost().getCoprocessors());
      } catch (IOException exception) {
        LOG.warn("Exception attempting to fetch wal coprocessor information for region " + region
          + "; skipping.");
        LOG.debug("Exception details for failure to fetch wal coprocessor information.", exception);
      }
    }
    coprocessors.addAll(rsHost.getCoprocessors());
    return coprocessors.toArray(new String[0]);
  }

  /**
   * Try to close the region, logs a warning on failure but continues.
   * @param region Region to close
   */
  private void closeRegionIgnoreErrors(RegionInfo region, final boolean abort) {
    try {
      if (!closeRegion(region.getEncodedName(), abort, null)) {
        LOG
          .warn("Failed to close " + region.getRegionNameAsString() + " - ignoring and continuing");
      }
    } catch (IOException e) {
      LOG.warn("Failed to close " + region.getRegionNameAsString() + " - ignoring and continuing",
        e);
    }
  }

  /**
   * Close asynchronously a region, can be called from the master or internally by the regionserver
   * when stopping. If called from the master, the region will update the status.
   * <p>
   * If an opening was in progress, this method will cancel it, but will not start a new close. The
   * coprocessors are not called in this case. A NotServingRegionException exception is thrown.
   * </p>
   * <p>
   * If a close was in progress, this new request will be ignored, and an exception thrown.
   * </p>
   * @param encodedName Region to close
   * @param abort       True if we are aborting
   * @param destination Where the Region is being moved too... maybe null if unknown.
   * @return True if closed a region.
   * @throws NotServingRegionException if the region is not online
   */
  protected boolean closeRegion(String encodedName, final boolean abort,
    final ServerName destination) throws NotServingRegionException {
    // Check for permissions to close.
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

    // previous can come back 'null' if not in map.
    final Boolean previous =
      this.regionsInTransitionInRS.putIfAbsent(Bytes.toBytes(encodedName), Boolean.FALSE);

    if (Boolean.TRUE.equals(previous)) {
      LOG.info("Received CLOSE for the region:" + encodedName + " , which we are already "
        + "trying to OPEN. Cancelling OPENING.");
      if (!regionsInTransitionInRS.replace(Bytes.toBytes(encodedName), previous, Boolean.FALSE)) {
        // The replace failed. That should be an exceptional case, but theoretically it can happen.
        // We're going to try to do a standard close then.
        LOG.warn("The opening for region " + encodedName + " was done before we could cancel it."
          + " Doing a standard close now");
        return closeRegion(encodedName, abort, destination);
      }
      // Let's get the region from the online region list again
      actualRegion = this.getRegion(encodedName);
      if (actualRegion == null) { // If already online, we still need to close it.
        LOG.info("The opening previously in progress has been cancelled by a CLOSE request.");
        // The master deletes the znode when it receives this exception.
        throw new NotServingRegionException(
          "The region " + encodedName + " was opening but not yet served. Opening is cancelled.");
      }
    } else if (previous == null) {
      LOG.info("Received CLOSE for {}", encodedName);
    } else if (Boolean.FALSE.equals(previous)) {
      LOG.info("Received CLOSE for the region: " + encodedName
        + ", which we are already trying to CLOSE, but not completed yet");
      return true;
    }

    if (actualRegion == null) {
      LOG.debug("Received CLOSE for a region which is not online, and we're not opening.");
      this.regionsInTransitionInRS.remove(Bytes.toBytes(encodedName));
      // The master deletes the znode when it receives this exception.
      throw new NotServingRegionException(
        "The region " + encodedName + " is not online, and is not opening.");
    }

    CloseRegionHandler crh;
    final RegionInfo hri = actualRegion.getRegionInfo();
    if (hri.isMetaRegion()) {
      crh = new CloseMetaHandler(this, this, hri, abort);
    } else {
      crh = new CloseRegionHandler(this, this, hri, abort, destination);
    }
    this.executorService.submit(crh);
    return true;
  }

  /**
   * @return HRegion for the passed binary <code>regionName</code> or null if named region is not
   *         member of the online regions.
   */
  public HRegion getOnlineRegion(final byte[] regionName) {
    String encodedRegionName = RegionInfo.encodeRegionName(regionName);
    return this.onlineRegions.get(encodedRegionName);
  }

  @Override
  public HRegion getRegion(final String encodedRegionName) {
    return this.onlineRegions.get(encodedRegionName);
  }

  @Override
  public boolean removeRegion(final HRegion r, ServerName destination) {
    HRegion toReturn = this.onlineRegions.remove(r.getRegionInfo().getEncodedName());
    metricsRegionServerImpl.requestsCountCache.remove(r.getRegionInfo().getEncodedName());
    if (destination != null) {
      long closeSeqNum = r.getMaxFlushedSeqId();
      if (closeSeqNum == HConstants.NO_SEQNUM) {
        // No edits in WAL for this region; get the sequence number when the region was opened.
        closeSeqNum = r.getOpenSeqNum();
        if (closeSeqNum == HConstants.NO_SEQNUM) {
          closeSeqNum = 0;
        }
      }
      boolean selfMove = ServerName.isSameAddress(destination, this.getServerName());
      addToMovedRegions(r.getRegionInfo().getEncodedName(), destination, closeSeqNum, selfMove);
      if (selfMove) {
        this.regionServerAccounting.getRetainedRegionRWRequestsCnt().put(
          r.getRegionInfo().getEncodedName(),
          new Pair<>(r.getReadRequestsCount(), r.getWriteRequestsCount()));
      }
    }
    this.regionFavoredNodesMap.remove(r.getRegionInfo().getEncodedName());
    configurationManager.deregisterObserver(r);
    return toReturn != null;
  }

  /**
   * Protected Utility method for safely obtaining an HRegion handle.
   * @param regionName Name of online {@link HRegion} to return
   * @return {@link HRegion} for <code>regionName</code>
   */
  protected HRegion getRegion(final byte[] regionName) throws NotServingRegionException {
    String encodedRegionName = RegionInfo.encodeRegionName(regionName);
    return getRegionByEncodedName(regionName, encodedRegionName);
  }

  public HRegion getRegionByEncodedName(String encodedRegionName) throws NotServingRegionException {
    return getRegionByEncodedName(null, encodedRegionName);
  }

  private HRegion getRegionByEncodedName(byte[] regionName, String encodedRegionName)
    throws NotServingRegionException {
    HRegion region = this.onlineRegions.get(encodedRegionName);
    if (region == null) {
      MovedRegionInfo moveInfo = getMovedRegion(encodedRegionName);
      if (moveInfo != null) {
        throw new RegionMovedException(moveInfo.getServerName(), moveInfo.getSeqNum());
      }
      Boolean isOpening = this.regionsInTransitionInRS.get(Bytes.toBytes(encodedRegionName));
      String regionNameStr =
        regionName == null ? encodedRegionName : Bytes.toStringBinary(regionName);
      if (isOpening != null && isOpening) {
        throw new RegionOpeningException(
          "Region " + regionNameStr + " is opening on " + this.serverName);
      }
      throw new NotServingRegionException(
        "" + regionNameStr + " is not online on " + this.serverName);
    }
    return region;
  }

  /**
   * Cleanup after Throwable caught invoking method. Converts <code>t</code> to IOE if it isn't
   * already.
   * @param t   Throwable
   * @param msg Message to log in error. Can be null.
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

  /**
   * @param msg Message to put in new IOE if passed <code>t</code> is not an IOE
   * @return Make <code>t</code> an IOE if it isn't already.
   */
  private IOException convertThrowableToIOE(final Throwable t, final String msg) {
    return (t instanceof IOException ? (IOException) t
      : msg == null || msg.length() == 0 ? new IOException(t)
      : new IOException(msg, t));
  }

  /**
   * Checks to see if the file system is still accessible. If not, sets abortRequested and
   * stopRequested
   * @return false if file system is not available
   */
  boolean checkFileSystem() {
    if (this.dataFsOk && this.dataFs != null) {
      try {
        FSUtils.checkFileSystemAvailable(this.dataFs);
      } catch (IOException e) {
        abort("File System not available", e);
        this.dataFsOk = false;
      }
    }
    return this.dataFsOk;
  }

  @Override
  public void updateRegionFavoredNodesMapping(String encodedRegionName,
    List<org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ServerName> favoredNodes) {
    Address[] addr = new Address[favoredNodes.size()];
    // Refer to the comment on the declaration of regionFavoredNodesMap on why
    // it is a map of region name to Address[]
    for (int i = 0; i < favoredNodes.size(); i++) {
      addr[i] = Address.fromParts(favoredNodes.get(i).getHostName(), favoredNodes.get(i).getPort());
    }
    regionFavoredNodesMap.put(encodedRegionName, addr);
  }

  /**
   * Return the favored nodes for a region given its encoded name. Look at the comment around
   * {@link #regionFavoredNodesMap} on why we convert to InetSocketAddress[] here.
   * @param encodedRegionName the encoded region name.
   * @return array of favored locations
   */
  @Override
  public InetSocketAddress[] getFavoredNodesForRegion(String encodedRegionName) {
    return Address.toSocketAddress(regionFavoredNodesMap.get(encodedRegionName));
  }

  @Override
  public ServerNonceManager getNonceManager() {
    return this.nonceManager;
  }

  private static class MovedRegionInfo {
    private final ServerName serverName;
    private final long seqNum;

    MovedRegionInfo(ServerName serverName, long closeSeqNum) {
      this.serverName = serverName;
      this.seqNum = closeSeqNum;
    }

    public ServerName getServerName() {
      return serverName;
    }

    public long getSeqNum() {
      return seqNum;
    }
  }

  /**
   * We need a timeout. If not there is a risk of giving a wrong information: this would double the
   * number of network calls instead of reducing them.
   */
  private static final int TIMEOUT_REGION_MOVED = (2 * 60 * 1000);

  private void addToMovedRegions(String encodedName, ServerName destination, long closeSeqNum,
    boolean selfMove) {
    if (selfMove) {
      LOG.warn("Not adding moved region record: " + encodedName + " to self.");
      return;
    }
    LOG.info("Adding " + encodedName + " move to " + destination + " record at close sequenceid="
      + closeSeqNum);
    movedRegionInfoCache.put(encodedName, new MovedRegionInfo(destination, closeSeqNum));
  }

  void removeFromMovedRegions(String encodedName) {
    movedRegionInfoCache.invalidate(encodedName);
  }

  @InterfaceAudience.Private
  public MovedRegionInfo getMovedRegion(String encodedRegionName) {
    return movedRegionInfoCache.getIfPresent(encodedRegionName);
  }

  @InterfaceAudience.Private
  public int movedRegionCacheExpiredTime() {
    return TIMEOUT_REGION_MOVED;
  }

  private String getMyEphemeralNodePath() {
    return zooKeeper.getZNodePaths().getRsPath(serverName);
  }

  private boolean isHealthCheckerConfigured() {
    String healthScriptLocation = this.conf.get(HConstants.HEALTH_SCRIPT_LOC);
    return org.apache.commons.lang3.StringUtils.isNotBlank(healthScriptLocation);
  }

  /** Returns the underlying {@link CompactSplit} for the servers */
  public CompactSplit getCompactSplitThread() {
    return this.compactSplitThread;
  }

  CoprocessorServiceResponse execRegionServerService(
    @SuppressWarnings("UnusedParameters") final RpcController controller,
    final CoprocessorServiceRequest serviceRequest) throws ServiceException {
    try {
      ServerRpcController serviceController = new ServerRpcController();
      CoprocessorServiceCall call = serviceRequest.getCall();
      String serviceName = call.getServiceName();
      Service service = coprocessorServiceHandlers.get(serviceName);
      if (service == null) {
        throw new UnknownProtocolException(null,
          "No registered coprocessor executorService found for " + serviceName);
      }
      ServiceDescriptor serviceDesc = service.getDescriptorForType();

      String methodName = call.getMethodName();
      MethodDescriptor methodDesc = serviceDesc.findMethodByName(methodName);
      if (methodDesc == null) {
        throw new UnknownProtocolException(service.getClass(),
          "Unknown method " + methodName + " called on executorService " + serviceName);
      }

      Message request = CoprocessorRpcUtils.getRequest(service, methodDesc, call.getRequest());
      final Message.Builder responseBuilder =
        service.getResponsePrototype(methodDesc).newBuilderForType();
      service.callMethod(methodDesc, serviceController, request, message -> {
        if (message != null) {
          responseBuilder.mergeFrom(message);
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
   * May be null if this is a master which not carry table.
   * @return The block cache instance used by the regionserver.
   */
  @Override
  public Optional<BlockCache> getBlockCache() {
    return Optional.ofNullable(this.blockCache);
  }

  /**
   * May be null if this is a master which not carry table.
   * @return The cache for mob files used by the regionserver.
   */
  @Override
  public Optional<MobFileCache> getMobFileCache() {
    return Optional.ofNullable(this.mobFileCache);
  }

  /** Returns : Returns the ConfigurationManager object for testing purposes. */
  ConfigurationManager getConfigurationManager() {
    return configurationManager;
  }

  CacheEvictionStats clearRegionBlockCache(Region region) {
    long evictedBlocks = 0;

    for (Store store : region.getStores()) {
      for (StoreFile hFile : store.getStorefiles()) {
        evictedBlocks += blockCache.evictBlocksByHfileName(hFile.getPath().getName());
      }
    }

    return CacheEvictionStats.builder().withEvictedBlocks(evictedBlocks).build();
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

  public MemStoreFlusher getMemStoreFlusher() {
    return cacheFlusher;
  }

  /**
   * For testing
   * @return whether all wal roll request finished for this regionserver
   */
  @InterfaceAudience.Private
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
    try {
      Superusers.initialize(newConf);
    } catch (IOException e) {
      LOG.warn("Failed to initialize SuperUsers on reloading of the configuration");
    }

    // update region server coprocessor if the configuration has changed.
    if (
      CoprocessorConfigurationUtil.checkConfigurationChange(getConfiguration(), newConf,
        CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY)
    ) {
      LOG.info("Update region server coprocessors because the configuration has changed");
      this.rsHost = new RegionServerCoprocessorHost(this, newConf);
    }
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
  public EntityLock regionLock(final List<RegionInfo> regionInfo, final String description,
    final Abortable abort) {
    final LockServiceClient client =
      new LockServiceClient(conf, lockStub, asyncClusterConnection.getNonceGenerator());
    return client.regionLock(regionInfo, description, abort);
  }

  @Override
  public void unassign(byte[] regionName) throws IOException {
    FutureUtils.get(asyncClusterConnection.getAdmin().unassign(regionName, false));
  }

  @Override
  public RegionServerSpaceQuotaManager getRegionServerSpaceQuotaManager() {
    return this.rsSpaceQuotaManager;
  }

  @Override
  public boolean reportFileArchivalForQuotas(TableName tableName,
    Collection<Entry<String, Long>> archivedFiles) {
    if (TEST_SKIP_REPORTING_TRANSITION) {
      return false;
    }
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

  void executeProcedure(long procId, RSProcedureCallable callable) {
    executorService.submit(new RSProcedureHandler(this, procId, callable));
  }

  public void remoteProcedureComplete(long procId, Throwable error) {
    procedureResultReporter.complete(procId, error);
  }

  void reportProcedureDone(ReportProcedureDoneRequest request) throws IOException {
    RegionServerStatusService.BlockingInterface rss;
    // TODO: juggling class state with an instance variable, outside of a synchronized block :'(
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
   * Will ignore the open/close region procedures which already submitted or executed. When master
   * had unfinished open/close region procedure and restarted, new active master may send duplicate
   * open/close region request to regionserver. The open/close request is submitted to a thread pool
   * and execute. So first need a cache for submitted open/close region procedures. After the
   * open/close region request executed and report region transition succeed, cache it in executed
   * region procedures cache. See {@link #finishRegionProcedure(long)}. After report region
   * transition succeed, master will not send the open/close region request to regionserver again.
   * And we thought that the ongoing duplicate open/close region request should not be delayed more
   * than 600 seconds. So the executed region procedures cache will expire after 600 seconds. See
   * HBASE-22404 for more details.
   * @param procId the id of the open/close region procedure
   * @return true if the procedure can be submitted.
   */
  boolean submitRegionProcedure(long procId) {
    if (procId == -1) {
      return true;
    }
    // Ignore the region procedures which already submitted.
    Long previous = submittedRegionProcedures.putIfAbsent(procId, procId);
    if (previous != null) {
      LOG.warn("Received procedure pid={}, which already submitted, just ignore it", procId);
      return false;
    }
    // Ignore the region procedures which already executed.
    if (executedRegionProcedures.getIfPresent(procId) != null) {
      LOG.warn("Received procedure pid={}, which already executed, just ignore it", procId);
      return false;
    }
    return true;
  }

  /**
   * See {@link #submitRegionProcedure(long)}.
   * @param procId the id of the open/close region procedure
   */
  public void finishRegionProcedure(long procId) {
    executedRegionProcedures.put(procId, procId);
    submittedRegionProcedures.remove(procId);
  }

  /**
   * Force to terminate region server when abort timeout.
   */
  private static class SystemExitWhenAbortTimeout extends TimerTask {

    public SystemExitWhenAbortTimeout() {
    }

    @Override
    public void run() {
      LOG.warn("Aborting region server timed out, terminating forcibly"
        + " and does not wait for any running shutdown hooks or finalizers to finish their work."
        + " Thread dump to stdout.");
      Threads.printThreadInfo(System.out, "Zombie HRegionServer");
      Runtime.getRuntime().halt(1);
    }
  }

  @InterfaceAudience.Private
  public CompactedHFilesDischarger getCompactedHFilesDischarger() {
    return compactedFileDischarger;
  }

  /**
   * Return pause time configured in {@link HConstants#HBASE_RPC_SHORTOPERATION_RETRY_PAUSE_TIME}}
   * @return pause time
   */
  @InterfaceAudience.Private
  public long getRetryPauseTime() {
    return this.retryPauseTime;
  }

  @Override
  public Optional<ServerName> getActiveMaster() {
    return Optional.ofNullable(masterAddressTracker.getMasterAddress());
  }

  @Override
  public List<ServerName> getBackupMasters() {
    return masterAddressTracker.getBackupMasters();
  }

  @Override
  public Iterator<ServerName> getBootstrapNodes() {
    return bootstrapNodeManager.getBootstrapNodes().iterator();
  }

  @Override
  public List<HRegionLocation> getMetaLocations() {
    return metaRegionLocationCache.getMetaRegionLocations();
  }

  @Override
  protected NamedQueueRecorder createNamedQueueRecord() {
    return NamedQueueRecorder.getInstance(conf);
  }

  @Override
  protected boolean clusterMode() {
    // this method will be called in the constructor of super class, so we can not return masterless
    // directly here, as it will always be false.
    return !conf.getBoolean(MASTERLESS_CONFIG_NAME, false);
  }

  @InterfaceAudience.Private
  public BrokenStoreFileCleaner getBrokenStoreFileCleaner() {
    return brokenStoreFileCleaner;
  }

  @InterfaceAudience.Private
  public RSMobFileCleanerChore getRSMobFileCleanerChore() {
    return rsMobFileCleanerChore;
  }

  RSSnapshotVerifier getRsSnapshotVerifier() {
    return rsSnapshotVerifier;
  }

  @Override
  protected void stopChores() {
    shutdownChore(nonceManagerChore);
    shutdownChore(compactionChecker);
    shutdownChore(compactedFileDischarger);
    shutdownChore(periodicFlusher);
    shutdownChore(healthCheckChore);
    shutdownChore(executorStatusChore);
    shutdownChore(storefileRefresher);
    shutdownChore(fsUtilizationChore);
    shutdownChore(namedQueueServiceChore);
    shutdownChore(brokenStoreFileCleaner);
    shutdownChore(rsMobFileCleanerChore);
    shutdownChore(replicationMarkerChore);
  }

  @Override
  public RegionReplicationBufferManager getRegionReplicationBufferManager() {
    return regionReplicationBufferManager;
  }
}
