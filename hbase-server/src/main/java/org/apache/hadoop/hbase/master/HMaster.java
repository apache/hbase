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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_SPLIT_COORDINATED_BY_ZK;
import static org.apache.hadoop.hbase.HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS;
import static org.apache.hadoop.hbase.HConstants.HBASE_SPLIT_WAL_COORDINATED_BY_ZK;
import static org.apache.hadoop.hbase.util.DNS.MASTER_HOSTNAME_KEY;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.ClusterMetricsBuilder;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ReplicationPeerNotFoundException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocateType;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.http.InfoServer;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.master.MasterRpcServices.BalanceSwitchMode;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.MergeTableRegionsProcedure;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.RegionStateStore;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.master.balancer.BalancerChore;
import org.apache.hadoop.hbase.master.balancer.ClusterStatusChore;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.master.cleaner.DirScanPool;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.master.cleaner.LogCleaner;
import org.apache.hadoop.hbase.master.cleaner.ReplicationBarrierCleaner;
import org.apache.hadoop.hbase.master.cleaner.SnapshotCleanerChore;
import org.apache.hadoop.hbase.master.locking.LockManager;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan.PlanType;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizer;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizerChore;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizerFactory;
import org.apache.hadoop.hbase.master.procedure.CreateTableProcedure;
import org.apache.hadoop.hbase.master.procedure.DeleteNamespaceProcedure;
import org.apache.hadoop.hbase.master.procedure.DeleteTableProcedure;
import org.apache.hadoop.hbase.master.procedure.DisableTableProcedure;
import org.apache.hadoop.hbase.master.procedure.EnableTableProcedure;
import org.apache.hadoop.hbase.master.procedure.InitMetaProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureScheduler;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil.NonceProcedureRunnable;
import org.apache.hadoop.hbase.master.procedure.ModifyTableProcedure;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.master.procedure.ProcedureSyncWait;
import org.apache.hadoop.hbase.master.procedure.ReopenTableRegionsProcedure;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.master.procedure.TruncateTableProcedure;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.master.replication.AbstractPeerProcedure;
import org.apache.hadoop.hbase.master.replication.AddPeerProcedure;
import org.apache.hadoop.hbase.master.replication.DisablePeerProcedure;
import org.apache.hadoop.hbase.master.replication.EnablePeerProcedure;
import org.apache.hadoop.hbase.master.replication.RemovePeerProcedure;
import org.apache.hadoop.hbase.master.replication.ReplicationPeerManager;
import org.apache.hadoop.hbase.master.replication.SyncReplicationReplayWALManager;
import org.apache.hadoop.hbase.master.replication.TransitPeerSyncReplicationStateProcedure;
import org.apache.hadoop.hbase.master.replication.UpdatePeerConfigProcedure;
import org.apache.hadoop.hbase.master.slowlog.SlowLogMasterService;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.master.zksyncer.MasterAddressSyncer;
import org.apache.hadoop.hbase.master.zksyncer.MetaLocationSyncer;
import org.apache.hadoop.hbase.mob.MobFileCleanerChore;
import org.apache.hadoop.hbase.mob.MobFileCompactionChore;
import org.apache.hadoop.hbase.monitoring.MemoryBoundedLogMessageBuffer;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.procedure.MasterProcedureManagerHost;
import org.apache.hadoop.hbase.procedure.flush.MasterFlushTableProcedureManager;
import org.apache.hadoop.hbase.procedure2.LockedResource;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteProcedure;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore.ProcedureStoreListener;
import org.apache.hadoop.hbase.procedure2.store.region.RegionProcedureStore;
import org.apache.hadoop.hbase.quotas.MasterQuotaManager;
import org.apache.hadoop.hbase.quotas.MasterQuotasObserver;
import org.apache.hadoop.hbase.quotas.QuotaObserverChore;
import org.apache.hadoop.hbase.quotas.QuotaTableUtil;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.quotas.SnapshotQuotaObserverChore;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot.SpaceQuotaStatus;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshotNotifier;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshotNotifierFactory;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicy;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.replication.master.ReplicationHFileCleaner;
import org.apache.hadoop.hbase.replication.master.ReplicationLogCleaner;
import org.apache.hadoop.hbase.replication.master.ReplicationPeerConfigUpgrader;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationStatus;
import org.apache.hadoop.hbase.rsgroup.RSGroupAdminEndpoint;
import org.apache.hadoop.hbase.rsgroup.RSGroupBasedLoadBalancer;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfoManager;
import org.apache.hadoop.hbase.rsgroup.RSGroupUtil;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.SecurityConstants;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.IdLock;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.LoadBalancerTracker;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.RegionNormalizerTracker;
import org.apache.hadoop.hbase.zookeeper.SnapshotCleanupTracker;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

/**
 * HMaster is the "master server" for HBase. An HBase cluster has one active
 * master.  If many masters are started, all compete.  Whichever wins goes on to
 * run the cluster.  All others park themselves in their constructor until
 * master or cluster shutdown or until the active master loses its lease in
 * zookeeper.  Thereafter, all running master jostle to take over master role.
 *
 * <p>The Master can be asked shutdown the cluster. See {@link #shutdown()}.  In
 * this case it will tell all regionservers to go down and then wait on them
 * all reporting in that they are down.  This master will then shut itself down.
 *
 * <p>You can also shutdown just this master.  Call {@link #stopMaster()}.
 *
 * @see org.apache.zookeeper.Watcher
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@SuppressWarnings("deprecation")
public class HMaster extends HRegionServer implements MasterServices {
  private static Logger LOG = LoggerFactory.getLogger(HMaster.class);

  /**
   * Protection against zombie master. Started once Master accepts active responsibility and
   * starts taking over responsibilities. Allows a finite time window before giving up ownership.
   */
  private static class InitializationMonitor extends Thread {
    /** The amount of time in milliseconds to sleep before checking initialization status. */
    public static final String TIMEOUT_KEY = "hbase.master.initializationmonitor.timeout";
    public static final long TIMEOUT_DEFAULT = TimeUnit.MILLISECONDS.convert(15, TimeUnit.MINUTES);

    /**
     * When timeout expired and initialization has not complete, call {@link System#exit(int)} when
     * true, do nothing otherwise.
     */
    public static final String HALT_KEY = "hbase.master.initializationmonitor.haltontimeout";
    public static final boolean HALT_DEFAULT = false;

    private final HMaster master;
    private final long timeout;
    private final boolean haltOnTimeout;

    /** Creates a Thread that monitors the {@link #isInitialized()} state. */
    InitializationMonitor(HMaster master) {
      super("MasterInitializationMonitor");
      this.master = master;
      this.timeout = master.getConfiguration().getLong(TIMEOUT_KEY, TIMEOUT_DEFAULT);
      this.haltOnTimeout = master.getConfiguration().getBoolean(HALT_KEY, HALT_DEFAULT);
      this.setDaemon(true);
    }

    @Override
    public void run() {
      try {
        while (!master.isStopped() && master.isActiveMaster()) {
          Thread.sleep(timeout);
          if (master.isInitialized()) {
            LOG.debug("Initialization completed within allotted tolerance. Monitor exiting.");
          } else {
            LOG.error("Master failed to complete initialization after " + timeout + "ms. Please"
                + " consider submitting a bug report including a thread dump of this process.");
            if (haltOnTimeout) {
              LOG.error("Zombie Master exiting. Thread dump to stdout");
              Threads.printThreadInfo(System.out, "Zombie HMaster");
              System.exit(-1);
            }
          }
        }
      } catch (InterruptedException ie) {
        LOG.trace("InitMonitor thread interrupted. Existing.");
      }
    }
  }

  // MASTER is name of the webapp and the attribute name used stuffing this
  //instance into web context.
  public static final String MASTER = "master";

  // Manager and zk listener for master election
  private final ActiveMasterManager activeMasterManager;
  // Region server tracker
  private RegionServerTracker regionServerTracker;
  // Draining region server tracker
  private DrainingServerTracker drainingServerTracker;
  // Tracker for load balancer state
  LoadBalancerTracker loadBalancerTracker;
  // Tracker for meta location, if any client ZK quorum specified
  MetaLocationSyncer metaLocationSyncer;
  // Tracker for active master location, if any client ZK quorum specified
  MasterAddressSyncer masterAddressSyncer;
  // Tracker for auto snapshot cleanup state
  SnapshotCleanupTracker snapshotCleanupTracker;

  // Tracker for split and merge state
  private SplitOrMergeTracker splitOrMergeTracker;

  // Tracker for region normalizer state
  private RegionNormalizerTracker regionNormalizerTracker;

  private ClusterSchemaService clusterSchemaService;

  public static final String HBASE_MASTER_WAIT_ON_SERVICE_IN_SECONDS =
    "hbase.master.wait.on.service.seconds";
  public static final int DEFAULT_HBASE_MASTER_WAIT_ON_SERVICE_IN_SECONDS = 5 * 60;

  public static final String HBASE_MASTER_CLEANER_INTERVAL = "hbase.master.cleaner.interval";

  public static final int DEFAULT_HBASE_MASTER_CLEANER_INTERVAL = 600 * 1000;

  // Metrics for the HMaster
  final MetricsMaster metricsMaster;
  // file system manager for the master FS operations
  private MasterFileSystem fileSystemManager;
  private MasterWalManager walManager;

  // manager to manage procedure-based WAL splitting, can be null if current
  // is zk-based WAL splitting. SplitWALManager will replace SplitLogManager
  // and MasterWalManager, which means zk-based WAL splitting code will be
  // useless after we switch to the procedure-based one. our eventual goal
  // is to remove all the zk-based WAL splitting code.
  private SplitWALManager splitWALManager;

  // server manager to deal with region server info
  private volatile ServerManager serverManager;

  // manager of assignment nodes in zookeeper
  private AssignmentManager assignmentManager;


  /**
   * Cache for the meta region replica's locations. Also tracks their changes to avoid stale
   * cache entries.
   */
  private final MetaRegionLocationCache metaRegionLocationCache;

  private RSGroupInfoManager rsGroupInfoManager;

  // manager of replication
  private ReplicationPeerManager replicationPeerManager;

  private SyncReplicationReplayWALManager syncReplicationReplayWALManager;

  // buffer for "fatal error" notices from region servers
  // in the cluster. This is only used for assisting
  // operations/debugging.
  MemoryBoundedLogMessageBuffer rsFatals;

  // flag set after we become the active master (used for testing)
  private volatile boolean activeMaster = false;

  // flag set after we complete initialization once active
  private final ProcedureEvent<?> initialized = new ProcedureEvent<>("master initialized");

  // flag set after master services are started,
  // initialization may have not completed yet.
  volatile boolean serviceStarted = false;

  // Maximum time we should run balancer for
  private final int maxBalancingTime;
  // Maximum percent of regions in transition when balancing
  private final double maxRitPercent;

  private final LockManager lockManager = new LockManager(this);

  private RSGroupBasedLoadBalancer balancer;
  // a lock to prevent concurrent normalization actions.
  private final ReentrantLock normalizationInProgressLock = new ReentrantLock();
  private RegionNormalizer normalizer;
  private BalancerChore balancerChore;
  private RegionNormalizerChore normalizerChore;
  private ClusterStatusChore clusterStatusChore;
  private ClusterStatusPublisher clusterStatusPublisherChore = null;
  private SnapshotCleanerChore snapshotCleanerChore = null;

  private HbckChore hbckChore;
  CatalogJanitor catalogJanitorChore;
  private DirScanPool cleanerPool;
  private LogCleaner logCleaner;
  private HFileCleaner hfileCleaner;
  private ReplicationBarrierCleaner replicationBarrierCleaner;
  private MobFileCleanerChore mobFileCleanerChore;
  private MobFileCompactionChore mobFileCompactionChore;
  // used to synchronize the mobCompactionStates
  private final IdLock mobCompactionLock = new IdLock();
  // save the information of mob compactions in tables.
  // the key is table name, the value is the number of compactions in that table.
  private Map<TableName, AtomicInteger> mobCompactionStates = Maps.newConcurrentMap();

  MasterCoprocessorHost cpHost;

  private final boolean preLoadTableDescriptors;

  // Time stamps for when a hmaster became active
  private long masterActiveTime;

  // Time stamp for when HMaster finishes becoming Active Master
  private long masterFinishedInitializationTime;

  Map<String, Service> coprocessorServiceHandlers = Maps.newHashMap();

  // monitor for snapshot of hbase tables
  SnapshotManager snapshotManager;
  // monitor for distributed procedures
  private MasterProcedureManagerHost mpmHost;

  private RegionsRecoveryChore regionsRecoveryChore = null;

  private RegionsRecoveryConfigManager regionsRecoveryConfigManager = null;
  // it is assigned after 'initialized' guard set to true, so should be volatile
  private volatile MasterQuotaManager quotaManager;
  private SpaceQuotaSnapshotNotifier spaceQuotaSnapshotNotifier;
  private QuotaObserverChore quotaObserverChore;
  private SnapshotQuotaObserverChore snapshotQuotaChore;

  private ProcedureExecutor<MasterProcedureEnv> procedureExecutor;
  private ProcedureStore procedureStore;

  // the master local storage to store procedure data, root table, etc.
  private MasterRegion masterRegion;

  // handle table states
  private TableStateManager tableStateManager;

  private long splitPlanCount;
  private long mergePlanCount;

  /** jetty server for master to redirect requests to regionserver infoServer */
  private Server masterJettyServer;

  // Determine if we should do normal startup or minimal "single-user" mode with no region
  // servers and no user tables. Useful for repair and recovery of hbase:meta
  private final boolean maintenanceMode;
  static final String MAINTENANCE_MODE = "hbase.master.maintenance_mode";

  // Cached clusterId on stand by masters to serve clusterID requests from clients.
  private final CachedClusterId cachedClusterId;

  public static class RedirectServlet extends HttpServlet {
    private static final long serialVersionUID = 2894774810058302473L;
    private final int regionServerInfoPort;
    private final String regionServerHostname;

    /**
     * @param infoServer that we're trying to send all requests to
     * @param hostname may be null. if given, will be used for redirects instead of host from client.
     */
    public RedirectServlet(InfoServer infoServer, String hostname) {
       regionServerInfoPort = infoServer.getPort();
       regionServerHostname = hostname;
    }

    @Override
    public void doGet(HttpServletRequest request,
        HttpServletResponse response) throws ServletException, IOException {
      String redirectHost = regionServerHostname;
      if(redirectHost == null) {
        redirectHost = request.getServerName();
        if(!Addressing.isLocalAddress(InetAddress.getByName(redirectHost))) {
          LOG.warn("Couldn't resolve '" + redirectHost + "' as an address local to this node and '" +
              MASTER_HOSTNAME_KEY + "' is not set; client will get an HTTP 400 response. If " +
              "your HBase deployment relies on client accessible names that the region server process " +
              "can't resolve locally, then you should set the previously mentioned configuration variable " +
              "to an appropriate hostname.");
          // no sending client provided input back to the client, so the goal host is just in the logs.
          response.sendError(400, "Request was to a host that I can't resolve for any of the network interfaces on " +
              "this node. If this is due to an intermediary such as an HTTP load balancer or other proxy, your HBase " +
              "administrator can set '" + MASTER_HOSTNAME_KEY + "' to point to the correct hostname.");
          return;
        }
      }
      // TODO this scheme should come from looking at the scheme registered in the infoserver's http server for the
      // host and port we're using, but it's buried way too deep to do that ATM.
      String redirectUrl = request.getScheme() + "://"
        + redirectHost + ":" + regionServerInfoPort
        + request.getRequestURI();
      response.sendRedirect(redirectUrl);
    }
  }

  /**
   * Initializes the HMaster. The steps are as follows:
   * <p>
   * <ol>
   * <li>Initialize the local HRegionServer
   * <li>Start the ActiveMasterManager.
   * </ol>
   * <p>
   * Remaining steps of initialization occur in
   * {@link #finishActiveMasterInitialization(MonitoredTask)} after the master becomes the
   * active one.
   */
  public HMaster(final Configuration conf) throws IOException {
    super(conf);
    TraceUtil.initTracer(conf);
    try {
      if (conf.getBoolean(MAINTENANCE_MODE, false)) {
        LOG.info("Detected {}=true via configuration.", MAINTENANCE_MODE);
        maintenanceMode = true;
      } else if (Boolean.getBoolean(MAINTENANCE_MODE)) {
        LOG.info("Detected {}=true via environment variables.", MAINTENANCE_MODE);
        maintenanceMode = true;
      } else {
        maintenanceMode = false;
      }
      this.rsFatals = new MemoryBoundedLogMessageBuffer(
          conf.getLong("hbase.master.buffer.for.rs.fatals", 1 * 1024 * 1024));
      LOG.info("hbase.rootdir={}, hbase.cluster.distributed={}", getDataRootDir(),
          this.conf.getBoolean(HConstants.CLUSTER_DISTRIBUTED, false));

      // Disable usage of meta replicas in the master
      this.conf.setBoolean(HConstants.USE_META_REPLICAS, false);

      decorateMasterConfiguration(this.conf);

      // Hack! Maps DFSClient => Master for logs.  HDFS made this
      // config param for task trackers, but we can piggyback off of it.
      if (this.conf.get("mapreduce.task.attempt.id") == null) {
        this.conf.set("mapreduce.task.attempt.id", "hb_m_" + this.serverName.toString());
      }

      this.metricsMaster = new MetricsMaster(new MetricsMasterWrapperImpl(this));

      // preload table descriptor at startup
      this.preLoadTableDescriptors = conf.getBoolean("hbase.master.preload.tabledescriptors", true);

      this.maxBalancingTime = getMaxBalancingTime();
      this.maxRitPercent = conf.getDouble(HConstants.HBASE_MASTER_BALANCER_MAX_RIT_PERCENT,
          HConstants.DEFAULT_HBASE_MASTER_BALANCER_MAX_RIT_PERCENT);

      // Do we publish the status?

      boolean shouldPublish = conf.getBoolean(HConstants.STATUS_PUBLISHED,
          HConstants.STATUS_PUBLISHED_DEFAULT);
      Class<? extends ClusterStatusPublisher.Publisher> publisherClass =
          conf.getClass(ClusterStatusPublisher.STATUS_PUBLISHER_CLASS,
              ClusterStatusPublisher.DEFAULT_STATUS_PUBLISHER_CLASS,
              ClusterStatusPublisher.Publisher.class);

      if (shouldPublish) {
        if (publisherClass == null) {
          LOG.warn(HConstants.STATUS_PUBLISHED + " is true, but " +
              ClusterStatusPublisher.DEFAULT_STATUS_PUBLISHER_CLASS +
              " is not set - not publishing status");
        } else {
          clusterStatusPublisherChore = new ClusterStatusPublisher(this, conf, publisherClass);
          LOG.debug("Created {}", this.clusterStatusPublisherChore);
          getChoreService().scheduleChore(clusterStatusPublisherChore);
        }
      }

      // Some unit tests don't need a cluster, so no zookeeper at all
      if (!conf.getBoolean("hbase.testing.nocluster", false)) {
        this.metaRegionLocationCache = new MetaRegionLocationCache(this.zooKeeper);
        this.activeMasterManager = createActiveMasterManager(zooKeeper, serverName, this);
      } else {
        this.metaRegionLocationCache = null;
        this.activeMasterManager = null;
      }
      cachedClusterId = new CachedClusterId(this, conf);
    } catch (Throwable t) {
      // Make sure we log the exception. HMaster is often started via reflection and the
      // cause of failed startup is lost.
      LOG.error("Failed construction of Master", t);
      throw t;
    }
  }

  /**
   * Protected to have custom implementations in tests override the default ActiveMaster
   * implementation.
   */
  protected ActiveMasterManager createActiveMasterManager(
      ZKWatcher zk, ServerName sn, org.apache.hadoop.hbase.Server server) {
    return new ActiveMasterManager(zk, sn, server);
  }

  @Override
  protected String getUseThisHostnameInstead(Configuration conf) {
    return conf.get(MASTER_HOSTNAME_KEY);
  }

  // Main run loop. Calls through to the regionserver run loop AFTER becoming active Master; will
  // block in here until then.
  @Override
  public void run() {
    try {
      if (!conf.getBoolean("hbase.testing.nocluster", false)) {
        Threads.setDaemonThreadRunning(new Thread(() -> {
          try {
            int infoPort = putUpJettyServer();
            startActiveMasterManager(infoPort);
          } catch (Throwable t) {
            // Make sure we log the exception.
            String error = "Failed to become Active Master";
            LOG.error(error, t);
            // Abort should have been called already.
            if (!isAborted()) {
              abort(error, t);
            }
          }
        }), getName() + ":becomeActiveMaster");
      }
      // Fall in here even if we have been aborted. Need to run the shutdown services and
      // the super run call will do this for us.
      super.run();
    } finally {
      if (this.clusterSchemaService != null) {
        // If on way out, then we are no longer active master.
        this.clusterSchemaService.stopAsync();
        try {
          this.clusterSchemaService.awaitTerminated(
              getConfiguration().getInt(HBASE_MASTER_WAIT_ON_SERVICE_IN_SECONDS,
              DEFAULT_HBASE_MASTER_WAIT_ON_SERVICE_IN_SECONDS), TimeUnit.SECONDS);
        } catch (TimeoutException te) {
          LOG.warn("Failed shutdown of clusterSchemaService", te);
        }
      }
      this.activeMaster = false;
    }
  }

  // return the actual infoPort, -1 means disable info server.
  private int putUpJettyServer() throws IOException {
    if (!conf.getBoolean("hbase.master.infoserver.redirect", true)) {
      return -1;
    }
    final int infoPort = conf.getInt("hbase.master.info.port.orig",
      HConstants.DEFAULT_MASTER_INFOPORT);
    // -1 is for disabling info server, so no redirecting
    if (infoPort < 0 || infoServer == null) {
      return -1;
    }
    if(infoPort == infoServer.getPort()) {
      return infoPort;
    }
    final String addr = conf.get("hbase.master.info.bindAddress", "0.0.0.0");
    if (!Addressing.isLocalAddress(InetAddress.getByName(addr))) {
      String msg =
          "Failed to start redirecting jetty server. Address " + addr
              + " does not belong to this host. Correct configuration parameter: "
              + "hbase.master.info.bindAddress";
      LOG.error(msg);
      throw new IOException(msg);
    }

    // TODO I'm pretty sure we could just add another binding to the InfoServer run by
    // the RegionServer and have it run the RedirectServlet instead of standing up
    // a second entire stack here.
    masterJettyServer = new Server();
    final ServerConnector connector = new ServerConnector(masterJettyServer);
    connector.setHost(addr);
    connector.setPort(infoPort);
    masterJettyServer.addConnector(connector);
    masterJettyServer.setStopAtShutdown(true);

    final String redirectHostname =
        StringUtils.isBlank(useThisHostnameInstead) ? null : useThisHostnameInstead;

    final RedirectServlet redirect = new RedirectServlet(infoServer, redirectHostname);
    final WebAppContext context = new WebAppContext(null, "/", null, null, null, null, WebAppContext.NO_SESSIONS);
    context.addServlet(new ServletHolder(redirect), "/*");
    context.setServer(masterJettyServer);

    try {
      masterJettyServer.start();
    } catch (Exception e) {
      throw new IOException("Failed to start redirecting jetty server", e);
    }
    return connector.getLocalPort();
  }

  /**
   * For compatibility, if failed with regionserver credentials, try the master one
   */
  @Override
  protected void login(UserProvider user, String host) throws IOException {
    try {
      super.login(user, host);
    } catch (IOException ie) {
      user.login(SecurityConstants.MASTER_KRB_KEYTAB_FILE,
              SecurityConstants.MASTER_KRB_PRINCIPAL, host);
    }
  }

  /**
   * If configured to put regions on active master,
   * wait till a backup master becomes active.
   * Otherwise, loop till the server is stopped or aborted.
   */
  @Override
  protected void waitForMasterActive(){
    if (maintenanceMode) {
      return;
    }
    boolean tablesOnMaster = LoadBalancer.isTablesOnMaster(conf);
    while (!(tablesOnMaster && activeMaster) && !isStopped() && !isAborted()) {
      sleeper.sleep();
    }
  }

  @VisibleForTesting
  public MasterRpcServices getMasterRpcServices() {
    return (MasterRpcServices)rpcServices;
  }

  public boolean balanceSwitch(final boolean b) throws IOException {
    return getMasterRpcServices().switchBalancer(b, BalanceSwitchMode.ASYNC);
  }

  @Override
  protected String getProcessName() {
    return MASTER;
  }

  @Override
  protected boolean canCreateBaseZNode() {
    return true;
  }

  @Override
  protected boolean canUpdateTableDescriptor() {
    return true;
  }

  @Override
  protected RSRpcServices createRpcServices() throws IOException {
    return new MasterRpcServices(this);
  }

  @Override
  protected void configureInfoServer() {
    infoServer.addUnprivilegedServlet("master-status", "/master-status", MasterStatusServlet.class);
    infoServer.setAttribute(MASTER, this);
    if (LoadBalancer.isTablesOnMaster(conf)) {
      super.configureInfoServer();
    }
  }

  @Override
  protected Class<? extends HttpServlet> getDumpServlet() {
    return MasterDumpServlet.class;
  }

  @Override
  public MetricsMaster getMasterMetrics() {
    return metricsMaster;
  }

  /**
   * <p>
   * Initialize all ZK based system trackers. But do not include {@link RegionServerTracker}, it
   * should have already been initialized along with {@link ServerManager}.
   * </p>
   * <p>
   * Will be overridden in tests.
   * </p>
   */
  @VisibleForTesting
  protected void initializeZKBasedSystemTrackers()
      throws IOException, InterruptedException, KeeperException, ReplicationException {
    this.balancer = new RSGroupBasedLoadBalancer();
    this.balancer.setConf(conf);
    this.loadBalancerTracker = new LoadBalancerTracker(zooKeeper, this);
    this.loadBalancerTracker.start();

    this.normalizer = RegionNormalizerFactory.getRegionNormalizer(conf);
    this.normalizer.setMasterServices(this);
    this.regionNormalizerTracker = new RegionNormalizerTracker(zooKeeper, this);
    this.regionNormalizerTracker.start();

    this.splitOrMergeTracker = new SplitOrMergeTracker(zooKeeper, conf, this);
    this.splitOrMergeTracker.start();

    // This is for backwards compatible. We do not need the CP for rs group now but if user want to
    // load it, we need to enable rs group.
    String[] cpClasses = conf.getStrings(MasterCoprocessorHost.MASTER_COPROCESSOR_CONF_KEY);
    if (cpClasses != null) {
      for (String cpClass : cpClasses) {
        if (RSGroupAdminEndpoint.class.getName().equals(cpClass)) {
          RSGroupUtil.enableRSGroup(conf);
          break;
        }
      }
    }
    this.rsGroupInfoManager = RSGroupInfoManager.create(this);

    this.replicationPeerManager = ReplicationPeerManager.create(zooKeeper, conf);

    this.drainingServerTracker = new DrainingServerTracker(zooKeeper, this, this.serverManager);
    this.drainingServerTracker.start();

    this.snapshotCleanupTracker = new SnapshotCleanupTracker(zooKeeper, this);
    this.snapshotCleanupTracker.start();

    String clientQuorumServers = conf.get(HConstants.CLIENT_ZOOKEEPER_QUORUM);
    boolean clientZkObserverMode = conf.getBoolean(HConstants.CLIENT_ZOOKEEPER_OBSERVER_MODE,
      HConstants.DEFAULT_CLIENT_ZOOKEEPER_OBSERVER_MODE);
    if (clientQuorumServers != null && !clientZkObserverMode) {
      // we need to take care of the ZK information synchronization
      // if given client ZK are not observer nodes
      ZKWatcher clientZkWatcher = new ZKWatcher(conf,
          getProcessName() + ":" + rpcServices.getSocketAddress().getPort() + "-clientZK", this,
          false, true);
      this.metaLocationSyncer = new MetaLocationSyncer(zooKeeper, clientZkWatcher, this);
      this.metaLocationSyncer.start();
      this.masterAddressSyncer = new MasterAddressSyncer(zooKeeper, clientZkWatcher, this);
      this.masterAddressSyncer.start();
      // set cluster id is a one-go effort
      ZKClusterId.setClusterId(clientZkWatcher, fileSystemManager.getClusterId());
    }

    // Set the cluster as up.  If new RSs, they'll be waiting on this before
    // going ahead with their startup.
    boolean wasUp = this.clusterStatusTracker.isClusterUp();
    if (!wasUp) this.clusterStatusTracker.setClusterUp();

    LOG.info("Active/primary master=" + this.serverName +
        ", sessionid=0x" +
        Long.toHexString(this.zooKeeper.getRecoverableZooKeeper().getSessionId()) +
        ", setting cluster-up flag (Was=" + wasUp + ")");

    // create/initialize the snapshot manager and other procedure managers
    this.snapshotManager = new SnapshotManager();
    this.mpmHost = new MasterProcedureManagerHost();
    this.mpmHost.register(this.snapshotManager);
    this.mpmHost.register(new MasterFlushTableProcedureManager());
    this.mpmHost.loadProcedures(conf);
    this.mpmHost.initialize(this, this.metricsMaster);
  }

  // Will be overriden in test to inject customized AssignmentManager
  @VisibleForTesting
  protected AssignmentManager createAssignmentManager(MasterServices master,
    MasterRegion masterRegion) {
    return new AssignmentManager(master, masterRegion);
  }

  /**
   * Load the meta region state from the meta region server ZNode.
   * @param zkw reference to the {@link ZKWatcher} which also contains configuration and operation
   * @param replicaId the ID of the replica
   * @return regionstate
   * @throws KeeperException if a ZooKeeper operation fails
   */
  private static RegionState getMetaRegionState(ZKWatcher zkw, int replicaId)
    throws KeeperException {
    RegionState regionState = null;
    try {
      byte[] data = ZKUtil.getData(zkw, zkw.getZNodePaths().getZNodeForReplica(replicaId));
      regionState = ProtobufUtil.parseMetaRegionStateFrom(data, replicaId);
    } catch (DeserializationException e) {
      throw ZKUtil.convert(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return regionState;
  }

  private void tryMigrateRootTableFromZooKeeper() throws IOException, KeeperException {
    // try migrate data from zookeeper
    try (RegionScanner scanner =
      masterRegion.getScanner(new Scan().addFamily(HConstants.CATALOG_FAMILY))) {
      List<Cell> cells = new ArrayList<>();
      boolean moreRows = scanner.next(cells);
      if (!cells.isEmpty() || moreRows) {
        // notice that all replicas for a region are in the same row, so the migration can be
        // done with in a one row put, which means if we have data in root table then we can make
        // sure that the migration is done.
        LOG.info("Root table already has data in it, skip migrating...");
        return;
      }
    }
    // start migrating
    Put put = null;
    List<String> metaReplicaNodes = zooKeeper.getMetaReplicaNodes();
    StringBuilder info = new StringBuilder("Migrating meta location:");
    for (String metaReplicaNode : metaReplicaNodes) {
      int replicaId = zooKeeper.getZNodePaths().getMetaReplicaIdFromZnode(metaReplicaNode);
      RegionState state = getMetaRegionState(zooKeeper, replicaId);
      if (put == null) {
        byte[] row = CatalogFamilyFormat.getMetaKeyForRegion(state.getRegion());
        put = new Put(row);
      }
      info.append(" ").append(state);
      put.setTimestamp(state.getStamp());
      MetaTableAccessor.addRegionInfo(put, state.getRegion());
      if (state.getServerName() != null) {
        MetaTableAccessor.addLocation(put, state.getServerName(), HConstants.NO_SEQNUM, replicaId);
      }
      put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setRow(put.getRow())
        .setFamily(HConstants.CATALOG_FAMILY)
        .setQualifier(RegionStateStore.getStateColumn(replicaId)).setTimestamp(put.getTimestamp())
        .setType(Cell.Type.Put).setValue(Bytes.toBytes(state.getState().name())).build());
    }
    if (put != null) {
      LOG.info(info.toString());
      final Put p = put;
      masterRegion.update(r -> r.put(p));
    } else {
      LOG.info("No meta location avaiable on zookeeper, skip migrating...");
    }
  }

  /**
   * Finish initialization of HMaster after becoming the primary master.
   * <p/>
   * The startup order is a bit complicated but very important, do not change it unless you know
   * what you are doing.
   * <ol>
   * <li>Initialize file system based components - file system manager, wal manager, table
   * descriptors, etc</li>
   * <li>Publish cluster id</li>
   * <li>Here comes the most complicated part - initialize server manager, assignment manager and
   * region server tracker
   * <ol type='i'>
   * <li>Create server manager</li>
   * <li>Create root table</li>
   * <li>Create procedure executor, load the procedures, but do not start workers. We will start it
   * later after we finish scheduling SCPs to avoid scheduling duplicated SCPs for the same
   * server</li>
   * <li>Create assignment manager and start it, load the meta region state, but do not load data
   * from meta region</li>
   * <li>Start region server tracker, construct the online servers set and find out dead servers and
   * schedule SCP for them. The online servers will be constructed by scanning zk, and we will also
   * scan the wal directory to find out possible live region servers, and the differences between
   * these two sets are the dead servers</li>
   * </ol>
   * </li>
   * <li>If this is a new deploy, schedule a InitMetaProcedure to initialize meta</li>
   * <li>Start necessary service threads - balancer, catalog janior, executor services, and also the
   * procedure executor, etc. Notice that the balancer must be created first as assignment manager
   * may use it when assigning regions.</li>
   * <li>Wait for meta to be initialized if necesssary, start table state manager.</li>
   * <li>Wait for enough region servers to check-in</li>
   * <li>Let assignment manager load data from meta and construct region states</li>
   * <li>Start all other things such as chore services, etc</li>
   * </ol>
   * <p/>
   * Notice that now we will not schedule a special procedure to make meta online(unless the first
   * time where meta has not been created yet), we will rely on SCP to bring meta online.
   */
  private void finishActiveMasterInitialization(MonitoredTask status) throws IOException,
          InterruptedException, KeeperException, ReplicationException {
    /*
     * We are active master now... go initialize components we need to run.
     */
    status.setStatus("Initializing Master file system");

    this.masterActiveTime = System.currentTimeMillis();
    // TODO: Do this using Dependency Injection, using PicoContainer, Guice or Spring.

    // always initialize the MemStoreLAB as we use a region to store data in master now, see
    // localStore.
    initializeMemStoreChunkCreator();
    this.fileSystemManager = new MasterFileSystem(conf);
    this.walManager = new MasterWalManager(this);

    // enable table descriptors cache
    this.tableDescriptors.setCacheOn();

    // warm-up HTDs cache on master initialization
    if (preLoadTableDescriptors) {
      status.setStatus("Pre-loading table descriptors");
      this.tableDescriptors.getAll();
    }

    // Publish cluster ID; set it in Master too. The superclass RegionServer does this later but
    // only after it has checked in with the Master. At least a few tests ask Master for clusterId
    // before it has called its run method and before RegionServer has done the reportForDuty.
    ClusterId clusterId = fileSystemManager.getClusterId();
    status.setStatus("Publishing Cluster ID " + clusterId + " in ZooKeeper");
    ZKClusterId.setClusterId(this.zooKeeper, fileSystemManager.getClusterId());
    this.clusterId = clusterId.toString();

    // Precaution. Put in place the old hbck1 lock file to fence out old hbase1s running their
    // hbck1s against an hbase2 cluster; it could do damage. To skip this behavior, set
    // hbase.write.hbck1.lock.file to false.
    if (this.conf.getBoolean("hbase.write.hbck1.lock.file", true)) {
      Pair<Path, FSDataOutputStream> result = null;
      try {
        result = HBaseFsck.checkAndMarkRunningHbck(this.conf,
            HBaseFsck.createLockRetryCounterFactory(this.conf).create());
      } finally {
        if (result != null) {
          IOUtils.closeQuietly(result.getSecond());
        }
      }
    }

    status.setStatus("Initialize ServerManager and schedule SCP for crash servers");
    // The below two managers must be created before loading procedures, as they will be used during
    // loading.
    this.serverManager = createServerManager(this);
    this.syncReplicationReplayWALManager = new SyncReplicationReplayWALManager(this);
    if (!conf.getBoolean(HBASE_SPLIT_WAL_COORDINATED_BY_ZK,
      DEFAULT_HBASE_SPLIT_COORDINATED_BY_ZK)) {
      this.splitWALManager = new SplitWALManager(this);
    }

    // initialize master local region
    masterRegion = MasterRegionFactory.create(this);

    tryMigrateRootTableFromZooKeeper();

    createProcedureExecutor();
    Map<Class<?>, List<Procedure<MasterProcedureEnv>>> procsByType =
      procedureExecutor.getActiveProceduresNoCopy().stream()
        .collect(Collectors.groupingBy(p -> p.getClass()));

    // Create Assignment Manager
    this.assignmentManager = createAssignmentManager(this, masterRegion);
    this.assignmentManager.start();
    // TODO: TRSP can perform as the sub procedure for other procedures, so even if it is marked as
    // completed, it could still be in the procedure list. This is a bit strange but is another
    // story, need to verify the implementation for ProcedureExecutor and ProcedureStore.
    List<TransitRegionStateProcedure> ritList =
      procsByType.getOrDefault(TransitRegionStateProcedure.class, Collections.emptyList()).stream()
        .filter(p -> !p.isFinished()).map(p -> (TransitRegionStateProcedure) p)
        .collect(Collectors.toList());
    this.assignmentManager.setupRIT(ritList);

    // Start RegionServerTracker with listing of servers found with exiting SCPs -- these should
    // be registered in the deadServers set -- and with the list of servernames out on the
    // filesystem that COULD BE 'alive' (we'll schedule SCPs for each and let SCP figure it out).
    // We also pass dirs that are already 'splitting'... so we can do some checks down in tracker.
    // TODO: Generate the splitting and live Set in one pass instead of two as we currently do.
    this.regionServerTracker = new RegionServerTracker(zooKeeper, this, this.serverManager);
    this.regionServerTracker.start(
      procsByType.getOrDefault(ServerCrashProcedure.class, Collections.emptyList()).stream()
        .map(p -> (ServerCrashProcedure) p).map(p -> p.getServerName()).collect(Collectors.toSet()),
      walManager.getLiveServersFromWALDir(), walManager.getSplittingServersFromWALDir());
    // This manager will be started AFTER hbase:meta is confirmed on line.
    // hbase.mirror.table.state.to.zookeeper is so hbase1 clients can connect. They read table
    // state from zookeeper while hbase2 reads it from hbase:meta. Disable if no hbase1 clients.
    this.tableStateManager =
      this.conf.getBoolean(MirroringTableStateManager.MIRROR_TABLE_STATE_TO_ZK_KEY, true)
        ?
        new MirroringTableStateManager(this):
        new TableStateManager(this);

    status.setStatus("Initializing ZK system trackers");
    initializeZKBasedSystemTrackers();
    status.setStatus("Loading last flushed sequence id of regions");
    try {
      this.serverManager.loadLastFlushedSequenceIds();
    } catch (IOException e) {
      LOG.info("Failed to load last flushed sequence id of regions"
          + " from file system", e);
    }
    // Set ourselves as active Master now our claim has succeeded up in zk.
    this.activeMaster = true;

    // Start the Zombie master detector after setting master as active, see HBASE-21535
    Thread zombieDetector = new Thread(new InitializationMonitor(this),
        "ActiveMasterInitializationMonitor-" + System.currentTimeMillis());
    zombieDetector.setDaemon(true);
    zombieDetector.start();

    // This is for backwards compatibility
    // See HBASE-11393
    status.setStatus("Update TableCFs node in ZNode");
    ReplicationPeerConfigUpgrader tableCFsUpdater =
        new ReplicationPeerConfigUpgrader(zooKeeper, conf);
    tableCFsUpdater.copyTableCFs();

    if (!maintenanceMode) {
      // Add the Observer to delete quotas on table deletion before starting all CPs by
      // default with quota support, avoiding if user specifically asks to not load this Observer.
      if (QuotaUtil.isQuotaEnabled(conf)) {
        updateConfigurationForQuotasObserver(conf);
      }
      // initialize master side coprocessors before we start handling requests
      status.setStatus("Initializing master coprocessors");
      this.cpHost = new MasterCoprocessorHost(this, this.conf);
    }

    // Checking if meta needs initializing.
    status.setStatus("Initializing meta table if this is a new deploy");
    InitMetaProcedure initMetaProc = null;
    if (!this.assignmentManager.getRegionStates().hasTableRegionStates(TableName.META_TABLE_NAME)) {
      Optional<InitMetaProcedure> optProc = procedureExecutor.getProcedures().stream()
        .filter(p -> p instanceof InitMetaProcedure).map(o -> (InitMetaProcedure) o).findAny();
      initMetaProc = optProc.orElseGet(() -> {
        // schedule an init meta procedure if meta has not been deployed yet
        InitMetaProcedure temp = new InitMetaProcedure();
        procedureExecutor.submitProcedure(temp);
        return temp;
      });
    }

    // initialize load balancer
    this.balancer.setMasterServices(this);
    this.balancer.setClusterMetrics(getClusterMetricsWithoutCoprocessor());
    this.balancer.initialize();

    // start up all service threads.
    status.setStatus("Initializing master service threads");
    startServiceThreads();
    // wait meta to be initialized after we start procedure executor
    if (initMetaProc != null) {
      initMetaProc.await();
    }
    // Wake up this server to check in
    sleeper.skipSleepCycle();

    // Wait for region servers to report in.
    // With this as part of master initialization, it precludes our being able to start a single
    // server that is both Master and RegionServer. Needs more thought. TODO.
    String statusStr = "Wait for region servers to report in";
    status.setStatus(statusStr);
    LOG.info(Objects.toString(status));
    waitForRegionServers(status);

    // Check if master is shutting down because issue initializing regionservers or balancer.
    if (isStopped()) {
      return;
    }

    status.setStatus("Starting assignment manager");
    // FIRST HBASE:META READ!!!!
    // The below cannot make progress w/o hbase:meta being online.
    // This is the FIRST attempt at going to hbase:meta. Meta on-lining is going on in background
    // as procedures run -- in particular SCPs for crashed servers... One should put up hbase:meta
    // if it is down. It may take a while to come online. So, wait here until meta if for sure
    // available. That's what waitForMetaOnline does.
    if (!waitForMetaOnline()) {
      return;
    }
    this.assignmentManager.joinCluster();
    // The below depends on hbase:meta being online.
    this.tableStateManager.start();
    // Below has to happen after tablestatemanager has started in the case where this hbase-2.x
    // is being started over an hbase-1.x dataset. tablestatemanager runs a migration as part
    // of its 'start' moving table state from zookeeper to hbase:meta. This migration needs to
    // complete before we do this next step processing offline regions else it fails reading
    // table states messing up master launch (namespace table, etc., are not assigned).
    this.assignmentManager.processOfflineRegions();
    // Initialize after meta is up as below scans meta
    if (getFavoredNodesManager() != null && !maintenanceMode) {
      SnapshotOfRegionAssignmentFromMeta snapshotOfRegionAssignment =
          new SnapshotOfRegionAssignmentFromMeta(getConnection());
      snapshotOfRegionAssignment.initialize();
      getFavoredNodesManager().initialize(snapshotOfRegionAssignment);
    }

    // set cluster status again after user regions are assigned
    this.balancer.setClusterMetrics(getClusterMetricsWithoutCoprocessor());

    // Start balancer and meta catalog janitor after meta and regions have been assigned.
    status.setStatus("Starting balancer and catalog janitor");
    this.clusterStatusChore = new ClusterStatusChore(this, balancer);
    getChoreService().scheduleChore(clusterStatusChore);
    this.balancerChore = new BalancerChore(this);
    getChoreService().scheduleChore(balancerChore);
    this.normalizerChore = new RegionNormalizerChore(this);
    getChoreService().scheduleChore(normalizerChore);
    this.catalogJanitorChore = new CatalogJanitor(this);
    getChoreService().scheduleChore(catalogJanitorChore);
    this.hbckChore = new HbckChore(this);
    getChoreService().scheduleChore(hbckChore);
    this.serverManager.startChore();

    // Only for rolling upgrade, where we need to migrate the data in namespace table to meta table.
    if (!waitForNamespaceOnline()) {
      return;
    }
    status.setStatus("Starting cluster schema service");
    initClusterSchemaService();

    if (this.cpHost != null) {
      try {
        this.cpHost.preMasterInitialization();
      } catch (IOException e) {
        LOG.error("Coprocessor preMasterInitialization() hook failed", e);
      }
    }

    status.markComplete("Initialization successful");
    LOG.info(String.format("Master has completed initialization %.3fsec",
       (System.currentTimeMillis() - masterActiveTime) / 1000.0f));
    this.masterFinishedInitializationTime = System.currentTimeMillis();
    configurationManager.registerObserver(this.balancer);
    configurationManager.registerObserver(this.cleanerPool);
    configurationManager.registerObserver(this.hfileCleaner);
    configurationManager.registerObserver(this.logCleaner);
    configurationManager.registerObserver(this.regionsRecoveryConfigManager);
    // Set master as 'initialized'.
    setInitialized(true);

    if (maintenanceMode) {
      LOG.info("Detected repair mode, skipping final initialization steps.");
      return;
    }

    assignmentManager.checkIfShouldMoveSystemRegionAsync();
    status.setStatus("Assign meta replicas");
    MasterMetaBootstrap metaBootstrap = createMetaBootstrap();
    try {
      metaBootstrap.assignMetaReplicas();
    } catch (IOException | KeeperException e){
      LOG.error("Assigning meta replica failed: ", e);
    }
    status.setStatus("Starting quota manager");
    initQuotaManager();
    if (QuotaUtil.isQuotaEnabled(conf)) {
      // Create the quota snapshot notifier
      spaceQuotaSnapshotNotifier = createQuotaSnapshotNotifier();
      spaceQuotaSnapshotNotifier.initialize(getConnection());
      this.quotaObserverChore = new QuotaObserverChore(this, getMasterMetrics());
      // Start the chore to read the region FS space reports and act on them
      getChoreService().scheduleChore(quotaObserverChore);

      this.snapshotQuotaChore = new SnapshotQuotaObserverChore(this, getMasterMetrics());
      // Start the chore to read snapshots and add their usage to table/NS quotas
      getChoreService().scheduleChore(snapshotQuotaChore);
    }
    final SlowLogMasterService slowLogMasterService = new SlowLogMasterService(conf, this);
    slowLogMasterService.init();

    // clear the dead servers with same host name and port of online server because we are not
    // removing dead server with same hostname and port of rs which is trying to check in before
    // master initialization. See HBASE-5916.
    this.serverManager.clearDeadServersWithSameHostNameAndPortOfOnlineServer();

    // Check and set the znode ACLs if needed in case we are overtaking a non-secure configuration
    status.setStatus("Checking ZNode ACLs");
    zooKeeper.checkAndSetZNodeAcls();

    status.setStatus("Initializing MOB Cleaner");
    initMobCleaner();

    status.setStatus("Calling postStartMaster coprocessors");
    if (this.cpHost != null) {
      // don't let cp initialization errors kill the master
      try {
        this.cpHost.postStartMaster();
      } catch (IOException ioe) {
        LOG.error("Coprocessor postStartMaster() hook failed", ioe);
      }
    }

    zombieDetector.interrupt();

    /*
     * After master has started up, lets do balancer post startup initialization. Since this runs
     * in activeMasterManager thread, it should be fine.
     */
    long start = System.currentTimeMillis();
    this.balancer.postMasterStartupInitialize();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Balancer post startup initialization complete, took " + (
          (System.currentTimeMillis() - start) / 1000) + " seconds");
    }
  }

  /**
   * Check hbase:meta is up and ready for reading. For use during Master startup only.
   * @return True if meta is UP and online and startup can progress. Otherwise, meta is not online
   *         and we will hold here until operator intervention.
   */
  @VisibleForTesting
  public boolean waitForMetaOnline() throws InterruptedException {
    Optional<RegionInfo> firstMetaRegion =
      this.assignmentManager.getRegionStates().getRegionsOfTable(TableName.META_TABLE_NAME).stream()
        .filter(RegionInfo::isFirst).filter(RegionReplicaUtil::isDefaultReplica).findFirst();
    return firstMetaRegion.isPresent() ? isRegionOnline(firstMetaRegion.get()) : false;
  }

  /**
   * @return True if region is online and scannable else false if an error or shutdown (Otherwise
   *   we just block in here holding up all forward-progess).
   */
  private boolean isRegionOnline(RegionInfo ri) {
    RetryCounter rc = null;
    while (!isStopped()) {
      RegionState rs = this.assignmentManager.getRegionStates().getRegionState(ri);
      if (rs.isOpened()) {
        if (this.getServerManager().isServerOnline(rs.getServerName())) {
          return true;
        }
      }
      // Region is not OPEN.
      Optional<Procedure<MasterProcedureEnv>> optProc = this.procedureExecutor.getProcedures().
          stream().filter(p -> p instanceof ServerCrashProcedure).findAny();
      // TODO: Add a page to refguide on how to do repair. Have this log message point to it.
      // Page will talk about loss of edits, how to schedule at least the meta WAL recovery, and
      // then how to assign including how to break region lock if one held.
      LOG.warn("{} is NOT online; state={}; ServerCrashProcedures={}. Master startup cannot " +
          "progress, in holding-pattern until region onlined.",
          ri.getRegionNameAsString(), rs, optProc.isPresent());
      // Check once-a-minute.
      if (rc == null) {
        rc = new RetryCounterFactory(1000).create();
      }
      Threads.sleep(rc.getBackoffTimeAndIncrementAttempts());
    }
    return false;
  }

  /**
   * Check hbase:namespace table is assigned. If not, startup will hang looking for the ns table
   * <p/>
   * This is for rolling upgrading, later we will migrate the data in ns table to the ns family of
   * meta table. And if this is a new cluster, this method will return immediately as there will be
   * no namespace table/region.
   * @return True if namespace table is up/online.
   */
  private boolean waitForNamespaceOnline() throws IOException {
    TableState nsTableState =
      MetaTableAccessor.getTableState(getConnection(), TableName.NAMESPACE_TABLE_NAME);
    if (nsTableState == null || nsTableState.isDisabled()) {
      // this means we have already migrated the data and disabled or deleted the namespace table,
      // or this is a new deploy which does not have a namespace table from the beginning.
      return true;
    }
    List<RegionInfo> ris =
      this.assignmentManager.getRegionStates().getRegionsOfTable(TableName.NAMESPACE_TABLE_NAME);
    if (ris.isEmpty()) {
      // maybe this will not happen any more, but anyway, no harm to add a check here...
      return true;
    }
    // Else there are namespace regions up in meta. Ensure they are assigned before we go on.
    for (RegionInfo ri : ris) {
      if (!isRegionOnline(ri)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Adds the {@code MasterQuotasObserver} to the list of configured Master observers to
   * automatically remove quotas for a table when that table is deleted.
   */
  @VisibleForTesting
  public void updateConfigurationForQuotasObserver(Configuration conf) {
    // We're configured to not delete quotas on table deletion, so we don't need to add the obs.
    if (!conf.getBoolean(
          MasterQuotasObserver.REMOVE_QUOTA_ON_TABLE_DELETE,
          MasterQuotasObserver.REMOVE_QUOTA_ON_TABLE_DELETE_DEFAULT)) {
      return;
    }
    String[] masterCoprocs = conf.getStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY);
    final int length = null == masterCoprocs ? 0 : masterCoprocs.length;
    String[] updatedCoprocs = new String[length + 1];
    if (length > 0) {
      System.arraycopy(masterCoprocs, 0, updatedCoprocs, 0, masterCoprocs.length);
    }
    updatedCoprocs[length] = MasterQuotasObserver.class.getName();
    conf.setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, updatedCoprocs);
  }

  private void initMobCleaner() {
    this.mobFileCleanerChore = new MobFileCleanerChore(this);
    getChoreService().scheduleChore(mobFileCleanerChore);
    this.mobFileCompactionChore = new MobFileCompactionChore(this);
    getChoreService().scheduleChore(mobFileCompactionChore);
  }

  /**
   * Create a {@link MasterMetaBootstrap} instance.
   * <p/>
   * Will be overridden in tests.
   */
  @VisibleForTesting
  protected MasterMetaBootstrap createMetaBootstrap() {
    // We put this out here in a method so can do a Mockito.spy and stub it out
    // w/ a mocked up MasterMetaBootstrap.
    return new MasterMetaBootstrap(this, masterRegion);
  }

  /**
   * <p>
   * Create a {@link ServerManager} instance.
   * </p>
   * <p>
   * Will be overridden in tests.
   * </p>
   */
  @VisibleForTesting
  protected ServerManager createServerManager(final MasterServices master) throws IOException {
    // We put this out here in a method so can do a Mockito.spy and stub it out
    // w/ a mocked up ServerManager.
    setupClusterConnection();
    return new ServerManager(master);
  }

  private void waitForRegionServers(final MonitoredTask status)
      throws IOException, InterruptedException {
    this.serverManager.waitForRegionServers(status);
  }

  // Will be overridden in tests
  @VisibleForTesting
  protected void initClusterSchemaService() throws IOException, InterruptedException {
    this.clusterSchemaService = new ClusterSchemaServiceImpl(this);
    this.clusterSchemaService.startAsync();
    try {
      this.clusterSchemaService.awaitRunning(getConfiguration().getInt(
        HBASE_MASTER_WAIT_ON_SERVICE_IN_SECONDS,
        DEFAULT_HBASE_MASTER_WAIT_ON_SERVICE_IN_SECONDS), TimeUnit.SECONDS);
    } catch (TimeoutException toe) {
      throw new IOException("Timedout starting ClusterSchemaService", toe);
    }
  }

  private void initQuotaManager() throws IOException {
    MasterQuotaManager quotaManager = new MasterQuotaManager(this);
    quotaManager.start();
    this.quotaManager = quotaManager;
  }

  private SpaceQuotaSnapshotNotifier createQuotaSnapshotNotifier() {
    SpaceQuotaSnapshotNotifier notifier =
        SpaceQuotaSnapshotNotifierFactory.getInstance().create(getConfiguration());
    return notifier;
  }

  boolean isCatalogJanitorEnabled() {
    return catalogJanitorChore != null ?
      catalogJanitorChore.getEnabled() : false;
  }

  boolean isCleanerChoreEnabled() {
    boolean hfileCleanerFlag = true, logCleanerFlag = true;

    if (hfileCleaner != null) {
      hfileCleanerFlag = hfileCleaner.getEnabled();
    }

    if (logCleaner != null) {
      logCleanerFlag = logCleaner.getEnabled();
    }

    return (hfileCleanerFlag && logCleanerFlag);
  }

  @Override
  public ServerManager getServerManager() {
    return this.serverManager;
  }

  @Override
  public MasterFileSystem getMasterFileSystem() {
    return this.fileSystemManager;
  }

  @Override
  public MasterWalManager getMasterWalManager() {
    return this.walManager;
  }

  @Override
  public SplitWALManager getSplitWALManager() {
    return splitWALManager;
  }

  @Override
  public TableStateManager getTableStateManager() {
    return tableStateManager;
  }

  /*
   * Start up all services. If any of these threads gets an unhandled exception
   * then they just die with a logged message.  This should be fine because
   * in general, we do not expect the master to get such unhandled exceptions
   *  as OOMEs; it should be lightly loaded. See what HRegionServer does if
   *  need to install an unexpected exception handler.
   */
  private void startServiceThreads() throws IOException {
    // Start the executor service pools
    this.executorService.startExecutorService(ExecutorType.MASTER_OPEN_REGION, conf.getInt(
      HConstants.MASTER_OPEN_REGION_THREADS, HConstants.MASTER_OPEN_REGION_THREADS_DEFAULT));
    this.executorService.startExecutorService(ExecutorType.MASTER_CLOSE_REGION, conf.getInt(
      HConstants.MASTER_CLOSE_REGION_THREADS, HConstants.MASTER_CLOSE_REGION_THREADS_DEFAULT));
    this.executorService.startExecutorService(ExecutorType.MASTER_SERVER_OPERATIONS,
      conf.getInt(HConstants.MASTER_SERVER_OPERATIONS_THREADS,
        HConstants.MASTER_SERVER_OPERATIONS_THREADS_DEFAULT));
    this.executorService.startExecutorService(ExecutorType.MASTER_META_SERVER_OPERATIONS,
      conf.getInt(HConstants.MASTER_META_SERVER_OPERATIONS_THREADS,
        HConstants.MASTER_META_SERVER_OPERATIONS_THREADS_DEFAULT));
    this.executorService.startExecutorService(ExecutorType.M_LOG_REPLAY_OPS, conf.getInt(
      HConstants.MASTER_LOG_REPLAY_OPS_THREADS, HConstants.MASTER_LOG_REPLAY_OPS_THREADS_DEFAULT));
    this.executorService.startExecutorService(ExecutorType.MASTER_SNAPSHOT_OPERATIONS, conf.getInt(
      SnapshotManager.SNAPSHOT_POOL_THREADS_KEY, SnapshotManager.SNAPSHOT_POOL_THREADS_DEFAULT));

    // We depend on there being only one instance of this executor running
    // at a time. To do concurrency, would need fencing of enable/disable of
    // tables.
    // Any time changing this maxThreads to > 1, pls see the comment at
    // AccessController#postCompletedCreateTableAction
    this.executorService.startExecutorService(ExecutorType.MASTER_TABLE_OPERATIONS, 1);
    startProcedureExecutor();

    // Create cleaner thread pool
    cleanerPool = new DirScanPool(conf);
    // Start log cleaner thread
    int cleanerInterval =
      conf.getInt(HBASE_MASTER_CLEANER_INTERVAL, DEFAULT_HBASE_MASTER_CLEANER_INTERVAL);
    this.logCleaner = new LogCleaner(cleanerInterval, this, conf,
      getMasterWalManager().getFileSystem(), getMasterWalManager().getOldLogDir(), cleanerPool);
    getChoreService().scheduleChore(logCleaner);

    // start the hfile archive cleaner thread
    Path archiveDir = HFileArchiveUtil.getArchivePath(conf);
    Map<String, Object> params = new HashMap<>();
    params.put(MASTER, this);
    this.hfileCleaner = new HFileCleaner(cleanerInterval, this, conf,
      getMasterFileSystem().getFileSystem(), archiveDir, cleanerPool, params);
    getChoreService().scheduleChore(hfileCleaner);

    // Regions Reopen based on very high storeFileRefCount is considered enabled
    // only if hbase.regions.recovery.store.file.ref.count has value > 0
    final int maxStoreFileRefCount = conf.getInt(
      HConstants.STORE_FILE_REF_COUNT_THRESHOLD,
      HConstants.DEFAULT_STORE_FILE_REF_COUNT_THRESHOLD);
    if (maxStoreFileRefCount > 0) {
      this.regionsRecoveryChore = new RegionsRecoveryChore(this, conf, this);
      getChoreService().scheduleChore(this.regionsRecoveryChore);
    } else {
      LOG.info("Reopening regions with very high storeFileRefCount is disabled. " +
          "Provide threshold value > 0 for {} to enable it.",
        HConstants.STORE_FILE_REF_COUNT_THRESHOLD);
    }

    this.regionsRecoveryConfigManager = new RegionsRecoveryConfigManager(this);

    replicationBarrierCleaner = new ReplicationBarrierCleaner(conf, this, getConnection(),
      replicationPeerManager);
    getChoreService().scheduleChore(replicationBarrierCleaner);

    final boolean isSnapshotChoreEnabled = this.snapshotCleanupTracker
        .isSnapshotCleanupEnabled();
    this.snapshotCleanerChore = new SnapshotCleanerChore(this, conf, getSnapshotManager());
    if (isSnapshotChoreEnabled) {
      getChoreService().scheduleChore(this.snapshotCleanerChore);
    } else {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Snapshot Cleaner Chore is disabled. Not starting up the chore..");
      }
    }
    serviceStarted = true;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Started service threads");
    }
  }

  @Override
  protected void stopServiceThreads() {
    if (masterJettyServer != null) {
      LOG.info("Stopping master jetty server");
      try {
        masterJettyServer.stop();
      } catch (Exception e) {
        LOG.error("Failed to stop master jetty server", e);
      }
    }
    stopChores();

    super.stopServiceThreads();
    if (cleanerPool != null) {
      cleanerPool.shutdownNow();
      cleanerPool = null;
    }

    LOG.debug("Stopping service threads");

    // stop procedure executor prior to other services such as server manager and assignment
    // manager, as these services are important for some running procedures. See HBASE-24117 for
    // example.
    stopProcedureExecutor();

    if (this.quotaManager != null) {
      this.quotaManager.stop();
    }

    if (this.activeMasterManager != null) {
      this.activeMasterManager.stop();
    }
    if (this.serverManager != null) {
      this.serverManager.stop();
    }
    if (this.assignmentManager != null) {
      this.assignmentManager.stop();
    }

    if (masterRegion != null) {
      masterRegion.close(isAborted());
    }
    if (this.walManager != null) {
      this.walManager.stop();
    }
    if (this.fileSystemManager != null) {
      this.fileSystemManager.stop();
    }
    if (this.mpmHost != null) {
      this.mpmHost.stop("server shutting down.");
    }
    if (this.regionServerTracker != null) {
      this.regionServerTracker.stop();
    }
  }

  private void createProcedureExecutor() throws IOException {
    MasterProcedureEnv procEnv = new MasterProcedureEnv(this);
    procedureStore =
      new RegionProcedureStore(this, masterRegion, new MasterProcedureEnv.FsUtilsLeaseRecovery(this));
    procedureStore.registerListener(new ProcedureStoreListener() {

      @Override
      public void abortProcess() {
        abort("The Procedure Store lost the lease", null);
      }
    });
    MasterProcedureScheduler procedureScheduler = procEnv.getProcedureScheduler();
    procedureExecutor = new ProcedureExecutor<>(conf, procEnv, procedureStore, procedureScheduler);
    configurationManager.registerObserver(procEnv);

    int cpus = Runtime.getRuntime().availableProcessors();
    final int numThreads = conf.getInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, Math.max(
      (cpus > 0 ? cpus / 4 : 0), MasterProcedureConstants.DEFAULT_MIN_MASTER_PROCEDURE_THREADS));
    final boolean abortOnCorruption =
      conf.getBoolean(MasterProcedureConstants.EXECUTOR_ABORT_ON_CORRUPTION,
        MasterProcedureConstants.DEFAULT_EXECUTOR_ABORT_ON_CORRUPTION);
    procedureStore.start(numThreads);
    // Just initialize it but do not start the workers, we will start the workers later by calling
    // startProcedureExecutor. See the javadoc for finishActiveMasterInitialization for more
    // details.
    procedureExecutor.init(numThreads, abortOnCorruption);
    if (!procEnv.getRemoteDispatcher().start()) {
      throw new HBaseIOException("Failed start of remote dispatcher");
    }
  }

  private void startProcedureExecutor() throws IOException {
    procedureExecutor.startWorkers();
  }

  /**
   * Turn on/off Snapshot Cleanup Chore
   *
   * @param on indicates whether Snapshot Cleanup Chore is to be run
   */
  void switchSnapshotCleanup(final boolean on, final boolean synchronous) {
    if (synchronous) {
      synchronized (this.snapshotCleanerChore) {
        switchSnapshotCleanup(on);
      }
    } else {
      switchSnapshotCleanup(on);
    }
  }

  private void switchSnapshotCleanup(final boolean on) {
    try {
      snapshotCleanupTracker.setSnapshotCleanupEnabled(on);
      if (on) {
        if (!getChoreService().isChoreScheduled(this.snapshotCleanerChore)) {
          getChoreService().scheduleChore(this.snapshotCleanerChore);
        }
      } else {
        getChoreService().cancelChore(this.snapshotCleanerChore);
      }
    } catch (KeeperException e) {
      LOG.error("Error updating snapshot cleanup mode to {}", on, e);
    }
  }


  private void stopProcedureExecutor() {
    if (procedureExecutor != null) {
      configurationManager.deregisterObserver(procedureExecutor.getEnvironment());
      procedureExecutor.getEnvironment().getRemoteDispatcher().stop();
      procedureExecutor.stop();
      procedureExecutor.join();
      procedureExecutor = null;
    }

    if (procedureStore != null) {
      procedureStore.stop(isAborted());
      procedureStore = null;
    }
  }

  private void stopChores() {
    ChoreService choreService = getChoreService();
    if (choreService != null) {
      choreService.cancelChore(this.mobFileCleanerChore);
      choreService.cancelChore(this.mobFileCompactionChore);
      choreService.cancelChore(this.balancerChore);
      choreService.cancelChore(this.normalizerChore);
      choreService.cancelChore(this.clusterStatusChore);
      choreService.cancelChore(this.catalogJanitorChore);
      choreService.cancelChore(this.clusterStatusPublisherChore);
      choreService.cancelChore(this.snapshotQuotaChore);
      choreService.cancelChore(this.logCleaner);
      choreService.cancelChore(this.hfileCleaner);
      choreService.cancelChore(this.replicationBarrierCleaner);
      choreService.cancelChore(this.snapshotCleanerChore);
      choreService.cancelChore(this.hbckChore);
      choreService.cancelChore(this.regionsRecoveryChore);
    }
  }

  /**
   * @return Get remote side's InetAddress
   */
  InetAddress getRemoteInetAddress(final int port,
      final long serverStartCode) throws UnknownHostException {
    // Do it out here in its own little method so can fake an address when
    // mocking up in tests.
    InetAddress ia = RpcServer.getRemoteIp();

    // The call could be from the local regionserver,
    // in which case, there is no remote address.
    if (ia == null && serverStartCode == startcode) {
      InetSocketAddress isa = rpcServices.getSocketAddress();
      if (isa != null && isa.getPort() == port) {
        ia = isa.getAddress();
      }
    }
    return ia;
  }

  /**
   * @return Maximum time we should run balancer for
   */
  private int getMaxBalancingTime() {
    // if max balancing time isn't set, defaulting it to period time
    int maxBalancingTime = getConfiguration().getInt(HConstants.HBASE_BALANCER_MAX_BALANCING,
      getConfiguration()
        .getInt(HConstants.HBASE_BALANCER_PERIOD, HConstants.DEFAULT_HBASE_BALANCER_PERIOD));
    return maxBalancingTime;
  }

  /**
   * @return Maximum number of regions in transition
   */
  private int getMaxRegionsInTransition() {
    int numRegions = this.assignmentManager.getRegionStates().getRegionAssignments().size();
    return Math.max((int) Math.floor(numRegions * this.maxRitPercent), 1);
  }

  /**
   * It first sleep to the next balance plan start time. Meanwhile, throttling by the max
   * number regions in transition to protect availability.
   * @param nextBalanceStartTime The next balance plan start time
   * @param maxRegionsInTransition max number of regions in transition
   * @param cutoffTime when to exit balancer
   */
  private void balanceThrottling(long nextBalanceStartTime, int maxRegionsInTransition,
      long cutoffTime) {
    boolean interrupted = false;

    // Sleep to next balance plan start time
    // But if there are zero regions in transition, it can skip sleep to speed up.
    while (!interrupted && System.currentTimeMillis() < nextBalanceStartTime
        && this.assignmentManager.getRegionStates().hasRegionsInTransition()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ie) {
        interrupted = true;
      }
    }

    // Throttling by max number regions in transition
    while (!interrupted
        && maxRegionsInTransition > 0
        && this.assignmentManager.getRegionStates().getRegionsInTransitionCount()
        >= maxRegionsInTransition && System.currentTimeMillis() <= cutoffTime) {
      try {
        // sleep if the number of regions in transition exceeds the limit
        Thread.sleep(100);
      } catch (InterruptedException ie) {
        interrupted = true;
      }
    }

    if (interrupted) Thread.currentThread().interrupt();
  }

  public boolean balance() throws IOException {
    return balance(false);
  }

  /**
   * Checks master state before initiating action over region topology.
   * @param action the name of the action under consideration, for logging.
   * @return {@code true} when the caller should exit early, {@code false} otherwise.
   */
  private boolean skipRegionManagementAction(final String action) {
    if (!isInitialized()) {
      LOG.debug("Master has not been initialized, don't run {}.", action);
      return true;
    }
    if (this.getServerManager().isClusterShutdown()) {
      LOG.info("Cluster is shutting down, don't run {}.", action);
      return true;
    }
    if (isInMaintenanceMode()) {
      LOG.info("Master is in maintenance mode, don't run {}.", action);
      return true;
    }
    return false;
  }

  public boolean balance(boolean force) throws IOException {
    if (loadBalancerTracker == null || !loadBalancerTracker.isBalancerOn()) {
      return false;
    }
    if (skipRegionManagementAction("balancer")) {
      return false;
    }

    synchronized (this.balancer) {
        // Only allow one balance run at at time.
      if (this.assignmentManager.hasRegionsInTransition()) {
        List<RegionStateNode> regionsInTransition = assignmentManager.getRegionsInTransition();
        // if hbase:meta region is in transition, result of assignment cannot be recorded
        // ignore the force flag in that case
        boolean metaInTransition = assignmentManager.isMetaRegionInTransition();
        String prefix = force && !metaInTransition ? "R" : "Not r";
        List<RegionStateNode> toPrint = regionsInTransition;
        int max = 5;
        boolean truncated = false;
        if (regionsInTransition.size() > max) {
          toPrint = regionsInTransition.subList(0, max);
          truncated = true;
        }
        LOG.info(prefix + "unning balancer because " + regionsInTransition.size() +
          " region(s) in transition: " + toPrint + (truncated? "(truncated list)": ""));
        if (!force || metaInTransition) return false;
      }
      if (this.serverManager.areDeadServersInProgress()) {
        LOG.info("Not running balancer because processing dead regionserver(s): " +
          this.serverManager.getDeadServers());
        return false;
      }

      if (this.cpHost != null) {
        try {
          if (this.cpHost.preBalance()) {
            LOG.debug("Coprocessor bypassing balancer request");
            return false;
          }
        } catch (IOException ioe) {
          LOG.error("Error invoking master coprocessor preBalance()", ioe);
          return false;
        }
      }

      Map<TableName, Map<ServerName, List<RegionInfo>>> assignments =
        this.assignmentManager.getRegionStates()
          .getAssignmentsForBalancer(tableStateManager, this.serverManager.getOnlineServersList());
      for (Map<ServerName, List<RegionInfo>> serverMap : assignments.values()) {
        serverMap.keySet().removeAll(this.serverManager.getDrainingServersList());
      }

      //Give the balancer the current cluster state.
      this.balancer.setClusterMetrics(getClusterMetricsWithoutCoprocessor());

      List<RegionPlan> plans = this.balancer.balanceCluster(assignments);

      if (skipRegionManagementAction("balancer")) {
        // make one last check that the cluster isn't shutting down before proceeding.
        return false;
      }

      List<RegionPlan> sucRPs = executeRegionPlansWithThrottling(plans);

      if (this.cpHost != null) {
        try {
          this.cpHost.postBalance(sucRPs);
        } catch (IOException ioe) {
          // balancing already succeeded so don't change the result
          LOG.error("Error invoking master coprocessor postBalance()", ioe);
        }
      }
    }
    // If LoadBalancer did not generate any plans, it means the cluster is already balanced.
    // Return true indicating a success.
    return true;
  }

  /**
   * Execute region plans with throttling
   * @param plans to execute
   * @return succeeded plans
   */
  public List<RegionPlan> executeRegionPlansWithThrottling(List<RegionPlan> plans) {
    List<RegionPlan> successRegionPlans = new ArrayList<>();
    int maxRegionsInTransition = getMaxRegionsInTransition();
    long balanceStartTime = System.currentTimeMillis();
    long cutoffTime = balanceStartTime + this.maxBalancingTime;
    int rpCount = 0;  // number of RegionPlans balanced so far
    if (plans != null && !plans.isEmpty()) {
      int balanceInterval = this.maxBalancingTime / plans.size();
      LOG.info("Balancer plans size is " + plans.size() + ", the balance interval is "
          + balanceInterval + " ms, and the max number regions in transition is "
          + maxRegionsInTransition);

      for (RegionPlan plan: plans) {
        LOG.info("balance " + plan);
        //TODO: bulk assign
        try {
          this.assignmentManager.moveAsync(plan);
        } catch (HBaseIOException hioe) {
          //should ignore failed plans here, avoiding the whole balance plans be aborted
          //later calls of balance() can fetch up the failed and skipped plans
          LOG.warn("Failed balance plan {}, skipping...", plan, hioe);
        }
        //rpCount records balance plans processed, does not care if a plan succeeds
        rpCount++;
        successRegionPlans.add(plan);

        if (this.maxBalancingTime > 0) {
          balanceThrottling(balanceStartTime + rpCount * balanceInterval, maxRegionsInTransition,
            cutoffTime);
        }

        // if performing next balance exceeds cutoff time, exit the loop
        if (this.maxBalancingTime > 0 && rpCount < plans.size()
          && System.currentTimeMillis() > cutoffTime) {
          // TODO: After balance, there should not be a cutoff time (keeping it as
          // a security net for now)
          LOG.debug("No more balancing till next balance run; maxBalanceTime="
              + this.maxBalancingTime);
          break;
        }
      }
    }
    return successRegionPlans;
  }

  @Override
  public RegionNormalizer getRegionNormalizer() {
    return this.normalizer;
  }

  /**
   * Perform normalization of cluster (invoked by {@link RegionNormalizerChore}).
   *
   * @return true if an existing normalization was already in progress, or if a new normalization
   *   was performed successfully; false otherwise (specifically, if HMaster finished initializing
   *   or normalization is globally disabled).
   */
  public boolean normalizeRegions() throws IOException {
    if (regionNormalizerTracker == null || !regionNormalizerTracker.isNormalizerOn()) {
      LOG.debug("Region normalization is disabled, don't run region normalizer.");
      return false;
    }
    if (skipRegionManagementAction("region normalizer")) {
      return false;
    }
    if (assignmentManager.hasRegionsInTransition()) {
      return false;
    }

    if (!normalizationInProgressLock.tryLock()) {
      // Don't run the normalizer concurrently
      LOG.info("Normalization already in progress. Skipping request.");
      return true;
    }

    try {
      final List<TableName> allEnabledTables =
        new ArrayList<>(tableStateManager.getTablesInStates(TableState.State.ENABLED));
      Collections.shuffle(allEnabledTables);

      final List<Long> submittedPlanProcIds = new ArrayList<>();
      for (TableName table : allEnabledTables) {
        if (table.isSystemTable()) {
          continue;
        }
        final TableDescriptor tblDesc = getTableDescriptors().get(table);
        if (tblDesc != null && !tblDesc.isNormalizationEnabled()) {
          LOG.debug(
            "Skipping table {} because normalization is disabled in its table properties.", table);
          continue;
        }

        // make one last check that the cluster isn't shutting down before proceeding.
        if (skipRegionManagementAction("region normalizer")) {
          return false;
        }

        final List<NormalizationPlan> plans = normalizer.computePlansForTable(table);
        if (CollectionUtils.isEmpty(plans)) {
          LOG.debug("No normalization required for table {}.", table);
          continue;
        }

        // as of this writing, `plan.submit()` is non-blocking and uses Async Admin APIs to
        // submit task , so there's no artificial rate-
        // limiting of merge/split requests due to this serial loop.
        for (NormalizationPlan plan : plans) {
          long procId = plan.submit(this);
          submittedPlanProcIds.add(procId);
          if (plan.getType() == PlanType.SPLIT) {
            splitPlanCount++;
          } else if (plan.getType() == PlanType.MERGE) {
            mergePlanCount++;
          }
        }
        int totalPlansSubmitted = submittedPlanProcIds.size();
        if (totalPlansSubmitted > 0 && LOG.isDebugEnabled()) {
          LOG.debug("Normalizer plans submitted. Total plans count: {} , procID list: {}",
            totalPlansSubmitted, submittedPlanProcIds);
        }
      }
    } finally {
      normalizationInProgressLock.unlock();
    }
    return true;
  }

  /**
   * @return Client info for use as prefix on an audit log string; who did an action
   */
  @Override
  public String getClientIdAuditPrefix() {
    return "Client=" + RpcServer.getRequestUserName().orElse(null)
        + "/" + RpcServer.getRemoteAddress().orElse(null);
  }

  /**
   * Switch for the background CatalogJanitor thread.
   * Used for testing.  The thread will continue to run.  It will just be a noop
   * if disabled.
   * @param b If false, the catalog janitor won't do anything.
   */
  public void setCatalogJanitorEnabled(final boolean b) {
    this.catalogJanitorChore.setEnabled(b);
  }

  @Override
  public long mergeRegions(
      final RegionInfo[] regionsToMerge,
      final boolean forcible,
      final long ng,
      final long nonce) throws IOException {
    checkInitialized();

    if (!isSplitOrMergeEnabled(MasterSwitchType.MERGE)) {
      String regionsStr = Arrays.deepToString(regionsToMerge);
      LOG.warn("Merge switch is off! skip merge of " + regionsStr);
      throw new DoNotRetryIOException("Merge of " + regionsStr +
          " failed because merge switch is off");
    }

    final String mergeRegionsStr = Arrays.stream(regionsToMerge).map(RegionInfo::getEncodedName)
      .collect(Collectors.joining(", "));
    return MasterProcedureUtil.submitProcedure(new NonceProcedureRunnable(this, ng, nonce) {
      @Override
      protected void run() throws IOException {
        getMaster().getMasterCoprocessorHost().preMergeRegions(regionsToMerge);
        String aid = getClientIdAuditPrefix();
        LOG.info("{} merge regions {}", aid, mergeRegionsStr);
        submitProcedure(new MergeTableRegionsProcedure(procedureExecutor.getEnvironment(),
            regionsToMerge, forcible));
        getMaster().getMasterCoprocessorHost().postMergeRegions(regionsToMerge);
      }

      @Override
      protected String getDescription() {
        return "MergeTableProcedure";
      }
    });
  }

  @Override
  public long splitRegion(final RegionInfo regionInfo, final byte[] splitRow,
      final long nonceGroup, final long nonce)
  throws IOException {
    checkInitialized();

    if (!isSplitOrMergeEnabled(MasterSwitchType.SPLIT)) {
      LOG.warn("Split switch is off! skip split of " + regionInfo);
      throw new DoNotRetryIOException("Split region " + regionInfo.getRegionNameAsString() +
          " failed due to split switch off");
    }

    return MasterProcedureUtil.submitProcedure(
        new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
      @Override
      protected void run() throws IOException {
        getMaster().getMasterCoprocessorHost().preSplitRegion(regionInfo.getTable(), splitRow);
        LOG.info(getClientIdAuditPrefix() + " split " + regionInfo.getRegionNameAsString());

        // Execute the operation asynchronously
        submitProcedure(getAssignmentManager().createSplitProcedure(regionInfo, splitRow));
      }

      @Override
      protected String getDescription() {
        return "SplitTableProcedure";
      }
    });
  }

  private void warmUpRegion(ServerName server, RegionInfo region) {
    FutureUtils.addListener(asyncClusterConnection.getRegionServerAdmin(server)
      .warmupRegion(RequestConverter.buildWarmupRegionRequest(region)), (r, e) -> {
        if (e != null) {
          LOG.warn("Failed to warm up region {} on server {}", region, server, e);
        }
      });
  }

  // Public so can be accessed by tests. Blocks until move is done.
  // Replace with an async implementation from which you can get
  // a success/failure result.
  @VisibleForTesting
  public void move(final byte[] encodedRegionName, byte[] destServerName) throws IOException {
    RegionState regionState = assignmentManager.getRegionStates().
      getRegionState(Bytes.toString(encodedRegionName));

    RegionInfo hri;
    if (regionState != null) {
      hri = regionState.getRegion();
    } else {
      throw new UnknownRegionException(Bytes.toStringBinary(encodedRegionName));
    }

    ServerName dest;
    List<ServerName> exclude = hri.getTable().isSystemTable() ? assignmentManager.getExcludedServersForSystemTable()
        : new ArrayList<>(1);
    if (destServerName != null && exclude.contains(ServerName.valueOf(Bytes.toString(destServerName)))) {
      LOG.info(
          Bytes.toString(encodedRegionName) + " can not move to " + Bytes.toString(destServerName)
              + " because the server is in exclude list");
      destServerName = null;
    }
    if (destServerName == null || destServerName.length == 0) {
      LOG.info("Passed destination servername is null/empty so " +
        "choosing a server at random");
      exclude.add(regionState.getServerName());
      final List<ServerName> destServers = this.serverManager.createDestinationServersList(exclude);
      dest = balancer.randomAssignment(hri, destServers);
      if (dest == null) {
        LOG.debug("Unable to determine a plan to assign " + hri);
        return;
      }
    } else {
      ServerName candidate = ServerName.valueOf(Bytes.toString(destServerName));
      dest = balancer.randomAssignment(hri, Lists.newArrayList(candidate));
      if (dest == null) {
        LOG.debug("Unable to determine a plan to assign " + hri);
        return;
      }
      // TODO: deal with table on master for rs group.
      if (dest.equals(serverName)) {
        // To avoid unnecessary region moving later by balancer. Don't put user
        // regions on master.
        LOG.debug("Skipping move of region " + hri.getRegionNameAsString() +
          " to avoid unnecessary region moving later by load balancer," +
          " because it should not be on master");
        return;
      }
    }

    if (dest.equals(regionState.getServerName())) {
      LOG.debug("Skipping move of region " + hri.getRegionNameAsString()
        + " because region already assigned to the same server " + dest + ".");
      return;
    }

    // Now we can do the move
    RegionPlan rp = new RegionPlan(hri, regionState.getServerName(), dest);
    assert rp.getDestination() != null: rp.toString() + " " + dest;

    try {
      checkInitialized();
      if (this.cpHost != null) {
        this.cpHost.preMove(hri, rp.getSource(), rp.getDestination());
      }

      TransitRegionStateProcedure proc =
        this.assignmentManager.createMoveRegionProcedure(rp.getRegionInfo(), rp.getDestination());
      // Warmup the region on the destination before initiating the move.
      // A region server could reject the close request because it either does not
      // have the specified region or the region is being split.
      warmUpRegion(rp.getDestination(), hri);

      LOG.info(getClientIdAuditPrefix() + " move " + rp + ", running balancer");
      Future<byte[]> future = ProcedureSyncWait.submitProcedure(this.procedureExecutor, proc);
      try {
        // Is this going to work? Will we throw exception on error?
        // TODO: CompletableFuture rather than this stunted Future.
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new HBaseIOException(e);
      }
      if (this.cpHost != null) {
        this.cpHost.postMove(hri, rp.getSource(), rp.getDestination());
      }
    } catch (IOException ioe) {
      if (ioe instanceof HBaseIOException) {
        throw (HBaseIOException)ioe;
      }
      throw new HBaseIOException(ioe);
    }
  }

  @Override
  public long createTable(final TableDescriptor tableDescriptor, final byte[][] splitKeys,
      final long nonceGroup, final long nonce) throws IOException {
    checkInitialized();
    TableDescriptor desc = getMasterCoprocessorHost().preCreateTableRegionsInfos(tableDescriptor);
    if (desc == null) {
      throw new IOException("Creation for " + tableDescriptor + " is canceled by CP");
    }
    String namespace = desc.getTableName().getNamespaceAsString();
    this.clusterSchemaService.getNamespace(namespace);

    RegionInfo[] newRegions = ModifyRegionUtils.createRegionInfos(desc, splitKeys);
    TableDescriptorChecker.sanityCheck(conf, desc);

    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
        @Override
        protected void run() throws IOException {
          getMaster().getMasterCoprocessorHost().preCreateTable(desc, newRegions);

          LOG.info(getClientIdAuditPrefix() + " create " + desc);

          // TODO: We can handle/merge duplicate requests, and differentiate the case of
          // TableExistsException by saying if the schema is the same or not.
          //
          // We need to wait for the procedure to potentially fail due to "prepare" sanity
          // checks. This will block only the beginning of the procedure. See HBASE-19953.
          ProcedurePrepareLatch latch = ProcedurePrepareLatch.createBlockingLatch();
          submitProcedure(
            new CreateTableProcedure(procedureExecutor.getEnvironment(), desc, newRegions, latch));
          latch.await();

          getMaster().getMasterCoprocessorHost().postCreateTable(desc, newRegions);
        }

        @Override
        protected String getDescription() {
          return "CreateTableProcedure";
        }
      });
  }

  @Override
  public long createSystemTable(final TableDescriptor tableDescriptor) throws IOException {
    if (isStopped()) {
      throw new MasterNotRunningException();
    }

    TableName tableName = tableDescriptor.getTableName();
    if (!(tableName.isSystemTable())) {
      throw new IllegalArgumentException(
        "Only system table creation can use this createSystemTable API");
    }

    RegionInfo[] newRegions = ModifyRegionUtils.createRegionInfos(tableDescriptor, null);

    LOG.info(getClientIdAuditPrefix() + " create " + tableDescriptor);

    // This special create table is called locally to master.  Therefore, no RPC means no need
    // to use nonce to detect duplicated RPC call.
    long procId = this.procedureExecutor.submitProcedure(
      new CreateTableProcedure(procedureExecutor.getEnvironment(), tableDescriptor, newRegions));

    return procId;
  }

  private void startActiveMasterManager(int infoPort) throws KeeperException {
    String backupZNode = ZNodePaths.joinZNode(
      zooKeeper.getZNodePaths().backupMasterAddressesZNode, serverName.toString());
    /*
    * Add a ZNode for ourselves in the backup master directory since we
    * may not become the active master. If so, we want the actual active
    * master to know we are backup masters, so that it won't assign
    * regions to us if so configured.
    *
    * If we become the active master later, ActiveMasterManager will delete
    * this node explicitly.  If we crash before then, ZooKeeper will delete
    * this node for us since it is ephemeral.
    */
    LOG.info("Adding backup master ZNode " + backupZNode);
    if (!MasterAddressTracker.setMasterAddress(zooKeeper, backupZNode, serverName, infoPort)) {
      LOG.warn("Failed create of " + backupZNode + " by " + serverName);
    }
    this.activeMasterManager.setInfoPort(infoPort);
    int timeout = conf.getInt(HConstants.ZK_SESSION_TIMEOUT, HConstants.DEFAULT_ZK_SESSION_TIMEOUT);
    // If we're a backup master, stall until a primary to write this address
    if (conf.getBoolean(HConstants.MASTER_TYPE_BACKUP, HConstants.DEFAULT_MASTER_TYPE_BACKUP)) {
      LOG.debug("HMaster started in backup mode. Stalling until master znode is written.");
      // This will only be a minute or so while the cluster starts up,
      // so don't worry about setting watches on the parent znode
      while (!activeMasterManager.hasActiveMaster()) {
        LOG.debug("Waiting for master address and cluster state znode to be written.");
        Threads.sleep(timeout);
      }
    }
    MonitoredTask status = TaskMonitor.get().createStatus("Master startup");
    status.setDescription("Master startup");
    try {
      if (activeMasterManager.blockUntilBecomingActiveMaster(timeout, status)) {
        finishActiveMasterInitialization(status);
      }
    } catch (Throwable t) {
      status.setStatus("Failed to become active: " + t.getMessage());
      LOG.error(HBaseMarkers.FATAL, "Failed to become active master", t);
      // HBASE-5680: Likely hadoop23 vs hadoop 20.x/1.x incompatibility
      if (t instanceof NoClassDefFoundError && t.getMessage().
          contains("org/apache/hadoop/hdfs/protocol/HdfsConstants$SafeModeAction")) {
        // improved error message for this special case
        abort("HBase is having a problem with its Hadoop jars.  You may need to recompile " +
          "HBase against Hadoop version " + org.apache.hadoop.util.VersionInfo.getVersion() +
          " or change your hadoop jars to start properly", t);
      } else {
        abort("Unhandled exception. Starting shutdown.", t);
      }
    } finally {
      status.cleanup();
    }
  }

  private static boolean isCatalogTable(final TableName tableName) {
    return tableName.equals(TableName.META_TABLE_NAME);
  }

  @Override
  public long deleteTable(
      final TableName tableName,
      final long nonceGroup,
      final long nonce) throws IOException {
    checkInitialized();

    return MasterProcedureUtil.submitProcedure(
        new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
      @Override
      protected void run() throws IOException {
        getMaster().getMasterCoprocessorHost().preDeleteTable(tableName);

        LOG.info(getClientIdAuditPrefix() + " delete " + tableName);

        // TODO: We can handle/merge duplicate request
        //
        // We need to wait for the procedure to potentially fail due to "prepare" sanity
        // checks. This will block only the beginning of the procedure. See HBASE-19953.
        ProcedurePrepareLatch latch = ProcedurePrepareLatch.createBlockingLatch();
        submitProcedure(new DeleteTableProcedure(procedureExecutor.getEnvironment(),
            tableName, latch));
        latch.await();

        getMaster().getMasterCoprocessorHost().postDeleteTable(tableName);
      }

      @Override
      protected String getDescription() {
        return "DeleteTableProcedure";
      }
    });
  }

  @Override
  public long truncateTable(
      final TableName tableName,
      final boolean preserveSplits,
      final long nonceGroup,
      final long nonce) throws IOException {
    checkInitialized();

    return MasterProcedureUtil.submitProcedure(
        new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
      @Override
      protected void run() throws IOException {
        getMaster().getMasterCoprocessorHost().preTruncateTable(tableName);

        LOG.info(getClientIdAuditPrefix() + " truncate " + tableName);
        ProcedurePrepareLatch latch = ProcedurePrepareLatch.createLatch(2, 0);
        submitProcedure(new TruncateTableProcedure(procedureExecutor.getEnvironment(),
            tableName, preserveSplits, latch));
        latch.await();

        getMaster().getMasterCoprocessorHost().postTruncateTable(tableName);
      }

      @Override
      protected String getDescription() {
        return "TruncateTableProcedure";
      }
    });
  }

  @Override
  public long addColumn(final TableName tableName, final ColumnFamilyDescriptor column,
      final long nonceGroup, final long nonce) throws IOException {
    checkInitialized();
    checkTableExists(tableName);

    return modifyTable(tableName, new TableDescriptorGetter() {

      @Override
      public TableDescriptor get() throws IOException {
        TableDescriptor old = getTableDescriptors().get(tableName);
        if (old.hasColumnFamily(column.getName())) {
          throw new InvalidFamilyOperationException("Column family '" + column.getNameAsString()
              + "' in table '" + tableName + "' already exists so cannot be added");
        }

        return TableDescriptorBuilder.newBuilder(old).setColumnFamily(column).build();
      }
    }, nonceGroup, nonce, true);
  }

  /**
   * Implement to return TableDescriptor after pre-checks
   */
  protected interface TableDescriptorGetter {
    TableDescriptor get() throws IOException;
  }

  @Override
  public long modifyColumn(final TableName tableName, final ColumnFamilyDescriptor descriptor,
      final long nonceGroup, final long nonce) throws IOException {
    checkInitialized();
    checkTableExists(tableName);
    return modifyTable(tableName, new TableDescriptorGetter() {

      @Override
      public TableDescriptor get() throws IOException {
        TableDescriptor old = getTableDescriptors().get(tableName);
        if (!old.hasColumnFamily(descriptor.getName())) {
          throw new InvalidFamilyOperationException("Family '" + descriptor.getNameAsString()
              + "' does not exist, so it cannot be modified");
        }

        return TableDescriptorBuilder.newBuilder(old).modifyColumnFamily(descriptor).build();
      }
    }, nonceGroup, nonce, true);
  }

  @Override
  public long deleteColumn(final TableName tableName, final byte[] columnName,
      final long nonceGroup, final long nonce) throws IOException {
    checkInitialized();
    checkTableExists(tableName);

    return modifyTable(tableName, new TableDescriptorGetter() {

      @Override
      public TableDescriptor get() throws IOException {
        TableDescriptor old = getTableDescriptors().get(tableName);

        if (!old.hasColumnFamily(columnName)) {
          throw new InvalidFamilyOperationException("Family '" + Bytes.toString(columnName)
              + "' does not exist, so it cannot be deleted");
        }
        if (old.getColumnFamilyCount() == 1) {
          throw new InvalidFamilyOperationException("Family '" + Bytes.toString(columnName)
              + "' is the only column family in the table, so it cannot be deleted");
        }
        return TableDescriptorBuilder.newBuilder(old).removeColumnFamily(columnName).build();
      }
    }, nonceGroup, nonce, true);
  }

  @Override
  public long enableTable(final TableName tableName, final long nonceGroup, final long nonce)
      throws IOException {
    checkInitialized();

    return MasterProcedureUtil.submitProcedure(
        new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
      @Override
      protected void run() throws IOException {
        getMaster().getMasterCoprocessorHost().preEnableTable(tableName);

        // Normally, it would make sense for this authorization check to exist inside
        // AccessController, but because the authorization check is done based on internal state
        // (rather than explicit permissions) we'll do the check here instead of in the
        // coprocessor.
        MasterQuotaManager quotaManager = getMasterQuotaManager();
        if (quotaManager != null) {
          if (quotaManager.isQuotaInitialized()) {
              SpaceQuotaSnapshot currSnapshotOfTable =
                  QuotaTableUtil.getCurrentSnapshotFromQuotaTable(getConnection(), tableName);
              if (currSnapshotOfTable != null) {
                SpaceQuotaStatus quotaStatus = currSnapshotOfTable.getQuotaStatus();
                if (quotaStatus.isInViolation()
                    && SpaceViolationPolicy.DISABLE == quotaStatus.getPolicy().orElse(null)) {
                throw new AccessDeniedException("Enabling the table '" + tableName
                    + "' is disallowed due to a violated space quota.");
              }
            }
          } else if (LOG.isTraceEnabled()) {
            LOG.trace("Unable to check for space quotas as the MasterQuotaManager is not enabled");
          }
        }

        LOG.info(getClientIdAuditPrefix() + " enable " + tableName);

        // Execute the operation asynchronously - client will check the progress of the operation
        // In case the request is from a <1.1 client before returning,
        // we want to make sure that the table is prepared to be
        // enabled (the table is locked and the table state is set).
        // Note: if the procedure throws exception, we will catch it and rethrow.
        final ProcedurePrepareLatch prepareLatch = ProcedurePrepareLatch.createLatch();
        submitProcedure(new EnableTableProcedure(procedureExecutor.getEnvironment(),
            tableName, prepareLatch));
        prepareLatch.await();

        getMaster().getMasterCoprocessorHost().postEnableTable(tableName);
      }

      @Override
      protected String getDescription() {
        return "EnableTableProcedure";
      }
    });
  }

  @Override
  public long disableTable(final TableName tableName, final long nonceGroup, final long nonce)
      throws IOException {
    checkInitialized();

    return MasterProcedureUtil.submitProcedure(
        new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
      @Override
      protected void run() throws IOException {
        getMaster().getMasterCoprocessorHost().preDisableTable(tableName);

        LOG.info(getClientIdAuditPrefix() + " disable " + tableName);

        // Execute the operation asynchronously - client will check the progress of the operation
        // In case the request is from a <1.1 client before returning,
        // we want to make sure that the table is prepared to be
        // enabled (the table is locked and the table state is set).
        // Note: if the procedure throws exception, we will catch it and rethrow.
        //
        // We need to wait for the procedure to potentially fail due to "prepare" sanity
        // checks. This will block only the beginning of the procedure. See HBASE-19953.
        final ProcedurePrepareLatch prepareLatch = ProcedurePrepareLatch.createBlockingLatch();
        submitProcedure(new DisableTableProcedure(procedureExecutor.getEnvironment(),
            tableName, false, prepareLatch));
        prepareLatch.await();

        getMaster().getMasterCoprocessorHost().postDisableTable(tableName);
      }

      @Override
      protected String getDescription() {
        return "DisableTableProcedure";
      }
    });
  }

  private long modifyTable(final TableName tableName,
      final TableDescriptorGetter newDescriptorGetter, final long nonceGroup, final long nonce,
      final boolean shouldCheckDescriptor) throws IOException {
    return MasterProcedureUtil
        .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
          @Override
          protected void run() throws IOException {
            TableDescriptor oldDescriptor = getMaster().getTableDescriptors().get(tableName);
            TableDescriptor newDescriptor = getMaster().getMasterCoprocessorHost()
                .preModifyTable(tableName, oldDescriptor, newDescriptorGetter.get());
            TableDescriptorChecker.sanityCheck(conf, newDescriptor);
            LOG.info("{} modify table {} from {} to {}", getClientIdAuditPrefix(), tableName,
                oldDescriptor, newDescriptor);

            // Execute the operation synchronously - wait for the operation completes before
            // continuing.
            //
            // We need to wait for the procedure to potentially fail due to "prepare" sanity
            // checks. This will block only the beginning of the procedure. See HBASE-19953.
            ProcedurePrepareLatch latch = ProcedurePrepareLatch.createBlockingLatch();
            submitProcedure(new ModifyTableProcedure(procedureExecutor.getEnvironment(),
                newDescriptor, latch, oldDescriptor, shouldCheckDescriptor));
            latch.await();

            getMaster().getMasterCoprocessorHost().postModifyTable(tableName, oldDescriptor,
              newDescriptor);
          }

          @Override
          protected String getDescription() {
            return "ModifyTableProcedure";
          }
        });

  }

  @Override
  public long modifyTable(final TableName tableName, final TableDescriptor newDescriptor,
      final long nonceGroup, final long nonce) throws IOException {
    checkInitialized();
    return modifyTable(tableName, new TableDescriptorGetter() {
      @Override
      public TableDescriptor get() throws IOException {
        return newDescriptor;
      }
    }, nonceGroup, nonce, false);

  }

  public long restoreSnapshot(final SnapshotDescription snapshotDesc,
      final long nonceGroup, final long nonce, final boolean restoreAcl) throws IOException {
    checkInitialized();
    getSnapshotManager().checkSnapshotSupport();

    // Ensure namespace exists. Will throw exception if non-known NS.
    final TableName dstTable = TableName.valueOf(snapshotDesc.getTable());
    getClusterSchema().getNamespace(dstTable.getNamespaceAsString());

    return MasterProcedureUtil.submitProcedure(
        new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
      @Override
      protected void run() throws IOException {
          setProcId(
            getSnapshotManager().restoreOrCloneSnapshot(snapshotDesc, getNonceKey(), restoreAcl));
      }

      @Override
      protected String getDescription() {
        return "RestoreSnapshotProcedure";
      }
    });
  }

  private void checkTableExists(final TableName tableName)
      throws IOException, TableNotFoundException {
    if (!MetaTableAccessor.tableExists(getConnection(), tableName)) {
      throw new TableNotFoundException(tableName);
    }
  }

  @Override
  public void checkTableModifiable(final TableName tableName)
      throws IOException, TableNotFoundException, TableNotDisabledException {
    if (isCatalogTable(tableName)) {
      throw new IOException("Can't modify catalog tables");
    }
    checkTableExists(tableName);
    TableState ts = getTableStateManager().getTableState(tableName);
    if (!ts.isDisabled()) {
      throw new TableNotDisabledException("Not DISABLED; " + ts);
    }
  }

  public ClusterMetrics getClusterMetricsWithoutCoprocessor() throws InterruptedIOException {
    return getClusterMetricsWithoutCoprocessor(EnumSet.allOf(Option.class));
  }

  public ClusterMetrics getClusterMetricsWithoutCoprocessor(EnumSet<Option> options)
      throws InterruptedIOException {
    ClusterMetricsBuilder builder = ClusterMetricsBuilder.newBuilder();
    // given that hbase1 can't submit the request with Option,
    // we return all information to client if the list of Option is empty.
    if (options.isEmpty()) {
      options = EnumSet.allOf(Option.class);
    }

    for (Option opt : options) {
      switch (opt) {
        case HBASE_VERSION: builder.setHBaseVersion(VersionInfo.getVersion()); break;
        case CLUSTER_ID: builder.setClusterId(getClusterId()); break;
        case MASTER: builder.setMasterName(getServerName()); break;
        case BACKUP_MASTERS: builder.setBackerMasterNames(getBackupMasters()); break;
        case LIVE_SERVERS: {
          if (serverManager != null) {
            builder.setLiveServerMetrics(serverManager.getOnlineServers().entrySet().stream()
              .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())));
          }
          break;
        }
        case DEAD_SERVERS: {
          if (serverManager != null) {
            builder.setDeadServerNames(new ArrayList<>(
              serverManager.getDeadServers().copyServerNames()));
          }
          break;
        }
        case MASTER_COPROCESSORS: {
          if (cpHost != null) {
            builder.setMasterCoprocessorNames(Arrays.asList(getMasterCoprocessors()));
          }
          break;
        }
        case REGIONS_IN_TRANSITION: {
          if (assignmentManager != null) {
            builder.setRegionsInTransition(assignmentManager.getRegionStates()
                .getRegionsStateInTransition());
          }
          break;
        }
        case BALANCER_ON: {
          if (loadBalancerTracker != null) {
            builder.setBalancerOn(loadBalancerTracker.isBalancerOn());
          }
          break;
        }
        case MASTER_INFO_PORT: {
          if (infoServer != null) {
            builder.setMasterInfoPort(infoServer.getPort());
          }
          break;
        }
        case SERVERS_NAME: {
          if (serverManager != null) {
            builder.setServerNames(serverManager.getOnlineServersList());
          }
          break;
        }
        case TABLE_TO_REGIONS_COUNT: {
          if (isActiveMaster() && isInitialized() && assignmentManager != null) {
            try {
              Map<TableName, RegionStatesCount> tableRegionStatesCountMap = new HashMap<>();
              Map<String, TableDescriptor> tableDescriptorMap = getTableDescriptors().getAll();
              for (TableDescriptor tableDescriptor : tableDescriptorMap.values()) {
                TableName tableName = tableDescriptor.getTableName();
                RegionStatesCount regionStatesCount = assignmentManager
                  .getRegionStatesCount(tableName);
                tableRegionStatesCountMap.put(tableName, regionStatesCount);
              }
              builder.setTableRegionStatesCount(tableRegionStatesCountMap);
            } catch (IOException e) {
              LOG.error("Error while populating TABLE_TO_REGIONS_COUNT for Cluster Metrics..", e);
            }
          }
          break;
        }
      }
    }
    return builder.build();
  }

  /**
   * @return cluster status
   */
  public ClusterMetrics getClusterMetrics() throws IOException {
    return getClusterMetrics(EnumSet.allOf(Option.class));
  }

  public ClusterMetrics getClusterMetrics(EnumSet<Option> options) throws IOException {
    if (cpHost != null) {
      cpHost.preGetClusterMetrics();
    }
    ClusterMetrics status = getClusterMetricsWithoutCoprocessor(options);
    if (cpHost != null) {
      cpHost.postGetClusterMetrics(status);
    }
    return status;
  }

  private List<ServerName> getBackupMasters() throws InterruptedIOException {
    // Build Set of backup masters from ZK nodes
    List<String> backupMasterStrings;
    try {
      backupMasterStrings = ZKUtil.listChildrenNoWatch(this.zooKeeper,
        this.zooKeeper.getZNodePaths().backupMasterAddressesZNode);
    } catch (KeeperException e) {
      LOG.warn(this.zooKeeper.prefix("Unable to list backup servers"), e);
      backupMasterStrings = null;
    }

    List<ServerName> backupMasters = Collections.emptyList();
    if (backupMasterStrings != null && !backupMasterStrings.isEmpty()) {
      backupMasters = new ArrayList<>(backupMasterStrings.size());
      for (String s: backupMasterStrings) {
        try {
          byte [] bytes;
          try {
            bytes = ZKUtil.getData(this.zooKeeper, ZNodePaths.joinZNode(
                this.zooKeeper.getZNodePaths().backupMasterAddressesZNode, s));
          } catch (InterruptedException e) {
            throw new InterruptedIOException();
          }
          if (bytes != null) {
            ServerName sn;
            try {
              sn = ProtobufUtil.parseServerNameFrom(bytes);
            } catch (DeserializationException e) {
              LOG.warn("Failed parse, skipping registering backup server", e);
              continue;
            }
            backupMasters.add(sn);
          }
        } catch (KeeperException e) {
          LOG.warn(this.zooKeeper.prefix("Unable to get information about " +
                   "backup servers"), e);
        }
      }
      Collections.sort(backupMasters, new Comparator<ServerName>() {
        @Override
        public int compare(ServerName s1, ServerName s2) {
          return s1.getServerName().compareTo(s2.getServerName());
        }});
    }
    return backupMasters;
  }

  /**
   * The set of loaded coprocessors is stored in a static set. Since it's
   * statically allocated, it does not require that HMaster's cpHost be
   * initialized prior to accessing it.
   * @return a String representation of the set of names of the loaded coprocessors.
   */
  public static String getLoadedCoprocessors() {
    return CoprocessorHost.getLoadedCoprocessors().toString();
  }

  /**
   * @return timestamp in millis when HMaster was started.
   */
  public long getMasterStartTime() {
    return startcode;
  }

  /**
   * @return timestamp in millis when HMaster became the active master.
   */
  public long getMasterActiveTime() {
    return masterActiveTime;
  }

  /**
   * @return timestamp in millis when HMaster finished becoming the active master
   */
  public long getMasterFinishedInitializationTime() {
    return masterFinishedInitializationTime;
  }

  public int getNumWALFiles() {
    return 0;
  }

  public ProcedureStore getProcedureStore() {
    return procedureStore;
  }

  public int getRegionServerInfoPort(final ServerName sn) {
    int port = this.serverManager.getInfoPort(sn);
    return port == 0 ? conf.getInt(HConstants.REGIONSERVER_INFO_PORT,
      HConstants.DEFAULT_REGIONSERVER_INFOPORT) : port;
  }

  @Override
  public String getRegionServerVersion(ServerName sn) {
    // Will return "0.0.0" if the server is not online to prevent move system region to unknown
    // version RS.
    return this.serverManager.getVersion(sn);
  }

  @Override
  public void checkIfShouldMoveSystemRegionAsync() {
    assignmentManager.checkIfShouldMoveSystemRegionAsync();
  }

  /**
   * @return array of coprocessor SimpleNames.
   */
  public String[] getMasterCoprocessors() {
    Set<String> masterCoprocessors = getMasterCoprocessorHost().getCoprocessors();
    return masterCoprocessors.toArray(new String[masterCoprocessors.size()]);
  }

  @Override
  public void abort(String reason, Throwable cause) {
    if (!setAbortRequested() || isStopped()) {
      LOG.debug("Abort called but aborted={}, stopped={}", isAborted(), isStopped());
      return;
    }
    if (cpHost != null) {
      // HBASE-4014: dump a list of loaded coprocessors.
      LOG.error(HBaseMarkers.FATAL, "Master server abort: loaded coprocessors are: " +
          getLoadedCoprocessors());
    }
    String msg = "***** ABORTING master " + this + ": " + reason + " *****";
    if (cause != null) {
      LOG.error(HBaseMarkers.FATAL, msg, cause);
    } else {
      LOG.error(HBaseMarkers.FATAL, msg);
    }

    try {
      stopMaster();
    } catch (IOException e) {
      LOG.error("Exception occurred while stopping master", e);
    }
  }

  @Override
  public ZKWatcher getZooKeeper() {
    return zooKeeper;
  }

  @Override
  public MasterCoprocessorHost getMasterCoprocessorHost() {
    return cpHost;
  }

  @Override
  public MasterQuotaManager getMasterQuotaManager() {
    return quotaManager;
  }

  @Override
  public ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return procedureExecutor;
  }

  @Override
  public ServerName getServerName() {
    return this.serverName;
  }

  @Override
  public AssignmentManager getAssignmentManager() {
    return this.assignmentManager;
  }

  @Override
  public CatalogJanitor getCatalogJanitor() {
    return this.catalogJanitorChore;
  }

  public MemoryBoundedLogMessageBuffer getRegionServerFatalLogBuffer() {
    return rsFatals;
  }

  /**
   * Shutdown the cluster.
   * Master runs a coordinated stop of all RegionServers and then itself.
   */
  public void shutdown() throws IOException {
    if (cpHost != null) {
      cpHost.preShutdown();
    }

    // Tell the servermanager cluster shutdown has been called. This makes it so when Master is
    // last running server, it'll stop itself. Next, we broadcast the cluster shutdown by setting
    // the cluster status as down. RegionServers will notice this change in state and will start
    // shutting themselves down. When last has exited, Master can go down.
    if (this.serverManager != null) {
      this.serverManager.shutdownCluster();
    }
    if (this.clusterStatusTracker != null) {
      try {
        this.clusterStatusTracker.setClusterDown();
      } catch (KeeperException e) {
        LOG.error("ZooKeeper exception trying to set cluster as down in ZK", e);
      }
    }
    // Stop the procedure executor. Will stop any ongoing assign, unassign, server crash etc.,
    // processing so we can go down.
    if (this.procedureExecutor != null) {
      this.procedureExecutor.stop();
    }
    // Shutdown our cluster connection. This will kill any hosted RPCs that might be going on;
    // this is what we want especially if the Master is in startup phase doing call outs to
    // hbase:meta, etc. when cluster is down. Without ths connection close, we'd have to wait on
    // the rpc to timeout.
    if (this.asyncClusterConnection != null) {
      this.asyncClusterConnection.close();
    }
  }

  public void stopMaster() throws IOException {
    if (cpHost != null) {
      cpHost.preStopMaster();
    }
    stop("Stopped by " + Thread.currentThread().getName());
  }

  @Override
  public void stop(String msg) {
    if (!isStopped()) {
      super.stop(msg);
      if (this.activeMasterManager != null) {
        this.activeMasterManager.stop();
      }
    }
  }

  @VisibleForTesting
  protected void checkServiceStarted() throws ServerNotRunningYetException {
    if (!serviceStarted) {
      throw new ServerNotRunningYetException("Server is not running yet");
    }
  }

  public static class MasterStoppedException extends DoNotRetryIOException {
    MasterStoppedException() {
      super();
    }
  }

  void checkInitialized() throws PleaseHoldException, ServerNotRunningYetException,
      MasterNotRunningException, MasterStoppedException {
    checkServiceStarted();
    if (!isInitialized()) {
      throw new PleaseHoldException("Master is initializing");
    }
    if (isStopped()) {
      throw new MasterStoppedException();
    }
  }

  /**
   * Report whether this master is currently the active master or not.
   * If not active master, we are parked on ZK waiting to become active.
   *
   * This method is used for testing.
   *
   * @return true if active master, false if not.
   */
  @Override
  public boolean isActiveMaster() {
    return activeMaster;
  }

  /**
   * Report whether this master has completed with its initialization and is
   * ready.  If ready, the master is also the active master.  A standby master
   * is never ready.
   *
   * This method is used for testing.
   *
   * @return true if master is ready to go, false if not.
   */
  @Override
  public boolean isInitialized() {
    return initialized.isReady();
  }

  /**
   * Report whether this master is in maintenance mode.
   *
   * @return true if master is in maintenanceMode
   */
  @Override
  public boolean isInMaintenanceMode() {
    return maintenanceMode;
  }

  @VisibleForTesting
  public void setInitialized(boolean isInitialized) {
    procedureExecutor.getEnvironment().setEventReady(initialized, isInitialized);
  }

  @Override
  public ProcedureEvent<?> getInitializedEvent() {
    return initialized;
  }

  /**
   * Compute the average load across all region servers.
   * Currently, this uses a very naive computation - just uses the number of
   * regions being served, ignoring stats about number of requests.
   * @return the average load
   */
  public double getAverageLoad() {
    if (this.assignmentManager == null) {
      return 0;
    }

    RegionStates regionStates = this.assignmentManager.getRegionStates();
    if (regionStates == null) {
      return 0;
    }
    return regionStates.getAverageLoad();
  }

  /*
   * @return the count of region split plans executed
   */
  public long getSplitPlanCount() {
    return splitPlanCount;
  }

  /*
   * @return the count of region merge plans executed
   */
  public long getMergePlanCount() {
    return mergePlanCount;
  }

  @Override
  public boolean registerService(Service instance) {
    /*
     * No stacking of instances is allowed for a single service name
     */
    Descriptors.ServiceDescriptor serviceDesc = instance.getDescriptorForType();
    String serviceName = CoprocessorRpcUtils.getServiceName(serviceDesc);
    if (coprocessorServiceHandlers.containsKey(serviceName)) {
      LOG.error("Coprocessor service "+serviceName+
          " already registered, rejecting request from "+instance
      );
      return false;
    }

    coprocessorServiceHandlers.put(serviceName, instance);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Registered master coprocessor service: service="+serviceName);
    }
    return true;
  }

  /**
   * Utility for constructing an instance of the passed HMaster class.
   * @param masterClass
   * @return HMaster instance.
   */
  public static HMaster constructMaster(Class<? extends HMaster> masterClass,
      final Configuration conf)  {
    try {
      Constructor<? extends HMaster> c = masterClass.getConstructor(Configuration.class);
      return c.newInstance(conf);
    } catch(Exception e) {
      Throwable error = e;
      if (e instanceof InvocationTargetException &&
          ((InvocationTargetException)e).getTargetException() != null) {
        error = ((InvocationTargetException)e).getTargetException();
      }
      throw new RuntimeException("Failed construction of Master: " + masterClass.toString() + ". "
        , error);
    }
  }

  /**
   * @see org.apache.hadoop.hbase.master.HMasterCommandLine
   */
  public static void main(String [] args) {
    LOG.info("STARTING service " + HMaster.class.getSimpleName());
    VersionInfo.logVersion();
    new HMasterCommandLine(HMaster.class).doMain(args);
  }

  public HFileCleaner getHFileCleaner() {
    return this.hfileCleaner;
  }

  public LogCleaner getLogCleaner() {
    return this.logCleaner;
  }

  /**
   * @return the underlying snapshot manager
   */
  @Override
  public SnapshotManager getSnapshotManager() {
    return this.snapshotManager;
  }

  /**
   * @return the underlying MasterProcedureManagerHost
   */
  @Override
  public MasterProcedureManagerHost getMasterProcedureManagerHost() {
    return mpmHost;
  }

  @Override
  public ClusterSchema getClusterSchema() {
    return this.clusterSchemaService;
  }

  /**
   * Create a new Namespace.
   * @param namespaceDescriptor descriptor for new Namespace
   * @param nonceGroup Identifier for the source of the request, a client or process.
   * @param nonce A unique identifier for this operation from the client or process identified by
   * <code>nonceGroup</code> (the source must ensure each operation gets a unique id).
   * @return procedure id
   */
  long createNamespace(final NamespaceDescriptor namespaceDescriptor, final long nonceGroup,
      final long nonce) throws IOException {
    checkInitialized();

    TableName.isLegalNamespaceName(Bytes.toBytes(namespaceDescriptor.getName()));

    return MasterProcedureUtil.submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this,
          nonceGroup, nonce) {
      @Override
      protected void run() throws IOException {
        getMaster().getMasterCoprocessorHost().preCreateNamespace(namespaceDescriptor);
        // We need to wait for the procedure to potentially fail due to "prepare" sanity
        // checks. This will block only the beginning of the procedure. See HBASE-19953.
        ProcedurePrepareLatch latch = ProcedurePrepareLatch.createBlockingLatch();
        LOG.info(getClientIdAuditPrefix() + " creating " + namespaceDescriptor);
        // Execute the operation synchronously - wait for the operation to complete before
        // continuing.
        setProcId(getClusterSchema().createNamespace(namespaceDescriptor, getNonceKey(), latch));
        latch.await();
        getMaster().getMasterCoprocessorHost().postCreateNamespace(namespaceDescriptor);
      }

      @Override
      protected String getDescription() {
        return "CreateNamespaceProcedure";
      }
    });
  }

  /**
   * Modify an existing Namespace.
   * @param nonceGroup Identifier for the source of the request, a client or process.
   * @param nonce A unique identifier for this operation from the client or process identified by
   * <code>nonceGroup</code> (the source must ensure each operation gets a unique id).
   * @return procedure id
   */
  long modifyNamespace(final NamespaceDescriptor newNsDescriptor, final long nonceGroup,
      final long nonce) throws IOException {
    checkInitialized();

    TableName.isLegalNamespaceName(Bytes.toBytes(newNsDescriptor.getName()));

    return MasterProcedureUtil.submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this,
          nonceGroup, nonce) {
      @Override
      protected void run() throws IOException {
        NamespaceDescriptor oldNsDescriptor = getNamespace(newNsDescriptor.getName());
        getMaster().getMasterCoprocessorHost().preModifyNamespace(oldNsDescriptor, newNsDescriptor);
        // We need to wait for the procedure to potentially fail due to "prepare" sanity
        // checks. This will block only the beginning of the procedure. See HBASE-19953.
        ProcedurePrepareLatch latch = ProcedurePrepareLatch.createBlockingLatch();
        LOG.info(getClientIdAuditPrefix() + " modify " + newNsDescriptor);
        // Execute the operation synchronously - wait for the operation to complete before
        // continuing.
        setProcId(getClusterSchema().modifyNamespace(newNsDescriptor, getNonceKey(), latch));
        latch.await();
        getMaster().getMasterCoprocessorHost().postModifyNamespace(oldNsDescriptor,
          newNsDescriptor);
      }

      @Override
      protected String getDescription() {
        return "ModifyNamespaceProcedure";
      }
    });
  }

  /**
   * Delete an existing Namespace. Only empty Namespaces (no tables) can be removed.
   * @param nonceGroup Identifier for the source of the request, a client or process.
   * @param nonce A unique identifier for this operation from the client or process identified by
   * <code>nonceGroup</code> (the source must ensure each operation gets a unique id).
   * @return procedure id
   */
  long deleteNamespace(final String name, final long nonceGroup, final long nonce)
      throws IOException {
    checkInitialized();

    return MasterProcedureUtil.submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this,
          nonceGroup, nonce) {
      @Override
      protected void run() throws IOException {
        getMaster().getMasterCoprocessorHost().preDeleteNamespace(name);
        LOG.info(getClientIdAuditPrefix() + " delete " + name);
        // Execute the operation synchronously - wait for the operation to complete before
        // continuing.
        //
        // We need to wait for the procedure to potentially fail due to "prepare" sanity
        // checks. This will block only the beginning of the procedure. See HBASE-19953.
        ProcedurePrepareLatch latch = ProcedurePrepareLatch.createBlockingLatch();
        setProcId(submitProcedure(
              new DeleteNamespaceProcedure(procedureExecutor.getEnvironment(), name, latch)));
        latch.await();
        // Will not be invoked in the face of Exception thrown by the Procedure's execution
        getMaster().getMasterCoprocessorHost().postDeleteNamespace(name);
      }

      @Override
      protected String getDescription() {
        return "DeleteNamespaceProcedure";
      }
    });
  }

  /**
   * Get a Namespace
   * @param name Name of the Namespace
   * @return Namespace descriptor for <code>name</code>
   */
  NamespaceDescriptor getNamespace(String name) throws IOException {
    checkInitialized();
    if (this.cpHost != null) this.cpHost.preGetNamespaceDescriptor(name);
    NamespaceDescriptor nsd = this.clusterSchemaService.getNamespace(name);
    if (this.cpHost != null) this.cpHost.postGetNamespaceDescriptor(nsd);
    return nsd;
  }

  /**
   * Get all Namespaces
   * @return All Namespace descriptors
   */
  List<NamespaceDescriptor> getNamespaces() throws IOException {
    checkInitialized();
    final List<NamespaceDescriptor> nsds = new ArrayList<>();
    if (cpHost != null) {
      cpHost.preListNamespaceDescriptors(nsds);
    }
    nsds.addAll(this.clusterSchemaService.getNamespaces());
    if (this.cpHost != null) {
      this.cpHost.postListNamespaceDescriptors(nsds);
    }
    return nsds;
  }

  /**
   * List namespace names
   * @return All namespace names
   */
  public List<String> listNamespaces() throws IOException {
    checkInitialized();
    List<String> namespaces = new ArrayList<>();
    if (cpHost != null) {
      cpHost.preListNamespaces(namespaces);
    }
    for (NamespaceDescriptor namespace : clusterSchemaService.getNamespaces()) {
      namespaces.add(namespace.getName());
    }
    if (cpHost != null) {
      cpHost.postListNamespaces(namespaces);
    }
    return namespaces;
  }

  @Override
  public List<TableName> listTableNamesByNamespace(String name) throws IOException {
    checkInitialized();
    return listTableNames(name, null, true);
  }

  @Override
  public List<TableDescriptor> listTableDescriptorsByNamespace(String name) throws IOException {
    checkInitialized();
    return listTableDescriptors(name, null, null, true);
  }

  @Override
  public boolean abortProcedure(final long procId, final boolean mayInterruptIfRunning)
      throws IOException {
    if (cpHost != null) {
      cpHost.preAbortProcedure(this.procedureExecutor, procId);
    }

    final boolean result = this.procedureExecutor.abort(procId, mayInterruptIfRunning);

    if (cpHost != null) {
      cpHost.postAbortProcedure();
    }

    return result;
  }

  @Override
  public List<Procedure<?>> getProcedures() throws IOException {
    if (cpHost != null) {
      cpHost.preGetProcedures();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    List<Procedure<?>> procList = (List) this.procedureExecutor.getProcedures();

    if (cpHost != null) {
      cpHost.postGetProcedures(procList);
    }

    return procList;
  }

  @Override
  public List<LockedResource> getLocks() throws IOException {
    if (cpHost != null) {
      cpHost.preGetLocks();
    }

    MasterProcedureScheduler procedureScheduler =
      procedureExecutor.getEnvironment().getProcedureScheduler();

    final List<LockedResource> lockedResources = procedureScheduler.getLocks();

    if (cpHost != null) {
      cpHost.postGetLocks(lockedResources);
    }

    return lockedResources;
  }

  /**
   * Returns the list of table descriptors that match the specified request
   * @param namespace the namespace to query, or null if querying for all
   * @param regex The regular expression to match against, or null if querying for all
   * @param tableNameList the list of table names, or null if querying for all
   * @param includeSysTables False to match only against userspace tables
   * @return the list of table descriptors
   */
  public List<TableDescriptor> listTableDescriptors(final String namespace, final String regex,
      final List<TableName> tableNameList, final boolean includeSysTables)
  throws IOException {
    List<TableDescriptor> htds = new ArrayList<>();
    if (cpHost != null) {
      cpHost.preGetTableDescriptors(tableNameList, htds, regex);
    }
    htds = getTableDescriptors(htds, namespace, regex, tableNameList, includeSysTables);
    if (cpHost != null) {
      cpHost.postGetTableDescriptors(tableNameList, htds, regex);
    }
    return htds;
  }

  /**
   * Returns the list of table names that match the specified request
   * @param regex The regular expression to match against, or null if querying for all
   * @param namespace the namespace to query, or null if querying for all
   * @param includeSysTables False to match only against userspace tables
   * @return the list of table names
   */
  public List<TableName> listTableNames(final String namespace, final String regex,
      final boolean includeSysTables) throws IOException {
    List<TableDescriptor> htds = new ArrayList<>();
    if (cpHost != null) {
      cpHost.preGetTableNames(htds, regex);
    }
    htds = getTableDescriptors(htds, namespace, regex, null, includeSysTables);
    if (cpHost != null) {
      cpHost.postGetTableNames(htds, regex);
    }
    List<TableName> result = new ArrayList<>(htds.size());
    for (TableDescriptor htd: htds) result.add(htd.getTableName());
    return result;
  }

  /**
   * @return list of table table descriptors after filtering by regex and whether to include system
   *    tables, etc.
   * @throws IOException
   */
  private List<TableDescriptor> getTableDescriptors(final List<TableDescriptor> htds,
      final String namespace, final String regex, final List<TableName> tableNameList,
      final boolean includeSysTables)
  throws IOException {
    if (tableNameList == null || tableNameList.isEmpty()) {
      // request for all TableDescriptors
      Collection<TableDescriptor> allHtds;
      if (namespace != null && namespace.length() > 0) {
        // Do a check on the namespace existence. Will fail if does not exist.
        this.clusterSchemaService.getNamespace(namespace);
        allHtds = tableDescriptors.getByNamespace(namespace).values();
      } else {
        allHtds = tableDescriptors.getAll().values();
      }
      for (TableDescriptor desc: allHtds) {
        if (tableStateManager.isTablePresent(desc.getTableName())
            && (includeSysTables || !desc.getTableName().isSystemTable())) {
          htds.add(desc);
        }
      }
    } else {
      for (TableName s: tableNameList) {
        if (tableStateManager.isTablePresent(s)) {
          TableDescriptor desc = tableDescriptors.get(s);
          if (desc != null) {
            htds.add(desc);
          }
        }
      }
    }

    // Retains only those matched by regular expression.
    if (regex != null) filterTablesByRegex(htds, Pattern.compile(regex));
    return htds;
  }

  /**
   * Removes the table descriptors that don't match the pattern.
   * @param descriptors list of table descriptors to filter
   * @param pattern the regex to use
   */
  private static void filterTablesByRegex(final Collection<TableDescriptor> descriptors,
      final Pattern pattern) {
    final String defaultNS = NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR;
    Iterator<TableDescriptor> itr = descriptors.iterator();
    while (itr.hasNext()) {
      TableDescriptor htd = itr.next();
      String tableName = htd.getTableName().getNameAsString();
      boolean matched = pattern.matcher(tableName).matches();
      if (!matched && htd.getTableName().getNamespaceAsString().equals(defaultNS)) {
        matched = pattern.matcher(defaultNS + TableName.NAMESPACE_DELIM + tableName).matches();
      }
      if (!matched) {
        itr.remove();
      }
    }
  }

  @Override
  public long getLastMajorCompactionTimestamp(TableName table) throws IOException {
    return getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
        .getLastMajorCompactionTimestamp(table);
  }

  @Override
  public long getLastMajorCompactionTimestampForRegion(byte[] regionName) throws IOException {
    return getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
        .getLastMajorCompactionTimestamp(regionName);
  }

  /**
   * Gets the mob file compaction state for a specific table.
   * Whether all the mob files are selected is known during the compaction execution, but
   * the statistic is done just before compaction starts, it is hard to know the compaction
   * type at that time, so the rough statistics are chosen for the mob file compaction. Only two
   * compaction states are available, CompactionState.MAJOR_AND_MINOR and CompactionState.NONE.
   * @param tableName The current table name.
   * @return If a given table is in mob file compaction now.
   */
  public CompactionState getMobCompactionState(TableName tableName) {
    AtomicInteger compactionsCount = mobCompactionStates.get(tableName);
    if (compactionsCount != null && compactionsCount.get() != 0) {
      return CompactionState.MAJOR_AND_MINOR;
    }
    return CompactionState.NONE;
  }

  public void reportMobCompactionStart(TableName tableName) throws IOException {
    IdLock.Entry lockEntry = null;
    try {
      lockEntry = mobCompactionLock.getLockEntry(tableName.hashCode());
      AtomicInteger compactionsCount = mobCompactionStates.get(tableName);
      if (compactionsCount == null) {
        compactionsCount = new AtomicInteger(0);
        mobCompactionStates.put(tableName, compactionsCount);
      }
      compactionsCount.incrementAndGet();
    } finally {
      if (lockEntry != null) {
        mobCompactionLock.releaseLockEntry(lockEntry);
      }
    }
  }

  public void reportMobCompactionEnd(TableName tableName) throws IOException {
    IdLock.Entry lockEntry = null;
    try {
      lockEntry = mobCompactionLock.getLockEntry(tableName.hashCode());
      AtomicInteger compactionsCount = mobCompactionStates.get(tableName);
      if (compactionsCount != null) {
        int count = compactionsCount.decrementAndGet();
        // remove the entry if the count is 0.
        if (count == 0) {
          mobCompactionStates.remove(tableName);
        }
      }
    } finally {
      if (lockEntry != null) {
        mobCompactionLock.releaseLockEntry(lockEntry);
      }
    }
  }


  /**
   * Queries the state of the {@link LoadBalancerTracker}. If the balancer is not initialized,
   * false is returned.
   *
   * @return The state of the load balancer, or false if the load balancer isn't defined.
   */
  public boolean isBalancerOn() {
    return !isInMaintenanceMode()
        && loadBalancerTracker != null
        && loadBalancerTracker.isBalancerOn();
  }

  /**
   * Queries the state of the {@link RegionNormalizerTracker}. If it's not initialized,
   * false is returned.
   */
  public boolean isNormalizerOn() {
    return !isInMaintenanceMode()
        && regionNormalizerTracker != null
        && regionNormalizerTracker.isNormalizerOn();
  }

  /**
   * Queries the state of the {@link SplitOrMergeTracker}. If it is not initialized,
   * false is returned. If switchType is illegal, false will return.
   * @param switchType see {@link org.apache.hadoop.hbase.client.MasterSwitchType}
   * @return The state of the switch
   */
  @Override
  public boolean isSplitOrMergeEnabled(MasterSwitchType switchType) {
    return !isInMaintenanceMode()
        && splitOrMergeTracker != null
        && splitOrMergeTracker.isSplitOrMergeEnabled(switchType);
  }

  /**
   * Fetch the configured {@link LoadBalancer} class name. If none is set, a default is returned.
   * <p/>
   * Notice that, the base load balancer will always be {@link RSGroupBasedLoadBalancer} now, so
   * this method will return the balancer used inside each rs group.
   * @return The name of the {@link LoadBalancer} in use.
   */
  public String getLoadBalancerClassName() {
    return conf.get(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
      LoadBalancerFactory.getDefaultLoadBalancerClass().getName());
  }

  /**
   * @return RegionNormalizerTracker instance
   */
  public RegionNormalizerTracker getRegionNormalizerTracker() {
    return regionNormalizerTracker;
  }

  public SplitOrMergeTracker getSplitOrMergeTracker() {
    return splitOrMergeTracker;
  }

  @Override
  public RSGroupBasedLoadBalancer getLoadBalancer() {
    return balancer;
  }

  @Override
  public FavoredNodesManager getFavoredNodesManager() {
    return balancer.getFavoredNodesManager();
  }

  private long executePeerProcedure(AbstractPeerProcedure<?> procedure) throws IOException {
    long procId = procedureExecutor.submitProcedure(procedure);
    procedure.getLatch().await();
    return procId;
  }

  @Override
  public long addReplicationPeer(String peerId, ReplicationPeerConfig peerConfig, boolean enabled)
      throws ReplicationException, IOException {
    LOG.info(getClientIdAuditPrefix() + " creating replication peer, id=" + peerId + ", config=" +
      peerConfig + ", state=" + (enabled ? "ENABLED" : "DISABLED"));
    return executePeerProcedure(new AddPeerProcedure(peerId, peerConfig, enabled));
  }

  @Override
  public long removeReplicationPeer(String peerId) throws ReplicationException, IOException {
    LOG.info(getClientIdAuditPrefix() + " removing replication peer, id=" + peerId);
    return executePeerProcedure(new RemovePeerProcedure(peerId));
  }

  @Override
  public long enableReplicationPeer(String peerId) throws ReplicationException, IOException {
    LOG.info(getClientIdAuditPrefix() + " enable replication peer, id=" + peerId);
    return executePeerProcedure(new EnablePeerProcedure(peerId));
  }

  @Override
  public long disableReplicationPeer(String peerId) throws ReplicationException, IOException {
    LOG.info(getClientIdAuditPrefix() + " disable replication peer, id=" + peerId);
    return executePeerProcedure(new DisablePeerProcedure(peerId));
  }

  @Override
  public ReplicationPeerConfig getReplicationPeerConfig(String peerId)
      throws ReplicationException, IOException {
    if (cpHost != null) {
      cpHost.preGetReplicationPeerConfig(peerId);
    }
    LOG.info(getClientIdAuditPrefix() + " get replication peer config, id=" + peerId);
    ReplicationPeerConfig peerConfig = this.replicationPeerManager.getPeerConfig(peerId)
        .orElseThrow(() -> new ReplicationPeerNotFoundException(peerId));
    if (cpHost != null) {
      cpHost.postGetReplicationPeerConfig(peerId);
    }
    return peerConfig;
  }

  @Override
  public long updateReplicationPeerConfig(String peerId, ReplicationPeerConfig peerConfig)
      throws ReplicationException, IOException {
    LOG.info(getClientIdAuditPrefix() + " update replication peer config, id=" + peerId +
      ", config=" + peerConfig);
    return executePeerProcedure(new UpdatePeerConfigProcedure(peerId, peerConfig));
  }

  @Override
  public List<ReplicationPeerDescription> listReplicationPeers(String regex)
      throws ReplicationException, IOException {
    if (cpHost != null) {
      cpHost.preListReplicationPeers(regex);
    }
    LOG.debug("{} list replication peers, regex={}", getClientIdAuditPrefix(), regex);
    Pattern pattern = regex == null ? null : Pattern.compile(regex);
    List<ReplicationPeerDescription> peers =
      this.replicationPeerManager.listPeers(pattern);
    if (cpHost != null) {
      cpHost.postListReplicationPeers(regex);
    }
    return peers;
  }

  @Override
  public long transitReplicationPeerSyncReplicationState(String peerId, SyncReplicationState state)
    throws ReplicationException, IOException {
    LOG.info(
      getClientIdAuditPrefix() +
        " transit current cluster state to {} in a synchronous replication peer id={}",
      state, peerId);
    return executePeerProcedure(new TransitPeerSyncReplicationStateProcedure(peerId, state));
  }

  /**
   * Mark region server(s) as decommissioned (previously called 'draining') to prevent additional
   * regions from getting assigned to them. Also unload the regions on the servers asynchronously.0
   * @param servers Region servers to decommission.
   */
  public void decommissionRegionServers(final List<ServerName> servers, final boolean offload)
      throws IOException {
    List<ServerName> serversAdded = new ArrayList<>(servers.size());
    // Place the decommission marker first.
    String parentZnode = getZooKeeper().getZNodePaths().drainingZNode;
    for (ServerName server : servers) {
      try {
        String node = ZNodePaths.joinZNode(parentZnode, server.getServerName());
        ZKUtil.createAndFailSilent(getZooKeeper(), node);
      } catch (KeeperException ke) {
        throw new HBaseIOException(
          this.zooKeeper.prefix("Unable to decommission '" + server.getServerName() + "'."), ke);
      }
      if (this.serverManager.addServerToDrainList(server)) {
        serversAdded.add(server);
      }
    }
    // Move the regions off the decommissioned servers.
    if (offload) {
      final List<ServerName> destServers = this.serverManager.createDestinationServersList();
      for (ServerName server : serversAdded) {
        final List<RegionInfo> regionsOnServer = this.assignmentManager.getRegionsOnServer(server);
        for (RegionInfo hri : regionsOnServer) {
          ServerName dest = balancer.randomAssignment(hri, destServers);
          if (dest == null) {
            throw new HBaseIOException("Unable to determine a plan to move " + hri);
          }
          RegionPlan rp = new RegionPlan(hri, server, dest);
          this.assignmentManager.moveAsync(rp);
        }
      }
    }
  }

  /**
   * List region servers marked as decommissioned (previously called 'draining') to not get regions
   * assigned to them.
   * @return List of decommissioned servers.
   */
  public List<ServerName> listDecommissionedRegionServers() {
    return this.serverManager.getDrainingServersList();
  }

  /**
   * Remove decommission marker (previously called 'draining') from a region server to allow regions
   * assignments. Load regions onto the server asynchronously if a list of regions is given
   * @param server Region server to remove decommission marker from.
   */
  public void recommissionRegionServer(final ServerName server,
      final List<byte[]> encodedRegionNames) throws IOException {
    // Remove the server from decommissioned (draining) server list.
    String parentZnode = getZooKeeper().getZNodePaths().drainingZNode;
    String node = ZNodePaths.joinZNode(parentZnode, server.getServerName());
    try {
      ZKUtil.deleteNodeFailSilent(getZooKeeper(), node);
    } catch (KeeperException ke) {
      throw new HBaseIOException(
        this.zooKeeper.prefix("Unable to recommission '" + server.getServerName() + "'."), ke);
    }
    this.serverManager.removeServerFromDrainList(server);

    // Load the regions onto the server if we are given a list of regions.
    if (encodedRegionNames == null || encodedRegionNames.isEmpty()) {
      return;
    }
    if (!this.serverManager.isServerOnline(server)) {
      return;
    }
    for (byte[] encodedRegionName : encodedRegionNames) {
      RegionState regionState =
        assignmentManager.getRegionStates().getRegionState(Bytes.toString(encodedRegionName));
      if (regionState == null) {
        LOG.warn("Unknown region " + Bytes.toStringBinary(encodedRegionName));
        continue;
      }
      RegionInfo hri = regionState.getRegion();
      if (server.equals(regionState.getServerName())) {
        LOG.info("Skipping move of region " + hri.getRegionNameAsString() +
          " because region already assigned to the same server " + server + ".");
        continue;
      }
      RegionPlan rp = new RegionPlan(hri, regionState.getServerName(), server);
      this.assignmentManager.moveAsync(rp);
    }
  }

  @Override
  public LockManager getLockManager() {
    return lockManager;
  }

  public QuotaObserverChore getQuotaObserverChore() {
    return this.quotaObserverChore;
  }

  public SpaceQuotaSnapshotNotifier getSpaceQuotaSnapshotNotifier() {
    return this.spaceQuotaSnapshotNotifier;
  }

  @SuppressWarnings("unchecked")
  private RemoteProcedure<MasterProcedureEnv, ?> getRemoteProcedure(long procId) {
    Procedure<?> procedure = procedureExecutor.getProcedure(procId);
    if (procedure == null) {
      return null;
    }
    assert procedure instanceof RemoteProcedure;
    return (RemoteProcedure<MasterProcedureEnv, ?>) procedure;
  }

  public void remoteProcedureCompleted(long procId) {
    LOG.debug("Remote procedure done, pid={}", procId);
    RemoteProcedure<MasterProcedureEnv, ?> procedure = getRemoteProcedure(procId);
    if (procedure != null) {
      procedure.remoteOperationCompleted(procedureExecutor.getEnvironment());
    }
  }

  public void remoteProcedureFailed(long procId, RemoteProcedureException error) {
    LOG.debug("Remote procedure failed, pid={}", procId, error);
    RemoteProcedure<MasterProcedureEnv, ?> procedure = getRemoteProcedure(procId);
    if (procedure != null) {
      procedure.remoteOperationFailed(procedureExecutor.getEnvironment(), error);
    }
  }

  /**
   * Reopen regions provided in the argument
   *
   * @param tableName The current table name
   * @param regionNames The region names of the regions to reopen
   * @param nonceGroup Identifier for the source of the request, a client or process
   * @param nonce A unique identifier for this operation from the client or process identified by
   *   <code>nonceGroup</code> (the source must ensure each operation gets a unique id).
   * @return procedure Id
   * @throws IOException if reopening region fails while running procedure
   */
  long reopenRegions(final TableName tableName, final List<byte[]> regionNames,
      final long nonceGroup, final long nonce)
      throws IOException {

    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {

        @Override
        protected void run() throws IOException {
          submitProcedure(new ReopenTableRegionsProcedure(tableName, regionNames));
        }

        @Override
        protected String getDescription() {
          return "ReopenTableRegionsProcedure";
        }

      });

  }

  @Override
  public ReplicationPeerManager getReplicationPeerManager() {
    return replicationPeerManager;
  }

  public HashMap<String, List<Pair<ServerName, ReplicationLoadSource>>>
      getReplicationLoad(ServerName[] serverNames) {
    List<ReplicationPeerDescription> peerList = this.getReplicationPeerManager().listPeers(null);
    if (peerList == null) {
      return null;
    }
    HashMap<String, List<Pair<ServerName, ReplicationLoadSource>>> replicationLoadSourceMap =
        new HashMap<>(peerList.size());
    peerList.stream()
        .forEach(peer -> replicationLoadSourceMap.put(peer.getPeerId(), new ArrayList<>()));
    for (ServerName serverName : serverNames) {
      List<ReplicationLoadSource> replicationLoadSources =
          getServerManager().getLoad(serverName).getReplicationLoadSourceList();
      for (ReplicationLoadSource replicationLoadSource : replicationLoadSources) {
        replicationLoadSourceMap.get(replicationLoadSource.getPeerID())
            .add(new Pair<>(serverName, replicationLoadSource));
      }
    }
    for (List<Pair<ServerName, ReplicationLoadSource>> loads : replicationLoadSourceMap.values()) {
      if (loads.size() > 0) {
        loads.sort(Comparator.comparingLong(load -> (-1) * load.getSecond().getReplicationLag()));
      }
    }
    return replicationLoadSourceMap;
  }

  /**
   * This method modifies the master's configuration in order to inject replication-related features
   */
  @VisibleForTesting
  public static void decorateMasterConfiguration(Configuration conf) {
    String plugins = conf.get(HBASE_MASTER_LOGCLEANER_PLUGINS);
    String cleanerClass = ReplicationLogCleaner.class.getCanonicalName();
    if (plugins == null || !plugins.contains(cleanerClass)) {
      conf.set(HBASE_MASTER_LOGCLEANER_PLUGINS, plugins + "," + cleanerClass);
    }
    if (ReplicationUtils.isReplicationForBulkLoadDataEnabled(conf)) {
      plugins = conf.get(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS);
      cleanerClass = ReplicationHFileCleaner.class.getCanonicalName();
      if (!plugins.contains(cleanerClass)) {
        conf.set(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS, plugins + "," + cleanerClass);
      }
    }
  }

  public SnapshotQuotaObserverChore getSnapshotQuotaObserverChore() {
    return this.snapshotQuotaChore;
  }

  @Override
  public SyncReplicationReplayWALManager getSyncReplicationReplayWALManager() {
    return this.syncReplicationReplayWALManager;
  }

  @Override
  public Map<String, ReplicationStatus> getWalGroupsReplicationStatus() {
    if (!this.isOnline() || !LoadBalancer.isMasterCanHostUserRegions(conf)) {
      return new HashMap<>();
    }
    return super.getWalGroupsReplicationStatus();
  }

  public HbckChore getHbckChore() {
    return this.hbckChore;
  }

  @Override
  public String getClusterId() {
    if (activeMaster) {
      return super.getClusterId();
    }
    return cachedClusterId.getFromCacheOrFetch();
  }

  public Optional<ServerName> getActiveMaster() {
    return activeMasterManager.getActiveMasterServerName();
  }

  @Override
  public void runReplicationBarrierCleaner() {
    ReplicationBarrierCleaner rbc = this.replicationBarrierCleaner;
    if (rbc != null) {
      rbc.chore();
    }
  }

  public MetaRegionLocationCache getMetaRegionLocationCache() {
    return this.metaRegionLocationCache;
  }

  @Override
  public RSGroupInfoManager getRSGroupInfoManager() {
    return rsGroupInfoManager;
  }

  public RegionLocations locateMeta(byte[] row, RegionLocateType locateType) throws IOException {
    if (locateType == RegionLocateType.AFTER) {
      // as we know the exact row after us, so we can just create the new row, and use the same
      // algorithm to locate it.
      row = Arrays.copyOf(row, row.length + 1);
      locateType = RegionLocateType.CURRENT;
    }
    Scan scan =
      CatalogFamilyFormat.createRegionLocateScan(TableName.META_TABLE_NAME, row, locateType, 1);
    try (RegionScanner scanner = masterRegion.getScanner(scan)) {
      boolean moreRows;
      List<Cell> cells = new ArrayList<>();
      do {
        moreRows = scanner.next(cells);
        if (cells.isEmpty()) {
          continue;
        }
        Result result = Result.create(cells);
        cells.clear();
        RegionLocations locs = CatalogFamilyFormat.getRegionLocations(result);
        if (locs == null || locs.getDefaultRegionLocation() == null) {
          LOG.warn("No location found when locating meta region with row='{}', locateType={}",
            Bytes.toStringBinary(row), locateType);
          return null;
        }
        HRegionLocation loc = locs.getDefaultRegionLocation();
        RegionInfo info = loc.getRegion();
        if (info == null) {
          LOG.warn("HRegionInfo is null when locating meta region with row='{}', locateType={}",
            Bytes.toStringBinary(row), locateType);
          return null;
        }
        if (info.isSplitParent()) {
          continue;
        }
        return locs;
      } while (moreRows);
      LOG.warn("No location available when locating meta region with row='{}', locateType={}",
        Bytes.toStringBinary(row), locateType);
      return null;
    }
  }

  public List<RegionLocations> getAllMetaRegionLocations(boolean excludeOfflinedSplitParents)
    throws IOException {
    Scan scan = new Scan().addFamily(HConstants.CATALOG_FAMILY);
    List<RegionLocations> list = new ArrayList<>();
    try (RegionScanner scanner = masterRegion.getScanner(scan)) {
      boolean moreRows;
      List<Cell> cells = new ArrayList<>();
      do {
        moreRows = scanner.next(cells);
        if (cells.isEmpty()) {
          continue;
        }
        Result result = Result.create(cells);
        cells.clear();
        RegionLocations locs = CatalogFamilyFormat.getRegionLocations(result);
        if (locs == null) {
          LOG.warn("No locations in {}", result);
          continue;
        }
        HRegionLocation loc = locs.getRegionLocation();
        if (loc == null) {
          LOG.warn("No non null location in {}", result);
          continue;
        }
        RegionInfo info = loc.getRegion();
        if (info == null) {
          LOG.warn("No serialized RegionInfo in {}", result);
          continue;
        }
        if (excludeOfflinedSplitParents && info.isSplitParent()) {
          continue;
        }
        list.add(locs);
      } while (moreRows);
    }
    return list;
  }
}
