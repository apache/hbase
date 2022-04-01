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

import com.google.errorprone.annotations.RestrictedApi;
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
import java.util.LinkedList;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServlet;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.ClusterMetricsBuilder;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HBaseServerBase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.PleaseRestartMasterException;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ReplicationPeerNotFoundException;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ServerTask;
import org.apache.hadoop.hbase.ServerTaskBuilder;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.client.BalanceResponse;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.NormalizeTableFilterParams;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionStatesCount;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.exceptions.MasterStoppedException;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.http.HttpServer;
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
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.master.balancer.ClusterStatusChore;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.master.balancer.MaintenanceLoadBalancer;
import org.apache.hadoop.hbase.master.cleaner.DirScanPool;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.master.cleaner.LogCleaner;
import org.apache.hadoop.hbase.master.cleaner.ReplicationBarrierCleaner;
import org.apache.hadoop.hbase.master.cleaner.SnapshotCleanerChore;
import org.apache.hadoop.hbase.master.http.MasterDumpServlet;
import org.apache.hadoop.hbase.master.http.MasterRedirectServlet;
import org.apache.hadoop.hbase.master.http.MasterStatusServlet;
import org.apache.hadoop.hbase.master.http.api_v1.ResourceConfigFactory;
import org.apache.hadoop.hbase.master.janitor.CatalogJanitor;
import org.apache.hadoop.hbase.master.locking.LockManager;
import org.apache.hadoop.hbase.master.migrate.RollingUpgradeChore;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizerFactory;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizerManager;
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
import org.apache.hadoop.hbase.namequeues.NamedQueueRecorder;
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
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.regionserver.storefiletracker.ModifyColumnFamilyStoreFileTrackerProcedure;
import org.apache.hadoop.hbase.regionserver.storefiletracker.ModifyTableStoreFileTrackerProcedure;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.replication.master.ReplicationHFileCleaner;
import org.apache.hadoop.hbase.replication.master.ReplicationLogCleaner;
import org.apache.hadoop.hbase.rsgroup.RSGroupAdminEndpoint;
import org.apache.hadoop.hbase.rsgroup.RSGroupBasedLoadBalancer;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfoManager;
import org.apache.hadoop.hbase.rsgroup.RSGroupUtil;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.SecurityConstants;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.CoprocessorConfigurationUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.IdLock;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.LoadBalancerTracker;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.RegionNormalizerTracker;
import org.apache.hadoop.hbase.zookeeper.SnapshotCleanupTracker;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.Server;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.ServerConnector;
import org.apache.hbase.thirdparty.org.eclipse.jetty.servlet.ServletHolder;
import org.apache.hbase.thirdparty.org.eclipse.jetty.webapp.WebAppContext;
import org.apache.hbase.thirdparty.org.glassfish.jersey.server.ResourceConfig;
import org.apache.hbase.thirdparty.org.glassfish.jersey.servlet.ServletContainer;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

/**
 * HMaster is the "master server" for HBase. An HBase cluster has one active master. If many masters
 * are started, all compete. Whichever wins goes on to run the cluster. All others park themselves
 * in their constructor until master or cluster shutdown or until the active master loses its lease
 * in zookeeper. Thereafter, all running master jostle to take over master role.
 * <p/>
 * The Master can be asked shutdown the cluster. See {@link #shutdown()}. In this case it will tell
 * all regionservers to go down and then wait on them all reporting in that they are down. This
 * master will then shut itself down.
 * <p/>
 * You can also shutdown just this master. Call {@link #stopMaster()}.
 * @see org.apache.zookeeper.Watcher
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class HMaster extends HBaseServerBase<MasterRpcServices> implements MasterServices {

  private static final Logger LOG = LoggerFactory.getLogger(HMaster.class);

  // MASTER is name of the webapp and the attribute name used stuffing this
  // instance into a web context !! AND OTHER PLACES !!
  public static final String MASTER = "master";

  // Manager and zk listener for master election
  private final ActiveMasterManager activeMasterManager;
  // Region server tracker
  private final RegionServerTracker regionServerTracker;
  // Draining region server tracker
  private DrainingServerTracker drainingServerTracker;
  // Tracker for load balancer state
  LoadBalancerTracker loadBalancerTracker;
  // Tracker for meta location, if any client ZK quorum specified
  private MetaLocationSyncer metaLocationSyncer;
  // Tracker for active master location, if any client ZK quorum specified
  @InterfaceAudience.Private
  MasterAddressSyncer masterAddressSyncer;
  // Tracker for auto snapshot cleanup state
  SnapshotCleanupTracker snapshotCleanupTracker;

  // Tracker for split and merge state
  private SplitOrMergeTracker splitOrMergeTracker;

  private ClusterSchemaService clusterSchemaService;

  public static final String HBASE_MASTER_WAIT_ON_SERVICE_IN_SECONDS =
    "hbase.master.wait.on.service.seconds";
  public static final int DEFAULT_HBASE_MASTER_WAIT_ON_SERVICE_IN_SECONDS = 5 * 60;

  public static final String HBASE_MASTER_CLEANER_INTERVAL = "hbase.master.cleaner.interval";

  public static final int DEFAULT_HBASE_MASTER_CLEANER_INTERVAL = 600 * 1000;

  private String clusterId;

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
  private BalancerChore balancerChore;
  private static boolean disableBalancerChoreForTest = false;
  private RegionNormalizerManager regionNormalizerManager;
  private ClusterStatusChore clusterStatusChore;
  private ClusterStatusPublisher clusterStatusPublisherChore = null;
  private SnapshotCleanerChore snapshotCleanerChore = null;

  private HbckChore hbckChore;
  CatalogJanitor catalogJanitorChore;
  // Threadpool for scanning the archive directory, used by the HFileCleaner
  private DirScanPool hfileCleanerPool;
  // Threadpool for scanning the Old logs directory, used by the LogCleaner
  private DirScanPool logCleanerPool;
  private LogCleaner logCleaner;
  private HFileCleaner hfileCleaner;
  private ReplicationBarrierCleaner replicationBarrierCleaner;
  private MobFileCleanerChore mobFileCleanerChore;
  private MobFileCompactionChore mobFileCompactionChore;
  private RollingUpgradeChore rollingUpgradeChore;
  // used to synchronize the mobCompactionStates
  private final IdLock mobCompactionLock = new IdLock();
  // save the information of mob compactions in tables.
  // the key is table name, the value is the number of compactions in that table.
  private Map<TableName, AtomicInteger> mobCompactionStates = Maps.newConcurrentMap();

  volatile MasterCoprocessorHost cpHost;

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

  // the master local storage to store procedure data, meta region locations, etc.
  private MasterRegion masterRegion;

  private RegionServerList rsListStorage;

  // handle table states
  private TableStateManager tableStateManager;

  /** jetty server for master to redirect requests to regionserver infoServer */
  private Server masterJettyServer;

  // Determine if we should do normal startup or minimal "single-user" mode with no region
  // servers and no user tables. Useful for repair and recovery of hbase:meta
  private final boolean maintenanceMode;
  static final String MAINTENANCE_MODE = "hbase.master.maintenance_mode";

  // the in process region server for carry system regions in maintenanceMode
  private JVMClusterUtil.RegionServerThread maintenanceRegionServer;

  // Cached clusterId on stand by masters to serve clusterID requests from clients.
  private final CachedClusterId cachedClusterId;

  public static final String WARMUP_BEFORE_MOVE = "hbase.master.warmup.before.move";
  private static final boolean DEFAULT_WARMUP_BEFORE_MOVE = true;

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
    super(conf, "Master");
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
      LOG.info("hbase.rootdir={}, hbase.cluster.distributed={}",
        CommonFSUtils.getRootDir(this.conf),
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
      this.activeMasterManager = createActiveMasterManager(zooKeeper, serverName, this);
      cachedClusterId = new CachedClusterId(this, conf);
      this.regionServerTracker = new RegionServerTracker(zooKeeper, this);
      this.rpcServices.start(zooKeeper);
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
  protected ActiveMasterManager createActiveMasterManager(ZKWatcher zk, ServerName sn,
      org.apache.hadoop.hbase.Server server) throws InterruptedIOException {
    return new ActiveMasterManager(zk, sn, server);
  }

  @Override
  protected String getUseThisHostnameInstead(Configuration conf) {
    return conf.get(MASTER_HOSTNAME_KEY);
  }

  private void registerConfigurationObservers() {
    configurationManager.registerObserver(this.rpcServices);
    configurationManager.registerObserver(this);
  }

  // Main run loop. Calls through to the regionserver run loop AFTER becoming active Master; will
  // block in here until then.
  @Override
  public void run() {
    try {
      registerConfigurationObservers();
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
      while (!isStopped() && !isAborted()) {
        sleeper.sleep();
      }
      stopInfoServer();
      closeClusterConnection();
      stopServiceThreads();
      if (this.rpcServices != null) {
        this.rpcServices.stop();
      }
      closeZooKeeper();
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
    if (infoPort == infoServer.getPort()) {
      // server is already running
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
    masterJettyServer.setHandler(HttpServer.buildGzipHandler(masterJettyServer.getHandler()));

    final String redirectHostname =
        StringUtils.isBlank(useThisHostnameInstead) ? null : useThisHostnameInstead;

    final MasterRedirectServlet redirect = new MasterRedirectServlet(infoServer, redirectHostname);
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
      user.login(SecurityConstants.REGIONSERVER_KRB_KEYTAB_FILE,
        SecurityConstants.REGIONSERVER_KRB_PRINCIPAL, host);
    } catch (IOException ie) {
      user.login(SecurityConstants.MASTER_KRB_KEYTAB_FILE, SecurityConstants.MASTER_KRB_PRINCIPAL,
        host);
    }
  }

  public MasterRpcServices getMasterRpcServices() {
    return rpcServices;
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
  protected boolean cacheTableDescriptor() {
    return true;
  }

  protected MasterRpcServices createRpcServices() throws IOException {
    return new MasterRpcServices(this);
  }

  @Override
  protected void configureInfoServer(InfoServer infoServer) {
    infoServer.addUnprivilegedServlet("master-status", "/master-status", MasterStatusServlet.class);
    infoServer.addUnprivilegedServlet("api_v1", "/api/v1/*", buildApiV1Servlet());

    infoServer.setAttribute(MASTER, this);
  }

  private ServletHolder buildApiV1Servlet() {
    final ResourceConfig config = ResourceConfigFactory.createResourceConfig(conf, this);
    return new ServletHolder(new ServletContainer(config));
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
   * Initialize all ZK based system trackers. But do not include {@link RegionServerTracker}, it
   * should have already been initialized along with {@link ServerManager}.
   */
  private void initializeZKBasedSystemTrackers()
    throws IOException, KeeperException, ReplicationException {
    if (maintenanceMode) {
      // in maintenance mode, always use MaintenanceLoadBalancer.
      conf.unset(LoadBalancer.HBASE_RSGROUP_LOADBALANCER_CLASS);
      conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, MaintenanceLoadBalancer.class,
        LoadBalancer.class);
    }
    this.balancer = new RSGroupBasedLoadBalancer();
    this.loadBalancerTracker = new LoadBalancerTracker(zooKeeper, this);
    this.loadBalancerTracker.start();

    this.regionNormalizerManager =
      RegionNormalizerFactory.createNormalizerManager(conf, zooKeeper, this);
    this.configurationManager.registerObserver(regionNormalizerManager);
    this.regionNormalizerManager.start();

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

    this.replicationPeerManager = ReplicationPeerManager.create(zooKeeper, conf, clusterId);

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
  @InterfaceAudience.Private
  protected AssignmentManager createAssignmentManager(MasterServices master,
    MasterRegion masterRegion) {
    return new AssignmentManager(master, masterRegion);
  }

  private void tryMigrateMetaLocationsFromZooKeeper() throws IOException, KeeperException {
    // try migrate data from zookeeper
    try (ResultScanner scanner =
      masterRegion.getScanner(new Scan().addFamily(HConstants.CATALOG_FAMILY))) {
      if (scanner.next() != null) {
        // notice that all replicas for a region are in the same row, so the migration can be
        // done with in a one row put, which means if we have data in catalog family then we can
        // make sure that the migration is done.
        LOG.info("The {} family in master local region already has data in it, skip migrating...",
          HConstants.CATALOG_FAMILY_STR);
        return;
      }
    }
    // start migrating
    byte[] row = CatalogFamilyFormat.getMetaKeyForRegion(RegionInfoBuilder.FIRST_META_REGIONINFO);
    Put put = new Put(row);
    List<String> metaReplicaNodes = zooKeeper.getMetaReplicaNodes();
    StringBuilder info = new StringBuilder("Migrating meta locations:");
    for (String metaReplicaNode : metaReplicaNodes) {
      int replicaId = zooKeeper.getZNodePaths().getMetaReplicaIdFromZNode(metaReplicaNode);
      RegionState state = MetaTableLocator.getMetaRegionState(zooKeeper, replicaId);
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
    if (!put.isEmpty()) {
      LOG.info(info.toString());
      masterRegion.update(r -> r.put(put));
    } else {
      LOG.info("No meta location available on zookeeper, skip migrating...");
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
   * <li>Create master local region</li>
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
   * <li>Start necessary service threads - balancer, catalog janitor, executor services, and also
   * the procedure executor, etc. Notice that the balancer must be created first as assignment
   * manager may use it when assigning regions.</li>
   * <li>Wait for meta to be initialized if necessary, start table state manager.</li>
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

    this.masterActiveTime = EnvironmentEdgeManager.currentTime();
    // TODO: Do this using Dependency Injection, using PicoContainer, Guice or Spring.

    // always initialize the MemStoreLAB as we use a region to store data in master now, see
    // localStore.
    initializeMemStoreChunkCreator(null);
    this.fileSystemManager = new MasterFileSystem(conf);
    this.walManager = new MasterWalManager(this);

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
          Closeables.close(result.getSecond(), true);
        }
      }
    }

    status.setStatus("Initialize ServerManager and schedule SCP for crash servers");
    // The below two managers must be created before loading procedures, as they will be used during
    // loading.
    // initialize master local region
    masterRegion = MasterRegionFactory.create(this);
    rsListStorage = new MasterRegionServerList(masterRegion, this);

    this.serverManager = createServerManager(this, rsListStorage);
    this.syncReplicationReplayWALManager = new SyncReplicationReplayWALManager(this);
    if (!conf.getBoolean(HBASE_SPLIT_WAL_COORDINATED_BY_ZK,
      DEFAULT_HBASE_SPLIT_COORDINATED_BY_ZK)) {
      this.splitWALManager = new SplitWALManager(this);
    }



    tryMigrateMetaLocationsFromZooKeeper();

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
    this.regionServerTracker.upgrade(
      procsByType.getOrDefault(ServerCrashProcedure.class, Collections.emptyList()).stream()
        .map(p -> (ServerCrashProcedure) p).map(p -> p.getServerName()).collect(Collectors.toSet()),
      Sets.union(rsListStorage.getAll(), walManager.getLiveServersFromWALDir()),
      walManager.getSplittingServersFromWALDir());
    // This manager must be accessed AFTER hbase:meta is confirmed on line..
    this.tableStateManager = new TableStateManager(this);

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
    Thread zombieDetector = new Thread(new MasterInitializationMonitor(this),
        "ActiveMasterInitializationMonitor-" + EnvironmentEdgeManager.currentTime());
    zombieDetector.setDaemon(true);
    zombieDetector.start();

    if (!maintenanceMode) {
      status.setStatus("Initializing master coprocessors");
      setQuotasObserver(conf);
      initializeCoprocessorHost(conf);
    } else {
      // start an in process region server for carrying system regions
      maintenanceRegionServer =
        JVMClusterUtil.createRegionServerThread(getConfiguration(), HRegionServer.class, 0);
      maintenanceRegionServer.start();
    }

    // Checking if meta needs initializing.
    status.setStatus("Initializing meta table if this is a new deploy");
    InitMetaProcedure initMetaProc = null;
    // Print out state of hbase:meta on startup; helps debugging.
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
    this.balancer.initialize();
    this.balancer.updateClusterMetrics(getClusterMetricsWithoutCoprocessor());

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

    TableDescriptor metaDescriptor =
      tableDescriptors.get(TableName.META_TABLE_NAME);
    final ColumnFamilyDescriptor tableFamilyDesc =
      metaDescriptor.getColumnFamily(HConstants.TABLE_FAMILY);
    final ColumnFamilyDescriptor replBarrierFamilyDesc =
      metaDescriptor.getColumnFamily(HConstants.REPLICATION_BARRIER_FAMILY);

    this.assignmentManager.joinCluster();
    // The below depends on hbase:meta being online.
    this.assignmentManager.processOfflineRegions();
    // this must be called after the above processOfflineRegions to prevent race
    this.assignmentManager.wakeMetaLoadedEvent();

    // for migrating from a version without HBASE-25099, and also for honoring the configuration
    // first.
    if (conf.get(HConstants.META_REPLICAS_NUM) != null) {
      int replicasNumInConf =
        conf.getInt(HConstants.META_REPLICAS_NUM, HConstants.DEFAULT_META_REPLICA_NUM);
      TableDescriptor metaDesc = tableDescriptors.get(TableName.META_TABLE_NAME);
      if (metaDesc.getRegionReplication() != replicasNumInConf) {
        // it is possible that we already have some replicas before upgrading, so we must set the
        // region replication number in meta TableDescriptor directly first, without creating a
        // ModifyTableProcedure, otherwise it may cause a double assign for the meta replicas.
        int existingReplicasCount =
          assignmentManager.getRegionStates().getRegionsOfTable(TableName.META_TABLE_NAME).size();
        if (existingReplicasCount > metaDesc.getRegionReplication()) {
          LOG.info("Update replica count of hbase:meta from {}(in TableDescriptor)" +
            " to {}(existing ZNodes)", metaDesc.getRegionReplication(), existingReplicasCount);
          metaDesc = TableDescriptorBuilder.newBuilder(metaDesc)
            .setRegionReplication(existingReplicasCount).build();
          tableDescriptors.update(metaDesc);
        }
        // check again, and issue a ModifyTableProcedure if needed
        if (metaDesc.getRegionReplication() != replicasNumInConf) {
          LOG.info(
            "The {} config is {} while the replica count in TableDescriptor is {}" +
              " for hbase:meta, altering...",
            HConstants.META_REPLICAS_NUM, replicasNumInConf, metaDesc.getRegionReplication());
          procedureExecutor.submitProcedure(new ModifyTableProcedure(
            procedureExecutor.getEnvironment(), TableDescriptorBuilder.newBuilder(metaDesc)
              .setRegionReplication(replicasNumInConf).build(),
            null, metaDesc, false));
        }
      }
    }
    // Initialize after meta is up as below scans meta
    FavoredNodesManager fnm = getFavoredNodesManager();
    if (fnm != null) {
      fnm.initializeFromMeta();
    }

    // set cluster status again after user regions are assigned
    this.balancer.updateClusterMetrics(getClusterMetricsWithoutCoprocessor());

    // Start balancer and meta catalog janitor after meta and regions have been assigned.
    status.setStatus("Starting balancer and catalog janitor");
    this.clusterStatusChore = new ClusterStatusChore(this, balancer);
    getChoreService().scheduleChore(clusterStatusChore);
    this.balancerChore = new BalancerChore(this);
    if (!disableBalancerChoreForTest) {
      getChoreService().scheduleChore(balancerChore);
    }
    if (regionNormalizerManager != null) {
      getChoreService().scheduleChore(regionNormalizerManager.getRegionNormalizerChore());
    }
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
    try {
      initClusterSchemaService();
    } catch (IllegalStateException e) {
      if (e.getCause() != null && e.getCause() instanceof NoSuchColumnFamilyException
          && tableFamilyDesc == null && replBarrierFamilyDesc == null) {
        LOG.info("ClusterSchema service could not be initialized. This is "
            + "expected during HBase 1 to 2 upgrade", e);
      } else {
        throw e;
      }
    }

    if (this.cpHost != null) {
      try {
        this.cpHost.preMasterInitialization();
      } catch (IOException e) {
        LOG.error("Coprocessor preMasterInitialization() hook failed", e);
      }
    }

    status.markComplete("Initialization successful");
    LOG.info(String.format("Master has completed initialization %.3fsec",
       (EnvironmentEdgeManager.currentTime() - masterActiveTime) / 1000.0f));
    this.masterFinishedInitializationTime = EnvironmentEdgeManager.currentTime();
    configurationManager.registerObserver(this.balancer);
    configurationManager.registerObserver(this.hfileCleanerPool);
    configurationManager.registerObserver(this.logCleanerPool);
    configurationManager.registerObserver(this.hfileCleaner);
    configurationManager.registerObserver(this.logCleaner);
    configurationManager.registerObserver(this.regionsRecoveryConfigManager);
    // Set master as 'initialized'.
    setInitialized(true);

    if (tableFamilyDesc == null && replBarrierFamilyDesc == null) {
      // create missing CFs in meta table after master is set to 'initialized'.
      createMissingCFsInMetaDuringUpgrade(metaDescriptor);

      // Throwing this Exception to abort active master is painful but this
      // seems the only way to add missing CFs in meta while upgrading from
      // HBase 1 to 2 (where HBase 2 has HBASE-23055 & HBASE-23782 checked-in).
      // So, why do we abort active master after adding missing CFs in meta?
      // When we reach here, we would have already bypassed NoSuchColumnFamilyException
      // in initClusterSchemaService(), meaning ClusterSchemaService is not
      // correctly initialized but we bypassed it. Similarly, we bypassed
      // tableStateManager.start() as well. Hence, we should better abort
      // current active master because our main task - adding missing CFs
      // in meta table is done (possible only after master state is set as
      // initialized) at the expense of bypassing few important tasks as part
      // of active master init routine. So now we abort active master so that
      // next active master init will not face any issues and all mandatory
      // services will be started during master init phase.
      throw new PleaseRestartMasterException("Aborting active master after missing"
          + " CFs are successfully added in meta. Subsequent active master "
          + "initialization should be uninterrupted");
    }

    if (maintenanceMode) {
      LOG.info("Detected repair mode, skipping final initialization steps.");
      return;
    }

    assignmentManager.checkIfShouldMoveSystemRegionAsync();
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
    long start = EnvironmentEdgeManager.currentTime();
    this.balancer.postMasterStartupInitialize();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Balancer post startup initialization complete, took " + (
          (EnvironmentEdgeManager.currentTime() - start) / 1000) + " seconds");
    }

    this.rollingUpgradeChore = new RollingUpgradeChore(this);
    getChoreService().scheduleChore(rollingUpgradeChore);
  }

  private void createMissingCFsInMetaDuringUpgrade(
      TableDescriptor metaDescriptor) throws IOException {
    TableDescriptor newMetaDesc =
        TableDescriptorBuilder.newBuilder(metaDescriptor)
            .setColumnFamily(FSTableDescriptors.getTableFamilyDescForMeta(conf))
            .setColumnFamily(FSTableDescriptors.getReplBarrierFamilyDescForMeta())
            .build();
    long pid = this.modifyTable(TableName.META_TABLE_NAME, () -> newMetaDesc,
        0, 0, false);
    int tries = 30;
    while (!(getMasterProcedureExecutor().isFinished(pid))
        && getMasterProcedureExecutor().isRunning() && tries > 0) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IOException("Wait interrupted", e);
      }
      tries--;
    }
    if (tries <= 0) {
      throw new HBaseIOException(
          "Failed to add table and rep_barrier CFs to meta in a given time.");
    } else {
      Procedure<?> result = getMasterProcedureExecutor().getResult(pid);
      if (result != null && result.isFailed()) {
        throw new IOException(
            "Failed to add table and rep_barrier CFs to meta. "
                + MasterProcedureUtil.unwrapRemoteIOException(result));
      }
    }
  }

  /**
   * Check hbase:meta is up and ready for reading. For use during Master startup only.
   * @return True if meta is UP and online and startup can progress. Otherwise, meta is not online
   *   and we will hold here until operator intervention.
   */
  @InterfaceAudience.Private
  public boolean waitForMetaOnline() {
    return isRegionOnline(RegionInfoBuilder.FIRST_META_REGIONINFO);
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
        rc = new RetryCounterFactory(Integer.MAX_VALUE, 1000, 60_000).create();
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
  @InterfaceAudience.Private
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
   * <p>
   * Create a {@link ServerManager} instance.
   * </p>
   * <p>
   * Will be overridden in tests.
   * </p>
   */
  @InterfaceAudience.Private
  protected ServerManager createServerManager(MasterServices master,
    RegionServerList storage) throws IOException {
    // We put this out here in a method so can do a Mockito.spy and stub it out
    // w/ a mocked up ServerManager.
    setupClusterConnection();
    return new ServerManager(master, storage);
  }

  private void waitForRegionServers(final MonitoredTask status)
      throws IOException, InterruptedException {
    this.serverManager.waitForRegionServers(status);
  }

  // Will be overridden in tests
  @InterfaceAudience.Private
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

  public boolean isCatalogJanitorEnabled() {
    return catalogJanitorChore != null ? catalogJanitorChore.getEnabled() : false;
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
    final int masterOpenRegionPoolSize = conf.getInt(
        HConstants.MASTER_OPEN_REGION_THREADS, HConstants.MASTER_OPEN_REGION_THREADS_DEFAULT);
    executorService.startExecutorService(executorService.new ExecutorConfig().setExecutorType(
        ExecutorType.MASTER_OPEN_REGION).setCorePoolSize(masterOpenRegionPoolSize));
    final int masterCloseRegionPoolSize = conf.getInt(
        HConstants.MASTER_CLOSE_REGION_THREADS, HConstants.MASTER_CLOSE_REGION_THREADS_DEFAULT);
    executorService.startExecutorService(executorService.new ExecutorConfig().setExecutorType(
        ExecutorType.MASTER_CLOSE_REGION).setCorePoolSize(masterCloseRegionPoolSize));
    final int masterServerOpThreads = conf.getInt(HConstants.MASTER_SERVER_OPERATIONS_THREADS,
        HConstants.MASTER_SERVER_OPERATIONS_THREADS_DEFAULT);
    executorService.startExecutorService(executorService.new ExecutorConfig().setExecutorType(
        ExecutorType.MASTER_SERVER_OPERATIONS).setCorePoolSize(masterServerOpThreads));
    final int masterServerMetaOpsThreads = conf.getInt(
        HConstants.MASTER_META_SERVER_OPERATIONS_THREADS,
        HConstants.MASTER_META_SERVER_OPERATIONS_THREADS_DEFAULT);
    executorService.startExecutorService(executorService.new ExecutorConfig().setExecutorType(
        ExecutorType.MASTER_META_SERVER_OPERATIONS).setCorePoolSize(masterServerMetaOpsThreads));
    final int masterLogReplayThreads = conf.getInt(
        HConstants.MASTER_LOG_REPLAY_OPS_THREADS, HConstants.MASTER_LOG_REPLAY_OPS_THREADS_DEFAULT);
    executorService.startExecutorService(executorService.new ExecutorConfig().setExecutorType(
        ExecutorType.M_LOG_REPLAY_OPS).setCorePoolSize(masterLogReplayThreads));
    final int masterSnapshotThreads = conf.getInt(
        SnapshotManager.SNAPSHOT_POOL_THREADS_KEY, SnapshotManager.SNAPSHOT_POOL_THREADS_DEFAULT);
    executorService.startExecutorService(executorService.new ExecutorConfig().setExecutorType(
        ExecutorType.MASTER_SNAPSHOT_OPERATIONS).setCorePoolSize(masterSnapshotThreads)
        .setAllowCoreThreadTimeout(true));
    final int masterMergeDispatchThreads = conf.getInt(HConstants.MASTER_MERGE_DISPATCH_THREADS,
        HConstants.MASTER_MERGE_DISPATCH_THREADS_DEFAULT);
    executorService.startExecutorService(executorService.new ExecutorConfig().setExecutorType(
        ExecutorType.MASTER_MERGE_OPERATIONS).setCorePoolSize(masterMergeDispatchThreads)
        .setAllowCoreThreadTimeout(true));

    // We depend on there being only one instance of this executor running
    // at a time. To do concurrency, would need fencing of enable/disable of
    // tables.
    // Any time changing this maxThreads to > 1, pls see the comment at
    // AccessController#postCompletedCreateTableAction
    executorService.startExecutorService(executorService.new ExecutorConfig().setExecutorType(
        ExecutorType.MASTER_TABLE_OPERATIONS).setCorePoolSize(1));
    startProcedureExecutor();

    // Create log cleaner thread pool
    logCleanerPool = DirScanPool.getLogCleanerScanPool(conf);
    Map<String, Object> params = new HashMap<>();
    params.put(MASTER, this);
    // Start log cleaner thread
    int cleanerInterval =
      conf.getInt(HBASE_MASTER_CLEANER_INTERVAL, DEFAULT_HBASE_MASTER_CLEANER_INTERVAL);
    this.logCleaner = new LogCleaner(cleanerInterval, this, conf,
      getMasterWalManager().getFileSystem(), getMasterWalManager().getOldLogDir(),
      logCleanerPool, params);
    getChoreService().scheduleChore(logCleaner);

    // start the hfile archive cleaner thread
    Path archiveDir = HFileArchiveUtil.getArchivePath(conf);
    // Create archive cleaner thread pool
    hfileCleanerPool = DirScanPool.getHFileCleanerScanPool(conf);
    this.hfileCleaner = new HFileCleaner(cleanerInterval, this, conf,
      getMasterFileSystem().getFileSystem(), archiveDir, hfileCleanerPool, params);
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

  protected void stopServiceThreads() {
    if (masterJettyServer != null) {
      LOG.info("Stopping master jetty server");
      try {
        masterJettyServer.stop();
      } catch (Exception e) {
        LOG.error("Failed to stop master jetty server", e);
      }
    }
    stopChoreService();
    stopExecutorService();
    if (hfileCleanerPool != null) {
      hfileCleanerPool.shutdownNow();
      hfileCleanerPool = null;
    }
    if (logCleanerPool != null) {
      logCleanerPool.shutdownNow();
      logCleanerPool = null;
    }
    if (maintenanceRegionServer != null) {
      maintenanceRegionServer.getRegionServer().stop(HBASE_MASTER_CLEANER_INTERVAL);
    }

    LOG.debug("Stopping service threads");
    // stop procedure executor prior to other services such as server manager and assignment
    // manager, as these services are important for some running procedures. See HBASE-24117 for
    // example.
    stopProcedureExecutor();

    if (regionNormalizerManager != null) {
      regionNormalizerManager.stop();
    }
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

  // will be override in UT
  protected void startProcedureExecutor() throws IOException {
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
        getChoreService().scheduleChore(this.snapshotCleanerChore);
      } else {
        this.snapshotCleanerChore.cancel();
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

  protected void stopChores() {
    shutdownChore(mobFileCleanerChore);
    shutdownChore(mobFileCompactionChore);
    shutdownChore(balancerChore);
    if (regionNormalizerManager != null) {
      shutdownChore(regionNormalizerManager.getRegionNormalizerChore());
    }
    shutdownChore(clusterStatusChore);
    shutdownChore(catalogJanitorChore);
    shutdownChore(clusterStatusPublisherChore);
    shutdownChore(snapshotQuotaChore);
    shutdownChore(logCleaner);
    shutdownChore(hfileCleaner);
    shutdownChore(replicationBarrierCleaner);
    shutdownChore(snapshotCleanerChore);
    shutdownChore(hbckChore);
    shutdownChore(regionsRecoveryChore);
    shutdownChore(rollingUpgradeChore);
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
    while (!interrupted && EnvironmentEdgeManager.currentTime() < nextBalanceStartTime
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
        >= maxRegionsInTransition && EnvironmentEdgeManager.currentTime() <= cutoffTime) {
      try {
        // sleep if the number of regions in transition exceeds the limit
        Thread.sleep(100);
      } catch (InterruptedException ie) {
        interrupted = true;
      }
    }

    if (interrupted) Thread.currentThread().interrupt();
  }

  public BalanceResponse balance() throws IOException {
    return balance(BalanceRequest.defaultInstance());
  }

  /**
   *  Trigger a normal balance, see {@link HMaster#balance()} . If the balance is not executed
   *  this time, the metrics related to the balance will be updated.
   *
   *  When balance is running, related metrics will be updated at the same time. But if some
   *  checking logic failed and cause the balancer exit early, we lost the chance to update
   *  balancer metrics. This will lead to user missing the latest balancer info.
   * */
  public BalanceResponse balanceOrUpdateMetrics() throws IOException{
    synchronized (this.balancer) {
      BalanceResponse response = balance();
      if (!response.isBalancerRan()) {
        Map<TableName, Map<ServerName, List<RegionInfo>>> assignments =
          this.assignmentManager.getRegionStates().getAssignmentsForBalancer(this.tableStateManager,
            this.serverManager.getOnlineServersList());
        for (Map<ServerName, List<RegionInfo>> serverMap : assignments.values()) {
          serverMap.keySet().removeAll(this.serverManager.getDrainingServersList());
        }
        this.balancer.updateBalancerLoadInfo(assignments);
      }
      return response;
    }
  }

  /**
   * Checks master state before initiating action over region topology.
   * @param action the name of the action under consideration, for logging.
   * @return {@code true} when the caller should exit early, {@code false} otherwise.
   */
  @Override
  public boolean skipRegionManagementAction(final String action) {
    // Note: this method could be `default` on MasterServices if but for logging.
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

  public BalanceResponse balance(BalanceRequest request) throws IOException {
    checkInitialized();

    BalanceResponse.Builder responseBuilder = BalanceResponse.newBuilder();

    if (loadBalancerTracker == null
      || !(loadBalancerTracker.isBalancerOn() || request.isDryRun())) {
      return responseBuilder.build();
    }

    if (skipRegionManagementAction("balancer")) {
      return responseBuilder.build();
    }

    synchronized (this.balancer) {
        // Only allow one balance run at at time.
      if (this.assignmentManager.hasRegionsInTransition()) {
        List<RegionStateNode> regionsInTransition = assignmentManager.getRegionsInTransition();
        // if hbase:meta region is in transition, result of assignment cannot be recorded
        // ignore the force flag in that case
        boolean metaInTransition = assignmentManager.isMetaRegionInTransition();
        List<RegionStateNode> toPrint = regionsInTransition;
        int max = 5;
        boolean truncated = false;
        if (regionsInTransition.size() > max) {
          toPrint = regionsInTransition.subList(0, max);
          truncated = true;
        }

        if (!request.isIgnoreRegionsInTransition() || metaInTransition) {
          LOG.info("Not running balancer (ignoreRIT=false" + ", metaRIT=" + metaInTransition +
            ") because " + regionsInTransition.size() + " region(s) in transition: " + toPrint
            + (truncated? "(truncated list)": ""));
          return responseBuilder.build();
        }
      }
      if (this.serverManager.areDeadServersInProgress()) {
        LOG.info("Not running balancer because processing dead regionserver(s): " +
          this.serverManager.getDeadServers());
        return responseBuilder.build();
      }

      if (this.cpHost != null) {
        try {
          if (this.cpHost.preBalance(request)) {
            LOG.debug("Coprocessor bypassing balancer request");
            return responseBuilder.build();
          }
        } catch (IOException ioe) {
          LOG.error("Error invoking master coprocessor preBalance()", ioe);
          return responseBuilder.build();
        }
      }

      Map<TableName, Map<ServerName, List<RegionInfo>>> assignments =
        this.assignmentManager.getRegionStates()
          .getAssignmentsForBalancer(tableStateManager, this.serverManager.getOnlineServersList());
      for (Map<ServerName, List<RegionInfo>> serverMap : assignments.values()) {
        serverMap.keySet().removeAll(this.serverManager.getDrainingServersList());
      }

      //Give the balancer the current cluster state.
      this.balancer.updateClusterMetrics(getClusterMetricsWithoutCoprocessor());

      List<RegionPlan> plans = this.balancer.balanceCluster(assignments);

      responseBuilder.setBalancerRan(true).setMovesCalculated(plans == null ? 0 : plans.size());

      if (skipRegionManagementAction("balancer")) {
        // make one last check that the cluster isn't shutting down before proceeding.
        return responseBuilder.build();
      }

      // For dry run we don't actually want to execute the moves, but we do want
      // to execute the coprocessor below
      List<RegionPlan> sucRPs = request.isDryRun()
        ? Collections.emptyList()
        : executeRegionPlansWithThrottling(plans);

      if (this.cpHost != null) {
        try {
          this.cpHost.postBalance(request, sucRPs);
        } catch (IOException ioe) {
          // balancing already succeeded so don't change the result
          LOG.error("Error invoking master coprocessor postBalance()", ioe);
        }
      }

      responseBuilder.setMovesExecuted(sucRPs.size());
    }

    // If LoadBalancer did not generate any plans, it means the cluster is already balanced.
    // Return true indicating a success.
    return responseBuilder.build();
  }

  /**
   * Execute region plans with throttling
   * @param plans to execute
   * @return succeeded plans
   */
  public List<RegionPlan> executeRegionPlansWithThrottling(List<RegionPlan> plans) {
    List<RegionPlan> successRegionPlans = new ArrayList<>();
    int maxRegionsInTransition = getMaxRegionsInTransition();
    long balanceStartTime = EnvironmentEdgeManager.currentTime();
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
          this.assignmentManager.balance(plan);
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
          && EnvironmentEdgeManager.currentTime() > cutoffTime) {
          // TODO: After balance, there should not be a cutoff time (keeping it as
          // a security net for now)
          LOG.debug("No more balancing till next balance run; maxBalanceTime="
              + this.maxBalancingTime);
          break;
        }
      }
    }
    LOG.debug("Balancer is going into sleep until next period in {}ms", getConfiguration()
      .getInt(HConstants.HBASE_BALANCER_PERIOD, HConstants.DEFAULT_HBASE_BALANCER_PERIOD));
    return successRegionPlans;
  }

  @Override
  public RegionNormalizerManager getRegionNormalizerManager() {
    return regionNormalizerManager;
  }

  @Override
  public boolean normalizeRegions(
    final NormalizeTableFilterParams ntfp,
    final boolean isHighPriority
  ) throws IOException {
    if (regionNormalizerManager == null || !regionNormalizerManager.isNormalizerOn()) {
      LOG.debug("Region normalization is disabled, don't run region normalizer.");
      return false;
    }
    if (skipRegionManagementAction("region normalizer")) {
      return false;
    }
    if (assignmentManager.hasRegionsInTransition()) {
      return false;
    }

    final Set<TableName> matchingTables = getTableDescriptors(new LinkedList<>(),
      ntfp.getNamespace(), ntfp.getRegex(), ntfp.getTableNames(), false)
      .stream()
      .map(TableDescriptor::getTableName)
      .collect(Collectors.toSet());
    final Set<TableName> allEnabledTables =
      tableStateManager.getTablesInStates(TableState.State.ENABLED);
    final List<TableName> targetTables =
      new ArrayList<>(Sets.intersection(matchingTables, allEnabledTables));
    Collections.shuffle(targetTables);
    return regionNormalizerManager.normalizeRegions(targetTables, isHighPriority);
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
  @InterfaceAudience.Private
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
      if (conf.getBoolean(WARMUP_BEFORE_MOVE, DEFAULT_WARMUP_BEFORE_MOVE)) {
        // Warmup the region on the destination before initiating the move.
        // A region server could reject the close request because it either does not
        // have the specified region or the region is being split.
        LOG.info(getClientIdAuditPrefix() + " move " + rp + ", warming up region on " +
          rp.getDestination());
        warmUpRegion(rp.getDestination(), hri);
      }
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
  public long modifyColumnStoreFileTracker(TableName tableName, byte[] family, String dstSFT,
    long nonceGroup, long nonce) throws IOException {
    checkInitialized();
    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {

        @Override
        protected void run() throws IOException {
          String sft = getMaster().getMasterCoprocessorHost()
            .preModifyColumnFamilyStoreFileTracker(tableName, family, dstSFT);
          LOG.info("{} modify column {} store file tracker of table {} to {}",
            getClientIdAuditPrefix(), Bytes.toStringBinary(family), tableName, sft);
          submitProcedure(new ModifyColumnFamilyStoreFileTrackerProcedure(
            procedureExecutor.getEnvironment(), tableName, family, sft));
          getMaster().getMasterCoprocessorHost().postModifyColumnFamilyStoreFileTracker(tableName,
            family, dstSFT);
        }

        @Override
        protected String getDescription() {
          return "ModifyColumnFamilyStoreFileTrackerProcedure";
        }
      });
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

  @Override
  public long modifyTableStoreFileTracker(TableName tableName, String dstSFT, long nonceGroup,
    long nonce) throws IOException {
    checkInitialized();
    return MasterProcedureUtil
      .submitProcedure(new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {

        @Override
        protected void run() throws IOException {
          String sft = getMaster().getMasterCoprocessorHost()
            .preModifyTableStoreFileTracker(tableName, dstSFT);
          LOG.info("{} modify table store file tracker of table {} to {}", getClientIdAuditPrefix(),
            tableName, sft);
          submitProcedure(new ModifyTableStoreFileTrackerProcedure(
            procedureExecutor.getEnvironment(), tableName, sft));
          getMaster().getMasterCoprocessorHost().postModifyTableStoreFileTracker(tableName, sft);
        }

        @Override
        protected String getDescription() {
          return "ModifyTableStoreFileTrackerProcedure";
        }
      });
  }

  public long restoreSnapshot(final SnapshotDescription snapshotDesc, final long nonceGroup,
    final long nonce, final boolean restoreAcl, final String customSFT) throws IOException {
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
            getSnapshotManager().restoreOrCloneSnapshot(snapshotDesc, getNonceKey(), restoreAcl,
              customSFT));
        }

        @Override
        protected String getDescription() {
          return "RestoreSnapshotProcedure";
        }
      });
  }

  private void checkTableExists(final TableName tableName)
    throws IOException, TableNotFoundException {
    if (!tableDescriptors.exists(tableName)) {
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

    // TASKS and/or LIVE_SERVERS will populate this map, which will be given to the builder if
    // not null after option processing completes.
    Map<ServerName, ServerMetrics> serverMetricsMap = null;

    for (Option opt : options) {
      switch (opt) {
        case HBASE_VERSION: builder.setHBaseVersion(VersionInfo.getVersion()); break;
        case CLUSTER_ID: builder.setClusterId(getClusterId()); break;
        case MASTER: builder.setMasterName(getServerName()); break;
        case BACKUP_MASTERS: builder.setBackerMasterNames(getBackupMasters()); break;
        case TASKS: {
          // Master tasks
          builder.setMasterTasks(TaskMonitor.get().getTasks().stream()
            .map(task -> ServerTaskBuilder.newBuilder()
              .setDescription(task.getDescription())
              .setStatus(task.getStatus())
              .setState(ServerTask.State.valueOf(task.getState().name()))
              .setStartTime(task.getStartTime())
              .setCompletionTime(task.getCompletionTimestamp())
              .build())
            .collect(Collectors.toList()));
          // TASKS is also synonymous with LIVE_SERVERS for now because task information for
          // regionservers is carried in ServerLoad.
          // Add entries to serverMetricsMap for all live servers, if we haven't already done so
          if (serverMetricsMap == null) {
            serverMetricsMap = getOnlineServers();
          }
          break;
        }
        case LIVE_SERVERS: {
          // Add entries to serverMetricsMap for all live servers, if we haven't already done so
          if (serverMetricsMap == null) {
            serverMetricsMap = getOnlineServers();
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

    if (serverMetricsMap != null) {
      builder.setLiveServerMetrics(serverMetricsMap);
    }

    return builder.build();
  }

  private Map<ServerName, ServerMetrics> getOnlineServers() {
    if (serverManager != null) {
      final Map<ServerName, ServerMetrics> map = new HashMap<>();
      serverManager.getOnlineServers().entrySet()
        .forEach(e -> map.put(e.getKey(), e.getValue()));
      return map;
    }
    return null;
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

  /**
   * @return info port of active master or 0 if any exception occurs.
   */
  public int getActiveMasterInfoPort() {
    return activeMasterManager.getActiveMasterInfoPort();
  }

  /**
   * @param sn is ServerName of the backup master
   * @return info port of backup master or 0 if any exception occurs.
   */
  public int getBackupMasterInfoPort(final ServerName sn) {
    return activeMasterManager.getBackupMasterInfoPort(sn);
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
    if (!this.stopped) {
      LOG.info("***** STOPPING master '" + this + "' *****");
      this.stopped = true;
      LOG.info("STOPPED: " + msg);
      // Wakes run() if it is sleeping
      sleeper.skipSleepCycle();
      if (this.activeMasterManager != null) {
        this.activeMasterManager.stop();
      }
    }
  }

  protected void checkServiceStarted() throws ServerNotRunningYetException {
    if (!serviceStarted) {
      throw new ServerNotRunningYetException("Server is not running yet");
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
   * Report whether this master is started
   *
   * This method is used for testing.
   *
   * @return true if master is ready to go, false if not.
   */
  public boolean isOnline() {
    return serviceStarted;
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
   * Return a list of table table descriptors after applying any provided filter parameters. Note
   * that the user-facing description of this filter logic is presented on the class-level javadoc
   * of {@link NormalizeTableFilterParams}.
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
  public GetRegionInfoResponse.CompactionState getMobCompactionState(TableName tableName) {
    AtomicInteger compactionsCount = mobCompactionStates.get(tableName);
    if (compactionsCount != null && compactionsCount.get() != 0) {
      return GetRegionInfoResponse.CompactionState.MAJOR_AND_MINOR;
    }
    return GetRegionInfoResponse.CompactionState.NONE;
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
      && getRegionNormalizerManager().isNormalizerOn();
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
        List<Pair<ServerName, ReplicationLoadSource>> replicationLoadSourceList =
          replicationLoadSourceMap.get(replicationLoadSource.getPeerID());
        if (replicationLoadSourceList == null) {
          LOG.debug("{} does not exist, but it exists "
            + "in znode(/hbase/replication/rs). when the rs restarts, peerId is deleted, so "
            + "we just need to ignore it", replicationLoadSource.getPeerID());
          continue;
        }
        replicationLoadSourceList.add(new Pair<>(serverName, replicationLoadSource));
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
  @InterfaceAudience.Private
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

  public ActiveMasterManager getActiveMasterManager() {
    return activeMasterManager;
  }

  @Override
  public SyncReplicationReplayWALManager getSyncReplicationReplayWALManager() {
    return this.syncReplicationReplayWALManager;
  }

  public HbckChore getHbckChore() {
    return this.hbckChore;
  }

  @Override
  public void runReplicationBarrierCleaner() {
    ReplicationBarrierCleaner rbc = this.replicationBarrierCleaner;
    if (rbc != null) {
      rbc.chore();
    }
  }

  @Override
  public RSGroupInfoManager getRSGroupInfoManager() {
    return rsGroupInfoManager;
  }

  /**
   * Get the compaction state of the table
   *
   * @param tableName The table name
   * @return CompactionState Compaction state of the table
   */
  public CompactionState getCompactionState(final TableName tableName) {
    CompactionState compactionState = CompactionState.NONE;
    try {
      List<RegionInfo> regions =
        assignmentManager.getRegionStates().getRegionsOfTable(tableName);
      for (RegionInfo regionInfo : regions) {
        ServerName serverName =
          assignmentManager.getRegionStates().getRegionServerOfRegion(regionInfo);
        if (serverName == null) {
          continue;
        }
        ServerMetrics sl = serverManager.getLoad(serverName);
        if (sl == null) {
          continue;
        }
        RegionMetrics regionMetrics = sl.getRegionMetrics().get(regionInfo.getRegionName());
        if (regionMetrics.getCompactionState() == CompactionState.MAJOR) {
          if (compactionState == CompactionState.MINOR) {
            compactionState = CompactionState.MAJOR_AND_MINOR;
          } else {
            compactionState = CompactionState.MAJOR;
          }
        } else if (regionMetrics.getCompactionState() == CompactionState.MINOR) {
          if (compactionState == CompactionState.MAJOR) {
            compactionState = CompactionState.MAJOR_AND_MINOR;
          } else {
            compactionState = CompactionState.MINOR;
          }
        }
      }
    } catch (Exception e) {
      compactionState = null;
      LOG.error("Exception when get compaction state for " + tableName.getNameAsString(), e);
    }
    return compactionState;
  }

  @Override
  public MetaLocationSyncer getMetaLocationSyncer() {
    return metaLocationSyncer;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*/src/test/.*")
  public MasterRegion getMasterRegion() {
    return masterRegion;
  }

  @Override
  public void onConfigurationChange(Configuration newConf) {
    try {
      Superusers.initialize(newConf);
    } catch (IOException e) {
      LOG.warn("Failed to initialize SuperUsers on reloading of the configuration");
    }
    // append the quotas observer back to the master coprocessor key
    setQuotasObserver(newConf);
    // update region server coprocessor if the configuration has changed.
    if (CoprocessorConfigurationUtil.checkConfigurationChange(getConfiguration(), newConf,
      CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY) && !maintenanceMode) {
      LOG.info("Update the master coprocessor(s) because the configuration has changed");
      initializeCoprocessorHost(newConf);
    }
  }

  @Override
  protected NamedQueueRecorder createNamedQueueRecord() {
    final boolean isBalancerDecisionRecording = conf
      .getBoolean(BaseLoadBalancer.BALANCER_DECISION_BUFFER_ENABLED,
        BaseLoadBalancer.DEFAULT_BALANCER_DECISION_BUFFER_ENABLED);
    final boolean isBalancerRejectionRecording = conf
      .getBoolean(BaseLoadBalancer.BALANCER_REJECTION_BUFFER_ENABLED,
        BaseLoadBalancer.DEFAULT_BALANCER_REJECTION_BUFFER_ENABLED);
    if (isBalancerDecisionRecording || isBalancerRejectionRecording) {
      return NamedQueueRecorder.getInstance(conf);
    } else {
      return null;
    }
  }

  @Override
  protected boolean clusterMode() {
    return true;
  }

  public String getClusterId() {
    if (activeMaster) {
      return clusterId;
    }
    return cachedClusterId.getFromCacheOrFetch();
  }

  public Optional<ServerName> getActiveMaster() {
    return activeMasterManager.getActiveMasterServerName();
  }

  public List<ServerName> getBackupMasters() {
    return activeMasterManager.getBackupMasters();
  }

  @Override
  public Iterator<ServerName> getBootstrapNodes() {
    return regionServerTracker.getRegionServers().iterator();
  }

  @Override
  public List<HRegionLocation> getMetaLocations() {
    return metaRegionLocationCache.getMetaRegionLocations();
  }

  public Collection<ServerName> getLiveRegionServers() {
    return regionServerTracker.getRegionServers();
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  void setLoadBalancer(RSGroupBasedLoadBalancer loadBalancer) {
    this.balancer = loadBalancer;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  void setAssignmentManager(AssignmentManager assignmentManager) {
    this.assignmentManager = assignmentManager;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  static void setDisableBalancerChoreForTest(boolean disable) {
    disableBalancerChoreForTest = disable;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
    allowedOnPath = ".*/src/test/.*")
  public ConfigurationManager getConfigurationManager() {
    return configurationManager;
  }


  private void setQuotasObserver(Configuration conf) {
    // Add the Observer to delete quotas on table deletion before starting all CPs by
    // default with quota support, avoiding if user specifically asks to not load this Observer.
    if (QuotaUtil.isQuotaEnabled(conf)) {
      updateConfigurationForQuotasObserver(conf);
    }
  }

  private void initializeCoprocessorHost(Configuration conf) {
    // initialize master side coprocessors before we start handling requests
    this.cpHost = new MasterCoprocessorHost(this, conf);
  }

}
