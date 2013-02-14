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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.DeserializationException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HealthCheckChore;
import org.apache.hadoop.hbase.MasterAdminProtocol;
import org.apache.hadoop.hbase.MasterMonitorProtocol;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.RegionServerStatusProtocol;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitorBase;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorService.ExecutorType;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.HBaseServerRPC;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.ipc.UnknownProtocolException;
import org.apache.hadoop.hbase.master.balancer.BalancerChore;
import org.apache.hadoop.hbase.master.balancer.ClusterStatusChore;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.master.cleaner.LogCleaner;
import org.apache.hadoop.hbase.master.handler.CreateTableHandler;
import org.apache.hadoop.hbase.master.handler.DeleteTableHandler;
import org.apache.hadoop.hbase.master.handler.DisableTableHandler;
import org.apache.hadoop.hbase.master.handler.EnableTableHandler;
import org.apache.hadoop.hbase.master.handler.ModifyTableHandler;
import org.apache.hadoop.hbase.master.handler.ServerShutdownHandler;
import org.apache.hadoop.hbase.master.handler.TableAddFamilyHandler;
import org.apache.hadoop.hbase.master.handler.TableDeleteFamilyHandler;
import org.apache.hadoop.hbase.master.handler.TableModifyFamilyHandler;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.monitoring.MemoryBoundedLogMessageBuffer;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.AddColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.AddColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.AssignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.AssignRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.BalanceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.BalanceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.CatalogScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.CatalogScanResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.CreateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.CreateTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.DeleteColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.DeleteColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.DeleteSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.DeleteSnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.DeleteTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.DeleteTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.DisableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.DisableTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.EnableCatalogJanitorRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.EnableCatalogJanitorResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.EnableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.EnableTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.IsCatalogJanitorEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.IsCatalogJanitorEnabledResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.IsRestoreSnapshotDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.IsRestoreSnapshotDoneResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.ListSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.ListSnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.ModifyColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.ModifyColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.ModifyTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.ModifyTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.MoveRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.OfflineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.OfflineRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.RestoreSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.RestoreSnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.SetBalancerRunningResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.ShutdownRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.ShutdownResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.StopMasterRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.StopMasterResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.TakeSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.TakeSnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.UnassignRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterMonitorProtos.GetClusterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterMonitorProtos.GetClusterStatusResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterMonitorProtos.GetSchemaAlterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterMonitorProtos.GetSchemaAlterStatusResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterMonitorProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterMonitorProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerReportResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.ReportRSFatalErrorRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.ReportRSFatalErrorResponse;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.trace.SpanReceiverHost;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CompressionTest;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.ClusterStatusTracker;
import org.apache.hadoop.hbase.zookeeper.DrainingServerTracker;
import org.apache.hadoop.hbase.zookeeper.LoadBalancerTracker;
import org.apache.hadoop.hbase.zookeeper.RegionServerTracker;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.net.DNS;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.collect.Maps;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

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
 * @see MasterMonitorProtocol
 * @see MasterAdminProtocol
 * @see RegionServerStatusProtocol
 * @see Watcher
 */
@InterfaceAudience.Private
@SuppressWarnings("deprecation")
public class HMaster extends HasThread
implements MasterMonitorProtocol, MasterAdminProtocol, RegionServerStatusProtocol, MasterServices,
Server {
  private static final Log LOG = LogFactory.getLog(HMaster.class.getName());

  // MASTER is name of the webapp and the attribute name used stuffing this
  //instance into web context.
  public static final String MASTER = "master";

  // The configuration for the Master
  private final Configuration conf;
  // server for the web ui
  private InfoServer infoServer;

  // Our zk client.
  private ZooKeeperWatcher zooKeeper;
  // Manager and zk listener for master election
  private ActiveMasterManager activeMasterManager;
  // Region server tracker
  private RegionServerTracker regionServerTracker;
  // Draining region server tracker
  private DrainingServerTracker drainingServerTracker;
  // Tracker for load balancer state
  private LoadBalancerTracker loadBalancerTracker;

  // RPC server for the HMaster
  private final RpcServer rpcServer;
  // Set after we've called HBaseServer#openServer and ready to receive RPCs.
  // Set back to false after we stop rpcServer.  Used by tests.
  private volatile boolean rpcServerOpen = false;

  /**
   * This servers address.
   */
  private final InetSocketAddress isa;

  // Metrics for the HMaster
  private final MetricsMaster metricsMaster;
  // file system manager for the master FS operations
  private MasterFileSystem fileSystemManager;

  // server manager to deal with region server info
  private ServerManager serverManager;

  // manager of assignment nodes in zookeeper
  AssignmentManager assignmentManager;
  // manager of catalog regions
  private CatalogTracker catalogTracker;
  // Cluster status zk tracker and local setter
  private ClusterStatusTracker clusterStatusTracker;

  // buffer for "fatal error" notices from region servers
  // in the cluster. This is only used for assisting
  // operations/debugging.
  private MemoryBoundedLogMessageBuffer rsFatals;

  // This flag is for stopping this Master instance.  Its set when we are
  // stopping or aborting
  private volatile boolean stopped = false;
  // Set on abort -- usually failure of our zk session.
  private volatile boolean abort = false;
  // flag set after we become the active master (used for testing)
  private volatile boolean isActiveMaster = false;
  // flag set after we complete initialization once active (used for testing)
  private volatile boolean initialized = false;
  // flag set after we complete assignRootAndMeta.
  private volatile boolean serverShutdownHandlerEnabled = false;

  // Instance of the hbase executor service.
  ExecutorService executorService;

  private LoadBalancer balancer;
  private Thread balancerChore;
  private Thread clusterStatusChore;

  private CatalogJanitor catalogJanitorChore;
  private LogCleaner logCleaner;
  private HFileCleaner hfileCleaner;

  private MasterCoprocessorHost cpHost;
  private final ServerName serverName;

  private TableDescriptors tableDescriptors;

  // Time stamps for when a hmaster was started and when it became active
  private long masterStartTime;
  private long masterActiveTime;

  /** time interval for emitting metrics values */
  private final int msgInterval;
  /**
   * MX Bean for MasterInfo
   */
  private ObjectName mxBean = null;

  //should we check the compression codec type at master side, default true, HBASE-6370
  private final boolean masterCheckCompression;

  private SpanReceiverHost spanReceiverHost;

  private Map<String, Service> coprocessorServiceHandlers = Maps.newHashMap();

  // monitor for snapshot of hbase tables
  private SnapshotManager snapshotManager;

  /** The health check chore. */
  private HealthCheckChore healthCheckChore;

  /**
   * Initializes the HMaster. The steps are as follows:
   * <p>
   * <ol>
   * <li>Initialize HMaster RPC and address
   * <li>Connect to ZooKeeper.
   * </ol>
   * <p>
   * Remaining steps of initialization occur in {@link #run()} so that they
   * run in their own thread rather than within the context of the constructor.
   * @throws InterruptedException
   */
  public HMaster(final Configuration conf)
  throws IOException, KeeperException, InterruptedException {
    this.conf = new Configuration(conf);
    // Disable the block cache on the master
    this.conf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
    // Set how many times to retry talking to another server over HConnection.
    HConnectionManager.setServerSideHConnectionRetries(this.conf, LOG);
    // Server to handle client requests.
    String hostname = Strings.domainNamePointerToHostName(DNS.getDefaultHost(
      conf.get("hbase.master.dns.interface", "default"),
      conf.get("hbase.master.dns.nameserver", "default")));
    int port = conf.getInt(HConstants.MASTER_PORT, HConstants.DEFAULT_MASTER_PORT);
    // Creation of a ISA will force a resolve.
    InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);
    if (initialIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initialIsa);
    }
    int numHandlers = conf.getInt("hbase.master.handler.count",
      conf.getInt("hbase.regionserver.handler.count", 25));
    this.rpcServer = HBaseServerRPC.getServer(MasterMonitorProtocol.class, this,
        new Class<?>[]{MasterMonitorProtocol.class,
            MasterAdminProtocol.class, RegionServerStatusProtocol.class},
        initialIsa.getHostName(), // BindAddress is IP we got for this server.
        initialIsa.getPort(),
        numHandlers,
        0, // we dont use high priority handlers in master
        conf.getBoolean("hbase.rpc.verbose", false), conf,
        0); // this is a DNC w/o high priority handlers
    // Set our address.
    this.isa = this.rpcServer.getListenerAddress();
    this.serverName = new ServerName(this.isa.getHostName(),
      this.isa.getPort(), System.currentTimeMillis());
    this.rsFatals = new MemoryBoundedLogMessageBuffer(
        conf.getLong("hbase.master.buffer.for.rs.fatals", 1*1024*1024));

    // login the zookeeper client principal (if using security)
    ZKUtil.loginClient(this.conf, "hbase.zookeeper.client.keytab.file",
      "hbase.zookeeper.client.kerberos.principal", this.isa.getHostName());

    // initialize server principal (if using secure Hadoop)
    User.login(conf, "hbase.master.keytab.file",
      "hbase.master.kerberos.principal", this.isa.getHostName());

    LOG.info("hbase.rootdir=" + FSUtils.getRootDir(this.conf) +
        ", hbase.cluster.distributed=" + this.conf.getBoolean("hbase.cluster.distributed", false));

    // set the thread name now we have an address
    setName(MASTER + "-" + this.serverName.toString());

    Replication.decorateMasterConfiguration(this.conf);

    // Hack! Maps DFSClient => Master for logs.  HDFS made this
    // config param for task trackers, but we can piggyback off of it.
    if (this.conf.get("mapred.task.id") == null) {
      this.conf.set("mapred.task.id", "hb_m_" + this.serverName.toString());
    }

    this.zooKeeper = new ZooKeeperWatcher(conf, MASTER + ":" + isa.getPort(), this, true);
    this.rpcServer.startThreads();

    // metrics interval: using the same property as region server.
    this.msgInterval = conf.getInt("hbase.regionserver.msginterval", 3 * 1000);

    //should we check the compression codec type at master side, default true, HBASE-6370
    this.masterCheckCompression = conf.getBoolean("hbase.master.check.compression", true);

    this.metricsMaster = new MetricsMaster( new MetricsMasterWrapperImpl(this));

    // Health checker thread.
    int sleepTime = this.conf.getInt(HConstants.HEALTH_CHORE_WAKE_FREQ,
      HConstants.DEFAULT_THREAD_WAKE_FREQUENCY);
    if (isHealthCheckerConfigured()) {
      healthCheckChore = new HealthCheckChore(sleepTime, this, getConfiguration());
    }
  }

  /**
   * Stall startup if we are designated a backup master; i.e. we want someone
   * else to become the master before proceeding.
   * @param c
   * @param amm
   * @throws InterruptedException
   */
  private static void stallIfBackupMaster(final Configuration c,
      final ActiveMasterManager amm)
  throws InterruptedException {
    // If we're a backup master, stall until a primary to writes his address
    if (!c.getBoolean(HConstants.MASTER_TYPE_BACKUP,
      HConstants.DEFAULT_MASTER_TYPE_BACKUP)) {
      return;
    }
    LOG.debug("HMaster started in backup mode.  " +
      "Stalling until master znode is written.");
    // This will only be a minute or so while the cluster starts up,
    // so don't worry about setting watches on the parent znode
    while (!amm.isActiveMaster()) {
      LOG.debug("Waiting for master address ZNode to be written " +
        "(Also watching cluster state node)");
      Thread.sleep(
        c.getInt(HConstants.ZK_SESSION_TIMEOUT, HConstants.DEFAULT_ZK_SESSION_TIMEOUT));
    }

  }

  MetricsMaster getMetrics() {
    return metricsMaster;
  }

  /**
   * Main processing loop for the HMaster.
   * <ol>
   * <li>Block until becoming active master
   * <li>Finish initialization via finishInitialization(MonitoredTask)
   * <li>Enter loop until we are stopped
   * <li>Stop services and perform cleanup once stopped
   * </ol>
   */
  @Override
  public void run() {
    MonitoredTask startupStatus =
      TaskMonitor.get().createStatus("Master startup");
    startupStatus.setDescription("Master startup");
    masterStartTime = System.currentTimeMillis();
    try {
      /*
       * Block on becoming the active master.
       *
       * We race with other masters to write our address into ZooKeeper.  If we
       * succeed, we are the primary/active master and finish initialization.
       *
       * If we do not succeed, there is another active master and we should
       * now wait until it dies to try and become the next active master.  If we
       * do not succeed on our first attempt, this is no longer a cluster startup.
       */
      becomeActiveMaster(startupStatus);

      // We are either the active master or we were asked to shutdown
      if (!this.stopped) {
        finishInitialization(startupStatus, false);
        loop();
      }
    } catch (Throwable t) {
      // HBASE-5680: Likely hadoop23 vs hadoop 20.x/1.x incompatibility
      if (t instanceof NoClassDefFoundError &&
          t.getMessage().contains("org/apache/hadoop/hdfs/protocol/FSConstants$SafeModeAction")) {
          // improved error message for this special case
          abort("HBase is having a problem with its Hadoop jars.  You may need to "
              + "recompile HBase against Hadoop version "
              +  org.apache.hadoop.util.VersionInfo.getVersion()
              + " or change your hadoop jars to start properly", t);
      } else {
        abort("Unhandled exception. Starting shutdown.", t);
      }
    } finally {
      startupStatus.cleanup();

      stopChores();
      // Wait for all the remaining region servers to report in IFF we were
      // running a cluster shutdown AND we were NOT aborting.
      if (!this.abort && this.serverManager != null &&
          this.serverManager.isClusterShutdown()) {
        this.serverManager.letRegionServersShutdown();
      }
      stopServiceThreads();
      // Stop services started for both backup and active masters
      if (this.activeMasterManager != null) this.activeMasterManager.stop();
      if (this.catalogTracker != null) this.catalogTracker.stop();
      if (this.serverManager != null) this.serverManager.stop();
      if (this.assignmentManager != null) this.assignmentManager.stop();
      if (this.fileSystemManager != null) this.fileSystemManager.stop();
      if (this.snapshotManager != null) this.snapshotManager.stop("server shutting down.");
      this.zooKeeper.close();
    }
    LOG.info("HMaster main thread exiting");
  }

  /**
   * Try becoming active master.
   * @param startupStatus
   * @return True if we could successfully become the active master.
   * @throws InterruptedException
   */
  private boolean becomeActiveMaster(MonitoredTask startupStatus)
  throws InterruptedException {
    // TODO: This is wrong!!!! Should have new servername if we restart ourselves,
    // if we come back to life.
    this.activeMasterManager = new ActiveMasterManager(zooKeeper, this.serverName,
        this);
    this.zooKeeper.registerListener(activeMasterManager);
    stallIfBackupMaster(this.conf, this.activeMasterManager);

    // The ClusterStatusTracker is setup before the other
    // ZKBasedSystemTrackers because it's needed by the activeMasterManager
    // to check if the cluster should be shutdown.
    this.clusterStatusTracker = new ClusterStatusTracker(getZooKeeper(), this);
    this.clusterStatusTracker.start();
    return this.activeMasterManager.blockUntilBecomingActiveMaster(startupStatus,
        this.clusterStatusTracker);
  }

  /**
   * Initialize all ZK based system trackers.
   * @throws IOException
   * @throws InterruptedException
   */
  private void initializeZKBasedSystemTrackers() throws IOException,
      InterruptedException, KeeperException {
    this.catalogTracker = createCatalogTracker(this.zooKeeper, this.conf, this);
    this.catalogTracker.start();

    this.balancer = LoadBalancerFactory.getLoadBalancer(conf);
    this.loadBalancerTracker = new LoadBalancerTracker(zooKeeper, this);
    this.loadBalancerTracker.start();
    this.assignmentManager = new AssignmentManager(this, serverManager,
      this.catalogTracker, this.balancer, this.executorService, this.metricsMaster);
    zooKeeper.registerListenerFirst(assignmentManager);

    this.regionServerTracker = new RegionServerTracker(zooKeeper, this,
        this.serverManager);
    this.regionServerTracker.start();

    this.drainingServerTracker = new DrainingServerTracker(zooKeeper, this,
      this.serverManager);
    this.drainingServerTracker.start();

    // Set the cluster as up.  If new RSs, they'll be waiting on this before
    // going ahead with their startup.
    boolean wasUp = this.clusterStatusTracker.isClusterUp();
    if (!wasUp) this.clusterStatusTracker.setClusterUp();

    LOG.info("Server active/primary master; " + this.serverName +
        ", sessionid=0x" +
        Long.toHexString(this.zooKeeper.getRecoverableZooKeeper().getSessionId()) +
        ", cluster-up flag was=" + wasUp);

    // create the snapshot manager
    this.snapshotManager = new SnapshotManager(this);
  }

  /**
   * Create CatalogTracker.
   * In its own method so can intercept and mock it over in tests.
   * @param zk If zk is null, we'll create an instance (and shut it down
   * when {@link #stop(String)} is called) else we'll use what is passed.
   * @param conf
   * @param abortable If fatal exception we'll call abort on this.  May be null.
   * If it is we'll use the Connection associated with the passed
   * {@link Configuration} as our {@link Abortable}.
   * @param defaultTimeout Timeout to use.  Pass zero for no timeout
   * ({@link Object#wait(long)} when passed a <code>0</code> waits for ever).
   * @throws IOException
   */
  CatalogTracker createCatalogTracker(final ZooKeeperWatcher zk,
      final Configuration conf, Abortable abortable)
  throws IOException {
    return new CatalogTracker(zk, conf, abortable);
  }

  // Check if we should stop every 100ms
  private Sleeper stopSleeper = new Sleeper(100, this);

  private void loop() {
    long lastMsgTs = 0l;
    long now = 0l;
    while (!this.stopped) {
      now = System.currentTimeMillis();
      if ((now - lastMsgTs) >= this.msgInterval) {
        doMetrics();
        lastMsgTs = System.currentTimeMillis();
      }
      stopSleeper.sleep();
    }
  }

  /**
   * Emit the HMaster metrics, such as region in transition metrics.
   * Surrounding in a try block just to be sure metrics doesn't abort HMaster.
   */
  private void doMetrics() {
    try {
      this.assignmentManager.updateRegionsInTransitionMetrics();
    } catch (Throwable e) {
      LOG.error("Couldn't update metrics: " + e.getMessage());
    }
  }

  /**
   * Finish initialization of HMaster after becoming the primary master.
   *
   * <ol>
   * <li>Initialize master components - file system manager, server manager,
   *     assignment manager, region server tracker, catalog tracker, etc</li>
   * <li>Start necessary service threads - rpc server, info server,
   *     executor services, etc</li>
   * <li>Set cluster as UP in ZooKeeper</li>
   * <li>Wait for RegionServers to check-in</li>
   * <li>Split logs and perform data recovery, if necessary</li>
   * <li>Ensure assignment of root and meta regions<li>
   * <li>Handle either fresh cluster start or master failover</li>
   * </ol>
   *
   * @param masterRecovery
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  private void finishInitialization(MonitoredTask status, boolean masterRecovery)
  throws IOException, InterruptedException, KeeperException {

    isActiveMaster = true;

    /*
     * We are active master now... go initialize components we need to run.
     * Note, there may be dross in zk from previous runs; it'll get addressed
     * below after we determine if cluster startup or failover.
     */

    status.setStatus("Initializing Master file system");
    this.masterActiveTime = System.currentTimeMillis();
    // TODO: Do this using Dependency Injection, using PicoContainer, Guice or Spring.
    this.fileSystemManager = new MasterFileSystem(this, this, metricsMaster, masterRecovery);

    this.tableDescriptors =
      new FSTableDescriptors(this.fileSystemManager.getFileSystem(),
      this.fileSystemManager.getRootDir());

    // publish cluster ID
    status.setStatus("Publishing Cluster ID in ZooKeeper");
    ZKClusterId.setClusterId(this.zooKeeper, fileSystemManager.getClusterId());

    if (!masterRecovery) {
      this.executorService = new ExecutorService(getServerName().toString());
      this.serverManager = createServerManager(this, this);
    }

    status.setStatus("Initializing ZK system trackers");
    initializeZKBasedSystemTrackers();

    if (!masterRecovery) {
      // initialize master side coprocessors before we start handling requests
      status.setStatus("Initializing master coprocessors");
      this.cpHost = new MasterCoprocessorHost(this, this.conf);

      spanReceiverHost = new SpanReceiverHost(getConfiguration());
      spanReceiverHost.loadSpanReceivers();

      // start up all service threads.
      status.setStatus("Initializing master service threads");
      startServiceThreads();
    }

    // Wait for region servers to report in.
    this.serverManager.waitForRegionServers(status);
    // Check zk for region servers that are up but didn't register
    for (ServerName sn: this.regionServerTracker.getOnlineServers()) {
      if (!this.serverManager.isServerOnline(sn)) {
        // Not registered; add it.
        LOG.info("Registering server found up in zk but who has not yet " +
          "reported in: " + sn);
        this.serverManager.recordNewServer(sn, ServerLoad.EMPTY_SERVERLOAD);
      }
    }

    if (!masterRecovery) {
      this.assignmentManager.startTimeOutMonitor();
    }

    // TODO: Should do this in background rather than block master startup
    status.setStatus("Splitting logs after master startup");
    splitLogAfterStartup(this.fileSystemManager);

    // Make sure root and meta assigned before proceeding.
    if (!assignRootAndMeta(status)) return;
    enableServerShutdownHandler();

    // Update meta with new PB serialization if required. i.e migrate all HRI
    // to PB serialization in meta and update the status in ROOT. This must happen
    // before we assign all user regions or else the assignment will fail.
    // TODO: Remove this after 0.96, when we do 0.98.
    org.apache.hadoop.hbase.catalog.MetaMigrationConvertingToPB
      .updateRootAndMetaIfNecessary(this);

    this.balancer.setMasterServices(this);
    // Fix up assignment manager status
    status.setStatus("Starting assignment manager");
    this.assignmentManager.joinCluster();

    this.balancer.setClusterStatus(getClusterStatus());

    // Fixing up missing daughters if any
    status.setStatus("Fixing up missing daughters");
    fixupDaughters(status);

    if (!masterRecovery) {
      // Start balancer and meta catalog janitor after meta and regions have
      // been assigned.
      status.setStatus("Starting balancer and catalog janitor");
      this.clusterStatusChore = getAndStartClusterStatusChore(this);
      this.balancerChore = getAndStartBalancerChore(this);
      this.catalogJanitorChore = new CatalogJanitor(this, this);
      startCatalogJanitorChore();
    }

    status.markComplete("Initialization successful");
    LOG.info("Master has completed initialization");
    initialized = true;
    // clear the dead servers with same host name and port of online server because we are not
    // removing dead server with same hostname and port of rs which is trying to check in before
    // master initialization. See HBASE-5916.
    this.serverManager.clearDeadServersWithSameHostNameAndPortOfOnlineServer();

    if (!masterRecovery) {
      if (this.cpHost != null) {
        // don't let cp initialization errors kill the master
        try {
          this.cpHost.postStartMaster();
        } catch (IOException ioe) {
          LOG.error("Coprocessor postStartMaster() hook failed", ioe);
        }
      }
    }
  }

  /**
   * Useful for testing purpose also where we have
   * master restart scenarios.
   */
  protected void startCatalogJanitorChore() {
    Threads.setDaemonThreadRunning(catalogJanitorChore.getThread());
  }

  /**
   * Override to change master's splitLogAfterStartup. Used testing
   * @param mfs
   */
  protected void splitLogAfterStartup(final MasterFileSystem mfs) {
    mfs.splitLogAfterStartup();
  }

  /**
   * Create a {@link ServerManager} instance.
   * @param master
   * @param services
   * @return An instance of {@link ServerManager}
   * @throws ZooKeeperConnectionException
   * @throws IOException
   */
  ServerManager createServerManager(final Server master,
      final MasterServices services)
  throws IOException {
    // We put this out here in a method so can do a Mockito.spy and stub it out
    // w/ a mocked up ServerManager.
    return new ServerManager(master, services);
  }

  /**
   * If ServerShutdownHandler is disabled, we enable it and expire those dead
   * but not expired servers.
   */
  private void enableServerShutdownHandler() {
    if (!serverShutdownHandlerEnabled) {
      serverShutdownHandlerEnabled = true;
      this.serverManager.processQueuedDeadServers();
    }
  }

  /**
   * Check <code>-ROOT-</code> and <code>.META.</code> are assigned.  If not,
   * assign them.
   * @throws InterruptedException
   * @throws IOException
   * @throws KeeperException
   * @return True if root and meta are healthy, assigned
   */
  boolean assignRootAndMeta(MonitoredTask status)
  throws InterruptedException, IOException, KeeperException {
    int assigned = 0;
    long timeout = this.conf.getLong("hbase.catalog.verification.timeout", 1000);

    // Work on ROOT region.  Is it in zk in transition?
    status.setStatus("Assigning ROOT region");
    assignmentManager.getRegionStates().createRegionState(
      HRegionInfo.ROOT_REGIONINFO);
    boolean rit = this.assignmentManager.
      processRegionInTransitionAndBlockUntilAssigned(HRegionInfo.ROOT_REGIONINFO);
    ServerName currentRootServer = null;
    boolean rootRegionLocation = catalogTracker.verifyRootRegionLocation(timeout);
    if (!rit && !rootRegionLocation) {
      currentRootServer = this.catalogTracker.getRootLocation();
      splitLogAndExpireIfOnline(currentRootServer);
      this.assignmentManager.assignRoot();
      // Make sure a -ROOT- location is set.
      if (!isRootLocation()) return false;
      // This guarantees that the transition assigning -ROOT- has completed
      this.assignmentManager.waitForAssignment(HRegionInfo.ROOT_REGIONINFO);
      assigned++;
    } else if (rit && !rootRegionLocation) {
      // Make sure a -ROOT- location is set.
      if (!isRootLocation()) return false;
      // This guarantees that the transition assigning -ROOT- has completed
      this.assignmentManager.waitForAssignment(HRegionInfo.ROOT_REGIONINFO);
      assigned++;
    } else if (rootRegionLocation) {
      // Region already assigned.  We didn't assign it.  Add to in-memory state.
      this.assignmentManager.regionOnline(HRegionInfo.ROOT_REGIONINFO,
        this.catalogTracker.getRootLocation());
    }
    // Enable the ROOT table if on process fail over the RS containing ROOT
    // was active.
    enableCatalogTables(Bytes.toString(HConstants.ROOT_TABLE_NAME));
    // Check for stopped, just in case
    if (this.stopped) return false;
    LOG.info("-ROOT- assigned=" + assigned + ", rit=" + rit +
      ", location=" + catalogTracker.getRootLocation());

    // Work on meta region
    status.setStatus("Assigning META region");
    assignmentManager.getRegionStates().createRegionState(
      HRegionInfo.FIRST_META_REGIONINFO);
    rit = this.assignmentManager.
      processRegionInTransitionAndBlockUntilAssigned(HRegionInfo.FIRST_META_REGIONINFO);
    boolean metaRegionLocation = this.catalogTracker.verifyMetaRegionLocation(timeout);
    if (!rit && !metaRegionLocation) {
      ServerName currentMetaServer =
        this.catalogTracker.getMetaLocationOrReadLocationFromRoot();
      if (currentMetaServer != null
          && !currentMetaServer.equals(currentRootServer)) {
        splitLogAndExpireIfOnline(currentMetaServer);
      }
      assignmentManager.assignMeta();
      enableSSHandWaitForMeta();
      assigned++;
    } else if (rit && !metaRegionLocation) {
      // Wait until META region added to region server onlineRegions. See HBASE-5875.
      enableSSHandWaitForMeta();
      assigned++;
    } else {
      // Region already assigned.  We didn't assign it.  Add to in-memory state.
      this.assignmentManager.regionOnline(HRegionInfo.FIRST_META_REGIONINFO,
        this.catalogTracker.getMetaLocation());
    }
    enableCatalogTables(Bytes.toString(HConstants.META_TABLE_NAME));
    LOG.info(".META. assigned=" + assigned + ", rit=" + rit +
      ", location=" + catalogTracker.getMetaLocation());
    status.setStatus("META and ROOT assigned.");
    return true;
  }

  private void enableSSHandWaitForMeta() throws IOException, InterruptedException {
    enableServerShutdownHandler();
    this.catalogTracker.waitForMeta();
    // Above check waits for general meta availability but this does not
    // guarantee that the transition has completed
    this.assignmentManager.waitForAssignment(HRegionInfo.FIRST_META_REGIONINFO);
  }

  /**
   * @return True if there a root available
   * @throws InterruptedException
   */
  private boolean isRootLocation() throws InterruptedException {
    // Cycle up here in master rather than down in catalogtracker so we can
    // check the master stopped flag every so often.
    while (!this.stopped) {
      try {
        if (this.catalogTracker.waitForRoot(100) != null) break;
      } catch (NotAllMetaRegionsOnlineException e) {
        // Ignore.  I know -ROOT- is not online yet.
      }
    }
    // We got here because we came of above loop.
    return !this.stopped;
  }

  private void enableCatalogTables(String catalogTableName) {
    if (!this.assignmentManager.getZKTable().isEnabledTable(catalogTableName)) {
      this.assignmentManager.setEnabledTable(catalogTableName);
    }
  }

  void fixupDaughters(final MonitoredTask status) throws IOException {
    final Map<HRegionInfo, Result> offlineSplitParents =
      new HashMap<HRegionInfo, Result>();
    // This visitor collects offline split parents in the .META. table
    MetaReader.Visitor visitor = new MetaReader.Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r == null || r.isEmpty()) return true;
        HRegionInfo info =
          HRegionInfo.getHRegionInfo(r);
        if (info == null) return true; // Keep scanning
        if (info.isOffline() && info.isSplit()) {
          offlineSplitParents.put(info, r);
        }
        // Returning true means "keep scanning"
        return true;
      }
    };
    // Run full scan of .META. catalog table passing in our custom visitor
    MetaReader.fullScan(this.catalogTracker, visitor);
    // Now work on our list of found parents. See if any we can clean up.
    int fixups = 0;
    for (Map.Entry<HRegionInfo, Result> e : offlineSplitParents.entrySet()) {
      ServerName sn = HRegionInfo.getServerName(e.getValue());
      if (!serverManager.isServerDead(sn)) { // Otherwise, let SSH take care of it
        fixups += ServerShutdownHandler.fixupDaughters(
          e.getValue(), assignmentManager, catalogTracker);
      }
    }
    if (fixups != 0) {
      LOG.info("Scanned the catalog and fixed up " + fixups +
        " missing daughter region(s)");
    }
  }

  /**
   * Split a server's log and expire it if we find it is one of the online
   * servers.
   * @param sn ServerName to check.
   * @throws IOException
   */
  private void splitLogAndExpireIfOnline(final ServerName sn)
      throws IOException {
    if (sn == null || !serverManager.isServerOnline(sn)) {
      return;
    }
    LOG.info("Forcing splitLog and expire of " + sn);
    fileSystemManager.splitMetaLog(sn);
    fileSystemManager.splitLog(sn);
    serverManager.expireServer(sn);
  }

  @Override
  public TableDescriptors getTableDescriptors() {
    return this.tableDescriptors;
  }

  /** @return InfoServer object. Maybe null.*/
  public InfoServer getInfoServer() {
    return this.infoServer;
  }

  @Override
  public Configuration getConfiguration() {
    return this.conf;
  }

  @Override
  public ServerManager getServerManager() {
    return this.serverManager;
  }

  @Override
  public ExecutorService getExecutorService() {
    return this.executorService;
  }

  @Override
  public MasterFileSystem getMasterFileSystem() {
    return this.fileSystemManager;
  }

  /**
   * Get the ZK wrapper object - needed by master_jsp.java
   * @return the zookeeper wrapper
   */
  public ZooKeeperWatcher getZooKeeperWatcher() {
    return this.zooKeeper;
  }

  /*
   * Start up all services. If any of these threads gets an unhandled exception
   * then they just die with a logged message.  This should be fine because
   * in general, we do not expect the master to get such unhandled exceptions
   *  as OOMEs; it should be lightly loaded. See what HRegionServer does if
   *  need to install an unexpected exception handler.
   */
  void startServiceThreads() throws IOException{

   // Start the executor service pools
   this.executorService.startExecutorService(ExecutorType.MASTER_OPEN_REGION,
      conf.getInt("hbase.master.executor.openregion.threads", 5));
   this.executorService.startExecutorService(ExecutorType.MASTER_CLOSE_REGION,
      conf.getInt("hbase.master.executor.closeregion.threads", 5));
   this.executorService.startExecutorService(ExecutorType.MASTER_SERVER_OPERATIONS,
      conf.getInt("hbase.master.executor.serverops.threads", 3));
   this.executorService.startExecutorService(ExecutorType.MASTER_META_SERVER_OPERATIONS,
      conf.getInt("hbase.master.executor.serverops.threads", 5));

   // We depend on there being only one instance of this executor running
   // at a time.  To do concurrency, would need fencing of enable/disable of
   // tables.
   this.executorService.startExecutorService(ExecutorType.MASTER_TABLE_OPERATIONS, 1);

   // Start log cleaner thread
   String n = Thread.currentThread().getName();
   int cleanerInterval = conf.getInt("hbase.master.cleaner.interval", 60 * 1000);
   this.logCleaner =
      new LogCleaner(cleanerInterval,
         this, conf, getMasterFileSystem().getFileSystem(),
         getMasterFileSystem().getOldLogDir());
         Threads.setDaemonThreadRunning(logCleaner.getThread(), n + ".oldLogCleaner");

   //start the hfile archive cleaner thread
    Path archiveDir = HFileArchiveUtil.getArchivePath(conf);
    this.hfileCleaner = new HFileCleaner(cleanerInterval, this, conf, getMasterFileSystem()
        .getFileSystem(), archiveDir);
    Threads.setDaemonThreadRunning(hfileCleaner.getThread(), n + ".archivedHFileCleaner");

   // Put up info server.
   int port = this.conf.getInt(HConstants.MASTER_INFO_PORT, 60010);
   if (port >= 0) {
     String a = this.conf.get("hbase.master.info.bindAddress", "0.0.0.0");
     this.infoServer = new InfoServer(MASTER, a, port, false, this.conf);
     this.infoServer.addServlet("status", "/master-status", MasterStatusServlet.class);
     this.infoServer.addServlet("dump", "/dump", MasterDumpServlet.class);
     this.infoServer.setAttribute(MASTER, this);
     this.infoServer.start();
    }

   // Start the health checker
   if (this.healthCheckChore != null) {
     Threads.setDaemonThreadRunning(this.healthCheckChore.getThread(), n + ".healthChecker");
   }

    // Start allowing requests to happen.
    this.rpcServer.openServer();
    this.rpcServerOpen = true;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Started service threads");
    }
  }

  /**
   * Use this when trying to figure when its ok to send in rpcs.  Used by tests.
   * @return True if we have successfully run {@link HBaseServer#openServer()}
   */
  boolean isRpcServerOpen() {
    return this.rpcServerOpen;
  }

  private void stopServiceThreads() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping service threads");
    }
    if (this.rpcServer != null) this.rpcServer.stop();
    this.rpcServerOpen = false;
    // Clean up and close up shop
    if (this.logCleaner!= null) this.logCleaner.interrupt();
    if (this.hfileCleaner != null) this.hfileCleaner.interrupt();

    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    if (this.executorService != null) this.executorService.shutdown();
    if (this.healthCheckChore != null) {
      this.healthCheckChore.interrupt();
    }
  }

  private static Thread getAndStartClusterStatusChore(HMaster master) {
    if (master == null || master.balancer == null) {
      return null;
    }
    Chore chore = new ClusterStatusChore(master, master.balancer);
    return Threads.setDaemonThreadRunning(chore.getThread());
  }

  private static Thread getAndStartBalancerChore(final HMaster master) {
    // Start up the load balancer chore
    Chore chore = new BalancerChore(master);
    return Threads.setDaemonThreadRunning(chore.getThread());
  }

  private void stopChores() {
    if (this.balancerChore != null) {
      this.balancerChore.interrupt();
    }
    if (this.clusterStatusChore != null) {
      this.clusterStatusChore.interrupt();
    }
    if (this.catalogJanitorChore != null) {
      this.catalogJanitorChore.interrupt();
    }
  }

  @Override
  public RegionServerStartupResponse regionServerStartup(
      RpcController controller, RegionServerStartupRequest request) throws ServiceException {
    // Register with server manager
    try {
      InetAddress ia = getRemoteInetAddress(request.getPort(), request.getServerStartCode());
      ServerName rs = this.serverManager.regionServerStartup(ia, request.getPort(),
        request.getServerStartCode(), request.getServerCurrentTime());

      // Send back some config info
      RegionServerStartupResponse.Builder resp = createConfigurationSubset();
      NameStringPair.Builder entry = NameStringPair.newBuilder()
        .setName(HConstants.KEY_FOR_HOSTNAME_SEEN_BY_MASTER)
        .setValue(rs.getHostname());
      resp.addMapEntries(entry.build());

      return resp.build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  /**
   * @return Get remote side's InetAddress
   * @throws UnknownHostException
   */
  InetAddress getRemoteInetAddress(final int port, final long serverStartCode)
  throws UnknownHostException {
    // Do it out here in its own little method so can fake an address when
    // mocking up in tests.
    return HBaseServer.getRemoteIp();
  }

  /**
   * @return Subset of configuration to pass initializing regionservers: e.g.
   * the filesystem to use and root directory to use.
   */
  protected RegionServerStartupResponse.Builder createConfigurationSubset() {
    RegionServerStartupResponse.Builder resp = addConfig(
      RegionServerStartupResponse.newBuilder(), HConstants.HBASE_DIR);
    return addConfig(resp, "fs.default.name");
  }

  private RegionServerStartupResponse.Builder addConfig(
      final RegionServerStartupResponse.Builder resp, final String key) {
    NameStringPair.Builder entry = NameStringPair.newBuilder()
      .setName(key)
      .setValue(this.conf.get(key));
    resp.addMapEntries(entry.build());
    return resp;
  }

  @Override
  public GetLastFlushedSequenceIdResponse getLastFlushedSequenceId(RpcController controller,
      GetLastFlushedSequenceIdRequest request) throws ServiceException {
    byte[] regionName = request.getRegionName().toByteArray();
    long seqId = serverManager.getLastFlushedSequenceId(regionName);
    return ResponseConverter.buildGetLastFlushedSequenceIdResponse(seqId);
  }

  @Override
  public RegionServerReportResponse regionServerReport(
      RpcController controller, RegionServerReportRequest request) throws ServiceException {
    try {
      HBaseProtos.ServerLoad sl = request.getLoad();
      this.serverManager.regionServerReport(ProtobufUtil.toServerName(request.getServer()), new ServerLoad(sl));
      if (sl != null && this.metricsMaster != null) {
        // Up our metrics.
        this.metricsMaster.incrementRequests(sl.getTotalNumberOfRequests());
      }
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }

    return RegionServerReportResponse.newBuilder().build();
  }

  @Override
  public ReportRSFatalErrorResponse reportRSFatalError(
      RpcController controller, ReportRSFatalErrorRequest request) throws ServiceException {
    String errorText = request.getErrorMessage();
    ServerName sn = ProtobufUtil.toServerName(request.getServer());
    String msg = "Region server " + Bytes.toString(sn.getVersionedBytes()) +
      " reported a fatal error:\n" + errorText;
    LOG.error(msg);
    rsFatals.add(msg);

    return ReportRSFatalErrorResponse.newBuilder().build();
  }

  public boolean isMasterRunning() {
    return !isStopped();
  }

  public IsMasterRunningResponse isMasterRunning(RpcController c, IsMasterRunningRequest req)
  throws ServiceException {
    return IsMasterRunningResponse.newBuilder().setIsMasterRunning(isMasterRunning()).build();
  }

  @Override
  public CatalogScanResponse runCatalogScan(RpcController c,
      CatalogScanRequest req) throws ServiceException {
    try {
      return ResponseConverter.buildCatalogScanResponse(catalogJanitorChore.scan());
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public EnableCatalogJanitorResponse enableCatalogJanitor(RpcController c,
      EnableCatalogJanitorRequest req) throws ServiceException {
    return EnableCatalogJanitorResponse.newBuilder().
        setPrevValue(catalogJanitorChore.setEnabled(req.getEnable())).build();
  }

  @Override
  public IsCatalogJanitorEnabledResponse isCatalogJanitorEnabled(RpcController c,
      IsCatalogJanitorEnabledRequest req) throws ServiceException {
    boolean isEnabled = catalogJanitorChore != null ? catalogJanitorChore.getEnabled() : false;
    return IsCatalogJanitorEnabledResponse.newBuilder().setValue(isEnabled).build();
  }

  /**
   * @return Maximum time we should run balancer for
   */
  private int getBalancerCutoffTime() {
    int balancerCutoffTime =
      getConfiguration().getInt("hbase.balancer.max.balancing", -1);
    if (balancerCutoffTime == -1) {
      // No time period set so create one -- do half of balancer period.
      int balancerPeriod =
        getConfiguration().getInt("hbase.balancer.period", 300000);
      balancerCutoffTime = balancerPeriod / 2;
      // If nonsense period, set it to balancerPeriod
      if (balancerCutoffTime <= 0) balancerCutoffTime = balancerPeriod;
    }
    return balancerCutoffTime;
  }

  public boolean balance() throws IOException {
    // if master not initialized, don't run balancer.
    if (!this.initialized) {
      LOG.debug("Master has not been initialized, don't run balancer.");
      return false;
    }
    // If balance not true, don't run balancer.
    if (!this.loadBalancerTracker.isBalancerOn()) return false;
    // Do this call outside of synchronized block.
    int maximumBalanceTime = getBalancerCutoffTime();
    long cutoffTime = System.currentTimeMillis() + maximumBalanceTime;
    boolean balancerRan;
    synchronized (this.balancer) {
      // Only allow one balance run at at time.
      if (this.assignmentManager.getRegionStates().isRegionsInTransition()) {
        Map<String, RegionState> regionsInTransition =
          this.assignmentManager.getRegionStates().getRegionsInTransition();
        LOG.debug("Not running balancer because " + regionsInTransition.size() +
          " region(s) in transition: " + org.apache.commons.lang.StringUtils.
            abbreviate(regionsInTransition.toString(), 256));
        return false;
      }
      if (this.serverManager.areDeadServersInProgress()) {
        LOG.debug("Not running balancer because processing dead regionserver(s): " +
          this.serverManager.getDeadServers());
        return false;
      }

      if (this.cpHost != null) {
        if (this.cpHost.preBalance()) {
          LOG.debug("Coprocessor bypassing balancer request");
          return false;
        }
      }

      Map<String, Map<ServerName, List<HRegionInfo>>> assignmentsByTable =
        this.assignmentManager.getRegionStates().getAssignmentsByTable();

      List<RegionPlan> plans = new ArrayList<RegionPlan>();
      //Give the balancer the current cluster state.
      this.balancer.setClusterStatus(getClusterStatus());
      for (Map<ServerName, List<HRegionInfo>> assignments : assignmentsByTable.values()) {
        List<RegionPlan> partialPlans = this.balancer.balanceCluster(assignments);
        if (partialPlans != null) plans.addAll(partialPlans);
      }
      int rpCount = 0;  // number of RegionPlans balanced so far
      long totalRegPlanExecTime = 0;
      balancerRan = plans != null;
      if (plans != null && !plans.isEmpty()) {
        for (RegionPlan plan: plans) {
          LOG.info("balance " + plan);
          long balStartTime = System.currentTimeMillis();
          this.assignmentManager.balance(plan);
          totalRegPlanExecTime += System.currentTimeMillis()-balStartTime;
          rpCount++;
          if (rpCount < plans.size() &&
              // if performing next balance exceeds cutoff time, exit the loop
              (System.currentTimeMillis() + (totalRegPlanExecTime / rpCount)) > cutoffTime) {
            LOG.debug("No more balancing till next balance run; maximumBalanceTime=" +
              maximumBalanceTime);
            break;
          }
        }
      }
      if (this.cpHost != null) {
        this.cpHost.postBalance(rpCount < plans.size() ? plans.subList(0, rpCount) : plans);
      }
    }
    return balancerRan;
  }

  @Override
  public BalanceResponse balance(RpcController c, BalanceRequest request) throws ServiceException {
    try {
      return BalanceResponse.newBuilder().setBalancerRan(balance()).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  enum BalanceSwitchMode {
    SYNC,
    ASYNC
  }
  /**
   * Assigns balancer switch according to BalanceSwitchMode
   * @param b new balancer switch
   * @param mode BalanceSwitchMode
   * @return old balancer switch
   */
  public boolean switchBalancer(final boolean b, BalanceSwitchMode mode) throws IOException {
    boolean oldValue = this.loadBalancerTracker.isBalancerOn();
    boolean newValue = b;
    try {
      if (this.cpHost != null) {
        newValue = this.cpHost.preBalanceSwitch(newValue);
      }
      try {
        if (mode == BalanceSwitchMode.SYNC) {
          synchronized (this.balancer) {
            this.loadBalancerTracker.setBalancerOn(newValue);
          }
        } else {
          this.loadBalancerTracker.setBalancerOn(newValue);
        }
      } catch (KeeperException ke) {
        throw new IOException(ke);
      }
      LOG.info("BalanceSwitch=" + newValue);
      if (this.cpHost != null) {
        this.cpHost.postBalanceSwitch(oldValue, newValue);
      }
    } catch (IOException ioe) {
      LOG.warn("Error flipping balance switch", ioe);
    }
    return oldValue;
  }

  public boolean synchronousBalanceSwitch(final boolean b) throws IOException {
    return switchBalancer(b, BalanceSwitchMode.SYNC);
  }

  public boolean balanceSwitch(final boolean b) throws IOException {
    return switchBalancer(b, BalanceSwitchMode.ASYNC);
  }

  @Override
  public SetBalancerRunningResponse setBalancerRunning(
      RpcController controller, SetBalancerRunningRequest req) throws ServiceException {
    try {
      boolean prevValue = (req.getSynchronous())?
        synchronousBalanceSwitch(req.getOn()):balanceSwitch(req.getOn());
      return SetBalancerRunningResponse.newBuilder().setPrevBalanceValue(prevValue).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  /**
   * Switch for the background CatalogJanitor thread.
   * Used for testing.  The thread will continue to run.  It will just be a noop
   * if disabled.
   * @param b If false, the catalog janitor won't do anything.
   */
  public void setCatalogJanitorEnabled(final boolean b) {
    ((CatalogJanitor)this.catalogJanitorChore).setEnabled(b);
  }

  @Override
  public MoveRegionResponse moveRegion(RpcController controller, MoveRegionRequest req)
  throws ServiceException {
    final byte [] encodedRegionName = req.getRegion().getValue().toByteArray();
    RegionSpecifierType type = req.getRegion().getType();
    final byte [] destServerName = (req.hasDestServerName())?
      Bytes.toBytes(ProtobufUtil.toServerName(req.getDestServerName()).getServerName()):null;
    MoveRegionResponse mrr = MoveRegionResponse.newBuilder().build();

    if (type != RegionSpecifierType.ENCODED_REGION_NAME) {
      LOG.warn("moveRegion specifier type: expected: " + RegionSpecifierType.ENCODED_REGION_NAME
        + " actual: " + type);
    }
    RegionState regionState = assignmentManager.getRegionStates().
      getRegionState(Bytes.toString(encodedRegionName));
    if (regionState == null) {
      throw new ServiceException(
        new UnknownRegionException(Bytes.toStringBinary(encodedRegionName)));
    }

    HRegionInfo hri = regionState.getRegion();
    ServerName dest;
    if (destServerName == null || destServerName.length == 0) {
      LOG.info("Passed destination servername is null/empty so " +
        "choosing a server at random");
      final List<ServerName> destServers = this.serverManager.createDestinationServersList(
        regionState.getServerName());
      dest = balancer.randomAssignment(hri, destServers);
    } else {
      dest = new ServerName(Bytes.toString(destServerName));
      if (dest.equals(regionState.getServerName())) {
        LOG.debug("Skipping move of region " + hri.getRegionNameAsString()
          + " because region already assigned to the same server " + dest + ".");
        return mrr;
      }
    }

    // Now we can do the move
    RegionPlan rp = new RegionPlan(hri, regionState.getServerName(), dest);

    try {
      if (this.cpHost != null) {
        if (this.cpHost.preMove(hri, rp.getSource(), rp.getDestination())) {
          return mrr;
        }
      }
      LOG.info("Added move plan " + rp + ", running balancer");
      this.assignmentManager.balance(rp);
      if (this.cpHost != null) {
        this.cpHost.postMove(hri, rp.getSource(), rp.getDestination());
      }
    } catch (IOException ioe) {
      UnknownRegionException ure = new UnknownRegionException(
        Bytes.toStringBinary(encodedRegionName));
      ure.initCause(ioe);
      throw new ServiceException(ure);
    }
    return mrr;
  }

  @Override
  public void createTable(HTableDescriptor hTableDescriptor,
    byte [][] splitKeys)
  throws IOException {
    if (!isMasterRunning()) {
      throw new MasterNotRunningException();
    }

    HRegionInfo [] newRegions = getHRegionInfos(hTableDescriptor, splitKeys);
    checkInitialized();
    checkCompression(hTableDescriptor);
    if (cpHost != null) {
      cpHost.preCreateTable(hTableDescriptor, newRegions);
    }

    this.executorService.submit(new CreateTableHandler(this,
      this.fileSystemManager, hTableDescriptor, conf,
      newRegions, catalogTracker, assignmentManager));
    if (cpHost != null) {
      cpHost.postCreateTable(hTableDescriptor, newRegions);
    }

  }

  private void checkCompression(final HTableDescriptor htd)
  throws IOException {
    if (!this.masterCheckCompression) return;
    for (HColumnDescriptor hcd : htd.getColumnFamilies()) {
      checkCompression(hcd);
    }
  }

  private void checkCompression(final HColumnDescriptor hcd)
  throws IOException {
    if (!this.masterCheckCompression) return;
    CompressionTest.testCompression(hcd.getCompression());
    CompressionTest.testCompression(hcd.getCompactionCompression());
  }

  @Override
  public CreateTableResponse createTable(RpcController controller, CreateTableRequest req)
  throws ServiceException {
    HTableDescriptor hTableDescriptor = HTableDescriptor.convert(req.getTableSchema());
    byte [][] splitKeys = ProtobufUtil.getSplitKeysArray(req);
    try {
      createTable(hTableDescriptor,splitKeys);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return CreateTableResponse.newBuilder().build();
  }

  private HRegionInfo[] getHRegionInfos(HTableDescriptor hTableDescriptor,
    byte[][] splitKeys) {
    HRegionInfo[] hRegionInfos = null;
    if (splitKeys == null || splitKeys.length == 0) {
      hRegionInfos = new HRegionInfo[]{
          new HRegionInfo(hTableDescriptor.getName(), null, null)};
    } else {
      int numRegions = splitKeys.length + 1;
      hRegionInfos = new HRegionInfo[numRegions];
      byte[] startKey = null;
      byte[] endKey = null;
      for (int i = 0; i < numRegions; i++) {
        endKey = (i == splitKeys.length) ? null : splitKeys[i];
        hRegionInfos[i] =
            new HRegionInfo(hTableDescriptor.getName(), startKey, endKey);
        startKey = endKey;
      }
    }
    return hRegionInfos;
  }

  private static boolean isCatalogTable(final byte [] tableName) {
    return Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME) ||
           Bytes.equals(tableName, HConstants.META_TABLE_NAME);
  }

  @Override
  public void deleteTable(final byte[] tableName) throws IOException {
    checkInitialized();
    if (cpHost != null) {
      cpHost.preDeleteTable(tableName);
    }
    this.executorService.submit(new DeleteTableHandler(tableName, this, this));
    if (cpHost != null) {
      cpHost.postDeleteTable(tableName);
    }
  }

  @Override
  public DeleteTableResponse deleteTable(RpcController controller, DeleteTableRequest request)
  throws ServiceException {
    try {
      deleteTable(request.getTableName().toByteArray());
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return DeleteTableResponse.newBuilder().build();
  }

  /**
   * Get the number of regions of the table that have been updated by the alter.
   *
   * @return Pair indicating the number of regions updated Pair.getFirst is the
   *         regions that are yet to be updated Pair.getSecond is the total number
   *         of regions of the table
   * @throws IOException
   */
  @Override
  public GetSchemaAlterStatusResponse getSchemaAlterStatus(
      RpcController controller, GetSchemaAlterStatusRequest req) throws ServiceException {
    // TODO: currently, we query using the table name on the client side. this
    // may overlap with other table operations or the table operation may
    // have completed before querying this API. We need to refactor to a
    // transaction system in the future to avoid these ambiguities.
    byte [] tableName = req.getTableName().toByteArray();

    try {
      Pair<Integer,Integer> pair = this.assignmentManager.getReopenStatus(tableName);
      GetSchemaAlterStatusResponse.Builder ret = GetSchemaAlterStatusResponse.newBuilder();
      ret.setYetToUpdateRegions(pair.getFirst());
      ret.setTotalRegions(pair.getSecond());
      return ret.build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public void addColumn(final byte[] tableName, final HColumnDescriptor column)
      throws IOException {
    checkInitialized();
    if (cpHost != null) {
      if (cpHost.preAddColumn(tableName, column)) {
        return;
      }
    }
    new TableAddFamilyHandler(tableName, column, this, this).process();
    if (cpHost != null) {
      cpHost.postAddColumn(tableName, column);
    }
  }

  @Override
  public AddColumnResponse addColumn(RpcController controller, AddColumnRequest req)
  throws ServiceException {
    try {
      addColumn(req.getTableName().toByteArray(),
        HColumnDescriptor.convert(req.getColumnFamilies()));
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return AddColumnResponse.newBuilder().build();
  }

  @Override
  public void modifyColumn(byte[] tableName, HColumnDescriptor descriptor)
      throws IOException {
    checkInitialized();
    checkCompression(descriptor);
    if (cpHost != null) {
      if (cpHost.preModifyColumn(tableName, descriptor)) {
        return;
      }
    }
    new TableModifyFamilyHandler(tableName, descriptor, this, this).process();
    if (cpHost != null) {
      cpHost.postModifyColumn(tableName, descriptor);
    }
  }

  @Override
  public ModifyColumnResponse modifyColumn(RpcController controller, ModifyColumnRequest req)
  throws ServiceException {
    try {
      modifyColumn(req.getTableName().toByteArray(),
        HColumnDescriptor.convert(req.getColumnFamilies()));
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return ModifyColumnResponse.newBuilder().build();
  }

  @Override
  public void deleteColumn(final byte[] tableName, final byte[] columnName)
      throws IOException {
    checkInitialized();
    if (cpHost != null) {
      if (cpHost.preDeleteColumn(tableName, columnName)) {
        return;
      }
    }
    new TableDeleteFamilyHandler(tableName, columnName, this, this).process();
    if (cpHost != null) {
      cpHost.postDeleteColumn(tableName, columnName);
    }
  }

  @Override
  public DeleteColumnResponse deleteColumn(RpcController controller, DeleteColumnRequest req)
  throws ServiceException {
    try {
      deleteColumn(req.getTableName().toByteArray(), req.getColumnName().toByteArray());
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return DeleteColumnResponse.newBuilder().build();
  }

  @Override
  public void enableTable(final byte[] tableName) throws IOException {
    checkInitialized();
    if (cpHost != null) {
      cpHost.preEnableTable(tableName);
    }
    this.executorService.submit(new EnableTableHandler(this, tableName,
      catalogTracker, assignmentManager, false));
    if (cpHost != null) {
      cpHost.postEnableTable(tableName);
   }
  }

  @Override
  public EnableTableResponse enableTable(RpcController controller, EnableTableRequest request)
  throws ServiceException {
    try {
      enableTable(request.getTableName().toByteArray());
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return EnableTableResponse.newBuilder().build();
  }

  @Override
  public void disableTable(final byte[] tableName) throws IOException {
    checkInitialized();
    if (cpHost != null) {
      cpHost.preDisableTable(tableName);
    }
    this.executorService.submit(new DisableTableHandler(this, tableName,
      catalogTracker, assignmentManager, false));
    if (cpHost != null) {
      cpHost.postDisableTable(tableName);
    }
  }

  @Override
  public DisableTableResponse disableTable(RpcController controller, DisableTableRequest request)
  throws ServiceException {
    try {
      disableTable(request.getTableName().toByteArray());
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return DisableTableResponse.newBuilder().build();
  }

  /**
   * Return the region and current deployment for the region containing
   * the given row. If the region cannot be found, returns null. If it
   * is found, but not currently deployed, the second element of the pair
   * may be null.
   */
  Pair<HRegionInfo, ServerName> getTableRegionForRow(
      final byte [] tableName, final byte [] rowKey)
  throws IOException {
    final AtomicReference<Pair<HRegionInfo, ServerName>> result =
      new AtomicReference<Pair<HRegionInfo, ServerName>>(null);

    MetaScannerVisitor visitor =
      new MetaScannerVisitorBase() {
        @Override
        public boolean processRow(Result data) throws IOException {
          if (data == null || data.size() <= 0) {
            return true;
          }
          Pair<HRegionInfo, ServerName> pair = HRegionInfo.getHRegionInfoAndServerName(data);
          if (pair == null) {
            return false;
          }
          if (!Bytes.equals(pair.getFirst().getTableName(), tableName)) {
            return false;
          }
          result.set(pair);
          return true;
        }
    };

    MetaScanner.metaScan(conf, visitor, tableName, rowKey, 1);
    return result.get();
  }

  @Override
  public void modifyTable(final byte[] tableName, final HTableDescriptor descriptor)
      throws IOException {
    checkInitialized();
    checkCompression(descriptor);
    if (cpHost != null) {
      cpHost.preModifyTable(tableName, descriptor);
    }
    new ModifyTableHandler(tableName, descriptor, this, this).process();

    if (cpHost != null) {
      cpHost.postModifyTable(tableName, descriptor);
    }
  }

  @Override
  public ModifyTableResponse modifyTable(RpcController controller, ModifyTableRequest req)
  throws ServiceException {
    try {
      modifyTable(req.getTableName().toByteArray(),
        HTableDescriptor.convert(req.getTableSchema()));
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return ModifyTableResponse.newBuilder().build();
  }

  @Override
  public void checkTableModifiable(final byte [] tableName)
      throws IOException, TableNotFoundException, TableNotDisabledException {
    String tableNameStr = Bytes.toString(tableName);
    if (isCatalogTable(tableName)) {
      throw new IOException("Can't modify catalog tables");
    }
    if (!MetaReader.tableExists(getCatalogTracker(), tableNameStr)) {
      throw new TableNotFoundException(tableNameStr);
    }
    if (!getAssignmentManager().getZKTable().
        isDisabledTable(Bytes.toString(tableName))) {
      throw new TableNotDisabledException(tableName);
    }
  }

  @Override
  public GetClusterStatusResponse getClusterStatus(RpcController controller,
      GetClusterStatusRequest req)
  throws ServiceException {
    GetClusterStatusResponse.Builder response = GetClusterStatusResponse.newBuilder();
    response.setClusterStatus(getClusterStatus().convert());
    return response.build();
  }

  /**
   * @return cluster status
   */
  public ClusterStatus getClusterStatus() {
    // Build Set of backup masters from ZK nodes
    List<String> backupMasterStrings;
    try {
      backupMasterStrings = ZKUtil.listChildrenNoWatch(this.zooKeeper,
        this.zooKeeper.backupMasterAddressesZNode);
    } catch (KeeperException e) {
      LOG.warn(this.zooKeeper.prefix("Unable to list backup servers"), e);
      backupMasterStrings = new ArrayList<String>(0);
    }
    List<ServerName> backupMasters = new ArrayList<ServerName>(
                                          backupMasterStrings.size());
    for (String s: backupMasterStrings) {
      try {
        byte [] bytes =
            ZKUtil.getData(this.zooKeeper, ZKUtil.joinZNode(
                this.zooKeeper.backupMasterAddressesZNode, s));
        if (bytes != null) {
          ServerName sn;
          try {
            sn = ServerName.parseFrom(bytes);
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
      public int compare(ServerName s1, ServerName s2) {
        return s1.getServerName().compareTo(s2.getServerName());
      }});

    return new ClusterStatus(VersionInfo.getVersion(),
      this.fileSystemManager.getClusterId().toString(),
      this.serverManager.getOnlineServers(),
      this.serverManager.getDeadServers(),
      this.serverName,
      backupMasters,
      this.assignmentManager.getRegionStates().getRegionsInTransition(),
      this.getCoprocessors(), this.loadBalancerTracker.isBalancerOn());
  }

  public String getClusterId() {
    if (fileSystemManager == null) {
      return "";
    }
    ClusterId id = fileSystemManager.getClusterId();
    if (id == null) {
      return "";
    }
    return id.toString();
  }

  /**
   * The set of loaded coprocessors is stored in a static set. Since it's
   * statically allocated, it does not require that HMaster's cpHost be
   * initialized prior to accessing it.
   * @return a String representation of the set of names of the loaded
   * coprocessors.
   */
  public static String getLoadedCoprocessors() {
    return CoprocessorHost.getLoadedCoprocessors().toString();
  }

  /**
   * @return timestamp in millis when HMaster was started.
   */
  public long getMasterStartTime() {
    return masterStartTime;
  }

  /**
   * @return timestamp in millis when HMaster became the active master.
   */
  public long getMasterActiveTime() {
    return masterActiveTime;
  }

  /**
   * @return array of coprocessor SimpleNames.
   */
  public String[] getCoprocessors() {
    Set<String> masterCoprocessors =
        getCoprocessorHost().getCoprocessors();
    return masterCoprocessors.toArray(new String[0]);
  }

  @Override
  public void abort(final String msg, final Throwable t) {
    if (cpHost != null) {
      // HBASE-4014: dump a list of loaded coprocessors.
      LOG.fatal("Master server abort: loaded coprocessors are: " +
          getLoadedCoprocessors());
    }

    if (abortNow(msg, t)) {
      if (t != null) LOG.fatal(msg, t);
      else LOG.fatal(msg);
      this.abort = true;
      stop("Aborting");
    }
  }

  /**
   * We do the following in a different thread.  If it is not completed
   * in time, we will time it out and assume it is not easy to recover.
   *
   * 1. Create a new ZK session. (since our current one is expired)
   * 2. Try to become a primary master again
   * 3. Initialize all ZK based system trackers.
   * 4. Assign root and meta. (they are already assigned, but we need to update our
   * internal memory state to reflect it)
   * 5. Process any RIT if any during the process of our recovery.
   *
   * @return True if we could successfully recover from ZK session expiry.
   * @throws InterruptedException
   * @throws IOException
   * @throws KeeperException
   * @throws ExecutionException
   */
  private boolean tryRecoveringExpiredZKSession() throws InterruptedException,
      IOException, KeeperException, ExecutionException {

    this.zooKeeper.reconnectAfterExpiration();

    Callable<Boolean> callable = new Callable<Boolean> () {
      public Boolean call() throws InterruptedException,
          IOException, KeeperException {
        MonitoredTask status =
          TaskMonitor.get().createStatus("Recovering expired ZK session");
        try {
          if (!becomeActiveMaster(status)) {
            return Boolean.FALSE;
          }
          serverShutdownHandlerEnabled = false;
          initialized = false;
          finishInitialization(status, true);
          return Boolean.TRUE;
        } finally {
          status.cleanup();
        }
      }
    };

    long timeout =
      conf.getLong("hbase.master.zksession.recover.timeout", 300000);
    java.util.concurrent.ExecutorService executor =
      Executors.newSingleThreadExecutor();
    Future<Boolean> result = executor.submit(callable);
    executor.shutdown();
    if (executor.awaitTermination(timeout, TimeUnit.MILLISECONDS)
        && result.isDone()) {
      Boolean recovered = result.get();
      if (recovered != null) {
        return recovered.booleanValue();
      }
    }
    executor.shutdownNow();
    return false;
  }

  /**
   * Check to see if the current trigger for abort is due to ZooKeeper session
   * expiry, and If yes, whether we can recover from ZK session expiry.
   *
   * @param msg Original abort message
   * @param t   The cause for current abort request
   * @return true if we should proceed with abort operation, false other wise.
   */
  private boolean abortNow(final String msg, final Throwable t) {
    if (!this.isActiveMaster) {
      return true;
    }
    if (t != null && t instanceof KeeperException.SessionExpiredException) {
      try {
        LOG.info("Primary Master trying to recover from ZooKeeper session " +
            "expiry.");
        return !tryRecoveringExpiredZKSession();
      } catch (Throwable newT) {
        LOG.error("Primary master encountered unexpected exception while " +
            "trying to recover from ZooKeeper session" +
            " expiry. Proceeding with server abort.", newT);
      }
    }
    return true;
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return zooKeeper;
  }

  @Override
  public MasterCoprocessorHost getCoprocessorHost() {
    return cpHost;
  }

  @Override
  public ServerName getServerName() {
    return this.serverName;
  }

  @Override
  public CatalogTracker getCatalogTracker() {
    return catalogTracker;
  }

  @Override
  public AssignmentManager getAssignmentManager() {
    return this.assignmentManager;
  }

  public MemoryBoundedLogMessageBuffer getRegionServerFatalLogBuffer() {
    return rsFatals;
  }

  public void shutdown() throws IOException {
    if (spanReceiverHost != null) { 
      spanReceiverHost.closeReceivers();
    }
    if (cpHost != null) {
      cpHost.preShutdown();
    }
    if (mxBean != null) {
      MBeanUtil.unregisterMBean(mxBean);
      mxBean = null;
    }
    if (this.assignmentManager != null) this.assignmentManager.shutdown();
    if (this.serverManager != null) this.serverManager.shutdownCluster();
    try {
      if (this.clusterStatusTracker != null){
        this.clusterStatusTracker.setClusterDown();
      }
    } catch (KeeperException e) {
      LOG.error("ZooKeeper exception trying to set cluster as down in ZK", e);
    }
  }

  @Override
  public ShutdownResponse shutdown(RpcController controller, ShutdownRequest request)
  throws ServiceException {
    try {
      shutdown();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return ShutdownResponse.newBuilder().build();
  }

  public void stopMaster() throws IOException {
    if (cpHost != null) {
      cpHost.preStopMaster();
    }
    stop("Stopped by " + Thread.currentThread().getName());
  }

  @Override
  public StopMasterResponse stopMaster(RpcController controller, StopMasterRequest request)
  throws ServiceException {
    try {
      stopMaster();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return StopMasterResponse.newBuilder().build();
  }

  @Override
  public void stop(final String why) {
    LOG.info(why);
    this.stopped = true;
    // We wake up the stopSleeper to stop immediately
    stopSleeper.skipSleepCycle();
    // If we are a backup master, we need to interrupt wait
    if (this.activeMasterManager != null) {
      synchronized (this.activeMasterManager.clusterHasActiveMaster) {
        this.activeMasterManager.clusterHasActiveMaster.notifyAll();
      }
    }
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  public boolean isAborted() {
    return this.abort;
  }

  void checkInitialized() throws PleaseHoldException {
    if (!this.initialized) {
      throw new PleaseHoldException("Master is initializing");
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
  public boolean isActiveMaster() {
    return isActiveMaster;
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
  public boolean isInitialized() {
    return initialized;
  }

  /**
   * ServerShutdownHandlerEnabled is set false before completing
   * assignRootAndMeta to prevent processing of ServerShutdownHandler.
   * @return true if assignRootAndMeta has completed;
   */
  public boolean isServerShutdownHandlerEnabled() {
    return this.serverShutdownHandlerEnabled;
  }

  @Override
  public AssignRegionResponse assignRegion(RpcController controller, AssignRegionRequest req)
  throws ServiceException {
    try {
      final byte [] regionName = req.getRegion().getValue().toByteArray();
      RegionSpecifierType type = req.getRegion().getType();
      AssignRegionResponse arr = AssignRegionResponse.newBuilder().build();

      checkInitialized();
      if (type != RegionSpecifierType.REGION_NAME) {
        LOG.warn("assignRegion specifier type: expected: " + RegionSpecifierType.REGION_NAME
          + " actual: " + type);
      }
      HRegionInfo regionInfo = assignmentManager.getRegionStates().getRegionInfo(regionName);
      if (regionInfo == null) throw new UnknownRegionException(Bytes.toString(regionName));
      if (cpHost != null) {
        if (cpHost.preAssign(regionInfo)) {
          return arr;
        }
      }
      assignmentManager.assign(regionInfo, true, true);
      if (cpHost != null) {
        cpHost.postAssign(regionInfo);
      }

      return arr;
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  public void assignRegion(HRegionInfo hri) {
    assignmentManager.assign(hri, true);
  }

  @Override
  public UnassignRegionResponse unassignRegion(RpcController controller, UnassignRegionRequest req)
  throws ServiceException {
    try {
      final byte [] regionName = req.getRegion().getValue().toByteArray();
      RegionSpecifierType type = req.getRegion().getType();
      final boolean force = req.getForce();
      UnassignRegionResponse urr = UnassignRegionResponse.newBuilder().build();

      checkInitialized();
      if (type != RegionSpecifierType.REGION_NAME) {
        LOG.warn("unassignRegion specifier type: expected: " + RegionSpecifierType.REGION_NAME
          + " actual: " + type);
      }
      Pair<HRegionInfo, ServerName> pair =
        MetaReader.getRegion(this.catalogTracker, regionName);
      if (pair == null) throw new UnknownRegionException(Bytes.toString(regionName));
      HRegionInfo hri = pair.getFirst();
      if (cpHost != null) {
        if (cpHost.preUnassign(hri, force)) {
          return urr;
        }
      }
      if (force) {
        this.assignmentManager.regionOffline(hri);
        assignRegion(hri);
      } else {
        this.assignmentManager.unassign(hri, force);
      }
      if (cpHost != null) {
        cpHost.postUnassign(hri, force);
      }

      return urr;
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  /**
   * Get list of TableDescriptors for requested tables.
   * @param controller Unused (set to null).
   * @param req GetTableDescriptorsRequest that contains:
   * - tableNames: requested tables, or if empty, all are requested
   * @return GetTableDescriptorsResponse
   * @throws ServiceException
   */
  public GetTableDescriptorsResponse getTableDescriptors(
	      RpcController controller, GetTableDescriptorsRequest req) throws ServiceException {
    GetTableDescriptorsResponse.Builder builder = GetTableDescriptorsResponse.newBuilder();
    if (req.getTableNamesCount() == 0) {
      // request for all TableDescriptors
      Map<String, HTableDescriptor> descriptors = null;
      try {
        descriptors = this.tableDescriptors.getAll();
      } catch (IOException e) {
          LOG.warn("Failed getting all descriptors", e);
      }
      if (descriptors != null) {
        for (HTableDescriptor htd : descriptors.values()) {
          builder.addTableSchema(htd.convert());
        }
      }
    }
    else {
      for (String s: req.getTableNamesList()) {
        HTableDescriptor htd = null;
        try {
          htd = this.tableDescriptors.get(s);
        } catch (IOException e) {
          LOG.warn("Failed getting descriptor for " + s, e);
        }
        if (htd == null) continue;
        builder.addTableSchema(htd.convert());
      }
    }
    return builder.build();
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

  /**
   * Special method, only used by hbck.
   */
  @Override
  public OfflineRegionResponse offlineRegion(RpcController controller, OfflineRegionRequest request)
  throws ServiceException {
    final byte [] regionName = request.getRegion().getValue().toByteArray();
    RegionSpecifierType type = request.getRegion().getType();
    if (type != RegionSpecifierType.REGION_NAME) {
      LOG.warn("moveRegion specifier type: expected: " + RegionSpecifierType.REGION_NAME
        + " actual: " + type);
    }

    try {
      Pair<HRegionInfo, ServerName> pair =
        MetaReader.getRegion(this.catalogTracker, regionName);
      if (pair == null) throw new UnknownRegionException(Bytes.toStringBinary(regionName));
      HRegionInfo hri = pair.getFirst();
      this.assignmentManager.regionOffline(hri);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return OfflineRegionResponse.newBuilder().build();
  }

  @Override
  public boolean registerService(Service instance) {
    /*
     * No stacking of instances is allowed for a single service name
     */
    Descriptors.ServiceDescriptor serviceDesc = instance.getDescriptorForType();
    if (coprocessorServiceHandlers.containsKey(serviceDesc.getFullName())) {
      LOG.error("Coprocessor service "+serviceDesc.getFullName()+
          " already registered, rejecting request from "+instance
      );
      return false;
    }

    coprocessorServiceHandlers.put(serviceDesc.getFullName(), instance);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Registered master coprocessor service: service="+serviceDesc.getFullName());
    }
    return true;
  }

  @Override
  public ClientProtos.CoprocessorServiceResponse execMasterService(final RpcController controller,
      final ClientProtos.CoprocessorServiceRequest request) throws ServiceException {
    try {
      ServerRpcController execController = new ServerRpcController();

      ClientProtos.CoprocessorServiceCall call = request.getCall();
      String serviceName = call.getServiceName();
      String methodName = call.getMethodName();
      if (!coprocessorServiceHandlers.containsKey(serviceName)) {
        throw new UnknownProtocolException(null,
            "No registered master coprocessor service found for name "+serviceName);
      }

      Service service = coprocessorServiceHandlers.get(serviceName);
      Descriptors.ServiceDescriptor serviceDesc = service.getDescriptorForType();
      Descriptors.MethodDescriptor methodDesc = serviceDesc.findMethodByName(methodName);
      if (methodDesc == null) {
        throw new UnknownProtocolException(service.getClass(),
            "Unknown method "+methodName+" called on master service "+serviceName);
      }

      //invoke the method
      Message execRequest = service.getRequestPrototype(methodDesc).newBuilderForType()
          .mergeFrom(call.getRequest()).build();
      final Message.Builder responseBuilder =
          service.getResponsePrototype(methodDesc).newBuilderForType();
      service.callMethod(methodDesc, execController, execRequest, new RpcCallback<Message>() {
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
      builder.setRegion(RequestConverter.buildRegionSpecifier(
          RegionSpecifierType.REGION_NAME, HConstants.EMPTY_BYTE_ARRAY));
      builder.setValue(
          builder.getValueBuilder().setName(execResult.getClass().getName())
              .setValue(execResult.toByteString()));
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Utility for constructing an instance of the passed HMaster class.
   * @param masterClass
   * @param conf
   * @return HMaster instance.
   */
  public static HMaster constructMaster(Class<? extends HMaster> masterClass,
      final Configuration conf)  {
    try {
      Constructor<? extends HMaster> c =
        masterClass.getConstructor(Configuration.class);
      return c.newInstance(conf);
    } catch (InvocationTargetException ite) {
      Throwable target = ite.getTargetException() != null?
        ite.getTargetException(): ite;
      if (target.getCause() != null) target = target.getCause();
      throw new RuntimeException("Failed construction of Master: " +
        masterClass.toString(), target);
    } catch (Exception e) {
      throw new RuntimeException("Failed construction of Master: " +
        masterClass.toString() + ((e.getCause() != null)?
          e.getCause().getMessage(): ""), e);
    }
  }

  /**
   * @see org.apache.hadoop.hbase.master.HMasterCommandLine
   */
  public static void main(String [] args) {
    VersionInfo.logVersion();
    new HMasterCommandLine(HMaster.class).doMain(args);
  }

  public HFileCleaner getHFileCleaner() {
    return this.hfileCleaner;
  }

  /**
   * Exposed for TESTING!
   * @return the underlying snapshot manager
   */
  public SnapshotManager getSnapshotManagerForTesting() {
    return this.snapshotManager;
  }

  /**
   * Triggers an asynchronous attempt to take a snapshot.
   * {@inheritDoc}
   */
  @Override
  public TakeSnapshotResponse snapshot(RpcController controller, TakeSnapshotRequest request)
      throws ServiceException {
    try {
      this.snapshotManager.checkSnapshotSupport();
    } catch (UnsupportedOperationException e) {
      throw new ServiceException(e);
    }

    LOG.debug("Submitting snapshot request for:" +
        SnapshotDescriptionUtils.toString(request.getSnapshot()));
    // get the snapshot information
    SnapshotDescription snapshot = SnapshotDescriptionUtils.validate(request.getSnapshot(),
      this.conf);
    try {
      snapshotManager.takeSnapshot(snapshot);
    } catch (IOException e) {
      throw new ServiceException(e);
    }

    // send back the max amount of time the client should wait for the snapshot to complete
    long waitTime = SnapshotDescriptionUtils.getMaxMasterTimeout(conf, snapshot.getType(),
      SnapshotDescriptionUtils.DEFAULT_MAX_WAIT_TIME);
    return TakeSnapshotResponse.newBuilder().setExpectedTimeout(waitTime).build();
  }

  /**
   * List the currently available/stored snapshots. Any in-progress snapshots are ignored
   */
  @Override
  public ListSnapshotResponse listSnapshots(RpcController controller, ListSnapshotRequest request)
      throws ServiceException {
    try {
      ListSnapshotResponse.Builder builder = ListSnapshotResponse.newBuilder();
      List<SnapshotDescription> snapshots = snapshotManager.getCompletedSnapshots();

      // convert to protobuf
      for (SnapshotDescription snapshot : snapshots) {
        builder.addSnapshots(snapshot);
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Execute Delete Snapshot operation.
   * @return DeleteSnapshotResponse (a protobuf wrapped void) if the snapshot existed and was
   *    deleted properly.
   * @throws ServiceException wrapping SnapshotDoesNotExistException if specified snapshot did not
   *    exist.
   */
  @Override
  public DeleteSnapshotResponse deleteSnapshot(RpcController controller,
      DeleteSnapshotRequest request) throws ServiceException {
    try {
      this.snapshotManager.checkSnapshotSupport();
    } catch (UnsupportedOperationException e) {
      throw new ServiceException(e);
    }

    try {
      snapshotManager.deleteSnapshot(request.getSnapshot());
      return DeleteSnapshotResponse.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Checks if the specified snapshot is done.
   * @return true if the snapshot is in file system ready to use,
   *   false if the snapshot is in the process of completing
   * @throws ServiceException wrapping UnknownSnapshotException if invalid snapshot, or
   *  a wrapped HBaseSnapshotException with progress failure reason.
   */
  @Override
  public IsSnapshotDoneResponse isSnapshotDone(RpcController controller,
      IsSnapshotDoneRequest request) throws ServiceException {
    LOG.debug("Checking to see if snapshot from request:" +
        SnapshotDescriptionUtils.toString(request.getSnapshot()) + " is done");
    try {
      IsSnapshotDoneResponse.Builder builder = IsSnapshotDoneResponse.newBuilder();
      boolean done = snapshotManager.isSnapshotDone(request.getSnapshot());
      builder.setDone(done);
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Execute Restore/Clone snapshot operation.
   *
   * <p>If the specified table exists a "Restore" is executed, replacing the table
   * schema and directory data with the content of the snapshot.
   * The table must be disabled, or a UnsupportedOperationException will be thrown.
   *
   * <p>If the table doesn't exist a "Clone" is executed, a new table is created
   * using the schema at the time of the snapshot, and the content of the snapshot.
   *
   * <p>The restore/clone operation does not require copying HFiles. Since HFiles
   * are immutable the table can point to and use the same files as the original one.
   */
  @Override
  public RestoreSnapshotResponse restoreSnapshot(RpcController controller,
      RestoreSnapshotRequest request) throws ServiceException {
    try {
      this.snapshotManager.checkSnapshotSupport();
    } catch (UnsupportedOperationException e) {
      throw new ServiceException(e);
    }

    try {
      SnapshotDescription reqSnapshot = request.getSnapshot();
      snapshotManager.restoreSnapshot(reqSnapshot);
      return RestoreSnapshotResponse.newBuilder().build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Returns the status of the requested snapshot restore/clone operation.
   * This method is not exposed to the user, it is just used internally by HBaseAdmin
   * to verify if the restore is completed.
   *
   * No exceptions are thrown if the restore is not running, the result will be "done".
   *
   * @return done <tt>true</tt> if the restore/clone operation is completed.
   * @throws RestoreSnapshotExcepton if the operation failed.
   */
  @Override
  public IsRestoreSnapshotDoneResponse isRestoreSnapshotDone(RpcController controller,
      IsRestoreSnapshotDoneRequest request) throws ServiceException {
    try {
      SnapshotDescription snapshot = request.getSnapshot();
      IsRestoreSnapshotDoneResponse.Builder builder = IsRestoreSnapshotDoneResponse.newBuilder();
      boolean isRestoring = snapshotManager.isRestoringTable(snapshot);
      builder.setDone(!isRestoring);
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  private boolean isHealthCheckerConfigured() {
    String healthScriptLocation = this.conf.get(HConstants.HEALTH_SCRIPT_LOC);
    return org.apache.commons.lang.StringUtils.isNotBlank(healthScriptLocation);
  }
}
