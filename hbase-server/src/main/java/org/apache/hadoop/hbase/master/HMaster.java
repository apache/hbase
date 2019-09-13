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
import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.CoordinatedStateException;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MetaMigrationConvertingToPB;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.RegionStateListener;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitorBase;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.http.InfoServer;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.MasterRpcServices.BalanceSwitchMode;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.balancer.BalancerChore;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.master.balancer.ClusterStatusChore;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.master.cleaner.DirScanPool;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.master.cleaner.LogCleaner;
import org.apache.hadoop.hbase.master.cleaner.ReplicationZKLockCleanerChore;
import org.apache.hadoop.hbase.master.cleaner.ReplicationZKNodeCleaner;
import org.apache.hadoop.hbase.master.cleaner.ReplicationZKNodeCleanerChore;
import org.apache.hadoop.hbase.master.cleaner.SnapshotCleanerChore;
import org.apache.hadoop.hbase.master.handler.DispatchMergingRegionHandler;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizer;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizerChore;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizerFactory;
import org.apache.hadoop.hbase.master.procedure.AddColumnFamilyProcedure;
import org.apache.hadoop.hbase.master.procedure.CreateNamespaceProcedure;
import org.apache.hadoop.hbase.master.procedure.CreateTableProcedure;
import org.apache.hadoop.hbase.master.procedure.DeleteColumnFamilyProcedure;
import org.apache.hadoop.hbase.master.procedure.DeleteNamespaceProcedure;
import org.apache.hadoop.hbase.master.procedure.DeleteTableProcedure;
import org.apache.hadoop.hbase.master.procedure.DisableTableProcedure;
import org.apache.hadoop.hbase.master.procedure.EnableTableProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureScheduler.ProcedureEvent;
import org.apache.hadoop.hbase.master.procedure.ModifyColumnFamilyProcedure;
import org.apache.hadoop.hbase.master.procedure.ModifyNamespaceProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.master.procedure.ModifyTableProcedure;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.master.procedure.ProcedureSyncWait;
import org.apache.hadoop.hbase.master.procedure.TruncateTableProcedure;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.monitoring.MemoryBoundedLogMessageBuffer;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan.PlanType;
import org.apache.hadoop.hbase.procedure.MasterProcedureManagerHost;
import org.apache.hadoop.hbase.procedure.flush.MasterFlushTableProcedureManager;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionServerInfo;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.quotas.MasterQuotaManager;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.ExploringCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.FIFOCompactionPolicy;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;
import org.apache.hadoop.hbase.replication.master.TableCFsUpdater;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.CompressionTest;
import org.apache.hadoop.hbase.util.ConfigUtil;
import org.apache.hadoop.hbase.util.EncryptionTest;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.DrainingServerTracker;
import org.apache.hadoop.hbase.zookeeper.LoadBalancerTracker;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.MasterMaintenanceModeTracker;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.RegionNormalizerTracker;
import org.apache.hadoop.hbase.zookeeper.RegionServerTracker;
import org.apache.hadoop.hbase.zookeeper.SnapshotCleanupTracker;
import org.apache.hadoop.hbase.zookeeper.SplitOrMergeTracker;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Service;

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
public class HMaster extends HRegionServer implements MasterServices, Server {
  private static final Log LOG = LogFactory.getLog(HMaster.class.getName());

  /**
   * Protection against zombie master. Started once Master accepts active responsibility and
   * starts taking over responsibilities. Allows a finite time window before giving up ownership.
   */
  private static class InitializationMonitor extends HasThread {
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
  RegionServerTracker regionServerTracker;
  // Draining region server tracker
  private DrainingServerTracker drainingServerTracker;
  // Tracker for load balancer state
  LoadBalancerTracker loadBalancerTracker;

  // Tracker for split and merge state
  private SplitOrMergeTracker splitOrMergeTracker;

  // Tracker for region normalizer state
  private RegionNormalizerTracker regionNormalizerTracker;

  /** Namespace stuff */
  private TableNamespaceManager tableNamespaceManager;

  // Tracker for auto snapshot cleanup state
  SnapshotCleanupTracker snapshotCleanupTracker;

  //Tracker for master maintenance mode setting
  private MasterMaintenanceModeTracker maintenanceModeTracker;

  // Metrics for the HMaster
  final MetricsMaster metricsMaster;
  // file system manager for the master FS operations
  private MasterFileSystem fileSystemManager;

  // server manager to deal with region server info
  volatile ServerManager serverManager;

  // manager of assignment nodes in zookeeper
  AssignmentManager assignmentManager;

  // buffer for "fatal error" notices from region servers
  // in the cluster. This is only used for assisting
  // operations/debugging.
  MemoryBoundedLogMessageBuffer rsFatals;

  // flag set after we become the active master (used for testing)
  private volatile boolean activeMaster = false;

  // flag set after we complete initialization once active,
  // it is not private since it's used in unit tests
  private final ProcedureEvent initialized = new ProcedureEvent("master initialized");

  // flag set after master services are started,
  // initialization may have not completed yet.
  volatile boolean serviceStarted = false;

  // flag set after we complete assignMeta.
  private final ProcedureEvent serverCrashProcessingEnabled =
    new ProcedureEvent("server crash processing");

  // Maximum time we should run balancer for
  private final int maxBlancingTime;
  // Maximum percent of regions in transition when balancing
  private final double maxRitPercent;

  LoadBalancer balancer;
  private RegionNormalizer normalizer;
  private BalancerChore balancerChore;
  private RegionNormalizerChore normalizerChore;
  private ClusterStatusChore clusterStatusChore;
  private ClusterStatusPublisher clusterStatusPublisherChore = null;
  private PeriodicDoMetrics periodicDoMetricsChore = null;
  private SnapshotCleanerChore snapshotCleanerChore = null;

  CatalogJanitor catalogJanitorChore;
  private DirScanPool cleanerPool;
  private ReplicationZKLockCleanerChore replicationZKLockCleanerChore;
  private ReplicationZKNodeCleanerChore replicationZKNodeCleanerChore;
  private LogCleaner logCleaner;
  private HFileCleaner hfileCleaner;

  MasterCoprocessorHost cpHost;

  private final boolean preLoadTableDescriptors;

  // Time stamps for when a hmaster became active
  private long masterActiveTime;

  // Time stamp for when HMaster finishes becoming Active Master
  private long masterFinishedInitializationTime;

  //should we check the compression codec type at master side, default true, HBASE-6370
  private final boolean masterCheckCompression;

  //should we check encryption settings at master side, default true
  private final boolean masterCheckEncryption;

  Map<String, Service> coprocessorServiceHandlers = Maps.newHashMap();

  // monitor for snapshot of hbase tables
  SnapshotManager snapshotManager;
  // monitor for distributed procedures
  private MasterProcedureManagerHost mpmHost;

  // it is assigned after 'initialized' guard set to true, so should be volatile
  private volatile MasterQuotaManager quotaManager;

  private ProcedureExecutor<MasterProcedureEnv> procedureExecutor;
  private WALProcedureStore procedureStore;

  private long splitPlanCount;
  private long mergePlanCount;

  /** flag used in test cases in order to simulate RS failures during master initialization */
  private volatile boolean initializationBeforeMetaAssignment = false;

  /** jetty server for master to redirect requests to regionserver infoServer */
  private org.mortbay.jetty.Server masterJettyServer;

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
              MASTER_HOSTNAME_KEY + "' is not set; client will get a HTTP 400 response. If " +
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

  private static class PeriodicDoMetrics extends ScheduledChore {
    private final HMaster server;
    public PeriodicDoMetrics(int doMetricsInterval, final HMaster server) {
      super(server.getServerName() + "-DoMetricsChore", server, doMetricsInterval);
      this.server = server;
    }

    @Override
    protected void chore() {
      server.doMetrics();
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
   * #finishActiveMasterInitialization(MonitoredTask) after
   * the master becomes the active one.
   *
   * @throws InterruptedException
   * @throws KeeperException
   * @throws IOException
   */
  public HMaster(final Configuration conf, CoordinatedStateManager csm)
      throws IOException, KeeperException, InterruptedException {
    super(conf, csm);
    this.rsFatals = new MemoryBoundedLogMessageBuffer(
      conf.getLong("hbase.master.buffer.for.rs.fatals", 1*1024*1024));

    LOG.info("hbase.rootdir=" + CommonFSUtils.getRootDir(this.conf) +
      ", hbase.cluster.distributed=" + this.conf.getBoolean(HConstants.CLUSTER_DISTRIBUTED, false));

    // Disable usage of meta replicas in the master
    this.conf.setBoolean(HConstants.USE_META_REPLICAS, false);

    Replication.decorateMasterConfiguration(this.conf);

    // Hack! Maps DFSClient => Master for logs.  HDFS made this
    // config param for task trackers, but we can piggyback off of it.
    if (this.conf.get("mapreduce.task.attempt.id") == null) {
      this.conf.set("mapreduce.task.attempt.id", "hb_m_" + this.serverName.toString());
    }

    // should we check the compression codec type at master side, default true, HBASE-6370
    this.masterCheckCompression = conf.getBoolean("hbase.master.check.compression", true);

    // should we check encryption settings at master side, default true
    this.masterCheckEncryption = conf.getBoolean("hbase.master.check.encryption", true);

    this.metricsMaster = new MetricsMaster(new MetricsMasterWrapperImpl(this));

    // preload table descriptor at startup
    this.preLoadTableDescriptors = conf.getBoolean("hbase.master.preload.tabledescriptors", true);

    this.maxBlancingTime = getMaxBalancingTime();
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
        getChoreService().scheduleChore(clusterStatusPublisherChore);
      }
    }

    // Some unit tests don't need a cluster, so no zookeeper at all
    if (!conf.getBoolean("hbase.testing.nocluster", false)) {
      setInitLatch(new CountDownLatch(1));
      activeMasterManager = new ActiveMasterManager(zooKeeper, this.serverName, this);
      int infoPort = putUpJettyServer();
      startActiveMasterManager(infoPort);
    } else {
      activeMasterManager = null;
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
    masterJettyServer = new org.mortbay.jetty.Server();
    Connector connector = new SelectChannelConnector();
    connector.setHost(addr);
    connector.setPort(infoPort);
    masterJettyServer.addConnector(connector);
    masterJettyServer.setStopAtShutdown(true);

    final String redirectHostname = shouldUseThisHostnameInstead() ? useThisHostnameInstead : null;

    final RedirectServlet redirect = new RedirectServlet(infoServer, redirectHostname);
    Context context = new Context(masterJettyServer, "/", Context.NO_SESSIONS);
    context.addServlet(new ServletHolder(redirect), "/*");

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
      user.login("hbase.master.keytab.file",
        "hbase.master.kerberos.principal", host);
    }
  }

  /**
   * If configured to put regions on active master,
   * wait till a backup master becomes active.
   * Otherwise, loop till the server is stopped or aborted.
   */
  @Override
  protected void waitForMasterActive(){
    boolean tablesOnMaster = BaseLoadBalancer.tablesOnMaster(conf);
    while (!(tablesOnMaster && activeMaster)
        && !isStopped() && !isAborted()) {
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
    infoServer.addServlet("master-status", "/master-status", MasterStatusServlet.class);
    infoServer.setAttribute(MASTER, this);
    if (BaseLoadBalancer.tablesOnMaster(conf)) {
      super.configureInfoServer();
    }
  }

  @Override
  protected Class<? extends HttpServlet> getDumpServlet() {
    return MasterDumpServlet.class;
  }

  /**
   * Emit the HMaster metrics, such as region in transition metrics.
   * Surrounding in a try block just to be sure metrics doesn't abort HMaster.
   */
  private void doMetrics() {
    try {
      if (assignmentManager != null) {
        assignmentManager.updateRegionsInTransitionMetrics();
      }
    } catch (Throwable e) {
      LOG.error("Couldn't update metrics: " + e.getMessage());
    }
  }

  MetricsMaster getMasterMetrics() {
    return metricsMaster;
  }

  /**
   * Initialize all ZK based system trackers.
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   * @throws CoordinatedStateException
   */
  void initializeZKBasedSystemTrackers() throws IOException,
      InterruptedException, KeeperException, CoordinatedStateException {
    this.balancer = LoadBalancerFactory.getLoadBalancer(conf);
    this.normalizer = RegionNormalizerFactory.getRegionNormalizer(conf);
    this.normalizer.setMasterServices(this);
    this.normalizer.setMasterRpcServices((MasterRpcServices)rpcServices);
    this.loadBalancerTracker = new LoadBalancerTracker(zooKeeper, this);
    this.loadBalancerTracker.start();

    this.regionNormalizerTracker = new RegionNormalizerTracker(zooKeeper, this);
    this.regionNormalizerTracker.start();

    this.splitOrMergeTracker = new SplitOrMergeTracker(zooKeeper, conf, this);
    this.splitOrMergeTracker.start();

    this.assignmentManager = new AssignmentManager(this, serverManager,
      this.balancer, this.service, this.metricsMaster,
      this.tableLockManager);
    zooKeeper.registerListenerFirst(assignmentManager);

    this.regionServerTracker = new RegionServerTracker(zooKeeper, this,
        this.serverManager);
    this.regionServerTracker.start();

    this.drainingServerTracker = new DrainingServerTracker(zooKeeper, this,
      this.serverManager);
    this.drainingServerTracker.start();

    this.snapshotCleanupTracker = new SnapshotCleanupTracker(zooKeeper, this);
    this.snapshotCleanupTracker.start();

    this.maintenanceModeTracker = new MasterMaintenanceModeTracker(zooKeeper);
    this.maintenanceModeTracker.start();

    // Set the cluster as up.  If new RSs, they'll be waiting on this before
    // going ahead with their startup.
    boolean wasUp = this.clusterStatusTracker.isClusterUp();
    if (!wasUp) this.clusterStatusTracker.setClusterUp();

    LOG.info("Server active/primary master=" + this.serverName +
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

  /**
   * Finish initialization of HMaster after becoming the primary master.
   *
   * <ol>
   * <li>Initialize master components - file system manager, server manager,
   *     assignment manager, region server tracker, etc</li>
   * <li>Start necessary service threads - balancer, catalog janior,
   *     executor services, etc</li>
   * <li>Set cluster as UP in ZooKeeper</li>
   * <li>Wait for RegionServers to check-in</li>
   * <li>Split logs and perform data recovery, if necessary</li>
   * <li>Ensure assignment of meta/namespace regions</li>
   * <li>Handle either fresh cluster start or master failover</li>
   * </ol>
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   * @throws CoordinatedStateException
   */
  private void finishActiveMasterInitialization(MonitoredTask status)
      throws IOException, InterruptedException, KeeperException, CoordinatedStateException {

    activeMaster = true;
    Thread zombieDetector = new Thread(new InitializationMonitor(this),
        "ActiveMasterInitializationMonitor-" + System.currentTimeMillis());
    zombieDetector.start();

    /*
     * We are active master now... go initialize components we need to run.
     * Note, there may be dross in zk from previous runs; it'll get addressed
     * below after we determine if cluster startup or failover.
     */

    status.setStatus("Initializing Master file system");

    this.masterActiveTime = System.currentTimeMillis();
    // TODO: Do this using Dependency Injection, using PicoContainer, Guice or Spring.
    this.fileSystemManager = new MasterFileSystem(this, this);

    // enable table descriptors cache
    this.tableDescriptors.setCacheOn();
    // set the META's descriptor to the correct replication
    this.tableDescriptors.get(TableName.META_TABLE_NAME).setRegionReplication(
        conf.getInt(HConstants.META_REPLICAS_NUM, HConstants.DEFAULT_META_REPLICA_NUM));
    // warm-up HTDs cache on master initialization
    if (preLoadTableDescriptors) {
      status.setStatus("Pre-loading table descriptors");
      this.tableDescriptors.getAll();
    }

    // publish cluster ID
    status.setStatus("Publishing Cluster ID in ZooKeeper");
    ZKClusterId.setClusterId(this.zooKeeper, fileSystemManager.getClusterId());
    this.initLatch.countDown();
    this.serverManager = createServerManager(this, this);

    setupClusterConnection();

    // Invalidate all write locks held previously
    this.tableLockManager.reapWriteLocks();

    status.setStatus("Initializing ZK system trackers");
    initializeZKBasedSystemTrackers();

    // This is for backwards compatibility
    // See HBASE-11393
    status.setStatus("Update TableCFs node in ZNode");
    TableCFsUpdater tableCFsUpdater = new TableCFsUpdater(zooKeeper,
            conf, this.clusterConnection);
    tableCFsUpdater.update();

    // initialize master side coprocessors before we start handling requests
    status.setStatus("Initializing master coprocessors");
    this.cpHost = new MasterCoprocessorHost(this, this.conf);

    // start up all service threads.
    status.setStatus("Initializing master service threads");
    startServiceThreads();

    // Wake up this server to check in
    sleeper.skipSleepCycle();

    // Wait for region servers to report in
    this.serverManager.waitForRegionServers(status);
    // Check zk for region servers that are up but didn't register
    for (ServerName sn: this.regionServerTracker.getOnlineServers()) {
      // The isServerOnline check is opportunistic, correctness is handled inside
      if (!this.serverManager.isServerOnline(sn)
          && serverManager.checkAndRecordNewServer(sn, ServerLoad.EMPTY_SERVERLOAD)) {
        LOG.info("Registered server found up in zk but who has not yet reported in: " + sn);
      }
    }

    // get a list for previously failed RS which need log splitting work
    // we recover hbase:meta region servers inside master initialization and
    // handle other failed servers in SSH in order to start up master node ASAP
    Set<ServerName> previouslyFailedServers =
      this.fileSystemManager.getFailedServersFromLogFolders();

    // log splitting for hbase:meta server
    ServerName oldMetaServerLocation = metaTableLocator.getMetaRegionLocation(this.getZooKeeper());
    if (oldMetaServerLocation != null && previouslyFailedServers.contains(oldMetaServerLocation)) {
      splitMetaLogBeforeAssignment(oldMetaServerLocation);
      // Note: we can't remove oldMetaServerLocation from previousFailedServers list because it
      // may also host user regions
    }
    Set<ServerName> previouslyFailedMetaRSs = getPreviouselyFailedMetaServersFromZK();
    // need to use union of previouslyFailedMetaRSs recorded in ZK and previouslyFailedServers
    // instead of previouslyFailedMetaRSs alone to address the following two situations:
    // 1) the chained failure situation(recovery failed multiple times in a row).
    // 2) master get killed right before it could delete the recovering hbase:meta from ZK while the
    // same server still has non-meta wals to be replayed so that
    // removeStaleRecoveringRegionsFromZK can't delete the stale hbase:meta region
    // Passing more servers into splitMetaLog is all right. If a server doesn't have hbase:meta wal,
    // there is no op for the server.
    previouslyFailedMetaRSs.addAll(previouslyFailedServers);

    this.initializationBeforeMetaAssignment = true;

    // Wait for regionserver to finish initialization.
    if (BaseLoadBalancer.tablesOnMaster(conf)) {
      waitForServerOnline();
    }

    //initialize load balancer
    this.balancer.setMasterServices(this);
    this.balancer.setClusterStatus(getClusterStatusWithoutCoprocessor());
    this.balancer.initialize();

    // Check if master is shutting down because of some issue
    // in initializing the regionserver or the balancer.
    if (isStopped()) return;

    // Make sure meta assigned before proceeding.
    status.setStatus("Assigning Meta Region");
    assignMeta(status, previouslyFailedMetaRSs, HRegionInfo.DEFAULT_REPLICA_ID);
    // check if master is shutting down because above assignMeta could return even hbase:meta isn't
    // assigned when master is shutting down
    if (isStopped()) return;

    status.setStatus("Submitting log splitting work for previously failed region servers");
    // Master has recovered hbase:meta region server and we put
    // other failed region servers in a queue to be handled later by SSH
    for (ServerName tmpServer : previouslyFailedServers) {
      this.serverManager.processDeadServer(tmpServer, true);
    }

    // Update meta with new PB serialization if required. i.e migrate all HRI to PB serialization
    // in meta. This must happen before we assign all user regions or else the assignment will fail.
    if (this.conf.getBoolean("hbase.MetaMigrationConvertingToPB", true)) {
      MetaMigrationConvertingToPB.updateMetaIfNecessary(this);
    }

    // Fix up assignment manager status
    status.setStatus("Starting assignment manager");
    this.assignmentManager.joinCluster();

    // set cluster status again after user regions are assigned
    this.balancer.setClusterStatus(getClusterStatusWithoutCoprocessor());

    // Start balancer and meta catalog janitor after meta and regions have been assigned.
    status.setStatus("Starting balancer and catalog janitor");
    this.clusterStatusChore = new ClusterStatusChore(this, balancer);
    getChoreService().scheduleChore(clusterStatusChore);
    this.balancerChore = new BalancerChore(this);
    getChoreService().scheduleChore(balancerChore);
    this.normalizerChore = new RegionNormalizerChore(this);
    getChoreService().scheduleChore(normalizerChore);
    this.catalogJanitorChore = new CatalogJanitor(this, this);
    getChoreService().scheduleChore(catalogJanitorChore);

    // Do Metrics periodically
    periodicDoMetricsChore = new PeriodicDoMetrics(msgInterval, this);
    getChoreService().scheduleChore(periodicDoMetricsChore);

    status.setStatus("Starting namespace manager");
    initNamespace();

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

    // Set master as 'initialized'.
    setInitialized(true);

    assignmentManager.checkIfShouldMoveSystemRegionAsync();

    status.setStatus("Starting quota manager");
    initQuotaManager();

    // assign the meta replicas
    Set<ServerName> EMPTY_SET = new HashSet<ServerName>();
    int numReplicas = conf.getInt(HConstants.META_REPLICAS_NUM,
           HConstants.DEFAULT_META_REPLICA_NUM);
    for (int i = 1; i < numReplicas; i++) {
      assignMeta(status, EMPTY_SET, i);
    }
    unassignExcessMetaReplica(zooKeeper, numReplicas);

    // clear the dead servers with same host name and port of online server because we are not
    // removing dead server with same hostname and port of rs which is trying to check in before
    // master initialization. See HBASE-5916.
    this.serverManager.clearDeadServersWithSameHostNameAndPortOfOnlineServer();

    // Check and set the znode ACLs if needed in case we are overtaking a non-secure configuration
    status.setStatus("Checking ZNode ACLs");
    zooKeeper.checkAndSetZNodeAcls();

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

  private void initQuotaManager() throws IOException {
    quotaManager = new MasterQuotaManager(this);
    this.assignmentManager.setRegionStateListener((RegionStateListener) quotaManager);
    quotaManager.start();
  }

  /**
   * Create a {@link ServerManager} instance.
   * @param master
   * @param services
   * @return An instance of {@link ServerManager}
   * @throws org.apache.hadoop.hbase.ZooKeeperConnectionException
   * @throws IOException
   */
  ServerManager createServerManager(final Server master,
      final MasterServices services)
  throws IOException {
    // We put this out here in a method so can do a Mockito.spy and stub it out
    // w/ a mocked up ServerManager.
    return new ServerManager(master, services);
  }

  private void unassignExcessMetaReplica(ZooKeeperWatcher zkw, int numMetaReplicasConfigured) {
    // unassign the unneeded replicas (for e.g., if the previous master was configured
    // with a replication of 3 and now it is 2, we need to unassign the 1 unneeded replica)
    try {
      List<String> metaReplicaZnodes = zooKeeper.getMetaReplicaNodes();
      for (String metaReplicaZnode : metaReplicaZnodes) {
        int replicaId = zooKeeper.getMetaReplicaIdFromZnode(metaReplicaZnode);
        if (replicaId >= numMetaReplicasConfigured) {
          RegionState r = MetaTableLocator.getMetaRegionState(zkw, replicaId);
          LOG.info("Closing excess replica of meta region " + r.getRegion());
          // send a close and wait for a max of 30 seconds
          ServerManager.closeRegionSilentlyAndWait(getConnection(), r.getServerName(),
              r.getRegion(), 30000);
          ZKUtil.deleteNode(zkw, zkw.getZNodeForReplica(replicaId));
        }
      }
    } catch (Exception ex) {
      // ignore the exception since we don't want the master to be wedged due to potential
      // issues in the cleanup of the extra regions. We can do that cleanup via hbck or manually
      LOG.warn("Ignoring exception " + ex);
    }
  }

  /**
   * Check <code>hbase:meta</code> is assigned. If not, assign it.
   * @param status MonitoredTask
   * @param previouslyFailedMetaRSs
   * @param replicaId
   * @throws InterruptedException
   * @throws IOException
   * @throws KeeperException
   */
  void assignMeta(MonitoredTask status, Set<ServerName> previouslyFailedMetaRSs, int replicaId)
      throws InterruptedException, IOException, KeeperException {
    // Work on meta region
    int assigned = 0;
    long timeout = this.conf.getLong("hbase.catalog.verification.timeout", 1000);
    if (replicaId == HRegionInfo.DEFAULT_REPLICA_ID) {
      status.setStatus("Assigning hbase:meta region");
    } else {
      status.setStatus("Assigning hbase:meta region, replicaId " + replicaId);
    }
    // Get current meta state from zk.
    RegionStates regionStates = assignmentManager.getRegionStates();
    RegionState metaState = MetaTableLocator.getMetaRegionState(getZooKeeper(), replicaId);
    HRegionInfo hri = RegionReplicaUtil.getRegionInfoForReplica(HRegionInfo.FIRST_META_REGIONINFO,
        replicaId);
    ServerName currentMetaServer = metaState.getServerName();
    if (!ConfigUtil.useZKForAssignment(conf)) {
      regionStates.createRegionState(hri, metaState.getState(),
        currentMetaServer, null);
    } else {
      regionStates.createRegionState(hri);
    }
    boolean rit = this.assignmentManager.
      processRegionInTransitionAndBlockUntilAssigned(hri);
    boolean metaRegionLocation = metaTableLocator.verifyMetaRegionLocation(
      this.getConnection(), this.getZooKeeper(), timeout, replicaId);
    if (!metaRegionLocation || !metaState.isOpened()) {
      // Meta location is not verified. It should be in transition, or offline.
      // We will wait for it to be assigned in enableSSHandWaitForMeta below.
      assigned++;
      if (!ConfigUtil.useZKForAssignment(conf)) {
        assignMetaZkLess(regionStates, metaState, timeout, previouslyFailedMetaRSs);
      } else if (!rit) {
        // Assign meta since not already in transition
        if (currentMetaServer != null) {
          // If the meta server is not known to be dead or online,
          // just split the meta log, and don't expire it since this
          // could be a full cluster restart. Otherwise, we will think
          // this is a failover and lose previous region locations.
          // If it is really a failover case, AM will find out in rebuilding
          // user regions. Otherwise, we are good since all logs are split
          // or known to be replayed before user regions are assigned.
          if (serverManager.isServerOnline(currentMetaServer)) {
            LOG.info("Forcing expire of " + currentMetaServer);
            serverManager.expireServer(currentMetaServer);
          }
          if (replicaId == HRegionInfo.DEFAULT_REPLICA_ID) {
            splitMetaLogBeforeAssignment(currentMetaServer);
            previouslyFailedMetaRSs.add(currentMetaServer);
          }
        }
        assignmentManager.assignMeta(hri);
      }
    } else {
      // Region already assigned. We didn't assign it. Add to in-memory state.
      regionStates.updateRegionState(hri, State.OPEN, currentMetaServer);
      this.assignmentManager.regionOnline(hri, currentMetaServer);
    }

    if (replicaId == HRegionInfo.DEFAULT_REPLICA_ID) enableMeta(TableName.META_TABLE_NAME);

    if ((RecoveryMode.LOG_REPLAY == this.getMasterFileSystem().getLogRecoveryMode())
        && (!previouslyFailedMetaRSs.isEmpty())) {
      // replay WAL edits mode need new hbase:meta RS is assigned firstly
      status.setStatus("replaying log for Meta Region");
      this.fileSystemManager.splitMetaLog(previouslyFailedMetaRSs);
    }

    // Make sure a hbase:meta location is set. We need to enable SSH here since
    // if the meta region server is died at this time, we need it to be re-assigned
    // by SSH so that system tables can be assigned.
    // No need to wait for meta is assigned = 0 when meta is just verified.
    if (replicaId == HRegionInfo.DEFAULT_REPLICA_ID) enableCrashedServerProcessing(assigned != 0);
    LOG.info("hbase:meta with replicaId " + replicaId + " assigned=" + assigned + ", rit=" + rit +
      ", location=" + metaTableLocator.getMetaRegionLocation(this.getZooKeeper(), replicaId));
    status.setStatus("META assigned.");
  }

  private void assignMetaZkLess(RegionStates regionStates, RegionState regionState, long timeout,
      Set<ServerName> previouslyFailedRs) throws IOException, KeeperException {
    ServerName currentServer = regionState.getServerName();
    if (serverManager.isServerOnline(currentServer)) {
      LOG.info("Meta was in transition on " + currentServer);
      assignmentManager.processRegionInTransitionZkLess();
    } else {
      if (currentServer != null) {
        if (regionState.getRegion().getReplicaId() == HRegionInfo.DEFAULT_REPLICA_ID) {
          splitMetaLogBeforeAssignment(currentServer);
          regionStates.logSplit(HRegionInfo.FIRST_META_REGIONINFO);
          previouslyFailedRs.add(currentServer);
        }
      }
      LOG.info("Re-assigning hbase:meta, it was on " + currentServer);
      regionStates.updateRegionState(regionState.getRegion(), State.OFFLINE);
      assignmentManager.assignMeta(regionState.getRegion());
    }
  }

  void initNamespace() throws IOException {
    //create namespace manager
    tableNamespaceManager = new TableNamespaceManager(this);
    tableNamespaceManager.start();
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

    if(logCleaner != null) {
      logCleanerFlag = logCleaner.getEnabled();
    }

    return (hfileCleanerFlag && logCleanerFlag);
  }

  private void splitMetaLogBeforeAssignment(ServerName currentMetaServer) throws IOException {
    if (RecoveryMode.LOG_REPLAY == this.getMasterFileSystem().getLogRecoveryMode()) {
      // In log replay mode, we mark hbase:meta region as recovering in ZK
      Set<HRegionInfo> regions = new HashSet<HRegionInfo>();
      regions.add(HRegionInfo.FIRST_META_REGIONINFO);
      this.fileSystemManager.prepareLogReplay(currentMetaServer, regions);
    } else {
      // In recovered.edits mode: create recovered edits file for hbase:meta server
      this.fileSystemManager.splitMetaLog(currentMetaServer);
    }
  }

  private void enableCrashedServerProcessing(final boolean waitForMeta)
  throws IOException, InterruptedException {
    // If crashed server processing is disabled, we enable it and expire those dead but not expired
    // servers. This is required so that if meta is assigning to a server which dies after
    // assignMeta starts assignment, ServerCrashProcedure can re-assign it. Otherwise, we will be
    // stuck here waiting forever if waitForMeta is specified.
    if (!isServerCrashProcessingEnabled()) {
      setServerCrashProcessingEnabled(true);
      this.serverManager.processQueuedDeadServers();
    }

    if (waitForMeta) {
      metaTableLocator.waitMetaRegionLocation(this.getZooKeeper());
      // Above check waits for general meta availability but this does not
      // guarantee that the transition has completed
      this.assignmentManager.waitForAssignment(HRegionInfo.FIRST_META_REGIONINFO);
    }
  }

  private void enableMeta(TableName metaTableName) {
    if (!this.assignmentManager.getTableStateManager().isTableState(metaTableName,
        ZooKeeperProtos.Table.State.ENABLED)) {
      this.assignmentManager.setEnabledTable(metaTableName);
    }
  }

  /**
   * This function returns a set of region server names under hbase:meta recovering region ZK node
   * @return Set of meta server names which were recorded in ZK
   * @throws KeeperException
   */
  private Set<ServerName> getPreviouselyFailedMetaServersFromZK() throws KeeperException {
    Set<ServerName> result = new HashSet<ServerName>();
    String metaRecoveringZNode = ZKUtil.joinZNode(zooKeeper.recoveringRegionsZNode,
      HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
    List<String> regionFailedServers = ZKUtil.listChildrenNoWatch(zooKeeper, metaRecoveringZNode);
    if (regionFailedServers == null) return result;

    for(String failedServer : regionFailedServers) {
      ServerName server = ServerName.parseServerName(failedServer);
      result.add(server);
    }
    return result;
  }

  @Override
  public TableDescriptors getTableDescriptors() {
    return this.tableDescriptors;
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
  public TableNamespaceManager getTableNamespaceManager() {
    return tableNamespaceManager;
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
    this.service.startExecutorService(ExecutorType.MASTER_OPEN_REGION, conf.getInt(
      HConstants.MASTER_OPEN_REGION_THREADS, HConstants.MASTER_OPEN_REGION_THREADS_DEFAULT));
    this.service.startExecutorService(ExecutorType.MASTER_CLOSE_REGION, conf.getInt(
      HConstants.MASTER_CLOSE_REGION_THREADS, HConstants.MASTER_CLOSE_REGION_THREADS_DEFAULT));
    this.service.startExecutorService(ExecutorType.MASTER_SERVER_OPERATIONS,
      conf.getInt(HConstants.MASTER_SERVER_OPERATIONS_THREADS,
        HConstants.MASTER_SERVER_OPERATIONS_THREADS_DEFAULT));
    this.service.startExecutorService(ExecutorType.MASTER_META_SERVER_OPERATIONS,
      conf.getInt(HConstants.MASTER_META_SERVER_OPERATIONS_THREADS,
        HConstants.MASTER_META_SERVER_OPERATIONS_THREADS_DEFAULT));
    this.service.startExecutorService(ExecutorType.M_LOG_REPLAY_OPS, conf.getInt(
      HConstants.MASTER_LOG_REPLAY_OPS_THREADS, HConstants.MASTER_LOG_REPLAY_OPS_THREADS_DEFAULT));
    this.service.startExecutorService(ExecutorType.MASTER_SNAPSHOT_OPERATIONS, conf.getInt(
      SnapshotManager.SNAPSHOT_POOL_THREADS_KEY, SnapshotManager.SNAPSHOT_POOL_THREADS_DEFAULT));

   // We depend on there being only one instance of this executor running
   // at a time.  To do concurrency, would need fencing of enable/disable of
   // tables.
   // Any time changing this maxThreads to > 1, pls see the comment at
   // AccessController#postCreateTableHandler
   this.service.startExecutorService(ExecutorType.MASTER_TABLE_OPERATIONS, 1);
   startProcedureExecutor();

    // Create cleaner thread pool
    cleanerPool = new DirScanPool(conf);
    // Start log cleaner thread
    int cleanerInterval = conf.getInt("hbase.master.cleaner.interval", 600 * 1000);
    this.logCleaner = new LogCleaner(cleanerInterval, this, conf,
      getMasterFileSystem().getOldLogDir().getFileSystem(conf),
      getMasterFileSystem().getOldLogDir(), cleanerPool);
   //start the hfile archive cleaner thread
    Path archiveDir = HFileArchiveUtil.getArchivePath(conf);
    Map<String, Object> params = new HashMap<String, Object>();
    params.put(MASTER, this);
    this.hfileCleaner = new HFileCleaner(cleanerInterval, this, conf,
      getMasterFileSystem().getFileSystem(), archiveDir, cleanerPool, params);
    getChoreService().scheduleChore(hfileCleaner);

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
    if (!conf.getBoolean(HConstants.ZOOKEEPER_USEMULTI, true)) {
      try {
        replicationZKLockCleanerChore = new ReplicationZKLockCleanerChore(this, this,
            cleanerInterval, this.getZooKeeper(), this.conf);
        getChoreService().scheduleChore(replicationZKLockCleanerChore);
      } catch (Exception e) {
        LOG.error("start replicationZKLockCleanerChore failed", e);
      }
    }
    try {
      replicationZKNodeCleanerChore = new ReplicationZKNodeCleanerChore(this, cleanerInterval,
          new ReplicationZKNodeCleaner(this.conf, this.getZooKeeper(), this));
      getChoreService().scheduleChore(replicationZKNodeCleanerChore);
    } catch (Exception e) {
      LOG.error("start replicationZKNodeCleanerChore failed", e);
    }
  }

  @Override
  protected void sendShutdownInterrupt() {
    super.sendShutdownInterrupt();
    stopProcedureExecutor();
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
    // Wait for all the remaining region servers to report in IFF we were
    // running a cluster shutdown AND we were NOT aborting.
    if (!isAborted() && this.serverManager != null &&
        this.serverManager.isClusterShutdown()) {
      this.serverManager.letRegionServersShutdown();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping service threads");
    }
    // Clean up and close up shop
    if (this.quotaManager != null) this.quotaManager.stop();
    if (this.activeMasterManager != null) this.activeMasterManager.stop();
    if (this.serverManager != null) this.serverManager.stop();
    if (this.assignmentManager != null) this.assignmentManager.stop();
    if (this.fileSystemManager != null) this.fileSystemManager.stop();
    if (this.mpmHost != null) this.mpmHost.stop("server shutting down.");
  }

  private void startProcedureExecutor() throws IOException {
    final MasterProcedureEnv procEnv = new MasterProcedureEnv(this);
    final Path walDir = new Path(CommonFSUtils.getWALRootDir(this.conf),
      WALProcedureStore.MASTER_PROCEDURE_LOGDIR);
    procedureStore = new WALProcedureStore(conf, walDir,
        new MasterProcedureEnv.WALStoreLeaseRecovery(this));
    procedureStore.registerListener(new MasterProcedureEnv.MasterProcedureStoreListener(this));
    procedureExecutor = new ProcedureExecutor(conf, procEnv, procedureStore,
        procEnv.getProcedureQueue());

    final int numThreads = conf.getInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS,
        Math.max(Runtime.getRuntime().availableProcessors(),
          MasterProcedureConstants.DEFAULT_MIN_MASTER_PROCEDURE_THREADS));
    final boolean abortOnCorruption = conf.getBoolean(
        MasterProcedureConstants.EXECUTOR_ABORT_ON_CORRUPTION,
        MasterProcedureConstants.DEFAULT_EXECUTOR_ABORT_ON_CORRUPTION);
    procedureStore.start(numThreads);
    procedureExecutor.start(numThreads, abortOnCorruption);
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
      LOG.error("Error updating snapshot cleanup mode to " + on, e);
    }
  }


  private void stopProcedureExecutor() {
    if (procedureExecutor != null) {
      procedureExecutor.stop();
    }

    if (procedureStore != null) {
      procedureStore.stop(isAborted());
    }
  }

  private void stopChores() {
    ChoreService choreService = getChoreService();
    if (choreService != null) {
      choreService.cancelChore(this.balancerChore);
      choreService.cancelChore(this.normalizerChore);
      choreService.cancelChore(this.clusterStatusChore);
      choreService.cancelChore(this.catalogJanitorChore);
      choreService.cancelChore(this.clusterStatusPublisherChore);
      choreService.cancelChore(this.periodicDoMetricsChore);
      choreService.cancelChore(this.logCleaner);
      choreService.cancelChore(this.hfileCleaner);
      choreService.cancelChore(this.replicationZKLockCleanerChore);
      choreService.cancelChore(this.replicationZKNodeCleanerChore);
      choreService.cancelChore(this.snapshotCleanerChore);
    }
  }

  /**
   * @return Get remote side's InetAddress
   * @throws UnknownHostException
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
    int maxBalancingTime = getConfiguration().
        getInt(HConstants.HBASE_BALANCER_MAX_BALANCING, -1);
    if (maxBalancingTime == -1) {
      // No time period set so create one
      maxBalancingTime = getConfiguration().getInt(HConstants.HBASE_BALANCER_PERIOD,
        HConstants.DEFAULT_HBASE_BALANCER_PERIOD);
    }
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
        && this.assignmentManager.getRegionStates().getRegionsInTransitionCount() != 0) {
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

  public boolean balance(boolean force) throws IOException {
    // if master not initialized, don't run balancer.
    if (!isInitialized()) {
      LOG.debug("Master has not been initialized, don't run balancer.");
      return false;
    }

    if (isInMaintenanceMode()) {
      LOG.info("Master is in maintenanceMode mode, don't run balancer.");
      return false;
    }

    int maxRegionsInTransition = getMaxRegionsInTransition();
    synchronized (this.balancer) {
      // If balance not true, don't run balancer.
      if (!this.loadBalancerTracker.isBalancerOn()) return false;
      // Only allow one balance run at at time.
      if (this.assignmentManager.getRegionStates().isRegionsInTransition()) {
        Set<RegionState> regionsInTransition =
          this.assignmentManager.getRegionStates().getRegionsInTransition();
        // if hbase:meta region is in transition, result of assignment cannot be recorded
        // ignore the force flag in that case
        boolean metaInTransition = assignmentManager.getRegionStates().isMetaRegionInTransition();
        String prefix = force && !metaInTransition ? "R" : "Not r";
        LOG.debug(prefix + "unning balancer because " + regionsInTransition.size() +
          " region(s) in transition: " + org.apache.commons.lang.StringUtils.
            abbreviate(regionsInTransition.toString(), 256));
        if (!force || metaInTransition) return false;
      }
      if (this.serverManager.areDeadServersInProgress()) {
        LOG.debug("Not running balancer because processing dead regionserver(s): " +
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

      Map<TableName, Map<ServerName, List<HRegionInfo>>> assignmentsByTable =
        this.assignmentManager.getRegionStates().getAssignmentsByTable();

      List<RegionPlan> plans = new ArrayList<RegionPlan>();

      //Give the balancer the current cluster state.
      this.balancer.setClusterStatus(getClusterStatusWithoutCoprocessor());
      for (Entry<TableName, Map<ServerName, List<HRegionInfo>>> e : assignmentsByTable.entrySet()) {
        List<RegionPlan> partialPlans = this.balancer.balanceCluster(e.getKey(), e.getValue());
        if (partialPlans != null) plans.addAll(partialPlans);
      }

      long balanceStartTime = System.currentTimeMillis();
      long cutoffTime = balanceStartTime +  this.maxBlancingTime;
      int rpCount = 0;  // number of RegionPlans balanced so far
      if (plans != null && !plans.isEmpty()) {
        int balanceInterval =  this.maxBlancingTime / plans.size();
        LOG.info("Balancer plans size is " + plans.size() + ", the balance interval is "
            + balanceInterval + " ms, and the max number regions in transition is "
            + maxRegionsInTransition);

        for (RegionPlan plan: plans) {
          LOG.info("balance " + plan);
          //TODO: bulk assign
          this.assignmentManager.balance(plan);
          rpCount++;

          balanceThrottling(balanceStartTime + rpCount * balanceInterval, maxRegionsInTransition,
            cutoffTime);

          // if performing next balance exceeds cutoff time, exit the loop
          if (rpCount < plans.size() && System.currentTimeMillis() > cutoffTime) {
            // TODO: After balance, there should not be a cutoff time (keeping it as a security net
            // for now)
            LOG.debug("No more balancing till next balance run; maxBalanceTime="
                +  this.maxBlancingTime);
            break;
          }
        }
      }

      if (this.cpHost != null) {
        try {
          this.cpHost.postBalance(rpCount < plans.size() ? plans.subList(0, rpCount) : plans);
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
   * Perform normalization of cluster (invoked by {@link RegionNormalizerChore}).
   *
   * @return true if normalization step was performed successfully, false otherwise
   *   (specifically, if HMaster hasn't been initialized properly or normalization
   *   is globally disabled)
   * @throws IOException
   * @throws CoordinatedStateException
   */
  public boolean normalizeRegions() throws IOException, CoordinatedStateException {
    if (!isInitialized()) {
      LOG.debug("Master has not been initialized, don't run region normalizer.");
      return false;
    }

    if (isInMaintenanceMode()) {
      LOG.info("Master is in maintenance mode, don't run region normalizer.");
      return false;
    }

    if (!this.regionNormalizerTracker.isNormalizerOn()) {
      LOG.debug("Region normalization is disabled, don't run region normalizer.");
      return false;
    }

    synchronized (this.normalizer) {
      // Don't run the normalizer concurrently
      List<TableName> allEnabledTables = new ArrayList<>(
        this.assignmentManager.getTableStateManager().getTablesInStates(
          ZooKeeperProtos.Table.State.ENABLED));

      Collections.shuffle(allEnabledTables);

      for (TableName table : allEnabledTables) {
        if (isInMaintenanceMode()) {
          LOG.debug("Master is in maintenance mode, stop running region normalizer.");
          return false;
        }

        if (quotaManager.getNamespaceQuotaManager() != null &&
            quotaManager.getNamespaceQuotaManager().getState(table.getNamespaceAsString()) != null){
          LOG.debug("Skipping normalizing " + table + " since its namespace has quota");
          continue;
        }
        if (table.isSystemTable() || (getTableDescriptors().get(table) != null &&
            !getTableDescriptors().get(table).isNormalizationEnabled())) {
          LOG.debug("Skipping normalization for table: " + table + ", as it's either system"
              + " table or doesn't have auto normalization turned on");
          continue;
        }
        List<NormalizationPlan> plans = this.normalizer.computePlanForTable(table);
        if (plans != null) {
          for (NormalizationPlan plan : plans) {
            plan.execute(clusterConnection.getAdmin());
            if (plan.getType() == PlanType.SPLIT) {
              splitPlanCount++;
            } else if (plan.getType() == PlanType.MERGE) {
              mergePlanCount++;
            }
          }
        }
      }
    }
    // If Region did not generate any plans, it means the cluster is already balanced.
    // Return true indicating a success.
    return true;
  }

  /**
   * @return Client info for use as prefix on an audit log string; who did an action
   */
  String getClientIdAuditPrefix() {
    return "Client=" + RpcServer.getRequestUserName() + "/" + RpcServer.getRemoteAddress();
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
  public void dispatchMergingRegions(final HRegionInfo region_a,
      final HRegionInfo region_b, final boolean forcible, final User user) throws IOException {
    checkInitialized();
    this.service.submit(new DispatchMergingRegionHandler(this,
      this.catalogJanitorChore, region_a, region_b, forcible, user));
  }

  @VisibleForTesting // Public so can be accessed by tests.
  public void move(final byte[] encodedRegionName,
      byte[] destServerName) throws HBaseIOException {
    RegionState regionState = assignmentManager.getRegionStates().
      getRegionState(Bytes.toString(encodedRegionName));
    if (regionState == null) {
      throw new UnknownRegionException(Bytes.toStringBinary(encodedRegionName));
    } else if (!assignmentManager.getRegionStates()
        .isRegionOnline(regionState.getRegion())) {
      throw new HBaseIOException(
          "moving region not onlined: " + regionState.getRegion() + ", "
              + regionState);
    }

    HRegionInfo hri = regionState.getRegion();
    ServerName dest;
    List<ServerName> exclude = hri.isSystemTable() ? assignmentManager.getExcludedServersForSystemTable()
        : new ArrayList<ServerName>(1);
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
      if (dest.equals(serverName) && balancer instanceof BaseLoadBalancer
          && !((BaseLoadBalancer)balancer).shouldBeOnMaster(hri)) {
        // To avoid unnecessary region moving later by balancer. Don't put user
        // regions on master. Regions on master could be put on other region
        // server intentionally by test however.
        LOG.debug("Skipping move of region " + hri.getRegionNameAsString()
          + " to avoid unnecessary region moving later by load balancer,"
          + " because it should not be on master");
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

    try {
      checkInitialized();
      if (this.cpHost != null) {
        if (this.cpHost.preMove(hri, rp.getSource(), rp.getDestination())) {
          return;
        }
      }
      // warmup the region on the destination before initiating the move. this call
      // is synchronous and takes some time. doing it before the source region gets
      // closed
      serverManager.sendRegionWarmup(rp.getDestination(), hri);

      LOG.info(getClientIdAuditPrefix() + " move " + rp + ", running balancer");
      this.assignmentManager.balance(rp);
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
  public long createTable(
      final HTableDescriptor hTableDescriptor,
      final byte [][] splitKeys,
      final long nonceGroup,
      final long nonce) throws IOException {
    if (isStopped()) {
      throw new MasterNotRunningException();
    }

    String namespace = hTableDescriptor.getTableName().getNamespaceAsString();
    ensureNamespaceExists(namespace);

    final HRegionInfo[] newRegions =
        ModifyRegionUtils.createHRegionInfos(hTableDescriptor, splitKeys);
    checkInitialized();
    sanityCheckTableDescriptor(hTableDescriptor);

    return MasterProcedureUtil.submitProcedure(
      new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
      @Override
      protected void run() throws IOException {
        getMaster().getMasterCoprocessorHost().preCreateTable(hTableDescriptor, newRegions);

        LOG.info(getClientIdAuditPrefix() + " create " + hTableDescriptor);

        // TODO: We can handle/merge duplicate requests, and differentiate the case of
        //       TableExistsException by saying if the schema is the same or not.
        ProcedurePrepareLatch latch = ProcedurePrepareLatch.createLatch();
        submitProcedure(new CreateTableProcedure(
          procedureExecutor.getEnvironment(), hTableDescriptor, newRegions, latch));
        latch.await();

        getMaster().getMasterCoprocessorHost().postCreateTable(hTableDescriptor, newRegions);
      }

      @Override
      protected String getDescription() {
        return "CreateTableProcedure";
      }
    });
  }

  @Override
  public long createSystemTable(final HTableDescriptor hTableDescriptor) throws IOException {
    if (isStopped()) {
      throw new MasterNotRunningException();
    }

    TableName tableName = hTableDescriptor.getTableName();
    if (!(tableName.isSystemTable())) {
      throw new IllegalArgumentException(
        "Only system table creation can use this createSystemTable API");
    }

    HRegionInfo[] newRegions = ModifyRegionUtils.createHRegionInfos(hTableDescriptor, null);

    LOG.info(getClientIdAuditPrefix() + " create " + hTableDescriptor);

    // This special create table is called locally to master.  Therefore, no RPC means no need
    // to use nonce to detect duplicated RPC call.
    long procId = this.procedureExecutor.submitProcedure(
      new CreateTableProcedure(procedureExecutor.getEnvironment(), hTableDescriptor, newRegions));

    return procId;
  }

  /**
   * Checks whether the table conforms to some sane limits, and configured
   * values (compression, etc) work. Throws an exception if something is wrong.
   * @throws IOException
   */
  private void sanityCheckTableDescriptor(final HTableDescriptor htd) throws IOException {
    final String CONF_KEY = "hbase.table.sanity.checks";
    boolean logWarn = false;
    if (!conf.getBoolean(CONF_KEY, true)) {
      logWarn = true;
    }
    String tableVal = htd.getConfigurationValue(CONF_KEY);
    if (tableVal != null && !Boolean.valueOf(tableVal)) {
      logWarn = true;
    }

    // check max file size
    long maxFileSizeLowerLimit = 2 * 1024 * 1024L; // 2M is the default lower limit
    long maxFileSize = htd.getMaxFileSize();
    if (maxFileSize < 0) {
      maxFileSize = conf.getLong(HConstants.HREGION_MAX_FILESIZE, maxFileSizeLowerLimit);
    }
    if (maxFileSize < conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit)) {
      String message = "MAX_FILESIZE for table descriptor or "
          + "\"hbase.hregion.max.filesize\" (" + maxFileSize
          + ") is too small, which might cause over splitting into unmanageable "
          + "number of regions.";
      warnOrThrowExceptionForFailure(logWarn, CONF_KEY, message, null);
    }

    // check flush size
    long flushSizeLowerLimit = 1024 * 1024L; // 1M is the default lower limit
    long flushSize = htd.getMemStoreFlushSize();
    if (flushSize < 0) {
      flushSize = conf.getLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, flushSizeLowerLimit);
    }
    if (flushSize < conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit)) {
      String message = "MEMSTORE_FLUSHSIZE for table descriptor or "
          + "\"hbase.hregion.memstore.flush.size\" ("+flushSize+") is too small, which might cause"
          + " very frequent flushing.";
      warnOrThrowExceptionForFailure(logWarn, CONF_KEY, message, null);
    }

    // check that coprocessors and other specified plugin classes can be loaded
    try {
      checkClassLoading(conf, htd);
    } catch (Exception ex) {
      warnOrThrowExceptionForFailure(logWarn, CONF_KEY, ex.getMessage(), null);
    }

    // check compression can be loaded
    try {
      checkCompression(htd);
    } catch (IOException e) {
      warnOrThrowExceptionForFailure(logWarn, CONF_KEY, e.getMessage(), e);
    }

    // check encryption can be loaded
    try {
      checkEncryption(conf, htd);
    } catch (IOException e) {
      warnOrThrowExceptionForFailure(logWarn, CONF_KEY, e.getMessage(), e);
    }
    // Verify compaction policy
    try{
      checkCompactionPolicy(conf, htd);
    } catch(IOException e){
      warnOrThrowExceptionForFailure(false, CONF_KEY, e.getMessage(), e);
    }
    // check that we have at least 1 CF
    if (htd.getColumnFamilies().length == 0) {
      String message = "Table should have at least one column family.";
      warnOrThrowExceptionForFailure(logWarn, CONF_KEY, message, null);
    }

    // check that we have minimum 1 region replicas
    int regionReplicas = htd.getRegionReplication();
    if (regionReplicas < 1) {
      String message = "Table region replication should be at least one.";
      warnOrThrowExceptionForFailure(logWarn, CONF_KEY, message, null);
    }

    for (HColumnDescriptor hcd : htd.getColumnFamilies()) {
      if (hcd.getTimeToLive() <= 0) {
        String message = "TTL for column family " + hcd.getNameAsString() + " must be positive.";
        warnOrThrowExceptionForFailure(logWarn, CONF_KEY, message, null);
      }

      // check blockSize
      if (hcd.getBlocksize() < 1024 || hcd.getBlocksize() > 16 * 1024 * 1024) {
        String message = "Block size for column family " + hcd.getNameAsString()
            + "  must be between 1K and 16MB.";
        warnOrThrowExceptionForFailure(logWarn, CONF_KEY, message, null);
      }

      // check versions
      if (hcd.getMinVersions() < 0) {
        String message = "Min versions for column family " + hcd.getNameAsString()
          + "  must be positive.";
        warnOrThrowExceptionForFailure(logWarn, CONF_KEY, message, null);
      }
      // max versions already being checked

      // HBASE-13776 Setting illegal versions for HColumnDescriptor
      //  does not throw IllegalArgumentException
      // check minVersions <= maxVerions
      if (hcd.getMinVersions() > hcd.getMaxVersions()) {
        String message = "Min versions for column family " + hcd.getNameAsString()
            + " must be less than the Max versions.";
        warnOrThrowExceptionForFailure(logWarn, CONF_KEY, message, null);
      }

      // check replication scope
      checkReplicationScope(hcd);

      // check data replication factor, it can be 0(default value) when user has not explicitly
      // set the value, in this case we use default replication factor set in the file system.
      if (hcd.getDFSReplication() < 0) {
        String message = "HFile Replication for column family " + hcd.getNameAsString()
            + "  must be greater than zero.";
        warnOrThrowExceptionForFailure(logWarn, CONF_KEY, message, null);
      }

      // TODO: should we check coprocessors and encryption ?
    }
  }

  private void checkReplicationScope(HColumnDescriptor hcd) throws IOException{
    // check replication scope
    WALProtos.ScopeType scop = WALProtos.ScopeType.valueOf(hcd.getScope());
    if (scop == null) {
      String message = "Replication scope for column family "
          + hcd.getNameAsString() + " is " + hcd.getScope() + " which is invalid.";

      LOG.error(message);
      throw new DoNotRetryIOException(message);
    }
  }

  private void checkCompactionPolicy(Configuration conf, HTableDescriptor htd)
      throws IOException {
    // FIFO compaction has some requirements
    // Actually FCP ignores periodic major compactions
    String className =
        htd.getConfigurationValue(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY);
    if (className == null) {
      className =
          conf.get(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY,
            ExploringCompactionPolicy.class.getName());
    }

    int blockingFileCount = HStore.DEFAULT_BLOCKING_STOREFILE_COUNT;
    String sv = htd.getConfigurationValue(HStore.BLOCKING_STOREFILES_KEY);
    if (sv != null) {
      blockingFileCount = Integer.parseInt(sv);
    } else {
      blockingFileCount = conf.getInt(HStore.BLOCKING_STOREFILES_KEY, blockingFileCount);
    }

    for (HColumnDescriptor hcd : htd.getColumnFamilies()) {
      String compactionPolicy =
          hcd.getConfigurationValue(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY);
      if (compactionPolicy == null) {
        compactionPolicy = className;
      }
      if (!compactionPolicy.equals(FIFOCompactionPolicy.class.getName())) {
        continue;
      }
      // FIFOCompaction
      String message = null;

      // 1. Check TTL
      if (hcd.getTimeToLive() == HColumnDescriptor.DEFAULT_TTL) {
        message = "Default TTL is not supported for FIFO compaction";
        throw new IOException(message);
      }

      // 2. Check min versions
      if (hcd.getMinVersions() > 0) {
        message = "MIN_VERSION > 0 is not supported for FIFO compaction";
        throw new IOException(message);
      }

      // 3. blocking file count
      sv = hcd.getConfigurationValue(HStore.BLOCKING_STOREFILES_KEY);
      if (sv != null) {
        blockingFileCount = Integer.parseInt(sv);
      }
      if (blockingFileCount < 1000) {
        message =
            "blocking file count '" + HStore.BLOCKING_STOREFILES_KEY + "' " + blockingFileCount
                + " is below recommended minimum of 1000";
        throw new IOException(message);
      }
    }
  }

  // HBASE-13350 - Helper method to log warning on sanity check failures if checks disabled.
  private static void warnOrThrowExceptionForFailure(boolean logWarn, String confKey,
      String message, Exception cause) throws IOException {
    if (!logWarn) {
      throw new DoNotRetryIOException(message + " Set " + confKey +
          " to false at conf or table descriptor if you want to bypass sanity checks", cause);
    }
    LOG.warn(message);
  }

  private void startActiveMasterManager(int infoPort) throws KeeperException {
    String backupZNode = ZKUtil.joinZNode(
      zooKeeper.backupMasterAddressesZNode, serverName.toString());
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
    if (!MasterAddressTracker.setMasterAddress(zooKeeper, backupZNode,
        serverName, infoPort)) {
      LOG.warn("Failed create of " + backupZNode + " by " + serverName);
    }

    activeMasterManager.setInfoPort(infoPort);
    // Start a thread to try to become the active master, so we won't block here
    Threads.setDaemonThreadRunning(new Thread(new Runnable() {
      @Override
      public void run() {
        int timeout = conf.getInt(HConstants.ZK_SESSION_TIMEOUT,
          HConstants.DEFAULT_ZK_SESSION_TIMEOUT);
        // If we're a backup master, stall until a primary to writes his address
        if (conf.getBoolean(HConstants.MASTER_TYPE_BACKUP,
          HConstants.DEFAULT_MASTER_TYPE_BACKUP)) {
          LOG.debug("HMaster started in backup mode. "
            + "Stalling until master znode is written.");
          // This will only be a minute or so while the cluster starts up,
          // so don't worry about setting watches on the parent znode
          while (!activeMasterManager.hasActiveMaster()) {
            LOG.debug("Waiting for master address ZNode to be written "
              + "(Also watching cluster state node)");
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
          LOG.fatal("Failed to become active master", t);
          // HBASE-5680: Likely hadoop23 vs hadoop 20.x/1.x incompatibility
          if (t instanceof NoClassDefFoundError &&
            t.getMessage()
              .contains("org/apache/hadoop/hdfs/protocol/HdfsConstants$SafeModeAction")) {
            // improved error message for this special case
            abort("HBase is having a problem with its Hadoop jars.  You may need to "
              + "recompile HBase against Hadoop version "
              + org.apache.hadoop.util.VersionInfo.getVersion()
              + " or change your hadoop jars to start properly", t);
          } else {
            abort("Unhandled exception. Starting shutdown.", t);
          }
        } finally {
          status.cleanup();
        }
      }
    }, getServerName().toShortString() + ".activeMasterManager"));
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

  private void checkEncryption(final Configuration conf, final HTableDescriptor htd)
  throws IOException {
    if (!this.masterCheckEncryption) return;
    for (HColumnDescriptor hcd : htd.getColumnFamilies()) {
      checkEncryption(conf, hcd);
    }
  }

  private void checkEncryption(final Configuration conf, final HColumnDescriptor hcd)
  throws IOException {
    if (!this.masterCheckEncryption) return;
    EncryptionTest.testEncryption(conf, hcd.getEncryptionType(), hcd.getEncryptionKey());
  }

  private void checkClassLoading(final Configuration conf, final HTableDescriptor htd)
  throws IOException {
    RegionSplitPolicy.getSplitPolicyClass(htd, conf);
    RegionCoprocessorHost.testTableCoprocessorAttrs(conf, htd);
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
        ProcedurePrepareLatch latch = ProcedurePrepareLatch.createLatch();
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
  public void truncateTable(
      final TableName tableName,
      final boolean preserveSplits,
      final long nonceGroup,
      final long nonce) throws IOException {
    checkInitialized();

    MasterProcedureUtil.submitProcedure(
      new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
      @Override
      protected void run() throws IOException {
        getMaster().getMasterCoprocessorHost().preTruncateTable(tableName);

        LOG.info(getClientIdAuditPrefix() + " truncate " + tableName);

        long procId = submitProcedure(new TruncateTableProcedure(procedureExecutor.getEnvironment(),
            tableName, preserveSplits));
        ProcedureSyncWait.waitForProcedureToComplete(procedureExecutor, procId);

        getMaster().getMasterCoprocessorHost().postTruncateTable(tableName);
      }

      @Override
      protected String getDescription() {
        return "TruncateTableProcedure";
      }
    });
  }

  @Override
  public void addColumn(
      final TableName tableName,
      final HColumnDescriptor columnDescriptor,
      final long nonceGroup,
      final long nonce)
      throws IOException {
    checkInitialized();
    checkCompression(columnDescriptor);
    checkEncryption(conf, columnDescriptor);
    checkReplicationScope(columnDescriptor);

    MasterProcedureUtil.submitProcedure(
      new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
      @Override
      protected void run() throws IOException {
        if (getMaster().getMasterCoprocessorHost().preAddColumn(tableName, columnDescriptor)) {
          return;
        }

        // Execute the operation synchronously, wait for the operation to complete before continuing
        long procId = submitProcedure(new AddColumnFamilyProcedure(
          procedureExecutor.getEnvironment(), tableName, columnDescriptor));
        ProcedureSyncWait.waitForProcedureToComplete(procedureExecutor, procId);

        getMaster().getMasterCoprocessorHost().postAddColumn(tableName, columnDescriptor);
      }

      @Override
      protected String getDescription() {
        return "AddColumnFamilyProcedure";
      }
    });
  }

  @Override
  public void modifyColumn(
      final TableName tableName,
      final HColumnDescriptor descriptor,
      final long nonceGroup,
      final long nonce)
      throws IOException {
    checkInitialized();
    checkCompression(descriptor);
    checkEncryption(conf, descriptor);
    checkReplicationScope(descriptor);

    MasterProcedureUtil.submitProcedure(
      new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
      @Override
      protected void run() throws IOException {
        if (getMaster().getMasterCoprocessorHost().preModifyColumn(tableName, descriptor)) {
          return;
        }

        LOG.info(getClientIdAuditPrefix() + " modify " + descriptor);

        // Execute the operation synchronously - wait for the operation to complete before continuing.
        long procId = submitProcedure(new ModifyColumnFamilyProcedure(
          procedureExecutor.getEnvironment(), tableName, descriptor));
        ProcedureSyncWait.waitForProcedureToComplete(procedureExecutor, procId);

        getMaster().getMasterCoprocessorHost().postModifyColumn(tableName, descriptor);
      }

      @Override
      protected String getDescription() {
        return "ModifyColumnFamilyProcedure";
      }
    });
  }

  @Override
  public void deleteColumn(
      final TableName tableName,
      final byte[] columnName,
      final long nonceGroup,
      final long nonce)
      throws IOException {
    checkInitialized();

    MasterProcedureUtil.submitProcedure(
      new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
      @Override
      protected void run() throws IOException {
        if (getMaster().getMasterCoprocessorHost().preDeleteColumn(tableName, columnName)) {
          return;
        }

        LOG.info(getClientIdAuditPrefix() + " delete " + Bytes.toString(columnName));

        // Execute the operation synchronously - wait for the operation to complete before
        // continuing.
        long procId = submitProcedure(new DeleteColumnFamilyProcedure(
          procedureExecutor.getEnvironment(), tableName, columnName));
        ProcedureSyncWait.waitForProcedureToComplete(procedureExecutor, procId);

        getMaster().getMasterCoprocessorHost().postDeleteColumn(tableName, columnName);
      }

      @Override
      protected String getDescription() {
        return "DeleteColumnFamilyProcedure";
      }
    });
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

        LOG.info(getClientIdAuditPrefix() + " enable " + tableName);

        // Execute the operation asynchronously - client will check the progress of the operation
        // In case the request is from a <1.1 client before returning,
        // we want to make sure that the table is prepared to be
        // enabled (the table is locked and the table state is set).
        // Note: if the procedure throws exception, we will catch it and rethrow.
        final ProcedurePrepareLatch prepareLatch = ProcedurePrepareLatch.createLatch();
        submitProcedure(new EnableTableProcedure(procedureExecutor.getEnvironment(),
            tableName, false, prepareLatch));
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
        final ProcedurePrepareLatch prepareLatch = ProcedurePrepareLatch.createLatch();
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

  /**
   * Return the region and current deployment for the region containing
   * the given row. If the region cannot be found, returns null. If it
   * is found, but not currently deployed, the second element of the pair
   * may be null.
   */
  @VisibleForTesting // Used by TestMaster.
  Pair<HRegionInfo, ServerName> getTableRegionForRow(
      final TableName tableName, final byte [] rowKey)
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
          if (!pair.getFirst().getTable().equals(tableName)) {
            return false;
          }
          result.set(pair);
          return true;
        }
    };

    MetaScanner.metaScan(clusterConnection, visitor, tableName, rowKey, 1);
    return result.get();
  }

  @Override
  public void modifyTable(
      final TableName tableName,
      final HTableDescriptor descriptor,
      final long nonceGroup,
      final long nonce)
      throws IOException {
    checkInitialized();
    sanityCheckTableDescriptor(descriptor);

    MasterProcedureUtil.submitProcedure(
      new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
      @Override
      protected void run() throws IOException {
        getMaster().getMasterCoprocessorHost().preModifyTable(tableName, descriptor);

        LOG.info(getClientIdAuditPrefix() + " modify " + tableName);

        // Execute the operation synchronously - wait for the operation completes before continuing.
        long procId = submitProcedure(new ModifyTableProcedure(
          procedureExecutor.getEnvironment(), descriptor));
        ProcedureSyncWait.waitForProcedureToComplete(procedureExecutor, procId);

        getMaster().getMasterCoprocessorHost().postModifyTable(tableName, descriptor);
      }

      @Override
      protected String getDescription() {
        return "ModifyTableProcedure";
      }
    });
  }

  @Override
  public void checkTableModifiable(final TableName tableName)
      throws IOException, TableNotFoundException, TableNotDisabledException {
    if (isCatalogTable(tableName)) {
      throw new IOException("Can't modify catalog tables");
    }
    if (!MetaTableAccessor.tableExists(getConnection(), tableName)) {
      throw new TableNotFoundException(tableName);
    }
    if (!getAssignmentManager().getTableStateManager().
        isTableState(tableName, ZooKeeperProtos.Table.State.DISABLED)) {
      throw new TableNotDisabledException(tableName);
    }
  }

  public ClusterStatus getClusterStatus() throws IOException {
    if (cpHost != null) {
      cpHost.preGetClusterStatus();
    }
    ClusterStatus status = getClusterStatusWithoutCoprocessor();
    if (cpHost != null) {
      cpHost.postGetClusterStatus(status);
    }
    return status;
  }

  /**
   * @return cluster status
   */
  public ClusterStatus getClusterStatusWithoutCoprocessor() throws InterruptedIOException {
    // Build Set of backup masters from ZK nodes
    List<String> backupMasterStrings;
    try {
      backupMasterStrings = ZKUtil.listChildrenNoWatch(this.zooKeeper,
        this.zooKeeper.backupMasterAddressesZNode);
    } catch (KeeperException e) {
      LOG.warn(this.zooKeeper.prefix("Unable to list backup servers"), e);
      backupMasterStrings = null;
    }

    List<ServerName> backupMasters = null;
    if (backupMasterStrings != null && !backupMasterStrings.isEmpty()) {
      backupMasters = new ArrayList<ServerName>(backupMasterStrings.size());
      for (String s: backupMasterStrings) {
        try {
          byte [] bytes;
          try {
            bytes = ZKUtil.getData(this.zooKeeper, ZKUtil.joinZNode(
                this.zooKeeper.backupMasterAddressesZNode, s));
          } catch (InterruptedException e) {
            throw new InterruptedIOException();
          }
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
        @Override
        public int compare(ServerName s1, ServerName s2) {
          return s1.getServerName().compareTo(s2.getServerName());
        }});
    }

    String clusterId = fileSystemManager != null ?
      fileSystemManager.getClusterId().toString() : null;
    Set<RegionState> regionsInTransition = assignmentManager != null ?
      assignmentManager.getRegionStates().getRegionsInTransition() : null;
    String[] coprocessors = cpHost != null ? getMasterCoprocessors() : null;
    boolean balancerOn = loadBalancerTracker != null ?
      loadBalancerTracker.isBalancerOn() : false;
    Map<ServerName, ServerLoad> onlineServers = null;
    Set<ServerName> deadServers = null;
    if (serverManager != null) {
      deadServers = serverManager.getDeadServers().copyServerNames();
      onlineServers = serverManager.getOnlineServers();
    }
    return new ClusterStatus(VersionInfo.getVersion(), clusterId,
      onlineServers, deadServers, serverName, backupMasters,
      regionsInTransition, coprocessors, balancerOn);
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
    return procedureStore != null ? procedureStore.getActiveLogs().size() : 0;
  }

  public WALProcedureStore getWalProcedureStore() {
    return procedureStore;
  }

  public int getRegionServerInfoPort(final ServerName sn) {
    RegionServerInfo info = this.regionServerTracker.getRegionServerInfo(sn);
    if (info == null || info.getInfoPort() == 0) {
      return conf.getInt(HConstants.REGIONSERVER_INFO_PORT,
        HConstants.DEFAULT_REGIONSERVER_INFOPORT);
    }
    return info.getInfoPort();
  }

  @Override
  public String getRegionServerVersion(final ServerName sn) {
    RegionServerInfo info = this.regionServerTracker.getRegionServerInfo(sn);
    if (info != null && info.hasVersionInfo()) {
      return info.getVersionInfo().getVersion();
    }
    return "0.0.0"; //Lowest version to prevent move system region to unknown version RS.
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
  public void abort(final String msg, final Throwable t) {
    if (isAborted() || isStopped()) {
      return;
    }
    if (cpHost != null) {
      // HBASE-4014: dump a list of loaded coprocessors.
      LOG.fatal("Master server abort: loaded coprocessors are: " +
          getLoadedCoprocessors());
    }
    if (t != null) {
      LOG.fatal(msg, t);
    } else {
      LOG.fatal(msg);
    }

    try {
      stopMaster();
    } catch (IOException e) {
      LOG.error("Exception occurred while stopping master", e);
    }
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
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

  public MemoryBoundedLogMessageBuffer getRegionServerFatalLogBuffer() {
    return rsFatals;
  }

  public void shutdown() throws IOException {
    if (cpHost != null) {
      cpHost.preShutdown();
    }

    if (this.serverManager != null) {
      this.serverManager.shutdownCluster();
    }
    if (this.clusterStatusTracker != null){
      try {
        this.clusterStatusTracker.setClusterDown();
      } catch (KeeperException e) {
        LOG.error("ZooKeeper exception trying to set cluster as down in ZK", e);
      }
    }
  }

  public void stopMaster() throws IOException {
    if (cpHost != null) {
      cpHost.preStopMaster();
    }
    stop("Stopped by " + Thread.currentThread().getName());
  }

  @VisibleForTesting
  protected void checkServiceStarted() throws ServerNotRunningYetException {
    if (!serviceStarted) {
      throw new ServerNotRunningYetException("Server is not running yet");
    }
  }

  void checkInitialized() throws PleaseHoldException, ServerNotRunningYetException {
    checkServiceStarted();
    if (!isInitialized()) {
      throw new PleaseHoldException("Master is initializing");
    }
  }

  void checkNamespaceManagerReady() throws IOException {
    checkInitialized();
    if (tableNamespaceManager == null ||
        !tableNamespaceManager.isTableAvailableAndInitialized(true)) {
      throw new IOException("Table Namespace Manager not ready yet, try again later");
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
  public boolean isInMaintenanceMode() throws IOException {
    if (!isInitialized()) {
      throw new PleaseHoldException("Master is initializing");
    }
    return maintenanceModeTracker.isInMaintenanceMode();
  }

  @VisibleForTesting
  public void setInitialized(boolean isInitialized) {
    procedureExecutor.getEnvironment().setEventReady(initialized, isInitialized);
  }

  public ProcedureEvent getInitializedEvent() {
    return initialized;
  }

  /**
   * ServerCrashProcessingEnabled is set false before completing assignMeta to prevent processing
   * of crashed servers.
   * @return true if assignMeta has completed;
   */
  @Override
  public boolean isServerCrashProcessingEnabled() {
    return serverCrashProcessingEnabled.isReady();
  }

  @VisibleForTesting
  public void setServerCrashProcessingEnabled(final boolean b) {
    procedureExecutor.getEnvironment().setEventReady(serverCrashProcessingEnabled, b);
  }

  public ProcedureEvent getServerCrashProcessingEnabledEvent() {
    return serverCrashProcessingEnabled;
  }

  /**
   * Report whether this master has started initialization and is about to do meta region assignment
   * @return true if master is in initialization &amp; about to assign hbase:meta regions
   */
  public boolean isInitializationStartsMetaRegionAssignment() {
    return this.initializationBeforeMetaAssignment;
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
   * @param conf
   * @return HMaster instance.
   */
  public static HMaster constructMaster(Class<? extends HMaster> masterClass,
      final Configuration conf, final CoordinatedStateManager cp)  {
    try {
      Constructor<? extends HMaster> c =
        masterClass.getConstructor(Configuration.class, CoordinatedStateManager.class);
      return c.newInstance(conf, cp);
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
  public void createNamespace(
      final NamespaceDescriptor descriptor,
      final long nonceGroup,
      final long nonce) throws IOException {
    TableName.isLegalNamespaceName(Bytes.toBytes(descriptor.getName()));
    checkNamespaceManagerReady();

    createNamespaceSync(descriptor, nonceGroup, nonce, true);
  }

  @Override
  public void createNamespaceSync(
      final NamespaceDescriptor descriptor,
      final long nonceGroup,
      final long nonce,
      final boolean executeCoprocessor) throws IOException {
    MasterProcedureUtil.submitProcedure(
      new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
      @Override
      protected void run() throws IOException {
        if (executeCoprocessor &&
            getMaster().getMasterCoprocessorHost().preCreateNamespace(descriptor)) {
          return;
        }
        LOG.info(getClientIdAuditPrefix() + " creating " + descriptor);
        // Execute the operation synchronously - wait for the operation to complete before
        // continuing.
        long procId = submitProcedure(new CreateNamespaceProcedure(
          procedureExecutor.getEnvironment(), descriptor));
        ProcedureSyncWait.waitForProcedureToComplete(procedureExecutor, procId);
        if (executeCoprocessor) {
          getMaster().getMasterCoprocessorHost().postCreateNamespace(descriptor);
        }
      }

      @Override
      protected String getDescription() {
        return "CreateNamespaceProcedure";
      }
    });
  }

  @Override
  public void modifyNamespace(
      final NamespaceDescriptor descriptor,
      final long nonceGroup,
      final long nonce) throws IOException {
    TableName.isLegalNamespaceName(Bytes.toBytes(descriptor.getName()));
    checkNamespaceManagerReady();

    MasterProcedureUtil.submitProcedure(
      new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
      @Override
      protected void run() throws IOException {
        if (getMaster().getMasterCoprocessorHost().preModifyNamespace(descriptor)) {
          return;
        }
        LOG.info(getClientIdAuditPrefix() + " modify " + descriptor);
        // Execute the operation synchronously - wait for the operation to complete before
        // continuing.
        long procId = submitProcedure(new ModifyNamespaceProcedure(
          procedureExecutor.getEnvironment(), descriptor));
        ProcedureSyncWait.waitForProcedureToComplete(procedureExecutor, procId);
        getMaster().getMasterCoprocessorHost().postModifyNamespace(descriptor);
    }

    @Override
    protected String getDescription() {
      return "ModifyNamespace";
    }
  });
  }

  @Override
  public void deleteNamespace(
      final String name,
      final long nonceGroup,
      final long nonce) throws IOException {
    checkNamespaceManagerReady();

    MasterProcedureUtil.submitProcedure(
      new MasterProcedureUtil.NonceProcedureRunnable(this, nonceGroup, nonce) {
      @Override
      protected void run() throws IOException {
        if (getMaster().getMasterCoprocessorHost().preDeleteNamespace(name)) {
          return;
        }
        LOG.info(getClientIdAuditPrefix() + " delete " + name);
        // Execute the operation synchronously - wait for the operation to complete before
        // continuing.
        long procId = submitProcedure(new DeleteNamespaceProcedure(
          procedureExecutor.getEnvironment(), name));
        ProcedureSyncWait.waitForProcedureToComplete(procedureExecutor, procId);
        getMaster().getMasterCoprocessorHost().postDeleteNamespace(name);
      }

      @Override
      protected String getDescription() {
        return "DeleteNamespaceProcedure";
      }
    });
  }

  /**
   * Ensure that the specified namespace exists, otherwise throws a NamespaceNotFoundException
   *
   * @param name the namespace to check
   * @throws IOException if the namespace manager is not ready yet.
   * @throws NamespaceNotFoundException if the namespace does not exists
   */
  protected void ensureNamespaceExists(final String name)
      throws IOException, NamespaceNotFoundException {
    checkNamespaceManagerReady();
    NamespaceDescriptor nsd = tableNamespaceManager.get(name);
    if (nsd == null) {
      throw new NamespaceNotFoundException(name);
    }
  }

  @Override
  public NamespaceDescriptor getNamespaceDescriptor(String name) throws IOException {
    checkNamespaceManagerReady();

    if (cpHost != null) {
      cpHost.preGetNamespaceDescriptor(name);
    }

    NamespaceDescriptor nsd = tableNamespaceManager.get(name);
    if (nsd == null) {
      throw new NamespaceNotFoundException(name);
    }

    if (cpHost != null) {
      cpHost.postGetNamespaceDescriptor(nsd);
    }

    return nsd;
  }

  @Override
  public List<String> listNamespaces() throws IOException {
    checkNamespaceManagerReady();
    List<String> namespaces = new ArrayList<>();
    boolean bypass = false;
    if (cpHost != null) {
      bypass = cpHost.preListNamespaces(namespaces);
    }
    if (!bypass) {
      for (NamespaceDescriptor namespace : tableNamespaceManager.list()) {
        namespaces.add(namespace.getName());
      }
    }
    if (cpHost != null) {
      cpHost.postListNamespaces(namespaces);
    }
    return namespaces;
  }

  @Override
  public List<NamespaceDescriptor> listNamespaceDescriptors() throws IOException {
    checkNamespaceManagerReady();

    final List<NamespaceDescriptor> descriptors = new ArrayList<NamespaceDescriptor>();
    boolean bypass = false;
    if (cpHost != null) {
      bypass = cpHost.preListNamespaceDescriptors(descriptors);
    }

    if (!bypass) {
      descriptors.addAll(tableNamespaceManager.list());

      if (cpHost != null) {
        cpHost.postListNamespaceDescriptors(descriptors);
      }
    }
    return descriptors;
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
  public List<ProcedureInfo> listProcedures() throws IOException {
    if (cpHost != null) {
      cpHost.preListProcedures();
    }

    final List<ProcedureInfo> procInfoList = this.procedureExecutor.listProcedures();

    if (cpHost != null) {
      cpHost.postListProcedures(procInfoList);
    }

    return procInfoList;
  }

  @Override
  public List<HTableDescriptor> listTableDescriptorsByNamespace(String name) throws IOException {
    ensureNamespaceExists(name);
    return listTableDescriptors(name, null, null, true);
  }

  @Override
  public List<TableName> listTableNamesByNamespace(String name) throws IOException {
    ensureNamespaceExists(name);
    return listTableNames(name, null, true);
  }

  /**
   * Returns the list of table descriptors that match the specified request
   *
   * @param namespace the namespace to query, or null if querying for all
   * @param regex The regular expression to match against, or null if querying for all
   * @param tableNameList the list of table names, or null if querying for all
   * @param includeSysTables False to match only against userspace tables
   * @return the list of table descriptors
   */
  public List<HTableDescriptor> listTableDescriptors(final String namespace, final String regex,
      final List<TableName> tableNameList, final boolean includeSysTables)
      throws IOException {
    final List<HTableDescriptor> descriptors = new ArrayList<HTableDescriptor>();

    boolean bypass = false;
    if (cpHost != null) {
      bypass = cpHost.preGetTableDescriptors(tableNameList, descriptors);
      // method required for AccessController.
      bypass |= cpHost.preGetTableDescriptors(tableNameList, descriptors, regex);
    }

    if (!bypass) {
      if (tableNameList == null || tableNameList.size() == 0) {
        // request for all TableDescriptors
        Collection<HTableDescriptor> htds;
        if (namespace != null && namespace.length() > 0) {
          htds = tableDescriptors.getByNamespace(namespace).values();
        } else {
          htds = tableDescriptors.getAll().values();
        }

        for (HTableDescriptor desc: htds) {
          if (includeSysTables || !desc.getTableName().isSystemTable()) {
            descriptors.add(desc);
          }
        }
      } else {
        for (TableName s: tableNameList) {
          HTableDescriptor desc = tableDescriptors.get(s);
          if (desc != null) {
            descriptors.add(desc);
          }
        }
      }

      // Retains only those matched by regular expression.
      if (regex != null) {
        filterTablesByRegex(descriptors, Pattern.compile(regex));
      }

      if (cpHost != null) {
        cpHost.postGetTableDescriptors(descriptors);
        // method required for AccessController.
        cpHost.postGetTableDescriptors(tableNameList, descriptors, regex);
      }
    }
    return descriptors;
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
    final List<HTableDescriptor> descriptors = new ArrayList<HTableDescriptor>();

    boolean bypass = false;
    if (cpHost != null) {
      bypass = cpHost.preGetTableNames(descriptors, regex);
    }

    if (!bypass) {
      // get all descriptors
      Collection<HTableDescriptor> htds;
      if (namespace != null && namespace.length() > 0) {
        htds = tableDescriptors.getByNamespace(namespace).values();
      } else {
        htds = tableDescriptors.getAll().values();
      }

      for (HTableDescriptor htd: htds) {
        if (includeSysTables || !htd.getTableName().isSystemTable()) {
          descriptors.add(htd);
        }
      }

      // Retains only those matched by regular expression.
      if (regex != null) {
        filterTablesByRegex(descriptors, Pattern.compile(regex));
      }

      if (cpHost != null) {
        cpHost.postGetTableNames(descriptors, regex);
      }
    }

    List<TableName> result = new ArrayList<TableName>(descriptors.size());
    for (HTableDescriptor htd: descriptors) {
      result.add(htd.getTableName());
    }
    return result;
  }


  /**
   * Removes the table descriptors that don't match the pattern.
   * @param descriptors list of table descriptors to filter
   * @param pattern the regex to use
   */
  private static void filterTablesByRegex(final Collection<HTableDescriptor> descriptors,
      final Pattern pattern) {
    final String defaultNS = NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR;
    Iterator<HTableDescriptor> itr = descriptors.iterator();
    while (itr.hasNext()) {
      HTableDescriptor htd = itr.next();
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
    return getClusterStatusWithoutCoprocessor().getLastMajorCompactionTsForTable(table);
  }

  @Override
  public long getLastMajorCompactionTimestampForRegion(byte[] regionName) throws IOException {
    return getClusterStatusWithoutCoprocessor().getLastMajorCompactionTsForRegion(regionName);
  }

  /**
   * Queries the state of the {@link LoadBalancerTracker}. If the balancer is not initialized,
   * false is returned.
   *
   * @return The state of the load balancer, or false if the load balancer isn't defined.
   */
  public boolean isBalancerOn() {
    try {
      if (null == loadBalancerTracker || isInMaintenanceMode()) {
        return false;
      }
    } catch (IOException e) {
      return false;
    }
    return loadBalancerTracker.isBalancerOn();
  }

  /**
   * Queries the state of the {@link RegionNormalizerTracker}. If it's not initialized,
   * false is returned.
   */
  public boolean isNormalizerOn() {
    try {
      return (null == regionNormalizerTracker || isInMaintenanceMode()) ?
          false: regionNormalizerTracker.isNormalizerOn();
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Queries the state of the {@link SplitOrMergeTracker}. If it is not initialized,
   * false is returned. If switchType is illegal, false will return.
   * @param switchType see {@link org.apache.hadoop.hbase.client.Admin.MasterSwitchType}
   * @return The state of the switch
   */
  public boolean isSplitOrMergeEnabled(Admin.MasterSwitchType switchType) {
    try {
      if (null == splitOrMergeTracker || isInMaintenanceMode()) {
        return false;
      }
    } catch (IOException e) {
      return false;
    }
    return splitOrMergeTracker.isSplitOrMergeEnabled(switchType);
  }

  /**
   * Fetch the configured {@link LoadBalancer} class name. If none is set, a default is returned.
   *
   * @return The name of the {@link LoadBalancer} in use.
   */
  public String getLoadBalancerClassName() {
    return conf.get(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, LoadBalancerFactory
        .getDefaultLoadBalancerClass().getName());
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
  public LoadBalancer getLoadBalancer() {
    return balancer;
  }

  public HashMap<String, List<Pair<ServerName, ReplicationLoadSource>>>
    getReplicationLoad(ServerName[] serverNames) {
    HashMap<String, List<Pair<ServerName, ReplicationLoadSource>>> replicationLoadSourceMap =
        new HashMap<>();
    for (ServerName serverName : serverNames) {
      List<ReplicationLoadSource> replicationLoadSources =
        getServerManager().getLoad(serverName).getReplicationLoadSourceList();
      for (ReplicationLoadSource replicationLoadSource : replicationLoadSources) {
        List<Pair<ServerName, ReplicationLoadSource>> list =
          replicationLoadSourceMap.get(replicationLoadSource.getPeerID());
        if (list == null) {
          list = new ArrayList<Pair<ServerName, ReplicationLoadSource>>();
          replicationLoadSourceMap.put(replicationLoadSource.getPeerID(), list);
        }
        list.add(new Pair<>(serverName, replicationLoadSource));
      }
    }
    return replicationLoadSourceMap;
  }
}
