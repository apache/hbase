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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.MasterRpcServices.BalanceSwitchMode;
import org.apache.hadoop.hbase.master.balancer.BalancerChore;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.master.balancer.ClusterStatusChore;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.master.cleaner.LogCleaner;
import org.apache.hadoop.hbase.master.handler.DisableTableHandler;
import org.apache.hadoop.hbase.master.handler.DispatchMergingRegionHandler;
import org.apache.hadoop.hbase.master.handler.EnableTableHandler;
import org.apache.hadoop.hbase.master.handler.ModifyTableHandler;
import org.apache.hadoop.hbase.master.handler.TableAddFamilyHandler;
import org.apache.hadoop.hbase.master.handler.TableDeleteFamilyHandler;
import org.apache.hadoop.hbase.master.handler.TableModifyFamilyHandler;
import org.apache.hadoop.hbase.master.handler.TruncateTableHandler;
import org.apache.hadoop.hbase.master.procedure.CreateTableProcedure;
import org.apache.hadoop.hbase.master.procedure.DeleteTableProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.monitoring.MemoryBoundedLogMessageBuffer;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.procedure.MasterProcedureManagerHost;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.procedure.flush.MasterFlushTableProcedureManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionServerInfo;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.quotas.MasterQuotaManager;
import org.apache.hadoop.hbase.quotas.RegionStateListener;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionSplitPolicy;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CompressionTest;
import org.apache.hadoop.hbase.util.EncryptionTest;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.util.ZKDataMigrator;
import org.apache.hadoop.hbase.zookeeper.DrainingServerTracker;
import org.apache.hadoop.hbase.zookeeper.LoadBalancerTracker;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.RegionServerTracker;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;

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

  /** Namespace stuff */
  private TableNamespaceManager tableNamespaceManager;

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
  private volatile boolean isActiveMaster = false;

  // flag set after we complete initialization once active,
  // it is not private since it's used in unit tests
  volatile boolean initialized = false;

  // flag set after master services are started,
  // initialization may have not completed yet.
  volatile boolean serviceStarted = false;

  // flag set after we complete assignMeta.
  private volatile boolean serverShutdownHandlerEnabled = false;

  LoadBalancer balancer;
  private BalancerChore balancerChore;
  private ClusterStatusChore clusterStatusChore;
  private ClusterStatusPublisher clusterStatusPublisherChore = null;

  CatalogJanitor catalogJanitorChore;
  private LogCleaner logCleaner;
  private HFileCleaner hfileCleaner;

  MasterCoprocessorHost cpHost;

  private final boolean preLoadTableDescriptors;

  // Time stamps for when a hmaster became active
  private long masterActiveTime;

  //should we check the compression codec type at master side, default true, HBASE-6370
  private final boolean masterCheckCompression;

  //should we check encryption settings at master side, default true
  private final boolean masterCheckEncryption;

  Map<String, Service> coprocessorServiceHandlers = Maps.newHashMap();

  // monitor for snapshot of hbase tables
  SnapshotManager snapshotManager;
  // monitor for distributed procedures
  MasterProcedureManagerHost mpmHost;

  // it is assigned after 'initialized' guard set to true, so should be volatile
  private volatile MasterQuotaManager quotaManager;

  private ProcedureExecutor<MasterProcedureEnv> procedureExecutor;
  private WALProcedureStore procedureStore;

  // handle table states
  private TableStateManager tableStateManager;

  /** flag used in test cases in order to simulate RS failures during master initialization */
  private volatile boolean initializationBeforeMetaAssignment = false;

  /** jetty server for master to redirect requests to regionserver infoServer */
  private org.mortbay.jetty.Server masterJettyServer;

  public static class RedirectServlet extends HttpServlet {
    private static final long serialVersionUID = 2894774810058302472L;
    private static int regionServerInfoPort;

    @Override
    public void doGet(HttpServletRequest request,
        HttpServletResponse response) throws ServletException, IOException {
      String redirectUrl = request.getScheme() + "://"
        + request.getServerName() + ":" + regionServerInfoPort
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
   * #finishActiveMasterInitialization(MonitoredTask) after
   * the master becomes the active one.
   *
   * @throws KeeperException
   * @throws IOException
   */
  public HMaster(final Configuration conf, CoordinatedStateManager csm)
      throws IOException, KeeperException {
    super(conf, csm);
    this.rsFatals = new MemoryBoundedLogMessageBuffer(
      conf.getLong("hbase.master.buffer.for.rs.fatals", 1*1024*1024));

    LOG.info("hbase.rootdir=" + FSUtils.getRootDir(this.conf) +
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

    this.metricsMaster = new MetricsMaster( new MetricsMasterWrapperImpl(this));

    // preload table descriptor at startup
    this.preLoadTableDescriptors = conf.getBoolean("hbase.master.preload.tabledescriptors", true);

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
    activeMasterManager = new ActiveMasterManager(zooKeeper, this.serverName, this);
    int infoPort = putUpJettyServer();
    startActiveMasterManager(infoPort);
  }

  // return the actual infoPort, -1 means disable info server.
  private int putUpJettyServer() throws IOException {
    if (!conf.getBoolean("hbase.master.infoserver.redirect", true)) {
      return -1;
    }
    int infoPort = conf.getInt("hbase.master.info.port.orig",
      HConstants.DEFAULT_MASTER_INFOPORT);
    // -1 is for disabling info server, so no redirecting
    if (infoPort < 0 || infoServer == null) {
      return -1;
    }
    String addr = conf.get("hbase.master.info.bindAddress", "0.0.0.0");
    if (!Addressing.isLocalAddress(InetAddress.getByName(addr))) {
      String msg =
          "Failed to start redirecting jetty server. Address " + addr
              + " does not belong to this host. Correct configuration parameter: "
              + "hbase.master.info.bindAddress";
      LOG.error(msg);
      throw new IOException(msg);
    }

    RedirectServlet.regionServerInfoPort = infoServer.getPort();
    masterJettyServer = new org.mortbay.jetty.Server();
    Connector connector = new SelectChannelConnector();
    connector.setHost(addr);
    connector.setPort(infoPort);
    masterJettyServer.addConnector(connector);
    masterJettyServer.setStopAtShutdown(true);
    Context context = new Context(masterJettyServer, "/", Context.NO_SESSIONS);
    context.addServlet(RedirectServlet.class, "/*");
    try {
      masterJettyServer.start();
    } catch (Exception e) {
      throw new IOException("Failed to start redirecting jetty server", e);
    }
    return connector.getLocalPort();
  }

  @Override
  protected TableDescriptors getFsTableDescriptors() throws IOException {
    return super.getFsTableDescriptors();
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
    while (!(tablesOnMaster && isActiveMaster)
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
  @Override
  protected void doMetrics() {
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
    this.loadBalancerTracker = new LoadBalancerTracker(zooKeeper, this);
    this.loadBalancerTracker.start();
    this.assignmentManager = new AssignmentManager(this, serverManager,
      this.balancer, this.service, this.metricsMaster,
      this.tableLockManager, tableStateManager);

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
   * <li>Ensure assignment of meta/namespace regions<li>
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

    isActiveMaster = true;
    Thread zombieDetector = new Thread(new InitializationMonitor(this));
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
    this.serverManager = createServerManager(this, this);

    setupClusterConnection();

    // Invalidate all write locks held previously
    this.tableLockManager.reapWriteLocks();
    this.tableStateManager = new TableStateManager(this);

    status.setStatus("Initializing ZK system trackers");
    initializeZKBasedSystemTrackers();

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
    Set<ServerName> previouslyFailedServers = this.fileSystemManager
        .getFailedServersFromLogFolders();

    // remove stale recovering regions from previous run
    this.fileSystemManager.removeStaleRecoveringRegionsFromZK(previouslyFailedServers);

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
    this.balancer.setClusterStatus(getClusterStatus());
    this.balancer.setMasterServices(this);
    this.balancer.initialize();

    // Check if master is shutting down because of some issue
    // in initializing the regionserver or the balancer.
    if(isStopped()) return;

    // Make sure meta assigned before proceeding.
    status.setStatus("Assigning Meta Region");
    assignMeta(status, previouslyFailedMetaRSs, HRegionInfo.DEFAULT_REPLICA_ID);
    // check if master is shutting down because above assignMeta could return even hbase:meta isn't
    // assigned when master is shutting down
    if(isStopped()) return;

    // migrating existent table state from zk, so splitters
    // and recovery process treat states properly.
    for (Map.Entry<TableName, TableState.State> entry : ZKDataMigrator
        .queryForTableStates(getZooKeeper()).entrySet()) {
      LOG.info("Converting state from zk to new states:" + entry);
      tableStateManager.setTableState(entry.getKey(), entry.getValue());
    }
    ZKUtil.deleteChildrenRecursively(getZooKeeper(), getZooKeeper().tableZNode);

    status.setStatus("Submitting log splitting work for previously failed region servers");
    // Master has recovered hbase:meta region server and we put
    // other failed region servers in a queue to be handled later by SSH
    for (ServerName tmpServer : previouslyFailedServers) {
      this.serverManager.processDeadServer(tmpServer, true);
    }

    // Fix up assignment manager status
    status.setStatus("Starting assignment manager");
    this.assignmentManager.joinCluster();

    //set cluster status again after user regions are assigned
    this.balancer.setClusterStatus(getClusterStatus());

    // Start balancer and meta catalog janitor after meta and regions have
    // been assigned.
    status.setStatus("Starting balancer and catalog janitor");
    this.clusterStatusChore = new ClusterStatusChore(this, balancer);
    getChoreService().scheduleChore(clusterStatusChore);
    this.balancerChore = new BalancerChore(this);
    getChoreService().scheduleChore(balancerChore);
    this.catalogJanitorChore = new CatalogJanitor(this, this);
    getChoreService().scheduleChore(catalogJanitorChore);

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
    LOG.info("Master has completed initialization");
    configurationManager.registerObserver(this.balancer);
    initialized = true;
    // assign the meta replicas
    Set<ServerName> EMPTY_SET = new HashSet<ServerName>();
    int numReplicas = conf.getInt(HConstants.META_REPLICAS_NUM,
           HConstants.DEFAULT_META_REPLICA_NUM);
    for (int i = 1; i < numReplicas; i++) {
      assignMeta(status, EMPTY_SET, i);
    }
    unassignExcessMetaReplica(zooKeeper, numReplicas);

    status.setStatus("Starting quota manager");
    initQuotaManager();

    // clear the dead servers with same host name and port of online server because we are not
    // removing dead server with same hostname and port of rs which is trying to check in before
    // master initialization. See HBASE-5916.
    this.serverManager.clearDeadServersWithSameHostNameAndPortOfOnlineServer();

    if (this.cpHost != null) {
      // don't let cp initialization errors kill the master
      try {
        this.cpHost.postStartMaster();
      } catch (IOException ioe) {
        LOG.error("Coprocessor postStartMaster() hook failed", ioe);
      }
    }

    zombieDetector.interrupt();
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
    RegionState metaState = MetaTableLocator.getMetaRegionState(getZooKeeper(), replicaId);
    HRegionInfo hri = RegionReplicaUtil.getRegionInfoForReplica(HRegionInfo.FIRST_META_REGIONINFO,
        replicaId);
    RegionStates regionStates = assignmentManager.getRegionStates();
    regionStates.createRegionState(hri, metaState.getState(),
        metaState.getServerName(), null);

    if (!metaState.isOpened() || !metaTableLocator.verifyMetaRegionLocation(
        this.getConnection(), this.getZooKeeper(), timeout, replicaId)) {
      ServerName currentMetaServer = metaState.getServerName();
      if (serverManager.isServerOnline(currentMetaServer)) {
        if (replicaId == HRegionInfo.DEFAULT_REPLICA_ID) {
          LOG.info("Meta was in transition on " + currentMetaServer);
        } else {
          LOG.info("Meta with replicaId " + replicaId + " was in transition on " +
                    currentMetaServer);
        }
        assignmentManager.processRegionsInTransition(Arrays.asList(metaState));
      } else {
        if (currentMetaServer != null) {
          if (replicaId == HRegionInfo.DEFAULT_REPLICA_ID) {
            splitMetaLogBeforeAssignment(currentMetaServer);
            regionStates.logSplit(HRegionInfo.FIRST_META_REGIONINFO);
            previouslyFailedMetaRSs.add(currentMetaServer);
          }
        }
        LOG.info("Re-assigning hbase:meta with replicaId, " + replicaId +
            " it was on " + currentMetaServer);
        assignmentManager.assignMeta(hri);
      }
      assigned++;
    }

    if (replicaId == HRegionInfo.DEFAULT_REPLICA_ID)
      getTableStateManager().setTableState(TableName.META_TABLE_NAME, TableState.State.ENABLED);
    // TODO: should we prevent from using state manager before meta was initialized?
    // tableStateManager.start();

    if ((RecoveryMode.LOG_REPLAY == this.getMasterFileSystem().getLogRecoveryMode())
        && (!previouslyFailedMetaRSs.isEmpty())) {
      // replay WAL edits mode need new hbase:meta RS is assigned firstly
      status.setStatus("replaying log for Meta Region");
      this.fileSystemManager.splitMetaLog(previouslyFailedMetaRSs);
    }

    this.assignmentManager.setEnabledTable(TableName.META_TABLE_NAME);
    tableStateManager.start();

    // Make sure a hbase:meta location is set. We need to enable SSH here since
    // if the meta region server is died at this time, we need it to be re-assigned
    // by SSH so that system tables can be assigned.
    // No need to wait for meta is assigned = 0 when meta is just verified.
    if (replicaId == HRegionInfo.DEFAULT_REPLICA_ID) enableServerShutdownHandler(assigned != 0);
    LOG.info("hbase:meta with replicaId " + replicaId + " assigned=" + assigned + ", location="
      + metaTableLocator.getMetaRegionLocation(this.getZooKeeper(), replicaId));
    status.setStatus("META assigned.");
  }

  void initNamespace() throws IOException {
    //create namespace manager
    tableNamespaceManager = new TableNamespaceManager(this);
    tableNamespaceManager.start();
  }

  void initQuotaManager() throws IOException {
    MasterQuotaManager quotaManager = new MasterQuotaManager(this);
    this.assignmentManager.setRegionStateListener((RegionStateListener)quotaManager);
    quotaManager.start();
    this.quotaManager = quotaManager;
  }

  boolean isCatalogJanitorEnabled() {
    return catalogJanitorChore != null ?
      catalogJanitorChore.getEnabled() : false;
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

  private void enableServerShutdownHandler(
      final boolean waitForMeta) throws IOException, InterruptedException {
    // If ServerShutdownHandler is disabled, we enable it and expire those dead
    // but not expired servers. This is required so that if meta is assigning to
    // a server which dies after assignMeta starts assignment,
    // SSH can re-assign it. Otherwise, we will be
    // stuck here waiting forever if waitForMeta is specified.
    if (!serverShutdownHandlerEnabled) {
      serverShutdownHandlerEnabled = true;
      this.serverManager.processQueuedDeadServers();
    }

    if (waitForMeta) {
      metaTableLocator.waitMetaRegionLocation(this.getZooKeeper());
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
  private void startServiceThreads() throws IOException{
   // Start the executor service pools
   this.service.startExecutorService(ExecutorType.MASTER_OPEN_REGION,
      conf.getInt("hbase.master.executor.openregion.threads", 5));
   this.service.startExecutorService(ExecutorType.MASTER_CLOSE_REGION,
      conf.getInt("hbase.master.executor.closeregion.threads", 5));
   this.service.startExecutorService(ExecutorType.MASTER_SERVER_OPERATIONS,
      conf.getInt("hbase.master.executor.serverops.threads", 5));
   this.service.startExecutorService(ExecutorType.MASTER_META_SERVER_OPERATIONS,
      conf.getInt("hbase.master.executor.meta.serverops.threads", 5));
   this.service.startExecutorService(ExecutorType.M_LOG_REPLAY_OPS,
      conf.getInt("hbase.master.executor.logreplayops.threads", 10));

   // We depend on there being only one instance of this executor running
   // at a time.  To do concurrency, would need fencing of enable/disable of
   // tables.
   // Any time changing this maxThreads to > 1, pls see the comment at
   // AccessController#postCreateTableHandler
   this.service.startExecutorService(ExecutorType.MASTER_TABLE_OPERATIONS, 1);
   startProcedureExecutor();

   // Start log cleaner thread
   int cleanerInterval = conf.getInt("hbase.master.cleaner.interval", 60 * 1000);
   this.logCleaner =
      new LogCleaner(cleanerInterval,
         this, conf, getMasterFileSystem().getFileSystem(),
         getMasterFileSystem().getOldLogDir());
    getChoreService().scheduleChore(logCleaner);

   //start the hfile archive cleaner thread
    Path archiveDir = HFileArchiveUtil.getArchivePath(conf);
    this.hfileCleaner = new HFileCleaner(cleanerInterval, this, conf, getMasterFileSystem()
        .getFileSystem(), archiveDir);
    getChoreService().scheduleChore(hfileCleaner);
    serviceStarted = true;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Started service threads");
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
    super.stopServiceThreads();
    stopChores();

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
    if (this.logCleaner != null) this.logCleaner.cancel(true);
    if (this.hfileCleaner != null) this.hfileCleaner.cancel(true);
    if (this.quotaManager != null) this.quotaManager.stop();
    if (this.activeMasterManager != null) this.activeMasterManager.stop();
    if (this.serverManager != null) this.serverManager.stop();
    if (this.assignmentManager != null) this.assignmentManager.stop();
    if (this.fileSystemManager != null) this.fileSystemManager.stop();
    if (this.mpmHost != null) this.mpmHost.stop("server shutting down.");
  }

  private void startProcedureExecutor() throws IOException {
    final MasterProcedureEnv procEnv = new MasterProcedureEnv(this);
    final Path logDir = new Path(fileSystemManager.getRootDir(),
        MasterProcedureConstants.MASTER_PROCEDURE_LOGDIR);

    procedureStore = new WALProcedureStore(conf, fileSystemManager.getFileSystem(), logDir,
        new MasterProcedureEnv.WALStoreLeaseRecovery(this));
    procedureStore.registerListener(new MasterProcedureEnv.MasterProcedureStoreListener(this));
    procedureExecutor = new ProcedureExecutor(conf, procEnv, procedureStore,
        procEnv.getProcedureQueue());

    final int numThreads = conf.getInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS,
        Math.max(Runtime.getRuntime().availableProcessors(),
          MasterProcedureConstants.DEFAULT_MIN_MASTER_PROCEDURE_THREADS));
    procedureStore.start(numThreads);
    procedureExecutor.start(numThreads);
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
    if (this.balancerChore != null) {
      this.balancerChore.cancel(true);
    }
    if (this.clusterStatusChore != null) {
      this.clusterStatusChore.cancel(true);
    }
    if (this.catalogJanitorChore != null) {
      this.catalogJanitorChore.cancel(true);
    }
    if (this.clusterStatusPublisherChore != null){
      clusterStatusPublisherChore.cancel(true);
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
  private int getBalancerCutoffTime() {
    int balancerCutoffTime = getConfiguration().getInt("hbase.balancer.max.balancing", -1);
    if (balancerCutoffTime == -1) {
      // if cutoff time isn't set, defaulting it to period time
      int balancerPeriod = getConfiguration().getInt("hbase.balancer.period", 300000);
      balancerCutoffTime = balancerPeriod;
    }
    return balancerCutoffTime;
  }

  public boolean balance() throws IOException {
    // if master not initialized, don't run balancer.
    if (!this.initialized) {
      LOG.debug("Master has not been initialized, don't run balancer.");
      return false;
    }
    // Do this call outside of synchronized block.
    int maximumBalanceTime = getBalancerCutoffTime();
    synchronized (this.balancer) {
      // If balance not true, don't run balancer.
      if (!this.loadBalancerTracker.isBalancerOn()) return false;
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
      this.balancer.setClusterStatus(getClusterStatus());
      for (Map<ServerName, List<HRegionInfo>> assignments : assignmentsByTable.values()) {
        List<RegionPlan> partialPlans = this.balancer.balanceCluster(assignments);
        if (partialPlans != null) plans.addAll(partialPlans);
      }
      long cutoffTime = System.currentTimeMillis() + maximumBalanceTime;
      int rpCount = 0;  // number of RegionPlans balanced so far
      long totalRegPlanExecTime = 0;
      if (plans != null && !plans.isEmpty()) {
        for (RegionPlan plan: plans) {
          LOG.info("balance " + plan);
          long balStartTime = System.currentTimeMillis();
          //TODO: bulk assign
          this.assignmentManager.balance(plan);
          totalRegPlanExecTime += System.currentTimeMillis()-balStartTime;
          rpCount++;
          if (rpCount < plans.size() &&
              // if performing next balance exceeds cutoff time, exit the loop
              (System.currentTimeMillis() + (totalRegPlanExecTime / rpCount)) > cutoffTime) {
            //TODO: After balance, there should not be a cutoff time (keeping it as
            // a security net for now)
            LOG.debug("No more balancing till next balance run; maximumBalanceTime=" +
              maximumBalanceTime);
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
      final HRegionInfo region_b, final boolean forcible) throws IOException {
    checkInitialized();
    this.service.submit(new DispatchMergingRegionHandler(this,
        this.catalogJanitorChore, region_a, region_b, forcible));
  }

  void move(final byte[] encodedRegionName,
      final byte[] destServerName) throws HBaseIOException {
    RegionState regionState = assignmentManager.getRegionStates().
      getRegionState(Bytes.toString(encodedRegionName));
    if (regionState == null) {
      throw new UnknownRegionException(Bytes.toStringBinary(encodedRegionName));
    }

    HRegionInfo hri = regionState.getRegion();
    ServerName dest;
    if (destServerName == null || destServerName.length == 0) {
      LOG.info("Passed destination servername is null/empty so " +
        "choosing a server at random");
      final List<ServerName> destServers = this.serverManager.createDestinationServersList(
        regionState.getServerName());
      dest = balancer.randomAssignment(hri, destServers);
      if (dest == null) {
        LOG.debug("Unable to determine a plan to assign " + hri);
        return;
      }
    } else {
      dest = ServerName.valueOf(Bytes.toString(destServerName));
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
  public long createTable(HTableDescriptor hTableDescriptor,
      byte [][] splitKeys) throws IOException {
    if (isStopped()) {
      throw new MasterNotRunningException();
    }

    String namespace = hTableDescriptor.getTableName().getNamespaceAsString();
    ensureNamespaceExists(namespace);

    HRegionInfo[] newRegions = ModifyRegionUtils.createHRegionInfos(hTableDescriptor, splitKeys);
    checkInitialized();
    sanityCheckTableDescriptor(hTableDescriptor);
    this.quotaManager.checkNamespaceTableAndRegionQuota(hTableDescriptor.getTableName(),
      newRegions.length);
    if (cpHost != null) {
      cpHost.preCreateTable(hTableDescriptor, newRegions);
    }
    LOG.info(getClientIdAuditPrefix() + " create " + hTableDescriptor);

    // TODO: We can handle/merge duplicate requests, and differentiate the case of
    //       TableExistsException by saying if the schema is the same or not.
    ProcedurePrepareLatch latch = ProcedurePrepareLatch.createLatch();
    long procId = this.procedureExecutor.submitProcedure(
      new CreateTableProcedure(procedureExecutor.getEnvironment(),
        hTableDescriptor, newRegions, latch));
    latch.await();

    if (cpHost != null) {
      cpHost.postCreateTable(hTableDescriptor, newRegions);
    }

    return procId;
  }

  /**
   * Checks whether the table conforms to some sane limits, and configured
   * values (compression, etc) work. Throws an exception if something is wrong.
   * @throws IOException
   */
  private void sanityCheckTableDescriptor(final HTableDescriptor htd) throws IOException {
    final String CONF_KEY = "hbase.table.sanity.checks";
    if (!conf.getBoolean(CONF_KEY, true)) {
      return;
    }
    String tableVal = htd.getConfigurationValue(CONF_KEY);
    if (tableVal != null && !Boolean.valueOf(tableVal)) {
      return;
    }

    // check max file size
    long maxFileSizeLowerLimit = 2 * 1024 * 1024L; // 2M is the default lower limit
    long maxFileSize = htd.getMaxFileSize();
    if (maxFileSize < 0) {
      maxFileSize = conf.getLong(HConstants.HREGION_MAX_FILESIZE, maxFileSizeLowerLimit);
    }
    if (maxFileSize < conf.getLong("hbase.hregion.max.filesize.limit", maxFileSizeLowerLimit)) {
      throw new DoNotRetryIOException("MAX_FILESIZE for table descriptor or "
        + "\"hbase.hregion.max.filesize\" (" + maxFileSize
        + ") is too small, which might cause over splitting into unmanageable "
        + "number of regions. Set " + CONF_KEY + " to false at conf or table descriptor "
          + "if you want to bypass sanity checks");
    }

    // check flush size
    long flushSizeLowerLimit = 1024 * 1024L; // 1M is the default lower limit
    long flushSize = htd.getMemStoreFlushSize();
    if (flushSize < 0) {
      flushSize = conf.getLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, flushSizeLowerLimit);
    }
    if (flushSize < conf.getLong("hbase.hregion.memstore.flush.size.limit", flushSizeLowerLimit)) {
      throw new DoNotRetryIOException("MEMSTORE_FLUSHSIZE for table descriptor or "
          + "\"hbase.hregion.memstore.flush.size\" ("+flushSize+") is too small, which might cause"
          + " very frequent flushing. Set " + CONF_KEY + " to false at conf or table descriptor "
          + "if you want to bypass sanity checks");
    }

    // check that coprocessors and other specified plugin classes can be loaded
    try {
      checkClassLoading(conf, htd);
    } catch (Exception ex) {
      throw new DoNotRetryIOException(ex);
    }

    // check compression can be loaded
    try {
      checkCompression(htd);
    } catch (IOException e) {
      throw new DoNotRetryIOException(e.getMessage(), e);
    }

    // check encryption can be loaded
    try {
      checkEncryption(conf, htd);
    } catch (IOException e) {
      throw new DoNotRetryIOException(e.getMessage(), e);
    }

    // check that we have at least 1 CF
    if (htd.getColumnFamilies().length == 0) {
      throw new DoNotRetryIOException("Table should have at least one column family "
          + "Set "+CONF_KEY+" at conf or table descriptor if you want to bypass sanity checks");
    }

    for (HColumnDescriptor hcd : htd.getColumnFamilies()) {
      if (hcd.getTimeToLive() <= 0) {
        throw new DoNotRetryIOException("TTL for column family " + hcd.getNameAsString()
          + "  must be positive. Set " + CONF_KEY + " to false at conf or table descriptor "
          + "if you want to bypass sanity checks");
      }

      // check blockSize
      if (hcd.getBlocksize() < 1024 || hcd.getBlocksize() > 16 * 1024 * 1024) {
        throw new DoNotRetryIOException("Block size for column family " + hcd.getNameAsString()
          + "  must be between 1K and 16MB Set "+CONF_KEY+" to false at conf or table descriptor "
          + "if you want to bypass sanity checks");
      }

      // check versions
      if (hcd.getMinVersions() < 0) {
        throw new DoNotRetryIOException("Min versions for column family " + hcd.getNameAsString()
          + "  must be positive. Set " + CONF_KEY + " to false at conf or table descriptor "
          + "if you want to bypass sanity checks");
      }
      // max versions already being checked

      // check replication scope
      if (hcd.getScope() < 0) {
        throw new DoNotRetryIOException("Replication scope for column family "
          + hcd.getNameAsString() + "  must be positive. Set " + CONF_KEY + " to false at conf "
          + "or table descriptor if you want to bypass sanity checks");
      }

      // TODO: should we check coprocessors and encryption ?
    }
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
                  .contains("org/apache/hadoop/hdfs/protocol/FSConstants$SafeModeAction")) {
            // improved error message for this special case
            abort("HBase is having a problem with its Hadoop jars.  You may need to "
              + "recompile HBase against Hadoop version "
              +  org.apache.hadoop.util.VersionInfo.getVersion()
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
  public long deleteTable(final TableName tableName) throws IOException {
    checkInitialized();
    if (cpHost != null) {
      cpHost.preDeleteTable(tableName);
    }
    LOG.info(getClientIdAuditPrefix() + " delete " + tableName);

    // TODO: We can handle/merge duplicate request
    ProcedurePrepareLatch latch = ProcedurePrepareLatch.createLatch();
    long procId = this.procedureExecutor.submitProcedure(
        new DeleteTableProcedure(procedureExecutor.getEnvironment(), tableName, latch));
    latch.await();

    if (cpHost != null) {
      cpHost.postDeleteTable(tableName);
    }

    return procId;
  }

  @Override
  public void truncateTable(TableName tableName, boolean preserveSplits) throws IOException {
    checkInitialized();
    if (cpHost != null) {
      cpHost.preTruncateTable(tableName);
    }
    LOG.info(getClientIdAuditPrefix() + " truncate " + tableName);
    TruncateTableHandler handler = new TruncateTableHandler(tableName, this, this, preserveSplits);
    handler.prepare();
    handler.process();
    if (cpHost != null) {
      cpHost.postTruncateTable(tableName);
    }
  }

  @Override
  public void addColumn(final TableName tableName, final HColumnDescriptor columnDescriptor)
      throws IOException {
    checkInitialized();
    checkCompression(columnDescriptor);
    checkEncryption(conf, columnDescriptor);
    if (cpHost != null) {
      if (cpHost.preAddColumn(tableName, columnDescriptor)) {
        return;
      }
    }
    //TODO: we should process this (and some others) in an executor
    new TableAddFamilyHandler(tableName, columnDescriptor, this, this).prepare().process();
    if (cpHost != null) {
      cpHost.postAddColumn(tableName, columnDescriptor);
    }
  }

  @Override
  public void modifyColumn(TableName tableName, HColumnDescriptor descriptor)
      throws IOException {
    checkInitialized();
    checkCompression(descriptor);
    checkEncryption(conf, descriptor);
    if (cpHost != null) {
      if (cpHost.preModifyColumn(tableName, descriptor)) {
        return;
      }
    }
    LOG.info(getClientIdAuditPrefix() + " modify " + descriptor);
    new TableModifyFamilyHandler(tableName, descriptor, this, this)
      .prepare().process();
    if (cpHost != null) {
      cpHost.postModifyColumn(tableName, descriptor);
    }
  }

  @Override
  public void deleteColumn(final TableName tableName, final byte[] columnName)
      throws IOException {
    checkInitialized();
    if (cpHost != null) {
      if (cpHost.preDeleteColumn(tableName, columnName)) {
        return;
      }
    }
    LOG.info(getClientIdAuditPrefix() + " delete " + Bytes.toString(columnName));
    new TableDeleteFamilyHandler(tableName, columnName, this, this).prepare().process();
    if (cpHost != null) {
      cpHost.postDeleteColumn(tableName, columnName);
    }
  }

  @Override
  public void enableTable(final TableName tableName) throws IOException {
    checkInitialized();
    if (cpHost != null) {
      cpHost.preEnableTable(tableName);
    }
    LOG.info(getClientIdAuditPrefix() + " enable " + tableName);
    this.service.submit(new EnableTableHandler(this, tableName,
      assignmentManager, tableLockManager, false).prepare());
    if (cpHost != null) {
      cpHost.postEnableTable(tableName);
   }
  }

  @Override
  public void disableTable(final TableName tableName) throws IOException {
    checkInitialized();
    if (cpHost != null) {
      cpHost.preDisableTable(tableName);
    }
    LOG.info(getClientIdAuditPrefix() + " disable " + tableName);
    this.service.submit(new DisableTableHandler(this, tableName,
      assignmentManager, tableLockManager, false).prepare());
    if (cpHost != null) {
      cpHost.postDisableTable(tableName);
    }
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

    MetaTableAccessor.Visitor visitor = new MetaTableAccessor.Visitor() {
        @Override
        public boolean visit(Result data) throws IOException {
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

    MetaTableAccessor.scanMeta(clusterConnection, visitor, tableName, rowKey, 1);
    return result.get();
  }

  @Override
  public void modifyTable(final TableName tableName, final HTableDescriptor descriptor)
      throws IOException {
    checkInitialized();
    sanityCheckTableDescriptor(descriptor);
    if (cpHost != null) {
      cpHost.preModifyTable(tableName, descriptor);
    }
    LOG.info(getClientIdAuditPrefix() + " modify " + tableName);
    new ModifyTableHandler(tableName, descriptor, this, this).prepare().process();
    if (cpHost != null) {
      cpHost.postModifyTable(tableName, descriptor);
    }
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
        isTableState(tableName, TableState.State.DISABLED)) {
      throw new TableNotDisabledException(tableName);
    }
  }

  /**
   * @return cluster status
   */
  public ClusterStatus getClusterStatus() throws InterruptedIOException {
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
    Map<String, RegionState> regionsInTransition = assignmentManager != null ?
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

  public int getRegionServerInfoPort(final ServerName sn) {
    RegionServerInfo info = this.regionServerTracker.getRegionServerInfo(sn);
    if (info == null || info.getInfoPort() == 0) {
      return conf.getInt(HConstants.REGIONSERVER_INFO_PORT,
        HConstants.DEFAULT_REGIONSERVER_INFOPORT);
    }
    return info.getInfoPort();
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
    if (t != null) LOG.fatal(msg, t);
    stop(msg);
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

  public void shutdown() {
    if (cpHost != null) {
      try {
        cpHost.preShutdown();
      } catch (IOException ioe) {
        LOG.error("Error call master coprocessor preShutdown()", ioe);
      }
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

  public void stopMaster() {
    if (cpHost != null) {
      try {
        cpHost.preStopMaster();
      } catch (IOException ioe) {
        LOG.error("Error call master coprocessor preStopMaster()", ioe);
      }
    }
    stop("Stopped by " + Thread.currentThread().getName());
  }

  void checkServiceStarted() throws ServerNotRunningYetException {
    if (!serviceStarted) {
      throw new ServerNotRunningYetException("Server is not running yet");
    }
  }

  void checkInitialized() throws PleaseHoldException, ServerNotRunningYetException {
    checkServiceStarted();
    if (!this.initialized) {
      throw new PleaseHoldException("Master is initializing");
    }
  }

  void checkNamespaceManagerReady() throws IOException {
    checkInitialized();
    if (tableNamespaceManager == null ||
        !tableNamespaceManager.isTableAvailableAndInitialized()) {
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
  @Override
  public boolean isInitialized() {
    return initialized;
  }

  /**
   * ServerShutdownHandlerEnabled is set false before completing
   * assignMeta to prevent processing of ServerShutdownHandler.
   * @return true if assignMeta has completed;
   */
  @Override
  public boolean isServerShutdownHandlerEnabled() {
    return this.serverShutdownHandlerEnabled;
  }

  /**
   * Report whether this master has started initialization and is about to do meta region assignment
   * @return true if master is in initialization & about to assign hbase:meta regions
   */
  public boolean isInitializationStartsMetaRegionAssignment() {
    return this.initializationBeforeMetaAssignment;
  }

  public void assignRegion(HRegionInfo hri) {
    assignmentManager.assign(hri);
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

  @Override
  public void createNamespace(NamespaceDescriptor descriptor) throws IOException {
    TableName.isLegalNamespaceName(Bytes.toBytes(descriptor.getName()));
    checkNamespaceManagerReady();
    if (cpHost != null) {
      if (cpHost.preCreateNamespace(descriptor)) {
        return;
      }
    }
    LOG.info(getClientIdAuditPrefix() + " creating " + descriptor);
    tableNamespaceManager.create(descriptor);
    if (cpHost != null) {
      cpHost.postCreateNamespace(descriptor);
    }
  }

  @Override
  public void modifyNamespace(NamespaceDescriptor descriptor) throws IOException {
    TableName.isLegalNamespaceName(Bytes.toBytes(descriptor.getName()));
    checkNamespaceManagerReady();
    if (cpHost != null) {
      if (cpHost.preModifyNamespace(descriptor)) {
        return;
      }
    }
    LOG.info(getClientIdAuditPrefix() + " modify " + descriptor);
    tableNamespaceManager.update(descriptor);
    if (cpHost != null) {
      cpHost.postModifyNamespace(descriptor);
    }
  }

  @Override
  public void deleteNamespace(String name) throws IOException {
    checkNamespaceManagerReady();
    if (cpHost != null) {
      if (cpHost.preDeleteNamespace(name)) {
        return;
      }
    }
    LOG.info(getClientIdAuditPrefix() + " delete " + name);
    tableNamespaceManager.remove(name);
    if (cpHost != null) {
      cpHost.postDeleteNamespace(name);
    }
  }

  /**
   * Ensure that the specified namespace exists, otherwise throws a NamespaceNotFoundException
   *
   * @param name the namespace to check
   * @throws IOException if the namespace manager is not ready yet.
   * @throws NamespaceNotFoundException if the namespace does not exists
   */
  private void ensureNamespaceExists(final String name)
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
      bypass = cpHost.preGetTableDescriptors(tableNameList, descriptors, regex);
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
          if (tableStateManager.isTablePresent(desc.getTableName())
              && (includeSysTables || !desc.getTableName().isSystemTable())) {
            descriptors.add(desc);
          }
        }
      } else {
        for (TableName s: tableNameList) {
          if (tableStateManager.isTablePresent(s)) {
            HTableDescriptor desc = tableDescriptors.get(s);
            if (desc != null) {
              descriptors.add(desc);
            }
          }
        }
      }

      // Retains only those matched by regular expression.
      if (regex != null) {
        filterTablesByRegex(descriptors, Pattern.compile(regex));
      }

      if (cpHost != null) {
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
    return getClusterStatus().getLastMajorCompactionTsForTable(table);
  }

  @Override
  public long getLastMajorCompactionTimestampForRegion(byte[] regionName) throws IOException {
    return getClusterStatus().getLastMajorCompactionTsForRegion(regionName);
  }

  /**
   * Queries the state of the {@link LoadBalancerTracker}. If the balancer is not initialized,
   * false is returned.
   *
   * @return The state of the load balancer, or false if the load balancer isn't defined.
   */
  public boolean isBalancerOn() {
    if (null == loadBalancerTracker) return false;
    return loadBalancerTracker.isBalancerOn();
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
}
