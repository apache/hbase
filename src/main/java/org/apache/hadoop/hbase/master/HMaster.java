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
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HealthCheckChore;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.UserProvider;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitorBase;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.coprocessor.Exec;
import org.apache.hadoop.hbase.client.coprocessor.ExecResult;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorService.ExecutorType;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HMasterRegionInterface;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;
import org.apache.hadoop.hbase.ipc.RpcServer;
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
import org.apache.hadoop.hbase.master.handler.TableEventHandler;
import org.apache.hadoop.hbase.master.handler.TableModifyFamilyHandler;
import org.apache.hadoop.hbase.master.metrics.MasterMetrics;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.monitoring.MemoryBoundedLogMessageBuffer;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionServerInfo;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.snapshot.HSnapshotDescription;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.ClusterId;
import org.apache.hadoop.hbase.zookeeper.ClusterStatusTracker;
import org.apache.hadoop.hbase.zookeeper.DrainingServerTracker;
import org.apache.hadoop.hbase.zookeeper.RegionServerTracker;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.net.DNS;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.collect.ClassToInstanceMap;
import com.google.common.collect.Maps;
import com.google.common.collect.MutableClassToInstanceMap;
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
 * @see HMasterInterface
 * @see HMasterRegionInterface
 * @see Watcher
 */
public class HMaster extends HasThread
implements HMasterInterface, HMasterRegionInterface, MasterServices,
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

  // RPC server for the HMaster
  private final RpcServer rpcServer;

  /**
   * This servers address.
   */
  private final InetSocketAddress isa;

  // Metrics for the HMaster
  private final MasterMetrics metrics;
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

  // flag set after we complete initialization once active,
  // it is not private since it's used in unit tests
  volatile boolean initialized = false;

  // flag set after we complete assignRootAndMeta.
  private volatile boolean serverShutdownHandlerEnabled = false;
  // flag to indicate that we should be handling meta hlogs differently for splitting
  private volatile boolean shouldSplitMetaSeparately;

  // Instance of the hbase executor service.
  ExecutorService executorService;

  private LoadBalancer balancer;
  private Thread balancerChore;
  // If 'true', the balancer is 'on'.  If 'false', the balancer will not run.
  private volatile boolean balanceSwitch = true;

  private CatalogJanitor catalogJanitorChore;
  private LogCleaner logCleaner;
  private HFileCleaner hfileCleaner;

  private MasterCoprocessorHost cpHost;
  private final ServerName serverName;

  private TableDescriptors tableDescriptors;

  // Time stamps for when a hmaster was started and when it became active
  private long masterStartTime;
  private long masterActiveTime;

  // monitor for snapshot of hbase tables
  private SnapshotManager snapshotManager;

  /**
   * MX Bean for MasterInfo
   */
  private ObjectName mxBean = null;

  // Registered master protocol handlers
  private ClassToInstanceMap<CoprocessorProtocol>
      protocolHandlers = MutableClassToInstanceMap.create();

  private Map<String, Class<? extends CoprocessorProtocol>>
      protocolHandlerNames = Maps.newHashMap();

  /** The health check chore. */
  private HealthCheckChore healthCheckChore;

  /** flag when true, Master waits for log splitting complete before start up */
  private boolean waitingOnLogSplitting = false;

  /** flag used in test cases in order to simulate RS failures during master initialization */
  private volatile boolean initializationBeforeMetaAssignment = false;

  /** The following is used in master recovery scenario to re-register listeners */
  private List<ZooKeeperListener> registeredZKListenersBeforeRecovery;

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
    // Test that the hostname is reachable
    InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);
    if (initialIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of hostname " + initialIsa);
    }
    // Verify that the bind address is reachable if set
    String bindAddress = conf.get("hbase.master.ipc.address");
    if (bindAddress != null) {
      initialIsa = new InetSocketAddress(bindAddress, port);
      if (initialIsa.getAddress() == null) {
        throw new IllegalArgumentException("Failed resolve of bind address " + initialIsa);
      }
    }
    int numHandlers = conf.getInt("hbase.master.handler.count",
      conf.getInt("hbase.regionserver.handler.count", 25));
    this.rpcServer = HBaseRPC.getServer(this,
      new Class<?>[]{HMasterInterface.class, HMasterRegionInterface.class},
        initialIsa.getHostName(), // This is bindAddress if set else it's hostname
        initialIsa.getPort(),
        numHandlers,
        0, // we dont use high priority handlers in master
        conf.getBoolean("hbase.rpc.verbose", false), conf,
        0); // this is a DNC w/o high priority handlers
    // Set our address.
    this.isa = this.rpcServer.getListenerAddress();
    // We don't want to pass isa's hostname here since it could be 0.0.0.0
    this.serverName = new ServerName(hostname,
      this.isa.getPort(), System.currentTimeMillis());
    this.rsFatals = new MemoryBoundedLogMessageBuffer(
        conf.getLong("hbase.master.buffer.for.rs.fatals", 1*1024*1024));

    // login the zookeeper client principal (if using security)
    ZKUtil.loginClient(this.conf, "hbase.zookeeper.client.keytab.file",
      "hbase.zookeeper.client.kerberos.principal", this.isa.getHostName());

    // initialize server principal (if using secure Hadoop)
    UserProvider provider = UserProvider.instantiate(conf);
    provider.login("hbase.master.keytab.file",
      "hbase.master.kerberos.principal", this.isa.getHostName());

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
    this.metrics = new MasterMetrics(getServerName().toString());

    // Health checker thread.
    int sleepTime = this.conf.getInt(HConstants.HEALTH_CHORE_WAKE_FREQ,
      HConstants.DEFAULT_THREAD_WAKE_FREQUENCY);
    if (isHealthCheckerConfigured()) {
      healthCheckChore = new HealthCheckChore(sleepTime, this, getConfiguration());
    }

    this.shouldSplitMetaSeparately = conf.getBoolean(HLog.SEPARATE_HLOG_FOR_META, false);
    waitingOnLogSplitting = this.conf.getBoolean("hbase.master.wait.for.log.splitting", false);
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
      Thread.sleep(c.getInt("zookeeper.session.timeout", 180 * 1000));
    }

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
      this.registeredZKListenersBeforeRecovery = this.zooKeeper.getListeners();

      // Put up info server.
      int port = this.conf.getInt("hbase.master.info.port", 60010);
      if (port >= 0) {
        String a = this.conf.get("hbase.master.info.bindAddress", "0.0.0.0");
        this.infoServer = new InfoServer(MASTER, a, port, false, this.conf);
        this.infoServer.addServlet("status", "/master-status", MasterStatusServlet.class);
        this.infoServer.addServlet("dump", "/dump", MasterDumpServlet.class);
        this.infoServer.setAttribute(MASTER, this);
        this.infoServer.start();
      }

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
    return this.activeMasterManager.blockUntilBecomingActiveMaster(startupStatus);
  }

  /**
   * Initialize all ZK based system trackers.
   * @throws IOException
   * @throws InterruptedException
   */
  private void initializeZKBasedSystemTrackers() throws IOException,
      InterruptedException, KeeperException {
    this.catalogTracker = new CatalogTracker(this.zooKeeper, this.conf, this);
    this.catalogTracker.start();

    this.balancer = LoadBalancerFactory.getLoadBalancer(conf);
    this.assignmentManager = new AssignmentManager(this, serverManager,
        this.catalogTracker, this.balancer, this.executorService);
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
    this.snapshotManager = new SnapshotManager(this, this.metrics);
  }

  // Check if we should stop every second.
  private Sleeper stopSleeper = new Sleeper(1000, this);
  private void loop() {
    while (!this.stopped) {
      stopSleeper.sleep();
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
    this.fileSystemManager = new MasterFileSystem(this, this, metrics, masterRecovery);

    this.tableDescriptors =
      new FSTableDescriptors(this.fileSystemManager.getFileSystem(),
      this.fileSystemManager.getRootDir());

    // publish cluster ID
    status.setStatus("Publishing Cluster ID in ZooKeeper");
    ClusterId.setClusterId(this.zooKeeper, fileSystemManager.getClusterId());
    if (!masterRecovery) {
      this.executorService = new ExecutorService(getServerName().toString());
      this.serverManager = new ServerManager(this, this);
    }


    status.setStatus("Initializing ZK system trackers");
    initializeZKBasedSystemTrackers();

    if (!masterRecovery) {
      // initialize master side coprocessors before we start handling requests
      status.setStatus("Initializing master coprocessors");
      this.cpHost = new MasterCoprocessorHost(this, this.conf);

      // start up all service threads.
      status.setStatus("Initializing master service threads");
      startServiceThreads();
    }

    // Wait for region servers to report in.
    this.serverManager.waitForRegionServers(status);
    // Check zk for regionservers that are up but didn't register
    for (ServerName sn: this.regionServerTracker.getOnlineServers()) {
      if (!this.serverManager.isServerOnline(sn)) {
        // Not registered; add it.
        LOG.info("Registering server found up in zk but who has not yet " +
          "reported in: " + sn);
        this.serverManager.recordNewServer(sn, HServerLoad.EMPTY_HSERVERLOAD);
      }
    }
    if (!masterRecovery) {
      this.assignmentManager.startTimeOutMonitor();
    }

    // get a list for previously failed RS which need recovery work
    Set<ServerName> failedServers = this.fileSystemManager.getFailedServersFromLogFolders();
    if (waitingOnLogSplitting) {
      List<ServerName> servers = new ArrayList<ServerName>(failedServers);
      this.fileSystemManager.splitAllLogs(servers);
      failedServers.clear();
    }

    ServerName preRootServer = this.catalogTracker.getRootLocation();
    if (preRootServer != null && failedServers.contains(preRootServer)) {
      // create recovered edits file for _ROOT_ server
      this.fileSystemManager.splitAllLogs(preRootServer);
      failedServers.remove(preRootServer);
    }

    this.initializationBeforeMetaAssignment = true;
    // Make sure root assigned before proceeding.
    if (!assignRoot(status)) return;

    // SSH should enabled for ROOT before META region assignment
    // because META region assignment is depending on ROOT server online.
    this.serverManager.enableSSHForRoot();

    // log splitting for .META. server
    ServerName preMetaServer = this.catalogTracker.getMetaLocationOrReadLocationFromRoot();
    if (preMetaServer != null && failedServers.contains(preMetaServer)) {
      // create recovered edits file for .META. server
      this.fileSystemManager.splitAllLogs(preMetaServer);
      failedServers.remove(preMetaServer);
    }

    // Make sure meta assigned before proceeding.
    if (!assignMeta(status, ((masterRecovery) ? null : preMetaServer), preRootServer)) return;

    enableServerShutdownHandler();

    // handle other dead servers in SSH
    status.setStatus("Submit log splitting work of non-meta region servers");
    for (ServerName curServer : failedServers) {
      this.serverManager.expireServer(curServer);
    }

    // Update meta with new HRI if required. i.e migrate all HRI with HTD to
    // HRI with out HTD in meta and update the status in ROOT. This must happen
    // before we assign all user regions or else the assignment will fail.
    // TODO: Remove this when we do 0.94.
    org.apache.hadoop.hbase.catalog.MetaMigrationRemovingHTD.
      updateMetaWithNewHRI(this);

    // Fixup assignment manager status
    status.setStatus("Starting assignment manager");
    this.assignmentManager.joinCluster();

    this.balancer.setClusterStatus(getClusterStatus());
    this.balancer.setMasterServices(this);

    // Fixing up missing daughters if any
    status.setStatus("Fixing up missing daughters");
    fixupDaughters(status);

    if (!masterRecovery) {
      // Start balancer and meta catalog janitor after meta and regions have
      // been assigned.
      status.setStatus("Starting balancer and catalog janitor");
      this.balancerChore = getAndStartBalancerChore(this);
      this.catalogJanitorChore = new CatalogJanitor(this, this);
      startCatalogJanitorChore();
      registerMBean();
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
   * If ServerShutdownHandler is disabled, we enable it and expire those dead
   * but not expired servers.
   *
   * @throws IOException
   */
  private void enableServerShutdownHandler() throws IOException {
    if (!serverShutdownHandlerEnabled) {
      serverShutdownHandlerEnabled = true;
      this.serverManager.expireDeadNotExpiredServers();
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
   * Check <code>-ROOT-</code> is assigned. If not, assign it.
   * @param status MonitoredTask
   * @throws InterruptedException
   * @throws IOException
   * @throws KeeperException
   */
  private boolean assignRoot(MonitoredTask status)
  throws InterruptedException, IOException, KeeperException {
    int assigned = 0;
    long timeout = this.conf.getLong("hbase.catalog.verification.timeout", 1000);

    // Work on ROOT region.  Is it in zk in transition?
    status.setStatus("Assigning ROOT region");
    boolean rit = this.assignmentManager.
      processRegionInTransitionAndBlockUntilAssigned(HRegionInfo.ROOT_REGIONINFO);
    ServerName currentRootServer = null;
    boolean rootRegionLocation = catalogTracker.verifyRootRegionLocation(timeout);
    if (!rit && !rootRegionLocation) {
      currentRootServer = this.catalogTracker.getRootLocation();
      splitLogAndExpireIfOnline(currentRootServer);
      this.assignmentManager.assignRoot();
      waitForRootAssignment();
      if (!this.assignmentManager.isRegionAssigned(HRegionInfo.ROOT_REGIONINFO) || this.stopped) {
        return false;
      }
      assigned++;
    } else if (rit && !rootRegionLocation) {
      waitForRootAssignment();
      if (!this.assignmentManager.isRegionAssigned(HRegionInfo.ROOT_REGIONINFO) || this.stopped) {
        return false;
      }
      assigned++;
    } else {
      // Region already assigned. We didn't assign it. Add to in-memory state.
      this.assignmentManager.regionOnline(HRegionInfo.ROOT_REGIONINFO,
          this.catalogTracker.getRootLocation());
    }
    // Enable the ROOT table if on process fail over the RS containing ROOT
    // was active.
    enableCatalogTables(Bytes.toString(HConstants.ROOT_TABLE_NAME));
    LOG.info("-ROOT- assigned=" + assigned + ", rit=" + rit +
      ", location=" + catalogTracker.getRootLocation());

    status.setStatus("ROOT assigned.");
    return true;
  }

  /**
   * Check <code>.META.</code> is assigned. If not, assign it.
   * @param status MonitoredTask
   * @param previousMetaServer ServerName of previous meta region server before current start up
   * @param previousRootServer ServerName of previous root region server before current start up
   * @throws InterruptedException
   * @throws IOException
   * @throws KeeperException
   */
  private boolean assignMeta(MonitoredTask status, ServerName previousMetaServer,
      ServerName previousRootServer)
      throws InterruptedException,
      IOException, KeeperException {
    int assigned = 0;
    long timeout = this.conf.getLong("hbase.catalog.verification.timeout", 1000);

    status.setStatus("Assigning META region");
    boolean rit =
        this.assignmentManager
            .processRegionInTransitionAndBlockUntilAssigned(HRegionInfo.FIRST_META_REGIONINFO);
    boolean metaRegionLocation = this.catalogTracker.verifyMetaRegionLocation(timeout);
    if (!rit && !metaRegionLocation) {
      ServerName currentMetaServer =
          (previousMetaServer != null) ? previousMetaServer : this.catalogTracker
              .getMetaLocationOrReadLocationFromRoot();
      if (currentMetaServer != null && !currentMetaServer.equals(previousRootServer)) {
        fileSystemManager.splitAllLogs(currentMetaServer);
        if (this.serverManager.isServerOnline(currentMetaServer)) {
          this.serverManager.expireServer(currentMetaServer);
        }
      }
      assignmentManager.assignMeta();
      enableSSHandWaitForMeta();
      if (!this.assignmentManager.isRegionAssigned(HRegionInfo.FIRST_META_REGIONINFO)
          || this.stopped) {
        return false;
      }
      assigned++;
    } else if (rit && !metaRegionLocation) {
      enableSSHandWaitForMeta();
      if (!this.assignmentManager.isRegionAssigned(HRegionInfo.FIRST_META_REGIONINFO)
          || this.stopped) {
        return false;
      }
      assigned++;
    } else {
      // Region already assigned. We didnt' assign it. Add to in-memory state.
      this.assignmentManager.regionOnline(HRegionInfo.FIRST_META_REGIONINFO,
        this.catalogTracker.getMetaLocation());
    }
    enableCatalogTables(Bytes.toString(HConstants.META_TABLE_NAME));
    LOG.info(".META. assigned=" + assigned + ", rit=" + rit + ", location="
        + catalogTracker.getMetaLocation());
    status.setStatus("META assigned.");
    return true;
  }

  private void enableSSHandWaitForMeta() throws IOException,
      InterruptedException {
    enableServerShutdownHandler();
    this.catalogTracker.waitForMeta();
    // Above check waits for general meta availability but this does not
    // guarantee that the transition has completed
    this.assignmentManager
        .waitForAssignment(HRegionInfo.FIRST_META_REGIONINFO);
  }

  private void waitForRootAssignment() throws InterruptedException, IOException {
    // Enable SSH for ROOT to prevent a newly assigned ROOT crashes again before global SSH is
    // enabled
    this.serverManager.enableSSHForRoot();
    this.catalogTracker.waitForRoot();
    // This guarantees that the transition has completed
    this.assignmentManager.waitForAssignment(HRegionInfo.ROOT_REGIONINFO);
  }

  private void enableCatalogTables(String catalogTableName) {
    if (!this.assignmentManager.getZKTable().isEnabledTable(catalogTableName)) {
      this.assignmentManager.setEnabledTable(catalogTableName);
    }
  }

  void fixupDaughters(final MonitoredTask status) throws IOException, KeeperException {
    final Map<HRegionInfo, Result> offlineSplitParents =
      new HashMap<HRegionInfo, Result>();
    // This visitor collects offline split parents in the .META. table
    MetaReader.Visitor visitor = new MetaReader.Visitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r == null || r.isEmpty()) return true;
        HRegionInfo info =
          MetaReader.parseHRegionInfoFromCatalogResult(
            r, HConstants.REGIONINFO_QUALIFIER);
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
      String node = ZKAssign.getNodeName(zooKeeper, e.getKey().getEncodedName());
      byte[] data = ZKUtil.getData(zooKeeper, node);
      if (data == null) { // otherwise, splitting is still going on, skip it
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
   * Expire a server if we find it is one of the online servers.
   * @param sn ServerName to check.
   * @throws IOException
   */
  private void splitLogAndExpireIfOnline(final ServerName sn)
      throws IOException {
    if (sn == null || !serverManager.isServerOnline(sn)) {
      return;
    }
    LOG.info("Forcing splitLog and expire of " + sn);
    if (this.shouldSplitMetaSeparately) {
      fileSystemManager.splitMetaLog(sn);
      fileSystemManager.splitLog(sn);
    } else {
      fileSystemManager.splitAllLogs(sn);
    }
    serverManager.expireServer(sn);
  }

  @Override
  public ProtocolSignature getProtocolSignature(
      String protocol, long version, int clientMethodsHashCode)
  throws IOException {
    if (HMasterInterface.class.getName().equals(protocol)) {
      return new ProtocolSignature(HMasterInterface.VERSION, null);
    } else if (HMasterRegionInterface.class.getName().equals(protocol)) {
      return new ProtocolSignature(HMasterRegionInterface.VERSION, null);
    }
    throw new IOException("Unknown protocol: " + protocol);
  }

  public long getProtocolVersion(String protocol, long clientVersion) {
    if (HMasterInterface.class.getName().equals(protocol)) {
      return HMasterInterface.VERSION;
    } else if (HMasterRegionInterface.class.getName().equals(protocol)) {
      return HMasterRegionInterface.VERSION;
    }
    // unknown protocol
    LOG.warn("Version requested for unimplemented protocol: "+protocol);
    return -1;
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

  public ActiveMasterManager getActiveMasterManager() {
    return this.activeMasterManager;
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

   // Start the health checker
   if (this.healthCheckChore != null) {
     Threads.setDaemonThreadRunning(this.healthCheckChore.getThread(), n + ".healthChecker");
   }

    // Start allowing requests to happen.
    this.rpcServer.openServer();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Started service threads");
    }

  }

  private void stopServiceThreads() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping service threads");
    }
    if (this.rpcServer != null) this.rpcServer.stop();
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

  private static Thread getAndStartBalancerChore(final HMaster master) {
    String name = master.getServerName() + "-BalancerChore";
    int balancerPeriod =
      master.getConfiguration().getInt("hbase.balancer.period", 300000);
    // Start up the load balancer chore
    Chore chore = new Chore(name, balancerPeriod, master) {
      @Override
      protected void chore() {
        master.balance();
      }
    };
    return Threads.setDaemonThreadRunning(chore.getThread());
  }

  private void stopChores() {
    if (this.balancerChore != null) {
      this.balancerChore.interrupt();
    }
    if (this.catalogJanitorChore != null) {
      this.catalogJanitorChore.interrupt();
    }
  }

  @Override
  public MapWritable regionServerStartup(final int port,
    final long serverStartCode, final long serverCurrentTime)
  throws IOException {
    // Register with server manager
    InetAddress ia = HBaseServer.getRemoteIp();
    ServerName rs = this.serverManager.regionServerStartup(ia, port,
      serverStartCode, serverCurrentTime);
    // Send back some config info
    MapWritable mw = createConfigurationSubset();
    mw.put(new Text(HConstants.KEY_FOR_HOSTNAME_SEEN_BY_MASTER),
      new Text(rs.getHostname()));
    return mw;
  }

  /**
   * @return Subset of configuration to pass initializing regionservers: e.g.
   * the filesystem to use and root directory to use.
   */
  protected MapWritable createConfigurationSubset() {
    MapWritable mw = addConfig(new MapWritable(), HConstants.HBASE_DIR);
    return addConfig(mw, "fs.default.name");
  }

  private MapWritable addConfig(final MapWritable mw, final String key) {
    mw.put(new Text(key), new Text(this.conf.get(key)));
    return mw;
  }

  @Override
  public void regionServerReport(final byte [] sn, final HServerLoad hsl)
  throws IOException {
    this.serverManager.regionServerReport(ServerName.parseVersionedServerName(sn), hsl);
    if (hsl != null && this.metrics != null) {
      // Up our metrics.
      this.metrics.incrementRequests(hsl.getTotalNumberOfRequests());
    }
  }

  @Override
  public void reportRSFatalError(byte [] sn, String errorText) {
    String msg = "Region server " + Bytes.toString(sn) +
      " reported a fatal error:\n" + errorText;
    LOG.error(msg);
    rsFatals.add(msg);
  }

  public boolean isMasterRunning() {
    return !isStopped();
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

  @Override
  public boolean balance() {
    // if master not initialized, don't run balancer.
    if (!this.initialized) {
      LOG.debug("Master has not been initialized, don't run balancer.");
      return false;
    }
    // If balance not true, don't run balancer.
    if (!this.balanceSwitch) return false;
    // Do this call outside of synchronized block.
    int maximumBalanceTime = getBalancerCutoffTime();
    long cutoffTime = System.currentTimeMillis() + maximumBalanceTime;
    boolean balancerRan;
    synchronized (this.balancer) {
      // Only allow one balance run at at time.
      if (this.assignmentManager.isRegionsInTransition()) {
        LOG.debug("Not running balancer because " +
          this.assignmentManager.getRegionsInTransition().size() +
          " region(s) in transition: " +
          org.apache.commons.lang.StringUtils.
            abbreviate(this.assignmentManager.getRegionsInTransition().toString(), 256));
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

      Map<String, Map<ServerName, List<HRegionInfo>>> assignmentsByTable =
        this.assignmentManager.getAssignmentsByTable();

      List<RegionPlan> plans = new ArrayList<RegionPlan>();
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
        try {
          this.cpHost.postBalance();
        } catch (IOException ioe) {
          // balancing already succeeded so don't change the result
          LOG.error("Error invoking master coprocessor postBalance()", ioe);
        }
      }
    }
    return balancerRan;
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
  public boolean switchBalancer(final boolean b, BalanceSwitchMode mode) {
    boolean oldValue = this.balanceSwitch;
    boolean newValue = b;
    try {
      if (this.cpHost != null) {
        newValue = this.cpHost.preBalanceSwitch(newValue);
      }
      if (mode == BalanceSwitchMode.SYNC) {
        synchronized (this.balancer) {
          this.balanceSwitch = newValue;
        }
      } else {
        this.balanceSwitch = newValue;
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

  @Override
  public boolean synchronousBalanceSwitch(final boolean b) {
    return switchBalancer(b, BalanceSwitchMode.SYNC);
  }

  @Override
  public boolean balanceSwitch(final boolean b) {
    return switchBalancer(b, BalanceSwitchMode.ASYNC);
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
  public void move(final byte[] encodedRegionName, final byte[] destServerName)
  throws UnknownRegionException {
    Pair<HRegionInfo, ServerName> p =
      this.assignmentManager.getAssignment(encodedRegionName);
    if (p == null)
      throw new UnknownRegionException(Bytes.toStringBinary(encodedRegionName));
    ServerName dest = null;
    if (destServerName == null || destServerName.length == 0) {
      LOG.info("Passed destination servername is null or empty so choosing a server at random");
      List<ServerName> destServers = this.serverManager.getOnlineServersList();
      destServers.remove(p.getSecond());
      // If i have only one RS then destination can be null.
      dest = balancer.randomAssignment(destServers);
    } else {
      dest = new ServerName(Bytes.toString(destServerName));
    }

    // Now we can do the move
    RegionPlan rp = new RegionPlan(p.getFirst(), p.getSecond(), dest);

    try {
      checkInitialized();
      if (this.cpHost != null) {
        if (this.cpHost.preMove(p.getFirst(), p.getSecond(), dest)) {
          return;
        }
      }
      LOG.info("Added move plan " + rp + ", running balancer");
      this.assignmentManager.balance(rp);
      if (this.cpHost != null) {
        this.cpHost.postMove(p.getFirst(), p.getSecond(), dest);
      }
    } catch (IOException ioe) {
      UnknownRegionException ure = new UnknownRegionException(
          Bytes.toStringBinary(encodedRegionName));
      ure.initCause(ioe);
      throw ure;
    }
  }

  public void createTable(HTableDescriptor hTableDescriptor,
    byte [][] splitKeys)
  throws IOException {
    if (!isMasterRunning()) {
      throw new MasterNotRunningException();
    }

    HRegionInfo [] newRegions = getHRegionInfos(hTableDescriptor, splitKeys);
    checkInitialized();
    if (cpHost != null) {
      cpHost.preCreateTable(hTableDescriptor, newRegions);
    }

    this.executorService.submit(new CreateTableHandler(this,
      this.fileSystemManager, this.serverManager, hTableDescriptor, conf,
      newRegions, catalogTracker, assignmentManager));

    if (cpHost != null) {
      cpHost.postCreateTable(hTableDescriptor, newRegions);
    }
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
  public void deleteTable(final byte [] tableName) throws IOException {
    checkInitialized();
    if (cpHost != null) {
      cpHost.preDeleteTable(tableName);
    }
    this.executorService.submit(new DeleteTableHandler(tableName, this, this));
    if (cpHost != null) {
      cpHost.postDeleteTable(tableName);
    }
  }

  /**
   * Get the number of regions of the table that have been updated by the alter.
   *
   * @return Pair indicating the number of regions updated Pair.getFirst is the
   *         regions that are yet to be updated Pair.getSecond is the total number
   *         of regions of the table
   * @throws IOException
   */
  public Pair<Integer, Integer> getAlterStatus(byte[] tableName)
  throws IOException {
    return this.assignmentManager.getReopenStatus(tableName);
  }

  public void addColumn(byte [] tableName, HColumnDescriptor column)
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

  public void modifyColumn(byte [] tableName, HColumnDescriptor descriptor)
  throws IOException {
    checkInitialized();
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

  public void deleteColumn(final byte [] tableName, final byte [] c)
  throws IOException {
    checkInitialized();
    if (cpHost != null) {
      if (cpHost.preDeleteColumn(tableName, c)) {
        return;
      }
    }
    new TableDeleteFamilyHandler(tableName, c, this, this).process();
    if (cpHost != null) {
      cpHost.postDeleteColumn(tableName, c);
    }
  }

  public void enableTable(final byte [] tableName) throws IOException {
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

  public void disableTable(final byte [] tableName) throws IOException {
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
          Pair<HRegionInfo, ServerName> pair = MetaReader.parseCatalogResult(data);
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
  public void modifyTable(final byte[] tableName, HTableDescriptor htd)
      throws IOException {
    checkInitialized();
    if (cpHost != null) {
      cpHost.preModifyTable(tableName, htd);
    }
    TableEventHandler tblHandler = new ModifyTableHandler(tableName, htd, this, this);
    this.executorService.submit(tblHandler);
    // prevent client from querying status even before the event is being handled.
    tblHandler.waitForEventBeingHandled();
    if (cpHost != null) {
      cpHost.postModifyTable(tableName, htd);
    }
  }

  @Override
  public void checkTableModifiable(final byte [] tableName)
  throws IOException {
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

  public void clearFromTransition(HRegionInfo hri) {
    if (this.assignmentManager.isRegionInTransition(hri) != null) {
      this.assignmentManager.regionOffline(hri);
    }
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
        byte[] bytes = ZKUtil.getData(this.zooKeeper, ZKUtil.joinZNode(this.zooKeeper.backupMasterAddressesZNode, s));
        if (bytes != null) {
          backupMasters.add(ServerName.parseVersionedServerName(bytes));
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
      this.fileSystemManager.getClusterId(),
      this.serverManager.getOnlineServers(),
      this.serverManager.getDeadServers(),
      this.serverName,
      backupMasters,
      this.assignmentManager.getRegionsInTransition(),
      this.getCoprocessors());
  }

  public String getClusterId() {
    return (fileSystemManager == null) ? null : fileSystemManager.getClusterId();
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
  public String[] getCoprocessors() {
    MasterCoprocessorHost cp = getCoprocessorHost();
    String[] cpList = new String[0];
    if (cp == null) return cpList;

    Set<String> masterCoprocessors = cp.getCoprocessors();
    return masterCoprocessors.toArray(cpList);
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

    this.zooKeeper.unregisterAllListeners();
    // add back listeners which were registered before master initialization
    // because they won't be added back in below Master re-initialization code
    if (this.registeredZKListenersBeforeRecovery != null) {
      for (ZooKeeperListener curListener : this.registeredZKListenersBeforeRecovery) {
        this.zooKeeper.registerListener(curListener);
      }
    }

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
          serverManager.disableSSHForRoot();
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
    if (!this.isActiveMaster || this.stopped) {
      return true;
    }

    boolean failFast = conf.getBoolean("fail.fast.expired.active.master", false);
    if (t != null && t instanceof KeeperException.SessionExpiredException
        && !failFast) {
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

  @SuppressWarnings("deprecation")
  @Override
  public void shutdown() {
    if (cpHost != null) {
      try {
        cpHost.preShutdown();
      } catch (IOException ioe) {
        LOG.error("Error call master coprocessor preShutdown()", ioe);
      }
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
      if (e instanceof KeeperException.SessionExpiredException) {
        LOG.warn("ZK session expired. Retry a new connection...");
        try {
          this.zooKeeper.reconnectAfterExpiration();
          this.clusterStatusTracker.setClusterDown();
        } catch (Exception ex) {
          LOG.error("Retry setClusterDown failed", ex);
        }
      } else {
        LOG.error("ZooKeeper exception trying to set cluster as down in ZK", e);
      }
    }
  }

  @Override
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
    // If no region server is online then master may stuck waiting on -ROOT- and .META. to come on
    // line. See HBASE-8422.
    if (this.catalogTracker != null && this.serverManager.getOnlineServers().isEmpty()) {
      this.catalogTracker.stop();
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

  public boolean shouldSplitMetaSeparately() {
    return this.shouldSplitMetaSeparately;
  }

  /**
   * Report whether this master has started initialization and is about to do meta region assignment
   * @return true if master is in initialization & about to assign ROOT & META regions
   */
  public boolean isInitializationStartsMetaRegoinAssignment() {
    return this.initializationBeforeMetaAssignment;
  }

  @Override
  @Deprecated
  public void assign(final byte[] regionName, final boolean force)
      throws IOException {
    assign(regionName);
  }

  @Override
  public void assign(final byte [] regionName)throws IOException {
    checkInitialized();
    Pair<HRegionInfo, ServerName> pair =
      MetaReader.getRegion(this.catalogTracker, regionName);
    if (pair == null) throw new UnknownRegionException(Bytes.toString(regionName));
    if (cpHost != null) {
      if (cpHost.preAssign(pair.getFirst())) {
        return;
      }
    }
    assignRegion(pair.getFirst());
    if (cpHost != null) {
      cpHost.postAssign(pair.getFirst());
    }
  }



  public void assignRegion(HRegionInfo hri) {
    assignmentManager.assign(hri, true);
  }

  @Override
  public void unassign(final byte [] regionName, final boolean force)
  throws IOException {
    checkInitialized();
    Pair<HRegionInfo, ServerName> pair =
      MetaReader.getRegion(this.catalogTracker, regionName);
    if (pair == null) throw new UnknownRegionException(Bytes.toString(regionName));
    HRegionInfo hri = pair.getFirst();
    if (cpHost != null) {
      if (cpHost.preUnassign(hri, force)) {
        return;
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
  }

  /**
   * Get HTD array for given tables
   * @param tableNames
   * @return HTableDescriptor[]
   */
  public HTableDescriptor[] getHTableDescriptors(List<String> tableNames)
      throws IOException {
    List<HTableDescriptor> descriptors =
      new ArrayList<HTableDescriptor>(tableNames.size());
    
    boolean bypass = false;
    if (this.cpHost != null) {
      bypass = this.cpHost.preGetTableDescriptors(tableNames, descriptors);
    }

    if (!bypass) {
      for (String s: tableNames) {
        HTableDescriptor htd = null;
        try {
          htd = this.tableDescriptors.get(s);
        } catch (IOException e) {
          LOG.warn("Failed getting descriptor for " + s, e);
        }
        if (htd == null) continue;
        descriptors.add(htd);
      }
    }

    if (this.cpHost != null) {
      this.cpHost.postGetTableDescriptors(descriptors);
    }

    return descriptors.toArray(new HTableDescriptor [] {});
  }

  @Override
  public <T extends CoprocessorProtocol> boolean registerProtocol(
      Class<T> protocol, T handler) {

    /* No stacking of protocol handlers is currently allowed.  The
     * first to claim wins!
     */
    if (protocolHandlers.containsKey(protocol)) {
      LOG.error("Protocol "+protocol.getName()+
          " already registered, rejecting request from "+
          handler
      );
      return false;
    }

    protocolHandlers.putInstance(protocol, handler);
    protocolHandlerNames.put(protocol.getName(), protocol);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Registered master protocol handler: protocol="+protocol.getName());
    }
    return true;
  }

  @Override
  public ExecResult execCoprocessor(Exec call) throws IOException {
    Class<? extends CoprocessorProtocol> protocol = call.getProtocol();
    if (protocol == null) {
      String protocolName = call.getProtocolName();
      if (LOG.isTraceEnabled()) {
        LOG.trace("Received dynamic protocol exec call with protocolName " + protocolName);
      }
      // detect the actual protocol class
      protocol  = protocolHandlerNames.get(protocolName);
      if (protocol == null) {
        throw new HBaseRPC.UnknownProtocolException(protocol,
            "No matching handler for master protocol "+protocolName);
      }
    }
    if (!protocolHandlers.containsKey(protocol)) {
      throw new HBaseRPC.UnknownProtocolException(protocol,
          "No matching handler for protocol ");
    }

    CoprocessorProtocol handler = protocolHandlers.getInstance(protocol);
    Object value;

    try {
      Method method = protocol.getMethod(
          call.getMethodName(), call.getParameterClasses());
      method.setAccessible(true);

      value = method.invoke(handler, call.getParameters());
    } catch (InvocationTargetException e) {
      Throwable target = e.getTargetException();
      if (target instanceof IOException) {
        throw (IOException)target;
      }
      IOException ioe = new IOException(target.toString());
      ioe.setStackTrace(target.getStackTrace());
      throw ioe;
    } catch (Throwable e) {
      if (!(e instanceof IOException)) {
        LOG.error("Unexpected throwable object ", e);
      }
      IOException ioe = new IOException(e.toString());
      ioe.setStackTrace(e.getStackTrace());
      throw ioe;
    }

    return new ExecResult(value);
  }

  /**
   * Get all table descriptors
   * @return All descriptors or null if none.
   * @throws IOException
   */
  public HTableDescriptor [] getHTableDescriptors() throws IOException {
    List<HTableDescriptor> descriptors = new ArrayList<HTableDescriptor>();
    boolean bypass = false;
    if (this.cpHost != null) {
      bypass = this.cpHost.preGetTableDescriptors(null, descriptors);
    }
    if (!bypass) {
      descriptors.addAll(this.tableDescriptors.getAll().values());
    }
    if (this.cpHost != null) {
      this.cpHost.postGetTableDescriptors(descriptors);
    }
    return descriptors.toArray(new HTableDescriptor [] {});
  }

  /**
   * Compute the average load across all region servers.
   * Currently, this uses a very naive computation - just uses the number of
   * regions being served, ignoring stats about number of requests.
   * @return the average load
   */
  public double getAverageLoad() {
    return this.assignmentManager.getAverageLoad();
  }

  /**
   * Special method, only used by hbck.
   */
  @Override
  public void offline(final byte[] regionName) throws IOException {
    Pair<HRegionInfo, ServerName> pair =
      MetaReader.getRegion(this.catalogTracker, regionName);
    if (pair == null) throw new UnknownRegionException(Bytes.toStringBinary(regionName));
    HRegionInfo hri = pair.getFirst();
    this.assignmentManager.regionOffline(hri);
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
  public static void main(String [] args) throws Exception {
	VersionInfo.logVersion();
    new HMasterCommandLine(HMaster.class).doMain(args);
  }

  /**
   * Register bean with platform management server
   */
  @SuppressWarnings("deprecation")
  void registerMBean() {
    MXBeanImpl mxBeanInfo = MXBeanImpl.init(this);
    MBeanUtil.registerMBean("Master", "Master", mxBeanInfo);
    LOG.info("Registered HMaster MXBean");
  }

  /**
   * Exposed for Testing!
   * @return the current hfile cleaner
   */
  public HFileCleaner getHFileCleaner() {
    return this.hfileCleaner;
  }

  private boolean isHealthCheckerConfigured() {
    String healthScriptLocation = this.conf.get(HConstants.HEALTH_SCRIPT_LOC);
    return org.apache.commons.lang.StringUtils.isNotBlank(healthScriptLocation);
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
  public long snapshot(final HSnapshotDescription request) throws IOException {
    LOG.debug("Submitting snapshot request for:" +
        SnapshotDescriptionUtils.toString(request.getProto()));
    try {
      this.snapshotManager.checkSnapshotSupport();
    } catch (UnsupportedOperationException e) {
      throw new IOException(e);
    }

    // get the snapshot information
    SnapshotDescription snapshot = SnapshotDescriptionUtils.validate(request.getProto(),
      this.conf);

    snapshotManager.takeSnapshot(snapshot);

    // send back the max amount of time the client should wait for the snapshot to complete
    long waitTime = SnapshotDescriptionUtils.getMaxMasterTimeout(conf, snapshot.getType(),
      SnapshotDescriptionUtils.DEFAULT_MAX_WAIT_TIME);
    return waitTime;
  }

  /**
   * List the currently available/stored snapshots. Any in-progress snapshots are ignored
   */
  @Override
  public List<HSnapshotDescription> getCompletedSnapshots() throws IOException {
    List<HSnapshotDescription> availableSnapshots = new ArrayList<HSnapshotDescription>();
    List<SnapshotDescription> snapshots = snapshotManager.getCompletedSnapshots();

    // convert to writables
    for (SnapshotDescription snapshot: snapshots) {
      availableSnapshots.add(new HSnapshotDescription(snapshot));
    }

    return availableSnapshots;
  }

  /**
   * Execute Delete Snapshot operation.
   * @throws ServiceException wrapping SnapshotDoesNotExistException if specified snapshot did not
   * exist.
   */
  @Override
  public void deleteSnapshot(final HSnapshotDescription request) throws IOException {
    try {
      this.snapshotManager.checkSnapshotSupport();
    } catch (UnsupportedOperationException e) {
      throw new IOException(e);
    }

    snapshotManager.deleteSnapshot(request.getProto());
  }

  /**
   * Checks if the specified snapshot is done.
   * @return true if the snapshot is in file system ready to use,
   * false if the snapshot is in the process of completing
   * @throws ServiceException wrapping UnknownSnapshotException if invalid snapshot, or
   * a wrapped HBaseSnapshotException with progress failure reason.
   */
  @Override
  public boolean isSnapshotDone(final HSnapshotDescription request) throws IOException {
    LOG.debug("Checking to see if snapshot from request:" +
      SnapshotDescriptionUtils.toString(request.getProto()) + " is done");
    return snapshotManager.isSnapshotDone(request.getProto());
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
  public void restoreSnapshot(final HSnapshotDescription request) throws IOException {
    try {
      this.snapshotManager.checkSnapshotSupport();
    } catch (UnsupportedOperationException e) {
      throw new IOException(e);
    }

    snapshotManager.restoreSnapshot(request.getProto());
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
  public boolean isRestoreSnapshotDone(final HSnapshotDescription request) throws IOException {
    return snapshotManager.isRestoreDone(request.getProto());
  }

  /**
   * Return all table names.
   * @return the list of table names
   * @throws IOException if an error occurred while getting the list of tables
   */
  @Override
  public String[] getTableNames() throws IOException {
    // Anyone is allowed to see the names of tables, so there is no coprocessor
    // hook nor AccessController interception necessary
    Collection<HTableDescriptor> descriptors = tableDescriptors.getAll().values();
    Iterator<HTableDescriptor> iter = descriptors.iterator();
    String names[] = new String[descriptors.size()];
    int i = 0;
    while (iter.hasNext()) {
      names[i++] = iter.next().getNameAsString();
    }
    return names;
  }
}
