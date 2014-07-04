/**
 * Copyright 2010 The Apache Software Foundation
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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.Modify;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.ProtocolVersion;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.StopStatus;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableLockTimeoutException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ServerConnection;
import org.apache.hadoop.hbase.client.ServerConnectionManager;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.executor.HBaseEventHandler.HBaseEventType;
import org.apache.hadoop.hbase.executor.HBaseExecutorService;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HBaseRPCOptions;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HMasterRegionInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.metrics.MasterMetrics;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.hbase.util.InjectionEvent;
import org.apache.hadoop.hbase.util.InjectionHandler;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RuntimeHaltAbortStrategy;
import org.apache.hadoop.hbase.util.StringBytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.DNS;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.google.common.collect.Lists;

/**
 * HMaster is the "master server" for HBase. An HBase cluster has one active
 * master.  If many masters are started, all compete.  Whichever wins goes on to
 * run the cluster.  All others park themselves in their constructor until
 * master or cluster shutdown or until the active master loses its lease in
 * zookeeper.  Thereafter, all running master jostle to take over master role.
 * @see HMasterInterface
 * @see HMasterRegionInterface
 * @see Watcher
 */
public class HMaster extends HasThread implements HMasterInterface,
HMasterRegionInterface, Watcher, StoppableMaster {
  // MASTER is name of the webapp and the attribute name used stuffing this
  //instance into web context.
  public static final String MASTER = "master";
  private static final Log LOG = LogFactory.getLog(HMaster.class.getName());


  private static final String LOCALITY_SNAPSHOT_FILE_NAME = "regionLocality-snapshot";

  /**
   * We start out with closed flag on. Its set to off after construction. Use
   * AtomicBoolean rather than plain boolean because we want other threads able
   * to set this flag and trigger a cluster shutdown. Using AtomicBoolean can
   * pass a reference rather than have them have to know about the hosting
   * Master class. This also has disadvantages, because this instance passed
   * to another object can be confused with another thread's own shutdown flag.
   */
  private final AtomicBoolean closed = new AtomicBoolean(true);

  /**
   * This flag indicates that cluster shutdown has been requested. This is
   * different from {@link #closed} in that it is initially false, but is
   * set to true at shutdown and remains true from then on. For stopping one
   * instance of the master, see {@link #stopped}.
   */
  private final AtomicBoolean clusterShutdownRequested =
      new AtomicBoolean(false);

  private final SortedMap<byte[], Long> flushedSequenceIdByRegion =
      new ConcurrentSkipListMap<byte[], Long>(Bytes.BYTES_COMPARATOR);

  private final Configuration conf;
  private final Path rootdir;
  private InfoServer infoServer;
  private final int threadWakeFrequency;
  private final int numRetries;

  // Metrics is set when we call run.
  private MasterMetrics metrics;

  /** A lock used for serial log splitting */
  final Lock splitLogLock = new ReentrantLock();

  final boolean distributedLogSplitting;
  SplitLogManager splitLogManager;

  // Our zk client.
  private ZooKeeperWrapper zooKeeperWrapper;
  // Watcher for master address and for cluster shutdown.
  private final ZKMasterAddressWatcher zkMasterAddressWatcher;
  // Table level lock manager for schema changes
  private final TableLockManager tableLockManager;
  // Keep around for convenience.
  private final FileSystem fs;
  // Is the filesystem ok?
  private volatile boolean fsOk = true;
  // The Path to the old logs dir
  private final Path oldLogDir;

  private HBaseServer rpcServer;
  private final HServerAddress address;

  private final ServerConnection connection;
  private ServerManager serverManager;
  private RegionManager regionManager;
  private ZKUnassignedWatcher unassignedWatcher;

  private long lastFragmentationQuery = -1L;
  private Map<String, Integer> fragmentation = null;
  private RegionServerOperationQueue regionServerOperationQueue;

  /**
   * True if this is the master that started the cluster. We mostly use this for unit testing, as
   * our master failover workflow does not depend on this anymore.
   */
  @Deprecated
  private boolean isClusterStartup;

  private long masterStartupTime = Long.MAX_VALUE;
  private AtomicBoolean isSplitLogAfterStartupDone = new AtomicBoolean(false);
  private MapWritable preferredRegionToRegionServerMapping = null;
  private long applyPreferredAssignmentPeriod = 0l;
  private long holdRegionForBestLocalityPeriod = 0l;

  /** True if the master is being stopped. No cluster shutdown is done. */
  private volatile boolean stopped = false;

  /** Flag set after we become the active master (used for testing). */
  private volatile boolean isActiveMaster = false;

  public ThreadPoolExecutor logSplitThreadPool;

  public RegionPlacementPolicy regionPlacement;

  /** Log directories split on startup for testing master failover */
  private List<String> logDirsSplitOnStartup;

  private boolean shouldAssignRegionsWithFavoredNodes = false;

  /**
   * The number of dead server log split requests received. This is not
   * incremented during log splitting on startup. This field is never
   * decremented. Used in unit tests.
   */
  private final AtomicInteger numDeadServerLogSplitRequests =
      new AtomicInteger();

  private String stopReason = "not stopping";

  private ZKClusterStateRecovery clusterStateRecovery;

  private AtomicBoolean isLoadBalancerDisabled = new AtomicBoolean(false);

  private ConfigurationManager configurationManager =
      new ConfigurationManager();

  protected boolean useThrift = false;

  private long schemaChangeTryLockTimeoutMs;

  /** Configuration variable to prefer IPv6 address for HMaster */
  public static final String HMASTER_PREFER_IPV6_Address = "hmaster.prefer.ipv6.address";

  /**
   * Constructor
   * @param conf configuration
   * @throws IOException
   */
  public HMaster(Configuration conf) throws IOException {
    this.conf = conf;
    this.conf.set(HConstants.CLIENT_TO_RS_PROTOCOL_VERSION,
        ProtocolVersion.THRIFT.name());

    // Get my address. So what's the difference between the temp a and address?
    // address.toString() will always produce an IP address as the hostname.
    // a.toString() can have the canonical-name of the host as the hostname.
    // If the configured master port is 0, then a will bind it to an
    // ephemeral port first by starting the rpc server.
    HServerAddress a = new HServerAddress(getMyAddress(this.conf));
    int port;
    if ((port = a.getPort()) == 0) {
      // We initialize the RPC server here if the port is unknown (e.g. in unit tests).
      // In production we try to start the RPC server immediately before starting RPC threads.
      initRpcServer(a);
      port = this.rpcServer.getListenerAddress().getPort();
    }
    this.address = new HServerAddress(new InetSocketAddress(a.getBindAddress(),
        port));
    setName(getServerName());

    // Figure out if this is a fresh cluster start. This is done by checking the
    // number of RS ephemeral nodes. RS ephemeral nodes are created only after
    // the primary master has written the address to ZK. So this has to be done
    // before we race to write our address to zookeeper.
    initializeZooKeeper();

    this.numRetries =  conf.getInt("hbase.client.retries.number", 2);
    this.threadWakeFrequency = conf.getInt(HConstants.THREAD_WAKE_FREQUENCY,
        10 * 1000);

    /**
     * Overriding the useThrift parameter depending upon the configuration.
     * We set CLIENT_TO_RS_USE_THRIFT to MASTER_TO_RS_USE_THRIFT_STRING
     */
    this.useThrift = conf.getBoolean(HConstants.MASTER_TO_RS_USE_THRIFT,
        HConstants.MASTER_TO_RS_USE_THRIFT_DEFAULT);
    conf.setBoolean(HConstants.CLIENT_TO_RS_USE_THRIFT, this.useThrift);
    this.connection = ServerConnectionManager.getConnection(conf);

    // hack! Maps DFSClient => Master for logs.  HDFS made this
    // config param for task trackers, but we can piggyback off of it.
    if (this.conf.get("mapred.task.id") == null) {
      this.conf.set("mapred.task.id", "hb_m_" + this.address.toString());
    }

    // Set filesystem to be that of this.rootdir else we get complaints about
    // mismatched filesystems if hbase.rootdir is hdfs and fs.defaultFS is
    // default localfs.  Presumption is that rootdir is fully-qualified before
    // we get to here with appropriate fs scheme.
    this.rootdir = FSUtils.getRootDir(this.conf);
    // Cover both bases, the old way of setting default fs and the new.
    // We're supposed to run on 0.20 and 0.21 anyways.
    this.conf.set("fs.default.name", this.rootdir.toString());
    this.conf.set("fs.defaultFS", this.rootdir.toString());
    this.fs = FileSystem.get(this.conf);
    checkRootDir(this.rootdir, this.conf, this.fs);

    this.distributedLogSplitting = conf.getBoolean(
        HConstants.DISTRIBUTED_LOG_SPLITTING_KEY, false);
    this.splitLogManager = null;

    // Make sure the region servers can archive their old logs
    this.oldLogDir = new Path(this.rootdir, HConstants.HREGION_OLDLOGDIR_NAME);
    if(!this.fs.exists(this.oldLogDir)) {
      this.fs.mkdirs(this.oldLogDir);
    }

    // Get our zookeeper wrapper and then try to write our address to zookeeper.
    // We'll succeed if we are only  master or if we win the race when many
    // masters.  Otherwise we park here inside in writeAddressToZooKeeper.
    // TODO: Bring up the UI to redirect to active Master.
    zooKeeperWrapper.registerListener(this);
    this.zkMasterAddressWatcher =
        new ZKMasterAddressWatcher(this.zooKeeperWrapper, this);
    zooKeeperWrapper.registerListener(zkMasterAddressWatcher);

    // if we're a backup master, stall until a primary to writes his address
    if (conf.getBoolean(HConstants.MASTER_TYPE_BACKUP,
        HConstants.DEFAULT_MASTER_TYPE_BACKUP)) {
      // ephemeral node expiry will be detected between about 40 to 60 seconds;
      // (if the session timeout is set to 60 seconds)
      // plus add a little extra since only ZK leader can expire nodes, and
      // leader maybe a little  bit delayed in getting info about the pings.
      // Conservatively, just double the time.
      int stallTime = getZKSessionTimeOutForMaster(conf) * 2;

      LOG.debug("HMaster started in backup mode. Stall " + stallTime +
          "ms giving primary master a fair chance to be the master...");
      try {
        Thread.sleep(stallTime);
      } catch (InterruptedException e) {
        // interrupted = user wants to kill us.  Don't continue
        throw new IOException("Interrupted waiting for master address");
      }
    }

    final String masterName = getServerName();
    // initialize the thread pool for non-distributed log splitting.
    int maxSplitLogThread =
        conf.getInt("hbase.master.splitLogThread.max", 1000);
    logSplitThreadPool = Threads.getBoundedCachedThreadPool(
        maxSplitLogThread, 30L, TimeUnit.SECONDS,
        new ThreadFactory() {
          private int count = 1;
          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r, masterName + "-LogSplittingThread" + "-"
                + count++);
            if (!t.isDaemon())
              t.setDaemon(true);
            return t;
          }
        });

    regionPlacement = new RegionPlacement(this.conf);

    // Only read favored nodes if using the assignment-based load balancer.
    this.shouldAssignRegionsWithFavoredNodes = conf.getClass(
        HConstants.LOAD_BALANCER_IMPL, Object.class).equals(
            RegionManager.AssignmentLoadBalancer.class);
    LOG.debug("Whether to read the favoredNodes from meta: " +
        (shouldAssignRegionsWithFavoredNodes ? "Yes" : "No"));

    // Initialize table level lock manager for schema changes, if enabled.
    if (conf.getBoolean(HConstants.MASTER_SCHEMA_CHANGES_LOCK_ENABLE,
        HConstants.DEFAULT_MASTER_SCHEMA_CHANGES_LOCK_ENABLE)) {
      int schemaChangeLockTimeoutMs = conf.getInt(
          HConstants.MASTER_SCHEMA_CHANGES_LOCK_TIMEOUT_MS,
          HConstants.DEFAULT_MASTER_SCHEMA_CHANGES_LOCK_TIMEOUT_MS);
      tableLockManager = new TableLockManager(zooKeeperWrapper,
          address, schemaChangeLockTimeoutMs);

      this.schemaChangeTryLockTimeoutMs = conf.getInt(
          HConstants.MASTER_SCHEMA_CHANGES_TRY_LOCK_TIMEOUT_MS,
          HConstants.DEFAULT_MASTER_SCHEMA_CHANGES_TRY_LOCK_TIMEOUT_MS);

    } else {
      tableLockManager = null;
    }
  }

  @Override
  public void enableLoadBalancer() {
    this.isLoadBalancerDisabled.set(false);
    LOG.info("Enable the load balancer");
  }

  @Override
  public void disableLoadBalancer() {
    this.isLoadBalancerDisabled.set(true);
    LOG.info("Disable the load balancer");
  }

  @Override
  public boolean isLoadBalancerDisabled() {
    return this.isLoadBalancerDisabled.get();
  }

  private void initRpcServer(HServerAddress address) throws IOException {
    if (this.rpcServer != null) {
      LOG.info("Master RPC server is already initialized");
      return;
    }
    LOG.info("Initializing master RPC server at " + address);
    this.rpcServer = HBaseRPC.getServer(this, address.getBindAddress(),
        address.getPort(),
        conf.getInt("hbase.regionserver.handler.count", 10),
        false, conf);
  }

  public boolean shouldAssignRegionsWithFavoredNodes() {
    return shouldAssignRegionsWithFavoredNodes;
  }

  void startZKUnassignedWatcher() throws IOException {
    if (unassignedWatcher != null) {
      LOG.error("ZK unassigned watcher already started", new Throwable());
      return;
    }

    // Start the unassigned watcher - which will create the unassigned region
    // in ZK. If ZK unassigned events happen before the initial scan of the ZK unassigned
    // directory is complete, they will be queued for further processing.
    unassignedWatcher = new ZKUnassignedWatcher(this);
  }

  /**
   * @return true if successfully became primary master
   */
  private boolean waitToBecomePrimary() throws SocketException {
    if (!this.zkMasterAddressWatcher.writeAddressToZooKeeper(this.address,
        true)) {
      LOG.info("Failed to write master address to ZooKeeper, not starting (" +
          "closed=" + closed.get() + ")");
      zooKeeperWrapper.close();
      return false;
    }
    isActiveMaster = true;

    synchronized(this) {
      serverManager = new ServerManager(this);
    }
    this.getConfigurationManager().registerObserver(serverManager);

    this.regionServerOperationQueue =
        new RegionServerOperationQueue(this.conf, serverManager,
            getClosedStatus());

    // start the "close region" executor service
    HBaseEventType.RS2ZK_REGION_CLOSED.startMasterExecutorService(
        address.toString());
    // start the "open region" executor service
    HBaseEventType.RS2ZK_REGION_OPENED.startMasterExecutorService(
        address.toString());

    // start the region manager
    try {
      regionManager = new RegionManager(this);
    } catch (IOException e) {
      LOG.error("Failed to instantiate region manager");
      throw new RuntimeException(e);
    }

    this.metrics = new MasterMetrics(MASTER, this.serverManager);
    // We're almost open for business
    this.closed.set(false);
    LOG.info("HMaster w/ hbck initialized on " + this.address.toString());
    return true;
  }

  public ThreadPoolExecutor getLogSplitThreadPool() {
    return this.logSplitThreadPool;
  }

  private int getZKSessionTimeOutForMaster(Configuration conf) {
    int zkTimeout = conf.getInt("hbase.master.zookeeper.session.timeout", 0);
    if (zkTimeout != 0) {
      return zkTimeout;
    }
    return conf.getInt(HConstants.ZOOKEEPER_SESSION_TIMEOUT,
        HConstants.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT);
  }

  private void initializeZooKeeper() throws IOException {
    boolean abortProcesstIfZKExpired = conf.getBoolean(
        HConstants.ZOOKEEPER_SESSION_EXPIRED_ABORT_PROCESS, true);
    // Set this property to set zk session timeout for master which is different
    // from what region servers use. The master's zk session timeout can be
    // much shorter than region server's. It is easier to recycle master because
    // it doesn't handle data. The region server can have an inflated zk session
    // timeout because they also rely on master to kill them if they miss any
    // heartbeat
    int zkTimeout = conf.getInt("hbase.master.zookeeper.session.timeout", 0);
    Configuration localConf;
    if (zkTimeout != 0) {
      localConf = new Configuration(conf);
      localConf.setInt(HConstants.ZOOKEEPER_SESSION_TIMEOUT, zkTimeout);
    } else {
      localConf = conf;
    }
    if (abortProcesstIfZKExpired) {
      zooKeeperWrapper = ZooKeeperWrapper.createInstance(localConf,
          getZKWrapperName(), RuntimeHaltAbortStrategy.INSTANCE);
    } else {
      zooKeeperWrapper = ZooKeeperWrapper.createInstance(localConf,
          getZKWrapperName(), new Abortable() {

        @Override
        public void abort(String why, Throwable e) {
          stop("ZK session expired");
        }

        @Override
        public boolean isAborted() {
          return stopped;
        }
      });
    }
  }

  public long getApplyPreferredAssignmentPeriod() {
    return this.applyPreferredAssignmentPeriod;
  }

  public long getHoldRegionForBestLocalityPeriod() {
    return this.holdRegionForBestLocalityPeriod;
  }

  public long getMasterStartupTime() {
    return this.masterStartupTime;
  }

  public MapWritable getPreferredRegionToRegionServerMapping() {
    return preferredRegionToRegionServerMapping;
  }

  public void clearPreferredRegionToRegionServerMapping() {
    preferredRegionToRegionServerMapping = null;
  }

  /**
   * Returns true if this master process was responsible for starting the cluster. Only used in
   * unit tests.
   */
  @Deprecated
  boolean isClusterStartup() {
    return isClusterStartup;
  }

  public HServerAddress getHServerAddress() {
    return address;
  }

  /*
   * Get the rootdir.  Make sure its wholesome and exists before returning.
   * @param rd
   * @param conf
   * @param fs
   * @return hbase.rootdir (after checks for existence and bootstrapping if
   * needed populating the directory with necessary bootup files).
   * @throws IOException
   */
  private static Path checkRootDir(final Path rd, final Configuration c,
      final FileSystem fs)
          throws IOException {
    // If FS is in safe mode wait till out of it.
    FSUtils.waitOnSafeMode(c, c.getInt(HConstants.THREAD_WAKE_FREQUENCY,
        10 * 1000));
    // Filesystem is good. Go ahead and check for hbase.rootdir.
    if (!fs.exists(rd)) {
      fs.mkdirs(rd);
      FSUtils.setVersion(fs, rd);
    } else {
      FSUtils.checkVersion(fs, rd, true);
    }
    // Make sure the root region directory exists!
    if (!FSUtils.rootRegionExists(fs, rd)) {
      bootstrap(rd, c);
    }
    return rd;
  }

  private static void bootstrap(final Path rd, final Configuration c)
      throws IOException {
    LOG.info("BOOTSTRAP: creating ROOT and first META regions");
    try {
      // Bootstrapping, make sure blockcache is off.  Else, one will be
      // created here in bootstap and it'll need to be cleaned up.  Better to
      // not make it in first place.  Turn off block caching for bootstrap.
      // Enable after.
      HRegionInfo rootHRI = HTableDescriptor.isMetaregionSeqidRecordEnabled(c) ?
          new HRegionInfo(HRegionInfo.ROOT_REGIONINFO_WITH_HISTORIAN_COLUMN) :
            new HRegionInfo(HRegionInfo.ROOT_REGIONINFO);
          setInfoFamilyCaching(rootHRI, false);
          HRegionInfo metaHRI = new HRegionInfo(HRegionInfo.FIRST_META_REGIONINFO);
          setInfoFamilyCaching(metaHRI, false);
          HRegion root = HRegion.createHRegion(rootHRI, rd, c);
          HRegion meta = HRegion.createHRegion(metaHRI, rd, c);
          setInfoFamilyCaching(rootHRI, true);
          setInfoFamilyCaching(metaHRI, true);
          // Add first region from the META table to the ROOT region.
          HRegion.addRegionToMETA(root, meta);
          root.close();
          root.getLog().closeAndDelete();
          meta.close();
          meta.getLog().closeAndDelete();
    } catch (IOException e) {
      e = RemoteExceptionHandler.checkIOException(e);
      LOG.error("bootstrap", e);
      throw e;
    }
  }

  /*
   * @param hri Set all family block caching to <code>b</code>
   * @param b
   */
  private static void setInfoFamilyCaching(final HRegionInfo hri, final boolean b) {
    for (HColumnDescriptor hcd: hri.getTableDesc().families.values()) {
      if (Bytes.equals(hcd.getName(), HConstants.CATALOG_FAMILY)) {
        hcd.setBlockCacheEnabled(b);
        hcd.setInMemory(b);
      }
    }
  }

  /*
   * @return This masters' address.
   * @throws UnknownHostException
   */
  private static String getMyAddress(final Configuration c)
      throws UnknownHostException, SocketException {
    // Find out our address up in DNS.
    String s = DNS.getDefaultHost(c.get("hbase.master.dns.interface","default"),
        c.get("hbase.master.dns.nameserver","default"));
    if (preferIpv6AddressForMaster(c)) {
      // Use IPv6 address if possible.
      s = HServerInfo.getIPv6AddrIfLocalMachine(s);
    }
    s += ":" + c.get(HConstants.MASTER_PORT,
        Integer.toString(HConstants.DEFAULT_MASTER_PORT));
    return s;
  }

  /**
   * Checks to see if the file system is still accessible.
   * If not, sets closed
   * @return false if file system is not available
   */
  protected boolean checkFileSystem(boolean shutdownClusterOnFail) {
    if (this.fsOk) {
      try {
        // Do not close dfs client when check fails. It's OK to reuse the
        //  same dfs client when namenode comes back.
        FSUtils.checkFileSystemAvailable(this.fs, false);
      } catch (IOException e) {
        if (shutdownClusterOnFail) {
          LOG.fatal("Shutting down HBase cluster: file system not available", e);
          shutdownClusterNow();
          this.fsOk = false;
        }
        else {
          LOG.warn("File system unavailable, but continuing anyway", e);
          return false;
        }
      }
    }
    return this.fsOk;
  }

  /** @return HServerAddress of the master server */
  public HServerAddress getMasterAddress() {
    return this.address;
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion) {
    return HBaseRPCProtocolVersion.versionID;
  }

  /** @return InfoServer object. Maybe null.*/
  public InfoServer getInfoServer() {
    return this.infoServer;
  }

  /**
   * @return HBase root dir.
   * @throws IOException
   */
  public Path getRootDir() {
    return this.rootdir;
  }

  public int getNumRetries() {
    return this.numRetries;
  }

  /**
   * @return Server metrics
   */
  public MasterMetrics getMetrics() {
    return this.metrics;
  }

  /**
   * @return Return configuration being used by this server.
   */
  public Configuration getConfiguration() {
    return this.conf;
  }

  public ServerManager getServerManager() {
    return this.serverManager;
  }

  public RegionManager getRegionManager() {
    return this.regionManager;
  }

  int getThreadWakeFrequency() {
    return this.threadWakeFrequency;
  }

  FileSystem getFileSystem() {
    return this.fs;
  }

  AtomicBoolean getClosed() {
    return this.closed;
  }

  public boolean isClosed() {
    return this.closed.get();
  }

  ServerConnection getServerConnection() {
    return this.connection;
  }

  /**
   * Get the ZK wrapper object
   * @return the zookeeper wrapper
   */
  public ZooKeeperWrapper getZooKeeperWrapper() {
    return this.zooKeeperWrapper;
  }

  int numServers() {
    return this.serverManager.numServers();
  }

  public double getAverageLoad() {
    return this.serverManager.getAverageLoad();
  }

  public RegionServerOperationQueue getRegionServerOperationQueue () {
    return this.regionServerOperationQueue;
  }

  /**
   * Get the directory where old logs go
   * @return the dir
   */
  public Path getOldLogDir() {
    return this.oldLogDir;
  }

  /** Main processing loop */
  @Override
  public void run() {
    try {
      if (!waitToBecomePrimary()) {
        LOG.error("Failed to become primary -- not starting");
        return;
      }
    } catch (SocketException e) {
      LOG.error(e);
      return;
    }

    if (isStopped()) {
      LOG.info("Master is shutting down, not starting the main loop");
      return;
    }

    MonitoredTask startupStatus =
        TaskMonitor.get().createStatus("Master startup");
    startupStatus.setDescription("Master startup");
    clusterStateRecovery = new ZKClusterStateRecovery(this, connection);
    try {
      if (!isStopped() && !shouldAssignRegionsWithFavoredNodes()) {
        // This is only done if we are using the old mechanism for locality-based assignment
        // not relying on the favored node functionality in HDFS.
        initPreferredAssignment();
      }

      if (!isStopped()) {
        clusterStateRecovery.registerLiveRegionServers();
        isClusterStartup = clusterStateRecovery.isClusterStartup();  // for testing
      }

      if (!isStopped() || isClusterShutdownRequested()) {
        // Start the server so that region servers are running before we start
        // splitting logs and before we start assigning regions. XXX What will
        // happen if master starts receiving requests before regions are assigned?
        // NOTE: If the master bind port is 0 (e.g. in unit tests) we initialize the RPC server
        // earlier and do nothing here.
        // TODO: move this to startServiceThreads once we break the dependency of distributed log
        // splitting on regionserver check-in.
        initRpcServer(address);
        rpcServer.start();
      }

      if (!isStopped()) {
        splitLogAfterStartup();
      }

      if (!isStopped()) {
        startZKUnassignedWatcher();
      }

      if (!isStopped()) {
        startupStatus.setStatus("Initializing master service threads");
        startServiceThreads();
        masterStartupTime = System.currentTimeMillis();
        startupStatus.markComplete("Initialization successful");
      }
    } catch (IOException e) {
      LOG.fatal("Unhandled exception. Master quits.", e);
      startupStatus.cleanup();
      zooKeeperWrapper.close();
      return;
    }

    if (isStopped()) {
      startupStatus.markComplete("Initialization aborted, shutting down");
    }

    if (!isStopped()) {
      clusterStateRecovery.backgroundRecoverRegionStateFromZK();
    }

    try {
      /* Main processing loop */
      FINISHED: while (!this.closed.get()) {
        // check if we should be shutting down
        if (clusterShutdownRequested.get()) {
          // The region servers won't all exit until we stop scanning the
          // meta regions
          this.regionManager.stopScanners();
          if (this.serverManager.numServers() == 0) {
            startShutdown();
            break;
          } else {
            LOG.debug("Waiting on " +
                this.serverManager.getServersToServerInfo().keySet().toString());
          }
        }
        switch (this.regionServerOperationQueue.process()) {
        case FAILED:
          // If FAILED op processing, bad. Exit.
          break FINISHED;
        case REQUEUED_BUT_PROBLEM:
          // LOG if the file system is down, but don't do anything.
          checkFileSystem(false);
          break;
        default:
          // Continue run loop if conditions are PROCESSED, NOOP, REQUEUED
          break;
        }
      }
    } catch (Throwable t) {
      LOG.fatal("Unhandled exception. Starting cluster shutdown.", t);
      startupStatus.cleanup();
      shutdownClusterNow();
    }
    closed.set(true);

    startShutdown();
    startupStatus.cleanup();

    if (clusterShutdownRequested.get()) {
      // Wait for all the remaining region servers to report in. Only doing
      // this when the cluster is shutting down.
      this.serverManager.letRegionServersShutdown();
    }
    serverManager.joinThreads();

    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (Exception ex) {
        LOG.error("Error stopping info server", ex);
      }
    }
    if (this.rpcServer != null) {
      this.rpcServer.stop();
    }

    logSplitThreadPool.shutdown();

    regionManager.joinThreads();

    zooKeeperWrapper.close();
    HBaseExecutorService.shutdown();
    LOG.info("HMaster main thread exiting");
  }

  private void initPreferredAssignment() {
    // assign the regions based on the region locality in this period of time
    this.applyPreferredAssignmentPeriod =
        conf.getLong("hbase.master.applyPreferredAssignment.period",
            5 * 60 * 1000);

    // disable scanning dfs by setting applyPreferredAssignmentPeriod to 0
    if (applyPreferredAssignmentPeriod > 0) {
      // if a region's best region server hasn't checked in for this much time
      // since master startup, then the master is free to assign this region
      // out to any region server
      this.holdRegionForBestLocalityPeriod =
          conf.getLong("hbase.master.holdRegionForBestLocality.period",
              1 * 60 * 1000);

      // try to get the locality map from disk
      this.preferredRegionToRegionServerMapping = getRegionLocalityFromSnapshot(conf);

      // if we were not successful, let's reevaluate it
      if (this.preferredRegionToRegionServerMapping == null) {
        this.preferredRegionToRegionServerMapping = reevaluateRegionLocality(conf, null, conf.getInt("hbase.master.localityCheck.threadPoolSize", 5));
      }

    }
  }

  public static MapWritable getRegionLocalityFromSnapshot(Configuration conf) {
    String region_assignment_snapshot_dir =
        conf.get("hbase.tmp.dir");
    if (region_assignment_snapshot_dir == null) {
      return null;
    }

    String region_assignment_snapshot =
        region_assignment_snapshot_dir + "/" + LOCALITY_SNAPSHOT_FILE_NAME;

    long refresh_interval =
        conf.getLong("hbase.master.regionLocality.snapshot.validity_time_ms",
            24 * 60 * 60 * 1000);

    File snapshotFile = new File(region_assignment_snapshot);
    try {
      if (!snapshotFile.exists()) {
        LOG.info("preferredRegionToRegionServerMapping snapshot not found. File Path: "
            + region_assignment_snapshot);
        return null;
      }

      long time_elapsed = System.currentTimeMillis() - snapshotFile.lastModified();

      MapWritable regionLocalityMap = null;
      if (time_elapsed < refresh_interval) {
        // load the information from disk
        LOG.debug("Loading preferredRegionToRegionServerMapping from "
            + region_assignment_snapshot);
        regionLocalityMap = new MapWritable();
        regionLocalityMap.readFields(
            new DataInputStream(new FileInputStream(region_assignment_snapshot)));
        return regionLocalityMap;
      }
      else {
        LOG.info("Too long since last evaluated region-assignments. "
            + "Ignoring saved region-assignemnt."
            + " time_elapsed (ms) = " + time_elapsed + " refresh_interval is " + refresh_interval);
      }
      return null;
    }
    catch (IOException e) {
      LOG.error("Error loading the preferredRegionToRegionServerMapping  file: " +
          region_assignment_snapshot +  " from Disk : " + e.toString());
      // do not pause the master's construction
      return null;
    }

  }

  /*
   * Save a copy of the MapWritable regionLocalityMap.
   * The exact location to be stored is fetched from the Configuration given:
   * ${hbase.tmp.dir}/regionLocality-snapshot
   */
  public static MapWritable reevaluateRegionLocality(Configuration conf, String tablename, int poolSize) {
    MapWritable regionLocalityMap = null;

    LOG.debug("Evaluate preferredRegionToRegionServerMapping; expecting pause here");
    try {
      regionLocalityMap = FSUtils
          .getRegionLocalityMappingFromFS(FileSystem.get(conf), FSUtils.getRootDir(conf),
              poolSize,
              conf,
              tablename);
    } catch (Exception e) {
      LOG.error("Got unexpected exception when evaluating " +
          "preferredRegionToRegionServerMapping : " + e.toString());
      // do not pause the master's construction
      return null;
    }

    String tmp_path = conf.get("hbase.tmp.dir");
    if (tmp_path == null) {
      LOG.info("Could not save preferredRegionToRegionServerMapping  " +
          " config paramater hbase.tmp.dir is not set.");
      return regionLocalityMap;
    }

    String region_assignment_snapshot = tmp_path
        + "/" + LOCALITY_SNAPSHOT_FILE_NAME;
    // write the preferredRegionAssignment to disk
    try {
      LOG.info("Saving preferredRegionToRegionServerMapping  " +
          "to file " + region_assignment_snapshot );
      regionLocalityMap.write(new DataOutputStream(
          new FileOutputStream(region_assignment_snapshot)));
    } catch (IOException e) {
      LOG.error("Error saving preferredRegionToRegionServerMapping  " +
          "to file " + region_assignment_snapshot +  " : " + e.toString());
    }
    return regionLocalityMap;
  }

  private void startSplitLogManager() {
    if (this.distributedLogSplitting) {
      // splitLogManager must be started before starting rpcServer because
      // region-servers dying will trigger log splitting
      this.splitLogManager = new SplitLogManager(zooKeeperWrapper, conf,
          getStopper(), address.toString());
      this.splitLogManager.finishInitialization();
    }
  }

  /**
   * Inspect the log directory to recover any log file without an active region
   * server.
   */
  private void splitLogAfterStartup() {
    startSplitLogManager();
    boolean retrySplitting = !conf.getBoolean("hbase.hlog.split.skip.errors",
        HLog.SPLIT_SKIP_ERRORS_DEFAULT);
    List<String> serverNames = new ArrayList<String>();
    try {
      do {
        if (isStopped()) {
          LOG.warn("Master is shutting down, aborting log splitting");
          return;
        }
        try {
          Path logsDirPath =
              new Path(this.rootdir, HConstants.HREGION_LOGDIR_NAME);
          if (!this.fs.exists(logsDirPath)) {
            LOG.debug("Log directory " + logsDirPath
                + " does not exist, no logs to split");
            return;
          }
          FileStatus[] logFolders = this.fs.listStatus(logsDirPath);
          if (logFolders == null || logFolders.length == 0) {
            LOG.debug("No log files to split, proceeding...");
            return;
          }
          for (FileStatus status : logFolders) {
            Path logDir = status.getPath();
            String serverName = logDir.getName();
            LOG.info("Found log folder : " + serverName);
            if (!clusterStateRecovery.liveRegionServersAtStartup().contains(serverName)
                // If a server now checked in with the new master, don't kill it.
                && serverManager.getServerInfo(serverName) == null) {
              LOG.info("Log folder " + status.getPath() + " doesn't belong " +
                  "to a known region server, splitting");
              serverNames.add(serverName);
            } else {
              LOG.info("Log folder " + status.getPath() +
                  " belongs to an existing region server, not splitting");
            }
          }
          logDirsSplitOnStartup = serverNames;

          splitLog(serverNames);
          retrySplitting = false;
        } catch (IOException ioe) {
          LOG.warn("Failed splitting of " + serverNames, ioe);
          // reset serverNames
          serverNames = new ArrayList<String>();

          // if the file system is down, then just log it, and retry the log
          //  splitting after 30 seconds if retry splitting is turned on
          checkFileSystem(false);

          try {
            if (retrySplitting && !isStopped()) {
              Thread.sleep(30000); //30s
            }
          } catch (InterruptedException e) {
            LOG.warn("Interrupted, returning w/o splitting at startup");
            Thread.currentThread().interrupt();
            retrySplitting = false;
          }
        }
      } while (retrySplitting);
    } finally {
      isSplitLogAfterStartupDone.set(true);
    }
  }

  public boolean getIsSplitLogAfterStartupDone() {
    return (isSplitLogAfterStartupDone.get());
  }

  public void splitDeadServerLog(final String serverName) throws IOException {
    // Maintain the number of dead server split log requests for testing.
    numDeadServerLogSplitRequests.incrementAndGet();
    splitLog(Collections.singletonList(serverName));
  }

  public void splitLog(final List<String> serverNames) throws IOException {
    long splitTime = 0, splitLogSize = 0, splitCount = 0;
    List<Path> logDirs = new ArrayList<Path>();
    for (String serverName : serverNames) {
      if (isStopped()) {
        LOG.warn("Master is shutting down, stopping log directory scan");
        return;
      }
      Path logDir = new Path(this.rootdir,
          HLog.getHLogDirectoryName(serverName));
      // rename the directory so a rogue RS doesn't create more HLogs
      if (!serverName.endsWith(HConstants.HLOG_SPLITTING_EXT)) {
        Path splitDir = logDir.suffix(HConstants.HLOG_SPLITTING_EXT);
        if (fs.exists(logDir)) {
          if (!this.fs.rename(logDir, splitDir)) {
            throw new IOException("Failed fs.rename for log split: " + logDir);
          }
          LOG.debug("Renamed region directory: " + splitDir);
        } else if (!fs.exists(splitDir)) {
          LOG.info("Log dir for server " + serverName + " does not exist");
          continue;
        }
        logDir = splitDir;
      }
      logDirs.add(logDir);

      ContentSummary contentSummary;
      contentSummary = fs.getContentSummary(logDir);
      splitCount += contentSummary.getFileCount();
      splitLogSize += contentSummary.getSpaceConsumed();
    }
    if (logDirs.isEmpty()) {
      LOG.info("No logs to split");
      this.metrics.addSplit(0, 0, 0);
      return;
    }
    splitTime = EnvironmentEdgeManager.currentTimeMillis();
    if (distributedLogSplitting) {
      // handleDeadWorker() is also called in serverManager
      // it is ok to call handleDeadWorkers with "rsname.splitting". These
      // altered names will just get ignored
      for (String serverName : serverNames) {
        if (isStopped()) {
          LOG.warn("Master is shutting down, stopping distributed log " +
              "splitting");
          return;
        }
        splitLogManager.handleDeadServer(serverName);
      }
      splitLogManager.splitLogDistributed(logDirs);
    } else {
      // Serial log splitting.
      // splitLogLock ensures that dead region servers' logs are processed
      // one at a time
      for (Path logDir : logDirs) {
        if (isStopped()) {
          LOG.warn("Master is shutting down, stopping serial log splitting");
          return;
        }
        this.splitLogLock.lock();
        try {
          HLog.splitLog(this.rootdir, logDir, oldLogDir, this.fs,
              getConfiguration(), HLog.DEFAULT_LATEST_TS_TO_INCLUDE, this);
        } finally {
          this.splitLogLock.unlock();
        }
      }
    }
    splitTime = EnvironmentEdgeManager.currentTimeMillis() - splitTime;
    if (this.metrics != null) {
      this.metrics.addSplit(splitTime, splitCount, splitLogSize);
    }
  }

  /**
   * @return true if the master is shutting down (with or without shutting down
   *         the cluster)
   */
  @Override
  public boolean isStopped() {
    return stopped;
  }

  @Override
  public String getStopReason() {
    return stopReason;
  }

  /*
   * Start up all services. If any of these threads gets an unhandled exception
   * then they just die with a logged message.  This should be fine because
   * in general, we do not expect the master to get such unhandled exceptions
   *  as OOMEs; it should be lightly loaded. See what HRegionServer does if
   *  need to install an unexpected exception handler.
   */
  private void startServiceThreads() {
    try {
      this.regionManager.start();
      // Put up info server.
      int port = this.conf.getInt(HConstants.MASTER_INFO_PORT, 60010);
      if (port >= 0) {
        String a = this.conf.get("hbase.master.info.bindAddress", "0.0.0.0");
        this.infoServer = new InfoServer(MASTER, a, port, false, conf);
        this.infoServer.setAttribute(MASTER, this);
        this.infoServer.start();
        LOG.info("Master info server started at: " + a + ":"
            + this.infoServer.getPort());
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Started service threads");
      }
    } catch (IOException e) {
      if (e instanceof RemoteException) {
        try {
          e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        } catch (IOException ex) {
          LOG.warn("thread start", ex);
        }
      }
      // Something happened during startup. Stop the entire HBase cluster
      // without quiescing region servers.
      this.closed.set(true);
      LOG.error("Failed startup", e);
    }
  }

  /**
   * Start shutting down the master. This does NOT trigger a cluster shutdown.
   * However, if cluster shutdown has been triggered, this will indicate that
   * it is now OK to stop regionservers, as it sets the master to the "closed"
   * state. Therefore, only call this on cluster shutdown when all region
   * servers have been quiesced.
   */
  void startShutdown() {
    this.closed.set(true);
    this.regionManager.stopScanners();
    this.regionServerOperationQueue.shutdown();
    this.serverManager.notifyServers();
    if (splitLogManager != null) {
      splitLogManager.stop();
    }
  }

  @Override
  public MapWritable regionServerStartup(final HServerInfo serverInfo)
      throws IOException {
    // Set the ip into the passed in serverInfo.  Its ip is more than likely
    // not the ip that the master sees here.  See at end of this method where
    // we pass it back to the regionserver by setting "hbase.regionserver.address"
    String rsAddress = HBaseServer.getRemoteAddress();
    serverInfo.setServerAddress(new HServerAddress(rsAddress,
        serverInfo.getServerAddress().getPort()));
    // Register with server manager
    this.serverManager.regionServerStartup(serverInfo);
    // Send back some config info
    MapWritable mw = createConfigurationSubset();
    mw.put(new Text("hbase.regionserver.address"), new Text(rsAddress));
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
  public HMsg [] regionServerReport(HServerInfo serverInfo, HMsg msgs[],
      HRegionInfo[] mostLoadedRegions)
          throws IOException {
    return adornRegionServerAnswer(serverInfo,
        this.serverManager.regionServerReport(serverInfo, msgs, mostLoadedRegions));
  }

  void updateLastFlushedSequenceIds(HServerInfo serverInfo) {
    SortedMap<byte[], Long> flushedSequenceIds = (SortedMap<byte[], Long>) serverInfo.getFlushedSequenceIdByRegion();
    for (Entry<byte[], Long> entry : flushedSequenceIds.entrySet()) {
      Long existingValue = flushedSequenceIdByRegion.get(entry.getKey());
      if (existingValue != null) {
        if (entry.getValue() < existingValue) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("RegionServer " + serverInfo
                + " indicates a last flushed sequence id (" + entry.getValue()
                + ") that is less than the previous last flushed sequence id ("
                + existingValue + ") for region "
                + Bytes.toStringBinary(entry.getKey()) + " Ignoring.");
          }
          continue; // Don't let smaller sequence ids override greater
          // sequence ids.
        }
      }
      flushedSequenceIdByRegion.put(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Override if you'd add messages to return to regionserver <code>hsi</code>
   * or to send an exception.
   * @param msgs Messages to add to
   * @return Messages to return to
   * @throws IOException exceptions that were injected for the region servers
   */
  protected HMsg [] adornRegionServerAnswer(final HServerInfo hsi,
      final HMsg [] msgs) throws IOException {
    return msgs;
  }

  @Override
  public boolean isMasterRunning() {
    return !this.closed.get();
  }

  /**
   * This method's name should indicate that it will shut down the whole
   * cluster, but renaming it may break client/server compatibility on
   * upgrades.
   */
  @Override
  public void shutdown() {
    requestClusterShutdown();
  }

  /**
   * Request a shutdown the whole HBase cluster. This only modifies state
   * flags in memory and in ZK, so it is safe to be called multiple times.
   */
  @Override
  public void requestClusterShutdown() {
    if (!clusterShutdownRequested.compareAndSet(false, true)) {
      // Only request cluster shutdown once.
      return;
    }

    if (!closed.get()) {
      LOG.info("Cluster shutdown requested. Starting to quiesce servers");
    }
    this.zooKeeperWrapper.setClusterState(false);
    stopped = true;
    stopReason = "cluster shutdown";
  }

  /** Shutdown the cluster quickly, don't quiesce regionservers */
  private void shutdownClusterNow() {
    closed.set(true);
    requestClusterShutdown();
  }

  private boolean isTableLockEnabled() {
    return tableLockManager != null;
  }

  public boolean isTableLocked(byte[] tableName) {
    if (isTableLockEnabled()) {
      return tableLockManager.isTableLocked(tableName);
    }
    return false;
  }


  protected boolean tryLockTable(byte[] tableName, String purpose, long timeout)
      throws IOException {
    if (isTableLockEnabled()) {
      return tableLockManager.tryLockTable(tableName, purpose, timeout);
    }
    return false;
  }

  protected void lockTable(byte[] tableName, String purpose)
      throws IOException {
    if (isTableLockEnabled()) {
      tableLockManager.lockTable(tableName, purpose);
    }
  }

  protected void unlockTable(byte[] tableName)
      throws IOException {
    if (isTableLockEnabled()) {
      tableLockManager.unlockTable(tableName);
    }
  }

  /**
   * Creates regionInfo for the new regions that are going to be part of the new
   * table
   *
   * @param desc - descriptor of the new table
   * @param splitKeys - split keys marking the start and end key for every region
   * @return array of new HRegionInfo
   * @throws IOException
   */
  public HRegionInfo[] createRegionsForNewTable(HTableDescriptor desc,
      byte[][] splitKeys) throws IOException {
    if (!isMasterRunning()) {
      throw new MasterNotRunningException();
    }
    HRegionInfo[] newRegions = null;
    if (splitKeys == null || splitKeys.length == 0) {
      newRegions = new HRegionInfo[] { new HRegionInfo(desc, null, null) };
    } else {
      int numRegions = splitKeys.length + 1;
      newRegions = new HRegionInfo[numRegions];
      byte[] startKey = null;
      byte[] endKey = null;
      for (int i = 0; i < numRegions; i++) {
        endKey = (i == splitKeys.length) ? null : splitKeys[i];
        newRegions[i] = new HRegionInfo(desc, startKey, endKey);
        startKey = endKey;
      }
    }
    return newRegions;
  }

  @Override
  public void createTable(HTableDescriptor desc, byte [][] splitKeys)
      throws IOException {
    HRegionInfo[] newRegions = createRegionsForNewTable(desc, splitKeys);
    try {
      // We can not create a table unless meta regions have already been
      // assigned and scanned.
      if (!this.regionManager.areAllMetaRegionsOnline()) {
        throw new NotAllMetaRegionsOnlineException();
      }
      if (!this.serverManager.hasEnoughRegionServers()) {
        throw new IOException("not enough servers to create table yet");
      }
      createTable(newRegions);
      LOG.info("Succeeded in creating table " + desc.getNameAsString());
    } catch (TableExistsException e) {
      throw e;
    } catch (IOException e) {
      LOG.error("Cannot create table " + desc.getNameAsString() +
          " because of " + e.toString());
      throw RemoteExceptionHandler.checkIOException(e);
    }
  }

  private static boolean tableExists(HRegionInterface srvr,
      byte[] metaRegionName, String tableName)
          throws IOException {
    byte[] firstRowInTable = Bytes.toBytes(tableName + ",,");
    Scan scan = new Scan(firstRowInTable);
    scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    long scannerid = srvr.openScanner(metaRegionName, scan);
    try {
      Result data =
          BaseScanner.getOneResultFromScanner(srvr, scannerid);
      if (data != null && data.size() > 0) {
        HRegionInfo info = Writables.getHRegionInfo(
            data.getValue(HConstants.CATALOG_FAMILY,
                HConstants.REGIONINFO_QUALIFIER));
        if (info.getTableDesc().getNameAsString().equals(tableName)) {
          // A region for this table already exists. Ergo table exists.
          return true;
        }
      }
    } finally {
      srvr.close(scannerid);
    }
    return false;
  }

  /**
   * Create table.  If the HTableDescriptor has servers listed the regions will be placed
   * on those servers.
   *
   * @param newRegions - new regions from the new table
   * @throws IOException
   */
  private synchronized void createTable(final HRegionInfo[] newRegions) throws IOException {
    String tableName = newRegions[0].getTableDesc().getNameAsString();
    AssignmentPlan assignmentPlan = null;
    if (this.shouldAssignRegionsWithFavoredNodes) {
      // Get the assignment domain for this table
      AssignmentDomain domain = this.getAssignmentDomain(newRegions[0].getTableDesc());
      // Get the assignment plan for the new regions
      assignmentPlan = regionPlacement.getNewAssignmentPlan(newRegions, domain);
    }

    // 1. Check to see if table already exists. Get meta region where
    // table would sit should it exist. Open scanner on it. If a region
    // for the table we want to create already exists, then table already
    // created. Throw already-exists exception.
    MetaRegion m = regionManager.getFirstMetaRegionForRegion(newRegions[0]);
    byte [] metaRegionName = m.getRegionName();
    HRegionInterface srvr = this.connection.getHRegionConnection(m.getServer(),
        true, new HBaseRPCOptions());
    if (tableExists(srvr, metaRegionName, tableName)) {
      throw new TableExistsException(tableName);
    }
    byte [] tableNameBytes = Bytes.toBytes(tableName);
    lockTable(tableNameBytes, "create");
    try {
      InjectionHandler.processEvent(InjectionEvent.HMASTER_CREATE_TABLE);
      // After acquiring the lock, verify again that the table does not
      // exist.
      if (tableExists(srvr, metaRegionName, tableName)) {
        throw new TableExistsException(tableName);
      }
      if (assignmentPlan == null) {
        LOG.info("NO assignment plan for new table " + tableName);
      } else {
        LOG.info("Generated the assignment plan for new table " + tableName);
      }

      for(HRegionInfo newRegion : newRegions) {
        if (assignmentPlan != null) {
          // create the region with favorite nodes.
          List<HServerAddress> favoredNodes =
              assignmentPlan.getAssignment(newRegion);
          regionManager.createRegion(newRegion, srvr, metaRegionName,
              favoredNodes);
        } else {
          regionManager.createRegion(newRegion, srvr, metaRegionName);
        }
      }
      // kick off a meta scan right away to assign the newly created regions
      regionManager.metaScannerThread.triggerNow();
    } finally {
      unlockTable(tableNameBytes);
    }
  }

  /**
   * Get the assignment domain for the table.
   * Currently the domain would be generated by shuffling all the online
   * region servers.
   *
   * @param htd
   * @return the assignment domain for the table.
   */
  private AssignmentDomain getAssignmentDomain(HTableDescriptor htd) {
    return new AssignmentDomain(this.conf, htd, this.serverManager.getOnlineRegionServerList());
  }

  @Override
  public void deleteTable(final byte [] tableName) throws IOException {
    lockTable(tableName, "delete");
    try {
      InjectionHandler.processEvent(InjectionEvent.HMASTER_DELETE_TABLE);
      if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
        throw new IOException("Can't delete root table");
      }
      new TableDelete(this, tableName).process();
    } finally {
      unlockTable(tableName);
    }
    LOG.info("deleted table: " + Bytes.toStringBinary(tableName));
  }

  @Override
  /**
   * returns true if all .regioninfo files were succesfully rewritten after alteration
   */
  public boolean alterTable(final byte[] tableName, List<HColumnDescriptor> columnAdditions,
      List<Pair<byte[], HColumnDescriptor>> columnModifications, List<byte[]> columnDeletions,
      int waitInterval, int maxConcurrentRegionsClosed) throws IOException {
    // This lock will be released when the ThrottledRegionReopener is done.
    if (!tryLockTable(tableName, "alter", schemaChangeTryLockTimeoutMs)) {
      throw new TableLockTimeoutException("Timed out acquiring lock for "
          + Bytes.toStringBinary(tableName) + " after " + schemaChangeTryLockTimeoutMs + " ms.");
    }

    InjectionHandler.processEvent(InjectionEvent.HMASTER_ALTER_TABLE);
    ThrottledRegionReopener reopener =
        this.regionManager.createThrottledReopener(Bytes.toString(tableName), waitInterval,
          maxConcurrentRegionsClosed);
    // Regions are added to the reopener in MultiColumnOperation
    new MultiColumnOperation(this, tableName, columnAdditions, columnModifications, columnDeletions)
        .process();
    reopener.startRegionsReopening();

    // after the alteration, all .regioninfo files for this table will be old so we rewrite them.
    try {
      writeRegionInfo(tableName);
    } catch (Exception e) {
      LOG.error(
        "Failed to complete rewriting .regioninfo files for table "
            + Bytes.toStringBinary(tableName), e);
      return false;
    }
    return true;
  }

  @Override
  /**
   * Returns true if all backup .regioninfo files were successfully rewritten
   */
  public boolean alterTable(final byte [] tableName,
      List<HColumnDescriptor> columnAdditions,
      List<Pair<byte [], HColumnDescriptor>> columnModifications,
      List<byte []> columnDeletions) throws IOException {

    int waitInterval = conf.getInt(HConstants.MASTER_SCHEMA_CHANGES_WAIT_INTERVAL_MS,
        HConstants.DEFAULT_MASTER_SCHEMA_CHANGES_WAIT_INTERVAL_MS);

    int maxClosedRegions = conf.getInt(HConstants.MASTER_SCHEMA_CHANGES_MAX_CONCURRENT_REGION_CLOSE,
        HConstants.DEFAULT_MASTER_SCHEMA_CHANGES_MAX_CONCURRENT_REGION_CLOSE);

    return alterTable(tableName, columnAdditions, columnModifications, columnDeletions, waitInterval, maxClosedRegions);
  }

  /**
   * This method rewrites all .regioninfo files for all regions belonging to the Table whose name is
   * tableName
   * @param tableName
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public void writeRegionInfo(byte[] tableName) throws IOException, InterruptedException{
    List<Pair<HRegionInfo, HServerAddress>> tableRegions = getTableRegions(tableName);
    ExecutorService executor = Executors.newCachedThreadPool();
    List<Future<Void>> futures = new ArrayList<>();
    for (final Pair<HRegionInfo, HServerAddress> entry : tableRegions) {
      Callable<Void> writer = new Callable<Void>() {
        
        public Void call() throws IOException {
          HRegionInfo hri = entry.getFirst();
            hri.writeToDisk(conf);
          return null;
        }
      };
      futures.add(executor.submit(writer));
    }
    for (Future<Void> f : futures) {
      try {
        f.get();
      } catch (ExecutionException e) {
        throw ExceptionUtils.toIOException(e.getCause());
      }
    }
  }

  @Override
  public Pair<Integer, Integer> getAlterStatus(byte [] tableName)
      throws IOException {
    Pair <Integer, Integer> p = new Pair<Integer, Integer>(0,0);
    if (regionManager.getThrottledReopener(Bytes.toString(tableName)) != null) {
      p = regionManager.getThrottledReopener(
          Bytes.toString(tableName)).getReopenStatus();
    } else {
      // Table is not reopening any regions return (0,0)
    }
    return p;
  }

  @Override
  public void addColumn(byte [] tableName, HColumnDescriptor column)
      throws IOException {
    alterTable(tableName, Arrays.asList(column), null, null);
  }

  @Override
  public void modifyColumn(byte [] tableName, byte [] columnName,
      HColumnDescriptor descriptor)
          throws IOException {
    alterTable(tableName, null, Arrays.asList(
        new Pair<byte[], HColumnDescriptor>(columnName, descriptor)), null);
  }

  @Override
  public void deleteColumn(final byte [] tableName, final byte [] c)
      throws IOException {
    alterTable(tableName, null, null,
        Arrays.asList(KeyValue.parseColumn(c)[0]));
  }

  @Override
  public void enableTable(final byte [] tableName) throws IOException {
    lockTable(tableName, "enable");
    try {
      InjectionHandler.processEvent(InjectionEvent.HMASTER_ENABLE_TABLE);
      if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
        throw new IOException("Can't enable root table");
      }
      new ChangeTableState(this, tableName, true).process();
    } finally {
      unlockTable(tableName);
    }
  }

  @Override
  public void disableTable(final byte [] tableName) throws IOException {
    lockTable(tableName, "disable");
    try {
      InjectionHandler.processEvent(InjectionEvent.HMASTER_DISABLE_TABLE);
      if (Bytes.equals(tableName, HConstants.ROOT_TABLE_NAME)) {
        throw new IOException("Can't disable root table");
      }
      new ChangeTableState(this, tableName, false).process();
    } finally {
      unlockTable(tableName);
    }
  }

  /**
   * Get a list of the regions for a given table. The pairs may have
   * null for their second element in the case that they are not
   * currently deployed.
   * TODO: Redo so this method does not duplicate code with subsequent methods.
   */
  public List<Pair<HRegionInfo,HServerAddress>> getTableRegions(
      final byte [] tableName)
          throws IOException {
    final ArrayList<Pair<HRegionInfo, HServerAddress>> result =
        Lists.newArrayList();

    if (!Bytes.equals(HConstants.META_TABLE_NAME, tableName)) {
      MetaScannerVisitor visitor =
          new MetaScannerVisitor() {
        @Override
        public boolean processRow(Result data) throws IOException {
          if (data == null || data.size() <= 0)
            return true;
          Pair<HRegionInfo, HServerAddress> pair =
              metaRowToRegionPair(data);
          if (pair == null) return false;
          if (!Bytes.equals(pair.getFirst().getTableDesc().getName(),
              tableName)) {
            return false;
          }
          result.add(pair);
          return true;
        }
      };

      MetaScanner.metaScan(conf, visitor, new StringBytes(tableName));
    }
    else {
      List<MetaRegion> metaRegions = regionManager.getListOfOnlineMetaRegions();
      for (MetaRegion mRegion: metaRegions) {
        if (Bytes.equals(mRegion.getRegionInfo().getTableDesc().getName(), tableName)) {
          result.add(new Pair<HRegionInfo, HServerAddress>
          (mRegion.getRegionInfo(), mRegion.getServer()));
        }
      }
    }
    return result;
  }

  private Pair<HRegionInfo, HServerAddress> metaRowToRegionPair(
      Result data) throws IOException {
    HRegionInfo info = Writables.getHRegionInfo(
        data.getValue(HConstants.CATALOG_FAMILY,
            HConstants.REGIONINFO_QUALIFIER));
    final byte[] value = data.getValue(HConstants.CATALOG_FAMILY,
        HConstants.SERVER_QUALIFIER);
    if (value != null && value.length > 0) {
      HServerAddress server = new HServerAddress(Bytes.toString(value));
      return new Pair<HRegionInfo,HServerAddress>(info, server);
    } else {
      //undeployed
      return new Pair<HRegionInfo, HServerAddress>(info, null);
    }
  }

  /**
   * Return the region and current deployment for the region containing
   * the given row. If the region cannot be found, returns null. If it
   * is found, but not currently deployed, the second element of the pair
   * may be null.
   */
  Pair<HRegionInfo,HServerAddress> getTableRegionForRow(
      final byte [] tableName, final byte [] rowKey)
          throws IOException {
    final AtomicReference<Pair<HRegionInfo, HServerAddress>> result =
        new AtomicReference<Pair<HRegionInfo, HServerAddress>>(null);

    MetaScannerVisitor visitor =
        new MetaScannerVisitor() {
      @Override
      public boolean processRow(Result data) throws IOException {
        if (data == null || data.size() <= 0)
          return true;
        Pair<HRegionInfo, HServerAddress> pair =
            metaRowToRegionPair(data);
        if (pair == null) return false;
        if (!Bytes.equals(pair.getFirst().getTableDesc().getName(),
            tableName)) {
          return false;
        }
        result.set(pair);
        return true;
      }
    };

    MetaScanner.metaScan(conf, visitor, new StringBytes(tableName), rowKey, 1);
    return result.get();
  }

  @SuppressWarnings("deprecation")
  Pair<HRegionInfo,HServerAddress> getTableRegionFromName(
      final byte [] regionName)
          throws IOException {
    byte [] tableName = HRegionInfo.parseRegionName(regionName)[0];

    Set<MetaRegion> regions = regionManager.getMetaRegionsForTable(tableName);
    for (MetaRegion m: regions) {
      byte [] metaRegionName = m.getRegionName();
      HRegionInterface srvr = connection.getHRegionConnection(m.getServer());
      Get get = new Get(regionName);
      get.addColumn(HConstants.CATALOG_FAMILY,
          HConstants.REGIONINFO_QUALIFIER);
      get.addColumn(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
      Result data = srvr.get(metaRegionName, get);
      if(data == null || data.size() <= 0) continue;
      return metaRowToRegionPair(data);
    }
    return null;
  }

  /**
   * Get row from meta table.
   * @param row
   * @param family
   * @return Result
   * @throws IOException
   */
  protected Result getFromMETA(final byte[] row, final byte[] family)
      throws IOException {
    MetaRegion meta = this.regionManager.getMetaRegionForRow(row);
    HRegionInterface srvr = getMETAServer(meta);
    Get get = new Get.Builder(row).addFamily(family).create();
    return srvr.get(meta.getRegionName(), get);
  }

  /*
   * @param meta
   * @return Server connection to <code>meta</code> .META. region.
   * @throws IOException
   */
  private HRegionInterface getMETAServer(final MetaRegion meta)
      throws IOException {
    return this.connection.getHRegionConnection(meta.getServer());
  }

  /**
   * Method for getting the tableDescriptor
   * @param tableName as a byte []
   * @return the tableDescriptor
   * @throws IOException if a remote or network exception occurs
   */
  public HTableDescriptor getTableDescriptor(final byte [] tableName)
      throws IOException {
    return this.connection.getHTableDescriptor(new StringBytes(tableName));
  }

  @Override
  public void modifyTable(final byte[] tableName, HConstants.Modify op,
      Writable[] args)
          throws IOException {
    switch (op) {
    case TABLE_SET_HTD:
      if (args == null || args.length < 1 ||
      !(args[0] instanceof HTableDescriptor))
        throw new IOException("SET_HTD request requires an HTableDescriptor");
      HTableDescriptor htd = (HTableDescriptor) args[0];
      LOG.info("modifyTable(SET_HTD): " + htd);
      new ModifyTableMeta(this, tableName, htd).process();
      break;

    case TABLE_SPLIT:
    case TABLE_COMPACT:
    case TABLE_MAJOR_COMPACT:
    case TABLE_FLUSH:
      if (args != null && args.length > 0) {
        if (!(args[0] instanceof ImmutableBytesWritable))
          throw new IOException(
              "request argument must be ImmutableBytesWritable");
        Pair<HRegionInfo,HServerAddress> pair = null;
        if(tableName == null) {
          byte [] regionName = ((ImmutableBytesWritable)args[0]).get();
          pair = getTableRegionFromName(regionName);
        } else {
          byte [] rowKey = ((ImmutableBytesWritable)args[0]).get();
          pair = getTableRegionForRow(tableName, rowKey);
        }
        LOG.info("About to " + op.toString() + " on "
            + Bytes.toStringBinary(tableName) + " and pair is " + pair);
        if (pair != null && pair.getSecond() != null) {
          // If the column family name is specified, we need to perform a
          // column family specific action instead of an action on the whole
          // region. For this purpose the second value in args is the column
          // family name.
          if (args.length == 2) {
            byte[] regionTableName = HRegionInfo.parseRegionName(
                pair.getFirst().getRegionName())[0];
            byte [] columnFamily = ((ImmutableBytesWritable)args[1]).get();
            if (getTableDescriptor(regionTableName).hasFamily(columnFamily)) {
              this.regionManager.startCFAction(pair.getFirst().getRegionName(),
                  columnFamily, pair.getFirst(), pair.getSecond(), op);
            }
          } else {
            this.regionManager.startAction(pair.getFirst().getRegionName(),
                pair.getFirst(), pair.getSecond(), op);
          }
        }
      } else {
        for (Pair<HRegionInfo,HServerAddress> pair: getTableRegions(tableName)) {
          if (pair.getSecond() == null) continue; // undeployed
          this.regionManager.startAction(pair.getFirst().getRegionName(),
              pair.getFirst(), pair.getSecond(), op);
        }
      }
      break;

      // format : {tableName row | region} splitPoint
    case TABLE_EXPLICIT_SPLIT:
      if (args == null || args.length < (tableName == null? 2 : 1)) {
        throw new IOException("incorrect number of arguments given");
      }
      Pair<HRegionInfo,HServerAddress> pair = null;
      byte[] splitPoint = null;

      // split a single region
      if(tableName == null) {
        byte [] regionName = ((ImmutableBytesWritable)args[0]).get();
        pair = getTableRegionFromName(regionName);
        splitPoint = ((ImmutableBytesWritable)args[1]).get();
      } else {
        splitPoint = ((ImmutableBytesWritable)args[0]).get();
        pair = getTableRegionForRow(tableName, splitPoint);
      }
      if (pair == null) {
        throw new IOException("couldn't find RegionInfo from region name");
      } else if (splitPoint == null) {
        throw new IOException("must give explicit split point");
      } else if (!pair.getFirst().containsRow(splitPoint)) {
        throw new IOException("split point outside specified region's range");
      }
      HRegionInfo r = pair.getFirst();
      r.setSplitPoint(splitPoint);
      LOG.info("About to " + op.toString() + " on "
          + Bytes.toStringBinary(pair.getFirst().getTableDesc().getName())
          + " at " + Bytes.toStringBinary(splitPoint) + " and pair is "
          + pair);
      if (pair.getSecond() != null) {
        this.regionManager.startAction(pair.getFirst().getRegionName(),
            pair.getFirst(), pair.getSecond(), Modify.TABLE_SPLIT);
      }
      break;

    case MOVE_REGION: {
      if (args == null || args.length != 2) {
        throw new IOException("Requires a region name and a hostname");
      }
      // Arguments are region name and an region server hostname.
      byte [] regionname = ((ImmutableBytesWritable)args[0]).get();

      // Need hri
      Result rr = getFromMETA(regionname, HConstants.CATALOG_FAMILY);
      HRegionInfo hri = getHRegionInfo(rr.getRow(), rr);
      String hostnameAndPort = Bytes.toString(((ImmutableBytesWritable)args[1]).get());
      HServerAddress serverAddress = new HServerAddress(hostnameAndPort);

      if (hri == null) {
        throw new IOException("Cannot locate " +
            Bytes.toStringBinary(regionname) + " in .META. Move failed");
      }

      this.regionManager.getAssignmentManager().
      addTransientAssignment(serverAddress, hri);
      // Close the region so that it will be re-opened by the preferred host.
      modifyTable(tableName, HConstants.Modify.CLOSE_REGION, new Writable[]{args[0]});

      break;
    }

    case CLOSE_REGION:
      if (args == null || args.length < 1 || args.length > 2) {
        throw new IOException("Requires at least a region name; " +
            "or cannot have more than region name and servername");
      }
      // Arguments are regionname and an optional server name.
      byte [] regionname = ((ImmutableBytesWritable)args[0]).get();
      LOG.debug("Attempting to close region: " + Bytes.toStringBinary(regionname));
      String hostnameAndPort = null;
      if (args.length == 2) {
        hostnameAndPort = Bytes.toString(((ImmutableBytesWritable)args[1]).get());
      }
      // Need hri
      Result rr = getFromMETA(regionname, HConstants.CATALOG_FAMILY);
      HRegionInfo hri = getHRegionInfo(rr.getRow(), rr);
      if (hostnameAndPort == null) {
        // Get server from the .META. if it wasn't passed as argument
        hostnameAndPort =
            Bytes.toString(rr.getValue(HConstants.CATALOG_FAMILY,
                HConstants.SERVER_QUALIFIER));
      }
      // Take region out of the intransistions in case it got stuck there doing
      // an open or whatever.
      this.regionManager.clearFromInTransition(regionname);
      // If hostnameAndPort is still null, then none, exit.
      if (hostnameAndPort == null) break;
      long startCode =
          Bytes.toLong(rr.getValue(HConstants.CATALOG_FAMILY,
              HConstants.STARTCODE_QUALIFIER));
      String name = HServerInfo.getServerName(hostnameAndPort, startCode);
      LOG.info("Marking " + hri.getRegionNameAsString() +
          " as closing on " + name + "; cleaning SERVER + STARTCODE; " +
          "master will tell regionserver to close region on next heartbeat");
      this.regionManager.setClosing(name, hri, hri.isOffline());
      break;

    default:
      throw new IOException("unsupported modifyTable op " + op);
    }
  }

  /**
   * @return cluster status
   */
  @Override
  public ClusterStatus getClusterStatus() {
    ClusterStatus status = new ClusterStatus();
    status.setHBaseVersion(VersionInfo.getVersion());
    List<HServerInfo> serverInfoCopy = Lists.newArrayList();
    for (HServerInfo hsi : serverManager.getServersToServerInfo().values()) {
      HServerInfo hsiCopy = new HServerInfo(hsi);
      hsiCopy.setSendSequenceIds(false);
      serverInfoCopy.add(hsiCopy);
    }
    status.setServerInfo(serverInfoCopy);
    status.setDeadServers(serverManager.getDeadServers());
    status.setRegionsInTransition(this.regionManager.getRegionsInTransition());
    return status;
  }

  /**
   * Get HRegionInfo from passed META map of row values.
   * Returns null if none found (and logs fact that expected COL_REGIONINFO
   * was missing).  Utility method used by scanners of META tables.
   * @param row name of the row
   * @param res Result to use to do lookup.
   * @return Null or found HRegionInfo.
   * @throws IOException
   */
  static HRegionInfo getHRegionInfo(final byte[] row, final Result res)
      throws IOException {
    byte[] regioninfo = res.getValue(HConstants.CATALOG_FAMILY,
        HConstants.REGIONINFO_QUALIFIER);

    if (regioninfo == null) {
      StringBuilder sb =  new StringBuilder();
      NavigableMap<byte[], byte[]> infoMap =
          res.getFamilyMap(HConstants.CATALOG_FAMILY);
      if (infoMap == null) {
        return null;
      }
      for (byte [] e: infoMap.keySet()) {
        if (sb.length() > 0) {
          sb.append(", ");
        }
        sb.append(Bytes.toString(HConstants.CATALOG_FAMILY) + ":"
            + Bytes.toString(e));
      }
      LOG.warn(Bytes.toString(HConstants.CATALOG_FAMILY) + ":" +
          Bytes.toString(HConstants.REGIONINFO_QUALIFIER)
          + " is empty for row: " + Bytes.toString(row) + "; has keys: "
          + sb.toString());
      return null;
    }
    return Writables.getHRegionInfo(regioninfo);
  }

  /*
   * When we find rows in a meta region that has an empty HRegionInfo, we
   * clean them up here.
   *
   * @param s connection to server serving meta region
   * @param metaRegionName name of the meta region we scanned
   * @param emptyRows the row keys that had empty HRegionInfos
   */
  protected void deleteEmptyMetaRows(HRegionInterface s,
      byte [] metaRegionName,
      List<byte []> emptyRows) {
    for (byte [] regionName: emptyRows) {
      try {
        HRegion.removeRegionFromMETA(s, metaRegionName, regionName);
        LOG.warn("Removed region: " + Bytes.toStringBinary(regionName)
            + " from meta region: " + Bytes.toStringBinary(metaRegionName)
            + " because HRegionInfo was empty");
      } catch (IOException e) {
        LOG.error("deleting region: " + Bytes.toStringBinary(regionName)
            + " from meta region: " + Bytes.toStringBinary(metaRegionName), e);
      }
    }
  }


  /**
   * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
   */
  @Override
  public void process(WatchedEvent event) {
    // no-op now
  }

  private static void printUsageAndExit() {
    System.err.println("Usage: Master [opts] start|stop");
    System.err.println(" start  Start Master. If local mode, start Master and RegionServer in same JVM");
    System.err.println(" stop   Start cluster shutdown; Master signals RegionServer shutdown");
    System.err.println(" where [opts] are:");
    System.err.println("   --minServers=<servers>    Minimum RegionServers needed to host user tables.");
    System.err.println("   -D opt=<value>            Override HBase configuration settings.");
    System.exit(0);
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
    } catch (Exception e) {
      throw new RuntimeException("Failed construction of " +
          "Master: " + masterClass.toString() +
          ((e.getCause() != null)? e.getCause().getMessage(): ""), e);
    }
  }

  /*
   * Version of master that will shutdown the passed zk cluster on its way out.
   */
  public static class LocalHMaster extends HMaster {
    private MiniZooKeeperCluster zkcluster = null;

    public LocalHMaster(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    public void run() {
      super.run();
      if (this.zkcluster != null) {
        try {
          this.zkcluster.shutdown();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    void setZKCluster(final MiniZooKeeperCluster zkcluster) {
      this.zkcluster = zkcluster;
    }
  }

  protected static void doMain(String [] args,
      Class<? extends HMaster> masterClass) {
    Configuration conf = HBaseConfiguration.create();

    Options opt = new Options();
    opt.addOption("minServers", true, "Minimum RegionServers needed to host user tables");
    opt.addOption("D", true, "Override HBase Configuration Settings");
    opt.addOption("backup", false, "Do not try to become HMaster until the primary fails");
    try {
      CommandLine cmd = new GnuParser().parse(opt, args);

      if (cmd.hasOption("minServers")) {
        String val = cmd.getOptionValue("minServers");
        conf.setInt("hbase.regions.server.count.min",
            Integer.valueOf(val));
        LOG.debug("minServers set to " + val);
      }

      if (cmd.hasOption("D")) {
        for (String confOpt : cmd.getOptionValues("D")) {
          String[] kv = confOpt.split("=", 2);
          if (kv.length == 2) {
            conf.set(kv[0], kv[1]);
            LOG.debug("-D configuration override: " + kv[0] + "=" + kv[1]);
          } else {
            throw new ParseException("-D option format invalid: " + confOpt);
          }
        }
      }

      // check if we are the backup master - override the conf if so
      if (cmd.hasOption("backup")) {
        conf.setBoolean(HConstants.MASTER_TYPE_BACKUP, true);
      }

      if (cmd.getArgList().contains("start")) {
        try {
          // Print out vm stats before starting up.
          RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
          if (runtime != null) {
            LOG.info("vmName=" + runtime.getVmName() + ", vmVendor=" +
                runtime.getVmVendor() + ", vmVersion=" + runtime.getVmVersion());
            LOG.info("vmInputArguments=" + runtime.getInputArguments());
          }
          // If 'local', defer to LocalHBaseCluster instance.  Starts master
          // and regionserver both in the one JVM.
          if (LocalHBaseCluster.isLocal(conf)) {
            final MiniZooKeeperCluster zooKeeperCluster =
                new MiniZooKeeperCluster();
            File zkDataPath = new File(conf.get("hbase.zookeeper.property.dataDir"));
            int zkClientPort = conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, 0);
            if (zkClientPort == 0) {
              throw new IOException("No config value for hbase.zookeeper.property.clientPort");
            }
            zooKeeperCluster.setTickTime(conf.getInt("hbase.zookeeper.property.tickTime", 3000));
            zooKeeperCluster.setClientPort(zkClientPort);
            int clientPort = zooKeeperCluster.startup(zkDataPath);
            if (clientPort != zkClientPort) {
              String errorMsg = "Couldnt start ZK at requested address of " +
                  zkClientPort + ", instead got: " + clientPort + ". Aborting. Why? " +
                  "Because clients (eg shell) wont be able to find this ZK quorum";
              System.err.println(errorMsg);
              throw new IOException(errorMsg);
            }
            conf.set(HConstants.ZOOKEEPER_CLIENT_PORT,
                Integer.toString(clientPort));
            // Need to have the zk cluster shutdown when master is shutdown.
            // Run a subclass that does the zk cluster shutdown on its way out.
            LocalHBaseCluster cluster = new LocalHBaseCluster(conf, 1, 1,
                LocalHMaster.class, HRegionServer.class);
            ((LocalHMaster)cluster.getMaster()).setZKCluster(zooKeeperCluster);
            cluster.startup();
          } else {
            HMaster master = constructMaster(masterClass, conf);
            if (master.clusterShutdownRequested.get()) {
              LOG.info("Won't bring the Master up as a shutdown is requested");
              return;
            }
            master.start();
          }
        } catch (Throwable t) {
          LOG.error("Failed to start master", t);
          System.exit(-1);
        }
      } else if (cmd.getArgList().contains("stop")) {
        HBaseAdmin adm = null;
        try {
          adm = new HBaseAdmin(conf);
        } catch (MasterNotRunningException e) {
          LOG.error("Master not running");
          System.exit(0);
        }
        try {
          adm.shutdown();
        } catch (Throwable t) {
          LOG.error("Failed to stop master", t);
          System.exit(-1);
        }
      } else {
        throw new ParseException("Unknown argument(s): " +
            org.apache.commons.lang.StringUtils.join(cmd.getArgs(), " "));
      }
    } catch (ParseException e) {
      LOG.error("Could not parse: ", e);
      printUsageAndExit();
    }
  }

  public Map<String, Integer> getTableFragmentation() throws IOException {
    long now = System.currentTimeMillis();
    // only check every two minutes by default
    int check = this.conf.getInt("hbase.master.fragmentation.check.frequency", 2 * 60 * 1000);
    if (lastFragmentationQuery == -1 || now - lastFragmentationQuery > check) {
      fragmentation = FSUtils.getTableFragmentation(this);
      lastFragmentationQuery = now;
    }
    return fragmentation;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
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

  public String getServerName() {
    return HMaster.class.getSimpleName() + "-" + address.toString();
  }

  @Override
  public String toString() {
    return getServerName();
  }

  /**
   * Main program
   * @param args
   */
  public static void main(String [] args) {
    doMain(args, HMaster.class);
  }

  @Override
  public void clearFromTransition(HRegionInfo region) {
    this.regionManager.clearFromInTransition(region.getRegionName());
    LOG.info("Cleared region " + region + " from transition map");
  }

  /**
   * Stop master without shutting down the cluster. Gets out of the master loop
   * quickly. Does not quiesce regionservers.
   */
  @Override
  public void stop(String why) {
    LOG.info("Master stop requested, isActiveMaster=" + isActiveMaster
        + ", reason=" + why);
    stopped = true;
    stopReason = why;

    // Get out of the master loop.
    closed.set(true);

    // If we are a backup master, we need to interrupt wait
    if (!isActiveMaster) {
      zkMasterAddressWatcher.cancelMasterZNodeWait();
    }

    synchronized(this) {
      if (serverManager != null) {
        serverManager.requestShutdown();
      }
    }
  }

  @Override
  public long getLastFlushedSequenceId(byte[] regionName) throws IOException {
    if (flushedSequenceIdByRegion.containsKey(regionName)) {
      return flushedSequenceIdByRegion.get(regionName);
    }
    return -1;
  }

  String getZKWrapperName() {
    return getServerName();
  }

  public SplitLogManager getSplitLogManager() {
    return this.splitLogManager;
  }

  List<String> getLogDirsSplitOnStartup() {
    return logDirsSplitOnStartup;
  }

  int getNumDeadServerLogSplitRequests() {
    return numDeadServerLogSplitRequests.get();
  }

  @Override
  public boolean isClusterShutdownRequested() {
    return clusterShutdownRequested.get();
  }

  public StoppableMaster getStopper() {
    return this;
  }

  public StopStatus getClosedStatus() {
    return new StopStatus() {
      @Override
      public boolean isStopped() {
        return closed.get();
      }
    };
  }

  ZKUnassignedWatcher getUnassignedWatcher() {
    return unassignedWatcher;
  }

  @Override
  public void clearAllBlacklistedServers() {
    ServerManager.clearRSBlacklist();
  }

  @Override
  public void clearBlacklistedServer(String hostAndPort) {
    ServerManager.removeServerFromBlackList(hostAndPort);
  }

  @Override
  public void addServerToBlacklist(String hostAndPort) {
    // Don't assign new regions to this server
    ServerManager.blacklistRSHostPort(hostAndPort);
  }

  @Override
  public boolean isServerBlackListed(final String hostAndPort) {
    return ServerManager.isServerBlackListed(hostAndPort);
  }

  @Override
  public void updateConfiguration() {
    LOG.info("Reloading the configuration from disk.");
    conf.reloadConfiguration();
    getConfigurationManager().notifyAllObservers(conf);
  }

  public ConfigurationManager getConfigurationManager() {
    return configurationManager;
  }

  public static boolean useThriftMasterToRS(Configuration c) {
    return c.getBoolean(HConstants.MASTER_TO_RS_USE_THRIFT,
        HConstants.MASTER_TO_RS_USE_THRIFT_DEFAULT);
  }

  @Override
  public void close() throws Exception {
  }

  /**
   * @param conf
   * @return true if the hmaster.prefer.ipv6.address is set
   */
  public static boolean preferIpv6AddressForMaster(Configuration conf) {
    return conf != null ? conf.getBoolean(HMASTER_PREFER_IPV6_Address, false) : false;
  }
}

