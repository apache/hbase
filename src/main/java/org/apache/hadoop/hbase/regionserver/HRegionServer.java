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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.management.ObjectName;

import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CallSequenceOutOfOrderException;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.ClockOutOfSyncException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HealthCheckChore;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterAddressTracker;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.UnknownRowLockException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.catalog.RootLocationEditor;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.MultiResponse;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.UserProvider;
import org.apache.hadoop.hbase.client.coprocessor.Exec;
import org.apache.hadoop.hbase.client.coprocessor.ExecResult;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorService.ExecutorType;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheColumnFamilySummary;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HBaseRPCErrorHandler;
import org.apache.hadoop.hbase.ipc.HBaseRpcMetrics;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.HMasterRegionInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;
import org.apache.hadoop.hbase.ipc.RpcEngine;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionServerInfo;
import org.apache.hadoop.hbase.regionserver.Leases.LeaseStillHeldException;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.handler.CloseMetaHandler;
import org.apache.hadoop.hbase.regionserver.handler.CloseRegionHandler;
import org.apache.hadoop.hbase.regionserver.handler.CloseRootHandler;
import org.apache.hadoop.hbase.regionserver.handler.OpenMetaHandler;
import org.apache.hadoop.hbase.regionserver.handler.OpenRegionHandler;
import org.apache.hadoop.hbase.regionserver.handler.OpenRootHandler;
import org.apache.hadoop.hbase.regionserver.metrics.RegionMetricsStorage;
import org.apache.hadoop.hbase.regionserver.metrics.RegionServerDynamicMetrics;
import org.apache.hadoop.hbase.regionserver.metrics.RegionServerMetrics;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics.StoreMetricType;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionServerSnapshotManager;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CompressionTest;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.ClusterId;
import org.apache.hadoop.hbase.zookeeper.ClusterStatusTracker;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * HRegionServer makes a set of HRegions available to clients. It checks in with
 * the HMaster. There are many HRegionServers in a single HBase deployment.
 */
public class HRegionServer implements HRegionInterface, HBaseRPCErrorHandler,
    Runnable, RegionServerServices {

  public static final Log LOG = LogFactory.getLog(HRegionServer.class);

  // Set when a report to the master comes back with a message asking us to
  // shutdown. Also set by call to stop when debugging or running unit tests
  // of HRegionServer in isolation.
  protected volatile boolean stopped = false;

  // A state before we go into stopped state.  At this stage we're closing user
  // space regions.
  private boolean stopping = false;

  // Go down hard. Used if file system becomes unavailable and also in
  // debugging and unit tests.
  protected volatile boolean abortRequested;

  private volatile boolean killed = false;

  // If false, the file system has become unavailable
  protected volatile boolean fsOk;

  protected final Configuration conf;

  protected final AtomicBoolean haveRootRegion = new AtomicBoolean(false);
  private HFileSystem fs;
  private boolean useHBaseChecksum; // verify hbase checksums?
  private Path rootDir;
  private final Random rand;

  //RegionName vs current action in progress
  //true - if open region action in progress
  //false - if close region action in progress
  private final ConcurrentSkipListMap<byte[], Boolean> regionsInTransitionInRS =
      new ConcurrentSkipListMap<byte[], Boolean>(Bytes.BYTES_COMPARATOR);

  /**
   * Map of regions currently being served by this region server. Key is the
   * encoded region name.  All access should be synchronized.
   */
  protected final Map<String, HRegion> onlineRegions =
    new ConcurrentHashMap<String, HRegion>();

  protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  final int numRetries;
  protected final int threadWakeFrequency;
  private final int msgInterval;

  protected final int numRegionsToReport;

  private final long maxScannerResultSize;

  // Remote HMaster
  private HMasterRegionInterface hbaseMaster;

  // RPC Engine for master connection
  private RpcEngine rpcEngine;

  // Server to handle client requests. Default access so can be accessed by
  // unit tests.
  RpcServer rpcServer;

  // Server to handle client requests.
  private HBaseServer server;  

  private final InetSocketAddress isa;
  private UncaughtExceptionHandler uncaughtExceptionHandler;

  // Leases
  private Leases leases;

  // Request counter.
  // Do we need this?  Can't we just sum region counters?  St.Ack 20110412
  private AtomicInteger requestCount = new AtomicInteger();

  // Info server. Default access so can be used by unit tests. REGIONSERVER
  // is name of the webapp and the attribute name used stuffing this instance
  // into web context.
  InfoServer infoServer;

  /** region server process name */
  public static final String REGIONSERVER = "regionserver";
  
  /** region server configuration name */
  public static final String REGIONSERVER_CONF = "regionserver_conf";

  /*
   * Space is reserved in HRS constructor and then released when aborting to
   * recover from an OOME. See HBASE-706. TODO: Make this percentage of the heap
   * or a minimum.
   */
  private final LinkedList<byte[]> reservedSpace = new LinkedList<byte[]>();

  private RegionServerMetrics metrics;

  private RegionServerDynamicMetrics dynamicMetrics;

  // Compactions
  public CompactSplitThread compactSplitThread;

  // Cache flushing
  MemStoreFlusher cacheFlusher;

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

  private final boolean separateHLogForMeta;

  // flag set after we're done setting up server threads (used for testing)
  protected volatile boolean isOnline;

  final Map<String, RegionScannerHolder> scanners =
    new ConcurrentHashMap<String, RegionScannerHolder>();

  // zookeeper connection and watcher
  private ZooKeeperWatcher zooKeeper;

  // master address manager and watcher
  private MasterAddressTracker masterAddressManager;

  // catalog tracker
  private CatalogTracker catalogTracker;

  // Cluster Status Tracker
  private ClusterStatusTracker clusterStatusTracker;

  // Log Splitting Worker
  private SplitLogWorker splitLogWorker;

  // A sleeper that sleeps for msgInterval.
  private final Sleeper sleeper;

  private final int rpcTimeout;

  // Instance of the hbase executor service.
  private ExecutorService service;

  // Replication services. If no replication, this handler will be null.
  private ReplicationSourceService replicationSourceHandler;
  private ReplicationSinkService replicationSinkHandler;

  private final RegionServerAccounting regionServerAccounting;

  // Cache configuration and block cache reference
  private final CacheConfig cacheConfig;

  // reference to the Thrift Server.
  volatile private HRegionThriftServer thriftServer;

  /**
   * The server name the Master sees us as.  Its made from the hostname the
   * master passes us, port, and server startcode. Gets set after registration
   * against  Master.  The hostname can differ from the hostname in {@link #isa}
   * but usually doesn't if both servers resolve .
   */
  private ServerName serverNameFromMasterPOV;

  // region server static info like info port
  private RegionServerInfo.Builder rsInfo;

  /**
   * This servers startcode.
   */
  private final long startcode;

  /**
   * Go here to get table descriptors.
   */
  private TableDescriptors tableDescriptors;

  /*
   * Strings to be used in forming the exception message for
   * RegionsAlreadyInTransitionException.
   */
  private static final String OPEN = "OPEN";
  private static final String CLOSE = "CLOSE";

  /**
   * MX Bean for RegionServerInfo
   */
  private ObjectName mxBean = null;

  /**
   * ClusterId
   */
  private ClusterId clusterId = null;

  private RegionServerCoprocessorHost rsHost;

  /** The health check chore. */
  private HealthCheckChore healthCheckChore;

  /**
   * Starts a HRegionServer at the default location
   *
   * @param conf
   * @throws IOException
   * @throws InterruptedException
   */
  public HRegionServer(Configuration conf)
  throws IOException, InterruptedException {
    this.fsOk = true;
    this.conf = conf;
    // Set how many times to retry talking to another server over HConnection.
    HConnectionManager.setServerSideHConnectionRetries(this.conf, LOG);
    this.isOnline = false;
    checkCodecs(this.conf);

    // do we use checksum verfication in the hbase? If hbase checksum verification
    // is enabled, then we automatically switch off hdfs checksum verification.
    this.useHBaseChecksum = conf.getBoolean(
      HConstants.HBASE_CHECKSUM_VERIFICATION, false);

    // Config'ed params
    this.separateHLogForMeta = conf.getBoolean(HLog.SEPARATE_HLOG_FOR_META, false);
    this.numRetries = conf.getInt("hbase.client.retries.number", 10);
    this.threadWakeFrequency = conf.getInt(HConstants.THREAD_WAKE_FREQUENCY,
      10 * 1000);
    this.msgInterval = conf.getInt("hbase.regionserver.msginterval", 3 * 1000);

    this.sleeper = new Sleeper(this.msgInterval, this);

    this.maxScannerResultSize = conf.getLong(
      HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY,
      HConstants.DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE);

    this.numRegionsToReport = conf.getInt(
      "hbase.regionserver.numregionstoreport", 10);

    this.rpcTimeout = conf.getInt(
      HConstants.HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY,
      HConstants.DEFAULT_HBASE_RPC_SHORTOPERATION_TIMEOUT);

    this.abortRequested = false;
    this.stopped = false;

    // Server to handle client requests.
    String hostname = conf.get("hbase.regionserver.ipc.address",
      Strings.domainNamePointerToHostName(DNS.getDefaultHost(
        conf.get("hbase.regionserver.dns.interface", "default"),
        conf.get("hbase.regionserver.dns.nameserver", "default"))));
    int port = conf.getInt(HConstants.REGIONSERVER_PORT,
      HConstants.DEFAULT_REGIONSERVER_PORT);
    // Creation of a HSA will force a resolve.
    InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);
    if (initialIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initialIsa);
    }

    this.rand = new Random(initialIsa.hashCode());
    this.rpcServer = HBaseRPC.getServer(this,
      new Class<?>[]{HRegionInterface.class, HBaseRPCErrorHandler.class,
        OnlineRegions.class},
        initialIsa.getHostName(), // BindAddress is IP we got for this server.
        initialIsa.getPort(),
        conf.getInt("hbase.regionserver.handler.count", 10),
        conf.getInt("hbase.regionserver.metahandler.count", 10),
        conf.getBoolean("hbase.rpc.verbose", false),
        conf, HConstants.QOS_THRESHOLD);
    if (rpcServer instanceof HBaseServer) server = (HBaseServer) rpcServer;
    // Set our address.
    this.isa = this.rpcServer.getListenerAddress();

    this.rpcServer.setErrorHandler(this);
    this.rpcServer.setQosFunction(new QosFunction());
    this.startcode = System.currentTimeMillis();

    conf.set("hbase.regionserver.rpc.client.socket.bind.address", this.isa.getHostName());

    // login the zookeeper client principal (if using security)
    ZKUtil.loginClient(this.conf, "hbase.zookeeper.client.keytab.file",
      "hbase.zookeeper.client.kerberos.principal", this.isa.getHostName());

    // login the server principal (if using secure Hadoop)
    UserProvider provider = UserProvider.instantiate(conf);
    provider.login("hbase.regionserver.keytab.file",
      "hbase.regionserver.kerberos.principal", this.isa.getHostName());
    regionServerAccounting = new RegionServerAccounting();
    cacheConfig = new CacheConfig(conf);
    uncaughtExceptionHandler = new UncaughtExceptionHandler() {
      public void uncaughtException(Thread t, Throwable e) {
        abort("Uncaught exception in service thread " + t.getName(), e);
      }
    };
    this.rsInfo = RegionServerInfo.newBuilder();
    // Put up the webui.  Webui may come up on port other than configured if
    // that port is occupied. Adjust serverInfo if this is the case.
    this.rsInfo.setInfoPort(putUpWebUI());
  }

  /** Handle all the snapshot requests to this server */
  RegionServerSnapshotManager snapshotManager;

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


  @Retention(RetentionPolicy.RUNTIME)
  private @interface QosPriority {
    int priority() default 0;
  }

  /**
   * Utility used ensuring higher quality of service for priority rpcs; e.g.
   * rpcs to .META. and -ROOT-, etc.
   */
  class QosFunction implements Function<Writable,Integer> {
    private final Map<String, Integer> annotatedQos;

    public QosFunction() {
      Map<String, Integer> qosMap = new HashMap<String, Integer>();
      for (Method m : HRegionServer.class.getMethods()) {
        QosPriority p = m.getAnnotation(QosPriority.class);
        if (p != null) {
          qosMap.put(m.getName(), p.priority());
        }
      }

      annotatedQos = qosMap;
    }

    public boolean isMetaTable(byte[] regionName) {
      HRegion region;
      try {
        region = getRegion(regionName);
      } catch (NotServingRegionException ignored) {
        return false;
      }
      return region.getRegionInfo().isMetaTable();
    }

    @Override
    public Integer apply(Writable from) {
      if (!(from instanceof Invocation)) return HConstants.NORMAL_QOS;

      Invocation inv = (Invocation) from;
      String methodName = inv.getMethodName();

      Integer priorityByAnnotation = annotatedQos.get(methodName);
      if (priorityByAnnotation != null) {
        return priorityByAnnotation;
      }

      // scanner methods...
      if (methodName.equals("next") || methodName.equals("close")) {
        // translate!
        Long scannerId;
        try {
          scannerId = (Long) inv.getParameters()[0];
        } catch (ClassCastException ignored) {
          // LOG.debug("Low priority: " + from);
          return HConstants.NORMAL_QOS;
        }
        String scannerIdString = Long.toString(scannerId);
        RegionScannerHolder holder = scanners.get(scannerIdString);
        if (holder != null && holder.getScanner().getRegionInfo().isMetaRegion()) {
          // LOG.debug("High priority scanner request: " + scannerId);
          return HConstants.HIGH_QOS;
        }
      } else if (inv.getParameterClasses().length == 0) {
       // Just let it through.  This is getOnlineRegions, etc.
      } else if (inv.getParameterClasses()[0] == byte[].class) {
        // first arg is byte array, so assume this is a regionname:
        if (isMetaTable((byte[]) inv.getParameters()[0])) {
          // LOG.debug("High priority with method: " + methodName +
          // " and region: "
          // + Bytes.toString((byte[]) inv.getParameters()[0]));
          return HConstants.HIGH_QOS;
        }
      } else if (inv.getParameterClasses()[0] == MultiAction.class) {
        MultiAction<?> ma = (MultiAction<?>) inv.getParameters()[0];
        Set<byte[]> regions = ma.getRegions();
        // ok this sucks, but if any single of the actions touches a meta, the
        // whole
        // thing gets pingged high priority. This is a dangerous hack because
        // people
        // can get their multi action tagged high QOS by tossing a Get(.META.)
        // AND this
        // regionserver hosts META/-ROOT-
        for (byte[] region : regions) {
          if (isMetaTable(region)) {
            // LOG.debug("High priority multi with region: " +
            // Bytes.toString(region));
            return HConstants.HIGH_QOS; // short circuit for the win.
          }
        }
      }
      // LOG.debug("Low priority: " + from.toString());
      return HConstants.NORMAL_QOS;
    }
  }

  /**
   * All initialization needed before we go register with Master.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  private void preRegistrationInitialization(){
    try {
      initializeZooKeeper();

      clusterId = new ClusterId(zooKeeper, this);
      if(clusterId.hasId()) {
        conf.set(HConstants.CLUSTER_ID, clusterId.getId());
      }

      initializeThreads();
      int nbBlocks = conf.getInt("hbase.regionserver.nbreservationblocks", 4);
      for (int i = 0; i < nbBlocks; i++) {
        reservedSpace.add(new byte[HConstants.DEFAULT_SIZE_RESERVATION_BLOCK]);
      }

      this.rpcEngine = HBaseRPC.getProtocolEngine(conf);
    } catch (Throwable t) {
      // Call stop if error or process will stick around for ever since server
      // puts up non-daemon threads.
      this.rpcServer.stop();
      abort("Initialization of RS failed.  Hence aborting RS.", t);
    }
  }

  /**
   * Bring up connection to zk ensemble and then wait until a master for this
   * cluster and then after that, wait until cluster 'up' flag has been set.
   * This is the order in which master does things.
   * Finally put up a catalog tracker.
   * @throws IOException
   * @throws InterruptedException
   */
  private void initializeZooKeeper() throws IOException, InterruptedException {
    // Open connection to zookeeper and set primary watcher
    this.zooKeeper = new ZooKeeperWatcher(conf, REGIONSERVER + ":" +
      this.isa.getPort(), this);

    // Create the master address manager, register with zk, and start it.  Then
    // block until a master is available.  No point in starting up if no master
    // running.
    this.masterAddressManager = new MasterAddressTracker(this.zooKeeper, this);
    this.masterAddressManager.start();
    blockAndCheckIfStopped(this.masterAddressManager);

    // Wait on cluster being up.  Master will set this flag up in zookeeper
    // when ready.
    this.clusterStatusTracker = new ClusterStatusTracker(this.zooKeeper, this);
    this.clusterStatusTracker.start();
    blockAndCheckIfStopped(this.clusterStatusTracker);

    // Create the catalog tracker and start it;
    this.catalogTracker = new CatalogTracker(this.zooKeeper, this.conf, this);
    catalogTracker.start();

    // watch for snapshots
    try {
      this.snapshotManager = new RegionServerSnapshotManager(this);
    } catch (KeeperException e) {
      this.abort("Failed to reach zk cluster when creating snapshot handler.");
    }
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
    return this.clusterStatusTracker.isClusterUp();
  }

  private void initializeThreads() throws IOException {
    // Cache flushing thread.
    this.cacheFlusher = new MemStoreFlusher(conf, this);

    // Compaction thread
    this.compactSplitThread = new CompactSplitThread(this);

    // Background thread to check for compactions; needed if region
    // has not gotten updates in a while. Make it run at a lesser frequency.
    int multiplier = this.conf.getInt(HConstants.THREAD_WAKE_FREQUENCY +
      ".multiplier", 1000);
    this.compactionChecker = new CompactionChecker(this,
      this.threadWakeFrequency * multiplier, this);

    this.periodicFlusher = new PeriodicMemstoreFlusher(this.threadWakeFrequency, this);

    // Health checker thread.
    int sleepTime = this.conf.getInt(HConstants.HEALTH_CHORE_WAKE_FREQ,
      HConstants.DEFAULT_THREAD_WAKE_FREQUENCY);
    if (isHealthCheckerConfigured()) {
      healthCheckChore = new HealthCheckChore(sleepTime, this, getConfiguration());
    }

    this.leases = new Leases((int) conf.getLong(
        HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,
        HConstants.DEFAULT_HBASE_REGIONSERVER_LEASE_PERIOD),
        this.threadWakeFrequency);

    // Create the thread for the ThriftServer.
    if (conf.getBoolean("hbase.regionserver.export.thrift", false)) {
      thriftServer = new HRegionThriftServer(this, conf);
      thriftServer.start();
      LOG.info("Started Thrift API from Region Server.");
    }
  }

  /**
   * The HRegionServer sticks in this loop until closed.
   */
  @SuppressWarnings("deprecation")
  public void run() {
    try {
      // Do pre-registration initializations; zookeeper, lease threads, etc.
      preRegistrationInitialization();
    } catch (Throwable e) {
      abort("Fatal exception during initialization", e);
    }

    try {
      // Try and register with the Master; tell it we are here.  Break if
      // server is stopped or the clusterup flag is down or hdfs went wacky.
      while (keepLooping()) {
        MapWritable w = reportForDuty();
        if (w == null) {
          LOG.warn("reportForDuty failed; sleeping and then retrying.");
          this.sleeper.sleep();
        } else {
          handleReportForDutyResponse(w);
          break;
        }
      }
      registerMBean();

      // start the snapshot handler, since the server is ready to run
      this.snapshotManager.start();

      // We registered with the Master.  Go into run mode.
      long lastMsg = 0;
      long oldRequestCount = -1;
      // The main run loop.
      while (!this.stopped && isHealthy()) {
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
          doMetrics();
          tryRegionServerReport();
          lastMsg = System.currentTimeMillis();
        }
        if (!this.stopped) this.sleeper.sleep();
      } // for
    } catch (Throwable t) {
      if (!checkOOME(t)) {
        abort("Unhandled exception: " + t.getMessage(), t);
      }
    }
    // Run shutdown.
    if (mxBean != null) {
      MBeanUtil.unregisterMBean(mxBean);
      mxBean = null;
    }
    if (this.thriftServer != null) this.thriftServer.shutdown();
    this.leases.closeAfterLeasesExpire();
    this.rpcServer.stop();
    if (this.splitLogWorker != null) {
      splitLogWorker.stop();
    }
    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    // Send cache a shutdown.
    if (cacheConfig.isBlockCacheEnabled()) {
      cacheConfig.getBlockCache().shutdown();
    }

    // Send interrupts to wake up threads if sleeping so they notice shutdown.
    // TODO: Should we check they are alive? If OOME could have exited already
    if (this.cacheFlusher != null) this.cacheFlusher.interruptIfNecessary();
    if (this.compactSplitThread != null) this.compactSplitThread.interruptIfNecessary();
    if (this.hlogRoller != null) this.hlogRoller.interruptIfNecessary();
    if (this.metaHLogRoller != null) this.metaHLogRoller.interruptIfNecessary();
    if (this.compactionChecker != null)
      this.compactionChecker.interrupt();
    if (this.healthCheckChore != null) {
      this.healthCheckChore.interrupt();
    }

    // Stop the snapshot handler, forcefully killing all running tasks
    try {
      if (snapshotManager != null) snapshotManager.stop(this.abortRequested || this.killed);
    } catch (IOException e) {
      LOG.warn("Failed to close snapshot handler cleanly", e);
    }

    if (this.killed) {
      // Just skip out w/o closing regions.  Used when testing.
    } else if (abortRequested) {
      if (this.fsOk) {
        closeUserRegions(abortRequested); // Don't leave any open file handles
      }
      LOG.info("aborting server " + this.serverNameFromMasterPOV);
    } else {
      closeUserRegions(abortRequested);
      closeAllScanners();
      LOG.info("stopping server " + this.serverNameFromMasterPOV);
    }
    // Interrupt catalog tracker here in case any regions being opened out in
    // handlers are stuck waiting on meta or root.
    if (this.catalogTracker != null) this.catalogTracker.stop();

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
      LOG.info("stopping server " + this.serverNameFromMasterPOV +
        "; all regions closed.");
    }

    //fsOk flag may be changed when closing regions throws exception.
    if (!this.killed && this.fsOk) {
      closeWAL(abortRequested ? false : true);
    }

    // Make sure the proxy is down.
    this.hbaseMaster = null;
    this.rpcEngine.close();
    this.leases.close();

    if (!killed) {
      join();
    }

    try {
      deleteMyEphemeralNode();
    } catch (KeeperException e) {
      LOG.warn("Failed deleting my ephemeral node", e);
    }
    this.zooKeeper.close();
    LOG.info("stopping server " + this.serverNameFromMasterPOV +
      "; zookeeper connection closed.");

    LOG.info(Thread.currentThread().getName() + " exiting");
  }

  private boolean containsMetaTableRegions() {
    return onlineRegions.containsKey(HRegionInfo.ROOT_REGIONINFO.getEncodedName())
        || onlineRegions.containsKey(HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
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

  void tryRegionServerReport()
  throws IOException {
    if (!keepLooping() && hbaseMaster == null) {
      // the current server is stopping
      return;
    }
    HServerLoad hsl = buildServerLoad();
    // Why we do this?
    this.requestCount.set(0);
    try {
      this.hbaseMaster.regionServerReport(this.serverNameFromMasterPOV.getVersionedBytes(), hsl);
    } catch (IOException ioe) {
      if (ioe instanceof RemoteException) {
        ioe = ((RemoteException)ioe).unwrapRemoteException();
      }
      if (ioe instanceof YouAreDeadException) {
        // This will be caught and handled as a fatal error in run()
        throw ioe;
      }
      // Couldn't connect to the master, get location from zk and reconnect
      // Method blocks until new master is found or we are stopped
      getMaster();
    }
  }

  HServerLoad buildServerLoad() {
    Collection<HRegion> regions = getOnlineRegionsLocalContext();
    TreeMap<byte [], HServerLoad.RegionLoad> regionLoads =
      new TreeMap<byte [], HServerLoad.RegionLoad>(Bytes.BYTES_COMPARATOR);
    for (HRegion region: regions) {
      regionLoads.put(region.getRegionName(), createRegionLoad(region));
    }
    MemoryUsage memory =
      ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    return new HServerLoad(requestCount.get(),(int)metrics.getRequests(),
      (int)(memory.getUsed() / 1024 / 1024),
      (int) (memory.getMax() / 1024 / 1024), regionLoads,
      this.hlog.getCoprocessorHost().getCoprocessors());
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
          closeRegion(hri, abort, false);
        }
      }
      // No regions in RIT, we could stop waiting now.
      if (this.regionsInTransitionInRS.isEmpty()) {
        if (!isOnlineRegionsEmpty()) {
          LOG.info("We were exiting though online regions are not empty, because some regions failed closing");
        }
        break;
      }
      Threads.sleep(200);
    }
  }

  private void closeWAL(final boolean delete) {
    if (this.hlogForMeta != null) {
      // All hlogs (meta and non-meta) are in the same directory. Don't call
      // closeAndDelete here since that would delete all hlogs not just the
      // meta ones. We will just 'close' the hlog for meta here, and leave
      // the directory cleanup to the follow-on closeAndDelete call.
      try { //Part of the patch from HBASE-7982 to do with exception handling 
        this.hlogForMeta.close();
      } catch (Throwable e) {
        LOG.error("Metalog close and delete failed", RemoteExceptionHandler.checkThrowable(e));
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
        LOG.error("Close and delete failed", RemoteExceptionHandler.checkThrowable(e));
      }
    }
  }

  private void closeAllScanners() {
    // Close any outstanding scanners. Means they'll get an UnknownScanner
    // exception next time they come in.
    for (Map.Entry<String, RegionScannerHolder> e : this.scanners.entrySet()) {
      try {
        e.getValue().getScanner().close();
      } catch (IOException ioe) {
        LOG.warn("Closing scanner " + e.getKey(), ioe);
      }
    }
  }

  /*
   * Run init. Sets up hlog and starts up all server threads.
   *
   * @param c Extra configuration.
   */
  protected void handleReportForDutyResponse(final MapWritable c)
  throws IOException {
    try {
      for (Map.Entry<Writable, Writable> e :c.entrySet()) {
        String key = e.getKey().toString();
        // The hostname the master sees us as.
        if (key.equals(HConstants.KEY_FOR_HOSTNAME_SEEN_BY_MASTER)) {
          String hostnameFromMasterPOV = e.getValue().toString();
          this.serverNameFromMasterPOV = new ServerName(hostnameFromMasterPOV,
            this.isa.getPort(), this.startcode);
          LOG.info("Master passed us hostname to use. Was=" +
            this.isa.getHostName() + ", Now=" +
            this.serverNameFromMasterPOV.getHostname());
          continue;
        }
        String value = e.getValue().toString();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Config from master: " + key + "=" + value);
        }
        this.conf.set(key, value);
      }

      // hack! Maps DFSClient => RegionServer for logs.  HDFS made this
      // config param for task trackers, but we can piggyback off of it.
      if (this.conf.get("mapred.task.id") == null) {
        this.conf.set("mapred.task.id", "hb_rs_" +
          this.serverNameFromMasterPOV.toString());
      }
      // Set our ephemeral znode up in zookeeper now we have a name.
      createMyEphemeralNode();

      // Master sent us hbase.rootdir to use. Should be fully qualified
      // path with file system specification included. Set 'fs.defaultFS'
      // to match the filesystem on hbase.rootdir else underlying hadoop hdfs
      // accessors will be going against wrong filesystem (unless all is set
      // to defaults).
      this.conf.set("fs.defaultFS", this.conf.get("hbase.rootdir"));
      // Get fs instance used by this RS
      this.fs = new HFileSystem(this.conf, this.useHBaseChecksum);
      this.rootDir = new Path(this.conf.get(HConstants.HBASE_DIR));
      this.tableDescriptors = new FSTableDescriptors(this.fs, this.rootDir, true);
      this.hlog = setupWALAndReplication();
      // Init in here rather than in constructor after thread name has been set
      this.metrics = new RegionServerMetrics();
      this.dynamicMetrics = RegionServerDynamicMetrics.newInstance(this);
      this.rsHost = new RegionServerCoprocessorHost(this, this.conf);
      startServiceThreads();
      LOG.info("Serving as " + this.serverNameFromMasterPOV +
        ", RPC listening on " + this.isa +
        ", sessionid=0x" +
        Long.toHexString(this.zooKeeper.getRecoverableZooKeeper().getSessionId()));
      isOnline = true;
    } catch (Throwable e) {
      LOG.warn("Exception in region server : ", e);
      this.isOnline = false;
      stop("Failed initialization");
      throw convertThrowableToIOE(cleanup(e, "Failed init"),
          "Region server startup failed");
    } finally {
      sleeper.skipSleepCycle();
    }
  }

  private String getMyEphemeralNodePath() {
    return ZKUtil.joinZNode(this.zooKeeper.rsZNode, getServerName().toString());
  }

  private void createMyEphemeralNode() throws KeeperException, IOException {
    byte[] data = ProtobufUtil.prependPBMagic(rsInfo.build().toByteArray());
    ZKUtil.createEphemeralNodeAndWatch(this.zooKeeper,
      getMyEphemeralNodePath(), data);
  }

  private void deleteMyEphemeralNode() throws KeeperException {
    ZKUtil.deleteNode(this.zooKeeper, getMyEphemeralNodePath());
  }

  public RegionServerAccounting getRegionServerAccounting() {
    return regionServerAccounting;
  }

  /*
   * @param r Region to get RegionLoad for.
   *
   * @return RegionLoad instance.
   *
   * @throws IOException
   */
  private HServerLoad.RegionLoad createRegionLoad(final HRegion r) {
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
    return new HServerLoad.RegionLoad(name, stores, storefiles,
        storeUncompressedSizeMB,
        storefileSizeMB, memstoreSizeMB, storefileIndexSizeMB, rootIndexSizeKB,
        totalStaticIndexSizeKB, totalStaticBloomSizeKB,
        (int) r.readRequestsCount.get(), (int) r.writeRequestsCount.get(),
        totalCompactingKVs, currentCompactedKVs);
  }

  /**
   * @param encodedRegionName
   * @return An instance of RegionLoad.
   */
  public HServerLoad.RegionLoad createRegionLoad(final String encodedRegionName) {
    HRegion r = null;
    r = this.onlineRegions.get(encodedRegionName);
    return r != null ? createRegionLoad(r) : null;
  }

  /*
   * Cleanup after Throwable caught invoking method. Converts <code>t</code> to
   * IOE if it isn't already.
   *
   * @param t Throwable
   *
   * @return Throwable converted to an IOE; methods can only let out IOEs.
   */
  private Throwable cleanup(final Throwable t) {
    return cleanup(t, null);
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
      LOG.debug("NotServingRegionException; " +  t.getMessage());
      return t;
    }
    if (msg == null) {
      LOG.error("", RemoteExceptionHandler.checkThrowable(t));
    } else {
      LOG.error(msg, RemoteExceptionHandler.checkThrowable(t));
    }
    if (!checkOOME(t)) {
      checkFileSystem();
    }
    return t;
  }

  /*
   * @param t
   *
   * @return Make <code>t</code> an IOE if it isn't already.
   */
  private IOException convertThrowableToIOE(final Throwable t) {
    return convertThrowableToIOE(t, null);
  }

  /*
   * @param t
   *
   * @param msg Message to put in new IOE if passed <code>t</code> is not an IOE
   *
   * @return Make <code>t</code> an IOE if it isn't already.
   */
  private IOException convertThrowableToIOE(final Throwable t, final String msg) {
    return (t instanceof IOException ? (IOException) t : msg == null
        || msg.length() == 0 ? new IOException(t) : new IOException(msg, t));
  }

  /*
   * Check if an OOME and, if so, abort immediately to avoid creating more objects.
   *
   * @param e
   *
   * @return True if we OOME'd and are aborting.
   */
  public boolean checkOOME(final Throwable e) {
    boolean stop = false;
    try {
      if (e instanceof OutOfMemoryError
          || (e.getCause() != null && e.getCause() instanceof OutOfMemoryError)
          || (e.getMessage() != null && e.getMessage().contains(
              "java.lang.OutOfMemoryError"))) {
        stop = true;
        LOG.fatal(
          "Run out of memory; HRegionServer will abort itself immediately", e);
      }
    } finally {
      if (stop) {
        Runtime.getRuntime().halt(1);
      }
    }
    return stop;
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

  /*
   * Inner class that runs on a long period checking if regions need compaction.
   */
  private static class CompactionChecker extends Chore {
    private final HRegionServer instance;
    private final int majorCompactPriority;
    private final static int DEFAULT_PRIORITY = Integer.MAX_VALUE;

    CompactionChecker(final HRegionServer h, final int sleepTime,
        final Stoppable stopper) {
      super("CompactionChecker", sleepTime, h);
      this.instance = h;
      LOG.info("Runs every " + StringUtils.formatTime(sleepTime));

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
            if (s.needsCompaction()) {
              // Queue a compaction. Will recognize if major is needed.
              this.instance.compactSplitThread.requestCompaction(r, s, getName()
                  + " requests compaction", null);
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
            long randomDelay = rand.nextInt(RANGE_OF_DELAY) + MIN_DELAY_TIME;
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
    return isOnline;
  }

  /**
   * Setup WAL log and replication if enabled.
   * Replication setup is done in here because it wants to be hooked up to WAL.
   * @return A WAL instance.
   * @throws IOException
   */
  private HLog setupWALAndReplication() throws IOException {
    final Path oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    Path logdir = new Path(rootDir,
      HLog.getHLogDirectoryName(this.serverNameFromMasterPOV.toString()));
    if (LOG.isDebugEnabled()) LOG.debug("logdir=" + logdir);
    if (this.fs.exists(logdir)) {
      throw new RegionServerRunningException("Region server has already " +
        "created directory at " + this.serverNameFromMasterPOV.toString());
    }

    // Instantiate replication manager if replication enabled.  Pass it the
    // log directories.
    createNewReplicationInstance(conf, this, this.fs, logdir, oldLogDir);
    return instantiateHLog(logdir, oldLogDir);
  }

  // The method is synchronized to guarantee atomic update to hlogForMeta - 
  // It is possible that multiple calls could be made to this method almost 
  // at the same time, one for _ROOT_ and another for .META. (if they happen
  // to be assigned to the same RS). Also, we want to use the same log for both
  private synchronized HLog getMetaWAL() throws IOException {
    if (this.hlogForMeta == null) {
      final String logName
      = HLog.getHLogDirectoryName(this.serverNameFromMasterPOV.toString());

      Path logdir = new Path(rootDir, logName);
      final Path oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
      if (LOG.isDebugEnabled()) LOG.debug("logdir=" + logdir);
      this.hlogForMeta = new HLog(this.fs.getBackingFs(), logdir, oldLogDir, this.conf,
          getMetaWALActionListeners(), false, this.serverNameFromMasterPOV.toString(), true);
    }
    return this.hlogForMeta;
  }

  /**
   * Called by {@link #setupWALAndReplication()} creating WAL instance.
   * @param logdir
   * @param oldLogDir
   * @return WAL instance.
   * @throws IOException
   */
  protected HLog instantiateHLog(Path logdir, Path oldLogDir) throws IOException {
    return new HLog(this.fs.getBackingFs(), logdir, oldLogDir, this.conf,
      getWALActionListeners(), this.serverNameFromMasterPOV.toString());
  }

  /**
   * Called by {@link #instantiateHLog(Path, Path)} setting up WAL instance.
   * Add any {@link WALActionsListener}s you want inserted before WAL startup.
   * @return List of WALActionsListener that will be passed in to
   * {@link HLog} on construction.
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
    // null (addendum patch on HBASE-7213)
    MetaLogRoller tmpLogRoller = new MetaLogRoller(this, this);
    String n = Thread.currentThread().getName();
    Threads.setDaemonThreadRunning(tmpLogRoller.getThread(),
        n + "MetaLogRoller", uncaughtExceptionHandler);
    this.metaHLogRoller = tmpLogRoller;
    tmpLogRoller = null;
    listeners.add(this.metaHLogRoller);
    return listeners;
  }

  protected LogRoller getLogRoller() {
    return hlogRoller;
  }

  /*
   * @param interval Interval since last time metrics were called.
   */
  protected void doMetrics() {
    try {
      metrics();
    } catch (Throwable e) {
      LOG.warn("Failed metrics", e);
    }
  }

  protected void metrics() {
    this.metrics.regions.set(this.onlineRegions.size());
    this.metrics.incrementRequests(this.requestCount.get());
    this.metrics.requests.intervalHeartBeat();
    // Is this too expensive every three seconds getting a lock on onlineRegions
    // and then per store carried? Can I make metrics be sloppier and avoid
    // the synchronizations?
    int stores = 0;
    int storefiles = 0;
    long memstoreSize = 0;
    long readRequestsCount = 0;
    long writeRequestsCount = 0;
    long storefileIndexSize = 0;
    HDFSBlocksDistribution hdfsBlocksDistribution =
      new HDFSBlocksDistribution();
    long totalStaticIndexSize = 0;
    long totalStaticBloomSize = 0;
    long numPutsWithoutWAL = 0;
    long dataInMemoryWithoutWAL = 0;
    long updatesBlockedMs = 0;

    // Note that this is a map of Doubles instead of Longs. This is because we
    // do effective integer division, which would perhaps truncate more than it
    // should because we do it only on one part of our sum at a time. Rather
    // than dividing at the end, where it is difficult to know the proper
    // factor, everything is exact then truncated.
    final Map<String, MutableDouble> tempVals =
        new HashMap<String, MutableDouble>();

    for (Map.Entry<String, HRegion> e : this.onlineRegions.entrySet()) {
      HRegion r = e.getValue();
      memstoreSize += r.memstoreSize.get();
      numPutsWithoutWAL += r.numPutsWithoutWAL.get();
      dataInMemoryWithoutWAL += r.dataInMemoryWithoutWAL.get();
      readRequestsCount += r.readRequestsCount.get();
      writeRequestsCount += r.writeRequestsCount.get();
      updatesBlockedMs += r.updatesBlockedMs.get();
      synchronized (r.stores) {
        stores += r.stores.size();
        for (Map.Entry<byte[], Store> ee : r.stores.entrySet()) {
            final Store store = ee.getValue();
            final SchemaMetrics schemaMetrics = store.getSchemaMetrics();

            {
              long tmpStorefiles = store.getStorefilesCount();
              schemaMetrics.accumulateStoreMetric(tempVals,
                  StoreMetricType.STORE_FILE_COUNT, tmpStorefiles);
              storefiles += tmpStorefiles;
            }


            {
              long tmpStorefileIndexSize = store.getStorefilesIndexSize();
              schemaMetrics.accumulateStoreMetric(tempVals,
                  StoreMetricType.STORE_FILE_INDEX_SIZE,
                  (long) (tmpStorefileIndexSize / (1024.0 * 1024)));
              storefileIndexSize += tmpStorefileIndexSize;
            }

            {
              long tmpStorefilesSize = store.getStorefilesSize();
              schemaMetrics.accumulateStoreMetric(tempVals,
                  StoreMetricType.STORE_FILE_SIZE_MB,
                  (long) (tmpStorefilesSize / (1024.0 * 1024)));
            }

            {
              long tmpStaticBloomSize = store.getTotalStaticBloomSize();
              schemaMetrics.accumulateStoreMetric(tempVals,
                  StoreMetricType.STATIC_BLOOM_SIZE_KB,
                  (long) (tmpStaticBloomSize / 1024.0));
              totalStaticBloomSize += tmpStaticBloomSize;
            }

            {
              long tmpStaticIndexSize = store.getTotalStaticIndexSize();
              schemaMetrics.accumulateStoreMetric(tempVals,
                  StoreMetricType.STATIC_INDEX_SIZE_KB,
                  (long) (tmpStaticIndexSize / 1024.0));
              totalStaticIndexSize += tmpStaticIndexSize;
            }

            schemaMetrics.accumulateStoreMetric(tempVals,
                StoreMetricType.MEMSTORE_SIZE_MB,
                (long) (store.getMemStoreSize() / (1024.0 * 1024)));
        }
      }

      hdfsBlocksDistribution.add(r.getHDFSBlocksDistribution());
    }

    for (Entry<String, MutableDouble> e : tempVals.entrySet()) {
      RegionMetricsStorage.setNumericMetric(e.getKey(), e.getValue().longValue());
    }

    this.metrics.stores.set(stores);
    this.metrics.storefiles.set(storefiles);
    this.metrics.hlogFileCount.set(this.hlog.getNumLogFiles());
    this.metrics.hlogFileSizeMB.set(this.hlog.getNumLogFileSize() /(1024 * 1024));
    this.metrics.memstoreSizeMB.set((int) (memstoreSize / (1024 * 1024)));
    this.metrics.mbInMemoryWithoutWAL.set((int) (dataInMemoryWithoutWAL / (1024 * 1024)));
    this.metrics.numPutsWithoutWAL.set(numPutsWithoutWAL);
    this.metrics.storefileIndexSizeMB.set(
        (int) (storefileIndexSize / (1024 * 1024)));
    this.metrics.rootIndexSizeKB.set(
        (int) (storefileIndexSize / 1024));
    this.metrics.totalStaticIndexSizeKB.set(
        (int) (totalStaticIndexSize / 1024));
    this.metrics.totalStaticBloomSizeKB.set(
        (int) (totalStaticBloomSize / 1024));
    this.metrics.readRequestsCount.set(readRequestsCount);
    this.metrics.writeRequestsCount.set(writeRequestsCount);
    this.metrics.compactionQueueSize.set(compactSplitThread
        .getCompactionQueueSize());
    this.metrics.flushQueueSize.set(cacheFlusher
        .getFlushQueueSize());
    this.metrics.updatesBlockedSeconds.set(updatesBlockedMs/1000);
    final long updatesBlockedMsHigherWater = cacheFlusher.getUpdatesBlockedMsHighWater().get();
    this.metrics.updatesBlockedSecondsHighWater.set(updatesBlockedMsHigherWater/1000);

    BlockCache blockCache = cacheConfig.getBlockCache();
    if (blockCache != null) {
      this.metrics.blockCacheCount.set(blockCache.size());
      this.metrics.blockCacheFree.set(blockCache.getFreeSize());
      this.metrics.blockCacheSize.set(blockCache.getCurrentSize());
      CacheStats cacheStats = blockCache.getStats();
      this.metrics.blockCacheHitCount.set(cacheStats.getHitCount());
      this.metrics.blockCacheMissCount.set(cacheStats.getMissCount());
      this.metrics.blockCacheEvictedCount.set(blockCache.getEvictedCount());
      double ratio = blockCache.getStats().getHitRatio();
      int percent = (int) (ratio * 100);
      this.metrics.blockCacheHitRatio.set(percent);
      ratio = blockCache.getStats().getHitCachingRatio();
      percent = (int) (ratio * 100);
      this.metrics.blockCacheHitCachingRatio.set(percent);
      // past N period block cache hit / hit caching ratios
      cacheStats.rollMetricsPeriod();
      ratio = cacheStats.getHitRatioPastNPeriods();
      percent = (int) (ratio * 100);
      this.metrics.blockCacheHitRatioPastNPeriods.set(percent);
      ratio = cacheStats.getHitCachingRatioPastNPeriods();
      percent = (int) (ratio * 100);
      this.metrics.blockCacheHitCachingRatioPastNPeriods.set(percent);
    }
    float localityIndex = hdfsBlocksDistribution.getBlockLocalityIndex(
      getServerName().getHostname());
    int percent = (int) (localityIndex * 100);
    this.metrics.hdfsBlocksLocalityIndex.set(percent);

  }

  /**
   * @return Region server metrics instance.
   */
  public RegionServerMetrics getMetrics() {
    return this.metrics;
  }

  /**
   * @return Master address tracker instance.
   */
  public MasterAddressTracker getMasterAddressManager() {
    return this.masterAddressManager;
  }

  /*
   * Start maintanence Threads, Server, Worker and lease checker threads.
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
    String n = Thread.currentThread().getName();
    // Start executor services
    this.service = new ExecutorService(getServerName().toString());
    this.service.startExecutorService(ExecutorType.RS_OPEN_REGION,
      conf.getInt("hbase.regionserver.executor.openregion.threads", 3));
    this.service.startExecutorService(ExecutorType.RS_OPEN_ROOT,
      conf.getInt("hbase.regionserver.executor.openroot.threads", 1));
    this.service.startExecutorService(ExecutorType.RS_OPEN_META,
      conf.getInt("hbase.regionserver.executor.openmeta.threads", 1));
    this.service.startExecutorService(ExecutorType.RS_CLOSE_REGION,
      conf.getInt("hbase.regionserver.executor.closeregion.threads", 3));
    this.service.startExecutorService(ExecutorType.RS_CLOSE_ROOT,
      conf.getInt("hbase.regionserver.executor.closeroot.threads", 1));
    this.service.startExecutorService(ExecutorType.RS_CLOSE_META,
      conf.getInt("hbase.regionserver.executor.closemeta.threads", 1));

    Threads.setDaemonThreadRunning(this.hlogRoller.getThread(), n + ".logRoller",
        uncaughtExceptionHandler);
    Threads.setDaemonThreadRunning(this.cacheFlusher.getThread(), n + ".cacheFlusher",
        uncaughtExceptionHandler);
    Threads.setDaemonThreadRunning(this.compactionChecker.getThread(), n +
      ".compactionChecker", uncaughtExceptionHandler);
    Threads.setDaemonThreadRunning(this.periodicFlusher.getThread(), n +
        ".periodicFlusher", uncaughtExceptionHandler);
    if (this.healthCheckChore != null) {
      Threads.setDaemonThreadRunning(this.healthCheckChore.getThread(), n + ".healthChecker",
          uncaughtExceptionHandler);
    }

    // Leases is not a Thread. Internally it runs a daemon thread. If it gets
    // an unhandled exception, it will just exit.
    this.leases.setName(n + ".leaseChecker");
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

    // Start Server.  This service is like leases in that it internally runs
    // a thread.
    this.rpcServer.start();

    // Create the log splitting worker and start it
    this.splitLogWorker = new SplitLogWorker(this.zooKeeper,
        this.getConfiguration(), this.getServerName().toString());
    splitLogWorker.start();
    
  }

  /**
   * Puts up the webui.
   * @return Returns final port -- maybe different from what we started with.
   * @throws IOException
   */
  private int putUpWebUI() throws IOException {
    int port = this.conf.getInt("hbase.regionserver.info.port", 60030);
    // -1 is for disabling info server
    if (port < 0) return port;
    String addr = this.conf.get("hbase.regionserver.info.bindAddress", "0.0.0.0");
    // check if auto port bind enabled
    boolean auto = this.conf.getBoolean(HConstants.REGIONSERVER_INFO_PORT_AUTO,
        false);
    while (true) {
      try {
        this.infoServer = new InfoServer("regionserver", addr, port, false, this.conf);
        this.infoServer.addServlet("status", "/rs-status", RSStatusServlet.class);
        this.infoServer.addServlet("dump", "/dump", RSDumpServlet.class);
        this.infoServer.setAttribute(REGIONSERVER, this);
        this.infoServer.setAttribute(REGIONSERVER_CONF, conf);
        this.infoServer.start();
        break;
      } catch (BindException e) {
        if (!auto) {
          // auto bind disabled throw BindException
          throw e;
        }
        // auto bind enabled, try to use another port
        LOG.info("Failed binding http info server to port: " + port);
        port++;
      }
    }
    return this.infoServer.getPort();
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
    //_ROOT_ and .META. regions have separate WAL. 
    if (this.separateHLogForMeta && 
        regionInfo != null && 
        regionInfo.isMetaTable()) {
      return getMetaWAL();
    }
    return this.hlog;
  }

  @Override
  public CatalogTracker getCatalogTracker() {
    return this.catalogTracker;
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
    while (!isOnline() && !isStopped()){
       sleeper.sleep();
    }
  }

  @Override
  public void postOpenDeployTasks(final HRegion r, final CatalogTracker ct,
      final boolean daughter)
  throws KeeperException, IOException {
    checkOpen();
    LOG.info("Post open deploy tasks for region=" + r.getRegionNameAsString() +
      ", daughter=" + daughter);
    // Do checks to see if we need to compact (references or too many files)
    for (Store s : r.getStores().values()) {
      if (s.hasReferences() || s.needsCompaction()) {
        getCompactionRequester().requestCompaction(r, s, "Opening Region", null);
      }
    }
    // Update ZK, ROOT or META
    if (r.getRegionInfo().isRootRegion()) {
      RootLocationEditor.setRootLocation(getZooKeeper(),
       this.serverNameFromMasterPOV);
    } else if (r.getRegionInfo().isMetaRegion()) {
      MetaEditor.updateMetaLocation(ct, r.getRegionInfo(),
        this.serverNameFromMasterPOV);
    } else {
      if (daughter) {
        // If daughter of a split, update whole row, not just location.
        MetaEditor.addDaughter(ct, r.getRegionInfo(),
          this.serverNameFromMasterPOV);
      } else {
        MetaEditor.updateRegionLocation(ct, r.getRegionInfo(),
          this.serverNameFromMasterPOV);
      }
    }
    LOG.info("Done with post open deploy task for region=" +
      r.getRegionNameAsString() + ", daughter=" + daughter);

  }

  /**
   * Return a reference to the metrics instance used for counting RPC calls.
   * @return Metrics instance.
   */
  public HBaseRpcMetrics getRpcMetrics() {
    return rpcServer.getRpcMetrics();
  }

  @Override
  public RpcServer getRpcServer() {
    return rpcServer;
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
  public void abort(String reason, Throwable cause) {
    String msg = "ABORTING region server " + this + ": " + reason;
    if (cause != null) {
      LOG.fatal(msg, cause);
    } else {
      LOG.fatal(msg);
    }
    this.abortRequested = true;
    this.reservedSpace.clear();
    // HBASE-4014: show list of coprocessors that were loaded to help debug
    // regionserver crashes.Note that we're implicitly using
    // java.util.HashSet's toString() method to print the coprocessor names.
    LOG.fatal("RegionServer abort: loaded coprocessors are: " +
        CoprocessorHost.getLoadedCoprocessors());
    if (this.metrics != null) {
      LOG.info("Dump of metrics: " + this.metrics);
    }
    // Do our best to report our abort to the master, but this may not work
    try {
      if (cause != null) {
        msg += "\nCause:\n" + StringUtils.stringifyException(cause);
      }
      if (hbaseMaster != null) {
        hbaseMaster.reportRSFatalError(
            this.serverNameFromMasterPOV.getVersionedBytes(), msg);
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
  protected void join() {
    Threads.shutdown(this.compactionChecker.getThread());
    Threads.shutdown(this.periodicFlusher.getThread());
    Threads.shutdown(this.cacheFlusher.getThread());
    if (this.healthCheckChore != null) {
      Threads.shutdown(this.healthCheckChore.getThread());
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
  private ServerName getMaster() {
    ServerName masterServerName = null;
    long previousLogTime = 0;
    HMasterRegionInterface master = null;
    InetSocketAddress masterIsa = null;
    while (keepLooping() && master == null) {
      masterServerName = this.masterAddressManager.getMasterAddress();
      if (masterServerName == null) {
        if (!keepLooping()) {
          // give up with no connection.
          LOG.debug("No master found and cluster is stopped; bailing out");
          return null;
        }
        LOG.debug("No master found; retry");
        previousLogTime = System.currentTimeMillis();

        sleeper.sleep();
        continue;
      }

      masterIsa =
        new InetSocketAddress(masterServerName.getHostname(), masterServerName.getPort());

      LOG.info("Attempting connect to Master server at " + masterServerName);
      try {
        // Do initial RPC setup. The final argument indicates that the RPC
        // should retry indefinitely.
        master = HBaseRPC.waitForProxy(this.rpcEngine,
            HMasterRegionInterface.class, HMasterRegionInterface.VERSION,
            masterIsa, this.conf, -1,
            this.rpcTimeout, this.rpcTimeout);
      } catch (IOException e) {
        e = e instanceof RemoteException ?
            ((RemoteException)e).unwrapRemoteException() : e;
        if (e instanceof ServerNotRunningYetException) {
          if (System.currentTimeMillis() > (previousLogTime+1000)){
            LOG.info("Master isn't available yet, retrying");
            previousLogTime = System.currentTimeMillis();
          }
        } else {
          if (System.currentTimeMillis() > (previousLogTime + 1000)) {
            LOG.warn("Unable to connect to master. Retrying. Error was:", e);
            previousLogTime = System.currentTimeMillis();
          }
        }
        try {
          Thread.sleep(200);
        } catch (InterruptedException ignored) {
        }
      }
    }
    LOG.info("Connected to master at " + masterIsa);
    this.hbaseMaster = master;
    return masterServerName;
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
  private MapWritable reportForDuty() throws IOException {
    MapWritable result = null;
    ServerName masterServerName = getMaster();
    if (masterServerName == null) return result;
    try {
      this.requestCount.set(0);
      LOG.info("Telling master at " + masterServerName + " that we are up " +
        "with port=" + this.isa.getPort() + ", startcode=" + this.startcode);
      long now = EnvironmentEdgeManager.currentTimeMillis();
      int port = this.isa.getPort();
      result = this.hbaseMaster.regionServerStartup(port, this.startcode, now);
    } catch (RemoteException e) {
      IOException ioe = e.unwrapRemoteException();
      if (ioe instanceof ClockOutOfSyncException) {
        LOG.fatal("Master rejected startup because clock is out of sync", ioe);
        // Re-throw IOE will cause RS to abort
        throw ioe;
      } else {
        LOG.warn("remote error telling master we are up", e);
      }
    } catch (IOException e) {
      LOG.warn("error telling master we are up", e);
    }
    return result;
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
   * Close root and meta regions if we carry them
   * @param abort Whether we're running an abort.
   */
  void closeMetaTableRegions(final boolean abort) {
    HRegion meta = null;
    HRegion root = null;
    this.lock.writeLock().lock();
    try {
      for (Map.Entry<String, HRegion> e: onlineRegions.entrySet()) {
        HRegionInfo hri = e.getValue().getRegionInfo();
        if (hri.isRootRegion()) {
          root = e.getValue();
        } else if (hri.isMetaRegion()) {
          meta = e.getValue();
        }
        if (meta != null && root != null) break;
      }
    } finally {
      this.lock.writeLock().unlock();
    }
    if (meta != null) closeRegion(meta.getRegionInfo(), abort, false);
    if (root != null) closeRegion(root.getRegionInfo(), abort, false);
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
          closeRegion(r.getRegionInfo(), abort, false);
        }
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public HRegionInfo getRegionInfo(final byte[] regionName)
  throws NotServingRegionException, IOException {
    checkOpen();
    requestCount.incrementAndGet();
    return getRegion(regionName).getRegionInfo();
  }

  public Result getClosestRowBefore(final byte[] regionName, final byte[] row,
      final byte[] family) throws IOException {
    checkOpen();
    requestCount.incrementAndGet();
    try {
      // locate the region we're operating on
      HRegion region = getRegion(regionName);
      // ask the region for all the data

      Result r = region.getClosestRowBefore(row, family);
      return r;
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  /** {@inheritDoc} */
  public Result get(byte[] regionName, Get get) throws IOException {
    checkOpen();
    requestCount.incrementAndGet();
    try {
      HRegion region = getRegion(regionName);
      return region.get(get, getLockFromId(get.getLockId()));
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  public boolean exists(byte[] regionName, Get get) throws IOException {
    checkOpen();
    requestCount.incrementAndGet();
    try {
      HRegion region = getRegion(regionName);
      Integer lock = getLockFromId(get.getLockId());
      if (region.getCoprocessorHost() != null) {
        Boolean result = region.getCoprocessorHost().preExists(get);
        if (result != null) {
          return result.booleanValue();
        }
      }
      Result r = region.get(get, lock);
      boolean result = r != null && !r.isEmpty();
      if (region.getCoprocessorHost() != null) {
        result = region.getCoprocessorHost().postExists(get, result);
      }
      return result;
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  public void put(final byte[] regionName, final Put put) throws IOException {
    if (put.getRow() == null) {
      throw new IllegalArgumentException("update has null row");
    }

    checkOpen();
    this.requestCount.incrementAndGet();
    HRegion region = getRegion(regionName);
    try {
      if (!region.getRegionInfo().isMetaTable()) {
        this.cacheFlusher.reclaimMemStoreMemory();
      }
      boolean writeToWAL = put.getWriteToWAL();
      region.put(put, getLockFromId(put.getLockId()), writeToWAL);
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  public int put(final byte[] regionName, final List<Put> puts)
      throws IOException {
    checkOpen();
    HRegion region = null;
    int i = 0;

    try {
      region = getRegion(regionName);
      if (!region.getRegionInfo().isMetaTable()) {
        this.cacheFlusher.reclaimMemStoreMemory();
      }

      @SuppressWarnings("unchecked")
      Pair<Mutation, Integer>[] putsWithLocks = new Pair[puts.size()];

      for (Put p : puts) {
        Integer lock = getLockFromId(p.getLockId());
        putsWithLocks[i++] = new Pair<Mutation, Integer>(p, lock);
      }

      this.requestCount.addAndGet(puts.size());
      OperationStatus codes[] = region.batchMutate(putsWithLocks);
      for (i = 0; i < codes.length; i++) {
        if (codes[i].getOperationStatusCode() != OperationStatusCode.SUCCESS) {
          return i;
        }
      }
      return -1;
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  private boolean checkAndMutate(final byte[] regionName, final byte[] row,
      final byte[] family, final byte[] qualifier, final CompareOp compareOp,
      final WritableByteArrayComparable comparator, final Writable w,
      Integer lock) throws IOException {
    checkOpen();
    this.requestCount.incrementAndGet();
    HRegion region = getRegion(regionName);
    try {
      if (!region.getRegionInfo().isMetaTable()) {
        this.cacheFlusher.reclaimMemStoreMemory();
      }
      return region.checkAndMutate(row, family, qualifier, compareOp,
        comparator, w, lock, true);
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  /**
   *
   * @param regionName
   * @param row
   * @param family
   * @param qualifier
   * @param value
   *          the expected value
   * @param put
   * @throws IOException
   * @return true if the new put was execute, false otherwise
   */
  public boolean checkAndPut(final byte[] regionName, final byte[] row,
      final byte[] family, final byte[] qualifier, final byte[] value,
      final Put put) throws IOException {
    checkOpen();
    if (regionName == null) {
      throw new IOException("Invalid arguments to checkAndPut "
          + "regionName is null");
    }
    HRegion region = getRegion(regionName);
    Integer lock = getLockFromId(put.getLockId());
    WritableByteArrayComparable comparator = new BinaryComparator(value);
    if (region.getCoprocessorHost() != null) {
      Boolean result = region.getCoprocessorHost()
        .preCheckAndPut(row, family, qualifier, CompareOp.EQUAL, comparator,
          put);
      if (result != null) {
        return result.booleanValue();
      }
    }
    boolean result = checkAndMutate(regionName, row, family, qualifier,
        CompareOp.EQUAL, comparator, put,
      lock);
    if (region.getCoprocessorHost() != null) {
      result = region.getCoprocessorHost().postCheckAndPut(row, family,
        qualifier, CompareOp.EQUAL, comparator, put, result);
    }
    return result;
  }

  /**
   *
   * @param regionName
   * @param row
   * @param family
   * @param qualifier
   * @param compareOp
   * @param comparator
   * @param put
   * @throws IOException
   * @return true if the new put was execute, false otherwise
   */
  public boolean checkAndPut(final byte[] regionName, final byte[] row,
      final byte[] family, final byte[] qualifier, final CompareOp compareOp,
      final WritableByteArrayComparable comparator, final Put put)
       throws IOException {
    checkOpen();
    if (regionName == null) {
      throw new IOException("Invalid arguments to checkAndPut "
          + "regionName is null");
    }
    HRegion region = getRegion(regionName);
    Integer lock = getLockFromId(put.getLockId());
    if (region.getCoprocessorHost() != null) {
      Boolean result = region.getCoprocessorHost()
        .preCheckAndPut(row, family, qualifier, compareOp, comparator, put);
      if (result != null) {
        return result.booleanValue();
      }
    }
    boolean result = checkAndMutate(regionName, row, family, qualifier,
      compareOp, comparator, put, lock);
    if (region.getCoprocessorHost() != null) {
      result = region.getCoprocessorHost().postCheckAndPut(row, family,
        qualifier, compareOp, comparator, put, result);
    }
    return result;
  }

  /**
   *
   * @param regionName
   * @param row
   * @param family
   * @param qualifier
   * @param value
   *          the expected value
   * @param delete
   * @throws IOException
   * @return true if the new put was execute, false otherwise
   */
  public boolean checkAndDelete(final byte[] regionName, final byte[] row,
      final byte[] family, final byte[] qualifier, final byte[] value,
      final Delete delete) throws IOException {
    checkOpen();

    if (regionName == null) {
      throw new IOException("Invalid arguments to checkAndDelete "
          + "regionName is null");
    }
    HRegion region = getRegion(regionName);
    Integer lock = getLockFromId(delete.getLockId());
    WritableByteArrayComparable comparator = new BinaryComparator(value);
    if (region.getCoprocessorHost() != null) {
      Boolean result = region.getCoprocessorHost().preCheckAndDelete(row,
        family, qualifier, CompareOp.EQUAL, comparator, delete);
      if (result != null) {
        return result.booleanValue();
      }
    }
    boolean result = checkAndMutate(regionName, row, family, qualifier,
      CompareOp.EQUAL, comparator, delete, lock);
    if (region.getCoprocessorHost() != null) {
      result = region.getCoprocessorHost().postCheckAndDelete(row, family,
        qualifier, CompareOp.EQUAL, comparator, delete, result);
    }
    return result;
  }

  @Override
  public List<String> getStoreFileList(byte[] regionName, byte[] columnFamily)
    throws IllegalArgumentException {
    return getStoreFileList(regionName, new byte[][]{columnFamily});
  }

  @Override
  public List<String> getStoreFileList(byte[] regionName, byte[][] columnFamilies)
    throws IllegalArgumentException {
    HRegion region = getOnlineRegion(regionName);
    if (region == null) {
      throw new IllegalArgumentException("No region: " + new String(regionName)
          + " available");
    }
    return region.getStoreFileList(columnFamilies);
  }

  public List<String> getStoreFileList(byte[] regionName)
    throws IllegalArgumentException {
    HRegion region = getOnlineRegion(regionName);
    if (region == null) {
      throw new IllegalArgumentException("No region: " + new String(regionName)
          + " available");
    }
    Set<byte[]> columnFamilies = region.getStores().keySet();
    int nCF = columnFamilies.size();
    return region.getStoreFileList(columnFamilies.toArray(new byte[nCF][]));
  }
  
 /**
  * Flushes the given region
  */
  public void flushRegion(byte[] regionName)
    throws IllegalArgumentException, IOException {
    HRegion region = getOnlineRegion(regionName);
    if (region == null) {
      throw new IllegalArgumentException("No region : " + new String(regionName)
      + " available");
    }
    boolean needsCompaction = region.flushcache().isCompactionNeeded();
    if (needsCompaction) {
      this.compactSplitThread.requestCompaction(region, "Compaction through user triggered flush");
    }
  }

 /**
   * Flushes the given region if lastFlushTime < ifOlderThanTS
   */
   public void flushRegion(byte[] regionName, long ifOlderThanTS)
     throws IllegalArgumentException, IOException {
     HRegion region = getOnlineRegion(regionName);
     if (region == null) {
       throw new IllegalArgumentException("No region : " + new String(regionName)
       + " available");
     }
     if (region.getLastFlushTime() < ifOlderThanTS) {
      boolean needsCompaction = region.flushcache().isCompactionNeeded();
      if (needsCompaction) {
        this.compactSplitThread
            .requestCompaction(region, "Compaction through user triggered flush");
      }
    }
   }

  /**
   * Gets last flush time for the given region
   * @return the last flush time for a region
   */
  public long getLastFlushTime(byte[] regionName) {
    HRegion region = getOnlineRegion(regionName);
    if (region == null) {
      throw new IllegalArgumentException("No region : " + new String(regionName)
      + " available");
    }
    return region.getLastFlushTime();
  }
 
  /**
   *
   * @param regionName
   * @param row
   * @param family
   * @param qualifier
   * @param compareOp
   * @param comparator
   * @param delete
   * @throws IOException
   * @return true if the new put was execute, false otherwise
   */
  public boolean checkAndDelete(final byte[] regionName, final byte[] row,
      final byte[] family, final byte[] qualifier, final CompareOp compareOp,
      final WritableByteArrayComparable comparator, final Delete delete)
      throws IOException {
    checkOpen();

    if (regionName == null) {
      throw new IOException("Invalid arguments to checkAndDelete "
        + "regionName is null");
    }
    HRegion region = getRegion(regionName);
    Integer lock = getLockFromId(delete.getLockId());
    if (region.getCoprocessorHost() != null) {
      Boolean result = region.getCoprocessorHost().preCheckAndDelete(row,
        family, qualifier, compareOp, comparator, delete);
     if (result != null) {
       return result.booleanValue();
     }
    }
    boolean result = checkAndMutate(regionName, row, family, qualifier,
      compareOp, comparator, delete, lock);
   if (region.getCoprocessorHost() != null) {
     result = region.getCoprocessorHost().postCheckAndDelete(row, family,
       qualifier, compareOp, comparator, delete, result);
   }
   return result;
 }

  //
  // remote scanner interface
  //
  
  public long openScanner(byte[] regionName, Scan scan) throws IOException {
    RegionScanner s = internalOpenScanner(regionName, scan);
    long scannerId = addScanner(s);
    return scannerId;
  }

  private RegionScanner internalOpenScanner(byte[] regionName, Scan scan)
      throws IOException {
    checkOpen();
    NullPointerException npe = null;
    if (regionName == null) {
      npe = new NullPointerException("regionName is null");
    } else if (scan == null) {
      npe = new NullPointerException("scan is null");
    }
    if (npe != null) {
      throw new IOException("Invalid arguments to openScanner", npe);
    }
    requestCount.incrementAndGet();
    try {
      HRegion r = getRegion(regionName);
      r.checkRow(scan.getStartRow(), "Scan");
      scan.setLoadColumnFamiliesOnDemand(r.isLoadingCfsOnDemandDefault()
          || scan.doLoadColumnFamiliesOnDemand());
      r.prepareScanner(scan);
      RegionScanner s = null;
      if (r.getCoprocessorHost() != null) {
        s = r.getCoprocessorHost().preScannerOpen(scan);
      }
      if (s == null) {
        s = r.getScanner(scan);
      }
      if (r.getCoprocessorHost() != null) {
        RegionScanner savedScanner = r.getCoprocessorHost().postScannerOpen(
            scan, s);
        if (savedScanner == null) {
          LOG.warn("PostScannerOpen impl returning null. "
              + "Check the RegionObserver implementation.");
        } else {
          s = savedScanner;
        }
      }
      return s;
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t, "Failed openScanner"));
    }
  }

  protected long addScanner(RegionScanner s) throws LeaseStillHeldException {
    long scannerId = -1L;
    scannerId = rand.nextLong();
    String scannerName = String.valueOf(scannerId);
    scanners.put(scannerName, new RegionScannerHolder(s));
    this.leases.createLease(scannerName, new ScannerListener(scannerName));
    return scannerId;
  }

  public Result next(final long scannerId) throws IOException {
    Result[] res = next(scannerId, 1);
    if (res == null || res.length == 0) {
      return null;
    }
    return res[0];
  }

   public Result[] next(final long scannerId, int nbRows) throws IOException {
    return next(scannerId, nbRows, -1);
  }

  public Result[] next(final long scannerId, int nbRows, long callSeq) throws IOException {
    String scannerName = String.valueOf(scannerId);
    RegionScannerHolder holder = this.scanners.get(scannerName);
    if (holder == null) {
      LOG.info("Client tried to access missing scanner " + scannerName);
      throw new UnknownScannerException("Name: " + scannerName);
    }
    // if callSeq does not match throw Exception straight away. This needs to be performed even
    // before checking of Lease.
    // Old next() APIs which do not take callSeq will pass it as -1 and for that no
    // need to match the callSeq from client and the one in server.
    if (callSeq != -1 && callSeq != holder.getCallSeq()) {
      throw new CallSequenceOutOfOrderException("Expected seq: " + holder.getCallSeq()
          + " But the seq got from client: " + callSeq);
     }
    // Increment the callSeq value which is the next expected from client.
    holder.incrCallSeq();
    return internalNext(holder.getScanner(), nbRows, scannerName);
  }

  private Result[] internalNext(final RegionScanner s, int nbRows,
      String scannerName) throws IOException {
    try {
      checkOpen();
    } catch (IOException e) {
      // If checkOpen failed, server not running or filesystem gone,
      // cancel this lease; filesystem is gone or we're closing or something.
      if (scannerName != null) {
        try {
          this.leases.cancelLease(scannerName);
        } catch (LeaseException le) {
          LOG.info("Server shutting down and client tried to access missing scanner "
              + scannerName);
        }
      }
      throw e;
    }
    Leases.Lease lease = null;
    try {
      // Remove lease while its being processed in server; protects against case
      // where processing of request takes > lease expiration time.
      try {
        if (scannerName != null) {
          lease = this.leases.removeLease(scannerName);
        }
      } catch (LeaseException le) {
        // What it really means is that there's no such scanner.
        LOG.info("Client tried to access missing scanner " + scannerName + " (no lease)");
        throw new UnknownScannerException("No lease for " + scannerName + ": " + le.getMessage());
      }
      List<Result> results = new ArrayList<Result>(nbRows);
      long currentScanResultSize = 0;
      List<KeyValue> values = new ArrayList<KeyValue>();

      // Call coprocessor. Get region info from scanner.
      HRegion region = getRegion(s.getRegionInfo().getRegionName());
      if (region != null && region.getCoprocessorHost() != null) {
        Boolean bypass = region.getCoprocessorHost().preScannerNext(s,
            results, nbRows);
        if (!results.isEmpty()) {
          for (Result r : results) {
            if (maxScannerResultSize < Long.MAX_VALUE){
              for (KeyValue kv : r.raw()) {
                currentScanResultSize += kv.heapSize();
              }
            }
          }
        }
        if (bypass != null) {
          return s.isFilterDone() && results.isEmpty() ? null
              : results.toArray(new Result[results.size()]);
        }
      }

      MultiVersionConsistencyControl.setThreadReadPoint(s.getMvccReadPoint());
      region.startRegionOperation();
      try {
        int i = 0;
        synchronized(s) {
          for (; i < nbRows
              && currentScanResultSize < maxScannerResultSize; ) {
            // Collect values to be returned here
            boolean moreRows = s.nextRaw(values, SchemaMetrics.METRIC_NEXTSIZE);
            if (!values.isEmpty()) {
              if (maxScannerResultSize < Long.MAX_VALUE){
                for (KeyValue kv : values) {
                  currentScanResultSize += kv.heapSize();
                }
              }
              results.add(new Result(values));
              i++;
            }
            if (!moreRows) {
              break;
            }
            values.clear();
          }
        }
        requestCount.addAndGet(i);
        region.readRequestsCount.add(i);
        region.setOpMetricsReadRequestCount(region.readRequestsCount.get());
      } finally {
        region.closeRegionOperation();
      }
      // coprocessor postNext hook
      if (region != null && region.getCoprocessorHost() != null) {
        region.getCoprocessorHost().postScannerNext(s, results, nbRows, true);
      }

      // If the scanner's filter - if any - is done with the scan
      // and wants to tell the client to stop the scan. This is done by passing
      // a null result.
      return s.isFilterDone() && results.isEmpty() ? null
          : results.toArray(new Result[results.size()]);
    } catch (Throwable t) {
      if (t instanceof NotServingRegionException && scannerName != null) {
        this.scanners.remove(scannerName);
      }
      throw convertThrowableToIOE(cleanup(t));
    } finally {
      // We're done. On way out readd the above removed lease.  Adding resets
      // expiration time on lease.
      if (scannerName != null && this.scanners.containsKey(scannerName)) {
        if (lease != null) this.leases.addLease(lease);
      }
    }
  }

  public void close(final long scannerId) throws IOException {
    String scannerName = String.valueOf(scannerId);
    RegionScannerHolder holder = this.scanners.get(scannerName);
    if (holder == null) throw new UnknownScannerException("Name: " + scannerName);
    internalCloseScanner(holder.getScanner(), scannerName);
  }

  private void internalCloseScanner(final RegionScanner s, String scannerName)
      throws IOException {
    try {
      checkOpen();
      requestCount.incrementAndGet();
      HRegion region = null;
      if (s != null) {
        // call coprocessor.
        region = getRegion(s.getRegionInfo().getRegionName());
        if (region != null && region.getCoprocessorHost() != null) {
          if (region.getCoprocessorHost().preScannerClose(s)) {
            return; // bypass
          }
        }
      }
      RegionScanner toCloseScanner = s;
      if (scannerName != null) {
        RegionScannerHolder holder = scanners.remove(scannerName);
        if (holder!= null) {
          toCloseScanner = holder.getScanner();
        }
      }
      if (toCloseScanner != null) {
        toCloseScanner.close();
        if (scannerName != null) {
          this.leases.cancelLease(scannerName);
        }

        if (region != null && region.getCoprocessorHost() != null) {
          region.getCoprocessorHost().postScannerClose(toCloseScanner);
        }
      }
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  @Override
  public Result[] scan(byte[] regionName, Scan scan, int numberOfRows)
      throws IOException {
    RegionScanner s = internalOpenScanner(regionName, scan);
    try {
      Result[] results = internalNext(s, numberOfRows, null);
      return results;
    } finally {
      internalCloseScanner(s, null);
    }
  }

  /**
   * Instantiated as a scanner lease. If the lease times out, the scanner is
   * closed
   */
  private class ScannerListener implements LeaseListener {
    private final String scannerName;

    ScannerListener(final String n) {
      this.scannerName = n;
    }

    public void leaseExpired() {
      RegionScannerHolder holder = scanners.remove(this.scannerName);
      if (holder != null) {
        RegionScanner s = holder.getScanner();
        LOG.info("Scanner " + this.scannerName + " lease expired on region "
            + s.getRegionInfo().getRegionNameAsString());
        try {
          HRegion region = getRegion(s.getRegionInfo().getRegionName());
          if (region != null && region.getCoprocessorHost() != null) {
            region.getCoprocessorHost().preScannerClose(s);
          }

          s.close();
          if (region != null && region.getCoprocessorHost() != null) {
            region.getCoprocessorHost().postScannerClose(s);
          }
        } catch (IOException e) {
          LOG.error("Closing scanner for "
              + s.getRegionInfo().getRegionNameAsString(), e);
        }
      } else {
        LOG.info("Scanner " + this.scannerName + " lease expired");
      }
    }
  }

  //
  // Methods that do the actual work for the remote API
  //
  public void delete(final byte[] regionName, final Delete delete)
      throws IOException {
    checkOpen();
    try {
      boolean writeToWAL = delete.getWriteToWAL();
      this.requestCount.incrementAndGet();
      HRegion region = getRegion(regionName);
      if (!region.getRegionInfo().isMetaTable()) {
        this.cacheFlusher.reclaimMemStoreMemory();
      }
      Integer lid = getLockFromId(delete.getLockId());
      region.delete(delete, lid, writeToWAL);
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  public int delete(final byte[] regionName, final List<Delete> deletes)
      throws IOException {
    checkOpen();
    // Count of Deletes processed.
    int i = 0;
    HRegion region = null;
    try {
      region = getRegion(regionName);
      if (!region.getRegionInfo().isMetaTable()) {
        this.cacheFlusher.reclaimMemStoreMemory();
      }
      int size = deletes.size();
      Integer[] locks = new Integer[size];
      for (Delete delete : deletes) {
        this.requestCount.incrementAndGet();
        locks[i] = getLockFromId(delete.getLockId());
        region.delete(delete, locks[i], delete.getWriteToWAL());
        i++;
      }
    } catch (WrongRegionException ex) {
      LOG.debug("Batch deletes: " + i, ex);
      return i;
    } catch (NotServingRegionException ex) {
      return i;
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
    return -1;
  }

  /**
   * @deprecated {@link RowLock} and associated operations are deprecated.
   */
  public long lockRow(byte[] regionName, byte[] row) throws IOException {
    checkOpen();
    NullPointerException npe = null;
    if (regionName == null) {
      npe = new NullPointerException("regionName is null");
    } else if (row == null) {
      npe = new NullPointerException("row to lock is null");
    }
    if (npe != null) {
      IOException io = new IOException("Invalid arguments to lockRow");
      io.initCause(npe);
      throw io;
    }
    requestCount.incrementAndGet();
    try {
      HRegion region = getRegion(regionName);
      if (region.getCoprocessorHost() != null) {
        region.getCoprocessorHost().preLockRow(regionName, row);
      }
      Integer r = region.obtainRowLock(row);
      long lockId = addRowLock(r, region);
      LOG.debug("Row lock " + lockId + " explicitly acquired by client");
      return lockId;
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t, "Error obtaining row lock (fsOk: "
          + this.fsOk + ")"));
    }
  }

  protected long addRowLock(Integer r, HRegion region)
      throws LeaseStillHeldException {
    long lockId = -1L;
    lockId = rand.nextLong();
    String lockName = String.valueOf(lockId);
    rowlocks.put(lockName, r);
    this.leases.createLease(lockName, new RowLockListener(lockName, region));
    return lockId;
  }

  /**
   * Method to get the Integer lock identifier used internally from the long
   * lock identifier used by the client.
   *
   * @param lockId
   *          long row lock identifier from client
   * @return intId Integer row lock used internally in HRegion
   * @throws IOException
   *           Thrown if this is not a valid client lock id.
   */
  Integer getLockFromId(long lockId) throws IOException {
    if (lockId == -1L) {
      return null;
    }
    String lockName = String.valueOf(lockId);
    Integer rl = rowlocks.get(lockName);
    if (rl == null) {
      throw new UnknownRowLockException("Invalid row lock");
    }
    this.leases.renewLease(lockName);
    return rl;
  }

  /**
   * @deprecated {@link RowLock} and associated operations are deprecated.
   */
  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public void unlockRow(byte[] regionName, long lockId) throws IOException {
    checkOpen();
    NullPointerException npe = null;
    if (regionName == null) {
      npe = new NullPointerException("regionName is null");
    } else if (lockId == -1L) {
      npe = new NullPointerException("lockId is null");
    }
    if (npe != null) {
      IOException io = new IOException("Invalid arguments to unlockRow");
      io.initCause(npe);
      throw io;
    }
    requestCount.incrementAndGet();
    try {
      HRegion region = getRegion(regionName);
      if (region.getCoprocessorHost() != null) {
        region.getCoprocessorHost().preUnLockRow(regionName, lockId);
      }
      String lockName = String.valueOf(lockId);
      Integer r = rowlocks.remove(lockName);
      if (r == null) {
        throw new UnknownRowLockException(lockName);
      }
      region.releaseRowLock(r);
      this.leases.cancelLease(lockName);
      LOG.debug("Row lock " + lockId
          + " has been explicitly released by client");
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  /**
   * Atomically bulk load several HFiles into an open region
   * @return true if successful, false is failed but recoverably (no action)
   * @throws IOException if failed unrecoverably
   */
  @Override
  public boolean bulkLoadHFiles(List<Pair<byte[], String>> familyPaths,
      byte[] regionName) throws IOException {
    return bulkLoadHFiles(familyPaths, regionName, false);
  }

  /**
   * Atomically bulk load several HFiles into an open region
   * @return true if successful, false is failed but recoverably (no action)
   * @throws IOException if failed unrecoverably
   */
  @Override
  public boolean bulkLoadHFiles(List<Pair<byte[], String>> familyPaths,
      byte[] regionName, boolean assignSeqNum) throws IOException {
    checkOpen();
    HRegion region = getRegion(regionName);
    boolean bypass = false;
    if (region.getCoprocessorHost() != null) {
      bypass = region.getCoprocessorHost().preBulkLoadHFile(familyPaths);
    }
    boolean loaded = false;
    if (!bypass) {
      loaded = region.bulkLoadHFiles(familyPaths, assignSeqNum);
    }
    if (region.getCoprocessorHost() != null) {
      loaded = region.getCoprocessorHost().postBulkLoadHFile(familyPaths, loaded);
    }
    return loaded;
  }

  Map<String, Integer> rowlocks = new ConcurrentHashMap<String, Integer>();

  /**
   * Instantiated as a row lock lease. If the lease times out, the row lock is
   * released
   */
  private class RowLockListener implements LeaseListener {
    private final String lockName;
    private final HRegion region;

    RowLockListener(final String lockName, final HRegion region) {
      this.lockName = lockName;
      this.region = region;
    }

    public void leaseExpired() {
      LOG.info("Row Lock " + this.lockName + " lease expired");
      Integer r = rowlocks.remove(this.lockName);
      if (r != null) {
        region.releaseRowLock(r);
      }
    }
  }

  // Region open/close direct RPCs

  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public RegionOpeningState openRegion(HRegionInfo region)
  throws IOException {
    return openRegion(region, -1);
  }

  @Override
  @QosPriority(priority = HConstants.HIGH_QOS)
  public RegionOpeningState openRegion(HRegionInfo region, int versionOfOfflineNode)
      throws IOException {
    return openRegion(region, versionOfOfflineNode, null);
  }

  private RegionOpeningState openRegion(HRegionInfo region, int versionOfOfflineNode,
      Map<String, HTableDescriptor> htds) throws IOException {
    checkOpen();
    HRegion onlineRegion = this.getFromOnlineRegions(region.getEncodedName());
    if (null != onlineRegion) {
      // See HBASE-5094. Cross check with META if still this RS is owning the
      // region.
      Pair<HRegionInfo, ServerName> p = MetaReader.getRegion(
          this.catalogTracker, region.getRegionName());
      if (this.getServerName().equals(p.getSecond())) {
        LOG.warn("Attempted open of " + region.getEncodedName()
            + " but already online on this server");
        return RegionOpeningState.ALREADY_OPENED;
      } else {
        LOG.warn("The region " + region.getEncodedName()
            + " is online on this server but META does not have this server.");
        this.removeFromOnlineRegions(region.getEncodedName());
      }
    }
    // Added to in-memory RS RIT that we are trying to open this region.
    // Clear it if we fail queuing an open executor.
    boolean isNewRit = addRegionsInTransition(region, OPEN);
    if (!isNewRit) {
      // An open is in progress. This is supported, but let's log this.
      LOG.info("Receiving OPEN for the region:" +
          region.getRegionNameAsString() + " , which we are already trying to OPEN" +
          " - ignoring this new request for this region.");
      return RegionOpeningState.OPENED;
    }
    try {
      LOG.info("Received request to open region: " +
        region.getRegionNameAsString());
      HTableDescriptor htd = null;
      if (htds == null) {
        htd = this.tableDescriptors.get(region.getTableName());
      } else {
        htd = htds.get(region.getTableNameAsString());
        if (htd == null) {
          htd = this.tableDescriptors.get(region.getTableName());
          htds.put(region.getTableNameAsString(), htd);
        }
      }

      // Mark the region as OPENING up in zk.  This is how we tell the master control of the
      // region has passed to this regionserver.
      int version = transitionZookeeperOfflineToOpening(region, versionOfOfflineNode);
      // Need to pass the expected version in the constructor.
      if (region.isRootRegion()) {
        this.service.submit(new OpenRootHandler(this, this, region, htd, version));
      } else if (region.isMetaRegion()) {
        this.service.submit(new OpenMetaHandler(this, this, region, htd, version));
      } else {
        this.service.submit(new OpenRegionHandler(this, this, region, htd, version));
      }
    } catch (IOException ie) {
      // Clear from this server's RIT list else will stick around for ever.
      removeFromRegionsInTransition(region);
      throw ie;
    }
    return RegionOpeningState.OPENED;
  }

  /**
   * Transition ZK node from OFFLINE to OPENING. The master will get a callback
   * and will know that the region is now ours.
   *
   * @param hri
   *          HRegionInfo whose znode we are updating
   * @param versionOfOfflineNode
   *          Version Of OfflineNode that needs to be compared before changing
   *          the node's state from OFFLINE
   * @throws IOException
   */
  int transitionZookeeperOfflineToOpening(final HRegionInfo hri, int versionOfOfflineNode)
      throws IOException {
    // TODO: should also handle transition from CLOSED?
    int version = -1;
    try {
      // Initialize the znode version.
      version = ZKAssign.transitionNode(this.zooKeeper, hri, this.getServerName(),
          EventType.M_ZK_REGION_OFFLINE, EventType.RS_ZK_REGION_OPENING, versionOfOfflineNode);
    } catch (KeeperException e) {
      LOG.error("Error transition from OFFLINE to OPENING for region=" + hri.getEncodedName(), e);
    }
    if (version == -1) {
      // TODO: Fix this sloppyness. The exception should be coming off zk
      // directly, not an
      // intepretation at this high-level (-1 when we call transitionNode can
      // mean many things).
      throw new IOException("Failed transition from OFFLINE to OPENING for region="
          + hri.getEncodedName());
    }
    return version;
  }

   /**
    * String currentAction) throws RegionAlreadyInTransitionException { Add
    * region to this regionservers list of in transitions regions ONLY if its not
    * already byte[] encodedName = region.getEncodedNameAsBytes(); in transition.
    * If a region already in RIT, we throw
    * {@link RegionAlreadyInTransitionException}. if
    * (this.regionsInTransitionInRS.containsKey(encodedName)) { Callers need to
    * call {@link #removeFromRegionsInTransition(HRegionInfo)} when done or if
    * boolean openAction = this.regionsInTransitionInRS.get(encodedName); error
    * processing.
    *
    * @param region
    *          Region to add
    * @param currentAction
    *          Whether OPEN or CLOSE.
    * @throws RegionAlreadyInTransitionException
    */
   protected boolean addRegionsInTransition(final HRegionInfo region, final String currentAction)
       throws RegionAlreadyInTransitionException {
     boolean isOpen = currentAction.equals(OPEN);
     Boolean action = this.regionsInTransitionInRS.putIfAbsent(
         region.getEncodedNameAsBytes(), isOpen);
     if (action == null) return true;
     if (isOpen && action.booleanValue()) {
       return false;
     }
     // The below exception message will be used in master.
     throw new RegionAlreadyInTransitionException("Received:" + currentAction
         + " for the region:" + region.getRegionNameAsString()
         + ", which we are already trying to " + (action ? OPEN : CLOSE) + ".");
   }

  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public void openRegions(List<HRegionInfo> regions)
  throws IOException {
    checkOpen();
    LOG.info("Received request to open " + regions.size() + " region(s)");
    Map<String, HTableDescriptor> htds = new HashMap<String, HTableDescriptor>(regions.size());
    for (HRegionInfo region : regions) openRegion(region, -1, htds);
  }

  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public boolean closeRegion(HRegionInfo region)
  throws IOException {
    return closeRegion(region, true, -1);
  }

  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public boolean closeRegion(final HRegionInfo region,
    final int versionOfClosingNode)
  throws IOException {
    return closeRegion(region, true, versionOfClosingNode);
  }

  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public boolean closeRegion(HRegionInfo region, final boolean zk)
  throws IOException {
    return closeRegion(region, zk, -1);
  }

  @QosPriority(priority=HConstants.HIGH_QOS)
  protected boolean closeRegion(HRegionInfo region, final boolean zk,
    final int versionOfClosingNode)
  throws IOException {
    checkOpen();
    //Check for permissions to close.
    HRegion actualRegion = this.getFromOnlineRegions(region.getEncodedName());
    if (actualRegion != null && actualRegion.getCoprocessorHost() != null) {
      actualRegion.getCoprocessorHost().preClose(false);
    }
    LOG.info("Received close region: " + region.getRegionNameAsString() +
      ". Version of ZK closing node:" + versionOfClosingNode);
    boolean hasit = this.onlineRegions.containsKey(region.getEncodedName());
    if (!hasit) {
      LOG.warn("Received close for region we are not serving; " +
        region.getEncodedName());
      throw new NotServingRegionException("Received close for "
        + region.getRegionNameAsString() + " but we are not serving it");
    }
    return closeRegion(region, false, zk, versionOfClosingNode);
  }

  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public boolean closeRegion(byte[] encodedRegionName, boolean zk)
    throws IOException {
    return closeRegion(encodedRegionName, false, zk);
  }

  /**
   * @param region Region to close
   * @param abort True if we are aborting
   * @param zk True if we are to update zk about the region close; if the close
   * was orchestrated by master, then update zk.  If the close is being run by
   * the regionserver because its going down, don't update zk.
   * @return True if closed a region.
   */
  protected boolean closeRegion(HRegionInfo region, final boolean abort,
      final boolean zk) {
    return closeRegion(region, abort, zk, -1);
  }


  /**
   * @param region Region to close
   * @param abort True if we are aborting
   * @param zk True if we are to update zk about the region close; if the close
   * was orchestrated by master, then update zk.  If the close is being run by
   * the regionserver because its going down, don't update zk.
   * @param versionOfClosingNode
   *   the version of znode to compare when RS transitions the znode from
   *   CLOSING state.
   * @return True if closed a region.
   */
  protected boolean closeRegion(HRegionInfo region, final boolean abort,
      final boolean zk, final int versionOfClosingNode) {
    
    HRegion actualRegion = this.getFromOnlineRegions(region.getEncodedName());
    if (actualRegion != null && actualRegion.getCoprocessorHost() != null) {
      try {
        actualRegion.getCoprocessorHost().preClose(abort);
      } catch (IOException e) {
        LOG.warn(e);
        return false;
      }
    }
    try {
      addRegionsInTransition(region, CLOSE);
    } catch (RegionAlreadyInTransitionException rate) {
      LOG.warn("Received close for region we are already opening or closing; "
          + region.getEncodedName());
      return false;
    }
    boolean success = false;
    try {
      CloseRegionHandler crh = null;
      if (region.isRootRegion()) {
        crh = new CloseRootHandler(this, this, region, abort, zk, versionOfClosingNode);
      } else if (region.isMetaRegion()) {
        crh = new CloseMetaHandler(this, this, region, abort, zk, versionOfClosingNode);
      } else {
        crh = new CloseRegionHandler(this, this, region, abort, zk, versionOfClosingNode);
      }
      this.service.submit(crh);
      success = true;
    } finally {
      // Remove from this server's RIT.
      if (!success) removeFromRegionsInTransition(region);
    }
    return true;
  }

  /**
   * @param encodedRegionName
   *          encodedregionName to close
   * @param abort
   *          True if we are aborting
   * @param zk
   *          True if we are to update zk about the region close; if the close
   *          was orchestrated by master, then update zk. If the close is being
   *          run by the regionserver because its going down, don't update zk.
   * @return True if closed a region.
   */
  protected boolean closeRegion(byte[] encodedRegionName, final boolean abort,
      final boolean zk) throws IOException {
    String encodedRegionNameStr = Bytes.toString(encodedRegionName);
    HRegion region = this.getFromOnlineRegions(encodedRegionNameStr);
    if (null != region) {
      return closeRegion(region.getRegionInfo(), abort, zk);
    }
    LOG.error("The specified region name" + encodedRegionNameStr
        + " does not exist to close the region.");
    return false;
  }

  // Manual remote region administration RPCs

  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public void flushRegion(HRegionInfo regionInfo)
      throws NotServingRegionException, IOException {
    checkOpen();
    LOG.info("Flushing " + regionInfo.getRegionNameAsString());
    HRegion region = getRegion(regionInfo.getRegionName());
    boolean needsCompaction = region.flushcache().isCompactionNeeded();
    if (needsCompaction) {
      this.compactSplitThread.requestCompaction(region, "Compaction through user triggered flush");
    }
  }

  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public void splitRegion(HRegionInfo regionInfo)
      throws NotServingRegionException, IOException {
    splitRegion(regionInfo, null);
  }

  @Override
  public void splitRegion(HRegionInfo regionInfo, byte[] splitPoint)
      throws NotServingRegionException, IOException {
    checkOpen();
    HRegion region = getRegion(regionInfo.getRegionName());
    region.flushcache();
    region.forceSplit(splitPoint);
    compactSplitThread.requestSplit(region, region.checkSplit());
  }

  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public void compactRegion(HRegionInfo regionInfo, boolean major)
      throws NotServingRegionException, IOException {
    compactRegion(regionInfo, major, null);
  }

  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public void compactRegion(HRegionInfo regionInfo, boolean major,  byte[] family)
      throws NotServingRegionException, IOException {
    checkOpen();
    HRegion region = getRegion(regionInfo.getRegionName());
    Store store = null;
    if (family != null) {
      store = region.getStore(family);
      if (store == null) {
        throw new IOException("column family " + Bytes.toString(family) +
          " does not exist in region " + new String(region.getRegionNameAsString()));
      }
    }

    if (major) {
      if (family != null) {
        store.triggerMajorCompaction();
      } else {
        region.triggerMajorCompaction();
      }
    }
    String familyLogMsg = (family != null)?" for column family: " + Bytes.toString(family):"";
    LOG.trace("User-triggered compaction requested for region " +
      region.getRegionNameAsString() + familyLogMsg);
    String log = "User-triggered " + (major ? "major " : "") + "compaction" + familyLogMsg;
    if (family != null) {
      compactSplitThread.requestCompaction(region, store, log,
        Store.PRIORITY_USER, null);
    } else {
      compactSplitThread.requestCompaction(region, log,
        Store.PRIORITY_USER, null);
    }
  }

  /** @return the info server */
  public InfoServer getInfoServer() {
    return infoServer;
  }

  /**
   * @return true if a stop has been requested.
   */
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
  public Configuration getConfiguration() {
    return conf;
  }

  public CacheConfig getCacheConfig() {
    return cacheConfig;
  }

  /** @return the write lock for the server */
  ReentrantReadWriteLock.WriteLock getWriteLock() {
    return lock.writeLock();
  }

  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public List<HRegionInfo> getOnlineRegions() throws IOException {
    checkOpen();
    List<HRegionInfo> list = new ArrayList<HRegionInfo>(onlineRegions.size());
    for (Map.Entry<String,HRegion> e: this.onlineRegions.entrySet()) {
      list.add(e.getValue().getRegionInfo());
    }
    Collections.sort(list);
    return list;
  }

  public int getNumberOfOnlineRegions() {
    return this.onlineRegions.size();
  }

  boolean isOnlineRegionsEmpty() {
    return this.onlineRegions.isEmpty();
  }

  /**
   * @param encodedRegionName
   * @return JSON Map of labels to values for passed in <code>encodedRegionName</code>
   * @throws IOException
   */
  public byte [] getRegionStats(final String encodedRegionName)
  throws IOException {
    HRegion r = null;
    synchronized (this.onlineRegions) {
      r = this.onlineRegions.get(encodedRegionName);
    }
    if (r == null) return null;
    ObjectMapper mapper = new ObjectMapper();
    int stores = 0;
    int storefiles = 0;
    int storefileSizeMB = 0;
    int memstoreSizeMB = (int) (r.memstoreSize.get() / 1024 / 1024);
    int storefileIndexSizeMB = 0;
    long totalCompactingKVs = 0;
    long currentCompactedKVs = 0;
    synchronized (r.stores) {
      stores += r.stores.size();
      for (Store store : r.stores.values()) {
        storefiles += store.getStorefilesCount();
        storefileSizeMB += (int) (store.getStorefilesSize() / 1024 / 1024);
        storefileIndexSizeMB += (int) (store.getStorefilesIndexSize() / 1024 / 1024);
      }
    }
    Map<String, Integer> map = new TreeMap<String, Integer>();
    map.put("stores", stores);
    map.put("storefiles", storefiles);
    map.put("storefileSizeMB", storefileIndexSizeMB);
    map.put("memstoreSizeMB", memstoreSizeMB);
    StringWriter w = new StringWriter();
    mapper.writeValue(w, map);
    w.close();
    return Bytes.toBytes(w.toString());
  }

  /**
   * For tests and web ui.
   * This method will only work if HRegionServer is in the same JVM as client;
   * HRegion cannot be serialized to cross an rpc.
   * @see #getOnlineRegions()
   */
  public Collection<HRegion> getOnlineRegionsLocalContext() {
    Collection<HRegion> regions = this.onlineRegions.values();
    return Collections.unmodifiableCollection(regions);
  }

  @Override
  public void addToOnlineRegions(HRegion region) {
    this.onlineRegions.put(region.getRegionInfo().getEncodedName(), region);
  }

  @Override
  public boolean removeFromOnlineRegions(final String encodedName) {
    HRegion toReturn = null;
    toReturn = this.onlineRegions.remove(encodedName);
    
    //Clear all of the dynamic metrics as they are now probably useless.
    //This is a clear because dynamic metrics could include metrics per cf and
    //per hfile.  Figuring out which cfs, hfiles, and regions are still relevant to
    //this region server would be an onerous task.  Instead just clear everything
    //and on the next tick of the metrics everything that is still relevant will be
    //re-added.
    this.dynamicMetrics.clear();
    return toReturn != null;
  }

  /**
   * @return A new Map of online regions sorted by region size with the first
   *         entry being the biggest.
   */
  public SortedMap<Long, HRegion> getCopyOfOnlineRegionsSortedBySize() {
    // we'll sort the regions in reverse
    SortedMap<Long, HRegion> sortedRegions = new TreeMap<Long, HRegion>(
        new Comparator<Long>() {
          public int compare(Long a, Long b) {
            return -1 * a.compareTo(b);
          }
        });
    // Copy over all regions. Regions are sorted by size with biggest first.
    for (HRegion region : this.onlineRegions.values()) {
      sortedRegions.put(Long.valueOf(region.memstoreSize.get()), region);
    }
    return sortedRegions;
  }

  @Override
  public HRegion getFromOnlineRegions(final String encodedRegionName) {
    HRegion r = null;
    r = this.onlineRegions.get(encodedRegionName);
    return r;
  }

  /**
   * @param regionName
   * @return HRegion for the passed binary <code>regionName</code> or null if
   *         named region is not member of the online regions.
   */
  public HRegion getOnlineRegion(final byte[] regionName) {
    return getFromOnlineRegions(HRegionInfo.encodeRegionName(regionName));
  }

  /** @return the request count */
  public AtomicInteger getRequestCount() {
    return this.requestCount;
  }

  /**
   * @return time stamp in millis of when this region server was started
   */
  public long getStartcode() {
    return this.startcode;
  }

  /** @return reference to FlushRequester */
  public FlushRequester getFlushRequester() {
    return this.cacheFlusher;
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
    HRegion region = null;
    region = getOnlineRegion(regionName);
    if (region == null) {
      throw new NotServingRegionException("Region is not online: " +
        Bytes.toStringBinary(regionName));
    }
    return region;
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

  /**
   * Called to verify that this server is up and running.
   *
   * @throws IOException
   */
  protected void checkOpen() throws IOException {
    if (this.stopped || this.abortRequested) {
      throw new RegionServerStoppedException("Server " + getServerName() +
        " not running" + (this.abortRequested ? ", aborting" : ""));
    }
    if (!fsOk) {
      throw new RegionServerStoppedException("File system not available");
    }
  }

  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public ProtocolSignature getProtocolSignature(
      String protocol, long version, int clientMethodsHashCode)
  throws IOException {
    if (protocol.equals(HRegionInterface.class.getName())) {
      return new ProtocolSignature(HRegionInterface.VERSION, null);
    }
    throw new IOException("Unknown protocol: " + protocol);
  }

  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public long getProtocolVersion(final String protocol, final long clientVersion)
  throws IOException {
    if (protocol.equals(HRegionInterface.class.getName())) {
      return HRegionInterface.VERSION;
    }
    throw new IOException("Unknown protocol: " + protocol);
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
  public FileSystem getFileSystem() {
    return fs;
  }

  /**
   * @return This servers {@link HServerInfo}
   */
  // TODO: Deprecate and do getServerName instead.
  public HServerInfo getServerInfo() {
    try {
      return getHServerInfo();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public void mutateRow(byte[] regionName, RowMutations rm)
      throws IOException {
    checkOpen();
    if (regionName == null) {
      throw new IOException("Invalid arguments to mutateRow " +
      "regionName is null");
    }
    requestCount.incrementAndGet();
    try {
      HRegion region = getRegion(regionName);
      if (!region.getRegionInfo().isMetaTable()) {
        this.cacheFlusher.reclaimMemStoreMemory();
      }
      region.mutateRow(rm);
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  @Override
  public Result append(byte[] regionName, Append append)
  throws IOException {
    checkOpen();
    if (regionName == null) {
      throw new IOException("Invalid arguments to increment " +
      "regionName is null");
    }
    requestCount.incrementAndGet();
    try {
      HRegion region = getRegion(regionName);
      Integer lock = getLockFromId(append.getLockId());
      Append appVal = append;
      Result resVal;
      if (region.getCoprocessorHost() != null) {
        resVal = region.getCoprocessorHost().preAppend(appVal);
        if (resVal != null) {
          return resVal;
        }
      }
      resVal = region.append(appVal, lock, append.getWriteToWAL());
      if (region.getCoprocessorHost() != null) {
        region.getCoprocessorHost().postAppend(appVal, resVal);
      }
      return resVal;
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  @Override
  public Result increment(byte[] regionName, Increment increment)
  throws IOException {
    checkOpen();
    if (regionName == null) {
      throw new IOException("Invalid arguments to increment " +
      "regionName is null");
    }
    requestCount.incrementAndGet();
    try {
      HRegion region = getRegion(regionName);
      Integer lock = getLockFromId(increment.getLockId());
      Increment incVal = increment;
      Result resVal;
      if (region.getCoprocessorHost() != null) {
        resVal = region.getCoprocessorHost().preIncrement(incVal);
        if (resVal != null) {
          return resVal;
        }
      }
      resVal = region.increment(incVal, lock,
          increment.getWriteToWAL());
      if (region.getCoprocessorHost() != null) {
        resVal = region.getCoprocessorHost().postIncrement(incVal, resVal);
      }
      return resVal;
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  /** {@inheritDoc} */
  public long incrementColumnValue(byte[] regionName, byte[] row,
      byte[] family, byte[] qualifier, long amount, boolean writeToWAL)
      throws IOException {
    checkOpen();

    if (regionName == null) {
      throw new IOException("Invalid arguments to incrementColumnValue "
          + "regionName is null");
    }
    requestCount.incrementAndGet();
    try {
      HRegion region = getRegion(regionName);
      if (region.getCoprocessorHost() != null) {
        Long amountVal = region.getCoprocessorHost().preIncrementColumnValue(row,
          family, qualifier, amount, writeToWAL);
        if (amountVal != null) {
          return amountVal.longValue();
        }
      }
      long retval = region.incrementColumnValue(row, family, qualifier, amount,
        writeToWAL);
      if (region.getCoprocessorHost() != null) {
        retval = region.getCoprocessorHost().postIncrementColumnValue(row,
          family, qualifier, amount, writeToWAL, retval);
      }
      return retval;
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  /** {@inheritDoc}
   * @deprecated Use {@link #getServerName()} instead.
   */
  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public HServerInfo getHServerInfo() throws IOException {
    checkOpen();
    return new HServerInfo(new HServerAddress(this.isa),
      this.startcode, this.rsInfo.getInfoPort());
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R> MultiResponse multi(MultiAction<R> multi) throws IOException {
    checkOpen();
    MultiResponse response = new MultiResponse();
    for (Map.Entry<byte[], List<Action<R>>> e : multi.actions.entrySet()) {
      byte[] regionName = e.getKey();
      List<Action<R>> actionsForRegion = e.getValue();
      // sort based on the row id - this helps in the case where we reach the
      // end of a region, so that we don't have to try the rest of the
      // actions in the list.
      Collections.sort(actionsForRegion);
      Row action;
      List<Action<R>> mutations = new ArrayList<Action<R>>();
      for (Action<R> a : actionsForRegion) {
        action = a.getAction();
        int originalIndex = a.getOriginalIndex();

        try {
          if (action instanceof Delete || action instanceof Put) {
            mutations.add(a); 
          } else if (action instanceof Get) {
            response.add(regionName, originalIndex,
                get(regionName, (Get)action));
          } else if (action instanceof Exec) {
            ExecResult result = execCoprocessor(regionName, (Exec)action);
            response.add(regionName, new Pair<Integer, Object>(
                a.getOriginalIndex(), result.getValue()
            ));
          } else if (action instanceof Increment) {
            response.add(regionName, originalIndex,
                increment(regionName, (Increment)action));
          } else if (action instanceof Append) {
            response.add(regionName, originalIndex,
                append(regionName, (Append)action));
          } else if (action instanceof RowMutations) {
            mutateRow(regionName, (RowMutations)action);
            response.add(regionName, originalIndex, new Result());
          } else {
            LOG.debug("Error: invalid Action, row must be a Get, Delete, " +
                "Put, Exec, Increment, or Append.");
            throw new DoNotRetryIOException("Invalid Action, row must be a " +
                "Get, Delete, Put, Exec, Increment, or Append.");
          }
        } catch (IOException ex) {
          response.add(regionName, originalIndex, ex);
        }
      }

      // We do the puts with result.put so we can get the batching efficiency
      // we so need. All this data munging doesn't seem great, but at least
      // we arent copying bytes or anything.
      if (!mutations.isEmpty()) {
        try {
          HRegion region = getRegion(regionName);

          if (!region.getRegionInfo().isMetaTable()) {
            this.cacheFlusher.reclaimMemStoreMemory();
          }

          List<Pair<Mutation,Integer>> mutationsWithLocks =
              Lists.newArrayListWithCapacity(mutations.size());
          for (Action<R> a : mutations) {
            Mutation m = (Mutation) a.getAction();

            Integer lock;
            try {
              lock = getLockFromId(m.getLockId());
            } catch (UnknownRowLockException ex) {
              response.add(regionName, a.getOriginalIndex(), ex);
              continue;
            }
            mutationsWithLocks.add(new Pair<Mutation, Integer>(m, lock));
          }

          this.requestCount.addAndGet(mutations.size());

          OperationStatus[] codes =
              region.batchMutate(mutationsWithLocks.toArray(new Pair[]{}));

          for( int i = 0 ; i < codes.length ; i++) {
            OperationStatus code = codes[i];

            Action<R> theAction = mutations.get(i);
            Object result = null;

            if (code.getOperationStatusCode() == OperationStatusCode.SUCCESS) {
              result = new Result();
            } else if (code.getOperationStatusCode()
                == OperationStatusCode.SANITY_CHECK_FAILURE) {
              // Don't send a FailedSanityCheckException as older clients will not know about
              // that class being a subclass of DoNotRetryIOException
              // and will retry mutations that will never succeed.
              result = new DoNotRetryIOException(code.getExceptionMsg());
            } else if (code.getOperationStatusCode() == OperationStatusCode.BAD_FAMILY) {
              result = new NoSuchColumnFamilyException(code.getExceptionMsg());
            }
            // FAILURE && NOT_RUN becomes null, aka: need to run again.

            response.add(regionName, theAction.getOriginalIndex(), result);
          }
        } catch (IOException ioe) {
          // fail all the puts with the ioe in question.
          for (Action<R> a: mutations) {
            response.add(regionName, a.getOriginalIndex(), ioe);
          }
        }
      }
    }
    return response;
  }

  /**
   * Executes a single {@link org.apache.hadoop.hbase.ipc.CoprocessorProtocol}
   * method using the registered protocol handlers.
   * {@link CoprocessorProtocol} implementations must be registered per-region
   * via the
   * {@link org.apache.hadoop.hbase.regionserver.HRegion#registerProtocol(Class, org.apache.hadoop.hbase.ipc.CoprocessorProtocol)}
   * method before they are available.
   *
   * @param regionName name of the region against which the invocation is executed
   * @param call an {@code Exec} instance identifying the protocol, method name,
   *     and parameters for the method invocation
   * @return an {@code ExecResult} instance containing the region name of the
   *     invocation and the return value
   * @throws IOException if no registered protocol handler is found or an error
   *     occurs during the invocation
   * @see org.apache.hadoop.hbase.regionserver.HRegion#registerProtocol(Class, org.apache.hadoop.hbase.ipc.CoprocessorProtocol)
   */
  @Override
  public ExecResult execCoprocessor(byte[] regionName, Exec call)
      throws IOException {
    checkOpen();
    requestCount.incrementAndGet();
    try {
      HRegion region = getRegion(regionName);
      return region.exec(call);
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

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
  public ServerName getServerName() {
    // Our servername could change after we talk to the master.
    return this.serverNameFromMasterPOV == null?
      new ServerName(this.isa.getHostName(), this.isa.getPort(), this.startcode):
        this.serverNameFromMasterPOV;
  }

  @Override
  public CompactionRequestor getCompactionRequester() {
    return this.compactSplitThread;
  }

  public ZooKeeperWatcher getZooKeeperWatcher() {
    return this.zooKeeper;
  }

  public RegionServerCoprocessorHost getCoprocessorHost(){
    return this.rsHost;
  }

  @Override
  public boolean removeFromRegionsInTransition(final HRegionInfo hri) {
    Boolean res = this.regionsInTransitionInRS.remove(hri.getEncodedNameAsBytes());
    return res != null && res.booleanValue();
  }

  @Override
  public boolean containsKeyInRegionsInTransition(final HRegionInfo hri) {
    return this.regionsInTransitionInRS.containsKey(hri.getEncodedNameAsBytes());
  }

  public ExecutorService getExecutorService() {
    return service;
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
    if (!conf.getBoolean(HConstants.REPLICATION_ENABLE_KEY, false)) {
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
    }
    else {
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
      throw new IOException("Cound not find class for " + classname);
    }

    // create an instance of the replication object.
    ReplicationService service = (ReplicationService)
                              ReflectionUtils.newInstance(clazz, conf);
    service.initialize(server, fs, logDir, oldLogDir);
    return service;
  }

  /**
   * @param hrs
   * @return Thread the RegionServer is running in correctly named.
   * @throws IOException
   */
  public static Thread startRegionServer(final HRegionServer hrs)
      throws IOException {
    return startRegionServer(hrs, "regionserver" + hrs.isa.getPort());
  }

  /**
   * @param hrs
   * @param name
   * @return Thread the RegionServer is running in correctly named.
   * @throws IOException
   */
  public static Thread startRegionServer(final HRegionServer hrs,
      final String name) throws IOException {
    Thread t = new Thread(hrs);
    t.setName(name);
    t.start();
    // Install shutdown hook that will catch signals and run an orderly shutdown
    // of the hrs.
    ShutdownHook.install(hrs.getConfiguration(), FileSystem.get(hrs
        .getConfiguration()), hrs, t);
    return t;
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

  @Override
  @QosPriority(priority=HConstants.REPLICATION_QOS)
  public void replicateLogEntries(final HLog.Entry[] entries)
  throws IOException {
    checkOpen();
    if (this.replicationSinkHandler == null) return;
    this.replicationSinkHandler.replicateLogEntries(entries);
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

  @Override
  public List<BlockCacheColumnFamilySummary> getBlockCacheColumnFamilySummaries() throws IOException {
    BlockCache c = new CacheConfig(this.conf).getBlockCache();
    return c.getBlockCacheColumnFamilySummaries(this.conf);
  }

  @Override
  public byte[][] rollHLogWriter() throws IOException, FailedLogCloseException {
    HLog wal = this.getWAL();
    return wal.rollWriter(true);
  }

  /**
   * Gets the online regions of the specified table.
   * This method looks at the in-memory onlineRegions.  It does not go to <code>.META.</code>.
   * Only returns <em>online</em> regions.  If a region on this table has been
   * closed during a disable, etc., it will not be included in the returned list.
   * So, the returned list may not necessarily be ALL regions in this table, its
   * all the ONLINE regions in the table.
   * @param tableName
   * @return Online regions from <code>tableName</code>
   */
   public List<HRegion> getOnlineRegions(byte[] tableName) {
     List<HRegion> tableRegions = new ArrayList<HRegion>();
     synchronized (this.onlineRegions) {
       for (HRegion region: this.onlineRegions.values()) {
         HRegionInfo regionInfo = region.getRegionInfo();
         if(Bytes.equals(regionInfo.getTableName(), tableName)) {
           tableRegions.add(region);
         }
       }
     }
     return tableRegions;
   }

  // used by org/apache/hbase/tmpl/regionserver/RSStatusTmpl.jamon (HBASE-4070).
  public String[] getCoprocessors() {
    TreeSet<String> coprocessors = new TreeSet<String>(
        this.hlog.getCoprocessorHost().getCoprocessors());
    Collection<HRegion> regions = getOnlineRegionsLocalContext();
    for (HRegion region: regions) {
      coprocessors.addAll(region.getCoprocessorHost().getCoprocessors());
    }
    return coprocessors.toArray(new String[0]);
  }

  /**
   * Register bean with platform management server
   */
  @SuppressWarnings("deprecation")
  void registerMBean() {
    MXBeanImpl mxBeanInfo = MXBeanImpl.init(this);
    mxBean = MBeanUtil.registerMBean("RegionServer", "RegionServer",
        mxBeanInfo);
    LOG.info("Registered RegionServer MXBean");
  }

  /**
   * Get the current compaction state of the region.
   *
   * @param regionName the name of the region to check compaction statte.
   * @return the compaction state name.
   * @throws IOException exception
   */
  public String getCompactionState(final byte[] regionName) throws IOException {
      checkOpen();
      requestCount.incrementAndGet();
      HRegion region = getRegion(regionName);
      HRegionInfo info = region.getRegionInfo();
      return CompactionRequest.getCompactionState(info.getRegionId()).name();
  }

  public long getResponseQueueSize(){
    if (server != null) {
      return server.getResponseQueueSize();
    }
    return 0;
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
}
