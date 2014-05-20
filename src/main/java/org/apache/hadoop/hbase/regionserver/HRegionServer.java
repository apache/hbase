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
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Constructor;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HMsg.Type;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LeaseListener;
import org.apache.hadoop.hbase.Leases;
import org.apache.hadoop.hbase.Leases.LeaseStillHeldException;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.UnknownRowLockException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.MultiPut;
import org.apache.hadoop.hbase.client.MultiPutResponse;
import org.apache.hadoop.hbase.client.MultiResponse;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ServerConnection;
import org.apache.hadoop.hbase.client.ServerConnectionManager;
import org.apache.hadoop.hbase.client.TableServers;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.coprocessor.endpoints.EndpointServer;
import org.apache.hadoop.hbase.coprocessor.endpoints.IEndpointServer;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.L2BucketCache;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache.CacheStats;
import org.apache.hadoop.hbase.io.hfile.PreloadThreadPool;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram.Bucket;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HBaseRPCErrorHandler;
import org.apache.hadoop.hbase.ipc.HBaseRPCOptions;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.ipc.HBaseRpcMetrics;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.HBaseServer.Call;
import org.apache.hadoop.hbase.ipc.HMasterRegionInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.ScannerResult;
import org.apache.hadoop.hbase.ipc.thrift.HBaseThriftRPC;
import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;
import org.apache.hadoop.hbase.master.AssignmentPlan;
import org.apache.hadoop.hbase.master.RegionPlacement;
import org.apache.hadoop.hbase.regionserver.metrics.RegionServerDynamicMetrics;
import org.apache.hadoop.hbase.regionserver.metrics.RegionServerMetrics;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics.StoreMetricType;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.thrift.HBaseNiftyThriftServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CompoundBloomFilter;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.hbase.util.InjectionEvent;
import org.apache.hadoop.hbase.util.InjectionHandler;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ParamFormat;
import org.apache.hadoop.hbase.util.ParamFormatter;
import org.apache.hadoop.hbase.util.RuntimeHaltAbortStrategy;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.StringBytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.google.common.base.Preconditions;

/**
 * HRegionServer makes a set of HRegions available to clients.  It checks in with
 * the HMaster. There are many HRegionServers in a single HBase deployment.
 */
public class HRegionServer implements HRegionServerIf, HBaseRPCErrorHandler,
    Runnable, Watcher, ConfigurationObserver {
  public static final Log LOG = LogFactory.getLog(HRegionServer.class);
  private static final HMsg REPORT_EXITING = new HMsg(Type.MSG_REPORT_EXITING);
  private static final HMsg REPORT_RESTARTING = new HMsg(
      Type.MSG_REPORT_EXITING_FOR_RESTART);
  private static final HMsg REPORT_BEGINNING_OF_THE_END = new HMsg(
      Type.MSG_REPORT_BEGINNING_OF_THE_END);
  private static final HMsg REPORT_QUIESCED = new HMsg(Type.MSG_REPORT_QUIESCED);
  private static final HMsg [] EMPTY_HMSG_ARRAY = new HMsg [] {};
  private static final String UNABLE_TO_READ_MASTER_ADDRESS_ERR_MSG =
      "Unable to read master address from ZooKeeper";
  private static final ArrayList<Put> emptyPutArray = new ArrayList<Put>(0);
  private static final int DEFAULT_NUM_TRACKED_CLOSED_REGION = 3;
  private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  // Set when a report to the master comes back with a message asking us to
  // shutdown.  Also set by call to stop when debugging or running unit tests
  // of HRegionServer in isolation. We use AtomicBoolean rather than
  // plain boolean so we can pass a reference to Chore threads.  Otherwise,
  // Chore threads need to know about the hosting class.
  // Stop is going to happen in 2 stages:
  // In Stage 1: we will close all non-essential chores, and start closing the regions.
  //    To reduce downtime, closing a region involves two flushes: a pre-flush,
  //    and a second flush. The pre-flush is going to flush all the data in
  //    memstore so far, while serving client requests in parallel. The second
  //    flush will flush the puts received during the pre-flush. It is essential
  //    that the chores corresponding to MemstoreFlusher, and the LogRoller are
  //    active at this stage.
  // In Stage 2: we have closed all the regions. So, we will close out the remaining
  //    threads and chores that need to be up during stage 1 (Memstore flusher,
  //    and log roller).
  protected final AtomicBoolean stopRequestedAtStageOne = new AtomicBoolean(false);
  protected final AtomicBoolean stopRequestedAtStageTwo = new AtomicBoolean(false);

  protected final AtomicBoolean quiesced = new AtomicBoolean(false);

  // Go down hard.  Used if file system becomes unavailable and also in
  // debugging and unit tests.
  protected volatile boolean abortRequested;

  protected volatile boolean restartRequested;

  private volatile boolean killed = false;

  // If false, the file system has become unavailable
  protected volatile boolean fsOk;

  // TODO gauravm
  // When both Hadoop RPC and Thrift RPC are switched on, what do we do here?
  protected volatile HServerInfo serverInfo;
  // HServerInfo for the RPC Server
  protected volatile HServerInfo rpcServerInfo;

  protected final Configuration conf;

  private final ServerConnection connection;
  private final TableServers regionServerConnection;

  protected final AtomicBoolean haveRootRegion = new AtomicBoolean(false);
  private FileSystem fs;
  private Path rootDir;
  private final Random rand = new Random();

  // Key is Bytes.hashCode of region name byte array and the value is HRegion
  // in both of the maps below.  Use Bytes.mapKey(byte []) generating key for
  // below maps.
  protected final Map<Integer, HRegion> onlineRegions =
    new ConcurrentHashMap<Integer, HRegion>();
  protected final Map<Integer, HRegionInfo> regionsOpening =
    new ConcurrentHashMap<Integer, HRegionInfo>();
  protected final Map<Integer, HRegionInfo> regionsClosing =
    new ConcurrentHashMap<Integer, HRegionInfo>();

  // this is a list of region info that we recently closed
  protected final List<ClosedRegionInfo> recentlyClosedRegions =
    new ArrayList<ClosedRegionInfo>(DEFAULT_NUM_TRACKED_CLOSED_REGION + 1);

  // This a list of regions that we need to re-try closing.
  protected final Map<Integer, HRegion> retryCloseRegions = Collections
      .synchronizedMap(new HashMap<Integer, HRegion>());

  protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final LinkedBlockingQueue<HMsg> outboundMsgs =
    new LinkedBlockingQueue<HMsg>();

  final int numRetries;
  protected final int threadWakeFrequency;
  private final int msgInterval;

  protected final int numRegionsToReport;

  // Remote HMaster
  private HMasterRegionInterface hbaseMaster;

  // Server to handle client requests.  Default access so can be accessed by
  // unit tests.
  HBaseServer server;
  private volatile boolean isRpcServerRunning;

  // Leases
  private Leases leases;

  // Info server.  Default access so can be used by unit tests.  REGIONSERVER
  // is name of the webapp and the attribute name used stuffing this instance
  // into web context.
  InfoServer infoServer;

  /** region server process name */
  public static final String REGIONSERVER = "regionserver";

  /*
   * Space is reserved in HRS constructor and then released when aborting
   * to recover from an OOME. See HBASE-706.  TODO: Make this percentage of the
   * heap or a minimum.
   */
  private final LinkedList<byte[]> reservedSpace = new LinkedList<byte []>();

  private RegionServerMetrics metrics;

  @SuppressWarnings("unused")
  private RegionServerDynamicMetrics dynamicMetrics;

  // Compactions
  public CompactSplitThread compactSplitThread;

  // Cache flushing
  MemStoreFlusher cacheFlusher;

  /* Check for major compactions.
   */
  Chore majorCompactionChecker;
  /*
   * Threadpool for doing scanner prefetches
   */
  public static ThreadPoolExecutor scanPrefetchThreadPool;

  // An array of HLog and HLog roller.  log is protected rather than private to avoid
  // eclipse warning when accessed by inner classes
  protected volatile HLog[] hlogs;
  protected LogRoller[] hlogRollers;

  private volatile int currentHLogIndex = 0;
  private Map<String, Integer> regionNameToHLogIDMap =
      new ConcurrentHashMap<String, Integer>();

  // flag set after we're done setting up server threads (used for testing)
  protected volatile boolean isOnline;

  final ConcurrentHashMap<String, InternalScanner> scanners =
    new ConcurrentHashMap<String, InternalScanner>();

  private ZooKeeperWrapper zooKeeperWrapper;

  private final ExecutorService logCloseThreadPool;
  private final ExecutorService regionOpenCloseThreadPool;

  // Log Splitting Worker
  private List<SplitLogWorker> splitLogWorkers;

  // A sleeper that sleeps for msgInterval.
  private final Sleeper sleeper;

  private final int rpcTimeoutToMaster;

  // Address passed in to constructor.  This is not always the address we run
  // with.  For example, if passed port is 0, then we are to pick a port.  The
  // actual address we run with is in the #serverInfo data member.
  private final HServerAddress address;

  // The main region server thread.
  private Thread regionServerThread;

  private final String machineName;

  private final AtomicLong globalMemstoreSize = new AtomicLong(0);

  // reference to the Thrift Server.
  volatile private HRegionThriftServer thriftServer;

  // The nifty thrift server
  volatile private HBaseNiftyThriftServer niftyThriftServer;

  // Cache configuration and block cache reference
  private final CacheConfig cacheConfig;

  // prevents excessive checking of filesystem
  private int minCheckFSIntervalMillis;

  // abort if checkFS continues to fail for this long
  private int checkFSAbortTimeOutMillis;

  // time when last checkFS was done
  private AtomicLong lastCheckFSAt = new AtomicLong(0);

  // time since checkFS has been failing
  private volatile long checkFSFailingSince;

  private String stopReason = "not stopping";

  // profiling threadlocal
  public static final ThreadLocal<Call> callContext = new ThreadLocal<Call> ();

  /** Regionserver launched by the main method. Not used in tests. */
  private static HRegionServer mainRegionServer;
  /** Keep a reference to the current active master for use by HLogSplitter. */
  private final AtomicReference<HMasterRegionInterface> masterRef =
      new AtomicReference<HMasterRegionInterface>();

  // maximum size (in bytes) for any allowed call. Applies to gets/nexts. If
  // the size of the Result to be returned exceeds this value, the server will
  // throw out an Exception. This is done to avoid OutOfMemory Errors, and
  // large GC issues.
  private static long responseSizeLimit;
  public static final AtomicBoolean enableServerSideProfilingForAllCalls =
      new AtomicBoolean(false);
  public static AtomicInteger numOptimizedSeeks = new AtomicInteger(0);
  private int numRowRequests = 0;

  // Configurations for quorum reads
  private int quorumReadThreadsMax = HConstants.DEFAULT_HDFS_QUORUM_READ_THREADS_MAX;
  private long quorumReadTimeoutMillis = HConstants.DEFAULT_HDFS_QUORUM_READ_TIMEOUT_MILLIS;

  public static boolean runMetrics = true;
  public static boolean useSeekNextUsingHint;

  // This object lets classes register themselves to get notified on
  // Configuration changes.
  public static final ConfigurationManager configurationManager =
          new ConfigurationManager();
  private boolean useThrift;
  private boolean useHadoopRPC;

  private IEndpointServer endpointServer = new EndpointServer();

  public static long getResponseSizeLimit() {
    return responseSizeLimit;
  }

  public static void setResponseSizeLimit(long responseSizeLimit) {
    HRegionServer.responseSizeLimit = responseSizeLimit;
  }

  // Cause artificial delay before region opening finishs, for testing purpose.
  // It should always be 0 in production environment!
  public static volatile long openRegionDelay = 0;

  /** Configuration variable to prefer IPv6 address for HRegionServer */
  public static final String HREGIONSERVER_PREFER_IPV6_Address =
    "hregionserver.prefer.ipv6.address";

  /**
   * Starts a HRegionServer at the default location. This should be followed
   * by a call to the initialize() method on the HRegionServer object, to start
   * up the RPC servers.
   *
   * @param conf
   * @throws IOException
   */
  public HRegionServer(Configuration conf) throws IOException {
    LOG.debug("Region server configured with ZK client port "
        + conf.get(HConstants.ZOOKEEPER_CLIENT_PORT));
    String defaultHost = DNS.getDefaultHost(
        conf.get("hbase.regionserver.dns.interface","default"),
        conf.get("hbase.regionserver.dns.nameserver","default"));
    if (preferIpv6AddressForRegionServer(conf)) {
      // Use IPv6 address for HRegionServer.
      machineName = HServerInfo.getIPv6AddrIfLocalMachine(defaultHost);
    } else {
      machineName = defaultHost;
    }
    String addressStr = machineName + ":" +
      conf.get(HConstants.REGIONSERVER_PORT,
          Integer.toString(HConstants.DEFAULT_REGIONSERVER_PORT));
    // This is not necessarily the address we will run with.  The address we
    // use will be in #serverInfo data member
    // If we are passed a port 0 then we have to bind to ephemeral port to get
    // the port value
    address = new HServerAddress(addressStr);
    LOG.info("My address is " + address);

    this.abortRequested = false;
    this.fsOk = true;
    this.conf = conf;
    this.connection = ServerConnectionManager.getConnection(conf);
    if (connection instanceof TableServers) {
      // This is always true in the current case...
      regionServerConnection = (TableServers) connection;
    } else {
      // ... but handling this possibility, just in case something changes.
      regionServerConnection = new TableServers(conf);
    }

    this.isOnline = false;

    // Config'ed params
    this.numRetries =  conf.getInt("hbase.client.retries.number", 2);
    this.threadWakeFrequency = conf.getInt(HConstants.THREAD_WAKE_FREQUENCY,
        10 * 1000);
    this.msgInterval = conf.getInt("hbase.regionserver.msginterval",
        HConstants.REGION_SERVER_MSG_INTERVAL);

    sleeper = new Sleeper(this.msgInterval, this);

    // Task thread to process requests from Master
    this.worker = new Worker();

    this.numRegionsToReport =
      conf.getInt("hbase.regionserver.numregionstoreport", 10);

    this.rpcTimeoutToMaster = conf.getInt(
        HConstants.HBASE_RS_REPORT_TIMEOUT_KEY,
        HConstants.DEFAULT_RS_REPORT_TIMEOUT);

    responseSizeLimit = conf.getLong("hbase.regionserver.results.size.max",
        (long)Integer.MAX_VALUE); // set the max to 2G
    enableServerSideProfilingForAllCalls.set(conf.getBoolean(
        HConstants.HREGIONSERVER_ENABLE_SERVERSIDE_PROFILING, false));

    HRegionServer.useSeekNextUsingHint =
        conf.getBoolean("hbase.regionserver.scan.timestampfilter.allow_seek_next_using_hint", true);

    SchemaMetrics.configureGlobally(conf);
    cacheConfig = new CacheConfig(conf);
    configurationManager.registerObserver(cacheConfig);

    minCheckFSIntervalMillis =
        conf.getInt("hbase.regionserver.min.check.fs.interval", 30000);
    checkFSAbortTimeOutMillis =
        conf.getInt("hbase.regionserver.check.fs.abort.timeout",
                    conf.getInt(HConstants.ZOOKEEPER_SESSION_TIMEOUT,
                                HConstants.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT));
    if (minCheckFSIntervalMillis > checkFSAbortTimeOutMillis) {
      minCheckFSIntervalMillis = checkFSAbortTimeOutMillis;
    }
    LOG.info("minCheckFSIntervalMillis=" + minCheckFSIntervalMillis);
    LOG.info("checkFSAbortTimeOutMillis=" + checkFSAbortTimeOutMillis);

    int logCloseThreads =
        conf.getInt("hbase.hlog.split.close.threads", 20);
    logCloseThreadPool =
        Executors.newFixedThreadPool(logCloseThreads,
            new DaemonThreadFactory("hregionserver-split-logClose-thread-"));

    int maxRegionOpenCloseThreads = Math.max(1,
        conf.getInt(HConstants.HREGION_OPEN_AND_CLOSE_THREADS_MAX,
            HConstants.DEFAULT_HREGION_OPEN_AND_CLOSE_THREADS_MAX));
    regionOpenCloseThreadPool = Threads
        .getBoundedCachedThreadPool(maxRegionOpenCloseThreads, 30L, TimeUnit.SECONDS,
            new ThreadFactory() {
              private int count = 1;

              @Override
              public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "regionOpenCloseThread-" + count++);
                t.setDaemon(true);
                return t;
              }
            });
    // Construct threads for preloading
    int corePreloadThreads =
        conf.getInt(HConstants.CORE_PRELOAD_THREAD_COUNT,
          HConstants.DEFAULT_CORE_PRELOAD_THREAD_COUNT);
    int maxPreloadThreads =
        conf.getInt(HConstants.MAX_PRELOAD_THREAD_COUNT, HConstants.DEFAULT_MAX_PRELOAD_THREAD_COUNT);
    PreloadThreadPool.constructPreloaderThreadPool(corePreloadThreads, maxPreloadThreads);

    // Configure use of Guava bytes comparator.
    Bytes.useGuavaBytesComparision = conf.getBoolean(
        HConstants.USE_GUAVA_BYTES_COMPARISION,
        HConstants.DEFAULT_USE_GUAVA_BYTES_COMPARISION);
  }

  /**
   * Creates all of the state that needs to be reconstructed in case we are
   * doing a restart. This is shared between the constructor and restart().
   * Both call it. Initialize must be called outside the constructor since the
   * regionserver object would be unpublished at that point.
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public void initialize() throws IOException {
    this.restartRequested = false;
    this.abortRequested = false;
    this.stopRequestedAtStageOne.set(false);
    this.stopRequestedAtStageTwo.set(false);

    // Address is giving a default IP for the moment. Will be changed after
    // calling the master.
    int port = 0;
    useThrift =
      conf.getBoolean(HConstants.REGIONSERVER_USE_THRIFT,
                      HConstants.DEFAULT_REGIONSERVER_USE_THRIFT);
    useHadoopRPC =
      conf.getBoolean(HConstants.REGIONSERVER_USE_HADOOP_RPC,
                      HConstants.DEFAULT_REGIONSERVER_USE_HADOOP_RPC);
    if (this.useHadoopRPC) {
      if ((port = address.getPort()) == 0) {
        // start the RPC server to get the actual ephemeral port value
        this.server = HBaseRPC.getServer(this, address.getBindAddress(),
            address.getPort(),
            conf.getInt("hbase.regionserver.handler.count", 10),
            false, conf);
        this.server.setErrorHandler(this);
        port = this.server.getListenerAddress().getPort();
      }

      this.rpcServerInfo = new HServerInfo(new HServerAddress(
        new InetSocketAddress(address.getBindAddress(), port)),
        System.currentTimeMillis(), machineName);
    }
    if (useThrift) {
      int niftyServerPort =
          conf.getInt(HConstants.REGIONSERVER_SWIFT_PORT,
              HConstants.DEFAULT_REGIONSERVER_SWIFT_PORT);
      Class<? extends ThriftHRegionServer> thriftServerClass =
          (Class<? extends ThriftHRegionServer>)
              conf.getClass(HConstants.THRIFT_REGION_SERVER_IMPL, ThriftHRegionServer.class);

      ThriftHRegionServer thriftServer;
      try {
        thriftServer = thriftServerClass.getConstructor(HRegionServer.class).newInstance(this);
      } catch (Exception e) {
        throw new IOException(e);
      }
      niftyThriftServer = new HBaseNiftyThriftServer(this.conf, thriftServer, niftyServerPort);

      if (conf.getBoolean(
        HConstants.REGION_SERVER_WRITE_THRIFT_INFO_TO_META,
        HConstants.REGION_SERVER_WRITE_THRIFT_INFO_TO_META_DEFAULT)) {
        port = niftyThriftServer.getPort();
      }
      isRpcServerRunning = true;
    }

    this.serverInfo = new HServerInfo(new HServerAddress(
      new InetSocketAddress(address.getBindAddress(), port)),
      System.currentTimeMillis(), machineName);
    if (regionServerThread != null) {
      Threads.renameThread(regionServerThread, getRSThreadName());
    }
    if (this.serverInfo.getServerAddress() == null) {
      throw new NullPointerException("Server address cannot be null; " +
        "hbase-958 debugging");
    }
    reinitializeThreads();
    initializeZooKeeper();
    int nbBlocks = conf.getInt("hbase.regionserver.nbreservationblocks", 4);
    for(int i = 0; i < nbBlocks; i++)  {
      reservedSpace.add(new byte[HConstants.DEFAULT_SIZE_RESERVATION_BLOCK]);
    }

    this.endpointServer.initialize(conf, this);
  }

  @Override
  public ScannerResult scanOpen(byte[] regionName, Scan scan, int numberOfRows)
      throws ThriftHBaseException {
    // TODO add implementation
    return null;
  }

  @Override
  public ScannerResult scanNext(long id, int numberOfRows)
      throws ThriftHBaseException {
    // TODO add implementation
    return null;
  }

  @Override
  public boolean scanClose(long id) throws ThriftHBaseException {
    // TODO add implementation
    return false;
  }

  public int getHadoopRPCServerPort() {
    return this.serverInfo.getServerAddress().getPort();
  }

  public int getThriftServerPort() {
    return this.niftyThriftServer.getPort();
  }

  public HBaseRpcMetrics getThriftMetrics() {
    return niftyThriftServer.getRpcMetrics();
  }

  private void initializeZooKeeper() throws IOException {
    boolean abortProcesstIfZKExpired = conf.getBoolean(
        HConstants.ZOOKEEPER_SESSION_EXPIRED_ABORT_PROCESS, true);
    if (abortProcesstIfZKExpired) {
      zooKeeperWrapper = ZooKeeperWrapper.createInstance(conf,
          serverInfo.getServerName(), RuntimeHaltAbortStrategy.INSTANCE);
    } else {
      zooKeeperWrapper = ZooKeeperWrapper.createInstance(conf,
          serverInfo.getServerName(), new Abortable() {

            @Override
            public void abort(String why, Throwable e) {
              kill();
            }

            @Override
            public boolean isAborted() {
              return killed;
            }
          });
    }
    zooKeeperWrapper.registerListener(this);
    try {
      zooKeeperWrapper.watchMasterAddress(zooKeeperWrapper);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  private void reinitializeThreads() {
    this.workerThread = new Thread(worker);

    // Cache flushing thread.
    this.cacheFlusher = new MemStoreFlusher(conf, this);

    // Compaction thread
    this.compactSplitThread = new CompactSplitThread(this);
    // Registering the compactSplitThread object with the ConfigurationManager.
    configurationManager.registerObserver(this.compactSplitThread);
    configurationManager.registerObserver(this.cacheFlusher);

    // Log rolling thread
    int hlogCntPerServer = this.conf.getInt(HConstants.HLOG_CNT_PER_SERVER, 2);
    if (conf.getBoolean(HConstants.HLOG_FORMAT_BACKWARD_COMPATIBILITY, true)) {
      hlogCntPerServer = 1;
      LOG.warn("Override HLOG_CNT_PER_SERVER as 1 due to HLOG_FORMAT_BACKWARD_COMPATIBILITY");
    }
    this.hlogRollers = new LogRoller[hlogCntPerServer];
    for (int i = 0; i < hlogCntPerServer; i++) {
      this.hlogRollers[i] = new LogRoller(this, i);
    }

    // Background thread to check for major compactions; needed if region
    // has not gotten updates in a while.  Make it run at a lesser frequency.
    int multiplier = this.conf.getInt(HConstants.THREAD_WAKE_FREQUENCY +
        ".multiplier", 1000);
    this.majorCompactionChecker = new MajorCompactionChecker(this,
      this.threadWakeFrequency * multiplier);

    this.leases = new Leases(
        (int) conf.getLong(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,
            HConstants.DEFAULT_HBASE_REGIONSERVER_LEASE_PERIOD),
        conf.getLong(HConstants.REGIONSERVER_LEASE_THREAD_WAKE_FREQUENCY,
            HConstants.DEFAULT_REGIONSERVER_LEASE_THREAD_WAKE_FREQUENCY));
  }

  /**
   * We register ourselves as a watcher on the master address ZNode. This is
   * called by ZooKeeper when we get an event on that ZNode. When this method
   * is called it means either our master has died, or a new one has come up.
   * Either way we need to update our knowledge of the master.
   * @param event WatchedEvent from ZooKeeper.
   */
  @Override
  public void process(WatchedEvent event) {
    EventType type = event.getType();

    // Ignore events if we're shutting down.
    if (this.stopRequestedAtStageOne.get()) {
      LOG.debug("Ignoring ZooKeeper event while shutting down");
      return;
    }

    if (!event.getPath().equals(zooKeeperWrapper.masterElectionZNode)) {
      return;
    }

    try {
      if (type == EventType.NodeDeleted) {
        handleMasterNodeDeleted();
      } else if (type == EventType.NodeCreated) {
        handleMasterNodeCreated();
      }
    } catch(KeeperException ke) {
      LOG.error("KeeperException handling master failover", ke);
      abort("ZooKeeper exception handling master failover");
    }
  }

  private void handleMasterNodeDeleted() throws KeeperException {
    if(zooKeeperWrapper.watchMasterAddress(zooKeeperWrapper)) {
      handleMasterNodeCreated();
    }
  }

  private void handleMasterNodeCreated() throws KeeperException {
    if(!zooKeeperWrapper.watchMasterAddress(zooKeeperWrapper)) {
      handleMasterNodeDeleted();
    } else {
      getMaster();
    }
  }

  /** @return ZooKeeperWrapper used by RegionServer. */
  public ZooKeeperWrapper getZooKeeperWrapper() {
    return zooKeeperWrapper;
  }

  /**
   * TODO: adela task is created to enable this feature on swift already
   * t2931033
   *
   * @return false
   */
  public static boolean isCurrentConnectionClosed() {
    return false;
  }

  private final static String NEWLINE = System.getProperty("line.separator");
  public static String printFailedRegionserverReport(HServerInfo server,
    HMsg[] inMsgs, HRegionInfo[] loadedRegions, HMsg[] outMsgs, Throwable t) {
    StringBuilder sb = new StringBuilder();
    sb.append("Could not send the response to the regionserver. " +
        "Printing diagnostic messages");
    sb.append(NEWLINE);
    sb.append("ServerAddress : " + server.getHostnamePort());
    sb.append(NEWLINE);
    sb.append("Incomming Messages :");
    sb.append(NEWLINE);
    if (inMsgs != null) {
      for (HMsg msg : inMsgs) {
        sb.append("HMsg : " + msg.toString());
        sb.append(NEWLINE);
      }
    }
    if (loadedRegions != null) {
      sb.append("Loaded Regions : ");
      for (HRegionInfo info : loadedRegions) {
        sb.append(info.getRegionNameAsString());
        sb.append(NEWLINE);
      }
    }
    sb.append("Outgoing Messages :");
    sb.append(NEWLINE);
    if (outMsgs != null) {
      for (HMsg msg : outMsgs) {
        sb.append("HMsg : " + msg.toString());
        sb.append(NEWLINE);
      }
    }
    sb.append("Related Exception : " + t);
    String s = sb.toString();
    return s;
  }

  /**
   * The HRegionServer sticks in this loop until closed. It repeatedly checks
   * in with the HMaster, sending heartbeats & reports, and receiving HRegion
   * load/unload instructions.
   */
  @Override
  public void run() {
    regionServerThread = Thread.currentThread();
    Threads.renameThread(regionServerThread, getRSThreadName());
    boolean quiesceRequested = false;
    try {
      MapWritable w = null;
      while (!stopRequestedAtStageOne.get()) {
        w = reportForDuty();
        if (w != null) {
          init(w);
          break;
        }
        sleeper.sleep();
        LOG.warn("No response from master on reportForDuty. Sleeping and " +
          "then trying again.");
      }
      List<HMsg> outboundMessages = new ArrayList<HMsg>();
      long lastMsg = 0;
      // Now ask master what it wants us to do and tell it what we have done
      for (int tries = 0; !stopRequestedAtStageOne.get() && isHealthy();) {
        // Try to get the root region location from the master.
        if (!haveRootRegion.get()) {
          HServerAddress rootServer = zooKeeperWrapper.readRootRegionLocation();
          if (rootServer != null) {
            // By setting the root region location, we bypass the wait imposed
            // on HTable for all regions being assigned.
            if (HTableDescriptor.isMetaregionSeqidRecordEnabled(conf)) {
              this.connection.setRootRegionLocation(
                  new HRegionLocation(HRegionInfo.ROOT_REGIONINFO_WITH_HISTORIAN_COLUMN,
                      rootServer));
            } else {
              this.connection.setRootRegionLocation(
                  new HRegionLocation(HRegionInfo.ROOT_REGIONINFO, rootServer));
            }
            haveRootRegion.set(true);
          }
        }
        long now = System.currentTimeMillis();
        // Drop into the send loop if msgInterval has elapsed or if something
        // to send.  If we fail talking to the master, then we'll sleep below
        // on poll of the outboundMsgs blocking queue.
        if ((now - lastMsg) >= msgInterval || !outboundMessages.isEmpty()) {
          try {
            MemoryUsage memory =
              ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            doMetrics();
            HServerLoad hsl = new HServerLoad(
              this.numRowRequests,
              (int)(memory.getUsed()/1024/1024),
              (int)(memory.getMax()/1024/1024));
            for (HRegion r: onlineRegions.values()) {
              hsl.addRegionInfo(createRegionLoad(r));
            }
            // XXX add a field in serverInfo to report to fsOK to master?
            this.serverInfo.setLoad(hsl);
            addOutboundMsgs(outboundMessages);
            HMsg msgs[] = null;
            LOG.debug("Attempting regionserver report with the master");
            try {
              msgs = this.hbaseMaster.regionServerReport(
                serverInfo, outboundMessages.toArray(EMPTY_HMSG_ARRAY),
                getMostLoadedRegions());
            } catch (IOException e) {
              LOG.debug("RegionServerReport failed.");
              LOG.debug(HRegionServer.printFailedRegionserverReport(this.serverInfo,
                  outboundMessages.toArray(EMPTY_HMSG_ARRAY),
                  getMostLoadedRegions(), msgs, (Throwable)e));
              throw e;
            }
            LOG.debug("Attempted regionserver report with the master");
            lastMsg = System.currentTimeMillis();
            updateOutboundMsgs(outboundMessages);
            outboundMessages.clear();
            if (this.quiesced.get() && onlineRegions.isEmpty()) {
              // We've just told the master we're exiting because we aren't
              // serving any regions. So set the stop bit and exit.
              LOG.info("Server quiesced and not serving any regions. " +
                "Starting shutdown");
              stopRequestedAtStageOne.set(true);
              this.outboundMsgs.clear();
              continue;
            }

            if (InjectionHandler.falseCondition(
                InjectionEvent.HREGIONSERVER_REPORT_RESPONSE, (Object[])msgs)) {
              continue;
            }

            // Queue up the HMaster's instruction stream for processing
            boolean restart = false;
            HRegionInfo regionInfo;
            Integer mapKey;
            for(int i = 0;
                !restart && !stopRequestedAtStageOne.get() && i < msgs.length;
                i++) {
              LOG.info(msgs[i].toString() + " " + this.address.getHostNameWithPort());
              this.connection.unsetRootRegionLocation();
              switch(msgs[i].getType()) {

              case MSG_REGIONSERVER_STOP:
                stopRequestedAtStageOne.set(true);
                break;

              case MSG_REGIONSERVER_QUIESCE:
                if (!quiesceRequested) {
                  try {
                    toDo.put(new ToDoEntry(msgs[i]));
                  } catch (InterruptedException e) {
                    throw new RuntimeException("Putting into msgQueue was " +
                        "interrupted.", e);
                  }
                  quiesceRequested = true;
                }
                break;

              case MSG_REGION_CLOSE:
                regionInfo = msgs[i].getRegionInfo();
                mapKey = Bytes.mapKey(regionInfo.getRegionName());

                this.lock.writeLock().lock();
                try {
                  if (!this.onlineRegions.containsKey(mapKey) || this.regionsClosing.containsKey(mapKey)) {
                    LOG.warn("Region " + regionInfo + " already being processed as Closed/closing. Ignoring " + msgs[i]);
                    break; // already closed the region, or it is closing. Ignore this request.
                  }
                  this.regionsClosing.put(mapKey, regionInfo);
                } finally {
                  this.lock.writeLock().unlock();
                }
                addProcessingCloseMessage(regionInfo);
                try {
                  toDo.put(new ToDoEntry(msgs[i]));
                } catch (InterruptedException e) {
                  throw new RuntimeException("Putting into msgQueue was " +
                      "interrupted.", e);
                }
                break;

              case MSG_REGION_OPEN:
                regionInfo = msgs[i].getRegionInfo();
                mapKey = Bytes.mapKey(regionInfo.getRegionName());

                this.lock.writeLock().lock();
                try {
                  if (this.onlineRegions.containsKey(mapKey) || this.regionsOpening.containsKey(mapKey)) {
                    LOG.warn("Region " + regionInfo + " already being processed as opened/opening. Ignoring " + msgs[i]);
                    break; // already opened the region, or it is opening. Ignore this request.
                  }
                  this.regionsOpening.put(mapKey, regionInfo);
                } finally {
                  this.lock.writeLock().unlock();
                }
                addProcessingMessage(regionInfo);
                try {
                  toDo.put(new ToDoEntry(msgs[i]));
                } catch (InterruptedException e) {
                  throw new RuntimeException("Putting into msgQueue was " +
                      "interrupted.", e);
                }
                break;

              default:
                try {
                  toDo.put(new ToDoEntry(msgs[i]));
                } catch (InterruptedException e) {
                  throw new RuntimeException("Putting into msgQueue was " +
                      "interrupted.", e);
                }
              }
            }
            // Reset tries count if we had a successful transaction.
            tries = 0;

            if (restart || this.stopRequestedAtStageOne.get()) {
              toDo.clear();
              continue;
            }
          } catch (Exception e) { // FindBugs REC_CATCH_EXCEPTION
            // Two special exceptions could be printed out here,
            // PleaseHoldException and YouAreDeadException
            if (e instanceof IOException) {
              e = RemoteExceptionHandler.checkIOException((IOException) e);
            }
            if (e instanceof YouAreDeadException) {
              // This will be caught and handled as a fatal error below
              throw e;
            }
            tries++;
            if (tries > 0 && (tries % this.numRetries) == 0) {
              // Check filesystem every so often.
              checkFileSystem();
            }
            if (this.stopRequestedAtStageOne.get()) {
              LOG.info("Stop requested, clearing toDo despite exception");
              toDo.clear();
              continue;
            }
            LOG.warn("Attempt=" + tries, e);
            // No point retrying immediately; this is probably connection to
            // master issue.  Doing below will cause us to sleep.
            lastMsg = System.currentTimeMillis();
          }
        }
        now = System.currentTimeMillis();
        HMsg msg = this.outboundMsgs.poll((msgInterval - (now - lastMsg)),
          TimeUnit.MILLISECONDS);
        // If we got something, add it to list of things to send.
        if (msg != null) outboundMessages.add(msg);
        // Do some housekeeping before going back around
        housekeeping();
      } // for
    } catch (Throwable t) {
      if (!checkOOME(t)) {
        abort("Unhandled exception", t);
      }
      LOG.warn("Stopping - unexpected ...", t);
    }

    if(!killed) {
      // tell the master that we are going to shut down
      // do it on separate thread because we don't want to block here if
      // master is inaccessible. It is OK if this thread's message arrives
      // out of order at the master.
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            HMsg[] exitMsg = new HMsg[1];
            exitMsg[0] = REPORT_BEGINNING_OF_THE_END;
            LOG.info("prepping master for region server shutdown : " +
                serverInfo.getServerName());
            hbaseMaster.regionServerReport(serverInfo, exitMsg, (HRegionInfo[])null);
          } catch (Throwable e) {
            LOG.warn("Failed to send exiting message to master: ",
                RemoteExceptionHandler.checkThrowable(e));
          }
        }
      };
      t.setName("reporting-start-of-exit-to-master");
      t.setDaemon(true);
      t.start();
    }

    if (killed) {
      // Just skip out w/o closing regions.
      this.killAllHLogs();
    } else if (abortRequested) {
      if (this.fsOk) {
        // Only try to clean up if the file system is available
        try {
          if (this.hlogs != null) {
            this.closeAllHLogs();
            LOG.info("On abort, closed hlog");
          }
        } catch (Throwable e) {
          LOG.error("Unable to close log in abort",
            RemoteExceptionHandler.checkThrowable(e));
        }
        closeAllRegions(); // Don't leave any open file handles
      }
      LOG.info("aborting server at: " + this.serverInfo.getServerName());
    } else {
      int numRegionsToClose  = this.onlineRegions.values().size();
      ArrayList<HRegion> regionsClosed = closeAllRegions();
      if (numRegionsToClose == regionsClosed.size()) {
        try {
          if (this.hlogs != null) {
            this.closeAndDeleteAllHLogs();
          }
        } catch (Throwable e) {
          LOG.error("Close and delete failed",
            RemoteExceptionHandler.checkThrowable(e));
        }
        try {
          HMsg[] exitMsg = new HMsg[regionsClosed.size() + 1];
          if (restartRequested) {
            exitMsg[0] = REPORT_RESTARTING;
          } else {
            exitMsg[0] = REPORT_EXITING;
          }
          // Tell the master what regions we are/were serving
          int i = 1;
          for (HRegion region: regionsClosed) {
            exitMsg[i++] = new HMsg(HMsg.Type.MSG_REPORT_CLOSE,
                region.getRegionInfo());
          }

          LOG.info("telling master that region server is shutting down at: " +
              serverInfo.getServerName());
          hbaseMaster.regionServerReport(serverInfo, exitMsg, (HRegionInfo[])null);
        } catch (Throwable e) {
          LOG.warn("Failed to send exiting message to master: ",
            RemoteExceptionHandler.checkThrowable(e));
        }
      }
      else {
        // if we don't inform the master, then the master is going to detect the expired
        // znode and cause log splitting. We need this for the region that we failed to
        // close (in case there were unflushed edits).
          LOG.info("Failed to close all regions"
              + " -- skip informing master that we are shutting down ");
      }
      LOG.info("stopping server at: " + this.serverInfo.getServerName());
    }

    // Let us close the server threads after all the regions are closed. This way, we
    // will have lesser errors (since the server is responsive during the pre-flush).
    shutdownServers();

    // Make sure the proxy is down.
    if (this.hbaseMaster != null) {
      HBaseRPC.stopProxy(this.hbaseMaster);
      this.hbaseMaster = null;
    }

    this.zooKeeperWrapper.close();
    if (!killed) {
      join();
      if ((this.fs != null) && (stopRequestedAtStageOne.get() || abortRequested)) {
        // Finally attempt to close the Filesystem, to flush out any open streams.
        try {
          this.fs.close();
        } catch (IOException ie) {
          LOG.error("Could not close FileSystem", ie);
        }
      }
    }
    LOG.info(Thread.currentThread().getName() + " exiting");
  }

  private void shutdownServers() {
    // We should start ignoring client requests now.
    // So, shutdown the MemstoreFlusher and LogRoller
    this.stopRequestedAtStageTwo.set(true);

    // Stop listening to configuration update event
    configurationManager.deregisterObserver(this);

    // shutdown thriftserver
    if (thriftServer != null) {
      thriftServer.shutdown();
    }

    this.leases.closeAfterLeasesExpire();
    isRpcServerRunning = false;
    this.worker.stop();
    // Don't let the worker thread delay us for 10 secs.
    this.workerThread.interrupt();

    if (this.server != null) {
      this.server.stop();
    }
    if (this.niftyThriftServer != null) {
      this.niftyThriftServer.stop();
    }
    if (this.splitLogWorkers != null) {
      for(SplitLogWorker splitLogWorker: splitLogWorkers) {
        splitLogWorker.stop();
      }
    }
    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    // interrupt the lease handler thread;
    this.leases.interrupt();

    // Send cache a shutdown.
    if (cacheConfig.isBlockCacheEnabled()) {
      cacheConfig.getBlockCache().shutdown();
    }

    if (cacheConfig.isL2CacheEnabled()) {
      cacheConfig.getL2Cache().shutdown();
    }

    // Send interrupts to wake up threads if sleeping so they notice shutdown.
    // TODO: Should we check they are alive?  If OOME could have exited already
    cacheFlusher.interruptIfNecessary();
    compactSplitThread.interruptIfNecessary();
    for (int i = 0; i < hlogRollers.length; i++) {
      hlogRollers[i].interruptIfNecessary();
    }
    this.majorCompactionChecker.interrupt();

    // shutdown the prefetch threads
    scanPrefetchThreadPool.shutdownNow();
  }

  /**
   * Add to the passed <code>msgs</code> messages to pass to the master.
   *
   * @param msgs Current outboundMsgs array; we'll add messages to this List.
   */
  private void addOutboundMsgs(List<HMsg> msgs) {
    if (msgs.isEmpty()) {
      this.outboundMsgs.drainTo(msgs);
      return;
    }
    Set<HMsg> msgsSet = new HashSet<HMsg>(msgs);
    for (HMsg m: this.outboundMsgs) {
      if (!msgsSet.contains(m)) {
        msgs.add(m);
        msgsSet.add(m);
      }
    }
  }

  /**
   * Remove from this.outboundMsgs those messsages we sent the master.
   *
   * @param msgs Messages we sent the master.
   */
  private void updateOutboundMsgs(final List<HMsg> msgs) {
    if (msgs.isEmpty()) return;
    Set<HMsg> msgsSet = new HashSet<HMsg>(msgs);
    for (Iterator<HMsg> iterator = this.outboundMsgs.iterator(); iterator.hasNext();) {
      HMsg m = (HMsg) iterator.next();
      if (msgsSet.contains(m)){
        iterator.remove();
      }
    }
  }

  /*
   * Run init. Sets up hlog and starts up all server threads.
   * @param c Extra configuration.
   */
  protected void init(final MapWritable c) throws IOException {
    try {
      for (Map.Entry<Writable, Writable> e: c.entrySet()) {
        String key = e.getKey().toString();
        String value = e.getValue().toString();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Config from master: " + key + "=" + value);
        }
        this.conf.set(key, value);
      }
      // Master may have sent us a new address with the other configs.
      // Update our address in this case. See HBASE-719
      String hra = conf.get("hbase.regionserver.address");
      // TODO: The below used to be this.address != null.  Was broken by what
      // looks like a mistake in:
      //
      // HBASE-1215 migration; metautils scan of meta region was broken; wouldn't see first row
      // ------------------------------------------------------------------------
      // r796326 | stack | 2009-07-21 07:40:34 -0700 (Tue, 21 Jul 2009) | 38 lines
      if (hra != null) {
        HServerAddress hsa = new HServerAddress (hra,
          this.serverInfo.getServerAddress().getPort());
        LOG.info("Master passed us address to use. Was=" +
          this.serverInfo.getServerAddress() + ", Now=" + hra);
        this.serverInfo.setServerAddress(hsa);
      }

      // hack! Maps DFSClient => RegionServer for logs.  HDFS made this
      // config param for task trackers, but we can piggyback off of it.
      if (this.conf.get("mapred.task.id") == null) {
        this.conf.set("mapred.task.id",
            "hb_rs_" + this.serverInfo.getServerName());
      }

      // Master sent us hbase.rootdir to use. Should be fully qualified
      // path with file system specification included.  Set 'fs.defaultFS'
      // to match the filesystem on hbase.rootdir else underlying hadoop hdfs
      // accessors will be going against wrong filesystem (unless all is set
      // to defaults).
      this.conf.set("fs.defaultFS", this.conf.get("hbase.rootdir"));

      // Check the log directory:
      this.fs = FileSystem.get(this.conf);
      this.rootDir = new Path(this.conf.get(HConstants.HBASE_DIR));
      Path logdir = new Path(rootDir, HLog.getHLogDirectoryName(this.serverInfo));
      if (LOG.isDebugEnabled()) {
        LOG.debug("HLog dir " + logdir);
      }
      if (!fs.exists(logdir)) {
        fs.mkdirs(logdir);
      } else {
        throw new RegionServerRunningException("region server already " +
            "running at " + this.serverInfo.getServerName() +
            " because logdir " + logdir.toString() + " exists");
      }
      // Check the old log directory
      final Path oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
      if (!fs.exists(oldLogDir)) {
        fs.mkdirs(oldLogDir);
      }
      // Initialize the HLogs
      setupHLog(logdir, oldLogDir, this.hlogRollers.length);

      // Set num of HDFS threads after this.fs is initialized.
      int parallelHDFSReadPoolSize = conf.getInt(
              HConstants.HDFS_QUORUM_READ_THREADS_MAX,
              HConstants.DEFAULT_HDFS_QUORUM_READ_THREADS_MAX);
      LOG.debug("parallelHDFSReadPoolSize is (for quorum)" + parallelHDFSReadPoolSize);
      this.setNumHDFSQuorumReadThreads(parallelHDFSReadPoolSize);

      // Init in here rather than in constructor after thread name has been set
      this.metrics = new RegionServerMetrics(this.conf);
      this.dynamicMetrics = RegionServerDynamicMetrics.newInstance(this);
      startServiceThreads();
      isOnline = true;

      // Create the thread for the ThriftServer.
      // NOTE this defaults to FALSE so you have to enable it in conf
      if (conf.getBoolean("hbase.regionserver.export.thrift", false)) {
        thriftServer = new HRegionThriftServer(this, conf);
        thriftServer.start();
        LOG.info("Started Thrift API from Region Server.");
      }

      this.endpointServer.reload(conf);

      // Register configuration update event for handling it on the fly
      configurationManager.registerObserver(this);
    } catch (Throwable e) {
      this.isOnline = false;
      this.stopRequestedAtStageOne.set(true);
      throw convertThrowableToIOE(cleanup(e, "Failed init"),
        "Region server startup failed");
    }
  }

  @Override
  public AtomicLong getGlobalMemstoreSize() {
    return globalMemstoreSize;
  }

  /*
   * @param r Region to get RegionLoad for.
   * @return RegionLoad instance.
   * @throws IOException
   */
  private HServerLoad.RegionLoad createRegionLoad(final HRegion r) {
    byte[] name = r.getRegionName();
    int stores = 0;
    int storefiles = 0;
    int storefileSizeMB = 0;
    int memstoreSizeMB = (int)(r.memstoreSize.get()/1024/1024);
    int storefileIndexSizeMB = 0;
    int rootIndexSizeKB = 0;
    int totalStaticIndexSizeKB = 0;
    int totalStaticBloomSizeKB = 0;
    synchronized (r.stores) {
      stores += r.stores.size();
      for (Store store: r.stores.values()) {
        storefiles += store.getStorefilesCount();

        storefileSizeMB +=
          (int)(store.getStorefilesSize()/1024/1024);

        storefileIndexSizeMB +=
          (int)(store.getStorefilesIndexSize()/1024/1024);

        rootIndexSizeKB +=
            (int) (store.getStorefilesIndexSize() / 1024);

        totalStaticIndexSizeKB +=
          (int) (store.getTotalStaticIndexSize() / 1024);

        totalStaticBloomSizeKB +=
          (int) (store.getTotalStaticBloomSize() / 1024);
      }
    }
    return new HServerLoad.RegionLoad(name, stores, storefiles,
      storefileSizeMB, memstoreSizeMB, storefileIndexSizeMB, rootIndexSizeKB,
      totalStaticIndexSizeKB, totalStaticBloomSizeKB);
  }

  /**
   * @param regionName
   * @return An instance of RegionLoad.
   * @throws IOException
   */
  public HServerLoad.RegionLoad createRegionLoad(final byte [] regionName) {
    return createRegionLoad(this.onlineRegions.get(Bytes.mapKey(regionName)));
  }

  /**
   * Cleanup after Throwable caught invoking method.  Converts <code>t</code>
   * to IOE if it isn't already.
   * @param t Throwable
   * @return Throwable converted to an IOE; methods can only let out IOEs.
   */
  private Throwable cleanup(final Throwable t) {
    return cleanup(t, null);
  }

  /**
   * Cleanup after Throwable caught invoking method.  Converts <code>t</code>
   * to IOE if it isn't already.
   * @param t Throwable
   * @param msg Message to log in error.  Can be null.
   * @return Throwable converted to an IOE; methods can only let out IOEs.
   */
  private Throwable cleanup(final Throwable t, final String msg) {
    if (t instanceof NotServingRegionException) {
      // In case of NotServingRegionException we should not make any sanity
      // checks for the FileSystem or OOM.
      LOG.info(t.toString());
      return t;
    } else {
      if (msg == null) {
        LOG.error("", RemoteExceptionHandler.checkThrowable(t));
      } else {
        LOG.error(msg, RemoteExceptionHandler.checkThrowable(t));
      }
    }
    if (!checkOOME(t)) {
      checkFileSystem();
    }
    return t;
  }

  /**
   * @param t
   * @return Make <code>t</code> an IOE if it isn't already.
   */
  private IOException convertThrowableToIOE(final Throwable t) {
    return convertThrowableToIOE(t, null);
  }

  /**
   * @param t
   * @param msg Message to put in new IOE if passed <code>t</code> is not an IOE
   * @return Make <code>t</code> an IOE if it isn't already.
   */
  private IOException convertThrowableToIOE(final Throwable t,
      final String msg) {
    return (t instanceof IOException? (IOException)t:
      msg == null || msg.length() == 0?
        new IOException(t): new IOException(msg, t));
  }

  /**
   * Check if an OOME and if so, call abort immediately and avoid creating more
   * objects.
   * @param e
   * @return True if we OOME'd and are aborting.
   */
  @Override
  public boolean checkOOME(final Throwable e) {
    boolean stop = false;
    try {
      if (e instanceof OutOfMemoryError ||
        (e.getCause() != null && e.getCause() instanceof OutOfMemoryError) ||
        (e.getMessage() != null &&
          e.getMessage().contains("java.lang.OutOfMemoryError"))) {
        stop = true;
        // not sure whether log4j will create more string obj here or not
        LOG.fatal("Run out of memory;HRegionServer aborts itself immediately",
            e);
      }
    } finally {
      if (stop) {
        this.forceAbort();
      }
    }
    return stop;
  }

  @Override
  public void checkFileSystem() {
    long curtime = EnvironmentEdgeManager.currentTimeMillis();
    synchronized (lastCheckFSAt){
      if ((curtime - this.lastCheckFSAt.get()) <= this.minCheckFSIntervalMillis) {
        return;
      }
      this.lastCheckFSAt.set(curtime);
    }
    if (this.fs != null) {
      try {
        FSUtils.checkFileSystemAvailable(this.fs, false);
        this.fsOk = true;
        return;
      } catch (IOException e) {
        if (this.fsOk) {
          this.checkFSFailingSince = curtime;
        }
        this.fsOk = false;
        // call abort immediately if checkFSAbortTimeOutMillis is 0
        long timeToAbort = checkFSFailingSince + checkFSAbortTimeOutMillis -
            curtime;
        if (timeToAbort <= 0) {
          abort("File System not available", e);
          return;
        }
        LOG.warn("File System not available, will abort after " +
            timeToAbort + "ms", e);
        return;
      }
    }
    this.fsOk = true;
    return;
  }

  /**
   * Inner class that stores information for recently closed regions.
   * including region name, closeDate, startKey, endKey.
   */
  public static class ClosedRegionInfo {
    private final String regionName;
    private final long closeDate;
    private final String startKey;
    private final String endKey;

    ClosedRegionInfo(String name, long date, byte[] startKey, byte[] endKey) {
      this.regionName = name;
      this.closeDate = date;
      this.startKey = (startKey == null) ? "" : Bytes.toStringBinary(startKey);
      this.endKey = (endKey == null) ? "" : Bytes.toStringBinary(endKey);
    }

    public String getRegionName() {
      return regionName;
    }

    public long getCloseDate() {
      return closeDate;
    }

    /**
     * the date is represented as a string in format "yyyy-MM-dd HH:mm:ss"
     */
    public String getCloseDateAsString() {
      Date date = new Date(closeDate);
      return formatter.format(date);
    }

    public String getStartKey() {
      return startKey;
    }

    public String getEndKey() {
      return endKey;
    }
  }

  /**
   * Inner class that runs on a long period checking if regions need major
   * compaction.
   */
  private static class MajorCompactionChecker extends Chore {
    private final HRegionServer instance;

    MajorCompactionChecker(final HRegionServer h,
        final int sleepTime) {
      super("MajorCompactionChecker", sleepTime, h);
      this.instance = h;
      LOG.info("Runs every " + sleepTime + "ms");
    }

    @Override
    protected void chore() {
      Set<Integer> keys = this.instance.onlineRegions.keySet();
      for (Integer i: keys) {
        HRegion r = this.instance.onlineRegions.get(i);
        if (r == null)
          continue;
        for (Store s : r.getStores().values()) {
          try {
            if (s.isMajorCompaction()) {
              // Queue a compaction. Will recognize if major is needed.
              this.instance.compactSplitThread.requestCompaction(r, s,
                  getName() + " requests major compaction");
            }
          } catch (IOException e) {
            LOG.warn("Failed major compaction check on " + r, e);
          }
        }
      }
    }
  }

  /**
   * Report the status of the server. A server is online once all the startup
   * is completed (setting up filesystem, starting service threads, etc.). This
   * method is designed mostly to be useful in tests.
   * @return true if online, false if not.
   */
  public boolean isOnline() {
    return isOnline;
  }

  private void setupHLog(Path logDir, Path oldLogDir, int totalHLogCnt) throws IOException {
    hlogs = new HLog[totalHLogCnt];
    for (int i = 0; i < totalHLogCnt; i++) {
      hlogs[i] = new HLog(this.fs, logDir, oldLogDir, this.conf, this.hlogRollers[i],
          null, (this.serverInfo.getServerAddress().toString()), i, totalHLogCnt);
    }
    LOG.info("Initialized " + totalHLogCnt + " HLogs");
  }

  private void killAllHLogs() {
    for (int i = 0; i < this.hlogs.length; i++) {
      hlogs[i].kill();
    }
  }

  private void closeAllHLogs() throws IOException {
    for (int i = 0; i < this.hlogs.length; i++) {
      hlogs[i].close();
    }
  }

  private void closeAndDeleteAllHLogs() throws IOException {
    for (int i = 0; i < this.hlogs.length; i++) {
      hlogs[i].closeAndDelete();
    }
  }

  protected void doMetrics() {
    // for testing purposes we don't want to collect the metrics if runMetrics is false
    if (runMetrics) {
      try {
        metrics();
      } catch (Throwable e) {
        LOG.warn("Failed metrics", e);
      }
    }
  }

  protected void metrics() {
    this.metrics.regions.set(this.onlineRegions.size());
    this.metrics.numOptimizedSeeks.set(numOptimizedSeeks.intValue());
    // Is this too expensive every three seconds getting a lock on onlineRegions
    // and then per store carried?  Can I make metrics be sloppier and avoid
    // the synchronizations?
    int stores = 0;
    int storefiles = 0;
    long memstoreSize = 0;
    long storefileIndexSize = 0;
    long totalStaticIndexSize = 0;
    long totalStaticBloomSize = 0;
    int rowReadCnt = 0;
    int rowUpdateCnt = 0;

    // Note that this is a map of Doubles instead of Longs. This is because we
    // do effective integer division, which would perhaps truncate more than it
    // should because we do it only on one part of our sum at a time. Rather
    // than dividing at the end, where it is difficult to know the proper
    // factor, everything is exact then truncated.
    final Map<String, MutableDouble> tempVals =
        new HashMap<String, MutableDouble>();

    synchronized (this.onlineRegions) {
      for (Map.Entry<Integer, HRegion> e: this.onlineRegions.entrySet()) {
        HRegion r = e.getValue();
        memstoreSize += r.memstoreSize.get();
        rowReadCnt += r.getAndResetRowReadCnt();
        rowUpdateCnt += r.getAndResetRowUpdateCnt();
        synchronized (r.stores) {
          stores += r.stores.size();
          for(Map.Entry<byte [], Store> ee: r.stores.entrySet()) {
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
      }
    }

    for (Entry<String, MutableDouble> e : tempVals.entrySet()) {
      HRegion.setNumericMetric(e.getKey(), e.getValue().longValue());
    }

    this.metrics.rowReadCnt.inc(rowReadCnt);
    this.metrics.rowUpdatedCnt.inc(rowUpdateCnt);
    this.numRowRequests  = rowReadCnt + rowUpdateCnt;
    this.metrics.requests.inc(numRowRequests);

    this.metrics.stores.set(stores);
    this.metrics.storefiles.set(storefiles);
    this.metrics.memstoreSizeMB.set((int)(memstoreSize/(1024*1024)));
    this.metrics.storefileIndexSizeMB.set(
        (int) (storefileIndexSize / (1024 * 1024)));
    this.metrics.rootIndexSizeKB.set(
        (int) (storefileIndexSize / 1024));
    this.metrics.totalStaticIndexSizeKB.set(
        (int) (totalStaticIndexSize / 1024));
    this.metrics.totalStaticBloomSizeKB.set(
        (int) (totalStaticBloomSize / 1024));
    this.metrics.compactionQueueSize.set(compactSplitThread.
      getCompactionQueueSize());

    this.metrics.totalCompoundBloomFilterLoadFailureCnt.set(CompoundBloomFilter.getFailedLoadCnt());

    LruBlockCache lruBlockCache = (LruBlockCache)cacheConfig.getBlockCache();
    if (lruBlockCache != null) {
      this.metrics.blockCacheCount.set(lruBlockCache.size());
      this.metrics.blockCacheFree.set(lruBlockCache.getFreeSize());
      this.metrics.blockCacheSize.set(lruBlockCache.getCurrentSize());
      CacheStats cacheStats = lruBlockCache.getStats();
      this.metrics.blockCacheHitCount.set(cacheStats.getHitCount());
      this.metrics.blockCacheMissCount.set(cacheStats.getMissCount());
      this.metrics.blockCacheEvictedCount.set(lruBlockCache.getEvictedCount());
      this.metrics.blockCacheEvictedSingleCount.set(
          cacheStats.getEvictedSingleCount());
      this.metrics.blockCacheEvictedMultiCount.set(
          cacheStats.getEvictedMultiCount());
      this.metrics.blockCacheEvictedMemoryCount.set(
          cacheStats.getEvictedMemoryCount());

      double ratio = cacheStats.getIncrementalHitRatio();
      int percent = (int) (ratio * 100);
      this.metrics.blockCacheHitRatio.set(percent);
    }

    L2BucketCache l2BucketCache = (L2BucketCache)cacheConfig.getL2Cache();
    if (l2BucketCache != null) {
      this.metrics.l2CacheCount.set(l2BucketCache.size());
      this.metrics.l2CacheFree.set(l2BucketCache.getFreeSize());
      this.metrics.l2CacheSize.set(l2BucketCache.getCurrentSize());
      this.metrics.l2CacheEvictedCount.set(l2BucketCache.getEvictedCount());
      CacheStats l2CacheStats = l2BucketCache.getStats();
      this.metrics.l2CacheHitCount.set(l2CacheStats.getHitCount());
      this.metrics.l2CacheMissCount.set(l2CacheStats.getMissCount());
      double ratio = l2CacheStats.getIncrementalHitRatio();
      int percent = (int) (ratio * 100);
      this.metrics.l2CacheHitRatio.set(percent);
    }

    long bytesRead = 0;
    long bytesLocalRead = 0;
    long bytesRackLocalRead = 0;
    long bytesWritten = 0;
    long filesCreated = 0;
    long filesRead = 0;
    long cntWriteException = 0;
    long cntReadException = 0;

    for (Statistics fsStatistic : FileSystem.getAllStatistics()) {
      bytesRead += fsStatistic.getBytesRead();
      bytesLocalRead += fsStatistic.getLocalBytesRead();
      bytesRackLocalRead += fsStatistic.getLocalBytesRead();
      bytesWritten += fsStatistic.getBytesWritten();
      filesCreated += fsStatistic.getFilesCreated();
      filesRead += fsStatistic.getFilesRead();
      cntWriteException += fsStatistic.getCntWriteException();
      cntReadException += fsStatistic.getCntReadException();
    }

    this.metrics.bytesRead.set(bytesRead);
    this.metrics.bytesLocalRead.set(bytesLocalRead);
    this.metrics.bytesRackLocalRead.set(bytesRackLocalRead);
    this.metrics.bytesWritten.set(bytesWritten);
    this.metrics.filesCreated.set(filesCreated);
    this.metrics.filesRead.set(filesRead);
    this.metrics.cntWriteException.set(cntWriteException);
    this.metrics.cntReadException.set(cntReadException);

    if (this.fs instanceof DistributedFileSystem) {
      DFSClient client = ((DistributedFileSystem)fs).getClient();

      long quorumReadsDone = client.getQuorumReadMetrics().getParallelReadOps();
      this.metrics.quorumReadsDone.set(quorumReadsDone);
      long quorumReadWins = client.getQuorumReadMetrics().getParallelReadWins();
      this.metrics.quorumReadWins.set(quorumReadWins);
      long quorumReadsExecutedInCurThread =
          client.getQuorumReadMetrics().getParallelReadOpsInCurThread();
      this.metrics.quorumReadsExecutedInCurThread.set(quorumReadsExecutedInCurThread);
    }
  }

  @Override
  public RegionServerMetrics getMetrics() {
    return this.metrics;
  }

  /*
   * Start maintanence Threads, Server, Worker and lease checker threads.
   * Install an UncaughtExceptionHandler that calls abort of RegionServer if we
   * get an unhandled exception.  We cannot set the handler on all threads.
   * Server's internal Listener thread is off limits.  For Server, if an OOME,
   * it waits a while then retries.  Meantime, a flush or a compaction that
   * tries to run should trigger same critical condition and the shutdown will
   * run.  On its way out, this server will shut down Server.  Leases are sort
   * of inbetween. It has an internal thread that while it inherits from
   * Chore, it keeps its own internal stop mechanism so needs to be stopped
   * by this hosting server.  Worker logs the exception and exits.
   */
  private void startServiceThreads() throws IOException {
    HBaseRPC.startProxy();
    String n = getRSThreadName();
    UncaughtExceptionHandler handler = new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        abort("Uncaught exception in service thread " + t.getName(), e);
      }
    };

    // Initialize the hlog roller threads
    for (int i = 0; i < this.hlogRollers.length; i++) {
      Threads.setDaemonThreadRunning(this.hlogRollers[i], n + ".logRoller-" + i, handler);
    }

    Threads.setDaemonThreadRunning(this.workerThread, n + ".worker", handler);
    Threads.setDaemonThreadRunning(this.majorCompactionChecker,
        n + ".majorCompactionChecker", handler);

    // Leases is not a Thread. Internally it runs a daemon thread.  If it gets
    // an unhandled exception, it will just exit.
    this.leases.setName(n + ".leaseChecker");
    this.leases.start();
    // Put up info server.
    int port = this.conf.getInt(HConstants.REGIONSERVER_INFO_PORT,
        HConstants.DEFAULT_REGIONSERVER_INFOPORT);
    // -1 is for disabling info server
    if (port >= 0) {
      String addr = this.conf.get("hbase.regionserver.info.bindAddress", "0.0.0.0");
      // check if auto port bind enabled
      boolean auto = this.conf.getBoolean(HConstants.REGIONSERVER_INFO_PORT_AUTO,
          false);
      while (true) {
        try {
          this.infoServer = new InfoServer("regionserver", addr, port, false,
              conf);
          this.infoServer.setAttribute("regionserver", this);
          this.infoServer.start();
          break;
        } catch (BindException e) {
          if (!auto){
            // auto bind disabled throw BindException
            throw e;
          }
          // auto bind enabled, try to use another port
          LOG.info("Failed binding http info server to port: " + port);
          port++;
        }
      }
    }

    if (this.useHadoopRPC) {
      if (this.server == null) {
        // Start Server to handle client requests.
        this.server = HBaseRPC.getServer(this,
            rpcServerInfo.getServerAddress().getBindAddress(),
            rpcServerInfo.getServerAddress().getPort(),
            conf.getInt("hbase.regionserver.handler.count", 10),
            false, conf);
        this.server.setErrorHandler(this);
      }
      this.server.start();
      isRpcServerRunning = true;
    }
    int numSplitLogWorkers = conf.getInt(HConstants.HREGIONSERVER_SPLITLOG_WORKERS_NUM, 3);
    // Create the log splitting worker and start it
    this.splitLogWorkers = new ArrayList<SplitLogWorker>(numSplitLogWorkers);
    for (int i = 0; i < numSplitLogWorkers; i++) {
      SplitLogWorker splitLogWorker = new SplitLogWorker(this.zooKeeperWrapper,
          this.getConfiguration(), this.serverInfo.getServerName(),
          logCloseThreadPool, masterRef);
      this.splitLogWorkers.add(splitLogWorker);
      splitLogWorker.start();
    }
    // start the scanner prefetch threadpool
    int numHandlers = conf.getInt("hbase.regionserver.handler.count", 10);
    scanPrefetchThreadPool =
      Threads.getBlockingThreadPool(numHandlers, 60, TimeUnit.SECONDS,
          new DaemonThreadFactory("scan-prefetch-"));

    LOG.info("HRegionServer started at: " +
      this.serverInfo.getServerAddress().toString());
  }

  /*
   * Verify that server is healthy
   */
  private boolean isHealthy() {
    // Declare yourself healthy even if filesystem is not OK.
    // Logic in checkFileSystem() and elsewhere now hopes that filesystem
    // failures are transient.

    // Verify that all threads are alive
    if (!(leases.isAlive() &&
        cacheFlusher.isAlive() && isAllHLogRollerAlive() &&
        workerThread.isAlive() && this.majorCompactionChecker.isAlive())) {
      stop("One or more threads are no longer alive");
      return false;
    }
    return true;
  }

  private boolean isAllHLogRollerAlive() {
    boolean res = true;
    for (int i = 0; i < this.hlogRollers.length; i++) {
      res = res && this.hlogRollers[i].isAlive();
    }
    return res;
  }

  /*
   * Run some housekeeping tasks.
   */
  private void housekeeping() {
    // If the todo list has > 0 messages, iterate looking for open region
    // messages. Send the master a message that we're working on its
    // processing so it doesn't assign the region elsewhere.
    if (this.toDo.isEmpty()) {
      return;
    }
    // This iterator isn't safe if elements are gone and HRS.Worker could
    // remove them (it already checks for null there). Goes from oldest.
    for (ToDoEntry e: this.toDo) {
      if(e == null) {
        LOG.warn("toDo gave a null entry during iteration");
        break;
      }
      HMsg msg = e.msg;
      if (msg != null) {
        if (msg.isType(HMsg.Type.MSG_REGION_OPEN)) {
          addProcessingMessage(msg.getRegionInfo());
        }
      } else {
        LOG.warn("Message is empty: " + e);
      }
    }
  }

  @Override
  public List<String> getHLogsList(boolean rollCurrentHLog) throws IOException {
    List <String> allHLogsList = new ArrayList<String>();

    for (int i = 0; i < hlogs.length; i++) {
      if (rollCurrentHLog) {
        this.hlogs[i].rollWriter();
      }
      allHLogsList.addAll(this.hlogs[i].getHLogsList());
    }

    return allHLogsList;
  }

  /**
   * Return the i th HLog in this region server
   */
  public HLog getLog(int i) {
    return this.hlogs[i];
  }

  public int getTotalHLogCnt() {
    return this.hlogs.length;
  }

  /**
   * Sets a flag that will cause all the HRegionServer threads to shut down
   * in an orderly fashion.  Used by unit tests.
   */
  @Override
  public void stop(String why) {
    this.stopRequestedAtStageOne.set(true);
    stopReason = why;
    synchronized(this) {
      // Wakes run() if it is sleeping
      notifyAll(); // FindBugs NN_NAKED_NOTIFY
    }
    logCloseThreadPool.shutdown();
  }

  /**
   * Method to set the flag that will cause
   */
  @Override
  public void stopForRestart() {
    restartRequested = true;
    LOG.info("Going down for a restart");
    stop("stop for restart");
  }

  /**
   * Cause the server to exit without closing the regions it is serving, the
   * log it is using and without notifying the master.
   * Used unit testing and on catastrophic events such as HDFS is yanked out
   * from under hbase or we OOME.
   *
   * @param reason the reason we are aborting
   * @param cause the exception that caused the abort, or null
   */
  public void abort(String reason, Throwable cause) {
    if (cause != null) {
      LOG.fatal("Aborting region server " + this + ": " + reason, cause);
    } else {
      LOG.fatal("Aborting region server " + this + ": " + reason);
    }
    this.abortRequested = true;
    this.reservedSpace.clear();
    if (this.metrics != null) {
      LOG.info("Dump of metrics: " + this.metrics);
    }
    stop("aborted: " + reason);
  }

  /**
   * @see HRegionServer#abort(String, Throwable)
   */
  public void abort(String reason) {
    abort(reason, null);
  }

  /**
   * when the region server run out of memory, it needs to abort itseft quickly
   * and avoid creating more objects.
   */
  public void forceAbort() {
    Runtime.getRuntime().halt(1);
  }

  /*
   * Simulate a kill -9 of this server.
   * Exits w/o closing regions or cleaninup logs but it does close socket in
   * case want to bring up server on old hostname+port immediately.
   */
  public void kill() {
    this.killed = true;
    abort("Simulated kill");
  }

  /**
   * Wait on all threads to finish.
   * Presumption is that all closes and stops have already been called.
   */
  protected void join() {
    Threads.shutdown(this.majorCompactionChecker);
    Threads.shutdown(this.workerThread);
    this.cacheFlusher.join();
    for (int i = 0; i < this.hlogRollers.length; i++) {
      Threads.shutdown(this.hlogRollers[i]);
    }
    this.compactSplitThread.join();
  }

  private boolean getMaster() {
    HServerAddress prevMasterAddress = null;
    HServerAddress masterAddress = null;
    HMasterRegionInterface master = null;
    while (!stopRequestedAtStageOne.get() && master == null) {
      // Re-read master address from ZK as it might have changed.
      masterAddress = readMasterAddressFromZK();
      if (masterAddress == null) {
        continue;
      }

      if (!masterAddress.equals(prevMasterAddress)) {
        LOG.info("Telling master at " + masterAddress + " that we are up");
        prevMasterAddress = masterAddress;
      }

      try {
        // Do initial RPC setup.  The final argument indicates that the RPC
        // should retry indefinitely.
        if (this.conf.getBoolean(HConstants.CLIENT_TO_MASTER_USE_THRIFT,
            HConstants.CLIENT_TO_MASTER_USE_THRIFT_DEFAULT)) {
          InetSocketAddress addr = new InetSocketAddress(masterAddress
              .getInetSocketAddress().getHostName(), conf.getInt(
              HConstants.MASTER_THRIFT_PORT,
              HConstants.MASTER_THRIFT_PORT_DEFAULT));
          master = (HMasterRegionInterface) HBaseThriftRPC.getClient(addr,
              this.conf, HMasterRegionInterface.class, HBaseRPCOptions.DEFAULT);
        } else {
          master = (HMasterRegionInterface) HBaseRPC.getProxy(
              HMasterRegionInterface.class, HBaseRPCProtocolVersion.versionID,
              masterAddress.getInetSocketAddress(), this.conf,
              this.rpcTimeoutToMaster, HBaseRPCOptions.DEFAULT);
        }
      } catch (IOException e) {
        LOG.warn("Unable to connect to master. Retrying. Error was:", e);
        sleeper.sleep();
      }
    }
    this.hbaseMaster = master;
    masterRef.set(hbaseMaster);
    return true;
  }

  private HServerAddress readMasterAddressFromZK() {
    HServerAddress masterAddress = null;
    try {
      masterAddress = HServerInfo.getAddress(zooKeeperWrapper.readAddressOrThrow(
          zooKeeperWrapper.masterElectionZNode, zooKeeperWrapper));
    } catch (KeeperException e) {
      LOG.fatal(UNABLE_TO_READ_MASTER_ADDRESS_ERR_MSG, e);
      forceAbort();
    }
    return masterAddress;
  }

  /*
   * Let the master know we're here
   * Run initialization using parameters passed us by the master.
   */
  private MapWritable reportForDuty() throws YouAreDeadException {
    while (!stopRequestedAtStageOne.get() && !getMaster()) {
      sleeper.sleep();
      LOG.warn("Unable to get master for initialization");
    }

    MapWritable result = null;
    long lastMsg = 0;
    boolean znodeWritten = false;
    while(!stopRequestedAtStageOne.get()) {
      try {
        MemoryUsage memory =
          ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        HServerLoad hsl = new HServerLoad(0, (int)memory.getUsed()/1024/1024,
          (int)memory.getMax()/1024/1024);
        this.serverInfo.setLoad(hsl);
        if (LOG.isDebugEnabled())
          LOG.debug("sending initial server load: " + hsl);
        lastMsg = System.currentTimeMillis();
        if (znodeWritten || zooKeeperWrapper.writeRSLocation(this.serverInfo)) {
          znodeWritten = true;
          // We either created the znode, or it existed already. Check in with the master.
          result = this.hbaseMaster.regionServerStartup(this.serverInfo);
          break;
        } else {
          LOG.error("Could not write RS znode " + serverInfo.getServerName()
              + " to ZK, will try again");
        }
      } catch (IOException e) {
        LOG.warn("error telling master we are up", e);
        if (e instanceof YouAreDeadException) {
          throw (YouAreDeadException) e;
        }
      }
      sleeper.sleep(lastMsg);
    }
    return result;
  }

  /**
   * Add to the outbound message buffer
   *
   * When a region splits, we need to tell the master that there are two new
   * regions that need to be assigned.
   *
   * We do not need to inform the master about the old region, because we've
   * updated the meta or root regions, and the master will pick that up on its
   * next rescan of the root or meta tables.
   */
  void reportSplit(HRegionInfo oldRegion, HRegionInfo newRegionA,
      HRegionInfo newRegionB) {
    this.outboundMsgs.add(new HMsg(HMsg.Type.MSG_REPORT_SPLIT_INCLUDES_DAUGHTERS,
      oldRegion, newRegionA, newRegionB,
      Bytes.toBytes("Daughters; " +
          newRegionA.getRegionNameAsString() + ", " +
          newRegionB.getRegionNameAsString())));
  }

  //////////////////////////////////////////////////////////////////////////////
  // HMaster-given operations
  //////////////////////////////////////////////////////////////////////////////

  /*
   * Data structure to hold a HMsg and retries count.
   */
  private static final class ToDoEntry {
    protected final AtomicInteger tries = new AtomicInteger(0);
    protected final HMsg msg;

    ToDoEntry(final HMsg msg) {
      this.msg = msg;
    }
  }

  final BlockingQueue<ToDoEntry> toDo = new LinkedBlockingQueue<ToDoEntry>();
  private Worker worker;
  private Thread workerThread;

  /** Thread that performs long running requests from the master */
  class Worker implements Runnable {
    void stop() {
      synchronized(toDo) {
        toDo.notifyAll();
      }
    }

    @Override
    public void run() {
      try {
        while(!stopRequestedAtStageOne.get()) {
          ToDoEntry e = null;
          try {
            e = toDo.poll(threadWakeFrequency, TimeUnit.MILLISECONDS);
            if(e == null || stopRequestedAtStageOne.get()) {
              continue;
            }
            LOG.info("Worker: " + e.msg);
            HRegion region = null;
            HRegionInfo info = e.msg.getRegionInfo();
            switch(e.msg.getType()) {

            case MSG_REGIONSERVER_QUIESCE:
              closeUserRegions();
              break;

            case MSG_REGION_OPEN:
              // Open a region
              boolean requeued = false;
              if (!haveRootRegion.get() && !info.isRootRegion()) {
                // root region is not online yet. requeue this task
                LOG.info("putting region open request back into queue because" +
                    " root region is not yet available");
                try {
                  toDo.put(e);
                  requeued = true;
                } catch (InterruptedException ex) {
                  LOG.warn("insertion into toDo queue was interrupted", ex);
                  break;
                }
              }
              if (!requeued) {
                String favouredNodes = null;
                if (e.msg.getMessage() != null && e.msg.getMessage().length > 0) {
                  favouredNodes = new String(e.msg.getMessage());
                }
                regionOpenCloseThreadPool.submit(
                    createRegionOpenCallable(info, favouredNodes));
              }
              break;

            case MSG_REGION_CLOSE:
              // Close a region
              closeRegion(e.msg.getRegionInfo(), true);
              break;

            case MSG_REGION_CLOSE_WITHOUT_REPORT:
              // Close a region, don't reply

              closeRegion(e.msg.getRegionInfo(), false);
              break;

            case MSG_REGION_SPLIT:
              region = getRegion(info.getRegionName());
              region.flushcache();
              region.triggerSplit();
              region.setSplitPoint(info.getSplitPoint());
              compactSplitThread.requestSplit(region, region.checkSplit());
              break;

            case MSG_REGION_MAJOR_COMPACT:
            case MSG_REGION_COMPACT:
              // Compact a region
              region = getRegion(info.getRegionName());
              if (e.msg.isType(Type.MSG_REGION_MAJOR_COMPACT)) {
                region.triggerMajorCompaction();
              }
              compactSplitThread.requestCompaction(region,
                e.msg.getType().name(),
                CompactSplitThread.PRIORITY_USER);
              break;
            case MSG_REGION_CF_MAJOR_COMPACT:
            case MSG_REGION_CF_COMPACT:
              region = getRegion(info.getRegionName());
              byte[] columnFamily = e.msg.getMessage();
              LOG.info("Compaction request for column family : "
                  + new String(columnFamily) + " within region : " + region +" received");
              Store store = region.getStore(columnFamily);
              if (e.msg.isType(Type.MSG_REGION_CF_MAJOR_COMPACT)) {
                store.triggerMajorCompaction();
              }
              compactSplitThread.requestCompaction(region,
                store,
                e.msg.getType().name(),
                CompactSplitThread.PRIORITY_USER);
              break;

            case MSG_REGION_FLUSH:
              region = getRegion(info.getRegionName());
              region.flushcache();
              break;

            case TESTING_MSG_BLOCK_RS:
              while (!stopRequestedAtStageOne.get()) {
                Threads.sleep(1000);
                LOG.info("Regionserver blocked by " +
                  HMsg.Type.TESTING_MSG_BLOCK_RS + "; " + stopRequestedAtStageOne.get());
              }
              break;

            default:
              throw new AssertionError(
                  "Impossible state during msg processing.  Instruction: "
                  + e.msg.toString());
            }
          } catch (InterruptedException ex) {
            LOG.warn("Processing Worker queue", ex);
          } catch (Exception ex) {
            if (ex instanceof IOException) {
              ex = RemoteExceptionHandler.checkIOException((IOException) ex);
            }
            if(e != null && e.tries.get() < numRetries) {
              LOG.warn(ex);
              e.tries.incrementAndGet();
              try {
                toDo.put(e);
              } catch (InterruptedException ie) {
                throw new RuntimeException("Putting into msgQueue was " +
                    "interrupted.", ex);
              }
            } else {
              LOG.error("FAILED TO PROCESS MESSAGE FROM MASTER" +
                  (e != null ? (": " + e.msg.toString()) : ""), ex);
              checkFileSystem();
            }
          }
        }
      } catch(Throwable t) {
        if (!checkOOME(t)) {
          LOG.fatal("Unhandled exception", t);
        }
      } finally {
        LOG.info("worker thread exiting");
      }
    }
  }

  void openRegion(final HRegionInfo regionInfo, String favoredNodes) {
    Integer mapKey = Bytes.mapKey(regionInfo.getRegionName());
    HRegion region = this.onlineRegions.get(mapKey);
    RSZookeeperUpdater zkUpdater = new RSZookeeperUpdater(
        this.zooKeeperWrapper, serverInfo.getServerName(),
          regionInfo.getEncodedName());
    HRegionSeqidTransition seqidTransition = null;
    if (region == null) {
      try {
        zkUpdater.startRegionOpenEvent(null, true);

        // Assign one of the HLogs to the new opening region.
        // If the region has been opened before, assign the previous HLog instance to that region.
        Integer hLogIndex = null;
        if ((hLogIndex = regionNameToHLogIDMap.get(regionInfo.getRegionNameAsString())) == null) {
          hLogIndex = Integer.valueOf((this.currentHLogIndex++) % (this.hlogs.length));
          this.regionNameToHLogIDMap.put(regionInfo.getRegionNameAsString(), hLogIndex);
        }

        ArrayList<HRegionSeqidTransition> seqidTransitionList =
          new ArrayList<HRegionSeqidTransition>();
        region = instantiateRegion(regionInfo,
            this.hlogs[hLogIndex.intValue()], seqidTransitionList);

        if (!seqidTransitionList.isEmpty()) {
          seqidTransition = seqidTransitionList.get(0);
        }
        LOG.info("Initiate the region: " + regionInfo.getRegionNameAsString() + " with HLog #" +
            hLogIndex + ((seqidTransition == null) ?
                " and no sequence id transition recorded." :
                " and recorded " + seqidTransition));

        // Set up the favorite nodes for all the HFile for that region
        setFavoredNodes(region, favoredNodes);

        // Startup a compaction early if one is needed, if store has references
        // or has too many store files
        for (Store s : region.getStores().values()) {
          if (s.hasReferences() || s.needsCompaction()) {
            this.compactSplitThread.requestCompaction(region, s,
                "Opening Region");
          }
        }

        if (openRegionDelay > 0) {
          Thread.sleep(openRegionDelay);
          LOG.debug("Slept for " + openRegionDelay + " seconds. Going to add "
              + regionInfo.getRegionNameAsString() + " to online region list");
        }
        this.lock.writeLock().lock();
        try {
          // Abort opening region when server is shutting down.
          // Reason:
          // If we let region opening succeed, there could be a race condition that closeAllRegion()
          // does not close this region so that edits behind flushing only exist in HLog but not in
          // HFiles after the shutdown. Since master thinks it's a clean shutdown, HLog won't be
          // replayed. Eventually we loses these edits. This bug is captured by replication verification.
          if (isStopRequested()) {
            throw new IOException("Server is shutting down, aborting open region!");
          }
          this.onlineRegions.put(mapKey, region);
          region.setRegionServer(this);
          region.setOpenDate(EnvironmentEdgeManager.currentTimeMillis());
          this.regionsOpening.remove(mapKey);
        } finally {
          this.lock.writeLock().unlock();
        }
      } catch (Throwable e) {
        Throwable t = cleanup(e,
          "Error opening " + regionInfo.getRegionNameAsString());
        // TODO: add an extra field in HRegionInfo to indicate that there is
        // an error. We can't do that now because that would be an incompatible
        // change that would require a migration
        try {
          HMsg hmsg = new HMsg(HMsg.Type.MSG_REPORT_CLOSE,
                               regionInfo,
                               StringUtils.stringifyException(t).getBytes());
          zkUpdater.abortOpenRegion(hmsg);
        } catch (IOException e1) {
          // TODO: Can we recover? Should be throw RTE?
          LOG.error("Failed to abort open region " + regionInfo.getRegionNameAsString(), e1);
        }

        this.lock.writeLock().lock();
        try {
          this.regionsOpening.remove(mapKey);
        } finally {
          this.lock.writeLock().unlock();
        }

        return;
      }
    }
    try {
      HMsg hmsg = new HMsg(HMsg.Type.MSG_REPORT_OPEN, regionInfo,
          HRegionSeqidTransition.toBytes(seqidTransition));
      zkUpdater.finishRegionOpenEvent(hmsg);
    } catch (IOException e) {
      try {
        // Do not report this region close to the master, as master seems to have
        // moved on, and is not expecting this RS to host this region anymore.
        closeRegion(regionInfo, false);
      } catch (IOException e1) {
        LOG.error("Failed to close region that could not be marked open " +
          regionInfo.getRegionNameAsString(), e1);
      }
      LOG.error("Failed to mark region " + regionInfo.getRegionNameAsString() + " as opened", e);
    }
  }

  private void setFavoredNodes(HRegion region, String favoredNodes) {
    if (favoredNodes != null && favoredNodes.length() > 0) {
      InetSocketAddress[] nodes =
        RegionPlacement.getFavoredInetSocketAddress(favoredNodes);
      region.setFavoredNodes(nodes);
      LOG.debug("Set the region " + region.getRegionNameAsString() +
          " with the favored nodes: " + favoredNodes);
    }
  }

  /*
   * @param regionInfo RegionInfo for the Region we're to instantiate and
   * initialize.
   * @param hlog Set into here the regions' seqid.
   * @param seqidTransitionList a list to pass back seqidTransition.
   * @return
   * @throws IOException
   */
  protected HRegion instantiateRegion(final HRegionInfo regionInfo, final HLog hlog,
      ArrayList<HRegionSeqidTransition> seqidTransitionList)
  throws IOException {
    Path dir =
      HTableDescriptor.getTableDir(rootDir, regionInfo.getTableDesc().getName());
    HRegion r = HRegion.newHRegion(dir, hlog, this.fs, conf, regionInfo,
      this.cacheFlusher);
    long seqid = r.initialize(new Progressable() {
      @Override
      public void progress() {
        addProcessingMessage(regionInfo);
      }
    });
    // If a wal and its seqid is < that of new region, use new regions seqid.
    if (hlog != null && seqid > hlog.getSequenceNumber()) {
      hlog.setSequenceNumber(seqid);
    }
    // if it is metaRegion and not enable recording metaRegion Seqid, skip
    if (regionInfo.isMetaTable() && !HTableDescriptor.isMetaregionSeqidRecordEnabled(conf)) {
      LOG.info("Recording of sequence id of meta region not enabled!");
    } else {
      // update seqids. This array will record the last seqid
      // and the next seqid on RegionServer. It will be recorded in HMaster meta table
      // note @Jiqing: these lines are not atomic, which means edits can come in
      // between setSequenceNumber and getSequenceNumber call.
      // Example:
      // If hlog.seqid = 100, seqid = 200, after hlog.setSequenceNumber(200),
      // hlog.getSequenceNumber may return 210 if there're 10 edits inbound.
      // The transition seqid is still valid
      // to be strict, next sequence id is getSequenceNumber() + 1
      // because of incrementAndGet in Hlog.java obtainSeqNum method
      // after calling hlog.writeSeqidTransition method next line.
      HRegionSeqidTransition seqidTransition =
        new HRegionSeqidTransition(Math.max(seqid-1, 0), hlog.getSequenceNumber() + 1);
      LOG.info("Sequence id of region " + regionInfo.getRegionNameAsString() +
          " has a transition from " + seqidTransition.getLastSeqid() +
          " to " + seqidTransition.getNextSeqid() +
          ". Will be recorded in meta/root table under " +
          Bytes.toString(HConstants.CATALOG_HISTORIAN_FAMILY)  + " family.");
      // append to hlog
      hlog.writeSeqidTransition(seqidTransition, serverInfo, regionInfo);
      seqidTransitionList.add(seqidTransition);
    }

    return r;
  }

  /**
   * Add a MSG_REPORT_PROCESS_OPEN to the outbound queue.
   * This method is called while region is in the queue of regions to process
   * and then while the region is being opened, it is called from the Worker
   * thread that is running the region open.
   * @param hri Region to add the message for
   */
  public void addProcessingMessage(final HRegionInfo hri) {
    getOutboundMsgs().add(new HMsg(HMsg.Type.MSG_REPORT_PROCESS_OPEN, hri));
  }

  /**
   * Add a MSG_REPORT_PROCESS_CLOSE to the outbound queue.
   * This method is called while region is in the queue of regions to process
   * and then while the region is being closed, it is called from the Worker
   * thread that is running the region close.
   * @param hri Region to add the message for
   */
  public void addProcessingCloseMessage(final HRegionInfo hri) {
    getOutboundMsgs().add(new HMsg(HMsg.Type.MSG_REPORT_PROCESS_CLOSE, hri));
  }

  private void addToRecentlyClosedRegions(ClosedRegionInfo info) {
      recentlyClosedRegions.add(0, info);
      if (recentlyClosedRegions.size() > DEFAULT_NUM_TRACKED_CLOSED_REGION)
        recentlyClosedRegions.remove(DEFAULT_NUM_TRACKED_CLOSED_REGION);
  }

  @Override
  public void closeRegion(final HRegionInfo hri, final boolean reportWhenCompleted)
  throws IOException {
    RSZookeeperUpdater zkUpdater = null;
    if(reportWhenCompleted) {
      zkUpdater = new RSZookeeperUpdater(this.zooKeeperWrapper,
          serverInfo.getServerName(), hri.getEncodedName());
      zkUpdater.startRegionCloseEvent(null, false);
    }
    HRegion region;
    try {
      region = getRegion(hri.getRegionName());
    } catch (NotServingRegionException ex) {
      // This is fine. Region is already closed.
      return;
    }
    if (region == null) {
      region = this.removeFromRetryCloseRegions(hri);
    }
    if (region != null) {
      try {
        region.close();
        this.removeFromOnlineRegions(hri);
      } catch (IOException e) {
        // If region closing fails, add it to retry map.
        this.addToRetryCloseRegions(region);
        throw e;
      }
      serverInfo.getFlushedSequenceIdByRegion().remove(hri.getRegionName());
      ClosedRegionInfo info =
        new ClosedRegionInfo(hri.getRegionNameAsString(), EnvironmentEdgeManager.currentTimeMillis() ,
                             hri.getStartKey(), hri.getEndKey());
      addToRecentlyClosedRegions(info);
      if(reportWhenCompleted) {
        if(zkUpdater != null) {
          HMsg hmsg = new HMsg(HMsg.Type.MSG_REPORT_CLOSE, hri, null);
          zkUpdater.finishRegionCloseEvent(hmsg);
        }
      }
    }
  }

  /** Called either when the master tells us to restart or from stop()
   * @throws Throwable */
  ArrayList<HRegion> closeAllRegions() {
    ArrayList<HRegion> regionsToClose = null;
    this.lock.writeLock().lock();
    try {
      regionsToClose = new ArrayList<HRegion>(onlineRegions.values());
    } finally {
      this.lock.writeLock().unlock();
    }

    // First, close any outstanding scanners.  Means they'll get an UnknownScanner
    // exception next time they come in.
    for (Map.Entry<String, InternalScanner> e: this.scanners.entrySet()) {
      try {
        e.getValue().close();
      } catch (Exception ioe) {
        LOG.warn("Closing scanner " , ioe);
      }
    }

    return closeRegionsInParallel(regionsToClose);
  }

  private ArrayList<HRegion> closeRegionsInParallel(List<HRegion> regionsToClose) {
    // Then, we close the regions
    List<Future<Object>> futures =
        new ArrayList<Future<Object>>(regionsToClose.size());

    for (int i = 0; i < regionsToClose.size(); i++ ) {
      futures.add(regionOpenCloseThreadPool.submit(
          createRegionCloseCallable(regionsToClose.get(i))));
    }

    ArrayList<HRegion> regionsClosed = new ArrayList<HRegion>(
        regionsToClose.size());
    for (int i = 0; i < futures.size(); i++ ) {
      Future<Object> future = futures.get(i);
      try {
        future.get();
        // add to regionsClosed only if we don't see an exception.
        regionsClosed.add(regionsToClose.get(i));
      } catch (Throwable e1) {
        if (e1 instanceof ExecutionException) e1 = e1.getCause();
        LOG.error("Error closingRegion " + regionsToClose.get(i), e1);
      }
    }

    return regionsClosed;
  }

  private Callable<Object> createRegionOpenCallable(final HRegionInfo rinfo,
      final String favouredNodes) {
    return new Callable<Object>() {
      @Override
      public Object call() throws IOException {
        openRegion(rinfo, favouredNodes);
        return null;
      }
    };
  }

  private Callable<Object> createRegionCloseCallable(final HRegion region) {
    return new Callable<Object>() {
      @Override
      public Object call() throws IOException {
        if (LOG.isDebugEnabled()) {
          LOG.debug("closing region "
              + Bytes.toStringBinary(region.getRegionName()));
        }
        try {
          region.close(abortRequested);
          removeFromOnlineRegions(region.getRegionInfo());
        } catch (IOException e) {
          cleanup(e,
              "Error closing " + Bytes.toStringBinary(region.getRegionName()));
          throw e;
        }
        return null;
      }
    };
  }

  /** Called as the first stage of cluster shutdown. */
  void closeUserRegions() {
    ArrayList<HRegion> regionsToClose = new ArrayList<HRegion>();
    this.lock.writeLock().lock();
    try {
      synchronized (onlineRegions) {
        for (Iterator<Map.Entry<Integer, HRegion>> i =
            onlineRegions.entrySet().iterator(); i.hasNext();) {
          Map.Entry<Integer, HRegion> e = i.next();
          HRegion r = e.getValue();
          if (!r.getRegionInfo().isMetaRegion()) {
            regionsToClose.add(r);
          }
        }
      }
    } finally {
      this.lock.writeLock().unlock();
    }
    // Run region closes in parallel.
    closeRegionsInParallel(regionsToClose);

    this.quiesced.set(true);
    if (onlineRegions.isEmpty()) {
      outboundMsgs.add(REPORT_EXITING);
    } else {
      outboundMsgs.add(REPORT_QUIESCED);
    }
  }

  //
  // HRegionInterface
  //

  @Override
  public HRegionInfo getRegionInfo(final byte[] regionName)
      throws NotServingRegionException {
      return getRegion(regionName).getRegionInfo();
  }


  @Override
  public Result getClosestRowBefore(final byte [] regionName,
    final byte [] row, final byte [] family)
  throws IOException {
    checkOpen();
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
  @Override
  public Result get(byte[] regionName, Get get) throws IOException {
    checkOpen();
    try {
      HRegion region = getRegion(regionName);
      Result r = region.get(get, getLockFromId(get.getLockId()));
      if (r == null) {
        return Result.SENTINEL_RESULT;
      }
      return r;
    } catch(Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  @Override
  public Result[] get(byte[] regionName, List<Get> gets)
      throws IOException {
    checkOpen();
    Result[] rets = new Result[gets.size()];
    try {
      HRegion region = getRegion(regionName);
      int i = 0;
      for (Get get : gets) {
        rets[i] = region.get(get, getLockFromId(get.getLockId()));
        i++;
      }
      return rets;
    } catch(Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  @Override
  public void mutateRow(byte[] regionName, List<RowMutations> armList)
      throws IOException {
    checkOpen();
    if (regionName == null) {
      throw new IOException("Invalid arguments to atomicMutation " +
      "regionName is null");
    }
    try {
      HRegion region = getRegion(regionName);
      if (!region.getRegionInfo().isMetaTable()) {
        this.cacheFlusher.reclaimMemStoreMemory();
      }
      for (RowMutations rm: armList) {
        region.mutateRow(rm);
      }
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  @Override
  public void mutateRow(byte[] regionName, RowMutations arm)
      throws IOException {
    mutateRow(regionName, Collections.singletonList(arm));
  }

  @Override
  public boolean exists(byte [] regionName, Get get) throws IOException {
    checkOpen();
    try {
      HRegion region = getRegion(regionName);
      Result r = region.get(get, getLockFromId(get.getLockId()));
      return r != null && !r.isEmpty();
    } catch(Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  @Override
  public void put(final byte [] regionName, final Put put)
  throws IOException {
    if (put.getRow() == null)
      throw new IllegalArgumentException("update has null row");

    checkOpen();
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

  @Override
  public int put(final byte[] regionName, final List<Put> puts)
  throws IOException {
    return applyMutations(regionName, puts, "multiput_");
  }

  private int applyMutations(final byte[] regionName,
      final List<? extends Mutation> mutations,
      String methodName)
  throws IOException {
    checkOpen();
    HRegion region = null;
    try {
      region = getRegion(regionName);
      if (!region.getRegionInfo().isMetaTable()) {
        this.cacheFlusher.reclaimMemStoreMemory();
      }

      @SuppressWarnings("unchecked")
      Pair<Mutation, Integer>[] opWithLocks = new Pair[mutations.size()];

      int i = 0;
      for (Mutation p : mutations) {
        Integer lock = getLockFromId(p.getLockId());
        opWithLocks[i++] = new Pair<Mutation, Integer>(p, lock);
      }

      OperationStatusCode[] codes = region.batchMutateWithLocks(opWithLocks,
          methodName);
      for (i = 0; i < codes.length; i++) {
        if (codes[i] != OperationStatusCode.SUCCESS)
          return i;
      }
      return HConstants.MULTIPUT_SUCCESS;
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  private boolean checkAndMutate(final byte[] regionName, final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Writable w, Integer lock) throws IOException {
    checkOpen();
    HRegion region = getRegion(regionName);
    try {
      if (!region.getRegionInfo().isMetaTable()) {
        this.cacheFlusher.reclaimMemStoreMemory();
      }
      return region.checkAndMutate(row, family, qualifier, value, w, lock,
          true);
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
   * @param value the expected value
   * @param put
   * @throws IOException
   * @return true if the new put was execute, false otherwise
   */
  @Override
  public boolean checkAndPut(final byte[] regionName, final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Put put) throws IOException{
    return checkAndMutate(regionName, row, family, qualifier, value, put,
        getLockFromId(put.getLockId()));
  }

  /**
   *
   * @param regionName
   * @param row
   * @param family
   * @param qualifier
   * @param value the expected value
   * @param delete
   * @throws IOException
   * @return true if the new put was execute, false otherwise
   */
  @Override
  public boolean checkAndDelete(final byte[] regionName, final byte [] row,
      final byte [] family, final byte [] qualifier, final byte [] value,
      final Delete delete) throws IOException{
    return checkAndMutate(regionName, row, family, qualifier, value, delete,
        getLockFromId(delete.getLockId()));
  }

  @Override
  public String getConfProperty(String name){
      String ret = conf.get(name);
      if (ret == null) return "";
      return ret;
  }

  //
  // remote scanner interface
  //

  @Override
  public long openScanner(final byte [] regionName, final Scan scan)
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
    try {
      HRegion r = getRegion(regionName);
      return addScanner(r.getScanner(scan));
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t, "Failed openScanner"));
    }
  }

  protected long addScanner(InternalScanner s) throws LeaseStillHeldException {
    long scannerId = -1;
    while (true) {
      scannerId = rand.nextLong();
      if (scannerId == -1)
        continue;
      String scannerName = String.valueOf(scannerId);
      InternalScanner existing = scanners.putIfAbsent(scannerName, s);
      if (existing == null) {
        this.leases.createLease(scannerName, new ScannerListener(scannerName));
        break;
      }
    }
    return scannerId;
  }

  @ParamFormat(clazz = ScanParamsFormatter.class)
  @Override
  public Result next(final long scannerId) throws IOException {
    Result[] res = next(scannerId, 1);
    if(res == null || res.length == 0) {
      return null;
    }
    return res[0];
  }

  /**
   * Pretty param printer for next() RPC calls. (works for 1 and 2 parameter
   * methods)
   * @see ParamFormatter
   */
  public static class ScanParamsFormatter implements ParamFormatter<HRegionServer> {
    // TODO make this configurable - same as Operation class's
    private static final int DEFAULT_MAX_COLS = 5;

    @Override
    public Map<String, Object> getMap(Object[] params, HRegionServer regionServer) {
      Map<String, Object> res = new HashMap<String, Object>();
      if (params == null || params.length == 0) return null; // bad request
      long scannerId = (Long) params[0];
      String scannerName = String.valueOf(scannerId);
      InternalScanner s = regionServer.scanners.get(scannerName);
      if (s != null && s instanceof RegionScanner) {
        res.put("scan", ((RegionScanner)s).getOriginalScan().toMap(DEFAULT_MAX_COLS));
      }

      if (params.length > 1) {
        res.put("maxrows", params[1]);
      }
      return res;
    }
  }

  @ParamFormat(clazz = ScanParamsFormatter.class)
  @Override
  public Result[] next(final long scannerId, int nbRows) throws IOException {
    Result[] ret = nextInternal(scannerId, nbRows);
    if (isScanDone(ret)) {
      return null;
    }
    return ret;
  }

  /**
   * Tells whether the scan on this region is complete and that the client
   * scanner should move onto the next region
   * @param values
   * @return
   */
  public static boolean isScanDone(Result[] values) {
    if (values == null) return true;
    if (values.length == 1) {
      return values[0].isSentinelResult();
    }
    return false;
  }

  /**
   * This function results for the next request. This function is used across
   * Thrift and HadoopRPC.
   * Termination of scan is represented by {@link Result#SENTINEL_RESULT_ARRAY}
   * @return
   */
  protected Result[] nextInternal(final long scannerId, int nbRows)
      throws IOException {
    try {
      String scannerName = String.valueOf(scannerId);
      // HRegionServer only deals with Region Scanner,
      // thus, we just typecast directly
      RegionScanner s = (RegionScanner)this.scanners.get(scannerName);
      if (s == null) {
        throw new UnknownScannerException("Name: " + scannerName);
      }
      try {
        checkOpen();
      } catch (IOException e) {
        // If checkOpen failed, server not running or filesystem gone,
        // cancel this lease; filesystem is gone or we're closing or something.
        this.leases.cancelLease(scannerName);
        throw e;
      }
      this.leases.renewLease(scannerName);
      return s.nextRows(nbRows, HRegion.METRIC_NEXTSIZE);
    } catch (Throwable t) {
      if (t instanceof NotServingRegionException) {
        String scannerName = String.valueOf(scannerId);
        this.scanners.remove(scannerName);
      }
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  @Override
  public void close(final long scannerId) throws IOException {
    try {
      checkOpen();
      String scannerName = String.valueOf(scannerId);
      InternalScanner s = scanners.remove(scannerName);
      if (s != null) {
        s.close();
        this.leases.cancelLease(scannerName);
      }
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  /**
   * Instantiated as a scanner lease.
   * If the lease times out, the scanner is closed
   */
  private class ScannerListener extends LeaseListener {

    ScannerListener(final String n) {
      super(n);
    }

    @Override
    public void leaseExpired() {
      LOG.info("Scanner " + this.getLeaseName() + " lease expired");
      InternalScanner s = scanners.remove(this.getLeaseName());
      if (s != null) {
        try {
          s.close();
        } catch (IOException e) {
          LOG.error("Closing scanner", e);
        }
      }
    }
  }

  //
  // Methods that do the actual work for the remote API
  //
  @Override
  public void delete(final byte [] regionName, final Delete delete)
  throws IOException {
    checkOpen();
    try {
      boolean writeToWAL = delete.getWriteToWAL();
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

  @Override
  public int delete(final byte[] regionName,
                                  final List<Delete> deletes)
  throws IOException {
    return applyMutations(regionName, deletes, "multidelete_");
  }

  @Override
  public long lockRow(byte [] regionName, byte [] row)
  throws IOException {
    checkOpen();
    NullPointerException npe = null;
    if(regionName == null) {
      npe = new NullPointerException("regionName is null");
    } else if(row == null) {
      npe = new NullPointerException("row to lock is null");
    }
    if(npe != null) {
      IOException io = new IOException("Invalid arguments to lockRow");
      io.initCause(npe);
      throw io;
    }
    try {
      HRegion region = getRegion(regionName);
      Integer r = region.obtainRowLock(row);
      long lockId = addRowLock(r,region);
      LOG.debug("Row lock " + lockId + " explicitly acquired by client");
      return lockId;
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t,
        "Error obtaining row lock (fsOk: " + this.fsOk + ")"));
    }
  }

  protected long addRowLock(Integer r, HRegion region) throws LeaseStillHeldException {
    long lockId = -1L;
    lockId = rand.nextLong();
    String lockName = String.valueOf(lockId);
    rowlocks.put(lockName, r);
    this.leases.
      createLease(lockName, new RowLockListener(lockName, region));
    return lockId;
  }

  /**
   * Method to get the Integer lock identifier used internally
   * from the long lock identifier used by the client.
   * @param lockId long row lock identifier from client
   * @return intId Integer row lock used internally in HRegion
   * @throws IOException Thrown if this is not a valid client lock id.
   */
  Integer getLockFromId(long lockId)
  throws IOException {
    if (lockId == -1L) {
      return null;
    }
    String lockName = String.valueOf(lockId);
    Integer rl = rowlocks.get(lockName);
    if (rl == null) {
      throw new IOException("Invalid row lock");
    }
    this.leases.renewLease(lockName);
    return rl;
  }

  @Override
  public void unlockRow(byte [] regionName, long lockId)
  throws IOException {
    checkOpen();
    NullPointerException npe = null;
    if(regionName == null) {
      npe = new NullPointerException("regionName is null");
    } else if(lockId == -1L) {
      npe = new NullPointerException("lockId is null");
    }
    if(npe != null) {
      IOException io = new IOException("Invalid arguments to unlockRow");
      io.initCause(npe);
      throw io;
    }
    try {
      HRegion region = getRegion(regionName);
      String lockName = String.valueOf(lockId);
      Integer r = rowlocks.remove(lockName);
      if(r == null) {
        throw new UnknownRowLockException(lockName);
      }
      region.releaseRowLock(r);
      this.leases.cancelLease(lockName);
      LOG.debug("Row lock " + lockId + " has been explicitly released by client");
    } catch (Throwable t) {
      throw convertThrowableToIOE(cleanup(t));
    }
  }

  @Override
  public void bulkLoadHFile(
      String hfilePath, byte[] regionName, byte[] familyName)
  throws IOException {
    bulkLoadHFile(hfilePath, regionName, familyName, false);
  }

  @Override
  public void bulkLoadHFile(
      String hfilePath, byte[] regionName, byte[] familyName,
      boolean assignSeqNum) throws IOException {
    HRegion region = getRegion(regionName);
    region.bulkLoadHFile(hfilePath, familyName, assignSeqNum);
  }

  @Override
  public void setNumHDFSQuorumReadThreads(int maxThreads) {
    LOG.debug("Setting setNumHDFSQuorumReadThreads to " + maxThreads);
    DFSClient client = null;
    if (this.fs instanceof DistributedFileSystem) {
      client = ((DistributedFileSystem)fs).getClient();

      if (maxThreads > 0) {
        client.enableParallelReads();
        client.setNumParallelThreadsForReads(maxThreads);
      } else {
        client.disableParallelReads();
      }
    }
    this.quorumReadThreadsMax = maxThreads;
  }

  @Override
  public void setHDFSQuorumReadTimeoutMillis(long timeoutMillis) {
    LOG.debug("Setting setHDFSQuorumReadTimeoutMillis to " + timeoutMillis + " ms.");
    DFSClient client;
    if (this.fs instanceof DistributedFileSystem) {
      client = ((DistributedFileSystem)fs).getClient();

      client.setQuorumReadTimeout(timeoutMillis);
    }
    this.quorumReadTimeoutMillis = timeoutMillis;
  }

  /**
   * Get max quorum read threads
   * @return max quorum read threads
   */
  public int getQuorumReadThreadsMax() {
    return quorumReadThreadsMax;
  }

  /**
   * Get quorum read timeout in millisecond
   * @return quorum read timeout
   */
  public long getQuorumReadTimeoutMillis() {
    return quorumReadTimeoutMillis;
  }

  Map<String, Integer> rowlocks =
    new ConcurrentHashMap<String, Integer>();

  /**
   * Instantiated as a row lock lease.
   * If the lease times out, the row lock is released
   */
  private class RowLockListener extends LeaseListener {
    private final HRegion region;

    RowLockListener(final String lockName, final HRegion region) {
      super(lockName);
      this.region = region;
    }

    @Override
    public void leaseExpired() {
      LOG.info("Row Lock " + this.getLeaseName() + " lease expired");
      Integer r = rowlocks.remove(this.getLeaseName());
      if(r != null) {
        region.releaseRowLock(r);
      }
    }
  }

  /** @return the info server */
  public InfoServer getInfoServer() {
    return infoServer;
  }

  /**
   * @return true if a stop or abort has been requested.
   */
  public boolean isStopRequested() {
    return this.stopRequestedAtStageOne.get() || abortRequested;
  }

  /**
   * @return true if a stop or abort has been requested.
   */
  public boolean isStopRequestedAtStageTwo() {
    return this.stopRequestedAtStageTwo.get() || abortRequested;
  }

  /**
   *
   * @return the configuration
   */
  public Configuration getConfiguration() {
    return conf;
  }

  /** @return the write lock for the server */
  ReentrantReadWriteLock.WriteLock getWriteLock() {
    return lock.writeLock();
  }

  /** @return recent closed regions info*/
  public List<ClosedRegionInfo> getRecentlyClosedRegionInfo() {
    return recentlyClosedRegions;
  }

  /**
   * @return Immutable list of this servers regions.
   */
  public Collection<HRegion> getOnlineRegions() {
    return Collections.unmodifiableCollection(onlineRegions.values());
  }

  @Override
  public HRegion [] getOnlineRegionsAsArray() {
    return getOnlineRegions().toArray(new HRegion[0]);
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

  @Override
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
  @Override
  public void flushRegion(byte[] regionName)
    throws IllegalArgumentException, IOException {
    HRegion region = getOnlineRegion(regionName);
    if (region == null) {
      throw new IllegalArgumentException("No region : " + new String(regionName)
      + " available");
    }
    region.flushcache();
  }

  /**
   * Flushes the given region if lastFlushTime < ifOlderThanTS
   */
   @Override
  public void flushRegion(byte[] regionName, long ifOlderThanTS)
     throws IllegalArgumentException, IOException {
     HRegion region = getOnlineRegion(regionName);
     if (region == null) {
       throw new IllegalArgumentException("No region : " + new String(regionName)
       + " available");
     }
     if (region.getMinFlushTimeForAllStores() < ifOlderThanTS) region.flushcache();
   }

  /**
   * @return the earliest time a store in the given region was flushed.
   */
  @Override
  public long getLastFlushTime(byte[] regionName) {
    HRegion region = getOnlineRegion(regionName);
    if (region == null) {
      throw new IllegalArgumentException("No region : " + new String(regionName)
      + " available");
    }
    return region.getMinFlushTimeForAllStores();
  }

  @Override
  public MapWritable getLastFlushTimes() {
     MapWritable map = new MapWritable();
     for (HRegion region: this.getOnlineRegions()) {
       map.put(new BytesWritable(region.getRegionName()),
           new LongWritable(region.getMinFlushTimeForAllStores()));
     }
     return map;
  }

  @Override
  public long getStartCode() {
    return this.serverInfo.getStartCode();
  }

  @Override
  public long getCurrentTimeMillis() {
    return EnvironmentEdgeManager.currentTimeMillis();
  }

  public Map<HRegionInfo, String> getSortedOnlineRegionInfosAndOpenDate() {
    TreeMap<HRegionInfo, String> result = new TreeMap<HRegionInfo, String>();
    synchronized(this.onlineRegions) {
      for (HRegion r: this.onlineRegions.values()) {
        result.put(r.getRegionInfo(), r.getOpenDateAsString());
      }
    }
    return result;
  }
  /**
   * This method removes HRegion corresponding to hri from the Map of onlineRegions.
   *
   * @param hri the HRegionInfo corresponding to the HRegion to-be-removed.
   * @return the removed HRegion, or null if the HRegion was not in onlineRegions.
   */
  HRegion removeFromOnlineRegions(HRegionInfo hri) {
    byte[] regionName = hri.getRegionName();
    serverInfo.getFlushedSequenceIdByRegion().remove(regionName);
    Integer key = Bytes.mapKey(regionName);
    HRegion toReturn = null;
    this.lock.writeLock().lock();
    try {
      toReturn = onlineRegions.remove(key);
      regionsClosing.remove(key);
    } finally {
      this.lock.writeLock().unlock();
    }
    return toReturn;
  }

  /**
   * This method removes HRegion corresponding to hri from the Map of regions to
   * retry closing.
   *
   * @param hri
   *          the HRegionInfo corresponding to the HRegion to-be-removed.
   * @return the removed HRegion, or null if the HRegion was not in
   *         onlineRegions.
   */
  HRegion removeFromRetryCloseRegions(HRegionInfo hri) {
    return this.retryCloseRegions.remove(Bytes.mapKey(hri.getRegionName()));
  }

  /**
   * This method adds HRegion corresponding to hri from the Map of regions to
   * retry closing
   *
   * @param hri
   *          the HRegionInfo corresponding to the HRegion to-be-added.
   */
  void addToRetryCloseRegions(HRegion hr) {
    this.retryCloseRegions.put(
        Bytes.mapKey(hr.getRegionInfo().getRegionName()), hr);
  }

  @Override
  public SortedMap<Long, HRegion> getCopyOfOnlineRegionsSortedBySize() {
    // we'll sort the regions in reverse
    SortedMap<Long, HRegion> sortedRegions = new TreeMap<Long, HRegion>(
        new Comparator<Long>() {
          @Override
          public int compare(Long a, Long b) {
            return -1 * a.compareTo(b);
          }
        });
    // Copy over all regions. Regions are sorted by size with biggest first.
    synchronized (this.onlineRegions) {
      for (HRegion region : this.onlineRegions.values()) {
        sortedRegions.put(Long.valueOf(region.memstoreSize.get()), region);
      }
    }
    return sortedRegions;
  }

  public HRegion getOnlineRegionByFullName(final String regionName) {
    return this.onlineRegions.get(Bytes.mapKey(Bytes.toBytes(
        regionName)));
  }

  /**
   * @param regionName
   * @return HRegion for the passed <code>regionName</code> or null if named
   * region is not member of the online regions.
   */
  public HRegion getOnlineRegion(final byte [] regionName) {
    return onlineRegions.get(Bytes.mapKey(regionName));
  }

  /** @return reference to FlushRequester */
  public FlushRequester getFlushRequester() {
    return this.cacheFlusher;
  }

  @Override
  public HRegion getRegion(final byte[] regionName)
  throws NotServingRegionException {
    HRegion region = null;
    this.lock.readLock().lock();
    try {
      region = onlineRegions.get(Integer.valueOf(Bytes.hashCode(regionName)));
      if (region == null) {
        throw new NotServingRegionException(regionName);
      }
      return region;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  /**
   * Get the top N most loaded regions this server is serving so we can
   * tell the master which regions it can reallocate if we're overloaded.
   * TODO: actually calculate which regions are most loaded. (Right now, we're
   * just grabbing N random regions being served regardless of load. We want to
   * avoid returning regions which are explicitly assigned to this server, or at
   * least not always return the same regions.)
   */
  protected HRegionInfo[] getMostLoadedRegions() {
    HRegionInfo[] regions = new HRegionInfo[onlineRegions.size()];
    int numOpenRegions = 0;
    synchronized (onlineRegions) {
      for (HRegion r : onlineRegions.values()) {
        if (r.isClosed() || r.isClosing()) {
          continue;
        }
        regions[numOpenRegions++] = r.getRegionInfo();
      }
    }
    int resultSize = Math.min(numRegionsToReport, numOpenRegions);
    if (numOpenRegions > numRegionsToReport) {
      // There are more candidate regions than the number of regions we want to
      // return. Shuffle them so that we are not always returning the same ones.
      // This is based on the Fisher-Yates algorithm but stops after the first N
      // regions because we only return the first N.
      Random rand = new Random();
      for (int i = 0; i < resultSize; i++) {
        int r = rand.nextInt(numOpenRegions - i) + i;
        HRegionInfo temp = regions[i];
        regions[i] = regions[r];
        regions[r] = temp;
      }
    }
    return Arrays.copyOfRange(regions, 0, resultSize);
  }

  /**
   * Called to verify that this server is up and running.
   *
   * @throws IOException
   */
  protected void checkOpen() throws IOException {
    if (this.stopRequestedAtStageTwo.get() || this.abortRequested) {
      throw new IOException("Server not running" +
        (this.abortRequested? ", aborting": ""));
    }
    // its ok to call checkFileSystem() because it is rate limited
    if (!fsOk) {
      checkFileSystem();
    }
    if (!fsOk) {
      throw new IOException("File system not available");
    }
  }

  /**
   * @return Returns list of non-closed regions hosted on this server.  If no
   * regions to check, returns an empty list.
   */
  protected Set<HRegion> getRegionsToCheck() {
    HashSet<HRegion> regionsToCheck = new HashSet<HRegion>();
    //TODO: is this locking necessary?
    lock.readLock().lock();
    try {
      regionsToCheck.addAll(this.onlineRegions.values());
    } finally {
      lock.readLock().unlock();
    }
    // Purge closed regions.
    for (final Iterator<HRegion> i = regionsToCheck.iterator(); i.hasNext();) {
      HRegion r = i.next();
      if (r.isClosed()) {
        i.remove();
      }
    }
    return regionsToCheck;
  }

  @Override
  public long getProtocolVersion(final String protocol,
      final long clientVersion)
  throws IOException {
    if (protocol.equals(HRegionInterface.class.getName())) {
      return HBaseRPCProtocolVersion.versionID;
    }
    throw new IOException("Unknown protocol to name node: " + protocol);
  }

  /**
   * @return Queue to which you can add outbound messages.
   */
  protected LinkedBlockingQueue<HMsg> getOutboundMsgs() {
    return this.outboundMsgs;
  }

  /**
   * @return Return the leases.
   */
  protected Leases getLeases() {
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
  protected FileSystem getFileSystem() {
    return fs;
  }

  /**
   * @return Info on port this server has bound to, etc.
   */
  public HServerInfo getServerInfo() {
    try {
      return getHServerInfo();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }
  }

  /** {@inheritDoc} */
  @Override
  public long incrementColumnValue(byte [] regionName, byte [] row,
      byte [] family, byte [] qualifier, long amount, boolean writeToWAL)
  throws IOException {
    checkOpen();

    if (regionName == null) {
      throw new IOException("Invalid arguments to incrementColumnValue " +
      "regionName is null");
    }
    try {
      HRegion region = getRegion(regionName);
      long retval = region.incrementColumnValue(row, family, qualifier, amount,
          writeToWAL);

      return retval;
    } catch (IOException e) {
      checkFileSystem();
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public HRegionInfo[] getRegionsAssignment() throws IOException {
    HRegionInfo[] regions = new HRegionInfo[onlineRegions.size()];
    Iterator<HRegion> ite = onlineRegions.values().iterator();
    for(int i = 0; ite.hasNext(); i++) {
      regions[i] = ite.next().getRegionInfo();
    }
    return regions;
  }

  /** {@inheritDoc} */
  @Override
  public HServerInfo getHServerInfo() throws IOException {
    return serverInfo;
  }

  @Override
  public MultiResponse multiAction(MultiAction mActions) throws IOException {
    checkOpen();
    MultiResponse response = new MultiResponse();
    if (mActions.getDeletes() != null) {
      for (Map.Entry<byte[], List<Delete>> e : mActions.getDeletes().entrySet()) {
        byte[] regionName = e.getKey();

        Object result;
         try {
           result = delete(regionName, e.getValue());
         } catch (IOException exception){
           result = exception;
         }

         response.addDeleteResponse(regionName, result);
      }
    }

    if (mActions.getPuts() != null) {
      for (Map.Entry<byte[], List<Put>> e : mActions.getPuts().entrySet()) {
        byte[] regionName = e.getKey();

        Object result;
         try {
           result = put(regionName, e.getValue());
         } catch (IOException exception){
           result = exception;
         }

         response.addPutResponse(regionName, result);
      }
    }

    if (mActions.getGets() != null) {
      for (Map.Entry<byte[], List<Get>> e : mActions.getGets().entrySet()) {
        byte[] regionName = e.getKey();

        Object result;
         try {
           result = get(regionName, e.getValue());
         } catch (IOException exception){
           result = exception;
         }

         response.addGetResponse(regionName, result);
      }
    }
    return response;
  }

  @Override
  public MultiPutResponse multiPut(MultiPut puts) throws IOException {
    MultiPutResponse resp = new MultiPutResponse();

    // do each region as it's own.
    int size = puts.puts.size();
    int index = 0;
    for( Map.Entry<byte[], List<Put>> e: puts.puts.entrySet()) {
      int result = put(e.getKey(), e.getValue());
      resp.addResult(e.getKey(), result);

      index++;
      if (index < size) {
        // remove the reference to the region list of Puts to save RAM except
        // for the last one. We will lose the reference to the last one pretty
        // soon anyway; keep it for a little more, until we get back
        // to HBaseServer level, where we might need to pretty print
        // the MultiPut request for debugging slow/large puts.
        // Note: A single row "put" from client also end up in server
        // as a multiPut().
        // We set the value to an empty array so taskmonitor can either get
        //  the old value or an empty array. If we call clear() on the array,
        //  then we might have a race condition where we're iterating over the
        //  array at the same time we clear it (which throws an exception)
        // This relies on the fact that Map.Entry.setValue() boils down to a
        //   simple reference assignment, which is atomic
        e.setValue(emptyPutArray); // clear some RAM
      }
    }

    return resp;
  }

  @Override
  public String toString() {
    return this.serverInfo.toString();
  }

  /**
   * Interval at which threads should run
   * @return the interval
   */
  public int getThreadWakeFrequency() {
    return threadWakeFrequency;
  }

  //
  // Main program and support routines
  //

  /**
   * @param hrs
   * @return Thread the RegionServer is running in correctly named.
   * @throws IOException
   */
  public static Thread startRegionServer(final HRegionServer hrs)
  throws IOException {
    return startRegionServer(hrs,
      "regionserver" + hrs.getServerInfo().getServerAddress().getPort());
  }

  /**
   * @param hrs
   * @param name
   * @return Thread the RegionServer is running in correctly named.
   * @throws IOException
   */
  public static Thread startRegionServer(final HRegionServer hrs,
      final String name)
  throws IOException {
    Thread t = new Thread(hrs);
    t.setName(name);
    t.start();
    // Install shutdown hook that will catch signals and run an orderly shutdown
    // of the hrs.
    ShutdownHook.install(hrs.getConfiguration(),
      FileSystem.get(hrs.getConfiguration()), hrs, t);
    return t;
  }

  private static void printUsageAndExit() {
    printUsageAndExit(null);
  }

  private static void printUsageAndExit(final String message) {
    if (message != null) {
      System.err.println(message);
    }
    System.err.println("Usage: java org.apache.hbase.HRegionServer start|stop [-D <conf.param=value>]");
    System.exit(0);
  }

  /**
   * Utility for constructing an instance of the passed HRegionServer class.
   * @param regionServerClass
   * @param conf2
   * @return HRegionServer instance.
   */
  public static HRegionServer constructRegionServer(Class<? extends HRegionServer> regionServerClass,
      final Configuration inputConf)  {
    try {
      Constructor<? extends HRegionServer> c =
        regionServerClass.getConstructor(Configuration.class);
      HRegionServer server = c.newInstance(inputConf);
      server.initialize();
      return server;
    } catch (Exception e) {
      throw new RuntimeException("Failed construction of " +
        "Master: " + regionServerClass.toString(), e);
    }
  }

  /**
   * Do class main.
   * @param args
   * @param regionServerClass HRegionServer to instantiate.
   */
  protected static void doMain(final String [] args,
      final Class<? extends HRegionServer> regionServerClass) {
    Configuration conf = HBaseConfiguration.create();

    Options opt = new Options();
    opt.addOption("D", true, "Override HBase Configuration Settings");
    try {
      CommandLine cmd = new GnuParser().parse(opt, args);

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

      if (cmd.getArgList().contains("start")) {
        try {
          // If 'local', don't start a region server here.  Defer to
          // LocalHBaseCluster.  It manages 'local' clusters.
          if (LocalHBaseCluster.isLocal(conf)) {
            LOG.warn("Not starting a distinct region server because " +
              HConstants.CLUSTER_DISTRIBUTED + " is false");
          } else {
            RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            if (runtime != null) {
              LOG.info("vmInputArguments=" + runtime.getInputArguments());
            }
            HRegionServer hrs = constructRegionServer(regionServerClass, conf);
            Preconditions.checkState(mainRegionServer == null,
                "Main regionserver initialized twice");
            mainRegionServer = hrs;
            startRegionServer(hrs);
          }
        } catch (Throwable t) {
          LOG.error( "Can not start region server because "+
              StringUtils.stringifyException(t) );
          System.exit(-1);
        }
      } else if (cmd.getArgList().contains("stop")) {
        throw new ParseException("To shutdown the regionserver run " +
            "bin/hbase-daemon.sh stop regionserver or send a kill signal to" +
            "the regionserver pid");
      } else {
        throw new ParseException("Unknown argument(s): " +
            org.apache.commons.lang.StringUtils.join(cmd.getArgs(), " "));
      }
    } catch (ParseException e) {
      LOG.error("Could not parse", e);
      printUsageAndExit();
    }
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(
        this, protocol, clientVersion, clientMethodsHash);
  }

  @Override
  public int updateFavoredNodes(AssignmentPlan plan)
  throws IOException {
    if (plan == null) {
      LOG.debug("The assignment plan is empty");
      return 0;
    }
    int counter = 0;
    for (Map.Entry<HRegionInfo, List<HServerAddress>> entry :
      plan.getAssignmentMap().entrySet()) {
        // Get the online region
      HRegion region =
        this.onlineRegions.get(Bytes.mapKey(entry.getKey().getRegionName()));

      if (region == null) {
        LOG.warn("Region " + entry.getKey().getRegionNameAsString() +
            " is not running on this " + this.serverInfo.getHostnamePort()
            + " region server any more !");
        continue;
      }
      List<HServerAddress> serverList = entry.getValue();
      if (serverList != null) {
        String favoredNodes = RegionPlacement.getFavoredNodes(serverList);
        InetSocketAddress[] favoredAddress =
          RegionPlacement.getFavoredInetSocketAddress(serverList);
        // Set the favored nodes
        region.setFavoredNodes(favoredAddress);
        LOG.debug("Update region " + region.getRegionNameAsString() +
            " with new favored nodes: " + favoredNodes);
        counter++;
      }
    }
    return counter;
  }

  /**
   * @param args
   */
  public static void main(String [] args) {
    Configuration conf = HBaseConfiguration.create();
    @SuppressWarnings("unchecked")
    Class<? extends HRegionServer> regionServerClass =
      (Class<? extends HRegionServer>) conf.getClass(HConstants.REGION_SERVER_IMPL,
        HRegionServer.class);
    doMain(args, regionServerClass);
  }

  @Override
  public boolean isStopped() {
    return stopRequestedAtStageOne.get();
  }

  @Override
  public String getStopReason() {
    return stopReason;
  }

  /**
   * Get the regionserver running in this JVM as part of the main method (mini-cluster RS instances
   * do not count), if the given address matches. This is used to "short-circuit" client calls done
   * by the Thrift handler within the regionserver to the same regionserver.
   * @return the main region server in this JVM or null if the address does not match
   */
  public static HRegionInterface getMainRS(HServerAddress address) {
    if (mainRegionServer != null &&
        address.equals(mainRegionServer.serverInfo.getServerAddress()) &&
        mainRegionServer.isRpcServerRunning) {
      return mainRegionServer;
    }
    return null;
  }

  @Override
  public String getRSThreadName() {
    return "RS-" + serverInfo.getServerName();
  }

  /**
   * Reload the configuration from disk.
   */
  @Override
  public void updateConfiguration() {
    LOG.info("Reloading the configuration from disk.");
    // Reload the configuration from disk.
    conf.reloadConfiguration();

    // Notify all the observers that the configuration has changed.
    configurationManager.notifyAllObservers(conf);
  }

  public long getResponseQueueSize(){
    HBaseServer s = server;
    if (s != null) {
      return s.getResponseQueueSize();
    }
    return 0;
  }

  @Override
  public void notifyOnChange(Configuration conf) {
    int threads = conf.getInt(
        HConstants.HDFS_QUORUM_READ_THREADS_MAX, HConstants.DEFAULT_HDFS_QUORUM_READ_THREADS_MAX);
    long timeout = conf.getLong(
        HConstants.HDFS_QUORUM_READ_TIMEOUT_MILLIS, HConstants.DEFAULT_HDFS_QUORUM_READ_TIMEOUT_MILLIS);

    boolean origProfiling = enableServerSideProfilingForAllCalls.get();
    boolean newProfiling = conf.getBoolean(
        "hbase.regionserver.enable.serverside.profiling", false);
    if (origProfiling != newProfiling) {
      enableServerSideProfilingForAllCalls.set(newProfiling);
      LOG.info("enableServerSideProfilingForAllCalls changed from "
          + origProfiling + " to " + newProfiling);
    }

    if (threads != this.quorumReadThreadsMax) {
      LOG.info("HDFS quorum read thread number is changed from " + this.quorumReadThreadsMax + " to " + threads);
      this.setNumHDFSQuorumReadThreads(threads);
    }

    if (timeout != this.quorumReadTimeoutMillis) {
      LOG.info("HDFS quorum read timeout is changed from " + this.quorumReadTimeoutMillis + " to " + timeout);
      this.setHDFSQuorumReadTimeoutMillis(timeout);
    }

    HRegionServer.useSeekNextUsingHint =
        conf.getBoolean("hbase.regionserver.scan.timestampfilter.allow_seek_next_using_hint", true);

    endpointServer.reload(conf);
  }

  /**
   *
   * Returns null if no data is available.
   */
  @Override
  public List<Bucket> getHistogram(byte[] regionName) throws IOException {
    checkOpen();
    HRegion region = getRegion(regionName);
    HFileHistogram hist = region.getHistogram();
    if (hist == null) return null;
    return HRegionUtilities.adjustHistogramBoundariesToRegionBoundaries(
        hist.getUniformBuckets(), region.getStartKey(), region.getEndKey());
  }

  @Override
  public List<List<Bucket>> getHistograms(List<byte[]> regionNames)
      throws IOException {
    List<List<Bucket>> ret = new ArrayList<>();
    for (HRegion oregion : this.onlineRegions.values()) {
      checkOpen();
      HRegion region = getRegion(oregion.getRegionName());
      HFileHistogram hist = region.getHistogram();
      if (hist == null) return null;
      ret.add(HRegionUtilities.adjustHistogramBoundariesToRegionBoundaries(
        hist.getUniformBuckets(), region.getStartKey(), region.getEndKey()));
    }
    return ret;
  }

  /**
   *
   * Returns null if no data is available.
   */
  @Override
  public List<Bucket> getHistogramForStore(byte[] regionName, byte[] family)
      throws IOException {
    checkOpen();
    HRegion region = getRegion(regionName);
    HFileHistogram hist = region.getStore(family).getHistogram();
    if (hist == null) return null;
    return HRegionUtilities.adjustHistogramBoundariesToRegionBoundaries(
        hist.getUniformBuckets(), region.getStartKey(), region.getEndKey());
  }

  @Override
  public void close() throws Exception {
    this.shutdownServers();
  }

  @Override
  public byte[] callEndpoint(String epName, String methodName,
      List<byte[]> params, final byte[] regionName, final byte[] startRow,
      final byte[] stopRow) throws ThriftHBaseException, IOException {
    return endpointServer.callEndpoint(epName, methodName, params, regionName,
        startRow, stopRow);
  }

  @Override
  public HRegionLocation getLocation(byte[] table, byte[] row, boolean reload)
      throws IOException {
    if (reload) {
      return regionServerConnection.relocateRegion(new StringBytes(table), row);
    }
    return regionServerConnection.locateRegion(new StringBytes(table), row);
  }

  @Override
  public boolean requestSplit(HRegionIf r) {
    return this.compactSplitThread.requestSplit((HRegion) r);
  }

  @Override
  public void requestCompaction(HRegionIf r, String why) {
    this.compactSplitThread.requestCompaction((HRegion) r, why);
  }

  /**
   * @param conf
   * @return true if the hregionserver.prefer.ipv6.address is set
   */
  public static boolean preferIpv6AddressForRegionServer(Configuration conf) {
    return conf != null ? conf.getBoolean(HREGIONSERVER_PREFER_IPV6_Address, false) : false;
  }
}
