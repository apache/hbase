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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.management.ObjectName;

import com.google.protobuf.Message;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.ClockOutOfSyncException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.FailedSanityCheckException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionMovedException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.UnknownRowLockException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.ZNodeClearer;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.AdminProtocol;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ClientProtocol;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Exec;
import org.apache.hadoop.hbase.client.coprocessor.ExecResult;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorService.ExecutorType;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.HBaseRPCErrorHandler;
import org.apache.hadoop.hbase.ipc.HBaseRpcMetrics;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.FlushRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetServerInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetServerInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetStoreFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetStoreFileResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionResponse.RegionOpeningState;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.SplitRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.SplitRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.StopServerResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ActionResult;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest.FamilyPath;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Condition;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ExecCoprocessorRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ExecCoprocessorResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.LockRowRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.LockRowResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Mutate.MutateType;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.UnlockRowRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.UnlockRowResponse;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameBytesPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionInfo;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RpcRequestBody;
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
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.security.User;
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
import org.apache.hadoop.hbase.zookeeper.ClusterStatusTracker;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.RootRegionTracker;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.codehaus.jackson.map.ObjectMapper;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.Coprocessor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionLoad;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.ReportRSFatalErrorRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;
import org.apache.hadoop.hbase.RegionServerStatusProtocol;

import com.google.common.base.Function;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;

import static org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import static org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceResponse;

/**
 * HRegionServer makes a set of HRegions available to clients. It checks in with
 * the HMaster. There are many HRegionServers in a single HBase deployment.
 */
@InterfaceAudience.Private
@SuppressWarnings("deprecation")
public class  HRegionServer implements ClientProtocol,
    AdminProtocol, Runnable, RegionServerServices, HBaseRPCErrorHandler, LastSequenceId {

  public static final Log LOG = LogFactory.getLog(HRegionServer.class);

  private final Random rand = new Random();

  /*
   * Strings to be used in forming the exception message for
   * RegionsAlreadyInTransitionException.
   */
  protected static final String OPEN = "OPEN";
  protected static final String CLOSE = "CLOSE";

  //RegionName vs current action in progress
  //true - if open region action in progress
  //false - if close region action in progress
  protected final ConcurrentSkipListMap<byte[], Boolean> regionsInTransitionInRS =
    new ConcurrentSkipListMap<byte[], Boolean>(Bytes.BYTES_COMPARATOR);

  protected long maxScannerResultSize;

  // Cache flushing
  protected MemStoreFlusher cacheFlusher;

  // catalog tracker
  protected CatalogTracker catalogTracker;

  /**
   * Go here to get table descriptors.
   */
  protected TableDescriptors tableDescriptors;

  // Replication services. If no replication, this handler will be null.
  protected ReplicationSourceService replicationSourceHandler;
  protected ReplicationSinkService replicationSinkHandler;

  // Compactions
  public CompactSplitThread compactSplitThread;

  final Map<String, RegionScanner> scanners =
      new ConcurrentHashMap<String, RegionScanner>();

  /**
   * Map of regions currently being served by this region server. Key is the
   * encoded region name.  All access should be synchronized.
   */
  protected final Map<String, HRegion> onlineRegions =
    new ConcurrentHashMap<String, HRegion>();

  // Leases
  protected Leases leases;

  // Instance of the hbase executor service.
  protected ExecutorService service;

  // Request counter.
  // Do we need this?  Can't we just sum region counters?  St.Ack 20110412
  protected AtomicInteger requestCount = new AtomicInteger();

  // If false, the file system has become unavailable
  protected volatile boolean fsOk;
  protected HFileSystem fs;

  // Set when a report to the master comes back with a message asking us to
  // shutdown. Also set by call to stop when debugging or running unit tests
  // of HRegionServer in isolation.
  protected volatile boolean stopped = false;

  // Go down hard. Used if file system becomes unavailable and also in
  // debugging and unit tests.
  protected volatile boolean abortRequested;

  // Port we put up the webui on.
  protected int webuiport = -1;

  ConcurrentMap<String, Integer> rowlocks = new ConcurrentHashMap<String, Integer>();

  // A state before we go into stopped state.  At this stage we're closing user
  // space regions.
  private boolean stopping = false;

  private volatile boolean killed = false;

  protected final Configuration conf;

  protected final AtomicBoolean haveRootRegion = new AtomicBoolean(false);
  private boolean useHBaseChecksum; // verify hbase checksums?
  private Path rootDir;

  protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  final int numRetries;
  protected final int threadWakeFrequency;
  private final int msgInterval;

  protected final int numRegionsToReport;

  // Remote HMaster
  private RegionServerStatusProtocol hbaseMaster;

  // Server to handle client requests. Default access so can be accessed by
  // unit tests.
  RpcServer rpcServer;

  private final InetSocketAddress isa;

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

  /*
   * Check for compactions requests.
   */
  Chore compactionChecker;

  // HLog and HLog roller. log is protected rather than private to avoid
  // eclipse warning when accessed by inner classes
  protected volatile HLog hlog;
  LogRoller hlogRoller;

  // flag set after we're done setting up server threads (used for testing)
  protected volatile boolean isOnline;

  // zookeeper connection and watcher
  private ZooKeeperWatcher zooKeeper;

  // master address manager and watcher
  private MasterAddressTracker masterAddressManager;

  // Cluster Status Tracker
  private ClusterStatusTracker clusterStatusTracker;

  // Log Splitting Worker
  private SplitLogWorker splitLogWorker;

  // A sleeper that sleeps for msgInterval.
  private final Sleeper sleeper;

  private final int rpcTimeout;

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

  /**
   * This servers startcode.
   */
  private final long startcode;

  /**
   * MX Bean for RegionServerInfo
   */
  private ObjectName mxBean = null;

  /**
   * Chore to clean periodically the moved region list
   */
  private MovedRegionsCleaner movedRegionsCleaner;

  /**
   * The lease timeout period for row locks (milliseconds).
   */
  private final int rowLockLeaseTimeoutPeriod;

  /**
   * The lease timeout period for client scanners (milliseconds).
   */
  private final int scannerLeaseTimeoutPeriod;

  /**
   * The reference to the QosFunction
   */
  private final QosFunction qosFunction;


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
      HConstants.HBASE_CHECKSUM_VERIFICATION, true);

    // Config'ed params
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
      HConstants.HBASE_RPC_TIMEOUT_KEY,
      HConstants.DEFAULT_HBASE_RPC_TIMEOUT);

    this.abortRequested = false;
    this.stopped = false;

    this.rowLockLeaseTimeoutPeriod = conf.getInt(
      HConstants.HBASE_REGIONSERVER_ROWLOCK_TIMEOUT_PERIOD,
      HConstants.DEFAULT_HBASE_REGIONSERVER_ROWLOCK_TIMEOUT_PERIOD);

    this.scannerLeaseTimeoutPeriod = conf.getInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
      HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);

    // Server to handle client requests.
    String hostname = Strings.domainNamePointerToHostName(DNS.getDefaultHost(
      conf.get("hbase.regionserver.dns.interface", "default"),
      conf.get("hbase.regionserver.dns.nameserver", "default")));
    int port = conf.getInt(HConstants.REGIONSERVER_PORT,
      HConstants.DEFAULT_REGIONSERVER_PORT);
    // Creation of a HSA will force a resolve.
    InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);
    if (initialIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initialIsa);
    }

    this.rpcServer = HBaseRPC.getServer(AdminProtocol.class, this,
      new Class<?>[]{ClientProtocol.class,
        AdminProtocol.class, HBaseRPCErrorHandler.class,
        OnlineRegions.class},
        initialIsa.getHostName(), // BindAddress is IP we got for this server.
        initialIsa.getPort(),
        conf.getInt("hbase.regionserver.handler.count", 10),
        conf.getInt("hbase.regionserver.metahandler.count", 10),
        conf.getBoolean("hbase.rpc.verbose", false),
        conf, HConstants.QOS_THRESHOLD);
    // Set our address.
    this.isa = this.rpcServer.getListenerAddress();

    this.rpcServer.setErrorHandler(this);
    this.rpcServer.setQosFunction((qosFunction = new QosFunction()));
    this.startcode = System.currentTimeMillis();

    // login the server principal (if using secure Hadoop)
    User.login(this.conf, "hbase.regionserver.keytab.file",
      "hbase.regionserver.kerberos.principal", this.isa.getHostName());
    regionServerAccounting = new RegionServerAccounting();
    cacheConfig = new CacheConfig(conf);
  }

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
  protected @interface QosPriority {
    int priority() default 0;
  }

  QosFunction getQosFunction() {
    return qosFunction;
  }

  RegionScanner getScanner(long scannerId) {
    String scannerIdString = Long.toString(scannerId);
    return scanners.get(scannerIdString);
  }

  /**
   * Utility used ensuring higher quality of service for priority rpcs; e.g.
   * rpcs to .META. and -ROOT-, etc.
   */
  class QosFunction implements Function<RpcRequestBody,Integer> {
    private final Map<String, Integer> annotatedQos;
    //We need to mock the regionserver instance for some unit tests (set via
    //setRegionServer method.
    //The field value is initially set to the enclosing instance of HRegionServer.
    private HRegionServer hRegionServer = HRegionServer.this;

    //The logic for figuring out high priority RPCs is as follows:
    //1. if the method is annotated with a QosPriority of QOS_HIGH,
    //   that is honored
    //2. parse out the protobuf message and see if the request is for meta
    //   region, and if so, treat it as a high priority RPC
    //Some optimizations for (2) are done here -
    //Clients send the argument classname as part of making the RPC. The server
    //decides whether to deserialize the proto argument message based on the
    //pre-established set of argument classes (knownArgumentClasses below).
    //This prevents the server from having to deserialize all proto argument
    //messages prematurely.
    //All the argument classes declare a 'getRegion' method that returns a
    //RegionSpecifier object. Methods can be invoked on the returned object
    //to figure out whether it is a meta region or not.
    @SuppressWarnings("unchecked")
    private final Class<? extends Message>[] knownArgumentClasses = new Class[]{
        GetRegionInfoRequest.class,
        GetStoreFileRequest.class,
        CloseRegionRequest.class,
        FlushRegionRequest.class,
        SplitRegionRequest.class,
        CompactRegionRequest.class,
        GetRequest.class,
        MutateRequest.class,
        ScanRequest.class,
        LockRowRequest.class,
        UnlockRowRequest.class,
        MultiRequest.class
    };

    //Some caches for helping performance
    private final Map<String, Class<? extends Message>> argumentToClassMap =
        new HashMap<String, Class<? extends Message>>();
    private final Map<String, Map<Class<? extends Message>, Method>>
      methodMap = new HashMap<String, Map<Class<? extends Message>, Method>>();

    public QosFunction() {
      Map<String, Integer> qosMap = new HashMap<String, Integer>();
      for (Method m : HRegionServer.class.getMethods()) {
        QosPriority p = m.getAnnotation(QosPriority.class);
        if (p != null) {
          qosMap.put(m.getName(), p.priority());
        }
      }

      annotatedQos = qosMap;
      if (methodMap.get("parseFrom") == null) {
        methodMap.put("parseFrom",
            new HashMap<Class<? extends Message>, Method>());
      }
      if (methodMap.get("getRegion") == null) {
        methodMap.put("getRegion", 
            new HashMap<Class<? extends Message>, Method>());
      }
      for (Class<? extends Message> cls : knownArgumentClasses) {
        argumentToClassMap.put(cls.getCanonicalName(), cls);
        try {
          methodMap.get("parseFrom").put(cls,
                          cls.getDeclaredMethod("parseFrom",ByteString.class));
          methodMap.get("getRegion").put(cls, cls.getDeclaredMethod("getRegion"));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    void setRegionServer(HRegionServer server) {
      this.hRegionServer = server;
    }

    public boolean isMetaRegion(byte[] regionName) {
      HRegion region;
      try {
        region = hRegionServer.getRegion(regionName);
      } catch (NotServingRegionException ignored) {
        return false;
      }
      return region.getRegionInfo().isMetaRegion();
    }

    @Override
    public Integer apply(RpcRequestBody from) {
      String methodName = from.getMethodName();
      Class<? extends Message> rpcArgClass = null;
      if (from.hasRequestClassName()) {
        String cls = from.getRequestClassName();
        rpcArgClass = argumentToClassMap.get(cls);
      }

      Integer priorityByAnnotation = annotatedQos.get(methodName);
      if (priorityByAnnotation != null) {
        return priorityByAnnotation;
      }

      if (rpcArgClass == null || from.getRequest().isEmpty()) {
        return HConstants.NORMAL_QOS;
      }
      Object deserializedRequestObj = null;
      //check whether the request has reference to Meta region
      try {
        Method parseFrom = methodMap.get("parseFrom").get(rpcArgClass);
        deserializedRequestObj = parseFrom.invoke(null, from.getRequest());
        Method getRegion = methodMap.get("getRegion").get(rpcArgClass);
        RegionSpecifier regionSpecifier =
            (RegionSpecifier)getRegion.invoke(deserializedRequestObj,
                (Object[])null);
        HRegion region = hRegionServer.getRegion(regionSpecifier);
        if (region.getRegionInfo().isMetaRegion()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("High priority: " + from.toString());
          }
          return HConstants.HIGH_QOS;
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }

      if (methodName.equals("scan")) { // scanner methods...
        ScanRequest request = (ScanRequest)deserializedRequestObj;
        if (!request.hasScannerId()) {
          return HConstants.NORMAL_QOS;
        }
        RegionScanner scanner = hRegionServer.getScanner(request.getScannerId());
        if (scanner != null && scanner.getRegionInfo().isMetaRegion()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("High priority scanner request: " + request.getScannerId());
          }
          return HConstants.HIGH_QOS;
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Low priority: " + from.toString());
      }
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
      initializeThreads();
      int nbBlocks = conf.getInt("hbase.regionserver.nbreservationblocks", 4);
      for (int i = 0; i < nbBlocks; i++) {
        reservedSpace.add(new byte[HConstants.DEFAULT_SIZE_RESERVATION_BLOCK]);
      }
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
    this.catalogTracker = new CatalogTracker(this.zooKeeper, this.conf,
      this, this.conf.getInt("hbase.regionserver.catalog.timeout", 600000));
    catalogTracker.start();

    // Retrieve clusterId
    // Since cluster status is now up
    // ID should have already been set by HMaster
    try {
      String clusterId = ZKClusterId.readClusterIdZNode(this.zooKeeper);
      if (clusterId == null) {
        this.abort("Cluster ID has not been set");
      }
      this.conf.set(HConstants.CLUSTER_ID, clusterId);
      LOG.info("ClusterId : "+clusterId);
    } catch (KeeperException e) {
      this.abort("Failed to retrieve Cluster ID",e);
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

    this.leases = new Leases(this.threadWakeFrequency);

    // Create the thread for the ThriftServer.
    if (conf.getBoolean("hbase.regionserver.export.thrift", false)) {
      thriftServer = new HRegionThriftServer(this, conf);
      thriftServer.start();
      LOG.info("Started Thrift API from Region Server.");
    }

    // Create the thread to clean the moved regions list
    movedRegionsCleaner = MovedRegionsCleaner.createAndStart(this);
  }

  /**
   * The HRegionServer sticks in this loop until closed.
   */
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
        RegionServerStartupResponse w = reportForDuty();
        if (w == null) {
          LOG.warn("reportForDuty failed; sleeping and then retrying.");
          this.sleeper.sleep();
        } else {
          handleReportForDutyResponse(w);
          break;
        }
      }
      registerMBean();

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
              // Set stopped if no requests since last time we went around the loop.
              // The remaining meta regions will be closed on our way out.
              if (oldRequestCount == this.requestCount.get()) {
                stop("Stopped; only catalog regions remaining online");
                break;
              }
              oldRequestCount = this.requestCount.get();
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
          tryRegionServerReport(lastMsg, now);
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

    movedRegionsCleaner.stop("Region Server stopping");

    // Send interrupts to wake up threads if sleeping so they notice shutdown.
    // TODO: Should we check they are alive? If OOME could have exited already
    if (this.cacheFlusher != null) this.cacheFlusher.interruptIfNecessary();
    if (this.compactSplitThread != null) this.compactSplitThread.interruptIfNecessary();
    if (this.hlogRoller != null) this.hlogRoller.interruptIfNecessary();
    if (this.compactionChecker != null)
      this.compactionChecker.interrupt();

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
    if (this.hbaseMaster != null) {
      HBaseRPC.stopProxy(this.hbaseMaster);
      this.hbaseMaster = null;
    }
    this.leases.close();

    if (!killed) {
      join();
    }

    try {
      deleteMyEphemeralNode();
    } catch (KeeperException e) {
      LOG.warn("Failed deleting my ephemeral node", e);
    }
    // We may have failed to delete the znode at the previous step, but
    //  we delete the file anyway: a second attempt to delete the znode is likely to fail again.
    ZNodeClearer.deleteMyEphemeralNodeOnDisk();
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

  void tryRegionServerReport(long reportStartTime, long reportEndTime)
  throws IOException {
    HBaseProtos.ServerLoad sl = buildServerLoad(reportStartTime, reportEndTime);
    // Why we do this?
    this.requestCount.set(0);
    try {
      RegionServerReportRequest.Builder request = RegionServerReportRequest.newBuilder();
      ServerName sn = ServerName.parseVersionedServerName(
        this.serverNameFromMasterPOV.getVersionedBytes());
      request.setServer(ProtobufUtil.toServerName(sn));
      request.setLoad(sl);
      this.hbaseMaster.regionServerReport(null, request.build());
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof YouAreDeadException) {
        // This will be caught and handled as a fatal error in run()
        throw ioe;
      }
      // Couldn't connect to the master, get location from zk and reconnect
      // Method blocks until new master is found or we are stopped
      getMaster();
    }
  }

  HBaseProtos.ServerLoad buildServerLoad(long reportStartTime, long reportEndTime) {
    Collection<HRegion> regions = getOnlineRegionsLocalContext();
    MemoryUsage memory =
      ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();

    HBaseProtos.ServerLoad.Builder serverLoad = HBaseProtos.ServerLoad.newBuilder();
    serverLoad.setNumberOfRequests((int)metrics.getRequests());
    serverLoad.setTotalNumberOfRequests(requestCount.get());
    serverLoad.setUsedHeapMB((int)(memory.getUsed() / 1024 / 1024));
    serverLoad.setMaxHeapMB((int) (memory.getMax() / 1024 / 1024));
    Set<String> coprocessors = this.hlog.getCoprocessorHost().getCoprocessors();
    for (String coprocessor : coprocessors) {
      serverLoad.addCoprocessors(
        Coprocessor.newBuilder().setName(coprocessor).build());
    }
    for (HRegion region : regions) {
      serverLoad.addRegionLoads(createRegionLoad(region));
    }
    serverLoad.setReportStartTime(reportStartTime);
    serverLoad.setReportEndTime(reportEndTime);

    return serverLoad.build();
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
          LOG.info("We were exiting though online regions are not empty," +
              " because some regions failed closing");
        }
        break;
      }
      Threads.sleep(200);
    }
  }

  private void closeWAL(final boolean delete) {
    try {
      if (this.hlog != null) {
        if (delete) {
          hlog.closeAndDelete();
        } else {
          hlog.close();
        }
      }
    } catch (Throwable e) {
      LOG.error("Close and delete failed", RemoteExceptionHandler.checkThrowable(e));
    }
  }

  private void closeAllScanners() {
    // Close any outstanding scanners. Means they'll get an UnknownScanner
    // exception next time they come in.
    for (Map.Entry<String, RegionScanner> e : this.scanners.entrySet()) {
      try {
        e.getValue().close();
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
  protected void handleReportForDutyResponse(final RegionServerStartupResponse c)
  throws IOException {
    try {
      for (NameStringPair e : c.getMapEntriesList()) {
        String key = e.getName();
        // The hostname the master sees us as.
        if (key.equals(HConstants.KEY_FOR_HOSTNAME_SEEN_BY_MASTER)) {
          String hostnameFromMasterPOV = e.getValue();
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

      // Save it in a file, this will allow to see if we crash
      ZNodeClearer.writeMyEphemeralNodeOnDisk(getMyEphemeralNodePath());

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
      this.dynamicMetrics = RegionServerDynamicMetrics.newInstance();
      startServiceThreads();
      LOG.info("Serving as " + this.serverNameFromMasterPOV +
        ", RPC listening on " + this.isa +
        ", sessionid=0x" +
        Long.toHexString(this.zooKeeper.getRecoverableZooKeeper().getSessionId()));
      isOnline = true;
    } catch (Throwable e) {
      this.isOnline = false;
      stop("Failed initialization");
      throw convertThrowableToIOE(cleanup(e, "Failed init"),
          "Region server startup failed");
    } finally {
      sleeper.skipSleepCycle();
    }
  }

  private void createMyEphemeralNode() throws KeeperException {
    ZKUtil.createEphemeralNodeAndWatch(this.zooKeeper, getMyEphemeralNodePath(),
      HConstants.EMPTY_BYTE_ARRAY);
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
  private RegionLoad createRegionLoad(final HRegion r) {
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
    RegionLoad.Builder regionLoad = RegionLoad.newBuilder();
    RegionSpecifier.Builder regionSpecifier = RegionSpecifier.newBuilder();
    regionSpecifier.setType(RegionSpecifierType.REGION_NAME);
    regionSpecifier.setValue(ByteString.copyFrom(name));
    regionLoad.setRegionSpecifier(regionSpecifier.build())
      .setStores(stores)
      .setStorefiles(storefiles)
      .setStoreUncompressedSizeMB(storeUncompressedSizeMB)
      .setStorefileSizeMB(storefileSizeMB)
      .setMemstoreSizeMB(memstoreSizeMB)
      .setStorefileIndexSizeMB(storefileIndexSizeMB)
      .setRootIndexSizeKB(rootIndexSizeKB)
      .setTotalStaticIndexSizeKB(totalStaticIndexSizeKB)
      .setTotalStaticBloomSizeKB(totalStaticBloomSizeKB)
      .setReadRequestsCount((int) r.readRequestsCount.get())
      .setWriteRequestsCount((int) r.writeRequestsCount.get())
      .setTotalCompactingKVs(totalCompactingKVs)
      .setCurrentCompactedKVs(currentCompactedKVs)
      .setCompleteSequenceId(r.completeSequenceId);
    Set<String> coprocessors = r.getCoprocessorHost().getCoprocessors();
    for (String coprocessor : coprocessors) {
      regionLoad.addCoprocessors(
        Coprocessor.newBuilder().setName(coprocessor).build());
    }

    return regionLoad.build();
  }

  /**
   * @param encodedRegionName
   * @return An instance of RegionLoad.
   */
  public RegionLoad createRegionLoad(final String encodedRegionName) {
    HRegion r = null;
    r = this.onlineRegions.get(encodedRegionName);
    return r != null ? createRegionLoad(r) : null;
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
              this.instance.compactSplitThread.requestCompaction(r, s,
                getName() + " requests compaction");
            } else if (s.isMajorCompaction()) {
              if (majorCompactPriority == DEFAULT_PRIORITY ||
                  majorCompactPriority > r.getCompactPriority()) {
                this.instance.compactSplitThread.requestCompaction(r, s,
                    getName() + " requests major compaction; use default priority");
              } else {
               this.instance.compactSplitThread.requestCompaction(r, s,
                  getName() + " requests major compaction; use configured priority",
                  this.majorCompactPriority);
              }
            }
          } catch (IOException e) {
            LOG.warn("Failed major compaction check on " + r, e);
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
    int readRequestsCount = 0;
    int writeRequestsCount = 0;
    long storefileIndexSize = 0;
    HDFSBlocksDistribution hdfsBlocksDistribution =
      new HDFSBlocksDistribution();
    long totalStaticIndexSize = 0;
    long totalStaticBloomSize = 0;
    long numPutsWithoutWAL = 0;
    long dataInMemoryWithoutWAL = 0;

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
    UncaughtExceptionHandler handler = new UncaughtExceptionHandler() {
      public void uncaughtException(Thread t, Throwable e) {
        abort("Uncaught exception in service thread " + t.getName(), e);
      }
    };

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

    Threads.setDaemonThreadRunning(this.hlogRoller.getThread(), n + ".logRoller", handler);
    Threads.setDaemonThreadRunning(this.cacheFlusher.getThread(), n + ".cacheFlusher",
      handler);
    Threads.setDaemonThreadRunning(this.compactionChecker.getThread(), n +
      ".compactionChecker", handler);

    // Leases is not a Thread. Internally it runs a daemon thread. If it gets
    // an unhandled exception, it will just exit.
    this.leases.setName(n + ".leaseChecker");
    this.leases.start();

    // Put up the webui.  Webui may come up on port other than configured if
    // that port is occupied. Adjust serverInfo if this is the case.
    this.webuiport = putUpWebUI();

    if (this.replicationSourceHandler == this.replicationSinkHandler &&
        this.replicationSourceHandler != null) {
      this.replicationSourceHandler.startReplicationService();
    } else if (this.replicationSourceHandler != null) {
      this.replicationSourceHandler.startReplicationService();
    } else if (this.replicationSinkHandler != null) {
      this.replicationSinkHandler.startReplicationService();
    }

    // Start Server.  This service is like leases in that it internally runs
    // a thread.
    this.rpcServer.start();

    // Create the log splitting worker and start it
    this.splitLogWorker = new SplitLogWorker(this.zooKeeper,
        this.getConfiguration(), this.getServerName(), this);
    splitLogWorker.start();
  }

  /**
   * Puts up the webui.
   * @return Returns final port -- maybe different from what we started with.
   * @throws IOException
   */
  private int putUpWebUI() throws IOException {
    int port = this.conf.getInt(HConstants.REGIONSERVER_INFO_PORT, 60030);
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
    return port;
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
        && this.compactionChecker.isAlive())) {
      stop("One or more threads are no longer alive -- stop");
      return false;
    }
    return true;
  }

  @Override
  public HLog getWAL() {
    return this.hlog;
  }

  @Override
  public CatalogTracker getCatalogTracker() {
    return this.catalogTracker;
  }

  @Override
  public void stop(final String msg) {
    this.stopped = true;
    LOG.info("STOPPED: " + msg);
    // Wakes run() if it is sleeping
    sleeper.skipSleepCycle();
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
        getCompactionRequester().requestCompaction(r, s, "Opening Region");
      }
    }
    // Update ZK, ROOT or META
    if (r.getRegionInfo().isRootRegion()) {
      RootRegionTracker.setRootLocation(getZooKeeper(),
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
        ReportRSFatalErrorRequest.Builder builder =
          ReportRSFatalErrorRequest.newBuilder();
        ServerName sn =
          ServerName.parseVersionedServerName(this.serverNameFromMasterPOV.getVersionedBytes());
        builder.setServer(ProtobufUtil.toServerName(sn));
        builder.setErrorMessage(msg);
        hbaseMaster.reportRSFatalError(
          null,builder.build());
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
    Threads.shutdown(this.cacheFlusher.getThread());
    if (this.hlogRoller != null) {
      Threads.shutdown(this.hlogRoller.getThread());
    }
    if (this.compactSplitThread != null) {
      this.compactSplitThread.join();
    }
    if (this.service != null) this.service.shutdown();
    if (this.replicationSourceHandler != null &&
        this.replicationSourceHandler == this.replicationSinkHandler) {
      this.replicationSourceHandler.stopReplicationService();
    } else if (this.replicationSourceHandler != null) {
      this.replicationSourceHandler.stopReplicationService();
    } else if (this.replicationSinkHandler != null) {
      this.replicationSinkHandler.stopReplicationService();
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
    RegionServerStatusProtocol master = null;
    boolean refresh = false; // for the first time, use cached data
    while (keepLooping() && master == null) {
      masterServerName = this.masterAddressManager.getMasterAddress(refresh);
      if (masterServerName == null) {
        if (!keepLooping()) {
          // give up with no connection.
          LOG.debug("No master found and cluster is stopped; bailing out");
          return null;
        }
        LOG.debug("No master found; retry");
        previousLogTime = System.currentTimeMillis();
        refresh = true; // let's try pull it from ZK directly

        sleeper.sleep();
        continue;
      }

      InetSocketAddress isa =
        new InetSocketAddress(masterServerName.getHostname(), masterServerName.getPort());

      LOG.info("Attempting connect to Master server at " +
        this.masterAddressManager.getMasterAddress());
      try {
        // Do initial RPC setup. The final argument indicates that the RPC
        // should retry indefinitely.
        master = (RegionServerStatusProtocol) HBaseRPC.waitForProxy(
            RegionServerStatusProtocol.class, RegionServerStatusProtocol.VERSION,
            isa, this.conf, -1,
            this.rpcTimeout, this.rpcTimeout);
        LOG.info("Connected to master at " + isa);
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
  private RegionServerStartupResponse reportForDuty() throws IOException {
    RegionServerStartupResponse result = null;
    ServerName masterServerName = getMaster();
    if (masterServerName == null) return result;
    try {
      this.requestCount.set(0);
      LOG.info("Telling master at " + masterServerName + " that we are up " +
        "with port=" + this.isa.getPort() + ", startcode=" + this.startcode);
      long now = EnvironmentEdgeManager.currentTimeMillis();
      int port = this.isa.getPort();
      RegionServerStartupRequest.Builder request = RegionServerStartupRequest.newBuilder();
      request.setPort(port);
      request.setServerStartCode(this.startcode);
      request.setServerCurrentTime(now);
      result = this.hbaseMaster.regionServerStartup(null, request.build());
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof ClockOutOfSyncException) {
        LOG.fatal("Master rejected startup because clock is out of sync", ioe);
        // Re-throw IOE will cause RS to abort
        throw ioe;
      } else {
        LOG.warn("error telling master we are up", se);
      }
    }
    return result;
  }

  @Override
  public long getLastSequenceId(byte[] region) {
    Long lastFlushedSequenceId = -1l;
    try {
      GetLastFlushedSequenceIdRequest req =
        RequestConverter.buildGetLastFlushedSequenceIdRequest(region);
      lastFlushedSequenceId = hbaseMaster.getLastFlushedSequenceId(null, req)
      .getLastFlushedSequenceId();
    } catch (ServiceException e) {
      lastFlushedSequenceId = -1l;
      LOG.warn("Unable to connect to the master to check " +
          "the last flushed sequence id", e);
    }
    return lastFlushedSequenceId;
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

  /** @return the write lock for the server */
  ReentrantReadWriteLock.WriteLock getWriteLock() {
    return lock.writeLock();
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
    map.put("storefileSizeMB", storefileSizeMB);
    map.put("storefileIndexSizeMB", storefileIndexSizeMB);
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

  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public ProtocolSignature getProtocolSignature(
      String protocol, long version, int clientMethodsHashCode)
  throws IOException {
    if (protocol.equals(ClientProtocol.class.getName())) {
      return new ProtocolSignature(ClientProtocol.VERSION, null);
    } else if (protocol.equals(AdminProtocol.class.getName())) {
      return new ProtocolSignature(AdminProtocol.VERSION, null);
    }
    throw new IOException("Unknown protocol: " + protocol);
  }

  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public long getProtocolVersion(final String protocol, final long clientVersion)
  throws IOException {
    if (protocol.equals(ClientProtocol.class.getName())) {
      return ClientProtocol.VERSION;
    } else if (protocol.equals(AdminProtocol.class.getName())) {
      return AdminProtocol.VERSION;
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


  public ConcurrentSkipListMap<byte[], Boolean> getRegionsInTransitionInRS() {
    return this.regionsInTransitionInRS;
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
    // passing fake times to buildServerLoad is okay, because we only care about the coprocessor part.
    HBaseProtos.ServerLoad sl = buildServerLoad(0, 0);
    return sl == null? null:
      new ServerLoad(sl).getRegionServerCoprocessors();
  }

  /**
   * Register bean with platform management server
   */
  void registerMBean() {
    MXBeanImpl mxBeanInfo = MXBeanImpl.init(this);
    mxBean = MBeanUtil.registerMBean("RegionServer", "RegionServer",
        mxBeanInfo);
    LOG.info("Registered RegionServer MXBean");
  }

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
      RegionScanner s = scanners.remove(this.scannerName);
      if (s != null) {
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

  protected void checkIfRegionInTransition(HRegionInfo region,
      String currentAction) throws RegionAlreadyInTransitionException {
    byte[] encodedName = region.getEncodedNameAsBytes();
    if (this.regionsInTransitionInRS.containsKey(encodedName)) {
      boolean openAction = this.regionsInTransitionInRS.get(encodedName);
      // The below exception message will be used in master.
      throw new RegionAlreadyInTransitionException("Received:" + currentAction +
        " for the region:" + region.getRegionNameAsString() +
        " ,which we are already trying to " +
        (openAction ? OPEN : CLOSE)+ ".");
    }
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
    return closeRegion(region, abort, zk, -1, null);
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
      final boolean zk, final int versionOfClosingNode, ServerName sn) {
    if (this.regionsInTransitionInRS.containsKey(region.getEncodedNameAsBytes())) {
      LOG.warn("Received close for region we are already opening or closing; " +
        region.getEncodedName());
      return false;
    }
    this.regionsInTransitionInRS.putIfAbsent(region.getEncodedNameAsBytes(), false);
    CloseRegionHandler crh = null;
    if (region.isRootRegion()) {
      crh = new CloseRootHandler(this, this, region, abort, zk,
        versionOfClosingNode);
    } else if (region.isMetaRegion()) {
      crh = new CloseMetaHandler(this, this, region, abort, zk,
        versionOfClosingNode);
    } else {
      crh = new CloseRegionHandler(this, this, region, abort, zk, versionOfClosingNode, sn);
    }
    this.service.submit(crh);
    return true;
  }

   /**
   * @param regionName
   * @return HRegion for the passed binary <code>regionName</code> or null if
   *         named region is not member of the online regions.
   */
  public HRegion getOnlineRegion(final byte[] regionName) {
    String encodedRegionName = HRegionInfo.encodeRegionName(regionName);
    return this.onlineRegions.get(encodedRegionName);
  }

  @Override
  public HRegion getFromOnlineRegions(final String encodedRegionName) {
    return this.onlineRegions.get(encodedRegionName);
  }


  @Override
  public boolean removeFromOnlineRegions(final String encodedRegionName, ServerName destination) {
    HRegion toReturn = this.onlineRegions.remove(encodedRegionName);

    if (destination != null){
      addToMovedRegions(encodedRegionName, destination);
    }
    
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
   * Protected utility method for safely obtaining an HRegion handle.
   *
   * @param regionName
   *          Name of online {@link HRegion} to return
   * @return {@link HRegion} for <code>regionName</code>
   * @throws NotServingRegionException
   */
  protected HRegion getRegion(final byte[] regionName)
      throws NotServingRegionException {
    String encodedRegionName = HRegionInfo.encodeRegionName(regionName);
    return getRegionByEncodedName(encodedRegionName);
  }

  protected HRegion getRegionByEncodedName(String encodedRegionName)
    throws NotServingRegionException {

    HRegion region = this.onlineRegions.get(encodedRegionName);
    if (region == null) {
      ServerName sn = getMovedRegion(encodedRegionName);
      if (sn != null) {
        throw new RegionMovedException(sn.getHostname(), sn.getPort());
      } else {
        throw new NotServingRegionException("Region is not online: " + encodedRegionName);
      }
    }
    return region;
  }

  /*
   * Cleanup after Throwable caught invoking method. Converts <code>t</code> to
   * IOE if it isn't already.
   *
   * @param t Throwable
   *
   * @return Throwable converted to an IOE; methods can only let out IOEs.
   */
  protected Throwable cleanup(final Throwable t) {
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
  protected Throwable cleanup(final Throwable t, final String msg) {
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
  protected IOException convertThrowableToIOE(final Throwable t) {
    return convertThrowableToIOE(t, null);
  }

  /*
   * @param t
   *
   * @param msg Message to put in new IOE if passed <code>t</code> is not an IOE
   *
   * @return Make <code>t</code> an IOE if it isn't already.
   */
  protected IOException convertThrowableToIOE(final Throwable t, final String msg) {
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

  protected long addRowLock(Integer r, HRegion region) throws LeaseStillHeldException {
    String lockName = null;
    long lockId;
    do {
      lockId = nextLong();
      lockName = String.valueOf(lockId);
    } while (rowlocks.putIfAbsent(lockName, r) != null);
    this.leases.createLease(lockName, this.rowLockLeaseTimeoutPeriod, new RowLockListener(lockName,
        region));
    return lockId;
  }

  protected long addScanner(RegionScanner s) throws LeaseStillHeldException {
    long scannerId = nextLong();
    String scannerName = String.valueOf(scannerId);
    scanners.put(scannerName, s);
    this.leases.createLease(scannerName, this.scannerLeaseTimeoutPeriod, new ScannerListener(
        scannerName));
    return scannerId;
  }

  /**
   * Generate a random positive long number
   *
   * @return a random positive long number
   */
  protected long nextLong() {
    long n = rand.nextLong();
    if (n == 0) {
      return nextLong();
    }
    if (n < 0) {
      n = -n;
    }
    return n;
  }

  // Start Client methods

  /**
   * Get data from a table.
   *
   * @param controller the RPC controller
   * @param request the get request
   * @throws ServiceException
   */
  @Override
  public GetResponse get(final RpcController controller,
      final GetRequest request) throws ServiceException {
    try {
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      GetResponse.Builder builder = GetResponse.newBuilder();
      ClientProtos.Get get = request.getGet();
      Boolean existence = null;
      Result r = null;
      if (request.getClosestRowBefore()) {
        if (get.getColumnCount() != 1) {
          throw new DoNotRetryIOException(
            "get ClosestRowBefore supports one and only one family now, not "
              + get.getColumnCount() + " families");
        }
        byte[] row = get.getRow().toByteArray();
        byte[] family = get.getColumn(0).getFamily().toByteArray();
        r = region.getClosestRowBefore(row, family);
      } else {
        Get clientGet = ProtobufUtil.toGet(get);
        if (request.getExistenceOnly() && region.getCoprocessorHost() != null) {
          existence = region.getCoprocessorHost().preExists(clientGet);
        }
        if (existence == null) {
          Integer lock = getLockFromId(clientGet.getLockId());
          r = region.get(clientGet, lock);
          if (request.getExistenceOnly()) {
            boolean exists = r != null && !r.isEmpty();
            if (region.getCoprocessorHost() != null) {
              exists = region.getCoprocessorHost().postExists(clientGet, exists);
            }
            existence = Boolean.valueOf(exists);
          }
        }
      }
      if (existence != null) {
        builder.setExists(existence.booleanValue());
      } else if (r != null) {
        builder.setResult(ProtobufUtil.toResult(r));
      }
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Mutate data in a table.
   *
   * @param controller the RPC controller
   * @param request the mutate request
   * @throws ServiceException
   */
  @Override
  public MutateResponse mutate(final RpcController controller,
      final MutateRequest request) throws ServiceException {
    try {
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      MutateResponse.Builder builder = MutateResponse.newBuilder();
      Mutate mutate = request.getMutate();
      if (!region.getRegionInfo().isMetaTable()) {
        cacheFlusher.reclaimMemStoreMemory();
      }
      Integer lock = null;
      Result r = null;
      Boolean processed = null;
      MutateType type = mutate.getMutateType();
      switch (type) {
      case APPEND:
        r = append(region, mutate);
        break;
      case INCREMENT:
        r = increment(region, mutate);
        break;
      case PUT:
        Put put = ProtobufUtil.toPut(mutate);
        lock = getLockFromId(put.getLockId());
        if (request.hasCondition()) {
          Condition condition = request.getCondition();
          byte[] row = condition.getRow().toByteArray();
          byte[] family = condition.getFamily().toByteArray();
          byte[] qualifier = condition.getQualifier().toByteArray();
          CompareOp compareOp = CompareOp.valueOf(condition.getCompareType().name());
          ByteArrayComparable comparator =
            ProtobufUtil.toComparator(condition.getComparator());
          if (region.getCoprocessorHost() != null) {
            processed = region.getCoprocessorHost().preCheckAndPut(
              row, family, qualifier, compareOp, comparator, put);
          }
          if (processed == null) {
            boolean result = region.checkAndMutate(row, family,
              qualifier, compareOp, comparator, put, lock, true);
            if (region.getCoprocessorHost() != null) {
              result = region.getCoprocessorHost().postCheckAndPut(row, family,
                qualifier, compareOp, comparator, put, result);
            }
            processed = Boolean.valueOf(result);
          }
        } else {
          region.put(put, lock);
          processed = Boolean.TRUE;
        }
        break;
      case DELETE:
        Delete delete = ProtobufUtil.toDelete(mutate);
        lock = getLockFromId(delete.getLockId());
        if (request.hasCondition()) {
          Condition condition = request.getCondition();
          byte[] row = condition.getRow().toByteArray();
          byte[] family = condition.getFamily().toByteArray();
          byte[] qualifier = condition.getQualifier().toByteArray();
          CompareOp compareOp = CompareOp.valueOf(condition.getCompareType().name());
          ByteArrayComparable comparator =
            ProtobufUtil.toComparator(condition.getComparator());
          if (region.getCoprocessorHost() != null) {
            processed = region.getCoprocessorHost().preCheckAndDelete(
              row, family, qualifier, compareOp, comparator, delete);
          }
          if (processed == null) {
            boolean result = region.checkAndMutate(row, family,
              qualifier, compareOp, comparator, delete, lock, true);
            if (region.getCoprocessorHost() != null) {
              result = region.getCoprocessorHost().postCheckAndDelete(row, family,
                qualifier, compareOp, comparator, delete, result);
            }
            processed = Boolean.valueOf(result);
          }
        } else {
          region.delete(delete, lock, delete.getWriteToWAL());
          processed = Boolean.TRUE;
        }
        break;
        default:
          throw new DoNotRetryIOException(
            "Unsupported mutate type: " + type.name());
      }
      if (processed != null) {
        builder.setProcessed(processed.booleanValue());
      } else if (r != null) {
        builder.setResult(ProtobufUtil.toResult(r));
      }
      return builder.build();
    } catch (IOException ie) {
      checkFileSystem();
      throw new ServiceException(ie);
    }
  }

  //
  // remote scanner interface
  //

  /**
   * Scan data in a table.
   *
   * @param controller the RPC controller
   * @param request the scan request
   * @throws ServiceException
   */
  @Override
  public ScanResponse scan(final RpcController controller,
      final ScanRequest request) throws ServiceException {
    Leases.Lease lease = null;
    String scannerName = null;
    try {
      if (!request.hasScannerId() && !request.hasScan()) {
        throw new DoNotRetryIOException(
          "Missing required input: scannerId or scan");
      }
      long scannerId = -1;
      if (request.hasScannerId()) {
        scannerId = request.getScannerId();
        scannerName = String.valueOf(scannerId);
      }
      try {
        checkOpen();
      } catch (IOException e) {
        // If checkOpen failed, server not running or filesystem gone,
        // cancel this lease; filesystem is gone or we're closing or something.
        if (scannerName != null) {
          try {
            leases.cancelLease(scannerName);
          } catch (LeaseException le) {
            LOG.info("Server shutting down and client tried to access missing scanner " +
              scannerName);
          }
        }
        throw e;
      }
      requestCount.incrementAndGet();

      try {
        int ttl = 0;
        HRegion region = null;
        RegionScanner scanner = null;
        boolean moreResults = true;
        boolean closeScanner = false;
        ScanResponse.Builder builder = ScanResponse.newBuilder();
        if (request.hasCloseScanner()) {
          closeScanner = request.getCloseScanner();
        }
        int rows = 1;
        if (request.hasNumberOfRows()) {
          rows = request.getNumberOfRows();
        }
        if (request.hasScannerId()) {
          scanner = scanners.get(scannerName);
          if (scanner == null) {
            throw new UnknownScannerException(
              "Name: " + scannerName + ", already closed?");
          }
          region = getRegion(scanner.getRegionInfo().getRegionName());
        } else {
          region = getRegion(request.getRegion());
          ClientProtos.Scan protoScan = request.getScan();
          Scan scan = ProtobufUtil.toScan(protoScan);
          region.prepareScanner(scan);
          if (region.getCoprocessorHost() != null) {
            scanner = region.getCoprocessorHost().preScannerOpen(scan);
          }
          if (scanner == null) {
            scanner = region.getScanner(scan);
          }
          if (region.getCoprocessorHost() != null) {
            scanner = region.getCoprocessorHost().postScannerOpen(scan, scanner);
          }
          scannerId = addScanner(scanner);
          scannerName = String.valueOf(scannerId);
          ttl = this.scannerLeaseTimeoutPeriod;
        }

        if (rows > 0) {
          try {
            // Remove lease while its being processed in server; protects against case
            // where processing of request takes > lease expiration time.
            lease = leases.removeLease(scannerName);
            List<Result> results = new ArrayList<Result>(rows);
            long currentScanResultSize = 0;

            boolean done = false;
            // Call coprocessor. Get region info from scanner.
            if (region != null && region.getCoprocessorHost() != null) {
              Boolean bypass = region.getCoprocessorHost().preScannerNext(
                scanner, results, rows);
              if (!results.isEmpty()) {
                for (Result r : results) {
                  for (KeyValue kv : r.raw()) {
                    currentScanResultSize += kv.heapSize();
                  }
                }
              }
              if (bypass != null && bypass.booleanValue()) {
                done = true;
              }
            }

            if (!done) {
              long maxResultSize = scanner.getMaxResultSize();
              if (maxResultSize <= 0) {
                maxResultSize = maxScannerResultSize;
              }
              List<KeyValue> values = new ArrayList<KeyValue>();
              for (int i = 0; i < rows
                  && currentScanResultSize < maxResultSize; i++) {
                // Collect values to be returned here
                boolean moreRows = scanner.next(values, SchemaMetrics.METRIC_NEXTSIZE);
                if (!values.isEmpty()) {
                  for (KeyValue kv : values) {
                    currentScanResultSize += kv.heapSize();
                  }
                  results.add(new Result(values));
                }
                if (!moreRows) {
                  break;
                }
                values.clear();
              }

              // coprocessor postNext hook
              if (region != null && region.getCoprocessorHost() != null) {
                region.getCoprocessorHost().postScannerNext(scanner, results, rows, true);
              }
            }

            // If the scanner's filter - if any - is done with the scan
            // and wants to tell the client to stop the scan. This is done by passing
            // a null result, and setting moreResults to false.
            if (scanner.isFilterDone() && results.isEmpty()) {
              moreResults = false;
              results = null;
            } else {
              for (Result result: results) {
                if (result != null) {
                  builder.addResult(ProtobufUtil.toResult(result));
                }
              }
            }
          } finally {
            // We're done. On way out re-add the above removed lease.
            // Adding resets expiration time on lease.
            if (scanners.containsKey(scannerName)) {
              if (lease != null) leases.addLease(lease);
              ttl = this.scannerLeaseTimeoutPeriod;
            }
          }
        }

        if (!moreResults || closeScanner) {
          ttl = 0;
          moreResults = false;
          if (region != null && region.getCoprocessorHost() != null) {
            if (region.getCoprocessorHost().preScannerClose(scanner)) {
              return builder.build(); // bypass
            }
          }
          scanner = scanners.remove(scannerName);
          if (scanner != null) {
            scanner.close();
            leases.cancelLease(scannerName);
            if (region != null && region.getCoprocessorHost() != null) {
              region.getCoprocessorHost().postScannerClose(scanner);
            }
          }
        }

        if (ttl > 0) {
          builder.setTtl(ttl);
        }
        builder.setScannerId(scannerId);
        builder.setMoreResults(moreResults);
        return builder.build();
      } catch (Throwable t) {
        if (scannerName != null &&
            t instanceof NotServingRegionException) {
          scanners.remove(scannerName);
        }
        throw convertThrowableToIOE(cleanup(t));
      }
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Lock a row in a table.
   *
   * @param controller the RPC controller
   * @param request the lock row request
   * @throws ServiceException
   */
  @Override
  public LockRowResponse lockRow(final RpcController controller,
      final LockRowRequest request) throws ServiceException {
    try {
      if (request.getRowCount() != 1) {
        throw new DoNotRetryIOException(
          "lockRow supports only one row now, not " + request.getRowCount() + " rows");
      }
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      byte[] row = request.getRow(0).toByteArray();
      try {
        Integer r = region.obtainRowLock(row);
        long lockId = addRowLock(r, region);
        LOG.debug("Row lock " + lockId + " explicitly acquired by client");
        LockRowResponse.Builder builder = LockRowResponse.newBuilder();
        builder.setLockId(lockId);
        return builder.build();
      } catch (Throwable t) {
        throw convertThrowableToIOE(cleanup(t,
          "Error obtaining row lock (fsOk: " + this.fsOk + ")"));
      }
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Unlock a locked row in a table.
   *
   * @param controller the RPC controller
   * @param request the unlock row request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public UnlockRowResponse unlockRow(final RpcController controller,
      final UnlockRowRequest request) throws ServiceException {
    try {
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      if (!request.hasLockId()) {
        throw new DoNotRetryIOException(
          "Invalid unlock rowrequest, missing lock id");
      }
      long lockId = request.getLockId();
      String lockName = String.valueOf(lockId);
      try {
        Integer r = rowlocks.remove(lockName);
        if (r == null) {
          throw new UnknownRowLockException(lockName);
        }
        region.releaseRowLock(r);
        this.leases.cancelLease(lockName);
        LOG.debug("Row lock " + lockId
          + " has been explicitly released by client");
        return UnlockRowResponse.newBuilder().build();
      } catch (Throwable t) {
        throw convertThrowableToIOE(cleanup(t));
      }
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Atomically bulk load several HFiles into an open region
   * @return true if successful, false is failed but recoverably (no action)
   * @throws IOException if failed unrecoverably
   */
  @Override
  public BulkLoadHFileResponse bulkLoadHFile(final RpcController controller,
      final BulkLoadHFileRequest request) throws ServiceException {
    try {
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      List<Pair<byte[], String>> familyPaths = new ArrayList<Pair<byte[], String>>();
      for (FamilyPath familyPath: request.getFamilyPathList()) {
        familyPaths.add(new Pair<byte[], String>(familyPath.getFamily().toByteArray(),
          familyPath.getPath()));
      }
      boolean bypass = false;
      if (region.getCoprocessorHost() != null) {
        bypass = region.getCoprocessorHost().preBulkLoadHFile(familyPaths);
      }
      boolean loaded = false;
      if (!bypass) {
        loaded = region.bulkLoadHFiles(familyPaths, request.getAssignSeqNum());
      }
      if (region.getCoprocessorHost() != null) {
        loaded = region.getCoprocessorHost().postBulkLoadHFile(familyPaths, loaded);
      }
      BulkLoadHFileResponse.Builder builder = BulkLoadHFileResponse.newBuilder();
      builder.setLoaded(loaded);
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
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
  public ExecCoprocessorResponse execCoprocessor(final RpcController controller,
      final ExecCoprocessorRequest request) throws ServiceException {
    try {
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      ExecCoprocessorResponse.Builder
        builder = ExecCoprocessorResponse.newBuilder();
      ClientProtos.Exec call = request.getCall();
      Exec clientCall = ProtobufUtil.toExec(call);
      ExecResult result = region.exec(clientCall);
      builder.setValue(ProtobufUtil.toParameter(result.getValue()));
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  public CoprocessorServiceResponse execService(final RpcController controller,
      final CoprocessorServiceRequest request) throws ServiceException {
    try {
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      // ignore the passed in controller (from the serialized call)
      ServerRpcController execController = new ServerRpcController();
      Message result = region.execService(execController, request.getCall());
      if (execController.getFailedOn() != null) {
        throw execController.getFailedOn();
      }
      CoprocessorServiceResponse.Builder builder =
          CoprocessorServiceResponse.newBuilder();
      builder.setRegion(RequestConverter.buildRegionSpecifier(
          RegionSpecifierType.REGION_NAME, region.getRegionName()));
      builder.setValue(
          builder.getValueBuilder().setName(result.getClass().getName())
              .setValue(result.toByteString()));
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Execute multiple actions on a table: get, mutate, and/or execCoprocessor
   *
   * @param controller the RPC controller
   * @param request the multi request
   * @throws ServiceException
   */
  @Override
  public MultiResponse multi(final RpcController controller,
      final MultiRequest request) throws ServiceException {
    try {
      HRegion region = getRegion(request.getRegion());
      MultiResponse.Builder builder = MultiResponse.newBuilder();
      if (request.hasAtomic() && request.getAtomic()) {
        List<Mutate> mutates = new ArrayList<Mutate>();
        for (ClientProtos.MultiAction actionUnion : request.getActionList()) {
          if (actionUnion.hasMutate()) {
            mutates.add(actionUnion.getMutate());
          } else {
            throw new DoNotRetryIOException(
              "Unsupported atomic action type: " + actionUnion);
          }
        }
        mutateRows(region, mutates);
      } else {
        ActionResult.Builder resultBuilder = null;
        List<Mutate> mutates = new ArrayList<Mutate>();
        for (ClientProtos.MultiAction actionUnion : request.getActionList()) {
          requestCount.incrementAndGet();
          try {
            Object result = null;
            if (actionUnion.hasGet()) {
              Get get = ProtobufUtil.toGet(actionUnion.getGet());
              Integer lock = getLockFromId(get.getLockId());
              Result r = region.get(get, lock);
              if (r != null) {
                result = ProtobufUtil.toResult(r);
              }
            } else if (actionUnion.hasMutate()) {
              Mutate mutate = actionUnion.getMutate();
              MutateType type = mutate.getMutateType();
              if (type != MutateType.PUT && type != MutateType.DELETE) {
                if (!mutates.isEmpty()) {
                  doBatchOp(builder, region, mutates);
                  mutates.clear();
                } else if (!region.getRegionInfo().isMetaTable()) {
                  cacheFlusher.reclaimMemStoreMemory();
                }
              }
              Result r = null;
              switch (type) {
              case APPEND:
                r = append(region, mutate);
                break;
              case INCREMENT:
                r = increment(region, mutate);
                break;
              case PUT:
                mutates.add(mutate);
                break;
              case DELETE:
                mutates.add(mutate);
                break;
              default:
                throw new DoNotRetryIOException("Unsupported mutate type: " + type.name());
              }
              if (r != null) {
                result = ProtobufUtil.toResult(r);
              }
            } else if (actionUnion.hasExec()) {
              Exec call = ProtobufUtil.toExec(actionUnion.getExec());
              result = region.exec(call).getValue();
            } else {
              LOG.warn("Error: invalid action: " + actionUnion + ". "
                + "it must be a Get, Mutate, or Exec.");
              throw new DoNotRetryIOException("Invalid action, "
                + "it must be a Get, Mutate, or Exec.");
            }
            if (result != null) {
              if (resultBuilder == null) {
                resultBuilder = ActionResult.newBuilder();
              } else {
                resultBuilder.clear();
              }
              NameBytesPair value = ProtobufUtil.toParameter(result);
              resultBuilder.setValue(value);
              builder.addResult(resultBuilder.build());
            }
          } catch (IOException ie) {
            builder.addResult(ResponseConverter.buildActionResult(ie));
          }
        }
        if (!mutates.isEmpty()) {
          doBatchOp(builder, region, mutates);
        }
      }
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

// End Client methods
// Start Admin methods

  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public GetRegionInfoResponse getRegionInfo(final RpcController controller,
      final GetRegionInfoRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      HRegionInfo info = region.getRegionInfo();
      GetRegionInfoResponse.Builder builder = GetRegionInfoResponse.newBuilder();
      builder.setRegionInfo(HRegionInfo.convert(info));
      if (request.hasCompactionState() && request.getCompactionState()) {
        builder.setCompactionState(
          CompactionRequest.getCompactionState(info.getRegionId()));
      }
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  public GetStoreFileResponse getStoreFile(final RpcController controller,
      final GetStoreFileRequest request) throws ServiceException {
    try {
      HRegion region = getRegion(request.getRegion());
      requestCount.incrementAndGet();
      Set<byte[]> columnFamilies = null;
      if (request.getFamilyCount() == 0) {
        columnFamilies = region.getStores().keySet();
      } else {
        columnFamilies = new HashSet<byte[]>();
        for (ByteString cf: request.getFamilyList()) {
          columnFamilies.add(cf.toByteArray());
        }
      }
      int nCF = columnFamilies.size();
      List<String>  fileList = region.getStoreFileList(
        columnFamilies.toArray(new byte[nCF][]));
      GetStoreFileResponse.Builder builder = GetStoreFileResponse.newBuilder();
      builder.addAllStoreFile(fileList);
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public GetOnlineRegionResponse getOnlineRegion(final RpcController controller,
      final GetOnlineRegionRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.incrementAndGet();
      List<HRegionInfo> list = new ArrayList<HRegionInfo>(onlineRegions.size());
      for (HRegion region: this.onlineRegions.values()) {
        list.add(region.getRegionInfo());
      }
      Collections.sort(list);
      return ResponseConverter.buildGetOnlineRegionResponse(list);
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }


  // Region open/close direct RPCs

  /**
   * Open a region on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public OpenRegionResponse openRegion(final RpcController controller, final OpenRegionRequest request)
  throws ServiceException {
    int versionOfOfflineNode = -1;
    if (request.hasVersionOfOfflineNode()) {
      versionOfOfflineNode = request.getVersionOfOfflineNode();
    }
    try {
      checkOpen();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
    requestCount.incrementAndGet();
    OpenRegionResponse.Builder builder = OpenRegionResponse.newBuilder();
    Map<String, HTableDescriptor> htds = new HashMap<String, HTableDescriptor>(
        request.getRegionList().size());
    boolean isBulkAssign = request.getRegionList().size() > 1;
    for (RegionInfo regionInfo : request.getRegionList()) {
      HRegionInfo region = HRegionInfo.convert(regionInfo);
      try {
        checkIfRegionInTransition(region, OPEN);
        HRegion onlineRegion = getFromOnlineRegions(region.getEncodedName());
        if (null != onlineRegion) {
          // See HBASE-5094. Cross check with META if still this RS is owning
          // the region.
          Pair<HRegionInfo, ServerName> p = MetaReader.getRegion(
              this.catalogTracker, region.getRegionName());
          if (this.getServerName().equals(p.getSecond())) {
            LOG.warn("Attempted open of " + region.getEncodedName()
                + " but already online on this server");
            builder.addOpeningState(RegionOpeningState.ALREADY_OPENED);
            continue;
          } else {
            LOG.warn("The region " + region.getEncodedName()
                + " is online on this server but META does not have this server.");
            removeFromOnlineRegions(region.getEncodedName(), null);
          }
        }
        LOG.info("Received request to open region: " + region.getRegionNameAsString() + " on "
            + this.serverNameFromMasterPOV);
        HTableDescriptor htd = htds.get(region.getTableNameAsString());
        if (htd == null) {
          htd = this.tableDescriptors.get(region.getTableName());
          htds.put(region.getTableNameAsString(), htd);
        }
        this.regionsInTransitionInRS.putIfAbsent(
            region.getEncodedNameAsBytes(), true);
        // Need to pass the expected version in the constructor.
        if (region.isRootRegion()) {
          this.service.submit(new OpenRootHandler(this, this, region, htd,
              versionOfOfflineNode));
        } else if (region.isMetaRegion()) {
          this.service.submit(new OpenMetaHandler(this, this, region, htd,
              versionOfOfflineNode));
        } else {
          this.service.submit(new OpenRegionHandler(this, this, region, htd,
              versionOfOfflineNode));
        }
        builder.addOpeningState(RegionOpeningState.OPENED);
      } catch (RegionAlreadyInTransitionException rie) {
        LOG.warn("Region is already in transition", rie);
        if (isBulkAssign) {
          builder.addOpeningState(RegionOpeningState.OPENED);
        } else {
          throw new ServiceException(rie);
        }
      } catch (IOException ie) {
        LOG.warn("Failed opening region " + region.getRegionNameAsString(), ie);
        if (isBulkAssign) {
          builder.addOpeningState(RegionOpeningState.FAILED_OPENING);
        } else {
          throw new ServiceException(ie);
        }
      }
    }
    return builder.build();

  }

  /**
   * Close a region on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public CloseRegionResponse closeRegion(final RpcController controller,
      final CloseRegionRequest request) throws ServiceException {
    int versionOfClosingNode = -1;
    if (request.hasVersionOfClosingNode()) {
      versionOfClosingNode = request.getVersionOfClosingNode();
    }
    boolean zk = request.getTransitionInZK();
    final ServerName sn = (request.hasDestinationServer() ?
      ProtobufUtil.toServerName(request.getDestinationServer()) : null);

    try {
      checkOpen();
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      CloseRegionResponse.Builder
        builder = CloseRegionResponse.newBuilder();
      LOG.info("Received close region: " + region.getRegionNameAsString() +
        ". Version of ZK closing node:" + versionOfClosingNode +
        ". Destination server:" + sn);
      HRegionInfo regionInfo = region.getRegionInfo();
      checkIfRegionInTransition(regionInfo, CLOSE);
      boolean closed = closeRegion(
        regionInfo, false, zk, versionOfClosingNode, sn);
      builder.setClosed(closed);
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Flush a region on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public FlushRegionResponse flushRegion(final RpcController controller,
      final FlushRegionRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      LOG.info("Flushing " + region.getRegionNameAsString());
      boolean shouldFlush = true;
      if (request.hasIfOlderThanTs()) {
        shouldFlush = region.getLastFlushTime() < request.getIfOlderThanTs();
      }
      FlushRegionResponse.Builder builder = FlushRegionResponse.newBuilder();
      if (shouldFlush) {
        builder.setFlushed(region.flushcache());
      }
      builder.setLastFlushTime(region.getLastFlushTime());
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Split a region on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public SplitRegionResponse splitRegion(final RpcController controller,
      final SplitRegionRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      LOG.info("Splitting " + region.getRegionNameAsString());
      region.flushcache();
      byte[] splitPoint = null;
      if (request.hasSplitPoint()) {
        splitPoint = request.getSplitPoint().toByteArray();
      }
      region.forceSplit(splitPoint);
      compactSplitThread.requestSplit(region, region.checkSplit());
      return SplitRegionResponse.newBuilder().build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Compact a region on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public CompactRegionResponse compactRegion(final RpcController controller,
      final CompactRegionRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.incrementAndGet();
      HRegion region = getRegion(request.getRegion());
      LOG.info("Compacting " + region.getRegionNameAsString());
      boolean major = false;
      if (request.hasMajor()) {
        major = request.getMajor();
      }
      if (major) {
        region.triggerMajorCompaction();
      }
      LOG.trace("User-triggered compaction requested for region " +
        region.getRegionNameAsString());
      compactSplitThread.requestCompaction(region,
        "User-triggered " + (major ? "major " : "") + "compaction",
          Store.PRIORITY_USER);
      return CompactRegionResponse.newBuilder().build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Replicate WAL entries on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HConstants.REPLICATION_QOS)
  public ReplicateWALEntryResponse replicateWALEntry(final RpcController controller,
      final ReplicateWALEntryRequest request) throws ServiceException {
    try {
      if (replicationSinkHandler != null) {
        checkOpen();
        requestCount.incrementAndGet();
        HLog.Entry[] entries = ProtobufUtil.toHLogEntries(request.getEntryList());
        if (entries != null && entries.length > 0) {
          replicationSinkHandler.replicateLogEntries(entries);
        }
      }
      return ReplicateWALEntryResponse.newBuilder().build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Roll the WAL writer of the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  public RollWALWriterResponse rollWALWriter(final RpcController controller,
      final RollWALWriterRequest request) throws ServiceException {
    try {
      requestCount.incrementAndGet();
      HLog wal = this.getWAL();
      byte[][] regionsToFlush = wal.rollWriter(true);
      RollWALWriterResponse.Builder builder = RollWALWriterResponse.newBuilder();
      if (regionsToFlush != null) {
        for (byte[] region: regionsToFlush) {
          builder.addRegionToFlush(ByteString.copyFrom(region));
        }
      }
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Stop the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  public StopServerResponse stopServer(final RpcController controller,
      final StopServerRequest request) throws ServiceException {
    requestCount.incrementAndGet();
    String reason = request.getReason();
    stop(reason);
    return StopServerResponse.newBuilder().build();
  }

  /**
   * Get some information of the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  public GetServerInfoResponse getServerInfo(final RpcController controller,
      final GetServerInfoRequest request) throws ServiceException {
    ServerName serverName = getServerName();
    requestCount.incrementAndGet();
    return ResponseConverter.buildGetServerInfoResponse(serverName, webuiport);
  }

// End Admin methods

  /**
   * Find the HRegion based on a region specifier
   *
   * @param regionSpecifier the region specifier
   * @return the corresponding region
   * @throws IOException if the specifier is not null,
   *    but failed to find the region
   */
  protected HRegion getRegion(
      final RegionSpecifier regionSpecifier) throws IOException {
    byte[] value = regionSpecifier.getValue().toByteArray();
    RegionSpecifierType type = regionSpecifier.getType();
    checkOpen();
    switch (type) {
      case REGION_NAME:
        return getRegion(value);
      case ENCODED_REGION_NAME:
        return getRegionByEncodedName(Bytes.toString(value));
      default:
        throw new DoNotRetryIOException(
          "Unsupported region specifier type: " + type);
    }
  }

  /**
   * Execute an append mutation.
   *
   * @param region
   * @param mutate
   * @return
   * @throws IOException
   */
  protected Result append(final HRegion region,
      final Mutate mutate) throws IOException {
    Append append = ProtobufUtil.toAppend(mutate);
    Result r = null;
    if (region.getCoprocessorHost() != null) {
      r = region.getCoprocessorHost().preAppend(append);
    }
    if (r == null) {
      Integer lock = getLockFromId(append.getLockId());
      r = region.append(append, lock, append.getWriteToWAL());
      if (region.getCoprocessorHost() != null) {
        region.getCoprocessorHost().postAppend(append, r);
      }
    }
    return r;
  }

  /**
   * Execute an increment mutation.
   *
   * @param region
   * @param mutate
   * @return
   * @throws IOException
   */
  protected Result increment(final HRegion region,
      final Mutate mutate) throws IOException {
    Increment increment = ProtobufUtil.toIncrement(mutate);
    Result r = null;
    if (region.getCoprocessorHost() != null) {
      r = region.getCoprocessorHost().preIncrement(increment);
    }
    if (r == null) {
      Integer lock = getLockFromId(increment.getLockId());
      r = region.increment(increment, lock, increment.getWriteToWAL());
      if (region.getCoprocessorHost() != null) {
        r = region.getCoprocessorHost().postIncrement(increment, r);
      }
    }
    return r;
  }

  /**
   * Execute a list of Put/Delete mutations.
   *
   * @param builder
   * @param region
   * @param mutates
   */
  protected void doBatchOp(final MultiResponse.Builder builder,
      final HRegion region, final List<Mutate> mutates) {
    @SuppressWarnings("unchecked")
    Pair<Mutation, Integer>[] mutationsWithLocks = new Pair[mutates.size()];

    try {
      ActionResult.Builder resultBuilder = ActionResult.newBuilder();
      NameBytesPair value = ProtobufUtil.toParameter(new Result());
      resultBuilder.setValue(value);
      ActionResult result = resultBuilder.build();

      int i = 0;
      for (Mutate m : mutates) {
        Mutation mutation = null;
        if (m.getMutateType() == MutateType.PUT) {
          mutation = ProtobufUtil.toPut(m);
        } else {
          mutation = ProtobufUtil.toDelete(m);
        }
        Integer lock = getLockFromId(mutation.getLockId());
        mutationsWithLocks[i++] = new Pair<Mutation, Integer>(mutation, lock);
        builder.addResult(result);
      }

      requestCount.addAndGet(mutates.size());
      if (!region.getRegionInfo().isMetaTable()) {
        cacheFlusher.reclaimMemStoreMemory();
      }

      OperationStatus codes[] = region.batchMutate(mutationsWithLocks);
      for (i = 0; i < codes.length; i++) {
        switch (codes[i].getOperationStatusCode()) {
          case BAD_FAMILY:
            result = ResponseConverter.buildActionResult(
                new NoSuchColumnFamilyException(codes[i].getExceptionMsg()));
            builder.setResult(i, result);
            break;

          case SANITY_CHECK_FAILURE:
            result = ResponseConverter.buildActionResult(
                new FailedSanityCheckException(codes[i].getExceptionMsg()));
            builder.setResult(i, result);
            break;

          default:
            result = ResponseConverter.buildActionResult(
                new DoNotRetryIOException(codes[i].getExceptionMsg()));
            builder.setResult(i, result);
            break;

          case SUCCESS:
            break;
        }
      }
    } catch (IOException ie) {
      ActionResult result = ResponseConverter.buildActionResult(ie);
      for (int i = 0, n = mutates.size(); i < n; i++) {
        builder.setResult(i, result);
      }
    }
  }

  /**
   * Mutate a list of rows atomically.
   *
   * @param region
   * @param mutates
   * @throws IOException
   */
  protected void mutateRows(final HRegion region,
      final List<Mutate> mutates) throws IOException {
    Mutate firstMutate = mutates.get(0);
    if (!region.getRegionInfo().isMetaTable()) {
      cacheFlusher.reclaimMemStoreMemory();
    }
    byte[] row = firstMutate.getRow().toByteArray();
    RowMutations rm = new RowMutations(row);
    for (Mutate mutate: mutates) {
      MutateType type = mutate.getMutateType();
      switch (mutate.getMutateType()) {
      case PUT:
        rm.add(ProtobufUtil.toPut(mutate));
        break;
      case DELETE:
        rm.add(ProtobufUtil.toDelete(mutate));
        break;
        default:
          throw new DoNotRetryIOException(
            "mutate supports atomic put and/or delete, not "
              + type.name());
      }
    }
    region.mutateRow(rm);
  }


  // This map will containsall the regions that we closed for a move.
  //  We add the time it was moved as we don't want to keep too old information
  protected Map<String, Pair<Long, ServerName>> movedRegions =
    new ConcurrentHashMap<String, Pair<Long, ServerName>>(3000);

  // We need a timeout. If not there is a risk of giving a wrong information: this would double
  //  the number of network calls instead of reducing them.
  private static final int TIMEOUT_REGION_MOVED = (2 * 60 * 1000);

  protected void addToMovedRegions(HRegionInfo hri, ServerName destination){
    addToMovedRegions(hri.getEncodedName(), destination);
  }

  protected void addToMovedRegions(String encodedName, ServerName destination){
    final  Long time = System.currentTimeMillis();

    movedRegions.put(
      encodedName,
      new Pair<Long, ServerName>(time, destination));
  }

  private ServerName getMovedRegion(final String encodedRegionName) {
    Pair<Long, ServerName> dest = movedRegions.get(encodedRegionName);

    if (dest != null) {
      if (dest.getFirst() > (System.currentTimeMillis() - TIMEOUT_REGION_MOVED)) {
        return dest.getSecond();
      } else {
        movedRegions.remove(encodedRegionName);
      }
    }

    return null;
  }

  /**
   * Remove the expired entries from the moved regions list.
   */
  protected void cleanMovedRegions(){
    final long cutOff = System.currentTimeMillis() - TIMEOUT_REGION_MOVED;
    Iterator<Entry<String, Pair<Long, ServerName>>> it = movedRegions.entrySet().iterator();

    while (it.hasNext()){
      Map.Entry<String, Pair<Long, ServerName>> e = it.next();
      if (e.getValue().getFirst() < cutOff){
        it.remove();
      }
    }
  }

  /**
   * Creates a Chore thread to clean the moved region cache.
   */
  protected static class MovedRegionsCleaner extends Chore implements Stoppable {
    private HRegionServer regionServer;
    Stoppable stoppable;

    private MovedRegionsCleaner(
      HRegionServer regionServer, Stoppable stoppable){
      super("MovedRegionsCleaner for region "+regionServer, TIMEOUT_REGION_MOVED, stoppable);
      this.regionServer = regionServer;
      this.stoppable = stoppable;
    }

    static MovedRegionsCleaner createAndStart(HRegionServer rs){
      Stoppable stoppable = new Stoppable() {
        private volatile boolean isStopped = false;
        @Override public void stop(String why) { isStopped = true;}
        @Override public boolean isStopped() {return isStopped;}
      };

      return new MovedRegionsCleaner(rs, stoppable);
    }

    @Override
    protected void chore() {
      regionServer.cleanMovedRegions();
    }

    @Override
    public void stop(String why) {
      stoppable.stop(why);
    }

    @Override
    public boolean isStopped() {
      return stoppable.isStopped();
    }
  }

  private String getMyEphemeralNodePath() {
    return ZKUtil.joinZNode(this.zooKeeper.rsZNode, getServerName().toString());
  }
}
