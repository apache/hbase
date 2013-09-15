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
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Constructor;
import java.net.BindException;
import java.net.InetSocketAddress;
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
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.ClockOutOfSyncException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HealthCheckChore;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.ZNodeClearer;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.exceptions.RegionOpeningException;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.ipc.HBaseRPCErrorHandler;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.master.SplitLogManager;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
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
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.MergeRegionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.MergeRegionsResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionRequest.RegionOpenInfo;
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
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.UpdateFavoredNodesRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.UpdateFavoredNodesResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ActionResult;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest.FamilyPath;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Condition;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiGetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiGetResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos.RegionLoad;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.Coprocessor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStatusService;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.ReportRSFatalErrorRequest;
import org.apache.hadoop.hbase.regionserver.HRegion.Operation;
import org.apache.hadoop.hbase.regionserver.Leases.LeaseStillHeldException;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.handler.CloseMetaHandler;
import org.apache.hadoop.hbase.regionserver.handler.CloseRegionHandler;
import org.apache.hadoop.hbase.regionserver.handler.OpenMetaHandler;
import org.apache.hadoop.hbase.regionserver.handler.OpenRegionHandler;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionServerSnapshotManager;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogFactory;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.trace.SpanReceiverHost;
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
import org.apache.hadoop.hbase.zookeeper.MetaRegionTracker;
import org.apache.hadoop.hbase.zookeeper.RecoveringRegionWatcher;
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
import org.apache.zookeeper.data.Stat;
import org.cliffc.high_scale_lib.Counter;

import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;

/**
 * HRegionServer makes a set of HRegions available to clients. It checks in with
 * the HMaster. There are many HRegionServers in a single HBase deployment.
 */
@InterfaceAudience.Private
@SuppressWarnings("deprecation")
public class HRegionServer implements ClientProtos.ClientService.BlockingInterface,
  AdminProtos.AdminService.BlockingInterface, Runnable, RegionServerServices,
  HBaseRPCErrorHandler, LastSequenceId {

  public static final Log LOG = LogFactory.getLog(HRegionServer.class);

  private final Random rand;

  /*
   * Strings to be used in forming the exception message for
   * RegionsAlreadyInTransitionException.
   */
  protected static final String OPEN = "OPEN";
  protected static final String CLOSE = "CLOSE";

  //RegionName vs current action in progress
  //true - if open region action in progress
  //false - if close region action in progress
  protected final ConcurrentMap<byte[], Boolean> regionsInTransitionInRS =
    new ConcurrentSkipListMap<byte[], Boolean>(Bytes.BYTES_COMPARATOR);

  protected long maxScannerResultSize;

  // Cache flushing
  protected MemStoreFlusher cacheFlusher;

  // catalog tracker
  protected CatalogTracker catalogTracker;

  // Watch if a region is out of recovering state from ZooKeeper
  @SuppressWarnings("unused")
  private RecoveringRegionWatcher recoveringRegionWatcher;

  /**
   * Go here to get table descriptors.
   */
  protected TableDescriptors tableDescriptors;

  // Replication services. If no replication, this handler will be null.
  protected ReplicationSourceService replicationSourceHandler;
  protected ReplicationSinkService replicationSinkHandler;

  // Compactions
  public CompactSplitThread compactSplitThread;

  final ConcurrentHashMap<String, RegionScannerHolder> scanners =
      new ConcurrentHashMap<String, RegionScannerHolder>();

  /**
   * Map of regions currently being served by this region server. Key is the
   * encoded region name.  All access should be synchronized.
   */
  protected final Map<String, HRegion> onlineRegions =
    new ConcurrentHashMap<String, HRegion>();

  /**
   * Map of encoded region names to the DataNode locations they should be hosted on
   * We store the value as InetSocketAddress since this is used only in HDFS
   * API (create() that takes favored nodes as hints for placing file blocks).
   * We could have used ServerName here as the value class, but we'd need to
   * convert it to InetSocketAddress at some point before the HDFS API call, and
   * it seems a bit weird to store ServerName since ServerName refers to RegionServers
   * and here we really mean DataNode locations.
   */
  protected final Map<String, InetSocketAddress[]> regionFavoredNodesMap =
      new ConcurrentHashMap<String, InetSocketAddress[]>();

  /**
   * Set of regions currently being in recovering state which means it can accept writes(edits from
   * previous failed region server) but not reads. A recovering region is also an online region.
   */
  protected final Map<String, HRegion> recoveringRegions = Collections
      .synchronizedMap(new HashMap<String, HRegion>());

  // Leases
  protected Leases leases;

  // Instance of the hbase executor service.
  protected ExecutorService service;

  // Request counter. (Includes requests that are not serviced by regions.)
  final Counter requestCount = new Counter();

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

  private boolean useHBaseChecksum; // verify hbase checksums?
  private Path rootDir;

  protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  final int numRetries;
  protected final int threadWakeFrequency;
  private final int msgInterval;

  protected final int numRegionsToReport;

  // Stub to do region server status calls against the master.
  private RegionServerStatusService.BlockingInterface rssStub;
  // RPC client. Used to make the stub above that does region server status checking.
  RpcClient rpcClient;

  // Server to handle client requests. Default access so can be accessed by
  // unit tests.
  RpcServerInterface rpcServer;

  private final InetSocketAddress isa;
  private UncaughtExceptionHandler uncaughtExceptionHandler;

  // Info server. Default access so can be used by unit tests. REGIONSERVER
  // is name of the webapp and the attribute name used stuffing this instance
  // into web context.
  InfoServer infoServer;

  /** region server process name */
  public static final String REGIONSERVER = "regionserver";

  /** region server configuration name */
  public static final String REGIONSERVER_CONF = "regionserver_conf";

  private MetricsRegionServer metricsRegionServer;
  private SpanReceiverHost spanReceiverHost;

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
  final CacheConfig cacheConfig;

  // reference to the Thrift Server.
  volatile private HRegionThriftServer thriftServer;

  /** The health check chore. */
  private HealthCheckChore healthCheckChore;

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
   * Unique identifier for the cluster we are a part of.
   */
  private String clusterId;

  /**
   * MX Bean for RegionServerInfo
   */
  private ObjectName mxBean = null;

  /**
   * Chore to clean periodically the moved region list
   */
  private MovedRegionsCleaner movedRegionsCleaner;

  /**
   * The lease timeout period for client scanners (milliseconds).
   */
  private final int scannerLeaseTimeoutPeriod;

  /**
   * The reference to the QosFunction
   */
  private final QosFunction qosFunction;

  private RegionServerCoprocessorHost rsHost;

  /** Handle all the snapshot requests to this server */
  RegionServerSnapshotManager snapshotManager;

  // configuration setting on if replay WAL edits directly to another RS
  private final boolean distributedLogReplay;

  // Table level lock manager for locking for region operations
  private TableLockManager tableLockManager;

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
    this.isOnline = false;
    checkCodecs(this.conf);

    // do we use checksum verification in the hbase? If hbase checksum verification
    // is enabled, then we automatically switch off hdfs checksum verification.
    this.useHBaseChecksum = conf.getBoolean(HConstants.HBASE_CHECKSUM_VERIFICATION, true);

    // check that the user has not set the "dfs.client.read.shortcircuit.skip.checksum" property.
    boolean shortCircuitSkipChecksum = conf.getBoolean(
        "dfs.client.read.shortcircuit.skip.checksum", false);
    if (shortCircuitSkipChecksum) {
      LOG.warn("Configuration \"dfs.client.read.shortcircuit.skip.checksum\" should not " +
          "be set to true." + (this.useHBaseChecksum ? " HBase checksum doesn't require " +
          "it, see https://issues.apache.org/jira/browse/HBASE-6868." : ""));
      assert !shortCircuitSkipChecksum; //this will fail if assertions are on
    }

    // Config'ed params
    this.numRetries = this.conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    this.threadWakeFrequency = conf.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
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

    this.scannerLeaseTimeoutPeriod = HBaseConfiguration.getInt(conf,
      HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
      HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,
      HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);

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
    String name = "regionserver/" + initialIsa.toString();
    // Set how many times to retry talking to another server over HConnection.
    HConnectionManager.setServerSideHConnectionRetries(this.conf, name, LOG);
    this.rpcServer = new RpcServer(this, name, getServices(),
      /*HBaseRPCErrorHandler.class, OnlineRegions.class},*/
      initialIsa, // BindAddress is IP we got for this server.
      conf.getInt("hbase.regionserver.handler.count", 10),
      conf.getInt("hbase.regionserver.metahandler.count", 10),
      conf, HConstants.QOS_THRESHOLD);

    // Set our address.
    this.isa = this.rpcServer.getListenerAddress();

    this.rpcServer.setErrorHandler(this);
    this.rpcServer.setQosFunction((qosFunction = new QosFunction(this)));
    this.startcode = System.currentTimeMillis();

    // login the zookeeper client principal (if using security)
    ZKUtil.loginClient(this.conf, "hbase.zookeeper.client.keytab.file",
      "hbase.zookeeper.client.kerberos.principal", this.isa.getHostName());

    // login the server principal (if using secure Hadoop)
    User.login(this.conf, "hbase.regionserver.keytab.file",
      "hbase.regionserver.kerberos.principal", this.isa.getHostName());
    regionServerAccounting = new RegionServerAccounting();
    cacheConfig = new CacheConfig(conf);
    uncaughtExceptionHandler = new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        abort("Uncaught exception in service thread " + t.getName(), e);
      }
    };
    this.rsHost = new RegionServerCoprocessorHost(this, this.conf);

    this.distributedLogReplay = this.conf.getBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY,
      HConstants.DEFAULT_DISTRIBUTED_LOG_REPLAY_CONFIG);
  }

  /**
   * @return list of blocking services and their security info classes that this server supports
   */
  private List<BlockingServiceAndInterface> getServices() {
    List<BlockingServiceAndInterface> bssi = new ArrayList<BlockingServiceAndInterface>(2);
    bssi.add(new BlockingServiceAndInterface(
        ClientProtos.ClientService.newReflectiveBlockingService(this),
        ClientProtos.ClientService.BlockingInterface.class));
    bssi.add(new BlockingServiceAndInterface(
        AdminProtos.AdminService.newReflectiveBlockingService(this),
        AdminProtos.AdminService.BlockingInterface.class));
    return bssi;
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

  String getClusterId() {
    return this.clusterId;
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
    RegionScannerHolder scannerHolder = scanners.get(scannerIdString);
    if (scannerHolder != null) {
      return scannerHolder.s;
    }
    return null;
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

    // Retrieve clusterId
    // Since cluster status is now up
    // ID should have already been set by HMaster
    try {
      clusterId = ZKClusterId.readClusterIdZNode(this.zooKeeper);
      if (clusterId == null) {
        this.abort("Cluster ID has not been set");
      }
      LOG.info("ClusterId : "+clusterId);
    } catch (KeeperException e) {
      this.abort("Failed to retrieve Cluster ID",e);
    }

    // watch for snapshots
    try {
      this.snapshotManager = new RegionServerSnapshotManager(this);
    } catch (KeeperException e) {
      this.abort("Failed to reach zk cluster when creating snapshot handler.");
    }
    this.tableLockManager = TableLockManager.createTableLockManager(conf, zooKeeper,
        new ServerName(isa.getHostName(), isa.getPort(), startcode));

    // register watcher for recovering regions
    if(this.distributedLogReplay) {
      this.recoveringRegionWatcher = new RecoveringRegionWatcher(this.zooKeeper, this);
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

    // Background thread to check for compactions; needed if region has not gotten updates
    // in a while. It will take care of not checking too frequently on store-by-store basis.
    this.compactionChecker = new CompactionChecker(this, this.threadWakeFrequency, this);
    this.periodicFlusher = new PeriodicMemstoreFlusher(this.threadWakeFrequency, this);
    // Health checker thread.
    int sleepTime = this.conf.getInt(HConstants.HEALTH_CHORE_WAKE_FREQ,
      HConstants.DEFAULT_THREAD_WAKE_FREQUENCY);
    if (isHealthCheckerConfigured()) {
      healthCheckChore = new HealthCheckChore(sleepTime, this, getConfiguration());
    }

    this.leases = new Leases(this.threadWakeFrequency);

    // Create the thread for the ThriftServer.
    if (conf.getBoolean("hbase.regionserver.export.thrift", false)) {
      thriftServer = new HRegionThriftServer(this, conf);
      thriftServer.start();
      LOG.info("Started Thrift API from Region Server.");
    }

    // Create the thread to clean the moved regions list
    movedRegionsCleaner = MovedRegionsCleaner.createAndStart(this);

    // Setup RPC client for master communication
    rpcClient = new RpcClient(conf, clusterId, new InetSocketAddress(
        this.isa.getAddress(), 0));
  }

  /**
   * The HRegionServer sticks in this loop until closed.
   */
  @Override
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

      if (!this.stopped && isHealthy()){
        // start the snapshot handler, since the server is ready to run
        this.snapshotManager.start();
      }

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
          tryRegionServerReport(lastMsg, now);
          lastMsg = System.currentTimeMillis();
        }
        if (!this.stopped) this.sleeper.sleep();
      } // for
    } catch (Throwable t) {
      if (!checkOOME(t)) {
        String prefix = t instanceof YouAreDeadException? "": "Unhandled: ";
        abort(prefix + t.getMessage(), t);
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
    // handlers are stuck waiting on meta.
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
    if (this.fsOk) {
      closeWAL(!abortRequested);
    }

    // Make sure the proxy is down.
    if (this.rssStub != null) {
      this.rssStub = null;
    }
    this.rpcClient.stop();
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
    return onlineRegions.containsKey(HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
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
    if (this.rssStub == null) {
      // the current server is stopping.
      return;
    }
    ClusterStatusProtos.ServerLoad sl = buildServerLoad(reportStartTime, reportEndTime);
    try {
      RegionServerReportRequest.Builder request = RegionServerReportRequest.newBuilder();
      ServerName sn = ServerName.parseVersionedServerName(
        this.serverNameFromMasterPOV.getVersionedBytes());
      request.setServer(ProtobufUtil.toServerName(sn));
      request.setLoad(sl);
      this.rssStub.regionServerReport(null, request.build());
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof YouAreDeadException) {
        // This will be caught and handled as a fatal error in run()
        throw ioe;
      }
      // Couldn't connect to the master, get location from zk and reconnect
      // Method blocks until new master is found or we are stopped
      Pair<ServerName, RegionServerStatusService.BlockingInterface> p =
        createRegionServerStatusStub();
      this.rssStub = p.getSecond();
    }
  }

  ClusterStatusProtos.ServerLoad buildServerLoad(long reportStartTime, long reportEndTime) {
    // We're getting the MetricsRegionServerWrapper here because the wrapper computes requests
    // per second, and other metrics  As long as metrics are part of ServerLoad it's best to use
    // the wrapper to compute those numbers in one place.
    // In the long term most of these should be moved off of ServerLoad and the heart beat.
    // Instead they should be stored in an HBase table so that external visibility into HBase is
    // improved; Additionally the load balancer will be able to take advantage of a more complete
    // history.
    MetricsRegionServerWrapper regionServerWrapper = this.metricsRegionServer.getRegionServerWrapper();
    Collection<HRegion> regions = getOnlineRegionsLocalContext();
    MemoryUsage memory =
      ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();

    ClusterStatusProtos.ServerLoad.Builder serverLoad =
      ClusterStatusProtos.ServerLoad.newBuilder();
    serverLoad.setNumberOfRequests((int) regionServerWrapper.getRequestsPerSecond());
    serverLoad.setTotalNumberOfRequests((int) regionServerWrapper.getTotalRequestCount());
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
    if (this.infoServer != null) {
      serverLoad.setInfoServerPort(this.infoServer.getPort());
    } else {
      serverLoad.setInfoServerPort(-1);
    }
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
          closeRegionIgnoreErrors(hri, abort);
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
    if (this.hlogForMeta != null) {
      // All hlogs (meta and non-meta) are in the same directory. Don't call
      // closeAndDelete here since that would delete all hlogs not just the
      // meta ones. We will just 'close' the hlog for meta here, and leave
      // the directory cleanup to the follow-on closeAndDelete call.
      try {
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
        e.getValue().s.close();
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
          if (!hostnameFromMasterPOV.equals(this.isa.getHostName())) {
            LOG.info("Master passed us a different hostname to use; was=" +
              this.isa.getHostName() + ", but now=" + hostnameFromMasterPOV);
          }
          continue;
        }
        String value = e.getValue();
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
      FSUtils.setFsDefault(this.conf, FSUtils.getRootDir(this.conf));
      // Get fs instance used by this RS
      this.fs = new HFileSystem(this.conf, this.useHBaseChecksum);
      this.rootDir = FSUtils.getRootDir(this.conf);
      this.tableDescriptors = new FSTableDescriptors(this.fs, this.rootDir, true);
      this.hlog = setupWALAndReplication();
      // Init in here rather than in constructor after thread name has been set
      this.metricsRegionServer = new MetricsRegionServer(new MetricsRegionServerWrapperImpl(this));

      spanReceiverHost = SpanReceiverHost.getInstance(getConfiguration());

      startServiceThreads();
      LOG.info("Serving as " + this.serverNameFromMasterPOV +
        ", RpcServer on " + this.isa +
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

  @Override
  public RegionServerAccounting getRegionServerAccounting() {
    return regionServerAccounting;
  }

  @Override
  public TableLockManager getTableLockManager() {
    return tableLockManager;
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
    private long iteration = 0;

    CompactionChecker(final HRegionServer h, final int sleepTime,
        final Stoppable stopper) {
      super("CompactionChecker", sleepTime, h);
      this.instance = h;
      LOG.info(this.getName() + " runs every " + StringUtils.formatTime(sleepTime));

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
            long multiplier = s.getCompactionCheckMultiplier();
            assert multiplier > 0;
            if (iteration % multiplier != 0) continue;
            if (s.needsCompaction()) {
              // Queue a compaction. Will recognize if major is needed.
              this.instance.compactSplitThread.requestSystemCompaction(r, s, getName()
                  + " requests compaction");
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
      iteration = (iteration == Long.MAX_VALUE) ? 0 : (iteration + 1);
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
    final String logName
      = HLogUtil.getHLogDirectoryName(this.serverNameFromMasterPOV.toString());

    Path logdir = new Path(rootDir, logName);
    if (LOG.isDebugEnabled()) LOG.debug("logdir=" + logdir);
    if (this.fs.exists(logdir)) {
      throw new RegionServerRunningException("Region server has already " +
        "created directory at " + this.serverNameFromMasterPOV.toString());
    }

    // Instantiate replication manager if replication enabled.  Pass it the
    // log directories.
    createNewReplicationInstance(conf, this, this.fs, logdir, oldLogDir);

    return instantiateHLog(rootDir, logName);
  }

  private HLog getMetaWAL() throws IOException {
    if (this.hlogForMeta != null) return this.hlogForMeta;
    final String logName = HLogUtil.getHLogDirectoryName(this.serverNameFromMasterPOV.toString());
    Path logdir = new Path(rootDir, logName);
    if (LOG.isDebugEnabled()) LOG.debug("logdir=" + logdir);
    this.hlogForMeta = HLogFactory.createMetaHLog(this.fs.getBackingFs(), rootDir, logName,
      this.conf, getMetaWALActionListeners(), this.serverNameFromMasterPOV.toString());
    return this.hlogForMeta;
  }

  /**
   * Called by {@link #setupWALAndReplication()} creating WAL instance.
   * @param rootdir
   * @param logName
   * @return WAL instance.
   * @throws IOException
   */
  protected HLog instantiateHLog(Path rootdir, String logName) throws IOException {
    return HLogFactory.createHLog(this.fs.getBackingFs(), rootdir, logName, this.conf,
      getWALActionListeners(), this.serverNameFromMasterPOV.toString());
  }

  /**
   * Called by {@link #instantiateHLog(Path, String)} setting up WAL instance.
   * Add any {@link WALActionsListener}s you want inserted before WAL startup.
   * @return List of WALActionsListener that will be passed in to
   * {@link org.apache.hadoop.hbase.regionserver.wal.FSHLog} on construction.
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
    // null
    MetaLogRoller tmpLogRoller = new MetaLogRoller(this, this);
    String n = Thread.currentThread().getName();
    Threads.setDaemonThreadRunning(tmpLogRoller.getThread(),
        n + "-MetaLogRoller", uncaughtExceptionHandler);
    this.metaHLogRoller = tmpLogRoller;
    tmpLogRoller = null;
    listeners.add(this.metaHLogRoller);
    return listeners;
  }

  protected LogRoller getLogRoller() {
    return hlogRoller;
  }

  public MetricsRegionServer getMetrics() {
    return this.metricsRegionServer;
  }

  /**
   * @return Master address tracker instance.
   */
  public MasterAddressTracker getMasterAddressManager() {
    return this.masterAddressManager;
  }

  /*
   * Start maintenance Threads, Server, Worker and lease checker threads.
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
    this.service = new ExecutorService(getServerName().toShortString());
    this.service.startExecutorService(ExecutorType.RS_OPEN_REGION,
      conf.getInt("hbase.regionserver.executor.openregion.threads", 3));
    this.service.startExecutorService(ExecutorType.RS_OPEN_META,
      conf.getInt("hbase.regionserver.executor.openmeta.threads", 1));
    this.service.startExecutorService(ExecutorType.RS_CLOSE_REGION,
      conf.getInt("hbase.regionserver.executor.closeregion.threads", 3));
    this.service.startExecutorService(ExecutorType.RS_CLOSE_META,
      conf.getInt("hbase.regionserver.executor.closemeta.threads", 1));
    if (conf.getBoolean(StoreScanner.STORESCANNER_PARALLEL_SEEK_ENABLE, false)) {
      this.service.startExecutorService(ExecutorType.RS_PARALLEL_SEEK,
        conf.getInt("hbase.storescanner.parallel.seek.threads", 10));
    }

    Threads.setDaemonThreadRunning(this.hlogRoller.getThread(), n + ".logRoller",
        uncaughtExceptionHandler);
    this.cacheFlusher.start(uncaughtExceptionHandler);
    Threads.setDaemonThreadRunning(this.compactionChecker.getThread(), n +
      ".compactionChecker", uncaughtExceptionHandler);
    Threads.setDaemonThreadRunning(this.periodicFlusher.getThread(), n +
        ".periodicFlusher", uncaughtExceptionHandler);
    if (this.healthCheckChore != null) {
    Threads
        .setDaemonThreadRunning(this.healthCheckChore.getThread(), n + ".healthChecker",
            uncaughtExceptionHandler);
    }

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
    // set a smaller retries to fast fail otherwise splitlogworker could be blocked for
    // quite a while inside HConnection layer. The worker won't be available for other
    // tasks even after current task is preempted after a split task times out.
    Configuration sinkConf = HBaseConfiguration.create(conf);
    sinkConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      conf.getInt("hbase.log.replay.retries.number", 8)); // 8 retries take about 23 seconds
    sinkConf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
      conf.getInt("hbase.log.replay.rpc.timeout", 30000)); // default 30 seconds
    sinkConf.setInt("hbase.client.serverside.retries.multiplier", 1);
    this.splitLogWorker = new SplitLogWorker(this.zooKeeper, sinkConf, this, this);
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
          LOG.error("Failed binding http info server to port: " + port);
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
        && this.compactionChecker.isAlive())
        && this.periodicFlusher.isAlive()) {
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
    //_ROOT_ and hbase:meta regions have separate WAL.
    if (regionInfo != null && regionInfo.isMetaTable()) {
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
    try {
      this.rsHost.preStop(msg);
      this.stopped = true;
      LOG.info("STOPPED: " + msg);
      // Wakes run() if it is sleeping
      sleeper.skipSleepCycle();
    } catch (IOException exp) {
      LOG.warn("The region server did not stop", exp);
    }
  }

  public void waitForServerOnline(){
    while (!isOnline() && !isStopped()){
       sleeper.sleep();
    }
  }

  @Override
  public void postOpenDeployTasks(final HRegion r, final CatalogTracker ct)
  throws KeeperException, IOException {
    checkOpen();
    LOG.info("Post open deploy tasks for region=" + r.getRegionNameAsString());
    // Do checks to see if we need to compact (references or too many files)
    for (Store s : r.getStores().values()) {
      if (s.hasReferences() || s.needsCompaction()) {
       this.compactSplitThread.requestSystemCompaction(r, s, "Opening Region");
      }
    }
    long openSeqNum = r.getOpenSeqNum();
    if (openSeqNum == HConstants.NO_SEQNUM) {
      // If we opened a region, we should have read some sequence number from it.
      LOG.error("No sequence number found when opening " + r.getRegionNameAsString());
      openSeqNum = 0;
    }

    // Update flushed sequence id of a recovering region in ZK
    updateRecoveringRegionLastFlushedSequenceId(r);

    // Update ZK, or META
    if (r.getRegionInfo().isMetaRegion()) {
      MetaRegionTracker.setMetaLocation(getZooKeeper(),
          this.serverNameFromMasterPOV);
    } else {
      MetaEditor.updateRegionLocation(ct, r.getRegionInfo(),
        this.serverNameFromMasterPOV, openSeqNum);
    }
    LOG.info("Finished post open deploy task for " + r.getRegionNameAsString());

  }

  @Override
  public RpcServerInterface getRpcServer() {
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
  @Override
  public void abort(String reason, Throwable cause) {
    String msg = "ABORTING region server " + this + ": " + reason;
    if (cause != null) {
      LOG.fatal(msg, cause);
    } else {
      LOG.fatal(msg);
    }
    this.abortRequested = true;
    // HBASE-4014: show list of coprocessors that were loaded to help debug
    // regionserver crashes.Note that we're implicitly using
    // java.util.HashSet's toString() method to print the coprocessor names.
    LOG.fatal("RegionServer abort: loaded coprocessors are: " +
        CoprocessorHost.getLoadedCoprocessors());
    // Do our best to report our abort to the master, but this may not work
    try {
      if (cause != null) {
        msg += "\nCause:\n" + StringUtils.stringifyException(cause);
      }
      // Report to the master but only if we have already registered with the master.
      if (rssStub != null && this.serverNameFromMasterPOV != null) {
        ReportRSFatalErrorRequest.Builder builder =
          ReportRSFatalErrorRequest.newBuilder();
        ServerName sn =
          ServerName.parseVersionedServerName(this.serverNameFromMasterPOV.getVersionedBytes());
        builder.setServer(ProtobufUtil.toServerName(sn));
        builder.setErrorMessage(msg);
        rssStub.reportRSFatalError(null, builder.build());
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

  @Override
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
    this.cacheFlusher.join();
    if (this.healthCheckChore != null) {
      Threads.shutdown(this.healthCheckChore.getThread());
    }
    if (this.spanReceiverHost != null) {
      this.spanReceiverHost.closeReceivers();
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
  private Pair<ServerName, RegionServerStatusService.BlockingInterface>
  createRegionServerStatusStub() {
    ServerName sn = null;
    long previousLogTime = 0;
    RegionServerStatusService.BlockingInterface master = null;
    boolean refresh = false; // for the first time, use cached data
    RegionServerStatusService.BlockingInterface intf = null;
    while (keepLooping() && master == null) {
      sn = this.masterAddressManager.getMasterAddress(refresh);
      if (sn == null) {
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

      new InetSocketAddress(sn.getHostname(), sn.getPort());
      try {
        BlockingRpcChannel channel = this.rpcClient.createBlockingRpcChannel(sn,
            User.getCurrent(), this.rpcTimeout);
        intf = RegionServerStatusService.newBlockingStub(channel);
        break;
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
    return new Pair<ServerName, RegionServerStatusService.BlockingInterface>(sn, intf);
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
    Pair<ServerName, RegionServerStatusService.BlockingInterface> p =
      createRegionServerStatusStub();
    this.rssStub = p.getSecond();
    ServerName masterServerName = p.getFirst();
    if (masterServerName == null) return result;
    try {
      this.requestCount.set(0);
      LOG.info("reportForDuty to master=" + masterServerName + " with port=" + this.isa.getPort() +
        ", startcode=" + this.startcode);
      long now = EnvironmentEdgeManager.currentTimeMillis();
      int port = this.isa.getPort();
      RegionServerStartupRequest.Builder request = RegionServerStartupRequest.newBuilder();
      request.setPort(port);
      request.setServerStartCode(this.startcode);
      request.setServerCurrentTime(now);
      result = this.rssStub.regionServerStartup(null, request.build());
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof ClockOutOfSyncException) {
        LOG.fatal("Master rejected startup because clock is out of sync", ioe);
        // Re-throw IOE will cause RS to abort
        throw ioe;
      } else if (ioe instanceof ServerNotRunningYetException) {
        LOG.debug("Master is not running yet");
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
      GetLastFlushedSequenceIdRequest req = RequestConverter
          .buildGetLastFlushedSequenceIdRequest(region);
      lastFlushedSequenceId = rssStub.getLastFlushedSequenceId(null, req)
          .getLastFlushedSequenceId();
    } catch (ServiceException e) {
      lastFlushedSequenceId = -1l;
      LOG.warn("Unable to connect to the master to check " + "the last flushed sequence id", e);
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
   * Close meta region if we carry it
   * @param abort Whether we're running an abort.
   */
  void closeMetaTableRegions(final boolean abort) {
    HRegion meta = null;
    this.lock.writeLock().lock();
    try {
      for (Map.Entry<String, HRegion> e: onlineRegions.entrySet()) {
        HRegionInfo hri = e.getValue().getRegionInfo();
        if (hri.isMetaRegion()) {
          meta = e.getValue();
        }
        if (meta != null) break;
      }
    } finally {
      this.lock.writeLock().unlock();
    }
    if (meta != null) closeRegionIgnoreErrors(meta.getRegionInfo(), abort);
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
          closeRegionIgnoreErrors(r.getRegionInfo(), abort);
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
  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  @Override
  public boolean isStopping() {
    return this.stopping;
  }

  @Override
  public Map<String, HRegion> getRecoveringRegions() {
    return this.recoveringRegions;
  }

  /**
   *
   * @return the configuration
   */
  @Override
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
   * For tests, web ui and metrics.
   * This method will only work if HRegionServer is in the same JVM as client;
   * HRegion cannot be serialized to cross an rpc.
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
   * @return A new Map of online regions sorted by region size with the first entry being the
   * biggest.  If two regions are the same size, then the last one found wins; i.e. this method
   * may NOT return all regions.
   */
  SortedMap<Long, HRegion> getCopyOfOnlineRegionsSortedBySize() {
    // we'll sort the regions in reverse
    SortedMap<Long, HRegion> sortedRegions = new TreeMap<Long, HRegion>(
        new Comparator<Long>() {
          @Override
          public int compare(Long a, Long b) {
            return -1 * a.compareTo(b);
          }
        });
    // Copy over all regions. Regions are sorted by size with biggest first.
    for (HRegion region : this.onlineRegions.values()) {
      sortedRegions.put(region.memstoreSize.get(), region);
    }
    return sortedRegions;
  }

  /**
   * @return time stamp in millis of when this region server was started
   */
  public long getStartcode() {
    return this.startcode;
  }

  /** @return reference to FlushRequester */
  @Override
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
  @Override
  public FileSystem getFileSystem() {
    return fs;
  }

  @Override
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
  public ConcurrentMap<byte[], Boolean> getRegionsInTransitionInRS() {
    return this.regionsInTransitionInRS;
  }

  @Override
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
    } else {
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
      throw new IOException("Could not find class for " + classname);
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
   * This method looks at the in-memory onlineRegions.  It does not go to <code>hbase:meta</code>.
   * Only returns <em>online</em> regions.  If a region on this table has been
   * closed during a disable, etc., it will not be included in the returned list.
   * So, the returned list may not necessarily be ALL regions in this table, its
   * all the ONLINE regions in the table.
   * @param tableName
   * @return Online regions from <code>tableName</code>
   */
  @Override
  public List<HRegion> getOnlineRegions(TableName tableName) {
     List<HRegion> tableRegions = new ArrayList<HRegion>();
     synchronized (this.onlineRegions) {
       for (HRegion region: this.onlineRegions.values()) {
         HRegionInfo regionInfo = region.getRegionInfo();
         if(regionInfo.getTable().equals(tableName)) {
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
    return coprocessors.toArray(new String[coprocessors.size()]);
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

    @Override
    public void leaseExpired() {
      RegionScannerHolder rsh = scanners.remove(this.scannerName);
      if (rsh != null) {
        RegionScanner s = rsh.s;
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


  /**
   * Try to close the region, logs a warning on failure but continues.
   * @param region Region to close
   */
  private void closeRegionIgnoreErrors(HRegionInfo region, final boolean abort) {
    try {
      if (!closeRegion(region.getEncodedName(), abort, false, -1, null)) {
        LOG.warn("Failed to close " + region.getRegionNameAsString() +
            " - ignoring and continuing");
      }
    } catch (IOException e) {
      LOG.warn("Failed to close " + region.getRegionNameAsString() +
          " - ignoring and continuing", e);
    }
  }

  /**
   * Close asynchronously a region, can be called from the master or internally by the regionserver
   * when stopping. If called from the master, the region will update the znode status.
   *
   * <p>
   * If an opening was in progress, this method will cancel it, but will not start a new close. The
   * coprocessors are not called in this case. A NotServingRegionException exception is thrown.
   * </p>

   * <p>
   *   If a close was in progress, this new request will be ignored, and an exception thrown.
   * </p>
   *
   * @param encodedName Region to close
   * @param abort True if we are aborting
   * @param zk True if we are to update zk about the region close; if the close
   * was orchestrated by master, then update zk.  If the close is being run by
   * the regionserver because its going down, don't update zk.
   * @param versionOfClosingNode the version of znode to compare when RS transitions the znode from
   *   CLOSING state.
   * @return True if closed a region.
   * @throws NotServingRegionException if the region is not online
   * @throws RegionAlreadyInTransitionException if the region is already closing
   */
  protected boolean closeRegion(String encodedName, final boolean abort,
      final boolean zk, final int versionOfClosingNode, final ServerName sn)
      throws NotServingRegionException, RegionAlreadyInTransitionException {
    //Check for permissions to close.
    HRegion actualRegion = this.getFromOnlineRegions(encodedName);
    if ((actualRegion != null) && (actualRegion.getCoprocessorHost() != null)) {
      try {
        actualRegion.getCoprocessorHost().preClose(false);
      } catch (IOException exp) {
        LOG.warn("Unable to close region: the coprocessor launched an error ", exp);
        return false;
      }
    }

    final Boolean previous = this.regionsInTransitionInRS.putIfAbsent(encodedName.getBytes(),
        Boolean.FALSE);

    if (Boolean.TRUE.equals(previous)) {
      LOG.info("Received CLOSE for the region:" + encodedName + " , which we are already " +
          "trying to OPEN. Cancelling OPENING.");
      if (!regionsInTransitionInRS.replace(encodedName.getBytes(), previous, Boolean.FALSE)){
        // The replace failed. That should be an exceptional case, but theoretically it can happen.
        // We're going to try to do a standard close then.
        LOG.warn("The opening for region " + encodedName + " was done before we could cancel it." +
            " Doing a standard close now");
        return closeRegion(encodedName, abort, zk, versionOfClosingNode, sn);
      }
      // Let's get the region from the online region list again
      actualRegion = this.getFromOnlineRegions(encodedName);
      if (actualRegion == null) { // If already online, we still need to close it.
        LOG.info("The opening previously in progress has been cancelled by a CLOSE request.");
        // The master deletes the znode when it receives this exception.
        throw new NotServingRegionException("The region " + encodedName +
          " was opening but not yet served. Opening is cancelled.");
      }
    } else if (Boolean.FALSE.equals(previous)) {
      LOG.info("Received CLOSE for the region: " + encodedName +
        " ,which we are already trying to CLOSE, but not completed yet");
      // The master will retry till the region is closed. We need to do this since
      // the region could fail to close somehow. If we mark the region closed in master
      // while it is not, there could be data loss.
      // If the region stuck in closing for a while, and master runs out of retries,
      // master will move the region to failed_to_close. Later on, if the region
      // is indeed closed, master can properly re-assign it.
      throw new RegionAlreadyInTransitionException("The region " + encodedName +
        " was already closing. New CLOSE request is ignored.");
    }

    if (actualRegion == null) {
      LOG.error("Received CLOSE for a region which is not online, and we're not opening.");
      this.regionsInTransitionInRS.remove(encodedName.getBytes());
      // The master deletes the znode when it receives this exception.
      throw new NotServingRegionException("The region " + encodedName +
          " is not online, and is not opening.");
    }

    CloseRegionHandler crh;
    final HRegionInfo hri = actualRegion.getRegionInfo();
    if (hri.isMetaRegion()) {
      crh = new CloseMetaHandler(this, this, hri, abort, zk, versionOfClosingNode);
    } else {
      crh = new CloseRegionHandler(this, this, hri, abort, zk, versionOfClosingNode, sn);
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

  public InetSocketAddress[] getRegionBlockLocations(final String encodedRegionName) {
    return this.regionFavoredNodesMap.get(encodedRegionName);
  }

  @Override
  public HRegion getFromOnlineRegions(final String encodedRegionName) {
    return this.onlineRegions.get(encodedRegionName);
  }


  @Override
  public boolean removeFromOnlineRegions(final HRegion r, ServerName destination) {
    HRegion toReturn = this.onlineRegions.remove(r.getRegionInfo().getEncodedName());

    if (destination != null) {
      HLog wal = getWAL();
      long closeSeqNum = wal.getEarliestMemstoreSeqNum(r.getRegionInfo().getEncodedNameAsBytes());
      if (closeSeqNum == HConstants.NO_SEQNUM) {
        // No edits in WAL for this region; get the sequence number when the region was opened.
        closeSeqNum = r.getOpenSeqNum();
        if (closeSeqNum == HConstants.NO_SEQNUM) {
          closeSeqNum = 0;
        }
      }
      addToMovedRegions(r.getRegionInfo().getEncodedName(), destination, closeSeqNum);
    }
    this.regionFavoredNodesMap.remove(r.getRegionInfo().getEncodedName());
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
      MovedRegionInfo moveInfo = getMovedRegion(encodedRegionName);
      if (moveInfo != null) {
        throw new RegionMovedException(moveInfo.getServerName(), moveInfo.getSeqNum());
      }
      Boolean isOpening = this.regionsInTransitionInRS.get(Bytes.toBytes(encodedRegionName));
      if (isOpening != null && isOpening.booleanValue()) {
        throw new RegionOpeningException("Region is being opened: " + encodedRegionName);
      }
      throw new NotServingRegionException("Region is not online: " + encodedRegionName);
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
      LOG.debug("NotServingRegionException; " + t.getMessage());
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
  @Override
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

  protected long addScanner(RegionScanner s) throws LeaseStillHeldException {
    long scannerId = -1;
    while (true) {
      scannerId = rand.nextLong();
      if (scannerId == -1) continue;
      String scannerName = String.valueOf(scannerId);
      RegionScannerHolder existing = scanners.putIfAbsent(scannerName, new RegionScannerHolder(s));
      if (existing == null) {
        this.leases.createLease(scannerName, this.scannerLeaseTimeoutPeriod,
            new ScannerListener(scannerName));
        break;
      }
    }
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
    long before = EnvironmentEdgeManager.currentTimeMillis();
    try {
      requestCount.increment();
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
          r = region.get(clientGet);
          if (request.getExistenceOnly()) {
            boolean exists = r != null && !r.isEmpty();
            if (region.getCoprocessorHost() != null) {
              exists = region.getCoprocessorHost().postExists(clientGet, exists);
            }
            existence = exists;
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
    } finally {
      metricsRegionServer.updateGet(EnvironmentEdgeManager.currentTimeMillis() - before);
    }
  }

  /**
   * Get multi data from a table.
   *
   * @param controller the RPC controller
   * @param request multi-the get request
   * @throws ServiceException
   */
  @Override
  public MultiGetResponse multiGet(final RpcController controller, final MultiGetRequest request)
      throws ServiceException {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    try {
      requestCount.add(request.getGetCount());
      HRegion region = getRegion(request.getRegion());
      MultiGetResponse.Builder builder = MultiGetResponse.newBuilder();
      for (ClientProtos.Get get: request.getGetList())
      {
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
            r = region.get(clientGet);
            if (request.getExistenceOnly()) {
              boolean exists = r != null && !r.isEmpty();
              if (region.getCoprocessorHost() != null) {
                exists = region.getCoprocessorHost().postExists(clientGet, exists);
              }
              existence = exists;
            }
          }
        }
        if (existence != null) {
          builder.addExists(existence.booleanValue());
        } else if (r != null) {
          builder.addResult(ProtobufUtil.toResult(r));
        }
      }
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    } finally {
      metricsRegionServer.updateGet(EnvironmentEdgeManager.currentTimeMillis() - before);
    }
  }

  /**
   * Mutate data in a table.
   *
   * @param rpcc the RPC controller
   * @param request the mutate request
   * @throws ServiceException
   */
  @Override
  public MutateResponse mutate(final RpcController rpcc,
      final MutateRequest request) throws ServiceException {
    // rpc controller is how we bring in data via the back door;  it is unprotobuf'ed data.
    // It is also the conduit via which we pass back data.
    PayloadCarryingRpcController controller = (PayloadCarryingRpcController)rpcc;
    CellScanner cellScanner = controller != null? controller.cellScanner(): null;
    // Clear scanner so we are not holding on to reference across call.
    controller.setCellScanner(null);
    try {
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      MutateResponse.Builder builder = MutateResponse.newBuilder();
      MutationProto mutation = request.getMutation();
      if (!region.getRegionInfo().isMetaTable()) {
        cacheFlusher.reclaimMemStoreMemory();
      }
      Result r = null;
      Boolean processed = null;
      MutationType type = mutation.getMutateType();
      switch (type) {
      case APPEND:
        r = append(region, mutation, cellScanner);
        break;
      case INCREMENT:
        r = increment(region, mutation, cellScanner);
        break;
      case PUT:
        Put put = ProtobufUtil.toPut(mutation, cellScanner);
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
              qualifier, compareOp, comparator, put, true);
            if (region.getCoprocessorHost() != null) {
              result = region.getCoprocessorHost().postCheckAndPut(row, family,
                qualifier, compareOp, comparator, put, result);
            }
            processed = result;
          }
        } else {
          region.put(put);
          processed = Boolean.TRUE;
        }
        break;
      case DELETE:
        Delete delete = ProtobufUtil.toDelete(mutation, cellScanner);
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
              qualifier, compareOp, comparator, delete, true);
            if (region.getCoprocessorHost() != null) {
              result = region.getCoprocessorHost().postCheckAndDelete(row, family,
                qualifier, compareOp, comparator, delete, result);
            }
            processed = result;
          }
        } else {
          region.delete(delete);
          processed = Boolean.TRUE;
        }
        break;
        default:
          throw new DoNotRetryIOException(
            "Unsupported mutate type: " + type.name());
      }
      CellScannable cellsToReturn = null;
      if (processed != null) {
        builder.setProcessed(processed.booleanValue());
      } else if (r != null) {
        builder.setResult(ProtobufUtil.toResultNoData(r));
        cellsToReturn = r;
      }
      if (cellsToReturn != null) {
        controller.setCellScanner(cellsToReturn.cellScanner());
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
  public ScanResponse scan(final RpcController controller, final ScanRequest request)
  throws ServiceException {
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
      requestCount.increment();

      try {
        int ttl = 0;
        HRegion region = null;
        RegionScanner scanner = null;
        RegionScannerHolder rsh = null;
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
          rsh = scanners.get(scannerName);
          if (rsh == null) {
            LOG.info("Client tried to access missing scanner " + scannerName);
            throw new UnknownScannerException(
              "Name: " + scannerName + ", already closed?");
          }
          scanner = rsh.s;
          region = getRegion(scanner.getRegionInfo().getRegionName());
        } else {
          region = getRegion(request.getRegion());
          ClientProtos.Scan protoScan = request.getScan();
          boolean isLoadingCfsOnDemandSet = protoScan.hasLoadColumnFamiliesOnDemand();
          Scan scan = ProtobufUtil.toScan(protoScan);
          // if the request doesn't set this, get the default region setting.
          if (!isLoadingCfsOnDemandSet) {
            scan.setLoadColumnFamiliesOnDemand(region.isLoadingCfsOnDemandDefault());
          }
          scan.getAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE);
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
          // if nextCallSeq does not match throw Exception straight away. This needs to be
          // performed even before checking of Lease.
          // See HBASE-5974
          if (request.hasNextCallSeq()) {
            if (rsh == null) {
              rsh = scanners.get(scannerName);
            }
            if (rsh != null) {
              if (request.getNextCallSeq() != rsh.nextCallSeq) {
                throw new OutOfOrderScannerNextException("Expected nextCallSeq: " + rsh.nextCallSeq
                  + " But the nextCallSeq got from client: " + request.getNextCallSeq() +
                  "; request=" + TextFormat.shortDebugString(request));
              }
              // Increment the nextCallSeq value which is the next expected from client.
              rsh.nextCallSeq++;
            }
          }
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
                  if (maxScannerResultSize < Long.MAX_VALUE){
                    for (Cell kv : r.rawCells()) {
                      // TODO
                      currentScanResultSize += KeyValueUtil.ensureKeyValue(kv).heapSize();
                    }
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
              List<Cell> values = new ArrayList<Cell>();
              MultiVersionConsistencyControl.setThreadReadPoint(scanner.getMvccReadPoint());
              region.startRegionOperation(Operation.SCAN);
              try {
                int i = 0;
                synchronized(scanner) {
                  for (; i < rows
                      && currentScanResultSize < maxResultSize; i++) {
                    // Collect values to be returned here
                    boolean moreRows = scanner.nextRaw(values);
                    if (!values.isEmpty()) {
                      if (maxScannerResultSize < Long.MAX_VALUE){
                        for (Cell kv : values) {
                          currentScanResultSize += KeyValueUtil.ensureKeyValue(kv).heapSize();
                        }
                      }
                      results.add(new Result(values));
                    }
                    if (!moreRows) {
                      break;
                    }
                    values.clear();
                  }
                }
                region.readRequestsCount.add(i);
              } finally {
                region.closeRegionOperation();
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
              formatResults(builder, results, controller);
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
          rsh = scanners.remove(scannerName);
          if (rsh != null) {
            scanner = rsh.s;
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
        if (scannerName != null && t instanceof NotServingRegionException) {
          scanners.remove(scannerName);
        }
        throw convertThrowableToIOE(cleanup(t));
      }
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  private void formatResults(final ScanResponse.Builder builder, final List<Result> results,
      final RpcController controller) {
    if (results == null || results.isEmpty()) return;
    RpcCallContext context = RpcServer.getCurrentCall();
    if (context != null && context.isClientCellBlockSupport()) {
      List<CellScannable> cellScannables = new ArrayList<CellScannable>(results.size());
      for (Result res : results) {
        cellScannables.add(res);
        builder.addCellsPerResult(res.size());
      }
      ((PayloadCarryingRpcController)controller).
        setCellScanner(CellUtil.createCellScanner(cellScannables));
    } else {
      for (Result res: results) {
        ClientProtos.Result pbr = ProtobufUtil.toResult(res);
        builder.addResults(pbr);
      }
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
      requestCount.increment();
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

  @Override
  public CoprocessorServiceResponse execService(final RpcController controller,
      final CoprocessorServiceRequest request) throws ServiceException {
    try {
      requestCount.increment();
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
   * @param rpcc the RPC controller
   * @param request the multi request
   * @throws ServiceException
   */
  @Override
  public MultiResponse multi(final RpcController rpcc, final MultiRequest request)
  throws ServiceException {
    // rpc controller is how we bring in data via the back door;  it is unprotobuf'ed data.
    // It is also the conduit via which we pass back data.
    PayloadCarryingRpcController controller = (PayloadCarryingRpcController)rpcc;
    CellScanner cellScanner = controller != null? controller.cellScanner(): null;
    // Clear scanner so we are not holding on to reference across call.
    controller.setCellScanner(null);
    List<CellScannable> cellsToReturn = null;
    try {
      HRegion region = getRegion(request.getRegion());
      MultiResponse.Builder builder = MultiResponse.newBuilder();
      List<MutationProto> mutations = new ArrayList<MutationProto>(request.getActionCount());
      // Do a bunch of mutations atomically.  Mutations are Puts and Deletes.  NOT Gets.
      if (request.hasAtomic() && request.getAtomic()) {
        // MultiAction is union type.  Has a Get or a Mutate.
        for (ClientProtos.MultiAction actionUnion : request.getActionList()) {
          if (actionUnion.hasMutation()) {
            mutations.add(actionUnion.getMutation());
          } else {
            throw new DoNotRetryIOException("Unsupported atomic action type: " + actionUnion);
          }
        }
        // TODO: We are not updating a metric here.  Should we up requestCount?
        if (!mutations.isEmpty()) mutateRows(region, mutations, cellScanner);
      } else {
        // Do a bunch of Actions.
        ActionResult.Builder resultBuilder = null;
        cellsToReturn = new ArrayList<CellScannable>(request.getActionCount());
        for (ClientProtos.MultiAction actionUnion : request.getActionList()) {
          this.requestCount.increment();
          ClientProtos.Result result = null;
          try {
            if (actionUnion.hasGet()) {
              Get get = ProtobufUtil.toGet(actionUnion.getGet());
              Result r = region.get(get);
              if (r != null) {
                // Get a result with no data.  The data will be carried alongside pbs, not as pbs.
                result = ProtobufUtil.toResultNoData(r);
                // Add the Result to controller so it gets serialized apart from pb.  Get
                // Results could be big so good if they are not serialized as pb.
                cellsToReturn.add(r);
              }
            } else if (actionUnion.hasMutation()) {
              MutationProto mutation = actionUnion.getMutation();
              MutationType type = mutation.getMutateType();
              if (type != MutationType.PUT && type != MutationType.DELETE) {
                if (!mutations.isEmpty()) {
                  doBatchOp(builder, region, mutations, cellScanner);
                  mutations.clear();
                } else if (!region.getRegionInfo().isMetaTable()) {
                  cacheFlusher.reclaimMemStoreMemory();
                }
              }
              Result r = null;
              switch (type) {
              case APPEND:
                r = append(region, mutation, cellScanner);
                break;
              case INCREMENT:
                r = increment(region, mutation, cellScanner);
                break;
              case PUT:
              case DELETE:
                mutations.add(mutation);
                break;
              default:
                throw new DoNotRetryIOException("Unsupported mutate type: " + type.name());
              }
              if (r != null) {
                // Put the data into the cellsToReturn and the metadata about the result is all that
                // we will pass back in the protobuf result.
                result = ProtobufUtil.toResultNoData(r);
                cellsToReturn.add(r);
              }
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
              resultBuilder.setValue(result);
              builder.addResult(resultBuilder.build());
            }
          } catch (IOException ie) {
            builder.addResult(ResponseConverter.buildActionResult(ie));
          }
        }
        if (!mutations.isEmpty()) {
          doBatchOp(builder, region, mutations, cellScanner);
        }
      }
      // Load the controller with the Cells to return.
      if (cellsToReturn != null && !cellsToReturn.isEmpty()) {
        controller.setCellScanner(CellUtil.createCellScanner(cellsToReturn));
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
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      HRegionInfo info = region.getRegionInfo();
      GetRegionInfoResponse.Builder builder = GetRegionInfoResponse.newBuilder();
      builder.setRegionInfo(HRegionInfo.convert(info));
      if (request.hasCompactionState() && request.getCompactionState()) {
        builder.setCompactionState(region.getCompactionState());
      }
      builder.setIsRecovering(region.isRecovering());
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
      requestCount.increment();
      Set<byte[]> columnFamilies;
      if (request.getFamilyCount() == 0) {
        columnFamilies = region.getStores().keySet();
      } else {
        columnFamilies = new TreeSet<byte[]>(Bytes.BYTES_RAWCOMPARATOR);
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
      requestCount.increment();
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
   * Open asynchronously a region or a set of regions on the region server.
   *
   * The opening is coordinated by ZooKeeper, and this method requires the znode to be created
   *  before being called. As a consequence, this method should be called only from the master.
   * <p>
   * Different manages states for the region are:<ul>
   *  <li>region not opened: the region opening will start asynchronously.</li>
   *  <li>a close is already in progress: this is considered as an error.</li>
   *  <li>an open is already in progress: this new open request will be ignored. This is important
   *  because the Master can do multiple requests if it crashes.</li>
   *  <li>the region is already opened:  this new open request will be ignored./li>
   *  </ul>
   * </p>
   * <p>
   * Bulk assign: If there are more than 1 region to open, it will be considered as a bulk assign.
   * For a single region opening, errors are sent through a ServiceException. For bulk assign,
   * errors are put in the response as FAILED_OPENING.
   * </p>
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public OpenRegionResponse openRegion(final RpcController controller,
      final OpenRegionRequest request) throws ServiceException {
    try {
      checkOpen();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
    requestCount.increment();
    OpenRegionResponse.Builder builder = OpenRegionResponse.newBuilder();
    final int regionCount = request.getOpenInfoCount();
    final Map<TableName, HTableDescriptor> htds =
        new HashMap<TableName, HTableDescriptor>(regionCount);
    final boolean isBulkAssign = regionCount > 1;
    for (RegionOpenInfo regionOpenInfo : request.getOpenInfoList()) {
      final HRegionInfo region = HRegionInfo.convert(regionOpenInfo.getRegion());

      int versionOfOfflineNode = -1;
      if (regionOpenInfo.hasVersionOfOfflineNode()) {
        versionOfOfflineNode = regionOpenInfo.getVersionOfOfflineNode();
      }
      HTableDescriptor htd;
      try {
        final HRegion onlineRegion = getFromOnlineRegions(region.getEncodedName());
        if (onlineRegion != null) {
          //Check if the region can actually be opened.
          if (onlineRegion.getCoprocessorHost() != null) {
            onlineRegion.getCoprocessorHost().preOpen();
          }
          // See HBASE-5094. Cross check with hbase:meta if still this RS is owning
          // the region.
          Pair<HRegionInfo, ServerName> p = MetaReader.getRegion(
              this.catalogTracker, region.getRegionName());
          if (this.getServerName().equals(p.getSecond())) {
            Boolean closing = regionsInTransitionInRS.get(region.getEncodedNameAsBytes());
            // Map regionsInTransitionInRSOnly has an entry for a region only if the region
            // is in transition on this RS, so here closing can be null. If not null, it can
            // be true or false. True means the region is opening on this RS; while false
            // means the region is closing. Only return ALREADY_OPENED if not closing (i.e.
            // not in transition any more, or still transition to open.
            if (!Boolean.FALSE.equals(closing)
                && getFromOnlineRegions(region.getEncodedName()) != null) {
              LOG.warn("Attempted open of " + region.getEncodedName()
                + " but already online on this server");
              builder.addOpeningState(RegionOpeningState.ALREADY_OPENED);
              continue;
            }
          } else {
            LOG.warn("The region " + region.getEncodedName() + " is online on this server" +
                " but hbase:meta does not have this server - continue opening.");
            removeFromOnlineRegions(onlineRegion, null);
          }
        }
        LOG.info("Open " + region.getRegionNameAsString());
        htd = htds.get(region.getTable());
        if (htd == null) {
          htd = this.tableDescriptors.get(region.getTable());
          htds.put(region.getTable(), htd);
        }

        final Boolean previous = this.regionsInTransitionInRS.putIfAbsent(
            region.getEncodedNameAsBytes(), Boolean.TRUE);

        if (Boolean.FALSE.equals(previous)) {
          // There is a close in progress. We need to mark this open as failed in ZK.
          OpenRegionHandler.
              tryTransitionFromOfflineToFailedOpen(this, region, versionOfOfflineNode);

          throw new RegionAlreadyInTransitionException("Received OPEN for the region:" +
              region.getRegionNameAsString() + " , which we are already trying to CLOSE ");
        }

        if (Boolean.TRUE.equals(previous)) {
          // An open is in progress. This is supported, but let's log this.
          LOG.info("Receiving OPEN for the region:" +
              region.getRegionNameAsString() + " , which we are already trying to OPEN" +
              " - ignoring this new request for this region.");
        }

        // We are opening this region. If it moves back and forth for whatever reason, we don't
        // want to keep returning the stale moved record while we are opening/if we close again.
        removeFromMovedRegions(region.getEncodedName());

        if (previous == null) {
          // check if the region to be opened is marked in recovering state in ZK
          if (this.distributedLogReplay
              && SplitLogManager.isRegionMarkedRecoveringInZK(this.getZooKeeper(),
            region.getEncodedName())) {
            this.recoveringRegions.put(region.getEncodedName(), null);
          }
          // If there is no action in progress, we can submit a specific handler.
          // Need to pass the expected version in the constructor.
          if (region.isMetaRegion()) {
            this.service.submit(new OpenMetaHandler(this, this, region, htd,
                versionOfOfflineNode));
          } else {
            updateRegionFavoredNodesMapping(region.getEncodedName(),
                regionOpenInfo.getFavoredNodesList());
            this.service.submit(new OpenRegionHandler(this, this, region, htd,
                versionOfOfflineNode));
          }
        }

        builder.addOpeningState(RegionOpeningState.OPENED);

      } catch (KeeperException zooKeeperEx) {
        LOG.error("Can't retrieve recovering state from zookeeper", zooKeeperEx);
        throw new ServiceException(zooKeeperEx);
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

  @Override
  public void updateRegionFavoredNodesMapping(String encodedRegionName,
      List<org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ServerName> favoredNodes) {
    InetSocketAddress[] addr = new InetSocketAddress[favoredNodes.size()];
    // Refer to the comment on the declaration of regionFavoredNodesMap on why
    // it is a map of region name to InetSocketAddress[]
    for (int i = 0; i < favoredNodes.size(); i++) {
      addr[i] = InetSocketAddress.createUnresolved(favoredNodes.get(i).getHostName(),
          favoredNodes.get(i).getPort());
    }
    regionFavoredNodesMap.put(encodedRegionName, addr);
  }

  /**
   * Return the favored nodes for a region given its encoded name. Look at the
   * comment around {@link #regionFavoredNodesMap} on why it is InetSocketAddress[]
   * @param encodedRegionName
   * @return array of favored locations
   */
  @Override
  public InetSocketAddress[] getFavoredNodesForRegion(String encodedRegionName) {
    return regionFavoredNodesMap.get(encodedRegionName);
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
      final String encodedRegionName = ProtobufUtil.getRegionEncodedName(request.getRegion());

      // Can be null if we're calling close on a region that's not online
      final HRegion region = this.getFromOnlineRegions(encodedRegionName);
      if ((region  != null) && (region .getCoprocessorHost() != null)) {
        region.getCoprocessorHost().preClose(false);
      }

      requestCount.increment();
      LOG.info("Close " + encodedRegionName + ", via zk=" + (zk ? "yes" : "no") +
        ", znode version=" + versionOfClosingNode + ", on " + sn);

      boolean closed = closeRegion(encodedRegionName, false, zk, versionOfClosingNode, sn);
      CloseRegionResponse.Builder builder = CloseRegionResponse.newBuilder().setClosed(closed);
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
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      LOG.info("Flushing " + region.getRegionNameAsString());
      boolean shouldFlush = true;
      if (request.hasIfOlderThanTs()) {
        shouldFlush = region.getLastFlushTime() < request.getIfOlderThanTs();
      }
      FlushRegionResponse.Builder builder = FlushRegionResponse.newBuilder();
      if (shouldFlush) {
        boolean result = region.flushcache();
        if (result) {
          this.compactSplitThread.requestSystemCompaction(region,
              "Compaction through user triggered flush");
        }
        builder.setFlushed(result);
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
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      region.startRegionOperation(Operation.SPLIT_REGION);
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
   * Merge regions on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @return merge regions response
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority = HConstants.HIGH_QOS)
  public MergeRegionsResponse mergeRegions(final RpcController controller,
      final MergeRegionsRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      HRegion regionA = getRegion(request.getRegionA());
      HRegion regionB = getRegion(request.getRegionB());
      boolean forcible = request.getForcible();
      regionA.startRegionOperation(Operation.MERGE_REGION);
      regionB.startRegionOperation(Operation.MERGE_REGION);
      LOG.info("Receiving merging request for  " + regionA + ", " + regionB
          + ",forcible=" + forcible);
      regionA.flushcache();
      regionB.flushcache();
      compactSplitThread.requestRegionsMerge(regionA, regionB, forcible);
      return MergeRegionsResponse.newBuilder().build();
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
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      region.startRegionOperation(Operation.COMPACT_REGION);
      LOG.info("Compacting " + region.getRegionNameAsString());
      boolean major = false;
      byte [] family = null;
      Store store = null;
      if (request.hasFamily()) {
        family = request.getFamily().toByteArray();
        store = region.getStore(family);
        if (store == null) {
          throw new ServiceException(new IOException("column family " + Bytes.toString(family) +
            " does not exist in region " + region.getRegionNameAsString()));
        }
      }
      if (request.hasMajor()) {
        major = request.getMajor();
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
      if(family != null) {
        compactSplitThread.requestCompaction(region, store, log,
          Store.PRIORITY_USER, null);
      } else {
        compactSplitThread.requestCompaction(region, log,
          Store.PRIORITY_USER, null);
      }
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
      final ReplicateWALEntryRequest request)
  throws ServiceException {
    try {
      if (replicationSinkHandler != null) {
        checkOpen();
        requestCount.increment();
        this.replicationSinkHandler.replicateLogEntries(request.getEntryList(),
          ((PayloadCarryingRpcController)controller).cellScanner());
      }
      return ReplicateWALEntryResponse.newBuilder().build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Replay the given changes when distributedLogReplay WAL edits from a failed RS. The guarantee is
   * that the given mutations will be durable on the receiving RS if this method returns without any
   * exception.
   * @param rpcc the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority = HConstants.REPLAY_QOS)
  public MultiResponse replay(final RpcController rpcc, final MultiRequest request)
      throws ServiceException {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    PayloadCarryingRpcController controller = (PayloadCarryingRpcController) rpcc;
    CellScanner cellScanner = controller != null ? controller.cellScanner() : null;
    // Clear scanner so we are not holding on to reference across call.
    controller.setCellScanner(null);
    try {
      checkOpen();
      HRegion region = getRegion(request.getRegion());
      MultiResponse.Builder builder = MultiResponse.newBuilder();
      List<MutationProto> mutates = new ArrayList<MutationProto>();
      for (ClientProtos.MultiAction actionUnion : request.getActionList()) {
        if (actionUnion.hasMutation()) {
          MutationProto mutate = actionUnion.getMutation();
          MutationType type = mutate.getMutateType();
          switch (type) {
          case PUT:
          case DELETE:
            mutates.add(mutate);
            break;
          default:
            throw new DoNotRetryIOException("Unsupported mutate type: " + type.name());
          }
        } else {
          LOG.warn("Error: invalid action: " + actionUnion + ". " + "it must be a Mutation.");
          throw new DoNotRetryIOException("Invalid action, " + "it must be a Mutation.");
        }
      }
      if (!mutates.isEmpty()) {
        doBatchOp(builder, region, mutates, cellScanner, true);
      }
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    } finally {
      metricsRegionServer.updateReplay(EnvironmentEdgeManager.currentTimeMillis() - before);
    }
  }

  /**
   * Roll the WAL writer of the region server.
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  public RollWALWriterResponse rollWALWriter(final RpcController controller,
      final RollWALWriterRequest request) throws ServiceException {
    try {
      requestCount.increment();
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
    requestCount.increment();
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
    requestCount.increment();
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
    return getRegionByEncodedName(
        ProtobufUtil.getRegionEncodedName(regionSpecifier));
  }

  /**
   * Execute an append mutation.
   *
   * @param region
   * @param m
   * @param cellScanner
   * @return result to return to client if default operation should be
   * bypassed as indicated by RegionObserver, null otherwise
   * @throws IOException
   */
  protected Result append(final HRegion region,
      final MutationProto m, final CellScanner cellScanner) throws IOException {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    Append append = ProtobufUtil.toAppend(m, cellScanner);
    Result r = null;
    if (region.getCoprocessorHost() != null) {
      r = region.getCoprocessorHost().preAppend(append);
    }
    if (r == null) {
      r = region.append(append);
      if (region.getCoprocessorHost() != null) {
        region.getCoprocessorHost().postAppend(append, r);
      }
    }
    metricsRegionServer.updateAppend(EnvironmentEdgeManager.currentTimeMillis() - before);
    return r;
  }

  /**
   * Execute an increment mutation.
   *
   * @param region
   * @param mutation
   * @return the Result
   * @throws IOException
   */
  protected Result increment(final HRegion region, final MutationProto mutation,
      final CellScanner cells)
  throws IOException {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    Increment increment = ProtobufUtil.toIncrement(mutation, cells);
    Result r = null;
    if (region.getCoprocessorHost() != null) {
      r = region.getCoprocessorHost().preIncrement(increment);
    }
    if (r == null) {
      r = region.increment(increment);
      if (region.getCoprocessorHost() != null) {
        r = region.getCoprocessorHost().postIncrement(increment, r);
      }
    }
    metricsRegionServer.updateIncrement(EnvironmentEdgeManager.currentTimeMillis() - before);
    return r;
  }

  /**
   * Execute a list of Put/Delete mutations.
   */
  protected void doBatchOp(final MultiResponse.Builder builder,
      final HRegion region, final List<MutationProto> mutates, final CellScanner cells) {
    doBatchOp(builder, region, mutates, cells, false);
  }

  /**
   * Execute a list of Put/Delete mutations.
   *
   * @param builder
   * @param region
   * @param mutations
   */
  protected void doBatchOp(final MultiResponse.Builder builder, final HRegion region,
      final List<MutationProto> mutations, final CellScanner cells, boolean isReplay) {
    Mutation[] mArray = new Mutation[mutations.size()];
    long before = EnvironmentEdgeManager.currentTimeMillis();
    boolean batchContainsPuts = false, batchContainsDelete = false;
    try {
      ActionResult.Builder resultBuilder = ActionResult.newBuilder();
      resultBuilder.setValue(ClientProtos.Result.newBuilder().build());
      ActionResult result = resultBuilder.build();
      int i = 0;
      for (MutationProto m : mutations) {
        Mutation mutation;
        if (m.getMutateType() == MutationType.PUT) {
          mutation = ProtobufUtil.toPut(m, cells);
          batchContainsPuts = true;
        } else {
          mutation = ProtobufUtil.toDelete(m, cells);
          batchContainsDelete = true;
        }
        mArray[i++] = mutation;
        builder.addResult(result);
      }

      requestCount.add(mutations.size());
      if (!region.getRegionInfo().isMetaTable()) {
        cacheFlusher.reclaimMemStoreMemory();
      }

      OperationStatus codes[] = region.batchMutate(mArray, isReplay);
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
        if (isReplay && codes[i].getOperationStatusCode() != OperationStatusCode.SUCCESS) {
          // in replay mode, we only need to catpure the first error because we will retry the whole
          // batch when an error happens
          break;
        }
      }
    } catch (IOException ie) {
      ActionResult result = ResponseConverter.buildActionResult(ie);
      for (int i = 0; i < mutations.size(); i++) {
        builder.setResult(i, result);
        if (isReplay) {
          // in replay mode, we only need to catpure the first error because we will retry the whole
          // batch when an error happens
          break;
        }
      }
    }
    long after = EnvironmentEdgeManager.currentTimeMillis();
    if (batchContainsPuts) {
      metricsRegionServer.updatePut(after - before);
    }
    if (batchContainsDelete) {
      metricsRegionServer.updateDelete(after - before);
    }
  }

  /**
   * Mutate a list of rows atomically.
   *
   * @param region
   * @param mutations
 * @param cellScanner if non-null, the mutation data -- the Cell content.
   * @throws IOException
   */
  protected void mutateRows(final HRegion region, final List<MutationProto> mutations,
      final CellScanner cellScanner)
  throws IOException {
    MutationProto firstMutate = mutations.get(0);
    if (!region.getRegionInfo().isMetaTable()) {
      cacheFlusher.reclaimMemStoreMemory();
    }
    byte [] row = firstMutate.getRow().toByteArray();
    RowMutations rm = new RowMutations(row);
    for (MutationProto mutate: mutations) {
      MutationType type = mutate.getMutateType();
      switch (mutate.getMutateType()) {
      case PUT:
        rm.add(ProtobufUtil.toPut(mutate, cellScanner));
        break;
      case DELETE:
        rm.add(ProtobufUtil.toDelete(mutate, cellScanner));
        break;
        default:
          throw new DoNotRetryIOException(
            "mutate supports atomic put and/or delete, not "
              + type.name());
      }
    }
    region.mutateRow(rm);
  }

  private static class MovedRegionInfo {
    private final ServerName serverName;
    private final long seqNum;
    private final long ts;

    public MovedRegionInfo(ServerName serverName, long closeSeqNum) {
      this.serverName = serverName;
      this.seqNum = closeSeqNum;
      ts = EnvironmentEdgeManager.currentTimeMillis();
     }

    public ServerName getServerName() {
      return serverName;
    }

    public long getSeqNum() {
      return seqNum;
    }

    public long getMoveTime() {
      return ts;
    }
  }

  // This map will contains all the regions that we closed for a move.
  //  We add the time it was moved as we don't want to keep too old information
  protected Map<String, MovedRegionInfo> movedRegions =
      new ConcurrentHashMap<String, MovedRegionInfo>(3000);

  // We need a timeout. If not there is a risk of giving a wrong information: this would double
  //  the number of network calls instead of reducing them.
  private static final int TIMEOUT_REGION_MOVED = (2 * 60 * 1000);

  protected void addToMovedRegions(String encodedName, ServerName destination, long closeSeqNum) {
    if (ServerName.isSameHostnameAndPort(destination, this.getServerName())) {
      LOG.warn("Not adding moved region record: " + encodedName + " to self.");
      return;
    }
    LOG.info("Adding moved region record: " + encodedName + " to "
        + destination.getServerName() + ":" + destination.getPort()
        + " as of " + closeSeqNum);
    movedRegions.put(encodedName, new MovedRegionInfo(destination, closeSeqNum));
  }

  private void removeFromMovedRegions(String encodedName) {
    movedRegions.remove(encodedName);
  }

  private MovedRegionInfo getMovedRegion(final String encodedRegionName) {
    MovedRegionInfo dest = movedRegions.get(encodedRegionName);

    long now = EnvironmentEdgeManager.currentTimeMillis();
    if (dest != null) {
      if (dest.getMoveTime() > (now - TIMEOUT_REGION_MOVED)) {
        return dest;
      } else {
        movedRegions.remove(encodedRegionName);
      }
    }

    return null;
  }

  /**
   * Remove the expired entries from the moved regions list.
   */
  protected void cleanMovedRegions() {
    final long cutOff = System.currentTimeMillis() - TIMEOUT_REGION_MOVED;
    Iterator<Entry<String, MovedRegionInfo>> it = movedRegions.entrySet().iterator();

    while (it.hasNext()){
      Map.Entry<String, MovedRegionInfo> e = it.next();
      if (e.getValue().getMoveTime() < cutOff) {
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

  /**
   * Holder class which holds the RegionScanner and nextCallSeq together.
   */
  private static class RegionScannerHolder {
    private RegionScanner s;
    private long nextCallSeq = 0L;

    public RegionScannerHolder(RegionScanner s) {
      this.s = s;
    }
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

  /**
   * A helper function to store the last flushed sequence Id with the previous failed RS for a
   * recovering region. The Id is used to skip wal edits which are flushed. Since the flushed
   * sequence id is only valid for each RS, we associate the Id with corresponding failed RS.
   * @throws KeeperException
   * @throws IOException
   */
  private void updateRecoveringRegionLastFlushedSequenceId(HRegion r) throws KeeperException,
      IOException {
    if (!r.isRecovering()) {
      // return immdiately for non-recovering regions
      return;
    }

    HRegionInfo region = r.getRegionInfo();
    ZooKeeperWatcher zkw = getZooKeeper();
    String previousRSName = this.getLastFailedRSFromZK(region.getEncodedName());
    Map<byte[], Long> maxSeqIdInStores = r.getMaxStoreSeqIdForLogReplay();
    long minSeqIdForLogReplay = -1;
    for (byte[] columnFamily : maxSeqIdInStores.keySet()) {
      Long storeSeqIdForReplay = maxSeqIdInStores.get(columnFamily);
      if (minSeqIdForLogReplay == -1 || storeSeqIdForReplay < minSeqIdForLogReplay) {
        minSeqIdForLogReplay = storeSeqIdForReplay;
      }
    }
    long lastRecordedFlushedSequenceId = -1;
    String nodePath = ZKUtil.joinZNode(this.zooKeeper.recoveringRegionsZNode,
      region.getEncodedName());
    // recovering-region level
    byte[] data = ZKUtil.getData(zkw, nodePath);
    if (data != null) {
      lastRecordedFlushedSequenceId = SplitLogManager.parseLastFlushedSequenceIdFrom(data);
    }
    if (data == null || lastRecordedFlushedSequenceId < minSeqIdForLogReplay) {
      ZKUtil.setData(zkw, nodePath, ZKUtil.positionToByteArray(minSeqIdForLogReplay));
    }
    if (previousRSName != null) {
      // one level deeper for the failed RS
      nodePath = ZKUtil.joinZNode(nodePath, previousRSName);
      ZKUtil.setData(zkw, nodePath,
        ZKUtil.regionSequenceIdsToByteArray(minSeqIdForLogReplay, maxSeqIdInStores));
      LOG.debug("Update last flushed sequence id of region " + region.getEncodedName() + " for "
          + previousRSName);
    } else {
      LOG.warn("Can't find failed region server for recovering region " + region.getEncodedName());
    }
  }

  /**
   * Return the last failed RS name under /hbase/recovering-regions/encodedRegionName
   * @param encodedRegionName
   * @return
   * @throws IOException
   * @throws KeeperException
   */
  private String getLastFailedRSFromZK(String encodedRegionName) throws KeeperException {
    String result = null;
    long maxZxid = 0;
    ZooKeeperWatcher zkw = this.getZooKeeper();
    String nodePath = ZKUtil.joinZNode(zkw.recoveringRegionsZNode, encodedRegionName);
    List<String> failedServers = ZKUtil.listChildrenNoWatch(zkw, nodePath);
    if (failedServers == null || failedServers.isEmpty()) {
      return result;
    }
    for (String failedServer : failedServers) {
      String rsPath = ZKUtil.joinZNode(nodePath, failedServer);
      Stat stat = new Stat();
      ZKUtil.getDataNoWatch(zkw, rsPath, stat);
      if (maxZxid < stat.getCzxid()) {
        maxZxid = stat.getCzxid();
        result = failedServer;
      }
    }
    return result;
  }

  @Override
  public UpdateFavoredNodesResponse updateFavoredNodes(RpcController controller,
      UpdateFavoredNodesRequest request) throws ServiceException {
    List<UpdateFavoredNodesRequest.RegionUpdateInfo> openInfoList = request.getUpdateInfoList();
    UpdateFavoredNodesResponse.Builder respBuilder = UpdateFavoredNodesResponse.newBuilder();
    for (UpdateFavoredNodesRequest.RegionUpdateInfo regionUpdateInfo : openInfoList) {
      HRegionInfo hri = HRegionInfo.convert(regionUpdateInfo.getRegion());
      updateRegionFavoredNodesMapping(hri.getEncodedName(),
          regionUpdateInfo.getFavoredNodesList());
    }
    respBuilder.setResponse(openInfoList.size());
    return respBuilder.build();
  }
}
