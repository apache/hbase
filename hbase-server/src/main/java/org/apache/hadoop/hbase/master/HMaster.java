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
import java.io.PrintWriter;
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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HealthCheckChore;
import org.apache.hadoop.hbase.MasterNotRunningException;
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
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitorBase;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.exceptions.MergeRegionException;
import org.apache.hadoop.hbase.exceptions.UnknownProtocolException;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.balancer.BalancerChore;
import org.apache.hadoop.hbase.master.balancer.ClusterStatusChore;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.master.cleaner.LogCleaner;
import org.apache.hadoop.hbase.master.handler.CreateTableHandler;
import org.apache.hadoop.hbase.master.handler.DeleteTableHandler;
import org.apache.hadoop.hbase.master.handler.DisableTableHandler;
import org.apache.hadoop.hbase.master.handler.DispatchMergingRegionHandler;
import org.apache.hadoop.hbase.master.handler.EnableTableHandler;
import org.apache.hadoop.hbase.master.handler.ModifyTableHandler;
import org.apache.hadoop.hbase.master.handler.TableAddFamilyHandler;
import org.apache.hadoop.hbase.master.handler.TableDeleteFamilyHandler;
import org.apache.hadoop.hbase.master.handler.TableModifyFamilyHandler;
import org.apache.hadoop.hbase.master.handler.TruncateTableHandler;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.monitoring.MemoryBoundedLogMessageBuffer;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.procedure.MasterProcedureManager;
import org.apache.hadoop.hbase.procedure.MasterProcedureManagerHost;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.*;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionServerInfo;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AddColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AddColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AssignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AssignRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.BalanceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.BalanceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteSnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DisableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DisableTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DispatchMergingRegionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DispatchMergingRegionsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableCatalogJanitorRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableCatalogJanitorResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ExecProcedureRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ExecProcedureResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetClusterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetClusterStatusResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetCompletedSnapshotsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetCompletedSnapshotsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetNamespaceDescriptorRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetNamespaceDescriptorResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetSchemaAlterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetSchemaAlterStatusResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableNamesRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableNamesResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsProcedureDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsProcedureDoneResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsRestoreSnapshotDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsRestoreSnapshotDoneResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListNamespaceDescriptorsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListNamespaceDescriptorsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListTableDescriptorsByNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListTableDescriptorsByNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListTableNamesByNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListTableNamesByNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MoveRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.OfflineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.OfflineRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RestoreSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RestoreSnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RunCatalogScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RunCatalogScanResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetBalancerRunningResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ShutdownRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ShutdownResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.StopMasterRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.StopMasterResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.TruncateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.TruncateTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.UnassignRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ExecProcedureRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ExecProcedureResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsProcedureDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsProcedureDoneResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerReportResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.ReportRSFatalErrorRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.ReportRSFatalErrorResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionSplitPolicy;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.trace.SpanReceiverHost;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CompressionTest;
import org.apache.hadoop.hbase.util.ConfigUtil;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.EncryptionTest;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.hbase.util.JvmPauseMonitor;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.ClusterStatusTracker;
import org.apache.hadoop.hbase.zookeeper.DrainingServerTracker;
import org.apache.hadoop.hbase.zookeeper.LoadBalancerTracker;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.RegionServerTracker;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.net.DNS;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.google.common.collect.Lists;
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
 * @see Watcher
 */
@InterfaceAudience.Private
@SuppressWarnings("deprecation")
public class HMaster extends HasThread implements MasterProtos.MasterService.BlockingInterface,
RegionServerStatusProtos.RegionServerStatusService.BlockingInterface,
MasterServices, Server {
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

  // The configuration for the Master
  private final Configuration conf;
  // server for the web ui
  private InfoServer infoServer;

  // Our zk client.
  private ZooKeeperWatcher zooKeeper;
  // Manager and zk listener for master election
  private ActiveMasterManager activeMasterManager;
  // Region server tracker
  RegionServerTracker regionServerTracker;
  // Draining region server tracker
  private DrainingServerTracker drainingServerTracker;
  // Tracker for load balancer state
  private LoadBalancerTracker loadBalancerTracker;
  // master address tracker
  private MasterAddressTracker masterAddressTracker;

  // RPC server for the HMaster
  private final RpcServerInterface rpcServer;
  private JvmPauseMonitor pauseMonitor;
  // Set after we've called HBaseServer#openServer and ready to receive RPCs.
  // Set back to false after we stop rpcServer.  Used by tests.
  private volatile boolean rpcServerOpen = false;

  /** Namespace stuff */
  private TableNamespaceManager tableNamespaceManager;

  /**
   * This servers address.
   */
  private final InetSocketAddress isa;

  // Metrics for the HMaster
  private final MetricsMaster metricsMaster;
  // file system manager for the master FS operations
  private MasterFileSystem fileSystemManager;

  // server manager to deal with region server info
  ServerManager serverManager;

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

  // flag set after we complete assignMeta.
  private volatile boolean serverShutdownHandlerEnabled = false;

  // Instance of the hbase executor service.
  ExecutorService executorService;

  private LoadBalancer balancer;
  private Thread balancerChore;
  private Thread clusterStatusChore;
  private ClusterStatusPublisher clusterStatusPublisherChore = null;

  private CatalogJanitor catalogJanitorChore;
  private LogCleaner logCleaner;
  private HFileCleaner hfileCleaner;

  private MasterCoprocessorHost cpHost;
  private final ServerName serverName;

  private final boolean preLoadTableDescriptors;

  private TableDescriptors tableDescriptors;

  // Table level lock manager for schema changes
  private TableLockManager tableLockManager;

  // Time stamps for when a hmaster was started and when it became active
  private long masterStartTime;
  private long masterActiveTime;

  /** time interval for emitting metrics values */
  private final int msgInterval;
  /**
   * MX Bean for MasterInfo
   */
  private ObjectName mxBean = null;

  // should we check the compression codec type at master side, default true, HBASE-6370
  private final boolean masterCheckCompression;

  // should we check encryption settings at master side, default true
  private final boolean masterCheckEncryption;

  private SpanReceiverHost spanReceiverHost;

  private Map<String, Service> coprocessorServiceHandlers = Maps.newHashMap();

  // monitor for snapshot of hbase tables
  private SnapshotManager snapshotManager;
  // monitor for distributed procedures
  private MasterProcedureManagerHost mpmHost;

  /** The health check chore. */
  private HealthCheckChore healthCheckChore;

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
    FSUtils.setupShortCircuitRead(conf);
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
    String name = "master/" + initialIsa.toString();
    // Set how many times to retry talking to another server over HConnection.
    HConnectionManager.setServerSideHConnectionRetries(this.conf, name, LOG);
    int numHandlers = conf.getInt(HConstants.MASTER_HANDLER_COUNT,
      conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT, HConstants.DEFAULT_MASTER_HANLDER_COUNT));
    this.rpcServer = new RpcServer(this, name, getServices(),
      initialIsa, // BindAddress is IP we got for this server.
      conf,
      new FifoRpcScheduler(conf, numHandlers));
    // Set our address.
    this.isa = this.rpcServer.getListenerAddress();
    // We don't want to pass isa's hostname here since it could be 0.0.0.0
    this.serverName = ServerName.valueOf(hostname, this.isa.getPort(), System.currentTimeMillis());
    this.rsFatals = new MemoryBoundedLogMessageBuffer(
      conf.getLong("hbase.master.buffer.for.rs.fatals", 1*1024*1024));

    // login the zookeeper client principal (if using security)
    ZKUtil.loginClient(this.conf, "hbase.zookeeper.client.keytab.file",
      "hbase.zookeeper.client.kerberos.principal", this.isa.getHostName());

    // initialize server principal (if using secure Hadoop)
    UserProvider provider = UserProvider.instantiate(conf);
    provider.login("hbase.master.keytab.file",
      "hbase.master.kerberos.principal", this.isa.getHostName());

    LOG.info("hbase.rootdir=" + FSUtils.getRootDir(this.conf) +
        ", hbase.cluster.distributed=" + this.conf.getBoolean("hbase.cluster.distributed", false));

    // set the thread name now we have an address
    setName(MASTER + ":" + this.serverName.toShortString());

    Replication.decorateMasterConfiguration(this.conf);

    // Hack! Maps DFSClient => Master for logs.  HDFS made this
    // config param for task trackers, but we can piggyback off of it.
    if (this.conf.get("mapred.task.id") == null) {
      this.conf.set("mapred.task.id", "hb_m_" + this.serverName.toString());
    }

    this.zooKeeper = new ZooKeeperWatcher(conf, MASTER + ":" + isa.getPort(), this, true);
    this.rpcServer.startThreads();
    this.pauseMonitor = new JvmPauseMonitor(conf);
    this.pauseMonitor.start();

    // metrics interval: using the same property as region server.
    this.msgInterval = conf.getInt("hbase.regionserver.msginterval", 3 * 1000);

    // should we check the compression codec type at master side, default true, HBASE-6370
    this.masterCheckCompression = conf.getBoolean("hbase.master.check.compression", true);

    // should we check encryption settings at master side, default true
    this.masterCheckEncryption = conf.getBoolean("hbase.master.check.encryption", true);

    this.metricsMaster = new MetricsMaster( new MetricsMasterWrapperImpl(this));

    // preload table descriptor at startup
    this.preLoadTableDescriptors = conf.getBoolean("hbase.master.preload.tabledescriptors", true);

    // Health checker thread.
    int sleepTime = this.conf.getInt(HConstants.HEALTH_CHORE_WAKE_FREQ,
      HConstants.DEFAULT_THREAD_WAKE_FREQUENCY);
    if (isHealthCheckerConfigured()) {
      healthCheckChore = new HealthCheckChore(sleepTime, this, getConfiguration());
    }

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
        Threads.setDaemonThreadRunning(clusterStatusPublisherChore.getThread());
      }
    }
  }

  /**
   * @return list of blocking services and their security info classes that this server supports
   */
  private List<BlockingServiceAndInterface> getServices() {
    List<BlockingServiceAndInterface> bssi = new ArrayList<BlockingServiceAndInterface>(3);
    bssi.add(new BlockingServiceAndInterface(
        MasterProtos.MasterService.newReflectiveBlockingService(this),
        MasterProtos.MasterService.BlockingInterface.class));
    bssi.add(new BlockingServiceAndInterface(
        RegionServerStatusProtos.RegionServerStatusService.newReflectiveBlockingService(this),
        RegionServerStatusProtos.RegionServerStatusService.BlockingInterface.class));
    return bssi;
  }

  /**
   * Stall startup if we are designated a backup master; i.e. we want someone
   * else to become the master before proceeding.
   * @param c configuration
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
      this.masterAddressTracker = new MasterAddressTracker(getZooKeeperWatcher(), this);
      this.masterAddressTracker.start();

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

      this.registeredZKListenersBeforeRecovery = this.zooKeeper.getListeners();
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
      if (this.mpmHost != null) this.mpmHost.stop("server shutting down.");
      this.zooKeeper.close();
    }
    LOG.info("HMaster main thread exiting");
  }

  /**
   * Useful for testing purpose also where we have
   * master restart scenarios.
   */
  protected void startCatalogJanitorChore() {
    Threads.setDaemonThreadRunning(catalogJanitorChore.getThread());
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
  void initializeZKBasedSystemTrackers() throws IOException,
      InterruptedException, KeeperException {
    this.catalogTracker = createCatalogTracker(this.zooKeeper, this.conf, this);
    this.catalogTracker.start();

    this.balancer = LoadBalancerFactory.getLoadBalancer(conf);
    this.loadBalancerTracker = new LoadBalancerTracker(zooKeeper, this);
    this.loadBalancerTracker.start();
    this.assignmentManager = new AssignmentManager(this, serverManager,
      this.catalogTracker, this.balancer, this.executorService, this.metricsMaster,
      this.tableLockManager);
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

    LOG.info("Server active/primary master=" + this.serverName +
        ", sessionid=0x" +
        Long.toHexString(this.zooKeeper.getRecoverableZooKeeper().getSessionId()) +
        ", setting cluster-up flag (Was=" + wasUp + ")");

    // create/initialize the snapshot manager and other procedure managers
    this.snapshotManager = new SnapshotManager();
    this.mpmHost = new MasterProcedureManagerHost();
    this.mpmHost.register(this.snapshotManager);
    this.mpmHost.loadProcedures(conf);
    this.mpmHost.initialize(this, this.metricsMaster);
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
   * <li>Ensure assignment of meta regions<li>
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
    this.fileSystemManager = new MasterFileSystem(this, this, masterRecovery);

    this.tableDescriptors =
      new FSTableDescriptors(this.conf, this.fileSystemManager.getFileSystem(),
      this.fileSystemManager.getRootDir());

    // enable table descriptors cache
    this.tableDescriptors.setCacheOn();

    // warm-up HTDs cache on master initialization
    if (preLoadTableDescriptors) {
      status.setStatus("Pre-loading table descriptors");
      this.tableDescriptors.getAll();
    }

    // publish cluster ID
    status.setStatus("Publishing Cluster ID in ZooKeeper");
    ZKClusterId.setClusterId(this.zooKeeper, fileSystemManager.getClusterId());

    if (!masterRecovery) {
      this.executorService = new ExecutorService(getServerName().toShortString());
      this.serverManager = createServerManager(this, this);
    }

    //Initialize table lock manager, and ensure that all write locks held previously
    //are invalidated
    this.tableLockManager = TableLockManager.createTableLockManager(conf, zooKeeper, serverName);
    if (!masterRecovery) {
      this.tableLockManager.reapWriteLocks();
    }

    status.setStatus("Initializing ZK system trackers");
    initializeZKBasedSystemTrackers();

    if (!masterRecovery) {
      // initialize master side coprocessors before we start handling requests
      status.setStatus("Initializing master coprocessors");
      this.cpHost = new MasterCoprocessorHost(this, this.conf);

      spanReceiverHost = SpanReceiverHost.getInstance(getConfiguration());

      // start up all service threads.
      status.setStatus("Initializing master service threads");
      startServiceThreads();
    }

    // Wait for region servers to report in.
    this.serverManager.waitForRegionServers(status);
    // Check zk for region servers that are up but didn't register
    for (ServerName sn: this.regionServerTracker.getOnlineServers()) {
      // The isServerOnline check is opportunistic, correctness is handled inside
      if (!this.serverManager.isServerOnline(sn)
          && serverManager.checkAndRecordNewServer(sn, ServerLoad.EMPTY_SERVERLOAD)) {
        LOG.info("Registered server found up in zk but who has not yet reported in: " + sn);
      }
    }

    if (!masterRecovery) {
      this.assignmentManager.startTimeOutMonitor();
    }

    // get a list for previously failed RS which need log splitting work
    // we recover hbase:meta region servers inside master initialization and
    // handle other failed servers in SSH in order to start up master node ASAP
    Set<ServerName> previouslyFailedServers = this.fileSystemManager
        .getFailedServersFromLogFolders();

    // remove stale recovering regions from previous run
    this.fileSystemManager.removeStaleRecoveringRegionsFromZK(previouslyFailedServers);

    // log splitting for hbase:meta server
    ServerName oldMetaServerLocation = this.catalogTracker.getMetaLocation();
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

    //initialize load balancer
    this.balancer.setClusterStatus(getClusterStatus());
    this.balancer.setMasterServices(this);
    this.balancer.initialize();

    // Make sure meta assigned before proceeding.
    status.setStatus("Assigning Meta Region");
    assignMeta(status, previouslyFailedMetaRSs);
    // check if master is shutting down because above assignMeta could return even hbase:meta isn't
    // assigned when master is shutting down
    if(this.stopped) return;

    status.setStatus("Submitting log splitting work for previously failed region servers");
    // Master has recovered hbase:meta region server and we put
    // other failed region servers in a queue to be handled later by SSH
    for (ServerName tmpServer : previouslyFailedServers) {
      this.serverManager.processDeadServer(tmpServer, true);
    }

    // Update meta with new PB serialization if required. i.e migrate all HRI to PB serialization
    // in meta. This must happen before we assign all user regions or else the assignment will
    // fail.
    if (this.conf.getBoolean("hbase.MetaMigrationConvertingToPB", true)) {
      org.apache.hadoop.hbase.catalog.MetaMigrationConvertingToPB.updateMetaIfNecessary(this);
    }

    // Fix up assignment manager status
    status.setStatus("Starting assignment manager");
    this.assignmentManager.joinCluster();

    //set cluster status again after user regions are assigned
    this.balancer.setClusterStatus(getClusterStatus());

    if (!masterRecovery) {
      // Start balancer and meta catalog janitor after meta and regions have
      // been assigned.
      status.setStatus("Starting balancer and catalog janitor");
      this.clusterStatusChore = getAndStartClusterStatusChore(this);
      this.balancerChore = getAndStartBalancerChore(this);
      this.catalogJanitorChore = new CatalogJanitor(this, this);
      startCatalogJanitorChore();
    }

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

  /**
   * Check <code>hbase:meta</code> is assigned. If not, assign it.
   * @param status MonitoredTask
   * @param previouslyFailedMetaRSs
   * @throws InterruptedException
   * @throws IOException
   * @throws KeeperException
   */
  void assignMeta(MonitoredTask status, Set<ServerName> previouslyFailedMetaRSs)
      throws InterruptedException, IOException, KeeperException {
    // Work on meta region
    int assigned = 0;
    long timeout = this.conf.getLong("hbase.catalog.verification.timeout", 1000);
    status.setStatus("Assigning hbase:meta region");

    RegionStates regionStates = assignmentManager.getRegionStates();
    
    RegionState regionState = this.catalogTracker.getMetaRegionState();
    ServerName currentMetaServer = regionState.getServerName();
    
    if (!ConfigUtil.useZKForAssignment(conf)) {
      regionStates.createRegionState(HRegionInfo.FIRST_META_REGIONINFO, regionState.getState(),
        currentMetaServer);
    } else {
      regionStates.createRegionState(HRegionInfo.FIRST_META_REGIONINFO);
    }
    boolean rit =
     this.assignmentManager
         .processRegionInTransitionAndBlockUntilAssigned(HRegionInfo.FIRST_META_REGIONINFO);
    boolean metaRegionLocation = this.catalogTracker.verifyMetaRegionLocation(timeout);
    if (!metaRegionLocation || !regionState.isOpened()) {
      // Meta location is not verified. It should be in transition, or offline.
      // We will wait for it to be assigned in enableSSHandWaitForMeta below.
      assigned++;
      if (!ConfigUtil.useZKForAssignment(conf)) {
        assignMetaZkLess(regionStates, regionState, timeout, previouslyFailedMetaRSs);
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
          splitMetaLogBeforeAssignment(currentMetaServer);
          previouslyFailedMetaRSs.add(currentMetaServer);
        }
        assignmentManager.assignMeta();
      }
    } else {
      // Region already assigned. We didn't assign it. Add to in-memory state.
      regionStates.updateRegionState(
        HRegionInfo.FIRST_META_REGIONINFO, State.OPEN, currentMetaServer);
      this.assignmentManager.regionOnline(
        HRegionInfo.FIRST_META_REGIONINFO, currentMetaServer);
    }

    enableMeta(TableName.META_TABLE_NAME);

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
    enableServerShutdownHandler(assigned != 0);

    LOG.info("hbase:meta assigned=" + assigned + ", rit=" + rit +
      ", location=" + catalogTracker.getMetaLocation());
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
        splitMetaLogBeforeAssignment(currentServer);
        regionStates.logSplit(HRegionInfo.FIRST_META_REGIONINFO);
        previouslyFailedRs.add(currentServer);
      }
      LOG.info("Re-assigning hbase:meta, it was on " + currentServer);
      regionStates.updateRegionState(HRegionInfo.FIRST_META_REGIONINFO, State.OFFLINE);
      assignmentManager.assignMeta();
    }
  }
  
  void initNamespace() throws IOException {
    //create namespace manager
    tableNamespaceManager = new TableNamespaceManager(this);
    tableNamespaceManager.start();
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
      this.catalogTracker.waitForMeta();
      // Above check waits for general meta availability but this does not
      // guarantee that the transition has completed
      this.assignmentManager.waitForAssignment(HRegionInfo.FIRST_META_REGIONINFO);
    }
  }

  private void enableMeta(TableName metaTableName) {
    if (!this.assignmentManager.getZKTable().isEnabledTable(metaTableName)) {
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

  public MasterAddressTracker getMasterAddressTracker() {
    return this.masterAddressTracker;
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
      conf.getInt("hbase.master.executor.serverops.threads", 5));
   this.executorService.startExecutorService(ExecutorType.MASTER_META_SERVER_OPERATIONS,
      conf.getInt("hbase.master.executor.serverops.threads", 5));
   this.executorService.startExecutorService(ExecutorType.M_LOG_REPLAY_OPS,
      conf.getInt("hbase.master.executor.logreplayops.threads", 10));

   // We depend on there being only one instance of this executor running
   // at a time.  To do concurrency, would need fencing of enable/disable of
   // tables.
   // Any time changing this maxThreads to > 1, pls see the comment at
   // AccessController#postCreateTableHandler
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
    this.rpcServerOpen = true;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Started service threads");
    }
  }

  /**
   * Use this when trying to figure when its ok to send in rpcs.  Used by tests.
   * @return True if we have successfully run {@link RpcServer#openServer()}
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
    if (this.pauseMonitor != null) {
      this.pauseMonitor.stop();
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
    if (this.clusterStatusPublisherChore != null){
      clusterStatusPublisherChore.interrupt();
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
    return RpcServer.getRemoteIp();
  }

  /**
   * @return Subset of configuration to pass initializing regionservers: e.g.
   * the filesystem to use and root directory to use.
   */
  protected RegionServerStartupResponse.Builder createConfigurationSubset() {
    RegionServerStartupResponse.Builder resp = addConfig(
      RegionServerStartupResponse.newBuilder(), HConstants.HBASE_DIR);
    resp = addConfig(resp, "fs.default.name");
    return addConfig(resp, "hbase.master.info.port");
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
      ClusterStatusProtos.ServerLoad sl = request.getLoad();
      ServerName serverName = ProtobufUtil.toServerName(request.getServer());
      ServerLoad oldLoad = serverManager.getLoad(serverName);
      this.serverManager.regionServerReport(serverName, new ServerLoad(sl));
      if (sl != null && this.metricsMaster != null) {
        // Up our metrics.
        this.metricsMaster.incrementRequests(sl.getTotalNumberOfRequests()
          - (oldLoad != null ? oldLoad.getTotalNumberOfRequests() : 0));
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
    String msg = "Region server " + sn +
      " reported a fatal error:\n" + errorText;
    LOG.error(msg);
    rsFatals.add(msg);

    return ReportRSFatalErrorResponse.newBuilder().build();
  }

  public boolean isMasterRunning() {
    return !isStopped();
  }

  @Override
  public IsMasterRunningResponse isMasterRunning(RpcController c, IsMasterRunningRequest req)
  throws ServiceException {
    return IsMasterRunningResponse.newBuilder().setIsMasterRunning(isMasterRunning()).build();
  }

  @Override
  public RunCatalogScanResponse runCatalogScan(RpcController c,
      RunCatalogScanRequest req) throws ServiceException {
    try {
      return ResponseConverter.buildRunCatalogScanResponse(catalogJanitorChore.scan());
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
      // No time period set so create one
      int balancerPeriod =
        getConfiguration().getInt("hbase.balancer.period", 300000);
      balancerCutoffTime = balancerPeriod;
      // If nonsense period, set it to balancerPeriod
      if (balancerCutoffTime <= 0) balancerCutoffTime = balancerPeriod;
    }
    return balancerCutoffTime;
  }

  public boolean balance() throws HBaseIOException {
    // if master not initialized, don't run balancer.
    if (!this.initialized) {
      LOG.debug("Master has not been initialized, don't run balancer.");
      return false;
    }
    // Do this call outside of synchronized block.
    int maximumBalanceTime = getBalancerCutoffTime();
    boolean balancerRan;
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
      balancerRan = plans != null;
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
            //TODO: After balance, there should not be a cutoff time (keeping it as a security net for now)
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
    return balancerRan;
  }

  @Override
  public BalanceResponse balance(RpcController c, BalanceRequest request) throws ServiceException {
    try {
      return BalanceResponse.newBuilder().setBalancerRan(balance()).build();
    } catch (HBaseIOException ex) {
      throw new ServiceException(ex);
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
      LOG.info(getClientIdAuditPrefix() + " set balanceSwitch=" + newValue);
      if (this.cpHost != null) {
        this.cpHost.postBalanceSwitch(oldValue, newValue);
      }
    } catch (IOException ioe) {
      LOG.warn("Error flipping balance switch", ioe);
    }
    return oldValue;
  }

  /**
   * @return Client info for use as prefix on an audit log string; who did an action
   */
  String getClientIdAuditPrefix() {
    return "Client=" + RpcServer.getRequestUserName() + "/" + RpcServer.getRemoteAddress();
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
    this.catalogJanitorChore.setEnabled(b);
  }

  @Override
  public DispatchMergingRegionsResponse dispatchMergingRegions(
      RpcController controller, DispatchMergingRegionsRequest request)
      throws ServiceException {
    final byte[] encodedNameOfRegionA = request.getRegionA().getValue()
        .toByteArray();
    final byte[] encodedNameOfRegionB = request.getRegionB().getValue()
        .toByteArray();
    final boolean forcible = request.getForcible();
    if (request.getRegionA().getType() != RegionSpecifierType.ENCODED_REGION_NAME
        || request.getRegionB().getType() != RegionSpecifierType.ENCODED_REGION_NAME) {
      LOG.warn("mergeRegions specifier type: expected: "
          + RegionSpecifierType.ENCODED_REGION_NAME + " actual: region_a="
          + request.getRegionA().getType() + ", region_b="
          + request.getRegionB().getType());
    }
    RegionState regionStateA = assignmentManager.getRegionStates()
        .getRegionState(Bytes.toString(encodedNameOfRegionA));
    RegionState regionStateB = assignmentManager.getRegionStates()
        .getRegionState(Bytes.toString(encodedNameOfRegionB));
    if (regionStateA == null || regionStateB == null) {
      throw new ServiceException(new UnknownRegionException(
          Bytes.toStringBinary(regionStateA == null ? encodedNameOfRegionA
              : encodedNameOfRegionB)));
    }

    if (!regionStateA.isOpened() || !regionStateB.isOpened()) {
      throw new ServiceException(new MergeRegionException(
        "Unable to merge regions not online " + regionStateA + ", " + regionStateB));
    }

    HRegionInfo regionInfoA = regionStateA.getRegion();
    HRegionInfo regionInfoB = regionStateB.getRegion();
    if (regionInfoA.compareTo(regionInfoB) == 0) {
      throw new ServiceException(new MergeRegionException(
        "Unable to merge a region to itself " + regionInfoA + ", " + regionInfoB));
    }

    if (!forcible && !HRegionInfo.areAdjacent(regionInfoA, regionInfoB)) {
      throw new ServiceException(new MergeRegionException(
        "Unable to merge not adjacent regions "
          + regionInfoA.getRegionNameAsString() + ", "
          + regionInfoB.getRegionNameAsString()
          + " where forcible = " + forcible));
    }

    try {
      dispatchMergingRegions(regionInfoA, regionInfoB, forcible);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }

    return DispatchMergingRegionsResponse.newBuilder().build();
  }

  @Override
  public void dispatchMergingRegions(final HRegionInfo region_a,
      final HRegionInfo region_b, final boolean forcible) throws IOException {
    checkInitialized();
    this.executorService.submit(new DispatchMergingRegionHandler(this,
        this.catalogJanitorChore, region_a, region_b, forcible));
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

    try {
      move(encodedRegionName, destServerName);
    } catch (HBaseIOException ioe) {
      throw new ServiceException(ioe);
    }
    return mrr;
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
    } else {
      dest = ServerName.valueOf(Bytes.toString(destServerName));
      if (dest.equals(regionState.getServerName())) {
        LOG.debug("Skipping move of region " + hri.getRegionNameAsString()
          + " because region already assigned to the same server " + dest + ".");
        return;
      }
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
  public void createTable(HTableDescriptor hTableDescriptor,
    byte [][] splitKeys)
  throws IOException {
    if (!isMasterRunning()) {
      throw new MasterNotRunningException();
    }

    String namespace = hTableDescriptor.getTableName().getNamespaceAsString();
    getNamespaceDescriptor(namespace); // ensure namespace exists

    HRegionInfo[] newRegions = getHRegionInfos(hTableDescriptor, splitKeys);
    checkInitialized();
    sanityCheckTableDescriptor(hTableDescriptor);
    if (cpHost != null) {
      cpHost.preCreateTable(hTableDescriptor, newRegions);
    }
    LOG.info(getClientIdAuditPrefix() + " create " + hTableDescriptor);
    this.executorService.submit(new CreateTableHandler(this,
      this.fileSystemManager, hTableDescriptor, conf,
      newRegions, this).prepare());
    if (cpHost != null) {
      cpHost.postCreateTable(hTableDescriptor, newRegions);
    }

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

    // check that we have at least 1 CF
    if (htd.getColumnFamilies().length == 0) {
      String message = "Table should have at least one column family.";
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

      // check replication scope
      if (hcd.getScope() < 0) {
        String message = "Replication scope for column family "
          + hcd.getNameAsString() + "  must be positive.";
        warnOrThrowExceptionForFailure(logWarn, CONF_KEY, message, null);
      }

      // TODO: should we check coprocessors and encryption ?
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
          new HRegionInfo(hTableDescriptor.getTableName(), null, null)};
    } else {
      int numRegions = splitKeys.length + 1;
      hRegionInfos = new HRegionInfo[numRegions];
      byte[] startKey = null;
      byte[] endKey = null;
      for (int i = 0; i < numRegions; i++) {
        endKey = (i == splitKeys.length) ? null : splitKeys[i];
        hRegionInfos[i] =
            new HRegionInfo(hTableDescriptor.getTableName(), startKey, endKey);
        startKey = endKey;
      }
    }
    return hRegionInfos;
  }

  private static boolean isCatalogTable(final TableName tableName) {
    return tableName.equals(TableName.META_TABLE_NAME);
  }

  @Override
  public void deleteTable(final TableName tableName) throws IOException {
    checkInitialized();
    if (cpHost != null) {
      cpHost.preDeleteTable(tableName);
    }
    LOG.info(getClientIdAuditPrefix() + " delete " + tableName);
    this.executorService.submit(new DeleteTableHandler(tableName, this, this).prepare());
    if (cpHost != null) {
      cpHost.postDeleteTable(tableName);
    }
  }

  @Override
  public DeleteTableResponse deleteTable(RpcController controller, DeleteTableRequest request)
  throws ServiceException {
    try {
      deleteTable(ProtobufUtil.toTableName(request.getTableName()));
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
    TableName tableName = ProtobufUtil.toTableName(req.getTableName());

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
  public AddColumnResponse addColumn(RpcController controller, AddColumnRequest req)
  throws ServiceException {
    try {
      addColumn(ProtobufUtil.toTableName(req.getTableName()),
        HColumnDescriptor.convert(req.getColumnFamilies()));
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return AddColumnResponse.newBuilder().build();
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
  public ModifyColumnResponse modifyColumn(RpcController controller, ModifyColumnRequest req)
  throws ServiceException {
    try {
      modifyColumn(ProtobufUtil.toTableName(req.getTableName()),
        HColumnDescriptor.convert(req.getColumnFamilies()));
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return ModifyColumnResponse.newBuilder().build();
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
  public DeleteColumnResponse deleteColumn(RpcController controller, DeleteColumnRequest req)
  throws ServiceException {
    try {
      deleteColumn(ProtobufUtil.toTableName(req.getTableName()),
          req.getColumnName().toByteArray());
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return DeleteColumnResponse.newBuilder().build();
  }

  @Override
  public void enableTable(final TableName tableName) throws IOException {
    checkInitialized();
    if (cpHost != null) {
      cpHost.preEnableTable(tableName);
    }
    LOG.info(getClientIdAuditPrefix() + " enable " + tableName);
    this.executorService.submit(new EnableTableHandler(this, tableName,
      catalogTracker, assignmentManager, tableLockManager, false).prepare());
    if (cpHost != null) {
      cpHost.postEnableTable(tableName);
   }
  }

  @Override
  public EnableTableResponse enableTable(RpcController controller, EnableTableRequest request)
  throws ServiceException {
    try {
      enableTable(ProtobufUtil.toTableName(request.getTableName()));
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return EnableTableResponse.newBuilder().build();
  }

  @Override
  public void disableTable(final TableName tableName) throws IOException {
    checkInitialized();
    if (cpHost != null) {
      cpHost.preDisableTable(tableName);
    }
    LOG.info(getClientIdAuditPrefix() + " disable " + tableName);
    this.executorService.submit(new DisableTableHandler(this, tableName,
      catalogTracker, assignmentManager, tableLockManager, false).prepare());
    if (cpHost != null) {
      cpHost.postDisableTable(tableName);
    }
  }

  @Override
  public DisableTableResponse disableTable(RpcController controller, DisableTableRequest request)
  throws ServiceException {
    try {
      disableTable(ProtobufUtil.toTableName(request.getTableName()));
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

    MetaScanner.metaScan(conf, visitor, tableName, rowKey, 1);
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
  public ModifyTableResponse modifyTable(RpcController controller, ModifyTableRequest req)
  throws ServiceException {
    try {
      modifyTable(ProtobufUtil.toTableName(req.getTableName()),
        HTableDescriptor.convert(req.getTableSchema()));
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return ModifyTableResponse.newBuilder().build();
  }

  @Override
  public void checkTableModifiable(final TableName tableName)
      throws IOException, TableNotFoundException, TableNotDisabledException {
    if (isCatalogTable(tableName)) {
      throw new IOException("Can't modify catalog tables");
    }
    if (!MetaReader.tableExists(getCatalogTracker(), tableName)) {
      throw new TableNotFoundException(tableName);
    }
    if (!getAssignmentManager().getZKTable().
        isDisabledTable(tableName)) {
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
      @Override
      public int compare(ServerName s1, ServerName s2) {
        return s1.getServerName().compareTo(s2.getServerName());
      }});

    return new ClusterStatus(VersionInfo.getVersion(),
      this.fileSystemManager.getClusterId().toString(),
      this.serverManager.getOnlineServers(),
      this.serverManager.getDeadServers().copyServerNames(),
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
    Set<String> masterCoprocessors =
        getCoprocessorHost().getCoprocessors();
    return masterCoprocessors.toArray(new String[masterCoprocessors.size()]);
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
   * 4. Assign meta. (they are already assigned, but we need to update our
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
      @Override
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
          return !stopped;
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

  @Override
  public TableLockManager getTableLockManager() {
    return this.tableLockManager;
  }

  public MemoryBoundedLogMessageBuffer getRegionServerFatalLogBuffer() {
    return rsFatals;
  }

  public void shutdown() {
    if (spanReceiverHost != null) {
      spanReceiverHost.closeReceivers();
    }
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
      LOG.error("ZooKeeper exception trying to set cluster as down in ZK", e);
    }
  }

  @Override
  public ShutdownResponse shutdown(RpcController controller, ShutdownRequest request)
  throws ServiceException {
    LOG.info(getClientIdAuditPrefix() + " shutdown");
    shutdown();
    return ShutdownResponse.newBuilder().build();
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

  @Override
  public StopMasterResponse stopMaster(RpcController controller, StopMasterRequest request)
  throws ServiceException {
    LOG.info(getClientIdAuditPrefix() + " stop");
    stopMaster();
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
    // If no region server is online then master may stuck waiting on hbase:meta to come on line.
    // See HBASE-8422.
    if (this.catalogTracker != null && this.serverManager.getOnlineServers().isEmpty()) {
      this.catalogTracker.stop();
    }
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  @Override
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
      LOG.info(getClientIdAuditPrefix() + " assign " + regionInfo.getRegionNameAsString());
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
      LOG.debug(getClientIdAuditPrefix() + " unassign " + hri.getRegionNameAsString()
          + " in current location if it is online and reassign.force=" + force);
      this.assignmentManager.unassign(hri, force);
      if (this.assignmentManager.getRegionStates().isRegionOffline(hri)) {
        LOG.debug("Region " + hri.getRegionNameAsString()
            + " is not online on any region server, reassigning it.");
        assignRegion(hri);
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
  @Override
  public GetTableDescriptorsResponse getTableDescriptors(
	      RpcController controller, GetTableDescriptorsRequest req) throws ServiceException {
    List<HTableDescriptor> descriptors = new ArrayList<HTableDescriptor>();
    List<TableName> tableNameList = new ArrayList<TableName>();
    for(HBaseProtos.TableName tableNamePB: req.getTableNamesList()) {
      tableNameList.add(ProtobufUtil.toTableName(tableNamePB));
    }
    boolean bypass = false;
    if (this.cpHost != null) {
      try {
        bypass = this.cpHost.preGetTableDescriptors(tableNameList, descriptors);
      } catch (IOException ioe) {
        throw new ServiceException(ioe);
      }
    }

    if (!bypass) {
      if (req.getTableNamesCount() == 0) {
        // request for all TableDescriptors
        Map<String, HTableDescriptor> descriptorMap = null;
        try {
          descriptorMap = this.tableDescriptors.getAll();
        } catch (IOException e) {
          LOG.warn("Failed getting all descriptors", e);
        }
        if (descriptorMap != null) {
          for(HTableDescriptor desc: descriptorMap.values()) {
            if(!desc.getTableName().isSystemTable()) {
              descriptors.add(desc);
            }
          }
        }
      } else {
        for (TableName s: tableNameList) {
          try {
            HTableDescriptor desc = this.tableDescriptors.get(s);
            if (desc != null) {
              descriptors.add(desc);
            }
          } catch (IOException e) {
            LOG.warn("Failed getting descriptor for " + s, e);
          }
        }
      }

      if (this.cpHost != null) {
        try {
          this.cpHost.postGetTableDescriptors(descriptors);
        } catch (IOException ioe) {
          throw new ServiceException(ioe);
        }
      }
    }

    GetTableDescriptorsResponse.Builder builder = GetTableDescriptorsResponse.newBuilder();
    for (HTableDescriptor htd: descriptors) {
      builder.addTableSchema(htd.convert());
    }
    return builder.build();
  }

  /**
   * Get list of userspace table names
   * @param controller Unused (set to null).
   * @param req GetTableNamesRequest
   * @return GetTableNamesResponse
   * @throws ServiceException
   */
  @Override
  public GetTableNamesResponse getTableNames(
        RpcController controller, GetTableNamesRequest req) throws ServiceException {
    try {
      Collection<HTableDescriptor> descriptors = this.tableDescriptors.getAll().values();
      GetTableNamesResponse.Builder builder = GetTableNamesResponse.newBuilder();
      for (HTableDescriptor descriptor: descriptors) {
        if (descriptor.getTableName().isSystemTable()) {
          continue;
        }
        builder.addTableNames(ProtobufUtil.toProtoTableName(descriptor.getTableName()));
      }
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
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
   * Offline specified region from master's in-memory state. It will not attempt to
   * reassign the region as in unassign.
   *
   * This is a special method that should be used by experts or hbck.
   *
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
      if (cpHost != null) {
        cpHost.preRegionOffline(hri);
      }
      LOG.info(getClientIdAuditPrefix() + " offline " + hri.getRegionNameAsString());
      this.assignmentManager.regionOffline(hri);
      if (cpHost != null) {
        cpHost.postRegionOffline(hri);
      }
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
  public SnapshotResponse snapshot(RpcController controller, SnapshotRequest request)
      throws ServiceException {
    try {
      this.snapshotManager.checkSnapshotSupport();
    } catch (UnsupportedOperationException e) {
      throw new ServiceException(e);
    }

    LOG.info(getClientIdAuditPrefix() + " snapshot request for:" +
        ClientSnapshotDescriptionUtils.toString(request.getSnapshot()));
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
    return SnapshotResponse.newBuilder().setExpectedTimeout(waitTime).build();
  }

  /**
   * List the currently available/stored snapshots. Any in-progress snapshots are ignored
   */
  @Override
  public GetCompletedSnapshotsResponse getCompletedSnapshots(RpcController controller,
      GetCompletedSnapshotsRequest request) throws ServiceException {
    try {
      GetCompletedSnapshotsResponse.Builder builder = GetCompletedSnapshotsResponse.newBuilder();
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
      LOG.info(getClientIdAuditPrefix() + " delete " + request.getSnapshot());
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
        ClientSnapshotDescriptionUtils.toString(request.getSnapshot()) + " is done");
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

    // ensure namespace exists
    try {
      TableName dstTable = TableName.valueOf(request.getSnapshot().getTable());
      getNamespaceDescriptor(dstTable.getNamespaceAsString());
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
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
   * @throws ServiceException if the operation failed.
   */
  @Override
  public IsRestoreSnapshotDoneResponse isRestoreSnapshotDone(RpcController controller,
      IsRestoreSnapshotDoneRequest request) throws ServiceException {
    try {
      SnapshotDescription snapshot = request.getSnapshot();
      IsRestoreSnapshotDoneResponse.Builder builder = IsRestoreSnapshotDoneResponse.newBuilder();
      boolean done = snapshotManager.isRestoreDone(snapshot);
      builder.setDone(done);
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * Triggers an asynchronous attempt to run a distributed procedure.
   * {@inheritDoc}
   */
  @Override
  public ExecProcedureResponse execProcedure(RpcController controller,
      ExecProcedureRequest request) throws ServiceException {
    ProcedureDescription desc = request.getProcedure();
    MasterProcedureManager mpm = this.mpmHost.getProcedureManager(desc
        .getSignature());
    if (mpm == null) {
      throw new ServiceException("The procedure is not registered: "
          + desc.getSignature());
    }

    LOG.info(getClientIdAuditPrefix() + " procedure request for: "
        + desc.getSignature());

    try {
      mpm.execProcedure(desc);
    } catch (IOException e) {
      throw new ServiceException(e);
    }

    // send back the max amount of time the client should wait for the procedure
    // to complete
    long waitTime = SnapshotDescriptionUtils.DEFAULT_MAX_WAIT_TIME;
    return ExecProcedureResponse.newBuilder().setExpectedTimeout(waitTime)
        .build();
  }

  /**
   * Checks if the specified procedure is done.
   * @return true if the procedure is done,
   *   false if the procedure is in the process of completing
   * @throws ServiceException if invalid procedure, or
   *  a failed procedure with progress failure reason.
   */
  @Override
  public IsProcedureDoneResponse isProcedureDone(RpcController controller,
      IsProcedureDoneRequest request) throws ServiceException {
    ProcedureDescription desc = request.getProcedure();
    MasterProcedureManager mpm = this.mpmHost.getProcedureManager(desc
        .getSignature());
    if (mpm == null) {
      throw new ServiceException("The procedure is not registered: "
          + desc.getSignature());
    }
    LOG.debug("Checking to see if procedure from request:"
        + desc.getSignature() + " is done");

    try {
      IsProcedureDoneResponse.Builder builder = IsProcedureDoneResponse
          .newBuilder();
      boolean done = mpm.isProcedureDone(desc);
      builder.setDone(done);
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ModifyNamespaceResponse modifyNamespace(RpcController controller,
      ModifyNamespaceRequest request) throws ServiceException {
    try {
      modifyNamespace(ProtobufUtil.toNamespaceDescriptor(request.getNamespaceDescriptor()));
      return ModifyNamespaceResponse.getDefaultInstance();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public CreateNamespaceResponse createNamespace(RpcController controller,
     CreateNamespaceRequest request) throws ServiceException {
    try {
      createNamespace(ProtobufUtil.toNamespaceDescriptor(request.getNamespaceDescriptor()));
      return CreateNamespaceResponse.getDefaultInstance();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public DeleteNamespaceResponse deleteNamespace(RpcController controller,
      DeleteNamespaceRequest request) throws ServiceException {
    try {
      deleteNamespace(request.getNamespaceName());
      return DeleteNamespaceResponse.getDefaultInstance();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public GetNamespaceDescriptorResponse getNamespaceDescriptor(
      RpcController controller, GetNamespaceDescriptorRequest request)
      throws ServiceException {
    try {
      return GetNamespaceDescriptorResponse.newBuilder()
          .setNamespaceDescriptor(
              ProtobufUtil.toProtoNamespaceDescriptor(getNamespaceDescriptor(request.getNamespaceName())))
          .build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListNamespaceDescriptorsResponse listNamespaceDescriptors(
      RpcController controller, ListNamespaceDescriptorsRequest request)
      throws ServiceException {
    try {
      ListNamespaceDescriptorsResponse.Builder response =
          ListNamespaceDescriptorsResponse.newBuilder();
      for(NamespaceDescriptor ns: listNamespaceDescriptors()) {
        response.addNamespaceDescriptor(ProtobufUtil.toProtoNamespaceDescriptor(ns));
      }
      return response.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListTableDescriptorsByNamespaceResponse listTableDescriptorsByNamespace(
      RpcController controller, ListTableDescriptorsByNamespaceRequest request)
      throws ServiceException {
    try {
      ListTableDescriptorsByNamespaceResponse.Builder b =
          ListTableDescriptorsByNamespaceResponse.newBuilder();
      for(HTableDescriptor htd: listTableDescriptorsByNamespace(request.getNamespaceName())) {
        b.addTableSchema(htd.convert());
      }
      return b.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public ListTableNamesByNamespaceResponse listTableNamesByNamespace(
      RpcController controller, ListTableNamesByNamespaceRequest request)
      throws ServiceException {
    try {
      ListTableNamesByNamespaceResponse.Builder b =
          ListTableNamesByNamespaceResponse.newBuilder();
      for (TableName tableName: listTableNamesByNamespace(request.getNamespaceName())) {
        b.addTableName(ProtobufUtil.toProtoTableName(tableName));
      }
      return b.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  private boolean isHealthCheckerConfigured() {
    String healthScriptLocation = this.conf.get(HConstants.HEALTH_SCRIPT_LOC);
    return org.apache.commons.lang.StringUtils.isNotBlank(healthScriptLocation);
  }

  @Override
  public void createNamespace(NamespaceDescriptor descriptor) throws IOException {
    TableName.isLegalNamespaceName(Bytes.toBytes(descriptor.getName()));
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

  @Override
  public NamespaceDescriptor getNamespaceDescriptor(String name) throws IOException {
    boolean ready = tableNamespaceManager != null &&
        tableNamespaceManager.isTableAvailableAndInitialized();
    if (!ready) {
      throw new IOException("Table Namespace Manager not ready yet, try again later");
    }
    NamespaceDescriptor nsd = tableNamespaceManager.get(name);
    if (nsd == null) {
      throw new NamespaceNotFoundException(name);
    }
    return nsd;
  }

  @Override
  public List<NamespaceDescriptor> listNamespaceDescriptors() throws IOException {
    return Lists.newArrayList(tableNamespaceManager.list());
  }

  @Override
  public List<HTableDescriptor> listTableDescriptorsByNamespace(String name) throws IOException {
    getNamespaceDescriptor(name); // check that namespace exists
    return Lists.newArrayList(tableDescriptors.getByNamespace(name).values());
  }

  @Override
  public List<TableName> listTableNamesByNamespace(String name) throws IOException {
    List<TableName> tableNames = Lists.newArrayList();
    getNamespaceDescriptor(name); // check that namespace exists
    for (HTableDescriptor descriptor: tableDescriptors.getByNamespace(name).values()) {
      tableNames.add(descriptor.getTableName());
    }
    return tableNames;
  }

  @Override
  public ReportRegionStateTransitionResponse reportRegionStateTransition(RpcController controller,
      ReportRegionStateTransitionRequest req) throws ServiceException {
    try {
      RegionStateTransition rt = req.getTransition(0);
      TableName tableName = ProtobufUtil.toTableName(
        rt.getRegionInfo(0).getTableName());
      RegionStates regionStates = assignmentManager.getRegionStates();
      if (!(TableName.META_TABLE_NAME.equals(tableName)
          && regionStates.getRegionState(HRegionInfo.FIRST_META_REGIONINFO) != null)
          && !assignmentManager.isFailoverCleanupDone()) {
        // Meta region is assigned before master finishes the
        // failover cleanup. So no need this check for it
        throw new PleaseHoldException("Master is rebuilding user regions");
      }
      ServerName sn = ProtobufUtil.toServerName(req.getServer());
      String error = assignmentManager.onRegionTransition(sn, rt);
      ReportRegionStateTransitionResponse.Builder rrtr =
        ReportRegionStateTransitionResponse.newBuilder();
      if (error != null) {
        rrtr.setErrorMessage(error);
      }
      return rrtr.build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
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
  public TruncateTableResponse truncateTable(RpcController controller, TruncateTableRequest request)
      throws ServiceException {
    try {
      truncateTable(ProtobufUtil.toTableName(request.getTableName()), request.getPreserveSplits());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return TruncateTableResponse.newBuilder().build();
  }

}
