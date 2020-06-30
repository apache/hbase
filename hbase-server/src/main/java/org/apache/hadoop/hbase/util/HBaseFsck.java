/*
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
package org.apache.hadoop.hbase.util;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ClientMetaTableAccessor;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.io.FileLink;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator;
import org.apache.hadoop.hbase.util.HbckErrorReporter.ERROR_CODE;
import org.apache.hadoop.hbase.util.hbck.HFileCorruptionChecker;
import org.apache.hadoop.hbase.util.hbck.ReplicationChecker;
import org.apache.hadoop.hbase.util.hbck.TableIntegrityErrorHandler;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.base.Joiner;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * HBaseFsck (hbck) is a tool for checking and repairing region consistency and
 * table integrity problems in a corrupted HBase. This tool was written for hbase-1.x. It does not
 * work with hbase-2.x; it can read state but is not allowed to change state; i.e. effect 'repair'.
 * Even though it can 'read' state, given how so much has changed in how hbase1 and hbase2 operate,
 * it will often misread. See hbck2 (HBASE-19121) for a hbck tool for hbase2. This class is
 * deprecated.
 *
 * <p>
 * Region consistency checks verify that hbase:meta, region deployment on region
 * servers and the state of data in HDFS (.regioninfo files) all are in
 * accordance.
 * <p>
 * Table integrity checks verify that all possible row keys resolve to exactly
 * one region of a table.  This means there are no individual degenerate
 * or backwards regions; no holes between regions; and that there are no
 * overlapping regions.
 * <p>
 * The general repair strategy works in two phases:
 * <ol>
 * <li> Repair Table Integrity on HDFS. (merge or fabricate regions)
 * <li> Repair Region Consistency with hbase:meta and assignments
 * </ol>
 * <p>
 * For table integrity repairs, the tables' region directories are scanned
 * for .regioninfo files.  Each table's integrity is then verified.  If there
 * are any orphan regions (regions with no .regioninfo files) or holes, new
 * regions are fabricated.  Backwards regions are sidelined as well as empty
 * degenerate (endkey==startkey) regions.  If there are any overlapping regions,
 * a new region is created and all data is merged into the new region.
 * <p>
 * Table integrity repairs deal solely with HDFS and could potentially be done
 * offline -- the hbase region servers or master do not need to be running.
 * This phase can eventually be used to completely reconstruct the hbase:meta table in
 * an offline fashion.
 * <p>
 * Region consistency requires three conditions -- 1) valid .regioninfo file
 * present in an HDFS region dir,  2) valid row with .regioninfo data in META,
 * and 3) a region is deployed only at the regionserver that was assigned to
 * with proper state in the master.
 * <p>
 * Region consistency repairs require hbase to be online so that hbck can
 * contact the HBase master and region servers.  The hbck#connect() method must
 * first be called successfully.  Much of the region consistency information
 * is transient and less risky to repair.
 * <p>
 * If hbck is run from the command line, there are a handful of arguments that
 * can be used to limit the kinds of repairs hbck will do.  See the code in
 * {@link #printUsageAndExit()} for more details.
 * @deprecated For removal in hbase-4.0.0. Use HBCK2 instead.
 */
@Deprecated
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@InterfaceStability.Evolving
public class HBaseFsck extends Configured implements Closeable {
  public static final long DEFAULT_TIME_LAG = 60000; // default value of 1 minute
  public static final long DEFAULT_SLEEP_BEFORE_RERUN = 10000;
  private static final int MAX_NUM_THREADS = 50; // #threads to contact regions
  private static boolean rsSupportsOffline = true;
  private static final int DEFAULT_OVERLAPS_TO_SIDELINE = 2;
  private static final int DEFAULT_MAX_MERGE = 5;

  /**
   * Here is where hbase-1.x used to default the lock for hbck1.
   * It puts in place a lock when it goes to write/make changes.
   */
  @VisibleForTesting
  public static final String HBCK_LOCK_FILE = "hbase-hbck.lock";
  private static final int DEFAULT_MAX_LOCK_FILE_ATTEMPTS = 5;
  private static final int DEFAULT_LOCK_FILE_ATTEMPT_SLEEP_INTERVAL = 200; // milliseconds
  private static final int DEFAULT_LOCK_FILE_ATTEMPT_MAX_SLEEP_TIME = 5000; // milliseconds
  // We have to set the timeout value > HdfsConstants.LEASE_SOFTLIMIT_PERIOD.
  // In HADOOP-2.6 and later, the Namenode proxy now created with custom RetryPolicy for
  // AlreadyBeingCreatedException which is implies timeout on this operations up to
  // HdfsConstants.LEASE_SOFTLIMIT_PERIOD (60 seconds).
  private static final int DEFAULT_WAIT_FOR_LOCK_TIMEOUT = 80; // seconds
  private static final int DEFAULT_MAX_CREATE_ZNODE_ATTEMPTS = 5;
  private static final int DEFAULT_CREATE_ZNODE_ATTEMPT_SLEEP_INTERVAL = 200; // milliseconds
  private static final int DEFAULT_CREATE_ZNODE_ATTEMPT_MAX_SLEEP_TIME = 5000; // milliseconds

  /**********************
   * Internal resources
   **********************/
  private static final Logger LOG = LoggerFactory.getLogger(HBaseFsck.class.getName());
  private ClusterMetrics status;
  private Connection connection;
  private Admin admin;
  private Table meta;
  // threads to do ||izable tasks: retrieve data from regionservers, handle overlapping regions
  protected ExecutorService executor;
  private long startMillis = EnvironmentEdgeManager.currentTime();
  private HFileCorruptionChecker hfcc;
  private int retcode = 0;
  private Path HBCK_LOCK_PATH;
  private FSDataOutputStream hbckOutFd;
  // This lock is to prevent cleanup of balancer resources twice between
  // ShutdownHook and the main code. We cleanup only if the connect() is
  // successful
  private final AtomicBoolean hbckLockCleanup = new AtomicBoolean(false);

  // Unsupported options in HBase 2.0+
  private static final Set<String> unsupportedOptionsInV2 = Sets.newHashSet("-fix",
      "-fixAssignments", "-fixMeta", "-fixHdfsHoles", "-fixHdfsOrphans", "-fixTableOrphans",
      "-fixHdfsOverlaps", "-sidelineBigOverlaps", "-fixSplitParents", "-removeParents",
      "-fixEmptyMetaCells", "-repair", "-repairHoles", "-maxOverlapsToSideline", "-maxMerge");

  /***********
   * Options
   ***********/
  private static boolean details = false; // do we display the full report
  private long timelag = DEFAULT_TIME_LAG; // tables whose modtime is older
  private static boolean forceExclusive = false; // only this hbck can modify HBase
  private boolean fixAssignments = false; // fix assignment errors?
  private boolean fixMeta = false; // fix meta errors?
  private boolean checkHdfs = true; // load and check fs consistency?
  private boolean fixHdfsHoles = false; // fix fs holes?
  private boolean fixHdfsOverlaps = false; // fix fs overlaps (risky)
  private boolean fixHdfsOrphans = false; // fix fs holes (missing .regioninfo)
  private boolean fixTableOrphans = false; // fix fs holes (missing .tableinfo)
  private boolean fixVersionFile = false; // fix missing hbase.version file in hdfs
  private boolean fixSplitParents = false; // fix lingering split parents
  private boolean removeParents = false; // remove split parents
  private boolean fixReferenceFiles = false; // fix lingering reference store file
  private boolean fixHFileLinks = false; // fix lingering HFileLinks
  private boolean fixEmptyMetaCells = false; // fix (remove) empty REGIONINFO_QUALIFIER rows
  private boolean fixReplication = false; // fix undeleted replication queues for removed peer
  private boolean cleanReplicationBarrier = false; // clean replication barriers of a table
  private boolean fixAny = false; // Set to true if any of the fix is required.

  // limit checking/fixes to listed tables, if empty attempt to check/fix all
  // hbase:meta are always checked
  private Set<TableName> tablesIncluded = new HashSet<>();
  private TableName cleanReplicationBarrierTable;
  private int maxMerge = DEFAULT_MAX_MERGE; // maximum number of overlapping regions to merge
  // maximum number of overlapping regions to sideline
  private int maxOverlapsToSideline = DEFAULT_OVERLAPS_TO_SIDELINE;
  private boolean sidelineBigOverlaps = false; // sideline overlaps with >maxMerge regions
  private Path sidelineDir = null;

  private boolean rerun = false; // if we tried to fix something, rerun hbck
  private static boolean summary = false; // if we want to print less output
  private boolean checkMetaOnly = false;
  private boolean checkRegionBoundaries = false;
  private boolean ignorePreCheckPermission = false; // if pre-check permission

  /*********
   * State
   *********/
  final private HbckErrorReporter errors;
  int fixes = 0;

  /**
   * This map contains the state of all hbck items.  It maps from encoded region
   * name to HbckRegionInfo structure.  The information contained in HbckRegionInfo is used
   * to detect and correct consistency (hdfs/meta/deployment) problems.
   */
  private TreeMap<String, HbckRegionInfo> regionInfoMap = new TreeMap<>();
  // Empty regioninfo qualifiers in hbase:meta
  private Set<Result> emptyRegionInfoQualifiers = new HashSet<>();

  /**
   * This map from Tablename -> TableInfo contains the structures necessary to
   * detect table consistency problems (holes, dupes, overlaps).  It is sorted
   * to prevent dupes.
   *
   * If tablesIncluded is empty, this map contains all tables.
   * Otherwise, it contains only meta tables and tables in tablesIncluded,
   * unless checkMetaOnly is specified, in which case, it contains only
   * the meta table
   */
  private SortedMap<TableName, HbckTableInfo> tablesInfo = new ConcurrentSkipListMap<>();

  /**
   * When initially looking at HDFS, we attempt to find any orphaned data.
   */
  private List<HbckRegionInfo> orphanHdfsDirs = Collections.synchronizedList(new ArrayList<>());

  private Map<TableName, Set<String>> orphanTableDirs = new HashMap<>();
  private Map<TableName, TableState> tableStates = new HashMap<>();
  private final RetryCounterFactory lockFileRetryCounterFactory;
  private final RetryCounterFactory createZNodeRetryCounterFactory;

  private Map<TableName, Set<String>> skippedRegions = new HashMap<>();

  private ZKWatcher zkw = null;
  private String hbckEphemeralNodePath = null;
  private boolean hbckZodeCreated = false;

  /**
   * Constructor
   *
   * @param conf Configuration object
   * @throws MasterNotRunningException if the master is not running
   * @throws ZooKeeperConnectionException if unable to connect to ZooKeeper
   */
  public HBaseFsck(Configuration conf) throws IOException, ClassNotFoundException {
    this(conf, createThreadPool(conf));
  }

  private static ExecutorService createThreadPool(Configuration conf) {
    int numThreads = conf.getInt("hbasefsck.numthreads", MAX_NUM_THREADS);
    return new ScheduledThreadPoolExecutor(numThreads, Threads.newDaemonThreadFactory("hbasefsck"));
  }

  /**
   * Constructor
   *
   * @param conf
   *          Configuration object
   * @throws MasterNotRunningException
   *           if the master is not running
   * @throws ZooKeeperConnectionException
   *           if unable to connect to ZooKeeper
   */
  public HBaseFsck(Configuration conf, ExecutorService exec) throws MasterNotRunningException,
      ZooKeeperConnectionException, IOException, ClassNotFoundException {
    super(conf);
    errors = getErrorReporter(getConf());
    this.executor = exec;
    lockFileRetryCounterFactory = createLockRetryCounterFactory(getConf());
    createZNodeRetryCounterFactory = createZnodeRetryCounterFactory(getConf());
    zkw = createZooKeeperWatcher();
  }

  /**
   * @return A retry counter factory configured for retrying lock file creation.
   */
  public static RetryCounterFactory createLockRetryCounterFactory(Configuration conf) {
    return new RetryCounterFactory(
        conf.getInt("hbase.hbck.lockfile.attempts", DEFAULT_MAX_LOCK_FILE_ATTEMPTS),
        conf.getInt("hbase.hbck.lockfile.attempt.sleep.interval",
            DEFAULT_LOCK_FILE_ATTEMPT_SLEEP_INTERVAL),
        conf.getInt("hbase.hbck.lockfile.attempt.maxsleeptime",
            DEFAULT_LOCK_FILE_ATTEMPT_MAX_SLEEP_TIME));
  }

  /**
   * @return A retry counter factory configured for retrying znode creation.
   */
  private static RetryCounterFactory createZnodeRetryCounterFactory(Configuration conf) {
    return new RetryCounterFactory(
        conf.getInt("hbase.hbck.createznode.attempts", DEFAULT_MAX_CREATE_ZNODE_ATTEMPTS),
        conf.getInt("hbase.hbck.createznode.attempt.sleep.interval",
            DEFAULT_CREATE_ZNODE_ATTEMPT_SLEEP_INTERVAL),
        conf.getInt("hbase.hbck.createznode.attempt.maxsleeptime",
            DEFAULT_CREATE_ZNODE_ATTEMPT_MAX_SLEEP_TIME));
  }

  /**
   * @return Return the tmp dir this tool writes too.
   */
  @VisibleForTesting
  public static Path getTmpDir(Configuration conf) throws IOException {
    return new Path(CommonFSUtils.getRootDir(conf), HConstants.HBASE_TEMP_DIRECTORY);
  }

  private static class FileLockCallable implements Callable<FSDataOutputStream> {
    RetryCounter retryCounter;
    private final Configuration conf;
    private Path hbckLockPath = null;

    public FileLockCallable(Configuration conf, RetryCounter retryCounter) {
      this.retryCounter = retryCounter;
      this.conf = conf;
    }

    /**
     * @return Will be <code>null</code> unless you call {@link #call()}
     */
    Path getHbckLockPath() {
      return this.hbckLockPath;
    }

    @Override
    public FSDataOutputStream call() throws IOException {
      try {
        FileSystem fs = CommonFSUtils.getCurrentFileSystem(this.conf);
        FsPermission defaultPerms =
          CommonFSUtils.getFilePermissions(fs, this.conf, HConstants.DATA_FILE_UMASK_KEY);
        Path tmpDir = getTmpDir(conf);
        this.hbckLockPath = new Path(tmpDir, HBCK_LOCK_FILE);
        fs.mkdirs(tmpDir);
        final FSDataOutputStream out = createFileWithRetries(fs, this.hbckLockPath, defaultPerms);
        out.writeBytes(InetAddress.getLocalHost().toString());
        // Add a note into the file we write on why hbase2 is writing out an hbck1 lock file.
        out.writeBytes(" Written by an hbase-2.x Master to block an " +
            "attempt by an hbase-1.x HBCK tool making modification to state. " +
            "See 'HBCK must match HBase server version' in the hbase refguide.");
        out.flush();
        return out;
      } catch(RemoteException e) {
        if(AlreadyBeingCreatedException.class.getName().equals(e.getClassName())){
          return null;
        } else {
          throw e;
        }
      }
    }

    private FSDataOutputStream createFileWithRetries(final FileSystem fs,
        final Path hbckLockFilePath, final FsPermission defaultPerms)
        throws IOException {
      IOException exception = null;
      do {
        try {
          return CommonFSUtils.create(fs, hbckLockFilePath, defaultPerms, false);
        } catch (IOException ioe) {
          LOG.info("Failed to create lock file " + hbckLockFilePath.getName()
              + ", try=" + (retryCounter.getAttemptTimes() + 1) + " of "
              + retryCounter.getMaxAttempts());
          LOG.debug("Failed to create lock file " + hbckLockFilePath.getName(),
              ioe);
          try {
            exception = ioe;
            retryCounter.sleepUntilNextRetry();
          } catch (InterruptedException ie) {
            throw (InterruptedIOException) new InterruptedIOException(
                "Can't create lock file " + hbckLockFilePath.getName())
            .initCause(ie);
          }
        }
      } while (retryCounter.shouldRetry());

      throw exception;
    }
  }

  /**
   * This method maintains a lock using a file. If the creation fails we return null
   *
   * @return FSDataOutputStream object corresponding to the newly opened lock file
   * @throws IOException if IO failure occurs
   */
  public static Pair<Path, FSDataOutputStream> checkAndMarkRunningHbck(Configuration conf,
      RetryCounter retryCounter) throws IOException {
    FileLockCallable callable = new FileLockCallable(conf, retryCounter);
    ExecutorService executor = Executors.newFixedThreadPool(1);
    FutureTask<FSDataOutputStream> futureTask = new FutureTask<>(callable);
    executor.execute(futureTask);
    final int timeoutInSeconds = conf.getInt(
      "hbase.hbck.lockfile.maxwaittime", DEFAULT_WAIT_FOR_LOCK_TIMEOUT);
    FSDataOutputStream stream = null;
    try {
      stream = futureTask.get(timeoutInSeconds, TimeUnit.SECONDS);
    } catch (ExecutionException ee) {
      LOG.warn("Encountered exception when opening lock file", ee);
    } catch (InterruptedException ie) {
      LOG.warn("Interrupted when opening lock file", ie);
      Thread.currentThread().interrupt();
    } catch (TimeoutException exception) {
      // took too long to obtain lock
      LOG.warn("Took more than " + timeoutInSeconds + " seconds in obtaining lock");
      futureTask.cancel(true);
    } finally {
      executor.shutdownNow();
    }
    return new Pair<Path, FSDataOutputStream>(callable.getHbckLockPath(), stream);
  }

  private void unlockHbck() {
    if (isExclusive() && hbckLockCleanup.compareAndSet(true, false)) {
      RetryCounter retryCounter = lockFileRetryCounterFactory.create();
      do {
        try {
          IOUtils.closeQuietly(hbckOutFd);
          CommonFSUtils.delete(CommonFSUtils.getCurrentFileSystem(getConf()), HBCK_LOCK_PATH, true);
          LOG.info("Finishing hbck");
          return;
        } catch (IOException ioe) {
          LOG.info("Failed to delete " + HBCK_LOCK_PATH + ", try="
              + (retryCounter.getAttemptTimes() + 1) + " of "
              + retryCounter.getMaxAttempts());
          LOG.debug("Failed to delete " + HBCK_LOCK_PATH, ioe);
          try {
            retryCounter.sleepUntilNextRetry();
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while deleting lock file" +
                HBCK_LOCK_PATH);
            return;
          }
        }
      } while (retryCounter.shouldRetry());
    }
  }

  /**
   * To repair region consistency, one must call connect() in order to repair
   * online state.
   */
  public void connect() throws IOException {

    if (isExclusive()) {
      // Grab the lock
      Pair<Path, FSDataOutputStream> pair =
          checkAndMarkRunningHbck(getConf(), this.lockFileRetryCounterFactory.create());
      HBCK_LOCK_PATH = pair.getFirst();
      this.hbckOutFd = pair.getSecond();
      if (hbckOutFd == null) {
        setRetCode(-1);
        LOG.error("Another instance of hbck is fixing HBase, exiting this instance. " +
            "[If you are sure no other instance is running, delete the lock file " +
            HBCK_LOCK_PATH + " and rerun the tool]");
        throw new IOException("Duplicate hbck - Abort");
      }

      // Make sure to cleanup the lock
      hbckLockCleanup.set(true);
    }


    // Add a shutdown hook to this thread, in case user tries to
    // kill the hbck with a ctrl-c, we want to cleanup the lock so that
    // it is available for further calls
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        IOUtils.closeQuietly(HBaseFsck.this);
        cleanupHbckZnode();
        unlockHbck();
      }
    });

    LOG.info("Launching hbck");

    connection = ConnectionFactory.createConnection(getConf());
    admin = connection.getAdmin();
    meta = connection.getTable(TableName.META_TABLE_NAME);
    status = admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS,
      Option.DEAD_SERVERS, Option.MASTER, Option.BACKUP_MASTERS,
      Option.REGIONS_IN_TRANSITION, Option.HBASE_VERSION));
  }

  /**
   * Get deployed regions according to the region servers.
   */
  private void loadDeployedRegions() throws IOException, InterruptedException {
    // From the master, get a list of all known live region servers
    Collection<ServerName> regionServers = status.getLiveServerMetrics().keySet();
    errors.print("Number of live region servers: " + regionServers.size());
    if (details) {
      for (ServerName rsinfo: regionServers) {
        errors.print("  " + rsinfo.getServerName());
      }
    }

    // From the master, get a list of all dead region servers
    Collection<ServerName> deadRegionServers = status.getDeadServerNames();
    errors.print("Number of dead region servers: " + deadRegionServers.size());
    if (details) {
      for (ServerName name: deadRegionServers) {
        errors.print("  " + name);
      }
    }

    // Print the current master name and state
    errors.print("Master: " + status.getMasterName());

    // Print the list of all backup masters
    Collection<ServerName> backupMasters = status.getBackupMasterNames();
    errors.print("Number of backup masters: " + backupMasters.size());
    if (details) {
      for (ServerName name: backupMasters) {
        errors.print("  " + name);
      }
    }

    errors.print("Average load: " + status.getAverageLoad());
    errors.print("Number of requests: " + status.getRequestCount());
    errors.print("Number of regions: " + status.getRegionCount());

    List<RegionState> rits = status.getRegionStatesInTransition();
    errors.print("Number of regions in transition: " + rits.size());
    if (details) {
      for (RegionState state: rits) {
        errors.print("  " + state.toDescriptiveString());
      }
    }

    // Determine what's deployed
    processRegionServers(regionServers);
  }

  /**
   * Clear the current state of hbck.
   */
  private void clearState() {
    // Make sure regionInfo is empty before starting
    fixes = 0;
    regionInfoMap.clear();
    emptyRegionInfoQualifiers.clear();
    tableStates.clear();
    errors.clear();
    tablesInfo.clear();
    orphanHdfsDirs.clear();
    skippedRegions.clear();
  }

  /**
   * This repair method analyzes hbase data in hdfs and repairs it to satisfy
   * the table integrity rules.  HBase doesn't need to be online for this
   * operation to work.
   */
  public void offlineHdfsIntegrityRepair() throws IOException, InterruptedException {
    // Initial pass to fix orphans.
    if (shouldCheckHdfs() && (shouldFixHdfsOrphans() || shouldFixHdfsHoles()
        || shouldFixHdfsOverlaps() || shouldFixTableOrphans())) {
      LOG.info("Loading regioninfos HDFS");
      // if nothing is happening this should always complete in two iterations.
      int maxIterations = getConf().getInt("hbase.hbck.integrityrepair.iterations.max", 3);
      int curIter = 0;
      do {
        clearState(); // clears hbck state and reset fixes to 0 and.
        // repair what's on HDFS
        restoreHdfsIntegrity();
        curIter++;// limit the number of iterations.
      } while (fixes > 0 && curIter <= maxIterations);

      // Repairs should be done in the first iteration and verification in the second.
      // If there are more than 2 passes, something funny has happened.
      if (curIter > 2) {
        if (curIter == maxIterations) {
          LOG.warn("Exiting integrity repairs after max " + curIter + " iterations. "
              + "Tables integrity may not be fully repaired!");
        } else {
          LOG.info("Successfully exiting integrity repairs after " + curIter + " iterations");
        }
      }
    }
  }

  /**
   * This repair method requires the cluster to be online since it contacts
   * region servers and the masters.  It makes each region's state in HDFS, in
   * hbase:meta, and deployments consistent.
   *
   * @return If &gt; 0 , number of errors detected, if &lt; 0 there was an unrecoverable
   *     error.  If 0, we have a clean hbase.
   */
  public int onlineConsistencyRepair() throws IOException, KeeperException,
    InterruptedException {

    // get regions according to what is online on each RegionServer
    loadDeployedRegions();
    // check whether hbase:meta is deployed and online
    recordMetaRegion();
    // Check if hbase:meta is found only once and in the right place
    if (!checkMetaRegion()) {
      String errorMsg = "hbase:meta table is not consistent. ";
      if (shouldFixAssignments()) {
        errorMsg += "HBCK will try fixing it. Rerun once hbase:meta is back to consistent state.";
      } else {
        errorMsg += "Run HBCK with proper fix options to fix hbase:meta inconsistency.";
      }
      errors.reportError(errorMsg + " Exiting...");
      return -2;
    }
    // Not going with further consistency check for tables when hbase:meta itself is not consistent.
    LOG.info("Loading regionsinfo from the hbase:meta table");
    boolean success = loadMetaEntries();
    if (!success) return -1;

    // Empty cells in hbase:meta?
    reportEmptyMetaCells();

    // Check if we have to cleanup empty REGIONINFO_QUALIFIER rows from hbase:meta
    if (shouldFixEmptyMetaCells()) {
      fixEmptyMetaCells();
    }

    // get a list of all tables that have not changed recently.
    if (!checkMetaOnly) {
      reportTablesInFlux();
    }

    // Get disabled tables states
    loadTableStates();

    // load regiondirs and regioninfos from HDFS
    if (shouldCheckHdfs()) {
      LOG.info("Loading region directories from HDFS");
      loadHdfsRegionDirs();
      LOG.info("Loading region information from HDFS");
      loadHdfsRegionInfos();
    }

    // fix the orphan tables
    fixOrphanTables();

    LOG.info("Checking and fixing region consistency");
    // Check and fix consistency
    checkAndFixConsistency();

    // Check integrity (does not fix)
    checkIntegrity();
    return errors.getErrorList().size();
  }

  /**
   * This method maintains an ephemeral znode. If the creation fails we return false or throw
   * exception
   *
   * @return true if creating znode succeeds; false otherwise
   * @throws IOException if IO failure occurs
   */
  private boolean setMasterInMaintenanceMode() throws IOException {
    RetryCounter retryCounter = createZNodeRetryCounterFactory.create();
    hbckEphemeralNodePath = ZNodePaths.joinZNode(
      zkw.getZNodePaths().masterMaintZNode,
      "hbck-" + Long.toString(EnvironmentEdgeManager.currentTime()));
    do {
      try {
        hbckZodeCreated = ZKUtil.createEphemeralNodeAndWatch(zkw, hbckEphemeralNodePath, null);
        if (hbckZodeCreated) {
          break;
        }
      } catch (KeeperException e) {
        if (retryCounter.getAttemptTimes() >= retryCounter.getMaxAttempts()) {
           throw new IOException("Can't create znode " + hbckEphemeralNodePath, e);
        }
        // fall through and retry
      }

      LOG.warn("Fail to create znode " + hbckEphemeralNodePath + ", try=" +
          (retryCounter.getAttemptTimes() + 1) + " of " + retryCounter.getMaxAttempts());

      try {
        retryCounter.sleepUntilNextRetry();
      } catch (InterruptedException ie) {
        throw (InterruptedIOException) new InterruptedIOException(
              "Can't create znode " + hbckEphemeralNodePath).initCause(ie);
      }
    } while (retryCounter.shouldRetry());
    return hbckZodeCreated;
  }

  private void cleanupHbckZnode() {
    try {
      if (zkw != null && hbckZodeCreated) {
        ZKUtil.deleteNode(zkw, hbckEphemeralNodePath);
        hbckZodeCreated = false;
      }
    } catch (KeeperException e) {
      // Ignore
      if (!e.code().equals(KeeperException.Code.NONODE)) {
        LOG.warn("Delete HBCK znode " + hbckEphemeralNodePath + " failed ", e);
      }
    }
  }

  /**
   * Contacts the master and prints out cluster-wide information
   * @return 0 on success, non-zero on failure
   */
  public int onlineHbck()
      throws IOException, KeeperException, InterruptedException, ReplicationException {
    // print hbase server version
    errors.print("Version: " + status.getHBaseVersion());

    // Clean start
    clearState();
    // Do offline check and repair first
    offlineHdfsIntegrityRepair();
    offlineReferenceFileRepair();
    offlineHLinkFileRepair();
    // If Master runs maintenance tasks (such as balancer, catalog janitor, etc) during online
    // hbck, it is likely that hbck would be misled and report transient errors.  Therefore, it
    // is better to set Master into maintenance mode during online hbck.
    //
    if (!setMasterInMaintenanceMode()) {
      LOG.warn("HBCK is running while master is not in maintenance mode, you might see transient "
        + "error.  Please run HBCK multiple times to reduce the chance of transient error.");
    }

    onlineConsistencyRepair();

    if (checkRegionBoundaries) {
      checkRegionBoundaries();
    }

    checkAndFixReplication();

    cleanReplicationBarrier();

    // Remove the hbck znode
    cleanupHbckZnode();

    // Remove the hbck lock
    unlockHbck();

    // Print table summary
    printTableSummary(tablesInfo);
    return errors.summarize();
  }

  public static byte[] keyOnly(byte[] b) {
    if (b == null)
      return b;
    int rowlength = Bytes.toShort(b, 0);
    byte[] result = new byte[rowlength];
    System.arraycopy(b, Bytes.SIZEOF_SHORT, result, 0, rowlength);
    return result;
  }

  @Override
  public void close() throws IOException {
    try {
      cleanupHbckZnode();
      unlockHbck();
    } catch (Exception io) {
      LOG.warn(io.toString(), io);
    } finally {
      if (zkw != null) {
        zkw.close();
        zkw = null;
      }
      IOUtils.closeQuietly(admin);
      IOUtils.closeQuietly(meta);
      IOUtils.closeQuietly(connection);
    }
  }

  private static class RegionBoundariesInformation {
    public byte [] regionName;
    public byte [] metaFirstKey;
    public byte [] metaLastKey;
    public byte [] storesFirstKey;
    public byte [] storesLastKey;
    @Override
    public String toString () {
      return "regionName=" + Bytes.toStringBinary(regionName) +
             "\nmetaFirstKey=" + Bytes.toStringBinary(metaFirstKey) +
             "\nmetaLastKey=" + Bytes.toStringBinary(metaLastKey) +
             "\nstoresFirstKey=" + Bytes.toStringBinary(storesFirstKey) +
             "\nstoresLastKey=" + Bytes.toStringBinary(storesLastKey);
    }
  }

  public void checkRegionBoundaries() {
    try {
      ByteArrayComparator comparator = new ByteArrayComparator();
      List<RegionInfo> regions = MetaTableAccessor.getAllRegions(connection, true);
      final RegionBoundariesInformation currentRegionBoundariesInformation =
          new RegionBoundariesInformation();
      Path hbaseRoot = CommonFSUtils.getRootDir(getConf());
      for (RegionInfo regionInfo : regions) {
        Path tableDir = CommonFSUtils.getTableDir(hbaseRoot, regionInfo.getTable());
        currentRegionBoundariesInformation.regionName = regionInfo.getRegionName();
        // For each region, get the start and stop key from the META and compare them to the
        // same information from the Stores.
        Path path = new Path(tableDir, regionInfo.getEncodedName());
        FileSystem fs = path.getFileSystem(getConf());
        FileStatus[] files = fs.listStatus(path);
        // For all the column families in this region...
        byte[] storeFirstKey = null;
        byte[] storeLastKey = null;
        for (FileStatus file : files) {
          String fileName = file.getPath().toString();
          fileName = fileName.substring(fileName.lastIndexOf("/") + 1);
          if (!fileName.startsWith(".") && !fileName.endsWith("recovered.edits")) {
            FileStatus[] storeFiles = fs.listStatus(file.getPath());
            // For all the stores in this column family.
            for (FileStatus storeFile : storeFiles) {
              HFile.Reader reader = HFile.createReader(fs, storeFile.getPath(),
                CacheConfig.DISABLED, true, getConf());
              if ((reader.getFirstKey() != null)
                  && ((storeFirstKey == null) || (comparator.compare(storeFirstKey,
                      ((KeyValue.KeyOnlyKeyValue) reader.getFirstKey().get()).getKey()) > 0))) {
                storeFirstKey = ((KeyValue.KeyOnlyKeyValue)reader.getFirstKey().get()).getKey();
              }
              if ((reader.getLastKey() != null)
                  && ((storeLastKey == null) || (comparator.compare(storeLastKey,
                      ((KeyValue.KeyOnlyKeyValue)reader.getLastKey().get()).getKey())) < 0)) {
                storeLastKey = ((KeyValue.KeyOnlyKeyValue)reader.getLastKey().get()).getKey();
              }
              reader.close();
            }
          }
        }
        currentRegionBoundariesInformation.metaFirstKey = regionInfo.getStartKey();
        currentRegionBoundariesInformation.metaLastKey = regionInfo.getEndKey();
        currentRegionBoundariesInformation.storesFirstKey = keyOnly(storeFirstKey);
        currentRegionBoundariesInformation.storesLastKey = keyOnly(storeLastKey);
        if (currentRegionBoundariesInformation.metaFirstKey.length == 0)
          currentRegionBoundariesInformation.metaFirstKey = null;
        if (currentRegionBoundariesInformation.metaLastKey.length == 0)
          currentRegionBoundariesInformation.metaLastKey = null;

        // For a region to be correct, we need the META start key to be smaller or equal to the
        // smallest start key from all the stores, and the start key from the next META entry to
        // be bigger than the last key from all the current stores. First region start key is null;
        // Last region end key is null; some regions can be empty and not have any store.

        boolean valid = true;
        // Checking start key.
        if ((currentRegionBoundariesInformation.storesFirstKey != null)
            && (currentRegionBoundariesInformation.metaFirstKey != null)) {
          valid = valid
              && comparator.compare(currentRegionBoundariesInformation.storesFirstKey,
                currentRegionBoundariesInformation.metaFirstKey) >= 0;
        }
        // Checking stop key.
        if ((currentRegionBoundariesInformation.storesLastKey != null)
            && (currentRegionBoundariesInformation.metaLastKey != null)) {
          valid = valid
              && comparator.compare(currentRegionBoundariesInformation.storesLastKey,
                currentRegionBoundariesInformation.metaLastKey) < 0;
        }
        if (!valid) {
          errors.reportError(ERROR_CODE.BOUNDARIES_ERROR, "Found issues with regions boundaries",
            tablesInfo.get(regionInfo.getTable()));
          LOG.warn("Region's boundaries not aligned between stores and META for:");
          LOG.warn(Objects.toString(currentRegionBoundariesInformation));
        }
      }
    } catch (IOException e) {
      LOG.error(e.toString(), e);
    }
  }

  /**
   * Iterates through the list of all orphan/invalid regiondirs.
   */
  private void adoptHdfsOrphans(Collection<HbckRegionInfo> orphanHdfsDirs) throws IOException {
    for (HbckRegionInfo hi : orphanHdfsDirs) {
      LOG.info("Attempting to handle orphan hdfs dir: " + hi.getHdfsRegionDir());
      adoptHdfsOrphan(hi);
    }
  }

  /**
   * Orphaned regions are regions without a .regioninfo file in them.  We "adopt"
   * these orphans by creating a new region, and moving the column families,
   * recovered edits, WALs, into the new region dir.  We determine the region
   * startkey and endkeys by looking at all of the hfiles inside the column
   * families to identify the min and max keys. The resulting region will
   * likely violate table integrity but will be dealt with by merging
   * overlapping regions.
   */
  @SuppressWarnings("deprecation")
  private void adoptHdfsOrphan(HbckRegionInfo hi) throws IOException {
    Path p = hi.getHdfsRegionDir();
    FileSystem fs = p.getFileSystem(getConf());
    FileStatus[] dirs = fs.listStatus(p);
    if (dirs == null) {
      LOG.warn("Attempt to adopt orphan hdfs region skipped because no files present in " +
          p + ". This dir could probably be deleted.");
      return ;
    }

    TableName tableName = hi.getTableName();
    HbckTableInfo tableInfo = tablesInfo.get(tableName);
    Preconditions.checkNotNull(tableInfo, "Table '" + tableName + "' not present!");
    TableDescriptor template = tableInfo.getTableDescriptor();

    // find min and max key values
    Pair<byte[],byte[]> orphanRegionRange = null;
    for (FileStatus cf : dirs) {
      String cfName= cf.getPath().getName();
      // TODO Figure out what the special dirs are
      if (cfName.startsWith(".") || cfName.equals(HConstants.SPLIT_LOGDIR_NAME)) continue;

      FileStatus[] hfiles = fs.listStatus(cf.getPath());
      for (FileStatus hfile : hfiles) {
        byte[] start, end;
        HFile.Reader hf = null;
        try {
          hf = HFile.createReader(fs, hfile.getPath(), CacheConfig.DISABLED, true, getConf());
          Optional<Cell> startKv = hf.getFirstKey();
          start = CellUtil.cloneRow(startKv.get());
          Optional<Cell> endKv = hf.getLastKey();
          end = CellUtil.cloneRow(endKv.get());
        } catch (IOException ioe) {
          LOG.warn("Problem reading orphan file " + hfile + ", skipping");
          continue;
        } catch (NullPointerException ioe) {
          LOG.warn("Orphan file " + hfile + " is possibly corrupted HFile, skipping");
          continue;
        } finally {
          if (hf != null) {
            hf.close();
          }
        }

        // expand the range to include the range of all hfiles
        if (orphanRegionRange == null) {
          // first range
          orphanRegionRange = new Pair<>(start, end);
        } else {
          // TODO add test

          // expand range only if the hfile is wider.
          if (Bytes.compareTo(orphanRegionRange.getFirst(), start) > 0) {
            orphanRegionRange.setFirst(start);
          }
          if (Bytes.compareTo(orphanRegionRange.getSecond(), end) < 0 ) {
            orphanRegionRange.setSecond(end);
          }
        }
      }
    }
    if (orphanRegionRange == null) {
      LOG.warn("No data in dir " + p + ", sidelining data");
      fixes++;
      sidelineRegionDir(fs, hi);
      return;
    }
    LOG.info("Min max keys are : [" + Bytes.toString(orphanRegionRange.getFirst()) + ", " +
        Bytes.toString(orphanRegionRange.getSecond()) + ")");

    // create new region on hdfs. move data into place.
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(template.getTableName())
        .setStartKey(orphanRegionRange.getFirst())
        .setEndKey(Bytes.add(orphanRegionRange.getSecond(), new byte[1]))
        .build();
    LOG.info("Creating new region : " + regionInfo);
    HRegion region = HBaseFsckRepair.createHDFSRegionDir(getConf(), regionInfo, template);
    Path target = region.getRegionFileSystem().getRegionDir();

    // rename all the data to new region
    mergeRegionDirs(target, hi);
    fixes++;
  }

  /**
   * This method determines if there are table integrity errors in HDFS.  If
   * there are errors and the appropriate "fix" options are enabled, the method
   * will first correct orphan regions making them into legit regiondirs, and
   * then reload to merge potentially overlapping regions.
   *
   * @return number of table integrity errors found
   */
  private int restoreHdfsIntegrity() throws IOException, InterruptedException {
    // Determine what's on HDFS
    LOG.info("Loading HBase regioninfo from HDFS...");
    loadHdfsRegionDirs(); // populating regioninfo table.

    int errs = errors.getErrorList().size();
    // First time just get suggestions.
    tablesInfo = loadHdfsRegionInfos(); // update tableInfos based on region info in fs.
    checkHdfsIntegrity(false, false);

    if (errors.getErrorList().size() == errs) {
      LOG.info("No integrity errors.  We are done with this phase. Glorious.");
      return 0;
    }

    if (shouldFixHdfsOrphans() && orphanHdfsDirs.size() > 0) {
      adoptHdfsOrphans(orphanHdfsDirs);
      // TODO optimize by incrementally adding instead of reloading.
    }

    // Make sure there are no holes now.
    if (shouldFixHdfsHoles()) {
      clearState(); // this also resets # fixes.
      loadHdfsRegionDirs();
      tablesInfo = loadHdfsRegionInfos(); // update tableInfos based on region info in fs.
      tablesInfo = checkHdfsIntegrity(shouldFixHdfsHoles(), false);
    }

    // Now we fix overlaps
    if (shouldFixHdfsOverlaps()) {
      // second pass we fix overlaps.
      clearState(); // this also resets # fixes.
      loadHdfsRegionDirs();
      tablesInfo = loadHdfsRegionInfos(); // update tableInfos based on region info in fs.
      tablesInfo = checkHdfsIntegrity(false, shouldFixHdfsOverlaps());
    }

    return errors.getErrorList().size();
  }

  /**
   * Scan all the store file names to find any lingering reference files,
   * which refer to some none-exiting files. If "fix" option is enabled,
   * any lingering reference file will be sidelined if found.
   * <p>
   * Lingering reference file prevents a region from opening. It has to
   * be fixed before a cluster can start properly.
   */
  private void offlineReferenceFileRepair() throws IOException, InterruptedException {
    clearState();
    Configuration conf = getConf();
    Path hbaseRoot = CommonFSUtils.getRootDir(conf);
    FileSystem fs = hbaseRoot.getFileSystem(conf);
    LOG.info("Computing mapping of all store files");
    Map<String, Path> allFiles = FSUtils.getTableStoreFilePathMap(fs, hbaseRoot,
      new FSUtils.ReferenceFileFilter(fs), executor, errors);
    errors.print("");
    LOG.info("Validating mapping using HDFS state");
    for (Path path: allFiles.values()) {
      Path referredToFile = StoreFileInfo.getReferredToFile(path);
      if (fs.exists(referredToFile)) continue;  // good, expected

      // Found a lingering reference file
      errors.reportError(ERROR_CODE.LINGERING_REFERENCE_HFILE,
        "Found lingering reference file " + path);
      if (!shouldFixReferenceFiles()) continue;

      // Now, trying to fix it since requested
      boolean success = false;
      String pathStr = path.toString();

      // A reference file path should be like
      // ${hbase.rootdir}/data/namespace/table_name/region_id/family_name/referred_file.region_name
      // Up 5 directories to get the root folder.
      // So the file will be sidelined to a similar folder structure.
      int index = pathStr.lastIndexOf(Path.SEPARATOR_CHAR);
      for (int i = 0; index > 0 && i < 5; i++) {
        index = pathStr.lastIndexOf(Path.SEPARATOR_CHAR, index - 1);
      }
      if (index > 0) {
        Path rootDir = getSidelineDir();
        Path dst = new Path(rootDir, pathStr.substring(index + 1));
        fs.mkdirs(dst.getParent());
        LOG.info("Trying to sideline reference file "
          + path + " to " + dst);
        setShouldRerun();

        success = fs.rename(path, dst);
        debugLsr(dst);

      }
      if (!success) {
        LOG.error("Failed to sideline reference file " + path);
      }
    }
  }

  /**
   * Scan all the store file names to find any lingering HFileLink files,
   * which refer to some none-exiting files. If "fix" option is enabled,
   * any lingering HFileLink file will be sidelined if found.
   */
  private void offlineHLinkFileRepair() throws IOException, InterruptedException {
    Configuration conf = getConf();
    Path hbaseRoot = CommonFSUtils.getRootDir(conf);
    FileSystem fs = hbaseRoot.getFileSystem(conf);
    LOG.info("Computing mapping of all link files");
    Map<String, Path> allFiles = FSUtils
        .getTableStoreFilePathMap(fs, hbaseRoot, new FSUtils.HFileLinkFilter(), executor, errors);
    errors.print("");

    LOG.info("Validating mapping using HDFS state");
    for (Path path : allFiles.values()) {
      // building HFileLink object to gather locations
      HFileLink actualLink = HFileLink.buildFromHFileLinkPattern(conf, path);
      if (actualLink.exists(fs)) continue; // good, expected

      // Found a lingering HFileLink
      errors.reportError(ERROR_CODE.LINGERING_HFILELINK, "Found lingering HFileLink " + path);
      if (!shouldFixHFileLinks()) continue;

      // Now, trying to fix it since requested
      setShouldRerun();

      // An HFileLink path should be like
      // ${hbase.rootdir}/data/namespace/table_name/region_id/family_name/linkedtable=linkedregionname-linkedhfilename
      // sidelineing will happen in the ${hbase.rootdir}/${sidelinedir} directory with the same folder structure.
      boolean success = sidelineFile(fs, hbaseRoot, path);

      if (!success) {
        LOG.error("Failed to sideline HFileLink file " + path);
      }

      // An HFileLink backreference path should be like
      // ${hbase.rootdir}/archive/data/namespace/table_name/region_id/family_name/.links-linkedhfilename
      // sidelineing will happen in the ${hbase.rootdir}/${sidelinedir} directory with the same folder structure.
      Path backRefPath = FileLink.getBackReferencesDir(HFileArchiveUtil
              .getStoreArchivePath(conf, HFileLink.getReferencedTableName(path.getName().toString()),
                  HFileLink.getReferencedRegionName(path.getName().toString()),
                  path.getParent().getName()),
          HFileLink.getReferencedHFileName(path.getName().toString()));
      success = sidelineFile(fs, hbaseRoot, backRefPath);

      if (!success) {
        LOG.error("Failed to sideline HFileLink backreference file " + path);
      }
    }
  }

  private boolean sidelineFile(FileSystem fs, Path hbaseRoot, Path path) throws IOException {
    URI uri = hbaseRoot.toUri().relativize(path.toUri());
    if (uri.isAbsolute()) return false;
    String relativePath = uri.getPath();
    Path rootDir = getSidelineDir();
    Path dst = new Path(rootDir, relativePath);
    boolean pathCreated = fs.mkdirs(dst.getParent());
    if (!pathCreated) {
      LOG.error("Failed to create path: " + dst.getParent());
      return false;
    }
    LOG.info("Trying to sideline file " + path + " to " + dst);
    return fs.rename(path, dst);
  }

  /**
   * TODO -- need to add tests for this.
   */
  private void reportEmptyMetaCells() {
    errors.print("Number of empty REGIONINFO_QUALIFIER rows in hbase:meta: " +
      emptyRegionInfoQualifiers.size());
    if (details) {
      for (Result r: emptyRegionInfoQualifiers) {
        errors.print("  " + r);
      }
    }
  }

  /**
   * TODO -- need to add tests for this.
   */
  private void reportTablesInFlux() {
    AtomicInteger numSkipped = new AtomicInteger(0);
    TableDescriptor[] allTables = getTables(numSkipped);
    errors.print("Number of Tables: " + allTables.length);
    if (details) {
      if (numSkipped.get() > 0) {
        errors.detail("Number of Tables in flux: " + numSkipped.get());
      }
      for (TableDescriptor td : allTables) {
        errors.detail("  Table: " + td.getTableName() + "\t" +
                           (td.isReadOnly() ? "ro" : "rw") + "\t" +
                            (td.isMetaRegion() ? "META" : "    ") + "\t" +
                           " families: " + td.getColumnFamilyCount());
      }
    }
  }

  public HbckErrorReporter getErrors() {
    return errors;
  }

  /**
   * Populate hbi's from regionInfos loaded from file system.
   */
  private SortedMap<TableName, HbckTableInfo> loadHdfsRegionInfos()
      throws IOException, InterruptedException {
    tablesInfo.clear(); // regenerating the data
    // generate region split structure
    Collection<HbckRegionInfo> hbckRegionInfos = regionInfoMap.values();

    // Parallelized read of .regioninfo files.
    List<WorkItemHdfsRegionInfo> hbis = new ArrayList<>(hbckRegionInfos.size());
    List<Future<Void>> hbiFutures;

    for (HbckRegionInfo hbi : hbckRegionInfos) {
      WorkItemHdfsRegionInfo work = new WorkItemHdfsRegionInfo(hbi, this, errors);
      hbis.add(work);
    }

    // Submit and wait for completion
    hbiFutures = executor.invokeAll(hbis);

    for(int i=0; i<hbiFutures.size(); i++) {
      WorkItemHdfsRegionInfo work = hbis.get(i);
      Future<Void> f = hbiFutures.get(i);
      try {
        f.get();
      } catch(ExecutionException e) {
        LOG.warn("Failed to read .regioninfo file for region " +
              work.hbi.getRegionNameAsString(), e.getCause());
      }
    }

    Path hbaseRoot = CommonFSUtils.getRootDir(getConf());
    FileSystem fs = hbaseRoot.getFileSystem(getConf());
    // serialized table info gathering.
    for (HbckRegionInfo hbi: hbckRegionInfos) {

      if (hbi.getHdfsHRI() == null) {
        // was an orphan
        continue;
      }


      // get table name from hdfs, populate various HBaseFsck tables.
      TableName tableName = hbi.getTableName();
      if (tableName == null) {
        // There was an entry in hbase:meta not in the HDFS?
        LOG.warn("tableName was null for: " + hbi);
        continue;
      }

      HbckTableInfo modTInfo = tablesInfo.get(tableName);
      if (modTInfo == null) {
        // only executed once per table.
        modTInfo = new HbckTableInfo(tableName, this);
        tablesInfo.put(tableName, modTInfo);
        try {
          TableDescriptor htd =
              FSTableDescriptors.getTableDescriptorFromFs(fs, hbaseRoot, tableName);
          modTInfo.htds.add(htd);
        } catch (IOException ioe) {
          if (!orphanTableDirs.containsKey(tableName)) {
            LOG.warn("Unable to read .tableinfo from " + hbaseRoot, ioe);
            //should only report once for each table
            errors.reportError(ERROR_CODE.NO_TABLEINFO_FILE,
                "Unable to read .tableinfo from " + hbaseRoot + "/" + tableName);
            Set<String> columns = new HashSet<>();
            orphanTableDirs.put(tableName, getColumnFamilyList(columns, hbi));
          }
        }
      }
      if (!hbi.isSkipChecks()) {
        modTInfo.addRegionInfo(hbi);
      }
    }

    loadTableInfosForTablesWithNoRegion();
    errors.print("");

    return tablesInfo;
  }

  /**
   * To get the column family list according to the column family dirs
   * @param columns
   * @param hbi
   * @return a set of column families
   * @throws IOException
   */
  private Set<String> getColumnFamilyList(Set<String> columns, HbckRegionInfo hbi)
      throws IOException {
    Path regionDir = hbi.getHdfsRegionDir();
    FileSystem fs = regionDir.getFileSystem(getConf());
    FileStatus[] subDirs = fs.listStatus(regionDir, new FSUtils.FamilyDirFilter(fs));
    for (FileStatus subdir : subDirs) {
      String columnfamily = subdir.getPath().getName();
      columns.add(columnfamily);
    }
    return columns;
  }

  /**
   * To fabricate a .tableinfo file with following contents<br>
   * 1. the correct tablename <br>
   * 2. the correct colfamily list<br>
   * 3. the default properties for both {@link TableDescriptor} and {@link ColumnFamilyDescriptor}<br>
   * @throws IOException
   */
  private boolean fabricateTableInfo(FSTableDescriptors fstd, TableName tableName,
      Set<String> columns) throws IOException {
    if (columns ==null || columns.isEmpty()) return false;
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    for (String columnfamimly : columns) {
      builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnfamimly));
    }
    fstd.createTableDescriptor(builder.build(), true);
    return true;
  }

  /**
   * To fix the empty REGIONINFO_QUALIFIER rows from hbase:meta <br>
   * @throws IOException
   */
  public void fixEmptyMetaCells() throws IOException {
    if (shouldFixEmptyMetaCells() && !emptyRegionInfoQualifiers.isEmpty()) {
      LOG.info("Trying to fix empty REGIONINFO_QUALIFIER hbase:meta rows.");
      for (Result region : emptyRegionInfoQualifiers) {
        deleteMetaRegion(region.getRow());
        errors.getErrorList().remove(ERROR_CODE.EMPTY_META_CELL);
      }
      emptyRegionInfoQualifiers.clear();
    }
  }

  /**
   * To fix orphan table by creating a .tableinfo file under tableDir <br>
   * 1. if TableInfo is cached, to recover the .tableinfo accordingly <br>
   * 2. else create a default .tableinfo file with following items<br>
   * &nbsp;2.1 the correct tablename <br>
   * &nbsp;2.2 the correct colfamily list<br>
   * &nbsp;2.3 the default properties for both {@link TableDescriptor} and {@link ColumnFamilyDescriptor}<br>
   * @throws IOException
   */
  public void fixOrphanTables() throws IOException {
    if (shouldFixTableOrphans() && !orphanTableDirs.isEmpty()) {

      List<TableName> tmpList = new ArrayList<>(orphanTableDirs.keySet().size());
      tmpList.addAll(orphanTableDirs.keySet());
      TableDescriptor[] htds = getTableDescriptors(tmpList);
      Iterator<Entry<TableName, Set<String>>> iter =
          orphanTableDirs.entrySet().iterator();
      int j = 0;
      int numFailedCase = 0;
      FSTableDescriptors fstd = new FSTableDescriptors(getConf());
      while (iter.hasNext()) {
        Entry<TableName, Set<String>> entry =
            iter.next();
        TableName tableName = entry.getKey();
        LOG.info("Trying to fix orphan table error: " + tableName);
        if (j < htds.length) {
          if (tableName.equals(htds[j].getTableName())) {
            TableDescriptor htd = htds[j];
            LOG.info("fixing orphan table: " + tableName + " from cache");
            fstd.createTableDescriptor(htd, true);
            j++;
            iter.remove();
          }
        } else {
          if (fabricateTableInfo(fstd, tableName, entry.getValue())) {
            LOG.warn("fixing orphan table: " + tableName + " with a default .tableinfo file");
            LOG.warn("Strongly recommend to modify the TableDescriptor if necessary for: " + tableName);
            iter.remove();
          } else {
            LOG.error("Unable to create default .tableinfo for " + tableName + " while missing column family information");
            numFailedCase++;
          }
        }
        fixes++;
      }

      if (orphanTableDirs.isEmpty()) {
        // all orphanTableDirs are luckily recovered
        // re-run doFsck after recovering the .tableinfo file
        setShouldRerun();
        LOG.warn("Strongly recommend to re-run manually hfsck after all orphanTableDirs being fixed");
      } else if (numFailedCase > 0) {
        LOG.error("Failed to fix " + numFailedCase
            + " OrphanTables with default .tableinfo files");
      }

    }
    //cleanup the list
    orphanTableDirs.clear();

  }

  /**
   * Log an appropriate message about whether or not overlapping merges are computed in parallel.
   */
  private void logParallelMerge() {
    if (getConf().getBoolean("hbasefsck.overlap.merge.parallel", true)) {
      LOG.info("Handling overlap merges in parallel. set hbasefsck.overlap.merge.parallel to" +
          " false to run serially.");
    } else {
      LOG.info("Handling overlap merges serially.  set hbasefsck.overlap.merge.parallel to" +
          " true to run in parallel.");
    }
  }

  private SortedMap<TableName, HbckTableInfo> checkHdfsIntegrity(boolean fixHoles,
      boolean fixOverlaps) throws IOException {
    LOG.info("Checking HBase region split map from HDFS data...");
    logParallelMerge();
    for (HbckTableInfo tInfo : tablesInfo.values()) {
      TableIntegrityErrorHandler handler;
      if (fixHoles || fixOverlaps) {
        handler = tInfo.new HDFSIntegrityFixer(tInfo, errors, getConf(),
          fixHoles, fixOverlaps);
      } else {
        handler = tInfo.new IntegrityFixSuggester(tInfo, errors);
      }
      if (!tInfo.checkRegionChain(handler)) {
        // should dump info as well.
        errors.report("Found inconsistency in table " + tInfo.getName());
      }
    }
    return tablesInfo;
  }

  Path getSidelineDir() throws IOException {
    if (sidelineDir == null) {
      Path hbaseDir = CommonFSUtils.getRootDir(getConf());
      Path hbckDir = new Path(hbaseDir, HConstants.HBCK_SIDELINEDIR_NAME);
      sidelineDir = new Path(hbckDir, hbaseDir.getName() + "-"
          + startMillis);
    }
    return sidelineDir;
  }

  /**
   * Sideline a region dir (instead of deleting it)
   */
  Path sidelineRegionDir(FileSystem fs, HbckRegionInfo hi) throws IOException {
    return sidelineRegionDir(fs, null, hi);
  }

  /**
   * Sideline a region dir (instead of deleting it)
   *
   * @param parentDir if specified, the region will be sidelined to folder like
   *     {@literal .../parentDir/<table name>/<region name>}. The purpose is to group together
   *     similar regions sidelined, for example, those regions should be bulk loaded back later
   *     on. If NULL, it is ignored.
   */
  Path sidelineRegionDir(FileSystem fs,
      String parentDir, HbckRegionInfo hi) throws IOException {
    TableName tableName = hi.getTableName();
    Path regionDir = hi.getHdfsRegionDir();

    if (!fs.exists(regionDir)) {
      LOG.warn("No previous " + regionDir + " exists.  Continuing.");
      return null;
    }

    Path rootDir = getSidelineDir();
    if (parentDir != null) {
      rootDir = new Path(rootDir, parentDir);
    }
    Path sidelineTableDir= CommonFSUtils.getTableDir(rootDir, tableName);
    Path sidelineRegionDir = new Path(sidelineTableDir, regionDir.getName());
    fs.mkdirs(sidelineRegionDir);
    boolean success = false;
    FileStatus[] cfs =  fs.listStatus(regionDir);
    if (cfs == null) {
      LOG.info("Region dir is empty: " + regionDir);
    } else {
      for (FileStatus cf : cfs) {
        Path src = cf.getPath();
        Path dst =  new Path(sidelineRegionDir, src.getName());
        if (fs.isFile(src)) {
          // simple file
          success = fs.rename(src, dst);
          if (!success) {
            String msg = "Unable to rename file " + src +  " to " + dst;
            LOG.error(msg);
            throw new IOException(msg);
          }
          continue;
        }

        // is a directory.
        fs.mkdirs(dst);

        LOG.info("Sidelining files from " + src + " into containing region " + dst);
        // FileSystem.rename is inconsistent with directories -- if the
        // dst (foo/a) exists and is a dir, and the src (foo/b) is a dir,
        // it moves the src into the dst dir resulting in (foo/a/b).  If
        // the dst does not exist, and the src a dir, src becomes dst. (foo/b)
        FileStatus[] hfiles = fs.listStatus(src);
        if (hfiles != null && hfiles.length > 0) {
          for (FileStatus hfile : hfiles) {
            success = fs.rename(hfile.getPath(), dst);
            if (!success) {
              String msg = "Unable to rename file " + src +  " to " + dst;
              LOG.error(msg);
              throw new IOException(msg);
            }
          }
        }
        LOG.debug("Sideline directory contents:");
        debugLsr(sidelineRegionDir);
      }
    }

    LOG.info("Removing old region dir: " + regionDir);
    success = fs.delete(regionDir, true);
    if (!success) {
      String msg = "Unable to delete dir " + regionDir;
      LOG.error(msg);
      throw new IOException(msg);
    }
    return sidelineRegionDir;
  }

  /**
   * Load the list of disabled tables in ZK into local set.
   * @throws ZooKeeperConnectionException
   * @throws IOException
   */
  private void loadTableStates()
  throws IOException {
    tableStates = MetaTableAccessor.getTableStates(connection);
    // Add hbase:meta so this tool keeps working. In hbase2, meta is always enabled though it
    // has no entry in the table states. HBCK doesn't work right w/ hbase2 but just do this in
    // meantime.
    this.tableStates.put(TableName.META_TABLE_NAME,
        new TableState(TableName.META_TABLE_NAME, TableState.State.ENABLED));
  }

  /**
   * Check if the specified region's table is disabled.
   * @param tableName table to check status of
   */
  boolean isTableDisabled(TableName tableName) {
    return tableStates.containsKey(tableName)
        && tableStates.get(tableName)
        .inStates(TableState.State.DISABLED, TableState.State.DISABLING);
  }

  /**
   * Scan HDFS for all regions, recording their information into
   * regionInfoMap
   */
  public void loadHdfsRegionDirs() throws IOException, InterruptedException {
    Path rootDir = CommonFSUtils.getRootDir(getConf());
    FileSystem fs = rootDir.getFileSystem(getConf());

    // list all tables from HDFS
    List<FileStatus> tableDirs = Lists.newArrayList();

    boolean foundVersionFile = fs.exists(new Path(rootDir, HConstants.VERSION_FILE_NAME));

    List<Path> paths = FSUtils.getTableDirs(fs, rootDir);
    for (Path path : paths) {
      TableName tableName = CommonFSUtils.getTableName(path);
       if ((!checkMetaOnly &&
           isTableIncluded(tableName)) ||
           tableName.equals(TableName.META_TABLE_NAME)) {
         tableDirs.add(fs.getFileStatus(path));
       }
    }

    // verify that version file exists
    if (!foundVersionFile) {
      errors.reportError(ERROR_CODE.NO_VERSION_FILE,
          "Version file does not exist in root dir " + rootDir);
      if (shouldFixVersionFile()) {
        LOG.info("Trying to create a new " + HConstants.VERSION_FILE_NAME
            + " file.");
        setShouldRerun();
        FSUtils.setVersion(fs, rootDir, getConf().getInt(
            HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000), getConf().getInt(
            HConstants.VERSION_FILE_WRITE_ATTEMPTS,
            HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS));
      }
    }

    // Avoid multithreading at table-level because already multithreaded internally at
    // region-level.  Additionally multithreading at table-level can lead to deadlock
    // if there are many tables in the cluster.  Since there are a limited # of threads
    // in the executor's thread pool and if we multithread at the table-level by putting
    // WorkItemHdfsDir callables into the executor, then we will have some threads in the
    // executor tied up solely in waiting for the tables' region-level calls to complete.
    // If there are enough tables then there will be no actual threads in the pool left
    // for the region-level callables to be serviced.
    for (FileStatus tableDir : tableDirs) {
      LOG.debug("Loading region dirs from " +tableDir.getPath());
      WorkItemHdfsDir item = new WorkItemHdfsDir(fs, errors, tableDir);
      try {
        item.call();
      } catch (ExecutionException e) {
        LOG.warn("Could not completely load table dir " +
            tableDir.getPath(), e.getCause());
      }
    }
    errors.print("");
  }

  /**
   * Record the location of the hbase:meta region as found in ZooKeeper.
   */
  private boolean recordMetaRegion() throws IOException {
    List<HRegionLocation> locs;
    try (RegionLocator locator = connection.getRegionLocator(TableName.META_TABLE_NAME)) {
      locs = locator.getRegionLocations(HConstants.EMPTY_START_ROW, true);
    }
    if (locs == null || locs.isEmpty()) {
      errors.reportError(ERROR_CODE.NULL_META_REGION, "META region was not found in ZooKeeper");
      return false;
    }
    for (HRegionLocation metaLocation : locs) {
      // Check if Meta region is valid and existing
      if (metaLocation == null) {
        errors.reportError(ERROR_CODE.NULL_META_REGION, "META region location is null");
        return false;
      }
      if (metaLocation.getRegion() == null) {
        errors.reportError(ERROR_CODE.NULL_META_REGION, "META location regionInfo is null");
        return false;
      }
      if (metaLocation.getHostname() == null) {
        errors.reportError(ERROR_CODE.NULL_META_REGION, "META location hostName is null");
        return false;
      }
      ServerName sn = metaLocation.getServerName();
      HbckRegionInfo.MetaEntry m = new HbckRegionInfo.MetaEntry(metaLocation.getRegion(), sn,
          EnvironmentEdgeManager.currentTime());
      HbckRegionInfo hbckRegionInfo = regionInfoMap.get(metaLocation.getRegion().getEncodedName());
      if (hbckRegionInfo == null) {
        regionInfoMap.put(metaLocation.getRegion().getEncodedName(), new HbckRegionInfo(m));
      } else {
        hbckRegionInfo.setMetaEntry(m);
      }
    }
    return true;
  }

  private ZKWatcher createZooKeeperWatcher() throws IOException {
    return new ZKWatcher(getConf(), "hbase Fsck", new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
        LOG.error(why, e);
        System.exit(1);
      }

      @Override
      public boolean isAborted() {
        return false;
      }

    });
  }

  /**
   * Contacts each regionserver and fetches metadata about regions.
   * @param regionServerList - the list of region servers to connect to
   * @throws IOException if a remote or network exception occurs
   */
  void processRegionServers(Collection<ServerName> regionServerList)
    throws IOException, InterruptedException {

    List<WorkItemRegion> workItems = new ArrayList<>(regionServerList.size());
    List<Future<Void>> workFutures;

    // loop to contact each region server in parallel
    for (ServerName rsinfo: regionServerList) {
      workItems.add(new WorkItemRegion(this, rsinfo, errors, connection));
    }

    workFutures = executor.invokeAll(workItems);

    for(int i=0; i<workFutures.size(); i++) {
      WorkItemRegion item = workItems.get(i);
      Future<Void> f = workFutures.get(i);
      try {
        f.get();
      } catch(ExecutionException e) {
        LOG.warn("Could not process regionserver {}", item.rsinfo.getAddress(),
            e.getCause());
      }
    }
  }

  /**
   * Check consistency of all regions that have been found in previous phases.
   */
  private void checkAndFixConsistency()
  throws IOException, KeeperException, InterruptedException {
    // Divide the checks in two phases. One for default/primary replicas and another
    // for the non-primary ones. Keeps code cleaner this way.

    List<CheckRegionConsistencyWorkItem> workItems = new ArrayList<>(regionInfoMap.size());
    for (java.util.Map.Entry<String, HbckRegionInfo> e: regionInfoMap.entrySet()) {
      if (e.getValue().getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID) {
        workItems.add(new CheckRegionConsistencyWorkItem(e.getKey(), e.getValue()));
      }
    }
    checkRegionConsistencyConcurrently(workItems);

    boolean prevHdfsCheck = shouldCheckHdfs();
    setCheckHdfs(false); //replicas don't have any hdfs data
    // Run a pass over the replicas and fix any assignment issues that exist on the currently
    // deployed/undeployed replicas.
    List<CheckRegionConsistencyWorkItem> replicaWorkItems = new ArrayList<>(regionInfoMap.size());
    for (java.util.Map.Entry<String, HbckRegionInfo> e: regionInfoMap.entrySet()) {
      if (e.getValue().getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
        replicaWorkItems.add(new CheckRegionConsistencyWorkItem(e.getKey(), e.getValue()));
      }
    }
    checkRegionConsistencyConcurrently(replicaWorkItems);
    setCheckHdfs(prevHdfsCheck);

    // If some regions is skipped during checkRegionConsistencyConcurrently() phase, we might
    // not get accurate state of the hbase if continuing. The config here allows users to tune
    // the tolerance of number of skipped region.
    // TODO: evaluate the consequence to continue the hbck operation without config.
    int terminateThreshold =  getConf().getInt("hbase.hbck.skipped.regions.limit", 0);
    int numOfSkippedRegions = skippedRegions.size();
    if (numOfSkippedRegions > 0 && numOfSkippedRegions > terminateThreshold) {
      throw new IOException(numOfSkippedRegions
        + " region(s) could not be checked or repaired.  See logs for detail.");
    }

    if (shouldCheckHdfs()) {
      checkAndFixTableStates();
    }
  }

  /**
   * Check consistency of all regions using mulitple threads concurrently.
   */
  private void checkRegionConsistencyConcurrently(
    final List<CheckRegionConsistencyWorkItem> workItems)
    throws IOException, KeeperException, InterruptedException {
    if (workItems.isEmpty()) {
      return;  // nothing to check
    }

    List<Future<Void>> workFutures = executor.invokeAll(workItems);
    for(Future<Void> f: workFutures) {
      try {
        f.get();
      } catch(ExecutionException e1) {
        LOG.warn("Could not check region consistency " , e1.getCause());
        if (e1.getCause() instanceof IOException) {
          throw (IOException)e1.getCause();
        } else if (e1.getCause() instanceof KeeperException) {
          throw (KeeperException)e1.getCause();
        } else if (e1.getCause() instanceof InterruptedException) {
          throw (InterruptedException)e1.getCause();
        } else {
          throw new IOException(e1.getCause());
        }
      }
    }
  }

  class CheckRegionConsistencyWorkItem implements Callable<Void> {
    private final String key;
    private final HbckRegionInfo hbi;

    CheckRegionConsistencyWorkItem(String key, HbckRegionInfo hbi) {
      this.key = key;
      this.hbi = hbi;
    }

    @Override
    public synchronized Void call() throws Exception {
      try {
        checkRegionConsistency(key, hbi);
      } catch (Exception e) {
        // If the region is non-META region, skip this region and send warning/error message; if
        // the region is META region, we should not continue.
        LOG.warn("Unable to complete check or repair the region '" + hbi.getRegionNameAsString()
          + "'.", e);
        if (hbi.getHdfsHRI().isMetaRegion()) {
          throw e;
        }
        LOG.warn("Skip region '" + hbi.getRegionNameAsString() + "'");
        addSkippedRegion(hbi);
      }
      return null;
    }
  }

  private void addSkippedRegion(final HbckRegionInfo hbi) {
    Set<String> skippedRegionNames = skippedRegions.get(hbi.getTableName());
    if (skippedRegionNames == null) {
      skippedRegionNames = new HashSet<>();
    }
    skippedRegionNames.add(hbi.getRegionNameAsString());
    skippedRegions.put(hbi.getTableName(), skippedRegionNames);
  }

  /**
   * Check and fix table states, assumes full info available:
   * - tableInfos
   * - empty tables loaded
   */
  private void checkAndFixTableStates() throws IOException {
    // first check dangling states
    for (Entry<TableName, TableState> entry : tableStates.entrySet()) {
      TableName tableName = entry.getKey();
      TableState tableState = entry.getValue();
      HbckTableInfo tableInfo = tablesInfo.get(tableName);
      if (isTableIncluded(tableName)
          && !tableName.isSystemTable()
          && tableInfo == null) {
        if (fixMeta) {
          MetaTableAccessor.deleteTableState(connection, tableName);
          TableState state = MetaTableAccessor.getTableState(connection, tableName);
          if (state != null) {
            errors.reportError(ERROR_CODE.ORPHAN_TABLE_STATE,
                tableName + " unable to delete dangling table state " + tableState);
          }
        } else if (!checkMetaOnly) {
          // dangling table state in meta if checkMetaOnly is false. If checkMetaOnly is
          // true, tableInfo will be null as tablesInfo are not polulated for all tables from hdfs
          errors.reportError(ERROR_CODE.ORPHAN_TABLE_STATE,
              tableName + " has dangling table state " + tableState);
        }
      }
    }
    // check that all tables have states
    for (TableName tableName : tablesInfo.keySet()) {
      if (isTableIncluded(tableName) && !tableStates.containsKey(tableName)) {
        if (fixMeta) {
          MetaTableAccessor.updateTableState(connection, tableName, TableState.State.ENABLED);
          TableState newState = MetaTableAccessor.getTableState(connection, tableName);
          if (newState == null) {
            errors.reportError(ERROR_CODE.NO_TABLE_STATE,
                "Unable to change state for table " + tableName + " in meta ");
          }
        } else {
          errors.reportError(ERROR_CODE.NO_TABLE_STATE,
              tableName + " has no state in meta ");
        }
      }
    }
  }

  private void preCheckPermission() throws IOException {
    if (shouldIgnorePreCheckPermission()) {
      return;
    }

    Path hbaseDir = CommonFSUtils.getRootDir(getConf());
    FileSystem fs = hbaseDir.getFileSystem(getConf());
    UserProvider userProvider = UserProvider.instantiate(getConf());
    UserGroupInformation ugi = userProvider.getCurrent().getUGI();
    FileStatus[] files = fs.listStatus(hbaseDir);
    for (FileStatus file : files) {
      try {
        fs.access(file.getPath(), FsAction.WRITE);
      } catch (AccessControlException ace) {
        LOG.warn("Got AccessDeniedException when preCheckPermission ", ace);
        errors.reportError(ERROR_CODE.WRONG_USAGE, "Current user " + ugi.getUserName()
          + " does not have write perms to " + file.getPath()
          + ". Please rerun hbck as hdfs user " + file.getOwner());
        throw ace;
      }
    }
  }

  /**
   * Deletes region from meta table
   */
  private void deleteMetaRegion(HbckRegionInfo hi) throws IOException {
    deleteMetaRegion(hi.getMetaEntry().getRegionInfo().getRegionName());
  }

  /**
   * Deletes region from meta table
   */
  private void deleteMetaRegion(byte[] metaKey) throws IOException {
    Delete d = new Delete(metaKey);
    meta.delete(d);
    LOG.info("Deleted " + Bytes.toString(metaKey) + " from META" );
  }

  /**
   * Reset the split parent region info in meta table
   */
  private void resetSplitParent(HbckRegionInfo hi) throws IOException {
    RowMutations mutations = new RowMutations(hi.getMetaEntry().getRegionInfo().getRegionName());
    Delete d = new Delete(hi.getMetaEntry().getRegionInfo().getRegionName());
    d.addColumn(HConstants.CATALOG_FAMILY, HConstants.SPLITA_QUALIFIER);
    d.addColumn(HConstants.CATALOG_FAMILY, HConstants.SPLITB_QUALIFIER);
    mutations.add(d);

    RegionInfo hri = RegionInfoBuilder.newBuilder(hi.getMetaEntry().getRegionInfo())
      .setOffline(false).setSplit(false).build();
    Put p = MetaTableAccessor.makePutFromRegionInfo(hri, EnvironmentEdgeManager.currentTime());
    mutations.add(p);

    meta.mutateRow(mutations);
    LOG.info("Reset split parent " + hi.getMetaEntry().getRegionInfo().getRegionNameAsString() +
      " in META");
  }

  /**
   * This backwards-compatibility wrapper for permanently offlining a region
   * that should not be alive.  If the region server does not support the
   * "offline" method, it will use the closest unassign method instead.  This
   * will basically work until one attempts to disable or delete the affected
   * table.  The problem has to do with in-memory only master state, so
   * restarting the HMaster or failing over to another should fix this.
   */
  void offline(byte[] regionName) throws IOException {
    String regionString = Bytes.toStringBinary(regionName);
    if (!rsSupportsOffline) {
      LOG.warn(
          "Using unassign region " + regionString + " instead of using offline method, you should" +
              " restart HMaster after these repairs");
      admin.unassign(regionName, true);
      return;
    }

    // first time we assume the rs's supports #offline.
    try {
      LOG.info("Offlining region " + regionString);
      admin.offline(regionName);
    } catch (IOException ioe) {
      String notFoundMsg = "java.lang.NoSuchMethodException: " +
          "org.apache.hadoop.hbase.master.HMaster.offline([B)";
      if (ioe.getMessage().contains(notFoundMsg)) {
        LOG.warn("Using unassign region " + regionString +
            " instead of using offline method, you should" +
            " restart HMaster after these repairs");
        rsSupportsOffline = false; // in the future just use unassign
        admin.unassign(regionName, true);
        return;
      }
      throw ioe;
    }
  }

  /**
   * Attempts to undeploy a region from a region server based in information in
   * META.  Any operations that modify the file system should make sure that
   * its corresponding region is not deployed to prevent data races.
   *
   * A separate call is required to update the master in-memory region state
   * kept in the AssignementManager.  Because disable uses this state instead of
   * that found in META, we can't seem to cleanly disable/delete tables that
   * have been hbck fixed.  When used on a version of HBase that does not have
   * the offline ipc call exposed on the master (&lt;0.90.5, &lt;0.92.0) a master
   * restart or failover may be required.
   */
  void closeRegion(HbckRegionInfo hi) throws IOException, InterruptedException {
    if (hi.getMetaEntry() == null && hi.getHdfsEntry() == null) {
      undeployRegions(hi);
      return;
    }

    // get assignment info and hregioninfo from meta.
    Get get = new Get(hi.getRegionName());
    get.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    get.addColumn(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
    get.addColumn(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER);
    // also get the locations of the replicas to close if the primary region is being closed
    if (hi.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID) {
      int numReplicas = admin.getDescriptor(hi.getTableName()).getRegionReplication();
      for (int i = 0; i < numReplicas; i++) {
        get.addColumn(HConstants.CATALOG_FAMILY, CatalogFamilyFormat.getServerColumn(i));
        get.addColumn(HConstants.CATALOG_FAMILY, CatalogFamilyFormat.getStartCodeColumn(i));
      }
    }
    Result r = meta.get(get);
    RegionLocations rl = CatalogFamilyFormat.getRegionLocations(r);
    if (rl == null) {
      LOG.warn("Unable to close region " + hi.getRegionNameAsString() +
          " since meta does not have handle to reach it");
      return;
    }
    for (HRegionLocation h : rl.getRegionLocations()) {
      ServerName serverName = h.getServerName();
      if (serverName == null) {
        errors.reportError("Unable to close region "
            + hi.getRegionNameAsString() +  " because meta does not "
            + "have handle to reach it.");
        continue;
      }
      RegionInfo hri = h.getRegion();
      if (hri == null) {
        LOG.warn("Unable to close region " + hi.getRegionNameAsString()
            + " because hbase:meta had invalid or missing "
            + HConstants.CATALOG_FAMILY_STR + ":"
            + Bytes.toString(HConstants.REGIONINFO_QUALIFIER)
            + " qualifier value.");
        continue;
      }
      // close the region -- close files and remove assignment
      HBaseFsckRepair.closeRegionSilentlyAndWait(connection, serverName, hri);
    }
  }

  private void undeployRegions(HbckRegionInfo hi) throws IOException, InterruptedException {
    undeployRegionsForHbi(hi);
    // undeploy replicas of the region (but only if the method is invoked for the primary)
    if (hi.getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
      return;
    }
    int numReplicas = admin.getDescriptor(hi.getTableName()).getRegionReplication();
    for (int i = 1; i < numReplicas; i++) {
      if (hi.getPrimaryHRIForDeployedReplica() == null) continue;
      RegionInfo hri = RegionReplicaUtil.getRegionInfoForReplica(
          hi.getPrimaryHRIForDeployedReplica(), i);
      HbckRegionInfo h = regionInfoMap.get(hri.getEncodedName());
      if (h != null) {
        undeployRegionsForHbi(h);
        //set skip checks; we undeployed it, and we don't want to evaluate this anymore
        //in consistency checks
        h.setSkipChecks(true);
      }
    }
  }

  private void undeployRegionsForHbi(HbckRegionInfo hi) throws IOException, InterruptedException {
    for (HbckRegionInfo.OnlineEntry rse : hi.getOnlineEntries()) {
      LOG.debug("Undeploy region "  + rse.getRegionInfo() + " from " + rse.getServerName());
      try {
        HBaseFsckRepair
            .closeRegionSilentlyAndWait(connection, rse.getServerName(), rse.getRegionInfo());
        offline(rse.getRegionInfo().getRegionName());
      } catch (IOException ioe) {
        LOG.warn("Got exception when attempting to offline region "
            + Bytes.toString(rse.getRegionInfo().getRegionName()), ioe);
      }
    }
  }

  private void tryAssignmentRepair(HbckRegionInfo hbi, String msg) throws IOException,
    KeeperException, InterruptedException {
    // If we are trying to fix the errors
    if (shouldFixAssignments()) {
      errors.print(msg);
      undeployRegions(hbi);
      setShouldRerun();
      RegionInfo hri = hbi.getHdfsHRI();
      if (hri == null) {
        hri = hbi.getMetaEntry().getRegionInfo();
      }
      HBaseFsckRepair.fixUnassigned(admin, hri);
      HBaseFsckRepair.waitUntilAssigned(admin, hri);

      // also assign replicas if needed (do it only when this call operates on a primary replica)
      if (hbi.getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) return;
      int replicationCount = admin.getDescriptor(hri.getTable()).getRegionReplication();
      for (int i = 1; i < replicationCount; i++) {
        hri = RegionReplicaUtil.getRegionInfoForReplica(hri, i);
        HbckRegionInfo h = regionInfoMap.get(hri.getEncodedName());
        if (h != null) {
          undeployRegions(h);
          //set skip checks; we undeploy & deploy it; we don't want to evaluate this hbi anymore
          //in consistency checks
          h.setSkipChecks(true);
        }
        HBaseFsckRepair.fixUnassigned(admin, hri);
        HBaseFsckRepair.waitUntilAssigned(admin, hri);
      }

    }
  }

  /**
   * Check a single region for consistency and correct deployment.
   */
  private void checkRegionConsistency(final String key, final HbckRegionInfo hbi)
      throws IOException, KeeperException, InterruptedException {

    if (hbi.isSkipChecks()) return;
    String descriptiveName = hbi.toString();
    boolean inMeta = hbi.getMetaEntry() != null;
    // In case not checking HDFS, assume the region is on HDFS
    boolean inHdfs = !shouldCheckHdfs() || hbi.getHdfsRegionDir() != null;
    boolean hasMetaAssignment = inMeta && hbi.getMetaEntry().regionServer != null;
    boolean isDeployed = !hbi.getDeployedOn().isEmpty();
    boolean isMultiplyDeployed = hbi.getDeployedOn().size() > 1;
    boolean deploymentMatchesMeta = hasMetaAssignment && isDeployed && !isMultiplyDeployed &&
      hbi.getMetaEntry().regionServer.equals(hbi.getDeployedOn().get(0));
    boolean splitParent = inMeta && hbi.getMetaEntry().getRegionInfo().isSplit() &&
      hbi.getMetaEntry().getRegionInfo().isOffline();
    boolean shouldBeDeployed =
      inMeta && !isTableDisabled(hbi.getMetaEntry().getRegionInfo().getTable());
    boolean recentlyModified = inHdfs &&
      hbi.getModTime() + timelag > EnvironmentEdgeManager.currentTime();

    // ========== First the healthy cases =============
    if (hbi.containsOnlyHdfsEdits()) {
      return;
    }
    if (inMeta && inHdfs && isDeployed && deploymentMatchesMeta && shouldBeDeployed) {
      return;
    } else if (inMeta && inHdfs && !shouldBeDeployed && !isDeployed) {
      LOG.info("Region " + descriptiveName + " is in META, and in a disabled " +
        "tabled that is not deployed");
      return;
    } else if (recentlyModified) {
      LOG.warn("Region " + descriptiveName + " was recently modified -- skipping");
      return;
    }
    // ========== Cases where the region is not in hbase:meta =============
    else if (!inMeta && !inHdfs && !isDeployed) {
      // We shouldn't have record of this region at all then!
      assert false : "Entry for region with no data";
    } else if (!inMeta && !inHdfs && isDeployed) {
      errors.reportError(ERROR_CODE.NOT_IN_META_HDFS, "Region "
          + descriptiveName + ", key=" + key + ", not on HDFS or in hbase:meta but " +
          "deployed on " + Joiner.on(", ").join(hbi.getDeployedOn()));
      if (shouldFixAssignments()) {
        undeployRegions(hbi);
      }

    } else if (!inMeta && inHdfs && !isDeployed) {
      if (hbi.isMerged()) {
        // This region has already been merged, the remaining hdfs file will be
        // cleaned by CatalogJanitor later
        hbi.setSkipChecks(true);
        LOG.info("Region " + descriptiveName
            + " got merge recently, its file(s) will be cleaned by CatalogJanitor later");
        return;
      }
      errors.reportError(ERROR_CODE.NOT_IN_META_OR_DEPLOYED, "Region "
          + descriptiveName + " on HDFS, but not listed in hbase:meta " +
          "or deployed on any region server");
      // restore region consistency of an adopted orphan
      if (shouldFixMeta()) {
        if (!hbi.isHdfsRegioninfoPresent()) {
          LOG.error("Region " + hbi.getHdfsHRI() + " could have been repaired"
              +  " in table integrity repair phase if -fixHdfsOrphans was" +
              " used.");
          return;
        }

        RegionInfo hri = hbi.getHdfsHRI();
        HbckTableInfo tableInfo = tablesInfo.get(hri.getTable());

        for (RegionInfo region : tableInfo.getRegionsFromMeta(this.regionInfoMap)) {
          if (Bytes.compareTo(region.getStartKey(), hri.getStartKey()) <= 0
              && (region.getEndKey().length == 0 || Bytes.compareTo(region.getEndKey(),
                hri.getEndKey()) >= 0)
              && Bytes.compareTo(region.getStartKey(), hri.getEndKey()) <= 0) {
            if(region.isSplit() || region.isOffline()) continue;
            Path regionDir = hbi.getHdfsRegionDir();
            FileSystem fs = regionDir.getFileSystem(getConf());
            List<Path> familyDirs = FSUtils.getFamilyDirs(fs, regionDir);
            for (Path familyDir : familyDirs) {
              List<Path> referenceFilePaths = FSUtils.getReferenceFilePaths(fs, familyDir);
              for (Path referenceFilePath : referenceFilePaths) {
                Path parentRegionDir =
                    StoreFileInfo.getReferredToFile(referenceFilePath).getParent().getParent();
                if (parentRegionDir.toString().endsWith(region.getEncodedName())) {
                  LOG.warn(hri + " start and stop keys are in the range of " + region
                      + ". The region might not be cleaned up from hdfs when region " + region
                      + " split failed. Hence deleting from hdfs.");
                  HRegionFileSystem.deleteRegionFromFileSystem(getConf(), fs,
                    regionDir.getParent(), hri);
                  return;
                }
              }
            }
          }
        }
        LOG.info("Patching hbase:meta with .regioninfo: " + hbi.getHdfsHRI());
        int numReplicas = admin.getDescriptor(hbi.getTableName()).getRegionReplication();
        HBaseFsckRepair.fixMetaHoleOnlineAndAddReplicas(getConf(), hbi.getHdfsHRI(),
            admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
              .getLiveServerMetrics().keySet(), numReplicas);

        tryAssignmentRepair(hbi, "Trying to reassign region...");
      }

    } else if (!inMeta && inHdfs && isDeployed) {
      errors.reportError(ERROR_CODE.NOT_IN_META, "Region " + descriptiveName
          + " not in META, but deployed on " + Joiner.on(", ").join(hbi.getDeployedOn()));
      debugLsr(hbi.getHdfsRegionDir());
      if (hbi.getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
        // for replicas, this means that we should undeploy the region (we would have
        // gone over the primaries and fixed meta holes in first phase under
        // checkAndFixConsistency; we shouldn't get the condition !inMeta at
        // this stage unless unwanted replica)
        if (shouldFixAssignments()) {
          undeployRegionsForHbi(hbi);
        }
      }
      if (shouldFixMeta() && hbi.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID) {
        if (!hbi.isHdfsRegioninfoPresent()) {
          LOG.error("This should have been repaired in table integrity repair phase");
          return;
        }

        LOG.info("Patching hbase:meta with with .regioninfo: " + hbi.getHdfsHRI());
        int numReplicas = admin.getDescriptor(hbi.getTableName()).getRegionReplication();
        HBaseFsckRepair.fixMetaHoleOnlineAndAddReplicas(getConf(), hbi.getHdfsHRI(),
            admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
              .getLiveServerMetrics().keySet(), numReplicas);
        tryAssignmentRepair(hbi, "Trying to fix unassigned region...");
      }

    // ========== Cases where the region is in hbase:meta =============
    } else if (inMeta && inHdfs && !isDeployed && splitParent) {
      // check whether this is an actual error, or just transient state where parent
      // is not cleaned
      if (hbi.getMetaEntry().splitA != null && hbi.getMetaEntry().splitB != null) {
        // check that split daughters are there
        HbckRegionInfo infoA = this.regionInfoMap.get(hbi.getMetaEntry().splitA.getEncodedName());
        HbckRegionInfo infoB = this.regionInfoMap.get(hbi.getMetaEntry().splitB.getEncodedName());
        if (infoA != null && infoB != null) {
          // we already processed or will process daughters. Move on, nothing to see here.
          hbi.setSkipChecks(true);
          return;
        }
      }

      // For Replica region, we need to do a similar check. If replica is not split successfully,
      // error is going to be reported against primary daughter region.
      if (hbi.getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
        LOG.info("Region " + descriptiveName + " is a split parent in META, in HDFS, "
            + "and not deployed on any region server. This may be transient.");
        hbi.setSkipChecks(true);
        return;
      }

      errors.reportError(ERROR_CODE.LINGERING_SPLIT_PARENT, "Region "
          + descriptiveName + " is a split parent in META, in HDFS, "
          + "and not deployed on any region server. This could be transient, "
          + "consider to run the catalog janitor first!");
      if (shouldFixSplitParents()) {
        setShouldRerun();
        resetSplitParent(hbi);
      }
    } else if (inMeta && !inHdfs && !isDeployed) {
      errors.reportError(ERROR_CODE.NOT_IN_HDFS_OR_DEPLOYED, "Region "
          + descriptiveName + " found in META, but not in HDFS "
          + "or deployed on any region server.");
      if (shouldFixMeta()) {
        deleteMetaRegion(hbi);
      }
    } else if (inMeta && !inHdfs && isDeployed) {
      errors.reportError(ERROR_CODE.NOT_IN_HDFS, "Region " + descriptiveName
          + " found in META, but not in HDFS, " +
          "and deployed on " + Joiner.on(", ").join(hbi.getDeployedOn()));
      // We treat HDFS as ground truth.  Any information in meta is transient
      // and equivalent data can be regenerated.  So, lets unassign and remove
      // these problems from META.
      if (shouldFixAssignments()) {
        errors.print("Trying to fix unassigned region...");
        undeployRegions(hbi);
      }
      if (shouldFixMeta()) {
        // wait for it to complete
        deleteMetaRegion(hbi);
      }
    } else if (inMeta && inHdfs && !isDeployed && shouldBeDeployed) {
      errors.reportError(ERROR_CODE.NOT_DEPLOYED, "Region " + descriptiveName
          + " not deployed on any region server.");
      tryAssignmentRepair(hbi, "Trying to fix unassigned region...");
    } else if (inMeta && inHdfs && isDeployed && !shouldBeDeployed) {
      errors.reportError(ERROR_CODE.SHOULD_NOT_BE_DEPLOYED,
          "Region " + descriptiveName + " should not be deployed according " +
          "to META, but is deployed on " + Joiner.on(", ").join(hbi.getDeployedOn()));
      if (shouldFixAssignments()) {
        errors.print("Trying to close the region " + descriptiveName);
        setShouldRerun();
        HBaseFsckRepair.fixMultiAssignment(connection, hbi.getMetaEntry().getRegionInfo(),
          hbi.getDeployedOn());
      }
    } else if (inMeta && inHdfs && isMultiplyDeployed) {
      errors.reportError(ERROR_CODE.MULTI_DEPLOYED,
        "Region " + descriptiveName + " is listed in hbase:meta on region server " +
          hbi.getMetaEntry().regionServer + " but is multiply assigned to region servers " +
          Joiner.on(", ").join(hbi.getDeployedOn()));
      // If we are trying to fix the errors
      if (shouldFixAssignments()) {
        errors.print("Trying to fix assignment error...");
        setShouldRerun();
        HBaseFsckRepair.fixMultiAssignment(connection, hbi.getMetaEntry().getRegionInfo(),
          hbi.getDeployedOn());
      }
    } else if (inMeta && inHdfs && isDeployed && !deploymentMatchesMeta) {
      errors.reportError(ERROR_CODE.SERVER_DOES_NOT_MATCH_META, "Region "
          + descriptiveName + " listed in hbase:meta on region server " +
          hbi.getMetaEntry().regionServer + " but found on region server " +
          hbi.getDeployedOn().get(0));
      // If we are trying to fix the errors
      if (shouldFixAssignments()) {
        errors.print("Trying to fix assignment error...");
        setShouldRerun();
        HBaseFsckRepair.fixMultiAssignment(connection, hbi.getMetaEntry().getRegionInfo(),
          hbi.getDeployedOn());
        HBaseFsckRepair.waitUntilAssigned(admin, hbi.getHdfsHRI());
      }
    } else {
      errors.reportError(ERROR_CODE.UNKNOWN, "Region " + descriptiveName +
          " is in an unforeseen state:" +
          " inMeta=" + inMeta +
          " inHdfs=" + inHdfs +
          " isDeployed=" + isDeployed +
          " isMultiplyDeployed=" + isMultiplyDeployed +
          " deploymentMatchesMeta=" + deploymentMatchesMeta +
          " shouldBeDeployed=" + shouldBeDeployed);
    }
  }

  /**
   * Checks tables integrity. Goes over all regions and scans the tables.
   * Collects all the pieces for each table and checks if there are missing,
   * repeated or overlapping ones.
   * @throws IOException
   */
  SortedMap<TableName, HbckTableInfo> checkIntegrity() throws IOException {
    tablesInfo = new TreeMap<>();
    LOG.debug("There are " + regionInfoMap.size() + " region info entries");
    for (HbckRegionInfo hbi : regionInfoMap.values()) {
      // Check only valid, working regions
      if (hbi.getMetaEntry() == null) {
        // this assumes that consistency check has run loadMetaEntry
        Path p = hbi.getHdfsRegionDir();
        if (p == null) {
          errors.report("No regioninfo in Meta or HDFS. " + hbi);
        }

        // TODO test.
        continue;
      }
      if (hbi.getMetaEntry().regionServer == null) {
        errors.detail("Skipping region because no region server: " + hbi);
        continue;
      }
      if (hbi.getMetaEntry().getRegionInfo().isOffline()) {
        errors.detail("Skipping region because it is offline: " + hbi);
        continue;
      }
      if (hbi.containsOnlyHdfsEdits()) {
        errors.detail("Skipping region because it only contains edits" + hbi);
        continue;
      }

      // Missing regionDir or over-deployment is checked elsewhere. Include
      // these cases in modTInfo, so we can evaluate those regions as part of
      // the region chain in META
      //if (hbi.foundRegionDir == null) continue;
      //if (hbi.deployedOn.size() != 1) continue;
      if (hbi.getDeployedOn().isEmpty()) {
        continue;
      }

      // We should be safe here
      TableName tableName = hbi.getMetaEntry().getRegionInfo().getTable();
      HbckTableInfo modTInfo = tablesInfo.get(tableName);
      if (modTInfo == null) {
        modTInfo = new HbckTableInfo(tableName, this);
      }
      for (ServerName server : hbi.getDeployedOn()) {
        modTInfo.addServer(server);
      }

      if (!hbi.isSkipChecks()) {
        modTInfo.addRegionInfo(hbi);
      }

      tablesInfo.put(tableName, modTInfo);
    }

    loadTableInfosForTablesWithNoRegion();

    logParallelMerge();
    for (HbckTableInfo tInfo : tablesInfo.values()) {
      TableIntegrityErrorHandler handler = tInfo.new IntegrityFixSuggester(tInfo, errors);
      if (!tInfo.checkRegionChain(handler)) {
        errors.report("Found inconsistency in table " + tInfo.getName());
      }
    }
    return tablesInfo;
  }

  /** Loads table info's for tables that may not have been included, since there are no
   * regions reported for the table, but table dir is there in hdfs
   */
  private void loadTableInfosForTablesWithNoRegion() throws IOException {
    Map<String, TableDescriptor> allTables = new FSTableDescriptors(getConf()).getAll();
    for (TableDescriptor htd : allTables.values()) {
      if (checkMetaOnly && !htd.isMetaTable()) {
        continue;
      }

      TableName tableName = htd.getTableName();
      if (isTableIncluded(tableName) && !tablesInfo.containsKey(tableName)) {
        HbckTableInfo tableInfo = new HbckTableInfo(tableName, this);
        tableInfo.htds.add(htd);
        tablesInfo.put(htd.getTableName(), tableInfo);
      }
    }
  }

  /**
   * Merge hdfs data by moving from contained HbckRegionInfo into targetRegionDir.
   * @return number of file move fixes done to merge regions.
   */
  public int mergeRegionDirs(Path targetRegionDir, HbckRegionInfo contained) throws IOException {
    int fileMoves = 0;
    String thread = Thread.currentThread().getName();
    LOG.debug("[" + thread + "] Contained region dir after close and pause");
    debugLsr(contained.getHdfsRegionDir());

    // rename the contained into the container.
    FileSystem fs = targetRegionDir.getFileSystem(getConf());
    FileStatus[] dirs = null;
    try {
      dirs = fs.listStatus(contained.getHdfsRegionDir());
    } catch (FileNotFoundException fnfe) {
      // region we are attempting to merge in is not present!  Since this is a merge, there is
      // no harm skipping this region if it does not exist.
      if (!fs.exists(contained.getHdfsRegionDir())) {
        LOG.warn("[" + thread + "] HDFS region dir " + contained.getHdfsRegionDir()
            + " is missing. Assuming already sidelined or moved.");
      } else {
        sidelineRegionDir(fs, contained);
      }
      return fileMoves;
    }

    if (dirs == null) {
      if (!fs.exists(contained.getHdfsRegionDir())) {
        LOG.warn("[" + thread + "] HDFS region dir " + contained.getHdfsRegionDir()
            + " already sidelined.");
      } else {
        sidelineRegionDir(fs, contained);
      }
      return fileMoves;
    }

    for (FileStatus cf : dirs) {
      Path src = cf.getPath();
      Path dst =  new Path(targetRegionDir, src.getName());

      if (src.getName().equals(HRegionFileSystem.REGION_INFO_FILE)) {
        // do not copy the old .regioninfo file.
        continue;
      }

      if (src.getName().equals(HConstants.HREGION_OLDLOGDIR_NAME)) {
        // do not copy the .oldlogs files
        continue;
      }

      LOG.info("[" + thread + "] Moving files from " + src + " into containing region " + dst);
      // FileSystem.rename is inconsistent with directories -- if the
      // dst (foo/a) exists and is a dir, and the src (foo/b) is a dir,
      // it moves the src into the dst dir resulting in (foo/a/b).  If
      // the dst does not exist, and the src a dir, src becomes dst. (foo/b)
      for (FileStatus hfile : fs.listStatus(src)) {
        boolean success = fs.rename(hfile.getPath(), dst);
        if (success) {
          fileMoves++;
        }
      }
      LOG.debug("[" + thread + "] Sideline directory contents:");
      debugLsr(targetRegionDir);
    }

    // if all success.
    sidelineRegionDir(fs, contained);
    LOG.info("[" + thread + "] Sidelined region dir "+ contained.getHdfsRegionDir() + " into " +
        getSidelineDir());
    debugLsr(contained.getHdfsRegionDir());

    return fileMoves;
  }


  static class WorkItemOverlapMerge implements Callable<Void> {
    private TableIntegrityErrorHandler handler;
    Collection<HbckRegionInfo> overlapgroup;

    WorkItemOverlapMerge(Collection<HbckRegionInfo> overlapgroup,
        TableIntegrityErrorHandler handler) {
      this.handler = handler;
      this.overlapgroup = overlapgroup;
    }

    @Override
    public Void call() throws Exception {
      handler.handleOverlapGroup(overlapgroup);
      return null;
    }
  }

  /**
   * Return a list of user-space table names whose metadata have not been
   * modified in the last few milliseconds specified by timelag
   * if any of the REGIONINFO_QUALIFIER, SERVER_QUALIFIER, STARTCODE_QUALIFIER,
   * SPLITA_QUALIFIER, SPLITB_QUALIFIER have not changed in the last
   * milliseconds specified by timelag, then the table is a candidate to be returned.
   * @return tables that have not been modified recently
   * @throws IOException if an error is encountered
   */
  TableDescriptor[] getTables(AtomicInteger numSkipped) {
    List<TableName> tableNames = new ArrayList<>();
    long now = EnvironmentEdgeManager.currentTime();

    for (HbckRegionInfo hbi : regionInfoMap.values()) {
      HbckRegionInfo.MetaEntry info = hbi.getMetaEntry();

      // if the start key is zero, then we have found the first region of a table.
      // pick only those tables that were not modified in the last few milliseconds.
      if (info != null && info.getRegionInfo().getStartKey().length == 0 &&
        !info.getRegionInfo().isMetaRegion()) {
        if (info.modTime + timelag < now) {
          tableNames.add(info.getRegionInfo().getTable());
        } else {
          numSkipped.incrementAndGet(); // one more in-flux table
        }
      }
    }
    return getTableDescriptors(tableNames);
  }

  TableDescriptor[] getTableDescriptors(List<TableName> tableNames) {
      LOG.info("getTableDescriptors == tableNames => " + tableNames);
    try (Connection conn = ConnectionFactory.createConnection(getConf());
        Admin admin = conn.getAdmin()) {
      List<TableDescriptor> tds = admin.listTableDescriptors(tableNames);
      return tds.toArray(new TableDescriptor[tds.size()]);
    } catch (IOException e) {
      LOG.debug("Exception getting table descriptors", e);
    }
    return new TableDescriptor[0];
  }

  /**
   * Gets the entry in regionInfo corresponding to the the given encoded
   * region name. If the region has not been seen yet, a new entry is added
   * and returned.
   */
  private synchronized HbckRegionInfo getOrCreateInfo(String name) {
    HbckRegionInfo hbi = regionInfoMap.get(name);
    if (hbi == null) {
      hbi = new HbckRegionInfo(null);
      regionInfoMap.put(name, hbi);
    }
    return hbi;
  }

  private void checkAndFixReplication() throws ReplicationException {
    ReplicationChecker checker = new ReplicationChecker(getConf(), zkw, errors);
    checker.checkUnDeletedQueues();

    if (checker.hasUnDeletedQueues() && this.fixReplication) {
      checker.fixUnDeletedQueues();
      setShouldRerun();
    }
  }

  /**
    * Check values in regionInfo for hbase:meta
    * Check if zero or more than one regions with hbase:meta are found.
    * If there are inconsistencies (i.e. zero or more than one regions
    * pretend to be holding the hbase:meta) try to fix that and report an error.
    * @throws IOException from HBaseFsckRepair functions
    * @throws KeeperException
    * @throws InterruptedException
    */
  boolean checkMetaRegion() throws IOException, KeeperException, InterruptedException {
    Map<Integer, HbckRegionInfo> metaRegions = new HashMap<>();
    for (HbckRegionInfo value : regionInfoMap.values()) {
      if (value.getMetaEntry() != null && value.getMetaEntry().getRegionInfo().isMetaRegion()) {
        metaRegions.put(value.getReplicaId(), value);
      }
    }
    int metaReplication = admin.getDescriptor(TableName.META_TABLE_NAME)
        .getRegionReplication();
    boolean noProblem = true;
    // There will be always entries in regionInfoMap corresponding to hbase:meta & its replicas
    // Check the deployed servers. It should be exactly one server for each replica.
    for (int i = 0; i < metaReplication; i++) {
      HbckRegionInfo metaHbckRegionInfo = metaRegions.remove(i);
      List<ServerName> servers = new ArrayList<>();
      if (metaHbckRegionInfo != null) {
        servers = metaHbckRegionInfo.getDeployedOn();
      }
      if (servers.size() != 1) {
        noProblem = false;
        if (servers.isEmpty()) {
          assignMetaReplica(i);
        } else if (servers.size() > 1) {
          errors
          .reportError(ERROR_CODE.MULTI_META_REGION, "hbase:meta, replicaId " +
                       metaHbckRegionInfo.getReplicaId() + " is found on more than one region.");
          if (shouldFixAssignments()) {
            errors.print("Trying to fix a problem with hbase:meta, replicaId " +
                metaHbckRegionInfo.getReplicaId() + "..");
            setShouldRerun();
            // try fix it (treat is a dupe assignment)
            HBaseFsckRepair.fixMultiAssignment(connection,
              metaHbckRegionInfo.getMetaEntry().getRegionInfo(), servers);
          }
        }
      }
    }
    // unassign whatever is remaining in metaRegions. They are excess replicas.
    for (Map.Entry<Integer, HbckRegionInfo> entry : metaRegions.entrySet()) {
      noProblem = false;
      errors.reportError(ERROR_CODE.SHOULD_NOT_BE_DEPLOYED,
          "hbase:meta replicas are deployed in excess. Configured " + metaReplication +
          ", deployed " + metaRegions.size());
      if (shouldFixAssignments()) {
        errors.print("Trying to undeploy excess replica, replicaId: " + entry.getKey() +
            " of hbase:meta..");
        setShouldRerun();
        unassignMetaReplica(entry.getValue());
      }
    }
    // if noProblem is false, rerun hbck with hopefully fixed META
    // if noProblem is true, no errors, so continue normally
    return noProblem;
  }

  private void unassignMetaReplica(HbckRegionInfo hi)
    throws IOException, InterruptedException, KeeperException {
    undeployRegions(hi);
    ZKUtil.deleteNode(zkw,
      zkw.getZNodePaths().getZNodeForReplica(hi.getMetaEntry().getRegionInfo().getReplicaId()));
  }

  private void assignMetaReplica(int replicaId) {
    errors.reportError(ERROR_CODE.NO_META_REGION,
      "hbase:meta, replicaId " + replicaId + " is not found on any region.");
    throw new UnsupportedOperationException("fix meta region is not allowed");
  }

  /**
   * Scan hbase:meta, adding all regions found to the regionInfo map.
   * @throws IOException if an error is encountered
   */
  boolean loadMetaEntries() throws IOException {
    ClientMetaTableAccessor.Visitor visitor = new ClientMetaTableAccessor.Visitor() {
      int countRecord = 1;

      // comparator to sort KeyValues with latest modtime
      final Comparator<Cell> comp = new Comparator<Cell>() {
        @Override
        public int compare(Cell k1, Cell k2) {
          return Long.compare(k1.getTimestamp(), k2.getTimestamp());
        }
      };

      @Override
      public boolean visit(Result result) throws IOException {
        try {

          // record the latest modification of this META record
          long ts =  Collections.max(result.listCells(), comp).getTimestamp();
          RegionLocations rl = CatalogFamilyFormat.getRegionLocations(result);
          if (rl == null) {
            emptyRegionInfoQualifiers.add(result);
            errors.reportError(ERROR_CODE.EMPTY_META_CELL,
              "Empty REGIONINFO_QUALIFIER found in hbase:meta");
            return true;
          }
          ServerName sn = null;
          if (rl.getRegionLocation(RegionInfo.DEFAULT_REPLICA_ID) == null ||
              rl.getRegionLocation(RegionInfo.DEFAULT_REPLICA_ID).getRegion() == null) {
            emptyRegionInfoQualifiers.add(result);
            errors.reportError(ERROR_CODE.EMPTY_META_CELL,
              "Empty REGIONINFO_QUALIFIER found in hbase:meta");
            return true;
          }
          RegionInfo hri = rl.getRegionLocation(RegionInfo.DEFAULT_REPLICA_ID).getRegion();
          if (!(isTableIncluded(hri.getTable())
              || hri.isMetaRegion())) {
            return true;
          }
          PairOfSameType<RegionInfo> daughters = MetaTableAccessor.getDaughterRegions(result);
          for (HRegionLocation h : rl.getRegionLocations()) {
            if (h == null || h.getRegion() == null) {
              continue;
            }
            sn = h.getServerName();
            hri = h.getRegion();

            HbckRegionInfo.MetaEntry m = null;
            if (hri.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID) {
              m = new HbckRegionInfo.MetaEntry(hri, sn, ts, daughters.getFirst(),
                  daughters.getSecond());
            } else {
              m = new HbckRegionInfo.MetaEntry(hri, sn, ts, null, null);
            }
            HbckRegionInfo previous = regionInfoMap.get(hri.getEncodedName());
            if (previous == null) {
              regionInfoMap.put(hri.getEncodedName(), new HbckRegionInfo(m));
            } else if (previous.getMetaEntry() == null) {
              previous.setMetaEntry(m);
            } else {
              throw new IOException("Two entries in hbase:meta are same " + previous);
            }
          }
          List<RegionInfo> mergeParents = MetaTableAccessor.getMergeRegions(result.rawCells());
          if (mergeParents != null) {
            for (RegionInfo mergeRegion : mergeParents) {
              if (mergeRegion != null) {
                // This region is already being merged
                HbckRegionInfo hbInfo = getOrCreateInfo(mergeRegion.getEncodedName());
                hbInfo.setMerged(true);
              }
            }
          }

          // show proof of progress to the user, once for every 100 records.
          if (countRecord % 100 == 0) {
            errors.progress();
          }
          countRecord++;
          return true;
        } catch (RuntimeException e) {
          LOG.error("Result=" + result);
          throw e;
        }
      }
    };
    if (!checkMetaOnly) {
      // Scan hbase:meta to pick up user regions
      MetaTableAccessor.fullScanRegions(connection, visitor);
    }

    errors.print("");
    return true;
  }

  /**
   * Prints summary of all tables found on the system.
   */
  private void printTableSummary(SortedMap<TableName, HbckTableInfo> tablesInfo) {
    StringBuilder sb = new StringBuilder();
    int numOfSkippedRegions;
    errors.print("Summary:");
    for (HbckTableInfo tInfo : tablesInfo.values()) {
      numOfSkippedRegions = (skippedRegions.containsKey(tInfo.getName())) ?
          skippedRegions.get(tInfo.getName()).size() : 0;

      if (errors.tableHasErrors(tInfo)) {
        errors.print("Table " + tInfo.getName() + " is inconsistent.");
      } else if (numOfSkippedRegions > 0){
        errors.print("Table " + tInfo.getName() + " is okay (with "
          + numOfSkippedRegions + " skipped regions).");
      }
      else {
        errors.print("Table " + tInfo.getName() + " is okay.");
      }
      errors.print("    Number of regions: " + tInfo.getNumRegions());
      if (numOfSkippedRegions > 0) {
        Set<String> skippedRegionStrings = skippedRegions.get(tInfo.getName());
        System.out.println("    Number of skipped regions: " + numOfSkippedRegions);
        System.out.println("      List of skipped regions:");
        for(String sr : skippedRegionStrings) {
          System.out.println("        " + sr);
        }
      }
      sb.setLength(0); // clear out existing buffer, if any.
      sb.append("    Deployed on: ");
      for (ServerName server : tInfo.deployedOn) {
        sb.append(" " + server.toString());
      }
      errors.print(sb.toString());
    }
  }

  static HbckErrorReporter getErrorReporter(final Configuration conf)
      throws ClassNotFoundException {
    Class<? extends HbckErrorReporter> reporter =
        conf.getClass("hbasefsck.errorreporter", PrintingErrorReporter.class,
            HbckErrorReporter.class);
    return ReflectionUtils.newInstance(reporter, conf);
  }

  static class PrintingErrorReporter implements HbckErrorReporter {
    public int errorCount = 0;
    private int showProgress;
    // How frequently calls to progress() will create output
    private static final int progressThreshold = 100;

    Set<HbckTableInfo> errorTables = new HashSet<>();

    // for use by unit tests to verify which errors were discovered
    private ArrayList<ERROR_CODE> errorList = new ArrayList<>();

    @Override
    public void clear() {
      errorTables.clear();
      errorList.clear();
      errorCount = 0;
    }

    @Override
    public synchronized void reportError(ERROR_CODE errorCode, String message) {
      if (errorCode == ERROR_CODE.WRONG_USAGE) {
        System.err.println(message);
        return;
      }

      errorList.add(errorCode);
      if (!summary) {
        System.out.println("ERROR: " + message);
      }
      errorCount++;
      showProgress = 0;
    }

    @Override
    public synchronized void reportError(ERROR_CODE errorCode, String message,
        HbckTableInfo table) {
      errorTables.add(table);
      reportError(errorCode, message);
    }

    @Override
    public synchronized void reportError(ERROR_CODE errorCode, String message, HbckTableInfo table,
                                         HbckRegionInfo info) {
      errorTables.add(table);
      String reference = "(region " + info.getRegionNameAsString() + ")";
      reportError(errorCode, reference + " " + message);
    }

    @Override
    public synchronized void reportError(ERROR_CODE errorCode, String message, HbckTableInfo table,
                                         HbckRegionInfo info1, HbckRegionInfo info2) {
      errorTables.add(table);
      String reference = "(regions " + info1.getRegionNameAsString()
          + " and " + info2.getRegionNameAsString() + ")";
      reportError(errorCode, reference + " " + message);
    }

    @Override
    public synchronized void reportError(String message) {
      reportError(ERROR_CODE.UNKNOWN, message);
    }

    /**
     * Report error information, but do not increment the error count.  Intended for cases
     * where the actual error would have been reported previously.
     * @param message
     */
    @Override
    public synchronized void report(String message) {
      if (! summary) {
        System.out.println("ERROR: " + message);
      }
      showProgress = 0;
    }

    @Override
    public synchronized int summarize() {
      System.out.println(Integer.toString(errorCount) +
                         " inconsistencies detected.");
      if (errorCount == 0) {
        System.out.println("Status: OK");
        return 0;
      } else {
        System.out.println("Status: INCONSISTENT");
        return -1;
      }
    }

    @Override
    public ArrayList<ERROR_CODE> getErrorList() {
      return errorList;
    }

    @Override
    public synchronized void print(String message) {
      if (!summary) {
        System.out.println(message);
      }
    }

    @Override
    public boolean tableHasErrors(HbckTableInfo table) {
      return errorTables.contains(table);
    }

    @Override
    public void resetErrors() {
      errorCount = 0;
    }

    @Override
    public synchronized void detail(String message) {
      if (details) {
        System.out.println(message);
      }
      showProgress = 0;
    }

    @Override
    public synchronized void progress() {
      if (showProgress++ == progressThreshold) {
        if (!summary) {
          System.out.print(".");
        }
        showProgress = 0;
      }
    }
  }

  /**
   * Contact a region server and get all information from it
   */
  static class WorkItemRegion implements Callable<Void> {
    private final HBaseFsck hbck;
    private final ServerName rsinfo;
    private final HbckErrorReporter errors;
    private final Connection connection;

    WorkItemRegion(HBaseFsck hbck, ServerName info, HbckErrorReporter errors,
        Connection connection) {
      this.hbck = hbck;
      this.rsinfo = info;
      this.errors = errors;
      this.connection = connection;
    }

    @Override
    public synchronized Void call() throws IOException {
      errors.progress();
      try {
        // list all online regions from this region server
        List<RegionInfo> regions = connection.getAdmin().getRegions(rsinfo);
        regions = filterRegions(regions);

        if (details) {
          errors.detail(
            "RegionServer: " + rsinfo.getServerName() + " number of regions: " + regions.size());
          for (RegionInfo rinfo : regions) {
            errors.detail("  " + rinfo.getRegionNameAsString() + " id: " + rinfo.getRegionId() +
              " encoded_name: " + rinfo.getEncodedName() + " start: " +
              Bytes.toStringBinary(rinfo.getStartKey()) + " end: " +
              Bytes.toStringBinary(rinfo.getEndKey()));
          }
        }

        // check to see if the existence of this region matches the region in META
        for (RegionInfo r : regions) {
          HbckRegionInfo hbi = hbck.getOrCreateInfo(r.getEncodedName());
          hbi.addServer(r, rsinfo);
        }
      } catch (IOException e) { // unable to connect to the region server.
        errors.reportError(ERROR_CODE.RS_CONNECT_FAILURE,
          "RegionServer: " + rsinfo.getServerName() + " Unable to fetch region information. " + e);
        throw e;
      }
      return null;
    }

    private List<RegionInfo> filterRegions(List<RegionInfo> regions) {
      List<RegionInfo> ret = Lists.newArrayList();
      for (RegionInfo hri : regions) {
        if (hri.isMetaRegion() || (!hbck.checkMetaOnly
            && hbck.isTableIncluded(hri.getTable()))) {
          ret.add(hri);
        }
      }
      return ret;
    }
  }

  /**
   * Contact hdfs and get all information about specified table directory into
   * regioninfo list.
   */
  class WorkItemHdfsDir implements Callable<Void> {
    private FileStatus tableDir;
    private HbckErrorReporter errors;
    private FileSystem fs;

    WorkItemHdfsDir(FileSystem fs, HbckErrorReporter errors, FileStatus status) {
      this.fs = fs;
      this.tableDir = status;
      this.errors = errors;
    }

    @Override
    public synchronized Void call() throws InterruptedException, ExecutionException {
      final Vector<Exception> exceptions = new Vector<>();

      try {
        final FileStatus[] regionDirs = fs.listStatus(tableDir.getPath());
        final List<Future<?>> futures = new ArrayList<>(regionDirs.length);

        for (final FileStatus regionDir : regionDirs) {
          errors.progress();
          final String encodedName = regionDir.getPath().getName();
          // ignore directories that aren't hexadecimal
          if (!encodedName.toLowerCase(Locale.ROOT).matches("[0-9a-f]+")) {
            continue;
          }

          if (!exceptions.isEmpty()) {
            break;
          }

          futures.add(executor.submit(new Runnable() {
            @Override
            public void run() {
              try {
                LOG.debug("Loading region info from hdfs:"+ regionDir.getPath());

                Path regioninfoFile = new Path(regionDir.getPath(), HRegionFileSystem.REGION_INFO_FILE);
                boolean regioninfoFileExists = fs.exists(regioninfoFile);

                if (!regioninfoFileExists) {
                  // As tables become larger it is more and more likely that by the time you
                  // reach a given region that it will be gone due to region splits/merges.
                  if (!fs.exists(regionDir.getPath())) {
                    LOG.warn("By the time we tried to process this region dir it was already gone: "
                        + regionDir.getPath());
                    return;
                  }
                }

                HbckRegionInfo hbi = HBaseFsck.this.getOrCreateInfo(encodedName);
                HbckRegionInfo.HdfsEntry he = new HbckRegionInfo.HdfsEntry();
                synchronized (hbi) {
                  if (hbi.getHdfsRegionDir() != null) {
                    errors.print("Directory " + encodedName + " duplicate??" +
                                 hbi.getHdfsRegionDir());
                  }

                  he.regionDir = regionDir.getPath();
                  he.regionDirModTime = regionDir.getModificationTime();
                  he.hdfsRegioninfoFilePresent = regioninfoFileExists;
                  // we add to orphan list when we attempt to read .regioninfo

                  // Set a flag if this region contains only edits
                  // This is special case if a region is left after split
                  he.hdfsOnlyEdits = true;
                  FileStatus[] subDirs = fs.listStatus(regionDir.getPath());
                  Path ePath = WALSplitUtil.getRegionDirRecoveredEditsDir(regionDir.getPath());
                  for (FileStatus subDir : subDirs) {
                    errors.progress();
                    String sdName = subDir.getPath().getName();
                    if (!sdName.startsWith(".") && !sdName.equals(ePath.getName())) {
                      he.hdfsOnlyEdits = false;
                      break;
                    }
                  }
                  hbi.setHdfsEntry(he);
                }
              } catch (Exception e) {
                LOG.error("Could not load region dir", e);
                exceptions.add(e);
              }
            }
          }));
        }

        // Ensure all pending tasks are complete (or that we run into an exception)
        for (Future<?> f : futures) {
          if (!exceptions.isEmpty()) {
            break;
          }
          try {
            f.get();
          } catch (ExecutionException e) {
            LOG.error("Unexpected exec exception!  Should've been caught already.  (Bug?)", e);
            // Shouldn't happen, we already logged/caught any exceptions in the Runnable
          }
        }
      } catch (IOException e) {
        LOG.error("Cannot execute WorkItemHdfsDir for " + tableDir, e);
        exceptions.add(e);
      } finally {
        if (!exceptions.isEmpty()) {
          errors.reportError(ERROR_CODE.RS_CONNECT_FAILURE, "Table Directory: "
              + tableDir.getPath().getName()
              + " Unable to fetch all HDFS region information. ");
          // Just throw the first exception as an indication something bad happened
          // Don't need to propagate all the exceptions, we already logged them all anyway
          throw new ExecutionException("First exception in WorkItemHdfsDir", exceptions.firstElement());
        }
      }
      return null;
    }
  }

  /**
   * Contact hdfs and get all information about specified table directory into
   * regioninfo list.
   */
  static class WorkItemHdfsRegionInfo implements Callable<Void> {
    private HbckRegionInfo hbi;
    private HBaseFsck hbck;
    private HbckErrorReporter errors;

    WorkItemHdfsRegionInfo(HbckRegionInfo hbi, HBaseFsck hbck, HbckErrorReporter errors) {
      this.hbi = hbi;
      this.hbck = hbck;
      this.errors = errors;
    }

    @Override
    public synchronized Void call() throws IOException {
      // only load entries that haven't been loaded yet.
      if (hbi.getHdfsHRI() == null) {
        try {
          errors.progress();
          hbi.loadHdfsRegioninfo(hbck.getConf());
        } catch (IOException ioe) {
          String msg = "Orphan region in HDFS: Unable to load .regioninfo from table "
              + hbi.getTableName() + " in hdfs dir "
              + hbi.getHdfsRegionDir()
              + "!  It may be an invalid format or version file.  Treating as "
              + "an orphaned regiondir.";
          errors.reportError(ERROR_CODE.ORPHAN_HDFS_REGION, msg);
          try {
            hbck.debugLsr(hbi.getHdfsRegionDir());
          } catch (IOException ioe2) {
            LOG.error("Unable to read directory " + hbi.getHdfsRegionDir(), ioe2);
            throw ioe2;
          }
          hbck.orphanHdfsDirs.add(hbi);
          throw ioe;
        }
      }
      return null;
    }
  }

  /**
   * Display the full report from fsck. This displays all live and dead region
   * servers, and all known regions.
   */
  public static void setDisplayFullReport() {
    details = true;
  }

  public static boolean shouldDisplayFullReport() {
    return details;
  }

  /**
   * Set exclusive mode.
   */
  public static void setForceExclusive() {
    forceExclusive = true;
  }

  /**
   * Only one instance of hbck can modify HBase at a time.
   */
  public boolean isExclusive() {
    return fixAny || forceExclusive;
  }

  /**
   * Set summary mode.
   * Print only summary of the tables and status (OK or INCONSISTENT)
   */
  static void setSummary() {
    summary = true;
  }

  /**
   * Set hbase:meta check mode.
   * Print only info about hbase:meta table deployment/state
   */
  void setCheckMetaOnly() {
    checkMetaOnly = true;
  }

  /**
   * Set region boundaries check mode.
   */
  void setRegionBoundariesCheck() {
    checkRegionBoundaries = true;
  }

  /**
   * Set replication fix mode.
   */
  public void setFixReplication(boolean shouldFix) {
    fixReplication = shouldFix;
    fixAny |= shouldFix;
  }

  public void setCleanReplicationBarrier(boolean shouldClean) {
    cleanReplicationBarrier = shouldClean;
  }

  /**
   * Check if we should rerun fsck again. This checks if we've tried to
   * fix something and we should rerun fsck tool again.
   * Display the full report from fsck. This displays all live and dead
   * region servers, and all known regions.
   */
  void setShouldRerun() {
    rerun = true;
  }

  public boolean shouldRerun() {
    return rerun;
  }

  /**
   * Fix inconsistencies found by fsck. This should try to fix errors (if any)
   * found by fsck utility.
   */
  public void setFixAssignments(boolean shouldFix) {
    fixAssignments = shouldFix;
    fixAny |= shouldFix;
  }

  boolean shouldFixAssignments() {
    return fixAssignments;
  }

  public void setFixMeta(boolean shouldFix) {
    fixMeta = shouldFix;
    fixAny |= shouldFix;
  }

  boolean shouldFixMeta() {
    return fixMeta;
  }

  public void setFixEmptyMetaCells(boolean shouldFix) {
    fixEmptyMetaCells = shouldFix;
    fixAny |= shouldFix;
  }

  boolean shouldFixEmptyMetaCells() {
    return fixEmptyMetaCells;
  }

  public void setCheckHdfs(boolean checking) {
    checkHdfs = checking;
  }

  boolean shouldCheckHdfs() {
    return checkHdfs;
  }

  public void setFixHdfsHoles(boolean shouldFix) {
    fixHdfsHoles = shouldFix;
    fixAny |= shouldFix;
  }

  boolean shouldFixHdfsHoles() {
    return fixHdfsHoles;
  }

  public void setFixTableOrphans(boolean shouldFix) {
    fixTableOrphans = shouldFix;
    fixAny |= shouldFix;
  }

  boolean shouldFixTableOrphans() {
    return fixTableOrphans;
  }

  public void setFixHdfsOverlaps(boolean shouldFix) {
    fixHdfsOverlaps = shouldFix;
    fixAny |= shouldFix;
  }

  boolean shouldFixHdfsOverlaps() {
    return fixHdfsOverlaps;
  }

  public void setFixHdfsOrphans(boolean shouldFix) {
    fixHdfsOrphans = shouldFix;
    fixAny |= shouldFix;
  }

  boolean shouldFixHdfsOrphans() {
    return fixHdfsOrphans;
  }

  public void setFixVersionFile(boolean shouldFix) {
    fixVersionFile = shouldFix;
    fixAny |= shouldFix;
  }

  public boolean shouldFixVersionFile() {
    return fixVersionFile;
  }

  public void setSidelineBigOverlaps(boolean sbo) {
    this.sidelineBigOverlaps = sbo;
  }

  public boolean shouldSidelineBigOverlaps() {
    return sidelineBigOverlaps;
  }

  public void setFixSplitParents(boolean shouldFix) {
    fixSplitParents = shouldFix;
    fixAny |= shouldFix;
  }

  public void setRemoveParents(boolean shouldFix) {
    removeParents = shouldFix;
    fixAny |= shouldFix;
  }

  boolean shouldFixSplitParents() {
    return fixSplitParents;
  }

  boolean shouldRemoveParents() {
    return removeParents;
  }

  public void setFixReferenceFiles(boolean shouldFix) {
    fixReferenceFiles = shouldFix;
    fixAny |= shouldFix;
  }

  boolean shouldFixReferenceFiles() {
    return fixReferenceFiles;
  }

  public void setFixHFileLinks(boolean shouldFix) {
    fixHFileLinks = shouldFix;
    fixAny |= shouldFix;
  }

  boolean shouldFixHFileLinks() {
    return fixHFileLinks;
  }

  public boolean shouldIgnorePreCheckPermission() {
    return !fixAny || ignorePreCheckPermission;
  }

  public void setIgnorePreCheckPermission(boolean ignorePreCheckPermission) {
    this.ignorePreCheckPermission = ignorePreCheckPermission;
  }

  /**
   * @param mm maximum number of regions to merge into a single region.
   */
  public void setMaxMerge(int mm) {
    this.maxMerge = mm;
  }

  public int getMaxMerge() {
    return maxMerge;
  }

  public void setMaxOverlapsToSideline(int mo) {
    this.maxOverlapsToSideline = mo;
  }

  public int getMaxOverlapsToSideline() {
    return maxOverlapsToSideline;
  }

  /**
   * Only check/fix tables specified by the list,
   * Empty list means all tables are included.
   */
  boolean isTableIncluded(TableName table) {
    return (tablesIncluded.isEmpty()) || tablesIncluded.contains(table);
  }

  public void includeTable(TableName table) {
    tablesIncluded.add(table);
  }

  Set<TableName> getIncludedTables() {
    return new HashSet<>(tablesIncluded);
  }

  /**
   * We are interested in only those tables that have not changed their state in
   * hbase:meta during the last few seconds specified by hbase.admin.fsck.timelag
   * @param seconds - the time in seconds
   */
  public void setTimeLag(long seconds) {
    timelag = seconds * 1000; // convert to milliseconds
  }

  /**
   *
   * @param sidelineDir - HDFS path to sideline data
   */
  public void setSidelineDir(String sidelineDir) {
    this.sidelineDir = new Path(sidelineDir);
  }

  protected HFileCorruptionChecker createHFileCorruptionChecker(boolean sidelineCorruptHFiles) throws IOException {
    return new HFileCorruptionChecker(getConf(), executor, sidelineCorruptHFiles);
  }

  public HFileCorruptionChecker getHFilecorruptionChecker() {
    return hfcc;
  }

  public void setHFileCorruptionChecker(HFileCorruptionChecker hfcc) {
    this.hfcc = hfcc;
  }

  public void setRetCode(int code) {
    this.retcode = code;
  }

  public int getRetCode() {
    return retcode;
  }

  protected HBaseFsck printUsageAndExit() {
    StringWriter sw = new StringWriter(2048);
    PrintWriter out = new PrintWriter(sw);
    out.println("");
    out.println("-----------------------------------------------------------------------");
    out.println("NOTE: As of HBase version 2.0, the hbck tool is significantly changed.");
    out.println("In general, all Read-Only options are supported and can be be used");
    out.println("safely. Most -fix/ -repair options are NOT supported. Please see usage");
    out.println("below for details on which options are not supported.");
    out.println("-----------------------------------------------------------------------");
    out.println("");
    out.println("Usage: fsck [opts] {only tables}");
    out.println(" where [opts] are:");
    out.println("   -help Display help options (this)");
    out.println("   -details Display full report of all regions.");
    out.println("   -timelag <timeInSeconds>  Process only regions that " +
                       " have not experienced any metadata updates in the last " +
                       " <timeInSeconds> seconds.");
    out.println("   -sleepBeforeRerun <timeInSeconds> Sleep this many seconds" +
        " before checking if the fix worked if run with -fix");
    out.println("   -summary Print only summary of the tables and status.");
    out.println("   -metaonly Only check the state of the hbase:meta table.");
    out.println("   -sidelineDir <hdfs://> HDFS path to backup existing meta.");
    out.println("   -boundaries Verify that regions boundaries are the same between META and store files.");
    out.println("   -exclusive Abort if another hbck is exclusive or fixing.");

    out.println("");
    out.println("  Datafile Repair options: (expert features, use with caution!)");
    out.println("   -checkCorruptHFiles     Check all Hfiles by opening them to make sure they are valid");
    out.println("   -sidelineCorruptHFiles  Quarantine corrupted HFiles.  implies -checkCorruptHFiles");

    out.println("");
    out.println(" Replication options");
    out.println("   -fixReplication   Deletes replication queues for removed peers");

    out.println("");
    out.println("  Metadata Repair options supported as of version 2.0: (expert features, use with caution!)");
    out.println("   -fixVersionFile   Try to fix missing hbase.version file in hdfs.");
    out.println("   -fixReferenceFiles  Try to offline lingering reference store files");
    out.println("   -fixHFileLinks  Try to offline lingering HFileLinks");
    out.println("   -noHdfsChecking   Don't load/check region info from HDFS."
        + " Assumes hbase:meta region info is good. Won't check/fix any HDFS issue, e.g. hole, orphan, or overlap");
    out.println("   -ignorePreCheckPermission  ignore filesystem permission pre-check");

    out.println("");
    out.println("NOTE: Following options are NOT supported as of HBase version 2.0+.");
    out.println("");
    out.println("  UNSUPPORTED Metadata Repair options: (expert features, use with caution!)");
    out.println("   -fix              Try to fix region assignments.  This is for backwards compatiblity");
    out.println("   -fixAssignments   Try to fix region assignments.  Replaces the old -fix");
    out.println("   -fixMeta          Try to fix meta problems.  This assumes HDFS region info is good.");
    out.println("   -fixHdfsHoles     Try to fix region holes in hdfs.");
    out.println("   -fixHdfsOrphans   Try to fix region dirs with no .regioninfo file in hdfs");
    out.println("   -fixTableOrphans  Try to fix table dirs with no .tableinfo file in hdfs (online mode only)");
    out.println("   -fixHdfsOverlaps  Try to fix region overlaps in hdfs.");
    out.println("   -maxMerge <n>     When fixing region overlaps, allow at most <n> regions to merge. (n=" + DEFAULT_MAX_MERGE +" by default)");
    out.println("   -sidelineBigOverlaps  When fixing region overlaps, allow to sideline big overlaps");
    out.println("   -maxOverlapsToSideline <n>  When fixing region overlaps, allow at most <n> regions to sideline per group. (n=" + DEFAULT_OVERLAPS_TO_SIDELINE +" by default)");
    out.println("   -fixSplitParents  Try to force offline split parents to be online.");
    out.println("   -removeParents    Try to offline and sideline lingering parents and keep daughter regions.");
    out.println("   -fixEmptyMetaCells  Try to fix hbase:meta entries not referencing any region"
        + " (empty REGIONINFO_QUALIFIER rows)");

    out.println("");
    out.println("  UNSUPPORTED Metadata Repair shortcuts");
    out.println("   -repair           Shortcut for -fixAssignments -fixMeta -fixHdfsHoles " +
        "-fixHdfsOrphans -fixHdfsOverlaps -fixVersionFile -sidelineBigOverlaps -fixReferenceFiles" +
        "-fixHFileLinks");
    out.println("   -repairHoles      Shortcut for -fixAssignments -fixMeta -fixHdfsHoles");
    out.println("");
    out.println(" Replication options");
    out.println("   -fixReplication   Deletes replication queues for removed peers");
    out.println("   -cleanReplicationBrarier [tableName] clean the replication barriers " +
        "of a specified table, tableName is required");
    out.flush();
    errors.reportError(ERROR_CODE.WRONG_USAGE, sw.toString());

    setRetCode(-2);
    return this;
  }

  /**
   * Main program
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    // create a fsck object
    Configuration conf = HBaseConfiguration.create();
    Path hbasedir = CommonFSUtils.getRootDir(conf);
    URI defaultFs = hbasedir.getFileSystem(conf).getUri();
    CommonFSUtils.setFsDefault(conf, new Path(defaultFs));
    int ret = ToolRunner.run(new HBaseFsckTool(conf), args);
    System.exit(ret);
  }

  /**
   * This is a Tool wrapper that gathers -Dxxx=yyy configuration settings from the command line.
   */
  static class HBaseFsckTool extends Configured implements Tool {
    HBaseFsckTool(Configuration conf) { super(conf); }
    @Override
    public int run(String[] args) throws Exception {
      HBaseFsck hbck = new HBaseFsck(getConf());
      hbck.exec(hbck.executor, args);
      hbck.close();
      return hbck.getRetCode();
    }
  }

  public HBaseFsck exec(ExecutorService exec, String[] args)
      throws KeeperException, IOException, InterruptedException, ReplicationException {
    long sleepBeforeRerun = DEFAULT_SLEEP_BEFORE_RERUN;

    boolean checkCorruptHFiles = false;
    boolean sidelineCorruptHFiles = false;

    // Process command-line args.
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      if (cmd.equals("-help") || cmd.equals("-h")) {
        return printUsageAndExit();
      } else if (cmd.equals("-details")) {
        setDisplayFullReport();
      } else if (cmd.equals("-exclusive")) {
        setForceExclusive();
      } else if (cmd.equals("-timelag")) {
        if (i == args.length - 1) {
          errors.reportError(ERROR_CODE.WRONG_USAGE, "HBaseFsck: -timelag needs a value.");
          return printUsageAndExit();
        }
        try {
          long timelag = Long.parseLong(args[++i]);
          setTimeLag(timelag);
        } catch (NumberFormatException e) {
          errors.reportError(ERROR_CODE.WRONG_USAGE, "-timelag needs a numeric value.");
          return printUsageAndExit();
        }
      } else if (cmd.equals("-sleepBeforeRerun")) {
        if (i == args.length - 1) {
          errors.reportError(ERROR_CODE.WRONG_USAGE,
            "HBaseFsck: -sleepBeforeRerun needs a value.");
          return printUsageAndExit();
        }
        try {
          sleepBeforeRerun = Long.parseLong(args[++i]);
        } catch (NumberFormatException e) {
          errors.reportError(ERROR_CODE.WRONG_USAGE, "-sleepBeforeRerun needs a numeric value.");
          return printUsageAndExit();
        }
      } else if (cmd.equals("-sidelineDir")) {
        if (i == args.length - 1) {
          errors.reportError(ERROR_CODE.WRONG_USAGE, "HBaseFsck: -sidelineDir needs a value.");
          return printUsageAndExit();
        }
        setSidelineDir(args[++i]);
      } else if (cmd.equals("-fix")) {
        errors.reportError(ERROR_CODE.WRONG_USAGE,
          "This option is deprecated, please use  -fixAssignments instead.");
        setFixAssignments(true);
      } else if (cmd.equals("-fixAssignments")) {
        setFixAssignments(true);
      } else if (cmd.equals("-fixMeta")) {
        setFixMeta(true);
      } else if (cmd.equals("-noHdfsChecking")) {
        setCheckHdfs(false);
      } else if (cmd.equals("-fixHdfsHoles")) {
        setFixHdfsHoles(true);
      } else if (cmd.equals("-fixHdfsOrphans")) {
        setFixHdfsOrphans(true);
      } else if (cmd.equals("-fixTableOrphans")) {
        setFixTableOrphans(true);
      } else if (cmd.equals("-fixHdfsOverlaps")) {
        setFixHdfsOverlaps(true);
      } else if (cmd.equals("-fixVersionFile")) {
        setFixVersionFile(true);
      } else if (cmd.equals("-sidelineBigOverlaps")) {
        setSidelineBigOverlaps(true);
      } else if (cmd.equals("-fixSplitParents")) {
        setFixSplitParents(true);
      } else if (cmd.equals("-removeParents")) {
        setRemoveParents(true);
      } else if (cmd.equals("-ignorePreCheckPermission")) {
        setIgnorePreCheckPermission(true);
      } else if (cmd.equals("-checkCorruptHFiles")) {
        checkCorruptHFiles = true;
      } else if (cmd.equals("-sidelineCorruptHFiles")) {
        sidelineCorruptHFiles = true;
      } else if (cmd.equals("-fixReferenceFiles")) {
        setFixReferenceFiles(true);
      } else if (cmd.equals("-fixHFileLinks")) {
        setFixHFileLinks(true);
      } else if (cmd.equals("-fixEmptyMetaCells")) {
        setFixEmptyMetaCells(true);
      } else if (cmd.equals("-repair")) {
        // this attempts to merge overlapping hdfs regions, needs testing
        // under load
        setFixHdfsHoles(true);
        setFixHdfsOrphans(true);
        setFixMeta(true);
        setFixAssignments(true);
        setFixHdfsOverlaps(true);
        setFixVersionFile(true);
        setSidelineBigOverlaps(true);
        setFixSplitParents(false);
        setCheckHdfs(true);
        setFixReferenceFiles(true);
        setFixHFileLinks(true);
      } else if (cmd.equals("-repairHoles")) {
        // this will make all missing hdfs regions available but may lose data
        setFixHdfsHoles(true);
        setFixHdfsOrphans(false);
        setFixMeta(true);
        setFixAssignments(true);
        setFixHdfsOverlaps(false);
        setSidelineBigOverlaps(false);
        setFixSplitParents(false);
        setCheckHdfs(true);
      } else if (cmd.equals("-maxOverlapsToSideline")) {
        if (i == args.length - 1) {
          errors.reportError(ERROR_CODE.WRONG_USAGE,
            "-maxOverlapsToSideline needs a numeric value argument.");
          return printUsageAndExit();
        }
        try {
          int maxOverlapsToSideline = Integer.parseInt(args[++i]);
          setMaxOverlapsToSideline(maxOverlapsToSideline);
        } catch (NumberFormatException e) {
          errors.reportError(ERROR_CODE.WRONG_USAGE,
            "-maxOverlapsToSideline needs a numeric value argument.");
          return printUsageAndExit();
        }
      } else if (cmd.equals("-maxMerge")) {
        if (i == args.length - 1) {
          errors.reportError(ERROR_CODE.WRONG_USAGE,
            "-maxMerge needs a numeric value argument.");
          return printUsageAndExit();
        }
        try {
          int maxMerge = Integer.parseInt(args[++i]);
          setMaxMerge(maxMerge);
        } catch (NumberFormatException e) {
          errors.reportError(ERROR_CODE.WRONG_USAGE,
            "-maxMerge needs a numeric value argument.");
          return printUsageAndExit();
        }
      } else if (cmd.equals("-summary")) {
        setSummary();
      } else if (cmd.equals("-metaonly")) {
        setCheckMetaOnly();
      } else if (cmd.equals("-boundaries")) {
        setRegionBoundariesCheck();
      } else if (cmd.equals("-fixReplication")) {
        setFixReplication(true);
      } else if (cmd.equals("-cleanReplicationBarrier")) {
        setCleanReplicationBarrier(true);
        if(args[++i].startsWith("-")){
          printUsageAndExit();
        }
        setCleanReplicationBarrierTable(args[i]);
      } else if (cmd.startsWith("-")) {
        errors.reportError(ERROR_CODE.WRONG_USAGE, "Unrecognized option:" + cmd);
        return printUsageAndExit();
      } else {
        includeTable(TableName.valueOf(cmd));
        errors.print("Allow checking/fixes for table: " + cmd);
      }
    }

    errors.print("HBaseFsck command line options: " + StringUtils.join(args, " "));

    // pre-check current user has FS write permission or not
    try {
      preCheckPermission();
    } catch (IOException ioe) {
      Runtime.getRuntime().exit(-1);
    }

    // do the real work of hbck
    connect();

    // after connecting to server above, we have server version
    // check if unsupported option is specified based on server version
    if (!isOptionsSupported(args)) {
      return printUsageAndExit();
    }

    try {
      // if corrupt file mode is on, first fix them since they may be opened later
      if (checkCorruptHFiles || sidelineCorruptHFiles) {
        LOG.info("Checking all hfiles for corruption");
        HFileCorruptionChecker hfcc = createHFileCorruptionChecker(sidelineCorruptHFiles);
        setHFileCorruptionChecker(hfcc); // so we can get result
        Collection<TableName> tables = getIncludedTables();
        Collection<Path> tableDirs = new ArrayList<>();
        Path rootdir = CommonFSUtils.getRootDir(getConf());
        if (tables.size() > 0) {
          for (TableName t : tables) {
            tableDirs.add(CommonFSUtils.getTableDir(rootdir, t));
          }
        } else {
          tableDirs = FSUtils.getTableDirs(CommonFSUtils.getCurrentFileSystem(getConf()), rootdir);
        }
        hfcc.checkTables(tableDirs);
        hfcc.report(errors);
      }

      // check and fix table integrity, region consistency.
      int code = onlineHbck();
      setRetCode(code);
      // If we have changed the HBase state it is better to run hbck again
      // to see if we haven't broken something else in the process.
      // We run it only once more because otherwise we can easily fall into
      // an infinite loop.
      if (shouldRerun()) {
        try {
          LOG.info("Sleeping " + sleepBeforeRerun + "ms before re-checking after fix...");
          Thread.sleep(sleepBeforeRerun);
        } catch (InterruptedException ie) {
          LOG.warn("Interrupted while sleeping");
          return this;
        }
        // Just report
        setFixAssignments(false);
        setFixMeta(false);
        setFixHdfsHoles(false);
        setFixHdfsOverlaps(false);
        setFixVersionFile(false);
        setFixTableOrphans(false);
        errors.resetErrors();
        code = onlineHbck();
        setRetCode(code);
      }
    } finally {
      IOUtils.closeQuietly(this);
    }
    return this;
  }

  private boolean isOptionsSupported(String[] args) {
    boolean result = true;
    String hbaseServerVersion = status.getHBaseVersion();
    if (VersionInfo.compareVersion("2.any.any", hbaseServerVersion) < 0) {
      // Process command-line args.
      for (String arg : args) {
        if (unsupportedOptionsInV2.contains(arg)) {
          errors.reportError(ERROR_CODE.UNSUPPORTED_OPTION,
              "option '" + arg + "' is not " + "supportted!");
          result = false;
          break;
        }
      }
    }
    return result;
  }

  public void setCleanReplicationBarrierTable(String cleanReplicationBarrierTable) {
    this.cleanReplicationBarrierTable = TableName.valueOf(cleanReplicationBarrierTable);
  }

  public void cleanReplicationBarrier() throws IOException {
    if (!cleanReplicationBarrier || cleanReplicationBarrierTable == null) {
      return;
    }
    if (cleanReplicationBarrierTable.isSystemTable()) {
      errors.reportError(ERROR_CODE.INVALID_TABLE,
        "invalid table: " + cleanReplicationBarrierTable);
      return;
    }

    boolean isGlobalScope = false;
    try {
      isGlobalScope = admin.getDescriptor(cleanReplicationBarrierTable).hasGlobalReplicationScope();
    } catch (TableNotFoundException e) {
      LOG.info("we may need to clean some erroneous data due to bugs");
    }

    if (isGlobalScope) {
      errors.reportError(ERROR_CODE.INVALID_TABLE,
        "table's replication scope is global: " + cleanReplicationBarrierTable);
      return;
    }
    List<byte[]> regionNames = new ArrayList<>();
    Scan barrierScan = new Scan();
    barrierScan.setCaching(100);
    barrierScan.addFamily(HConstants.REPLICATION_BARRIER_FAMILY);
    barrierScan
        .withStartRow(ClientMetaTableAccessor.getTableStartRowForMeta(cleanReplicationBarrierTable,
          ClientMetaTableAccessor.QueryType.REGION))
        .withStopRow(ClientMetaTableAccessor.getTableStopRowForMeta(cleanReplicationBarrierTable,
          ClientMetaTableAccessor.QueryType.REGION));
    Result result;
    try (ResultScanner scanner = meta.getScanner(barrierScan)) {
      while ((result = scanner.next()) != null) {
        regionNames.add(result.getRow());
      }
    }
    if (regionNames.size() <= 0) {
      errors.reportError(ERROR_CODE.INVALID_TABLE,
        "there is no barriers of this table: " + cleanReplicationBarrierTable);
      return;
    }
    ReplicationQueueStorage queueStorage =
        ReplicationStorageFactory.getReplicationQueueStorage(zkw, getConf());
    List<ReplicationPeerDescription> peerDescriptions = admin.listReplicationPeers();
    if (peerDescriptions != null && peerDescriptions.size() > 0) {
      List<String> peers = peerDescriptions.stream()
          .filter(peerConfig -> peerConfig.getPeerConfig()
            .needToReplicate(cleanReplicationBarrierTable))
          .map(peerConfig -> peerConfig.getPeerId()).collect(Collectors.toList());
      try {
        List<String> batch = new ArrayList<>();
        for (String peer : peers) {
          for (byte[] regionName : regionNames) {
            batch.add(RegionInfo.encodeRegionName(regionName));
            if (batch.size() % 100 == 0) {
              queueStorage.removeLastSequenceIds(peer, batch);
              batch.clear();
            }
          }
          if (batch.size() > 0) {
            queueStorage.removeLastSequenceIds(peer, batch);
            batch.clear();
          }
        }
      } catch (ReplicationException re) {
        throw new IOException(re);
      }
    }
    for (byte[] regionName : regionNames) {
      meta.delete(new Delete(regionName).addFamily(HConstants.REPLICATION_BARRIER_FAMILY));
    }
    setShouldRerun();
  }

  /**
   * ls -r for debugging purposes
   */
  void debugLsr(Path p) throws IOException {
    debugLsr(getConf(), p, errors);
  }

  /**
   * ls -r for debugging purposes
   */
  public static void debugLsr(Configuration conf,
      Path p) throws IOException {
    debugLsr(conf, p, new PrintingErrorReporter());
  }

  /**
   * ls -r for debugging purposes
   */
  public static void debugLsr(Configuration conf,
      Path p, HbckErrorReporter errors) throws IOException {
    if (!LOG.isDebugEnabled() || p == null) {
      return;
    }
    FileSystem fs = p.getFileSystem(conf);

    if (!fs.exists(p)) {
      // nothing
      return;
    }
    errors.print(p.toString());

    if (fs.isFile(p)) {
      return;
    }

    if (fs.getFileStatus(p).isDirectory()) {
      FileStatus[] fss= fs.listStatus(p);
      for (FileStatus status : fss) {
        debugLsr(conf, status.getPath(), errors);
      }
    }
  }
}
