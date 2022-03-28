/**
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
package org.apache.hadoop.hbase.master.snapshot;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MetricsMaster;
import org.apache.hadoop.hbase.master.SnapshotSentinel;
import org.apache.hadoop.hbase.master.WorkerAssigner;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.master.cleaner.HFileLinkCleaner;
import org.apache.hadoop.hbase.master.procedure.CloneSnapshotProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureScheduler;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureUtil;
import org.apache.hadoop.hbase.master.procedure.RestoreSnapshotProcedure;
import org.apache.hadoop.hbase.master.procedure.SnapshotProcedure;
import org.apache.hadoop.hbase.master.procedure.SnapshotVerifyProcedure;
import org.apache.hadoop.hbase.procedure.MasterProcedureManager;
import org.apache.hadoop.hbase.procedure.Procedure;
import org.apache.hadoop.hbase.procedure.ProcedureCoordinator;
import org.apache.hadoop.hbase.procedure.ProcedureCoordinatorRpcs;
import org.apache.hadoop.hbase.procedure.ZKProcedureCoordinator;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerValidationUtils;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.SnapshotScannerHDFSAclCleaner;
import org.apache.hadoop.hbase.security.access.SnapshotScannerHDFSAclHelper;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotDoesNotExistException;
import org.apache.hadoop.hbase.snapshot.SnapshotExistsException;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.snapshot.TablePartiallyOpenException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.NonceKey;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription.Type;

/**
 * This class manages the procedure of taking and restoring snapshots. There is only one
 * SnapshotManager for the master.
 * <p>
 * The class provides methods for monitoring in-progress snapshot actions.
 * <p>
 * Note: Currently there can only be one snapshot being taken at a time over the cluster. This is a
 * simplification in the current implementation.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
@InterfaceStability.Unstable
public class SnapshotManager extends MasterProcedureManager implements Stoppable {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotManager.class);

  /** By default, check to see if the snapshot is complete every WAKE MILLIS (ms) */
  private static final int SNAPSHOT_WAKE_MILLIS_DEFAULT = 500;

  /**
   * Wait time before removing a finished sentinel from the in-progress map
   *
   * NOTE: This is used as a safety auto cleanup.
   * The snapshot and restore handlers map entries are removed when a user asks if a snapshot or
   * restore is completed. This operation is part of the HBaseAdmin snapshot/restore API flow.
   * In case something fails on the client side and the snapshot/restore state is not reclaimed
   * after a default timeout, the entry is removed from the in-progress map.
   * At this point, if the user asks for the snapshot/restore status, the result will be
   * snapshot done if exists or failed if it doesn't exists.
   */
  public static final String HBASE_SNAPSHOT_SENTINELS_CLEANUP_TIMEOUT_MILLIS =
      "hbase.snapshot.sentinels.cleanup.timeoutMillis";
  public static final long SNAPSHOT_SENTINELS_CLEANUP_TIMEOUT_MILLS_DEFAULT = 60 * 1000L;

  /** Enable or disable snapshot support */
  public static final String HBASE_SNAPSHOT_ENABLED = "hbase.snapshot.enabled";

  /**
   * Conf key for # of ms elapsed between checks for snapshot errors while waiting for
   * completion.
   */
  private static final String SNAPSHOT_WAKE_MILLIS_KEY = "hbase.snapshot.master.wakeMillis";

  /** Name of the operation to use in the controller */
  public static final String ONLINE_SNAPSHOT_CONTROLLER_DESCRIPTION = "online-snapshot";

  /** Conf key for # of threads used by the SnapshotManager thread pool */
  public static final String SNAPSHOT_POOL_THREADS_KEY = "hbase.snapshot.master.threads";

  /** number of current operations running on the master */
  public static final int SNAPSHOT_POOL_THREADS_DEFAULT = 1;

  /** Conf key for preserving original max file size configs */
  public static final String SNAPSHOT_MAX_FILE_SIZE_PRESERVE =
    "hbase.snapshot.max.filesize.preserve";

  /** Enable or disable snapshot procedure */
  public static final String SNAPSHOT_PROCEDURE_ENABLED = "hbase.snapshot.procedure.enabled";

  public static final boolean SNAPSHOT_PROCEDURE_ENABLED_DEFAULT = true;

  private boolean stopped;
  private MasterServices master;  // Needed by TableEventHandlers
  private ProcedureCoordinator coordinator;

  // Is snapshot feature enabled?
  private boolean isSnapshotSupported = false;

  // Snapshot handlers map, with table name as key.
  // The map is always accessed and modified under the object lock using synchronized.
  // snapshotTable() will insert an Handler in the table.
  // isSnapshotDone() will remove the handler requested if the operation is finished.
  private final Map<TableName, SnapshotSentinel> snapshotHandlers = new ConcurrentHashMap<>();
  private final ScheduledExecutorService scheduleThreadPool =
      Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
          .setNameFormat("SnapshotHandlerChoreCleaner").setDaemon(true).build());
  private ScheduledFuture<?> snapshotHandlerChoreCleanerTask;

  // Restore map, with table name as key, procedure ID as value.
  // The map is always accessed and modified under the object lock using synchronized.
  // restoreSnapshot()/cloneSnapshot() will insert a procedure ID in the map.
  //
  // TODO: just as the Apache HBase 1.x implementation, this map would not survive master
  // restart/failover. This is just a stopgap implementation until implementation of taking
  // snapshot using Procedure-V2.
  private Map<TableName, Long> restoreTableToProcIdMap = new HashMap<>();

  // SnapshotDescription -> SnapshotProcId
  private final ConcurrentHashMap<SnapshotDescription, Long>
    snapshotToProcIdMap = new ConcurrentHashMap<>();

  private WorkerAssigner verifyWorkerAssigner;

  private Path rootDir;
  private ExecutorService executorService;

  /**
   * Read write lock between taking snapshot and snapshot HFile cleaner. The cleaner should skip to
   * check the HFiles if any snapshot is in progress, otherwise it may clean a HFile which would
   * belongs to the newly creating snapshot. So we should grab the write lock first when cleaner
   * start to work. (See HBASE-21387)
   */
  private ReentrantReadWriteLock takingSnapshotLock = new ReentrantReadWriteLock(true);

  public SnapshotManager() {}

  /**
   * Fully specify all necessary components of a snapshot manager. Exposed for testing.
   * @param master services for the master where the manager is running
   * @param coordinator procedure coordinator instance.  exposed for testing.
   * @param pool HBase ExecutorServcie instance, exposed for testing.
   */
  @InterfaceAudience.Private
  SnapshotManager(final MasterServices master, ProcedureCoordinator coordinator,
      ExecutorService pool, int sentinelCleanInterval)
      throws IOException, UnsupportedOperationException {
    this.master = master;

    this.rootDir = master.getMasterFileSystem().getRootDir();
    Configuration conf = master.getConfiguration();
    checkSnapshotSupport(conf, master.getMasterFileSystem());

    this.coordinator = coordinator;
    this.executorService = pool;
    resetTempDir();
    snapshotHandlerChoreCleanerTask = this.scheduleThreadPool.scheduleAtFixedRate(
      this::cleanupSentinels, sentinelCleanInterval, sentinelCleanInterval, TimeUnit.SECONDS);
  }

  /**
   * Gets the list of all completed snapshots.
   * @return list of SnapshotDescriptions
   * @throws IOException File system exception
   */
  public List<SnapshotDescription> getCompletedSnapshots() throws IOException {
    return getCompletedSnapshots(SnapshotDescriptionUtils.getSnapshotsDir(rootDir), true);
  }

  /**
   * Gets the list of all completed snapshots.
   * @param snapshotDir snapshot directory
   * @param withCpCall Whether to call CP hooks
   * @return list of SnapshotDescriptions
   * @throws IOException File system exception
   */
  private List<SnapshotDescription> getCompletedSnapshots(Path snapshotDir, boolean withCpCall)
      throws IOException {
    List<SnapshotDescription> snapshotDescs = new ArrayList<>();
    // first create the snapshot root path and check to see if it exists
    FileSystem fs = master.getMasterFileSystem().getFileSystem();
    if (snapshotDir == null) snapshotDir = SnapshotDescriptionUtils.getSnapshotsDir(rootDir);

    // if there are no snapshots, return an empty list
    if (!fs.exists(snapshotDir)) {
      return snapshotDescs;
    }

    // ignore all the snapshots in progress
    FileStatus[] snapshots = fs.listStatus(snapshotDir,
      new SnapshotDescriptionUtils.CompletedSnaphotDirectoriesFilter(fs));
    MasterCoprocessorHost cpHost = master.getMasterCoprocessorHost();
    withCpCall = withCpCall && cpHost != null;
    // loop through all the completed snapshots
    for (FileStatus snapshot : snapshots) {
      Path info = new Path(snapshot.getPath(), SnapshotDescriptionUtils.SNAPSHOTINFO_FILE);
      // if the snapshot is bad
      if (!fs.exists(info)) {
        LOG.error("Snapshot information for " + snapshot.getPath() + " doesn't exist");
        continue;
      }
      FSDataInputStream in = null;
      try {
        in = fs.open(info);
        SnapshotDescription desc = SnapshotDescription.parseFrom(in);
        org.apache.hadoop.hbase.client.SnapshotDescription descPOJO = (withCpCall)
            ? ProtobufUtil.createSnapshotDesc(desc) : null;
        if (withCpCall) {
          try {
            cpHost.preListSnapshot(descPOJO);
          } catch (AccessDeniedException e) {
            LOG.warn("Current user does not have access to " + desc.getName() + " snapshot. "
                + "Either you should be owner of this snapshot or admin user.");
            // Skip this and try for next snapshot
            continue;
          }
        }
        snapshotDescs.add(desc);

        // call coproc post hook
        if (withCpCall) {
          cpHost.postListSnapshot(descPOJO);
        }
      } catch (IOException e) {
        LOG.warn("Found a corrupted snapshot " + snapshot.getPath(), e);
      } finally {
        if (in != null) {
          in.close();
        }
      }
    }
    return snapshotDescs;
  }

  /**
   *  Cleans up any zk-coordinated snapshots in the snapshot/.tmp directory that were left from
   *  failed snapshot attempts. For unfinished procedure2-coordinated snapshots, keep the working
   *  directory.
   *
   * @throws IOException if we can't reach the filesystem
   */
  private void resetTempDir() throws IOException {
    Set<String> workingProcedureCoordinatedSnapshotNames =
      snapshotToProcIdMap.keySet().stream().map(s -> s.getName()).collect(Collectors.toSet());

    Path tmpdir = SnapshotDescriptionUtils.getWorkingSnapshotDir(rootDir,
      master.getConfiguration());
    FileSystem tmpFs = tmpdir.getFileSystem(master.getConfiguration());
    FileStatus[] workingSnapshotDirs = CommonFSUtils.listStatus(tmpFs, tmpdir);
    if (workingSnapshotDirs == null) {
      return;
    }
    for (FileStatus workingSnapshotDir : workingSnapshotDirs) {
      String workingSnapshotName = workingSnapshotDir.getPath().getName();
      if (!workingProcedureCoordinatedSnapshotNames.contains(workingSnapshotName)) {
        try {
          if (tmpFs.delete(workingSnapshotDir.getPath(), true)) {
            LOG.info("delete unfinished zk-coordinated snapshot working directory {}",
              workingSnapshotDir.getPath());
          } else {
            LOG.warn("Couldn't delete unfinished zk-coordinated snapshot working directory {}",
              workingSnapshotDir.getPath());
          }
        } catch (IOException e) {
          LOG.warn("Couldn't delete unfinished zk-coordinated snapshot working directory {}",
            workingSnapshotDir.getPath(), e);
        }
      } else {
        LOG.debug("find working directory of unfinished procedure {}", workingSnapshotName);
      }
    }
  }

  /**
   * Delete the specified snapshot
   * @param snapshot
   * @throws SnapshotDoesNotExistException If the specified snapshot does not exist.
   * @throws IOException For filesystem IOExceptions
   */
  public void deleteSnapshot(SnapshotDescription snapshot) throws IOException {
    // check to see if it is completed
    if (!isSnapshotCompleted(snapshot)) {
      throw new SnapshotDoesNotExistException(ProtobufUtil.createSnapshotDesc(snapshot));
    }

    String snapshotName = snapshot.getName();
    // first create the snapshot description and check to see if it exists
    FileSystem fs = master.getMasterFileSystem().getFileSystem();
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    // Get snapshot info from file system. The one passed as parameter is a "fake" snapshotInfo with
    // just the "name" and it does not contains the "real" snapshot information
    snapshot = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);

    // call coproc pre hook
    MasterCoprocessorHost cpHost = master.getMasterCoprocessorHost();
    org.apache.hadoop.hbase.client.SnapshotDescription snapshotPOJO = null;
    if (cpHost != null) {
      snapshotPOJO = ProtobufUtil.createSnapshotDesc(snapshot);
      cpHost.preDeleteSnapshot(snapshotPOJO);
    }

    LOG.debug("Deleting snapshot: " + snapshotName);
    // delete the existing snapshot
    if (!fs.delete(snapshotDir, true)) {
      throw new HBaseSnapshotException("Failed to delete snapshot directory: " + snapshotDir);
    }

    // call coproc post hook
    if (cpHost != null) {
      cpHost.postDeleteSnapshot(snapshotPOJO);
    }

  }

  /**
   * Check if the specified snapshot is done
   *
   * @param expected
   * @return true if snapshot is ready to be restored, false if it is still being taken.
   * @throws IOException IOException if error from HDFS or RPC
   * @throws UnknownSnapshotException if snapshot is invalid or does not exist.
   */
  public boolean isSnapshotDone(SnapshotDescription expected) throws IOException {
    // check the request to make sure it has a snapshot
    if (expected == null) {
      throw new UnknownSnapshotException(
         "No snapshot name passed in request, can't figure out which snapshot you want to check.");
    }

    Long procId = snapshotToProcIdMap.get(expected);
    if (procId != null) {
      if (master.getMasterProcedureExecutor().isRunning()) {
        return master.getMasterProcedureExecutor().isFinished(procId);
      } else {
        return false;
      }
    }

    String ssString = ClientSnapshotDescriptionUtils.toString(expected);

    // check to see if the sentinel exists,
    // and if the task is complete removes it from the in-progress snapshots map.
    SnapshotSentinel handler = removeSentinelIfFinished(this.snapshotHandlers, expected);

    // stop tracking "abandoned" handlers
    cleanupSentinels();

    if (handler == null) {
      // If there's no handler in the in-progress map, it means one of the following:
      //   - someone has already requested the snapshot state
      //   - the requested snapshot was completed long time ago (cleanupSentinels() timeout)
      //   - the snapshot was never requested
      // In those cases returns to the user the "done state" if the snapshots exists on disk,
      // otherwise raise an exception saying that the snapshot is not running and doesn't exist.
      if (!isSnapshotCompleted(expected)) {
        throw new UnknownSnapshotException("Snapshot " + ssString
            + " is not currently running or one of the known completed snapshots.");
      }
      // was done, return true;
      return true;
    }

    // pass on any failure we find in the sentinel
    try {
      handler.rethrowExceptionIfFailed();
    } catch (ForeignException e) {
      // Give some procedure info on an exception.
      String status;
      Procedure p = coordinator.getProcedure(expected.getName());
      if (p != null) {
        status = p.getStatus();
      } else {
        status = expected.getName() + " not found in proclist " + coordinator.getProcedureNames();
      }
      throw new HBaseSnapshotException("Snapshot " + ssString +  " had an error.  " + status, e,
        ProtobufUtil.createSnapshotDesc(expected));
    }

    // check to see if we are done
    if (handler.isFinished()) {
      LOG.debug("Snapshot '" + ssString + "' has completed, notifying client.");
      return true;
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Snapshoting '" + ssString + "' is still in progress!");
    }
    return false;
  }

  /**
   * Check to see if there is a snapshot in progress with the same name or on the same table.
   * Currently we have a limitation only allowing a single snapshot per table at a time. Also we
   * don't allow snapshot with the same name.
   * @param snapshot description of the snapshot being checked.
   * @param checkTable check if the table is already taking a snapshot.
   * @return <tt>true</tt> if there is a snapshot in progress with the same name or on the same
   *         table.
   */
  synchronized boolean isTakingSnapshot(final SnapshotDescription snapshot, boolean checkTable) {
    if (checkTable) {
      TableName snapshotTable = TableName.valueOf(snapshot.getTable());
      if (isTakingSnapshot(snapshotTable)) {
        return true;
      }
    }
    Iterator<Map.Entry<TableName, SnapshotSentinel>> it = snapshotHandlers.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<TableName, SnapshotSentinel> entry = it.next();
      SnapshotSentinel sentinel = entry.getValue();
      if (snapshot.getName().equals(sentinel.getSnapshot().getName()) && !sentinel.isFinished()) {
        return true;
      }
    }
    Iterator<Map.Entry<SnapshotDescription, Long>> spIt = snapshotToProcIdMap.entrySet().iterator();
    while (spIt.hasNext()) {
      Map.Entry<SnapshotDescription, Long> entry = spIt.next();
      if (snapshot.getName().equals(entry.getKey().getName())
        && !master.getMasterProcedureExecutor().isFinished(entry.getValue())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check to see if the specified table has a snapshot in progress.  Currently we have a
   * limitation only allowing a single snapshot per table at a time.
   * @param tableName name of the table being snapshotted.
   * @return <tt>true</tt> if there is a snapshot in progress on the specified table.
   */
  public boolean isTakingSnapshot(final TableName tableName) {
    return isTakingSnapshot(tableName, false);
  }

  public boolean isTableTakingAnySnapshot(final TableName tableName) {
    return isTakingSnapshot(tableName, true);
  }

  /**
   * Check to see if the specified table has a snapshot in progress. Since we introduce the
   * SnapshotProcedure, it is a little bit different from before. For zk-coordinated
   * snapshot, we can just consider tables in snapshotHandlers only, but for
   * {@link org.apache.hadoop.hbase.master.assignment.MergeTableRegionsProcedure} and
   * {@link org.apache.hadoop.hbase.master.assignment.SplitTableRegionProcedure}, we need
   * to consider tables in snapshotToProcIdMap also, for the snapshot procedure, we don't
   * need to check if table in snapshot.
   * @param tableName name of the table being snapshotted.
   * @param checkProcedure true if we should check tables in snapshotToProcIdMap
   * @return <tt>true</tt> if there is a snapshot in progress on the specified table.
   */
  private synchronized boolean isTakingSnapshot(TableName tableName, boolean checkProcedure) {
    SnapshotSentinel handler = this.snapshotHandlers.get(tableName);
    if (handler != null && !handler.isFinished()) {
      return true;
    }
    if (checkProcedure) {
      for (Map.Entry<SnapshotDescription, Long> entry : snapshotToProcIdMap.entrySet()) {
        if (TableName.valueOf(entry.getKey().getTable()).equals(tableName)
          && !master.getMasterProcedureExecutor().isFinished(entry.getValue())) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Check to make sure that we are OK to run the passed snapshot. Checks to make sure that we
   * aren't already running a snapshot or restore on the requested table.
   * @param snapshot description of the snapshot we want to start
   * @throws HBaseSnapshotException if the filesystem could not be prepared to start the snapshot
   */
  public synchronized void prepareWorkingDirectory(SnapshotDescription snapshot)
      throws HBaseSnapshotException {
    Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir,
        master.getConfiguration());

    try {
      FileSystem workingDirFS = workingDir.getFileSystem(master.getConfiguration());
      // delete the working directory, since we aren't running the snapshot. Likely leftovers
      // from a failed attempt.
      workingDirFS.delete(workingDir, true);

      // recreate the working directory for the snapshot
      if (!workingDirFS.mkdirs(workingDir)) {
        throw new SnapshotCreationException("Couldn't create working directory (" + workingDir
            + ") for snapshot" , ProtobufUtil.createSnapshotDesc(snapshot));
      }
    } catch (HBaseSnapshotException e) {
      throw e;
    } catch (IOException e) {
      throw new SnapshotCreationException(
          "Exception while checking to see if snapshot could be started.", e,
          ProtobufUtil.createSnapshotDesc(snapshot));
    }
  }

  /**
   * Take a snapshot of a disabled table.
   * @param snapshot description of the snapshot to take. Modified to be {@link Type#DISABLED}.
   * @throws IOException if the snapshot could not be started or filesystem for snapshot
   *         temporary directory could not be determined
   */
  private synchronized void snapshotDisabledTable(SnapshotDescription snapshot)
      throws IOException {
    // setup the snapshot
    prepareWorkingDirectory(snapshot);

    // set the snapshot to be a disabled snapshot, since the client doesn't know about that
    snapshot = snapshot.toBuilder().setType(Type.DISABLED).build();

    // Take the snapshot of the disabled table
    DisabledTableSnapshotHandler handler =
        new DisabledTableSnapshotHandler(snapshot, master, this);
    snapshotTable(snapshot, handler);
  }

  /**
   * Take a snapshot of an enabled table.
   * @param snapshot description of the snapshot to take.
   * @throws IOException if the snapshot could not be started or filesystem for snapshot
   *         temporary directory could not be determined
   */
  private synchronized void snapshotEnabledTable(SnapshotDescription snapshot)
          throws IOException {
    // setup the snapshot
    prepareWorkingDirectory(snapshot);

    // Take the snapshot of the enabled table
    EnabledTableSnapshotHandler handler =
        new EnabledTableSnapshotHandler(snapshot, master, this);
    snapshotTable(snapshot, handler);
  }

  /**
   * Take a snapshot using the specified handler.
   * On failure the snapshot temporary working directory is removed.
   * NOTE: prepareToTakeSnapshot() called before this one takes care of the rejecting the
   *       snapshot request if the table is busy with another snapshot/restore operation.
   * @param snapshot the snapshot description
   * @param handler the snapshot handler
   */
  private synchronized void snapshotTable(SnapshotDescription snapshot,
      final TakeSnapshotHandler handler) throws IOException {
    try {
      handler.prepare();
      this.executorService.submit(handler);
      this.snapshotHandlers.put(TableName.valueOf(snapshot.getTable()), handler);
    } catch (Exception e) {
      // cleanup the working directory by trying to delete it from the fs.
      Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir,
          master.getConfiguration());
      FileSystem workingDirFs = workingDir.getFileSystem(master.getConfiguration());
      try {
        if (!workingDirFs.delete(workingDir, true)) {
          LOG.error("Couldn't delete working directory (" + workingDir + " for snapshot:" +
              ClientSnapshotDescriptionUtils.toString(snapshot));
        }
      } catch (IOException e1) {
        LOG.error("Couldn't delete working directory (" + workingDir + " for snapshot:" +
            ClientSnapshotDescriptionUtils.toString(snapshot));
      }
      // fail the snapshot
      throw new SnapshotCreationException("Could not build snapshot handler", e,
        ProtobufUtil.createSnapshotDesc(snapshot));
    }
  }

  public ReadWriteLock getTakingSnapshotLock() {
    return this.takingSnapshotLock;
  }

  /**
   * The snapshot operation processing as following: <br>
   * 1. Create a Snapshot Handler, and do some initialization; <br>
   * 2. Put the handler into snapshotHandlers <br>
   * So when we consider if any snapshot is taking, we should consider both the takingSnapshotLock
   * and snapshotHandlers;
   * @return true to indicate that there're some running snapshots.
   */
  public synchronized boolean isTakingAnySnapshot() {
    return this.takingSnapshotLock.getReadHoldCount() > 0
      || this.snapshotHandlers.size() > 0
      || this.snapshotToProcIdMap.size() > 0;
  }

  /**
   * Take a snapshot based on the enabled/disabled state of the table.
   * @param snapshot
   * @throws HBaseSnapshotException when a snapshot specific exception occurs.
   * @throws IOException when some sort of generic IO exception occurs.
   */
  public void takeSnapshot(SnapshotDescription snapshot) throws IOException {
    this.takingSnapshotLock.readLock().lock();
    try {
      takeSnapshotInternal(snapshot);
    } finally {
      this.takingSnapshotLock.readLock().unlock();
    }
  }

  public synchronized long takeSnapshot(SnapshotDescription snapshot,
      long nonceGroup, long nonce) throws IOException {
    this.takingSnapshotLock.readLock().lock();
    try {
      return submitSnapshotProcedure(snapshot, nonceGroup, nonce);
    } finally {
      this.takingSnapshotLock.readLock().unlock();
    }
  }

  private long submitSnapshotProcedure(SnapshotDescription snapshot,
      long nonceGroup, long nonce) throws IOException {
    return MasterProcedureUtil.submitProcedure(
      new MasterProcedureUtil.NonceProcedureRunnable(master, nonceGroup, nonce) {
        @Override
        protected void run() throws IOException {
          sanityCheckBeforeSnapshot(snapshot, false);

          long procId = submitProcedure(new SnapshotProcedure(
            getMaster().getMasterProcedureExecutor().getEnvironment(), snapshot));

          getMaster().getSnapshotManager().registerSnapshotProcedure(snapshot, procId);
        }

        @Override
        protected String getDescription() {
          return "SnapshotProcedure";
        }
      });
  }

  private void takeSnapshotInternal(SnapshotDescription snapshot) throws IOException {
    TableDescriptor desc = sanityCheckBeforeSnapshot(snapshot, true);

    // call pre coproc hook
    MasterCoprocessorHost cpHost = master.getMasterCoprocessorHost();
    org.apache.hadoop.hbase.client.SnapshotDescription snapshotPOJO = null;
    if (cpHost != null) {
      snapshotPOJO = ProtobufUtil.createSnapshotDesc(snapshot);
      cpHost.preSnapshot(snapshotPOJO, desc, RpcServer.getRequestUser().orElse(null));
    }

    // if the table is enabled, then have the RS run actually the snapshot work
    TableName snapshotTable = TableName.valueOf(snapshot.getTable());
    if (master.getTableStateManager().isTableState(snapshotTable,
        TableState.State.ENABLED)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Table enabled, starting distributed snapshots for {}",
          ClientSnapshotDescriptionUtils.toString(snapshot));
      }
      snapshotEnabledTable(snapshot);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Started snapshot: {}", ClientSnapshotDescriptionUtils.toString(snapshot));
      }
    }
    // For disabled table, snapshot is created by the master
    else if (master.getTableStateManager().isTableState(snapshotTable,
        TableState.State.DISABLED)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Table is disabled, running snapshot entirely on master for {}",
          ClientSnapshotDescriptionUtils.toString(snapshot));
      }
      snapshotDisabledTable(snapshot);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Started snapshot: {}", ClientSnapshotDescriptionUtils.toString(snapshot));
      }
    } else {
      LOG.error("Can't snapshot table '" + snapshot.getTable()
          + "', isn't open or closed, we don't know what to do!");
      TablePartiallyOpenException tpoe = new TablePartiallyOpenException(snapshot.getTable()
          + " isn't fully open.");
      throw new SnapshotCreationException("Table is not entirely open or closed", tpoe,
        ProtobufUtil.createSnapshotDesc(snapshot));
    }

    // call post coproc hook
    if (cpHost != null) {
      cpHost.postSnapshot(snapshotPOJO, desc, RpcServer.getRequestUser().orElse(null));
    }
  }

  /**
   *  Check if the snapshot can be taken. Currently we have some limitations, for zk-coordinated
   *  snapshot, we don't allow snapshot with same name or taking multiple snapshots of a table at
   *  the same time, for procedure-coordinated snapshot, we don't allow snapshot with same name.
   * @param snapshot description of the snapshot being checked.
   * @param checkTable check if the table is already taking a snapshot. For zk-coordinated
   *                   snapshot, we need to check if another zk-coordinated snapshot is in
   *                   progress, for the snapshot procedure, this is unnecessary.
   * @return the table descriptor of the table
   */
  private synchronized TableDescriptor sanityCheckBeforeSnapshot(
      SnapshotDescription snapshot, boolean checkTable) throws IOException {
    // check to see if we already completed the snapshot
    if (isSnapshotCompleted(snapshot)) {
      throw new SnapshotExistsException("Snapshot '" + snapshot.getName() +
        "' already stored on the filesystem.", ProtobufUtil.createSnapshotDesc(snapshot));
    }
    LOG.debug("No existing snapshot, attempting snapshot...");

    // stop tracking "abandoned" handlers
    cleanupSentinels();

    TableName snapshotTable =
      TableName.valueOf(snapshot.getTable());
    // make sure we aren't already running a snapshot
    if (isTakingSnapshot(snapshot, checkTable)) {
      throw new SnapshotCreationException("Rejected taking "
        + ClientSnapshotDescriptionUtils.toString(snapshot)
        + " because we are already running another snapshot"
        + " on the same table or with the same name");
    }

    // make sure we aren't running a restore on the same table
    if (isRestoringTable(snapshotTable)) {
      throw new SnapshotCreationException("Rejected taking "
        + ClientSnapshotDescriptionUtils.toString(snapshot)
        + " because we are already have a restore in progress on the same snapshot.");
    }

    // check to see if the table exists
    TableDescriptor desc = null;
    try {
      desc = master.getTableDescriptors().get(TableName.valueOf(snapshot.getTable()));
    } catch (FileNotFoundException e) {
      String msg = "Table:" + snapshot.getTable() + " info doesn't exist!";
      LOG.error(msg);
      throw new SnapshotCreationException(msg, e, ProtobufUtil.createSnapshotDesc(snapshot));
    } catch (IOException e) {
      throw new SnapshotCreationException(
        "Error while geting table description for table " + snapshot.getTable(), e,
        ProtobufUtil.createSnapshotDesc(snapshot));
    }
    if (desc == null) {
      throw new SnapshotCreationException(
        "Table '" + snapshot.getTable() + "' doesn't exist, can't take snapshot.",
        ProtobufUtil.createSnapshotDesc(snapshot));
    }
    return desc;
  }

  /**
   * Set the handler for the current snapshot
   * <p>
   * Exposed for TESTING
   * @param tableName
   * @param handler handler the master should use
   *
   * TODO get rid of this if possible, repackaging, modify tests.
   */
  public synchronized void setSnapshotHandlerForTesting(
      final TableName tableName,
      final SnapshotSentinel handler) {
    if (handler != null) {
      this.snapshotHandlers.put(tableName, handler);
    } else {
      this.snapshotHandlers.remove(tableName);
    }
  }

  /**
   * @return distributed commit coordinator for all running snapshots
   */
  ProcedureCoordinator getCoordinator() {
    return coordinator;
  }

  /**
   * Check to see if the snapshot is one of the currently completed snapshots
   * Returns true if the snapshot exists in the "completed snapshots folder".
   *
   * @param snapshot expected snapshot to check
   * @return <tt>true</tt> if the snapshot is stored on the {@link FileSystem}, <tt>false</tt> if is
   *         not stored
   * @throws IOException if the filesystem throws an unexpected exception,
   * @throws IllegalArgumentException if snapshot name is invalid.
   */
  private boolean isSnapshotCompleted(SnapshotDescription snapshot) throws IOException {
    try {
      final Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
      FileSystem fs = master.getMasterFileSystem().getFileSystem();
      // check to see if the snapshot already exists
      return fs.exists(snapshotDir);
    } catch (IllegalArgumentException iae) {
      throw new UnknownSnapshotException("Unexpected exception thrown", iae);
    }
  }

  /**
   * Clone the specified snapshot.
   * The clone will fail if the destination table has a snapshot or restore in progress.
   *
   * @param reqSnapshot Snapshot Descriptor from request
   * @param tableName table to clone
   * @param snapshot Snapshot Descriptor
   * @param snapshotTableDesc Table Descriptor
   * @param nonceKey unique identifier to prevent duplicated RPC
   * @return procId the ID of the clone snapshot procedure
   * @throws IOException
   */
  private long cloneSnapshot(final SnapshotDescription reqSnapshot, final TableName tableName,
    final SnapshotDescription snapshot, final TableDescriptor snapshotTableDesc,
    final NonceKey nonceKey, final boolean restoreAcl, final String customSFT) throws IOException {
    MasterCoprocessorHost cpHost = master.getMasterCoprocessorHost();
    TableDescriptor htd = TableDescriptorBuilder.copy(tableName, snapshotTableDesc);
    org.apache.hadoop.hbase.client.SnapshotDescription snapshotPOJO = null;
    if (cpHost != null) {
      snapshotPOJO = ProtobufUtil.createSnapshotDesc(snapshot);
      cpHost.preCloneSnapshot(snapshotPOJO, htd);
    }
    long procId;
    try {
      procId = cloneSnapshot(snapshot, htd, nonceKey, restoreAcl, customSFT);
    } catch (IOException e) {
      LOG.error("Exception occurred while cloning the snapshot " + snapshot.getName()
        + " as table " + tableName.getNameAsString(), e);
      throw e;
    }
    LOG.info("Clone snapshot=" + snapshot.getName() + " as table=" + tableName);

    if (cpHost != null) {
      cpHost.postCloneSnapshot(snapshotPOJO, htd);
    }
    return procId;
  }

  /**
   * Clone the specified snapshot into a new table.
   * The operation will fail if the destination table has a snapshot or restore in progress.
   *
   * @param snapshot Snapshot Descriptor
   * @param tableDescriptor Table Descriptor of the table to create
   * @param nonceKey unique identifier to prevent duplicated RPC
   * @return procId the ID of the clone snapshot procedure
   */
  synchronized long cloneSnapshot(final SnapshotDescription snapshot,
    final TableDescriptor tableDescriptor, final NonceKey nonceKey, final boolean restoreAcl,
    final String customSFT)
      throws HBaseSnapshotException {
    TableName tableName = tableDescriptor.getTableName();

    // make sure we aren't running a snapshot on the same table
    if (isTableTakingAnySnapshot(tableName)) {
      throw new RestoreSnapshotException("Snapshot in progress on the restore table=" + tableName);
    }

    // make sure we aren't running a restore on the same table
    if (isRestoringTable(tableName)) {
      throw new RestoreSnapshotException("Restore already in progress on the table=" + tableName);
    }

    try {
      long procId = master.getMasterProcedureExecutor().submitProcedure(
        new CloneSnapshotProcedure(master.getMasterProcedureExecutor().getEnvironment(),
                tableDescriptor, snapshot, restoreAcl, customSFT),
        nonceKey);
      this.restoreTableToProcIdMap.put(tableName, procId);
      return procId;
    } catch (Exception e) {
      String msg = "Couldn't clone the snapshot="
        + ClientSnapshotDescriptionUtils.toString(snapshot) + " on table=" + tableName;
      LOG.error(msg, e);
      throw new RestoreSnapshotException(msg, e);
    }
  }

  /**
   * Restore or Clone the specified snapshot
   * @param reqSnapshot
   * @param nonceKey unique identifier to prevent duplicated RPC
   * @throws IOException
   */
  public long restoreOrCloneSnapshot(final SnapshotDescription reqSnapshot, final NonceKey nonceKey,
      final boolean restoreAcl, String customSFT) throws IOException {
    FileSystem fs = master.getMasterFileSystem().getFileSystem();
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(reqSnapshot, rootDir);

    // check if the snapshot exists
    if (!fs.exists(snapshotDir)) {
      LOG.error("A Snapshot named '" + reqSnapshot.getName() + "' does not exist.");
      throw new SnapshotDoesNotExistException(
        ProtobufUtil.createSnapshotDesc(reqSnapshot));
    }

    // Get snapshot info from file system. The reqSnapshot is a "fake" snapshotInfo with
    // just the snapshot "name" and table name to restore. It does not contains the "real" snapshot
    // information.
    SnapshotDescription snapshot = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    SnapshotManifest manifest = SnapshotManifest.open(master.getConfiguration(), fs,
        snapshotDir, snapshot);
    TableDescriptor snapshotTableDesc = manifest.getTableDescriptor();
    TableName tableName = TableName.valueOf(reqSnapshot.getTable());

    // sanity check the new table descriptor
    TableDescriptorChecker.sanityCheck(master.getConfiguration(), snapshotTableDesc);

    // stop tracking "abandoned" handlers
    cleanupSentinels();

    // Verify snapshot validity
    SnapshotReferenceUtil.verifySnapshot(master.getConfiguration(), fs, manifest);

    // Execute the restore/clone operation
    long procId;
    if (master.getTableDescriptors().exists(tableName)) {
      procId =
        restoreSnapshot(reqSnapshot, tableName, snapshot, snapshotTableDesc, nonceKey, restoreAcl);
    } else {
      procId =
        cloneSnapshot(reqSnapshot, tableName, snapshot, snapshotTableDesc, nonceKey, restoreAcl,
          customSFT);
    }
    return procId;
  }

  /**
   * Restore the specified snapshot. The restore will fail if the destination table has a snapshot
   * or restore in progress.
   * @param reqSnapshot Snapshot Descriptor from request
   * @param tableName table to restore
   * @param snapshot Snapshot Descriptor
   * @param snapshotTableDesc Table Descriptor
   * @param nonceKey unique identifier to prevent duplicated RPC
   * @param restoreAcl true to restore acl of snapshot
   * @return procId the ID of the restore snapshot procedure
   * @throws IOException
   */
  private long restoreSnapshot(final SnapshotDescription reqSnapshot, final TableName tableName,
      final SnapshotDescription snapshot, final TableDescriptor snapshotTableDesc,
      final NonceKey nonceKey, final boolean restoreAcl) throws IOException {
    MasterCoprocessorHost cpHost = master.getMasterCoprocessorHost();

    //have to check first if restoring the snapshot would break current SFT setup
    StoreFileTrackerValidationUtils.validatePreRestoreSnapshot(
      master.getTableDescriptors().get(tableName), snapshotTableDesc, master.getConfiguration());

    if (master.getTableStateManager().isTableState(
      TableName.valueOf(snapshot.getTable()), TableState.State.ENABLED)) {
      throw new UnsupportedOperationException("Table '" +
        TableName.valueOf(snapshot.getTable()) + "' must be disabled in order to " +
        "perform a restore operation.");
    }

    // call Coprocessor pre hook
    org.apache.hadoop.hbase.client.SnapshotDescription snapshotPOJO = null;
    if (cpHost != null) {
      snapshotPOJO = ProtobufUtil.createSnapshotDesc(snapshot);
      cpHost.preRestoreSnapshot(snapshotPOJO, snapshotTableDesc);
    }

    long procId;
    try {
      procId = restoreSnapshot(snapshot, snapshotTableDesc, nonceKey, restoreAcl);
    } catch (IOException e) {
      LOG.error("Exception occurred while restoring the snapshot " + snapshot.getName()
        + " as table " + tableName.getNameAsString(), e);
      throw e;
    }
    LOG.info("Restore snapshot=" + snapshot.getName() + " as table=" + tableName);

    if (cpHost != null) {
      cpHost.postRestoreSnapshot(snapshotPOJO, snapshotTableDesc);
    }

    return procId;
  }

  /**
   * Restore the specified snapshot. The restore will fail if the destination table has a snapshot
   * or restore in progress.
   * @param snapshot Snapshot Descriptor
   * @param tableDescriptor Table Descriptor
   * @param nonceKey unique identifier to prevent duplicated RPC
   * @param restoreAcl true to restore acl of snapshot
   * @return procId the ID of the restore snapshot procedure
   */
  private synchronized long restoreSnapshot(final SnapshotDescription snapshot,
    final TableDescriptor tableDescriptor, final NonceKey nonceKey, final boolean restoreAcl)
      throws HBaseSnapshotException {
    final TableName tableName = tableDescriptor.getTableName();

    // make sure we aren't running a snapshot on the same table
    if (isTableTakingAnySnapshot(tableName)) {
      throw new RestoreSnapshotException("Snapshot in progress on the restore table=" + tableName);
    }

    // make sure we aren't running a restore on the same table
    if (isRestoringTable(tableName)) {
      throw new RestoreSnapshotException("Restore already in progress on the table=" + tableName);
    }

    try {
      long procId = master.getMasterProcedureExecutor().submitProcedure(
        new RestoreSnapshotProcedure(master.getMasterProcedureExecutor().getEnvironment(),
                tableDescriptor, snapshot, restoreAcl),
        nonceKey);
      this.restoreTableToProcIdMap.put(tableName, procId);
      return procId;
    } catch (Exception e) {
      String msg = "Couldn't restore the snapshot=" + ClientSnapshotDescriptionUtils.toString(
          snapshot)  +
          " on table=" + tableName;
      LOG.error(msg, e);
      throw new RestoreSnapshotException(msg, e);
    }
  }

  /**
   * Verify if the restore of the specified table is in progress.
   *
   * @param tableName table under restore
   * @return <tt>true</tt> if there is a restore in progress of the specified table.
   */
  private synchronized boolean isRestoringTable(final TableName tableName) {
    Long procId = this.restoreTableToProcIdMap.get(tableName);
    if (procId == null) {
      return false;
    }
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    if (procExec.isRunning() && !procExec.isFinished(procId)) {
      return true;
    } else {
      this.restoreTableToProcIdMap.remove(tableName);
      return false;
    }
  }

  /**
   * Return the handler if it is currently live and has the same snapshot target name.
   * The handler is removed from the sentinels map if completed.
   * @param sentinels live handlers
   * @param snapshot snapshot description
   * @return null if doesn't match, else a live handler.
   */
  private synchronized SnapshotSentinel removeSentinelIfFinished(
      final Map<TableName, SnapshotSentinel> sentinels,
      final SnapshotDescription snapshot) {
    if (!snapshot.hasTable()) {
      return null;
    }

    TableName snapshotTable = TableName.valueOf(snapshot.getTable());
    SnapshotSentinel h = sentinels.get(snapshotTable);
    if (h == null) {
      return null;
    }

    if (!h.getSnapshot().getName().equals(snapshot.getName())) {
      // specified snapshot is to the one currently running
      return null;
    }

    // Remove from the "in-progress" list once completed
    if (h.isFinished()) {
      sentinels.remove(snapshotTable);
    }

    return h;
  }

  /**
   * Removes "abandoned" snapshot/restore requests.
   * As part of the HBaseAdmin snapshot/restore API the operation status is checked until completed,
   * and the in-progress maps are cleaned up when the status of a completed task is requested.
   * To avoid having sentinels staying around for long time if something client side is failed,
   * each operation tries to clean up the in-progress maps sentinels finished from a long time.
   */
  private void cleanupSentinels() {
    cleanupSentinels(this.snapshotHandlers);
    cleanupCompletedRestoreInMap();
    cleanupCompletedSnapshotInMap();
  }

  /**
   * Remove the sentinels that are marked as finished and the completion time
   * has exceeded the removal timeout.
   * @param sentinels map of sentinels to clean
   */
  private synchronized void cleanupSentinels(final Map<TableName, SnapshotSentinel> sentinels) {
    long currentTime = EnvironmentEdgeManager.currentTime();
    long sentinelsCleanupTimeoutMillis =
        master.getConfiguration().getLong(HBASE_SNAPSHOT_SENTINELS_CLEANUP_TIMEOUT_MILLIS,
          SNAPSHOT_SENTINELS_CLEANUP_TIMEOUT_MILLS_DEFAULT);
    Iterator<Map.Entry<TableName, SnapshotSentinel>> it = sentinels.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<TableName, SnapshotSentinel> entry = it.next();
      SnapshotSentinel sentinel = entry.getValue();
      if (sentinel.isFinished()
          && (currentTime - sentinel.getCompletionTimestamp()) > sentinelsCleanupTimeoutMillis) {
        it.remove();
      }
    }
  }

  /**
   * Remove the procedures that are marked as finished
   */
  private synchronized void cleanupCompletedRestoreInMap() {
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    Iterator<Map.Entry<TableName, Long>> it = restoreTableToProcIdMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<TableName, Long> entry = it.next();
      Long procId = entry.getValue();
      if (procExec.isRunning() && procExec.isFinished(procId)) {
        it.remove();
      }
    }
  }

  /**
   * Remove the procedures that are marked as finished
   */
  private synchronized void cleanupCompletedSnapshotInMap() {
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    Iterator<Map.Entry<SnapshotDescription, Long>> it = snapshotToProcIdMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<SnapshotDescription, Long> entry = it.next();
      Long procId = entry.getValue();
      if (procExec.isRunning() && procExec.isFinished(procId)) {
        it.remove();
      }
    }
  }


  //
  // Implementing Stoppable interface
  //

  @Override
  public void stop(String why) {
    // short circuit
    if (this.stopped) return;
    // make sure we get stop
    this.stopped = true;
    // pass the stop onto take snapshot handlers
    for (SnapshotSentinel snapshotHandler: this.snapshotHandlers.values()) {
      snapshotHandler.cancel(why);
    }
    if (snapshotHandlerChoreCleanerTask != null) {
      snapshotHandlerChoreCleanerTask.cancel(true);
    }
    try {
      if (coordinator != null) {
        coordinator.close();
      }
    } catch (IOException e) {
      LOG.error("stop ProcedureCoordinator error", e);
    }
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  /**
   * Throws an exception if snapshot operations (take a snapshot, restore, clone) are not supported.
   * Called at the beginning of snapshot() and restoreSnapshot() methods.
   * @throws UnsupportedOperationException if snapshot are not supported
   */
  public void checkSnapshotSupport() throws UnsupportedOperationException {
    if (!this.isSnapshotSupported) {
      throw new UnsupportedOperationException(
        "To use snapshots, You must add to the hbase-site.xml of the HBase Master: '" +
          HBASE_SNAPSHOT_ENABLED + "' property with value 'true'.");
    }
  }

  /**
   * Called at startup, to verify if snapshot operation is supported, and to avoid
   * starting the master if there're snapshots present but the cleaners needed are missing.
   * Otherwise we can end up with snapshot data loss.
   * @param conf The {@link Configuration} object to use
   * @param mfs The MasterFileSystem to use
   * @throws IOException in case of file-system operation failure
   * @throws UnsupportedOperationException in case cleaners are missing and
   *         there're snapshot in the system
   */
  private void checkSnapshotSupport(final Configuration conf, final MasterFileSystem mfs)
      throws IOException, UnsupportedOperationException {
    // Verify if snapshot is disabled by the user
    String enabled = conf.get(HBASE_SNAPSHOT_ENABLED);
    boolean snapshotEnabled = conf.getBoolean(HBASE_SNAPSHOT_ENABLED, false);
    boolean userDisabled = (enabled != null && enabled.trim().length() > 0 && !snapshotEnabled);

    // Extract cleaners from conf
    Set<String> hfileCleaners = new HashSet<>();
    String[] cleaners = conf.getStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS);
    if (cleaners != null) Collections.addAll(hfileCleaners, cleaners);

    Set<String> logCleaners = new HashSet<>();
    cleaners = conf.getStrings(HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS);
    if (cleaners != null) Collections.addAll(logCleaners, cleaners);

    // check if an older version of snapshot directory was present
    Path oldSnapshotDir = new Path(mfs.getRootDir(), HConstants.OLD_SNAPSHOT_DIR_NAME);
    FileSystem fs = mfs.getFileSystem();
    List<SnapshotDescription> ss = getCompletedSnapshots(new Path(rootDir, oldSnapshotDir), false);
    if (ss != null && !ss.isEmpty()) {
      LOG.error("Snapshots from an earlier release were found under: " + oldSnapshotDir);
      LOG.error("Please rename the directory as " + HConstants.SNAPSHOT_DIR_NAME);
    }

    // If the user has enabled the snapshot, we force the cleaners to be present
    // otherwise we still need to check if cleaners are enabled or not and verify
    // that there're no snapshot in the .snapshot folder.
    if (snapshotEnabled) {
      // Inject snapshot cleaners, if snapshot.enable is true
      hfileCleaners.add(SnapshotHFileCleaner.class.getName());
      hfileCleaners.add(HFileLinkCleaner.class.getName());
      // If sync acl to HDFS feature is enabled, then inject the cleaner
      if (SnapshotScannerHDFSAclHelper.isAclSyncToHdfsEnabled(conf)) {
        hfileCleaners.add(SnapshotScannerHDFSAclCleaner.class.getName());
      }

      // Set cleaners conf
      conf.setStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS,
        hfileCleaners.toArray(new String[hfileCleaners.size()]));
      conf.setStrings(HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS,
        logCleaners.toArray(new String[logCleaners.size()]));
    } else {
      // There may be restore tables if snapshot is enabled and then disabled, so add
      // HFileLinkCleaner, see HBASE-26670 for more details.
      hfileCleaners.add(HFileLinkCleaner.class.getName());
      conf.setStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS,
        hfileCleaners.toArray(new String[hfileCleaners.size()]));
      // Verify if SnapshotHFileCleaner are present
      snapshotEnabled = hfileCleaners.contains(SnapshotHFileCleaner.class.getName());

      // Warn if the cleaners are enabled but the snapshot.enabled property is false/not set.
      if (snapshotEnabled) {
        LOG.warn("Snapshot log and hfile cleaners are present in the configuration, " +
          "but the '" + HBASE_SNAPSHOT_ENABLED + "' property " +
          (userDisabled ? "is set to 'false'." : "is not set."));
      }
    }

    // Mark snapshot feature as enabled if cleaners are present and user has not disabled it.
    this.isSnapshotSupported = snapshotEnabled && !userDisabled;

    // If cleaners are not enabled, verify that there're no snapshot in the .snapshot folder
    // otherwise we end up with snapshot data loss.
    if (!snapshotEnabled) {
      LOG.info("Snapshot feature is not enabled, missing log and hfile cleaners.");
      Path snapshotDir = SnapshotDescriptionUtils.getSnapshotsDir(mfs.getRootDir());
      if (fs.exists(snapshotDir)) {
        FileStatus[] snapshots = CommonFSUtils.listStatus(fs, snapshotDir,
          new SnapshotDescriptionUtils.CompletedSnaphotDirectoriesFilter(fs));
        if (snapshots != null) {
          LOG.error("Snapshots are present, but cleaners are not enabled.");
          checkSnapshotSupport();
        }
      }
    }
  }

  @Override
  public void initialize(MasterServices master, MetricsMaster metricsMaster) throws KeeperException,
      IOException, UnsupportedOperationException {
    this.master = master;

    this.rootDir = master.getMasterFileSystem().getRootDir();
    checkSnapshotSupport(master.getConfiguration(), master.getMasterFileSystem());

    // get the configuration for the coordinator
    Configuration conf = master.getConfiguration();
    long wakeFrequency = conf.getInt(SNAPSHOT_WAKE_MILLIS_KEY, SNAPSHOT_WAKE_MILLIS_DEFAULT);
    long timeoutMillis = Math.max(
            conf.getLong(SnapshotDescriptionUtils.MASTER_SNAPSHOT_TIMEOUT_MILLIS,
                    SnapshotDescriptionUtils.DEFAULT_MAX_WAIT_TIME),
            conf.getLong(SnapshotDescriptionUtils.MASTER_SNAPSHOT_TIMEOUT_MILLIS,
                    SnapshotDescriptionUtils.DEFAULT_MAX_WAIT_TIME));
    int opThreads = conf.getInt(SNAPSHOT_POOL_THREADS_KEY, SNAPSHOT_POOL_THREADS_DEFAULT);

    // setup the default procedure coordinator
    String name = master.getServerName().toString();
    ThreadPoolExecutor tpool = ProcedureCoordinator.defaultPool(name, opThreads);
    ProcedureCoordinatorRpcs comms = new ZKProcedureCoordinator(
        master.getZooKeeper(), SnapshotManager.ONLINE_SNAPSHOT_CONTROLLER_DESCRIPTION, name);

    this.coordinator = new ProcedureCoordinator(comms, tpool, timeoutMillis, wakeFrequency);
    this.executorService = master.getExecutorService();
    this.verifyWorkerAssigner = new WorkerAssigner(master,
      conf.getInt("hbase.snapshot.verify.task.max", 3),
      new ProcedureEvent<>("snapshot-verify-worker-assigning"));
    restoreUnfinishedSnapshotProcedure();
    restoreWorkers();
    resetTempDir();
    snapshotHandlerChoreCleanerTask =
        scheduleThreadPool.scheduleAtFixedRate(this::cleanupSentinels, 10, 10, TimeUnit.SECONDS);
  }

  private void restoreUnfinishedSnapshotProcedure() {
    master.getMasterProcedureExecutor()
      .getActiveProceduresNoCopy()
      .stream().filter(p -> p instanceof SnapshotProcedure)
      .filter(p -> !p.isFinished()).map(p -> (SnapshotProcedure) p)
      .forEach(p -> {
        registerSnapshotProcedure(p.getSnapshot(), p.getProcId());
        LOG.info("restore unfinished snapshot procedure {}", p);
      });
  }

  @Override
  public String getProcedureSignature() {
    return ONLINE_SNAPSHOT_CONTROLLER_DESCRIPTION;
  }

  @Override
  public void execProcedure(ProcedureDescription desc) throws IOException {
    takeSnapshot(toSnapshotDescription(desc));
  }

  @Override
  public void checkPermissions(ProcedureDescription desc, AccessChecker accessChecker, User user)
      throws IOException {
    // Done by AccessController as part of preSnapshot coprocessor hook (legacy code path).
    // In future, when we AC is removed for good, that check should be moved here.
  }

  @Override
  public boolean isProcedureDone(ProcedureDescription desc) throws IOException {
    return isSnapshotDone(toSnapshotDescription(desc));
  }

  private SnapshotDescription toSnapshotDescription(ProcedureDescription desc)
      throws IOException {
    SnapshotDescription.Builder builder = SnapshotDescription.newBuilder();
    if (!desc.hasInstance()) {
      throw new IOException("Snapshot name is not defined: " + desc.toString());
    }
    String snapshotName = desc.getInstance();
    List<NameStringPair> props = desc.getConfigurationList();
    String table = null;
    for (NameStringPair prop : props) {
      if ("table".equalsIgnoreCase(prop.getName())) {
        table = prop.getValue();
      }
    }
    if (table == null) {
      throw new IOException("Snapshot table is not defined: " + desc.toString());
    }
    TableName tableName = TableName.valueOf(table);
    builder.setTable(tableName.getNameAsString());
    builder.setName(snapshotName);
    builder.setType(SnapshotDescription.Type.FLUSH);
    return builder.build();
  }

  public void registerSnapshotProcedure(SnapshotDescription snapshot, long procId) {
    snapshotToProcIdMap.put(snapshot, procId);
    LOG.debug("register snapshot={}, snapshot procedure id = {}",
      ClientSnapshotDescriptionUtils.toString(snapshot), procId);
  }

  public void unregisterSnapshotProcedure(SnapshotDescription snapshot, long procId) {
    snapshotToProcIdMap.remove(snapshot, procId);
    LOG.debug("unregister snapshot={}, snapshot procedure id = {}",
      ClientSnapshotDescriptionUtils.toString(snapshot), procId);
  }

  public boolean snapshotProcedureEnabled() {
    return master.getConfiguration()
      .getBoolean(SNAPSHOT_PROCEDURE_ENABLED, SNAPSHOT_PROCEDURE_ENABLED_DEFAULT);
  }

  public ServerName acquireSnapshotVerifyWorker(SnapshotVerifyProcedure procedure)
      throws ProcedureSuspendedException {
    Optional<ServerName> worker = verifyWorkerAssigner.acquire();
    if (worker.isPresent()) {
      LOG.debug("{} Acquired verify snapshot worker={}", procedure, worker.get());
      return worker.get();
    }
    verifyWorkerAssigner.suspend(procedure);
    throw new ProcedureSuspendedException();
  }

  public void releaseSnapshotVerifyWorker(SnapshotVerifyProcedure procedure,
      ServerName worker, MasterProcedureScheduler scheduler) {
    LOG.debug("{} Release verify snapshot worker={}", procedure, worker);
    verifyWorkerAssigner.release(worker);
    verifyWorkerAssigner.wake(scheduler);
  }

  private void restoreWorkers() {
    master.getMasterProcedureExecutor().getActiveProceduresNoCopy().stream()
      .filter(p -> p instanceof SnapshotVerifyProcedure)
      .map(p -> (SnapshotVerifyProcedure) p)
      .filter(p -> !p.isFinished())
      .filter(p -> p.getServerName() != null)
      .forEach(p -> {
        verifyWorkerAssigner.addUsedWorker(p.getServerName());
        LOG.debug("{} restores used worker {}", p, p.getServerName());
      });
  }

  public Integer getAvailableWorker(ServerName serverName) {
    return verifyWorkerAssigner.getAvailableWorker(serverName);
  }
}
