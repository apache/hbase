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
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MetricsMaster;
import org.apache.hadoop.hbase.master.SnapshotSentinel;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.master.cleaner.HFileLinkCleaner;
import org.apache.hadoop.hbase.procedure.MasterProcedureManager;
import org.apache.hadoop.hbase.procedure.Procedure;
import org.apache.hadoop.hbase.procedure.ProcedureCoordinator;
import org.apache.hadoop.hbase.procedure.ProcedureCoordinatorRpcs;
import org.apache.hadoop.hbase.procedure.ZKProcedureCoordinatorRpcs;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.quotas.QuotaExceededException;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
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
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.zookeeper.KeeperException;

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
  private static final Log LOG = LogFactory.getLog(SnapshotManager.class);

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
  private static final int SNAPSHOT_SENTINELS_CLEANUP_TIMEOUT = 60 * 1000;

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
  private static final String SNAPSHOT_POOL_THREADS_KEY = "hbase.snapshot.master.threads";

  /** number of current operations running on the master */
  private static final int SNAPSHOT_POOL_THREADS_DEFAULT = 1;

  private boolean stopped;
  private MasterServices master;  // Needed by TableEventHandlers
  private ProcedureCoordinator coordinator;

  // Is snapshot feature enabled?
  private boolean isSnapshotSupported = false;

  // Snapshot handlers map, with table name as key.
  // The map is always accessed and modified under the object lock using synchronized.
  // snapshotTable() will insert an Handler in the table.
  // isSnapshotDone() will remove the handler requested if the operation is finished.
  private Map<TableName, SnapshotSentinel> snapshotHandlers =
      new HashMap<TableName, SnapshotSentinel>();

  // Restore Sentinels map, with table name as key.
  // The map is always accessed and modified under the object lock using synchronized.
  // restoreSnapshot()/cloneSnapshot() will insert an Handler in the table.
  // isRestoreDone() will remove the handler requested if the operation is finished.
  private Map<TableName, SnapshotSentinel> restoreHandlers =
      new HashMap<TableName, SnapshotSentinel>();

  private Path rootDir;
  private ExecutorService executorService;

  public SnapshotManager() {}

  /**
   * Fully specify all necessary components of a snapshot manager. Exposed for testing.
   * @param master services for the master where the manager is running
   * @param coordinator procedure coordinator instance.  exposed for testing.
   * @param pool HBase ExecutorServcie instance, exposed for testing.
   */
  public SnapshotManager(final MasterServices master, final MetricsMaster metricsMaster,
      ProcedureCoordinator coordinator, ExecutorService pool)
      throws IOException, UnsupportedOperationException {
    this.master = master;

    this.rootDir = master.getMasterFileSystem().getRootDir();
    checkSnapshotSupport(master.getConfiguration(), master.getMasterFileSystem());

    this.coordinator = coordinator;
    this.executorService = pool;
    resetTempDir();
  }

  /**
   * Gets the list of all completed snapshots.
   * @return list of SnapshotDescriptions
   * @throws IOException File system exception
   */
  public List<SnapshotDescription> getCompletedSnapshots() throws IOException {
    return getCompletedSnapshots(SnapshotDescriptionUtils.getSnapshotsDir(rootDir));
  }

  /**
   * Gets the list of all completed snapshots.
   * @param snapshotDir snapshot directory
   * @return list of SnapshotDescriptions
   * @throws IOException File system exception
   */
  private List<SnapshotDescription> getCompletedSnapshots(Path snapshotDir) throws IOException {
    List<SnapshotDescription> snapshotDescs = new ArrayList<SnapshotDescription>();
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
        if (cpHost != null) {
          try {
            cpHost.preListSnapshot(desc);
          } catch (AccessDeniedException e) {
            LOG.warn("Current user does not have access to " + desc.getName() + " snapshot. "
                + "Either you should be owner of this snapshot or admin user.");
            // Skip this and try for next snapshot
            continue;
          }
        }
        snapshotDescs.add(desc);

        // call coproc post hook
        if (cpHost != null) {
          cpHost.postListSnapshot(desc);
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
   * Cleans up any snapshots in the snapshot/.tmp directory that were left from failed
   * snapshot attempts.
   *
   * @throws IOException if we can't reach the filesystem
   */
  void resetTempDir() throws IOException {
    // cleanup any existing snapshots.
    Path tmpdir = SnapshotDescriptionUtils.getWorkingSnapshotDir(rootDir);
    if (master.getMasterFileSystem().getFileSystem().exists(tmpdir)) {
      if (!master.getMasterFileSystem().getFileSystem().delete(tmpdir, true)) {
        LOG.warn("Couldn't delete working snapshot directory: " + tmpdir);
      }
    }
  }

  /**
   * Delete the specified snapshot
   * @param snapshot
   * @throws SnapshotDoesNotExistException If the specified snapshot does not exist.
   * @throws IOException For filesystem IOExceptions
   */
  public void deleteSnapshot(SnapshotDescription snapshot) throws SnapshotDoesNotExistException, IOException {
    // check to see if it is completed
    if (!isSnapshotCompleted(snapshot)) {
      throw new SnapshotDoesNotExistException(snapshot);
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
    if (cpHost != null) {
      cpHost.preDeleteSnapshot(snapshot);
    }

    LOG.debug("Deleting snapshot: " + snapshotName);
    // delete the existing snapshot
    if (!fs.delete(snapshotDir, true)) {
      throw new HBaseSnapshotException("Failed to delete snapshot directory: " + snapshotDir);
    }

    // call coproc post hook
    if (cpHost != null) {
      cpHost.postDeleteSnapshot(snapshot);
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
          expected);
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
   * @return <tt>true</tt> if there is a snapshot in progress with the same name or on the same
   *         table.
   */
  synchronized boolean isTakingSnapshot(final SnapshotDescription snapshot) {
    TableName snapshotTable = TableName.valueOf(snapshot.getTable());
    if (isTakingSnapshot(snapshotTable)) {
      return true;
    }
    Iterator<Map.Entry<TableName, SnapshotSentinel>> it = this.snapshotHandlers.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<TableName, SnapshotSentinel> entry = it.next();
      SnapshotSentinel sentinel = entry.getValue();
      if (snapshot.getName().equals(sentinel.getSnapshot().getName()) && !sentinel.isFinished()) {
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
  synchronized boolean isTakingSnapshot(final TableName tableName) {
    SnapshotSentinel handler = this.snapshotHandlers.get(tableName);
    return handler != null && !handler.isFinished();
  }

  /**
   * Check to make sure that we are OK to run the passed snapshot. Checks to make sure that we
   * aren't already running a snapshot or restore on the requested table.
   * @param snapshot description of the snapshot we want to start
   * @throws HBaseSnapshotException if the filesystem could not be prepared to start the snapshot
   */
  private synchronized void prepareToTakeSnapshot(SnapshotDescription snapshot)
      throws HBaseSnapshotException {
    FileSystem fs = master.getMasterFileSystem().getFileSystem();
    Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir);
    TableName snapshotTable =
        TableName.valueOf(snapshot.getTable());

    // make sure we aren't already running a snapshot
    if (isTakingSnapshot(snapshot)) {
      SnapshotSentinel handler = this.snapshotHandlers.get(snapshotTable);
      throw new SnapshotCreationException("Rejected taking "
          + ClientSnapshotDescriptionUtils.toString(snapshot)
          + " because we are already running another snapshot "
          + (handler != null ? ("on the same table " +
              ClientSnapshotDescriptionUtils.toString(handler.getSnapshot()))
              : "with the same name"), snapshot);
    }

    // make sure we aren't running a restore on the same table
    if (isRestoringTable(snapshotTable)) {
      SnapshotSentinel handler = restoreHandlers.get(snapshotTable);
      throw new SnapshotCreationException("Rejected taking "
          + ClientSnapshotDescriptionUtils.toString(snapshot)
          + " because we are already have a restore in progress on the same snapshot "
          + ClientSnapshotDescriptionUtils.toString(handler.getSnapshot()), snapshot);
    }

    try {
      // delete the working directory, since we aren't running the snapshot. Likely leftovers
      // from a failed attempt.
      fs.delete(workingDir, true);

      // recreate the working directory for the snapshot
      if (!fs.mkdirs(workingDir)) {
        throw new SnapshotCreationException("Couldn't create working directory (" + workingDir
            + ") for snapshot" , snapshot);
      }
    } catch (HBaseSnapshotException e) {
      throw e;
    } catch (IOException e) {
      throw new SnapshotCreationException(
          "Exception while checking to see if snapshot could be started.", e, snapshot);
    }
  }

  /**
   * Take a snapshot of a disabled table.
   * @param snapshot description of the snapshot to take. Modified to be {@link Type#DISABLED}.
   * @throws HBaseSnapshotException if the snapshot could not be started
   */
  private synchronized void snapshotDisabledTable(SnapshotDescription snapshot)
      throws HBaseSnapshotException {
    // setup the snapshot
    prepareToTakeSnapshot(snapshot);

    // set the snapshot to be a disabled snapshot, since the client doesn't know about that
    snapshot = snapshot.toBuilder().setType(Type.DISABLED).build();

    // Take the snapshot of the disabled table
    DisabledTableSnapshotHandler handler =
        new DisabledTableSnapshotHandler(snapshot, master);
    snapshotTable(snapshot, handler);
  }

  /**
   * Take a snapshot of an enabled table.
   * @param snapshot description of the snapshot to take.
   * @throws HBaseSnapshotException if the snapshot could not be started
   */
  private synchronized void snapshotEnabledTable(SnapshotDescription snapshot)
      throws HBaseSnapshotException {
    // setup the snapshot
    prepareToTakeSnapshot(snapshot);

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
      final TakeSnapshotHandler handler) throws HBaseSnapshotException {
    try {
      handler.prepare();
      this.executorService.submit(handler);
      this.snapshotHandlers.put(TableName.valueOf(snapshot.getTable()), handler);
    } catch (Exception e) {
      // cleanup the working directory by trying to delete it from the fs.
      Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir);
      try {
        if (!this.master.getMasterFileSystem().getFileSystem().delete(workingDir, true)) {
          LOG.error("Couldn't delete working directory (" + workingDir + " for snapshot:" +
              ClientSnapshotDescriptionUtils.toString(snapshot));
        }
      } catch (IOException e1) {
        LOG.error("Couldn't delete working directory (" + workingDir + " for snapshot:" +
            ClientSnapshotDescriptionUtils.toString(snapshot));
      }
      // fail the snapshot
      throw new SnapshotCreationException("Could not build snapshot handler", e, snapshot);
    }
  }

  /**
   * Take a snapshot based on the enabled/disabled state of the table.
   *
   * @param snapshot
   * @throws HBaseSnapshotException when a snapshot specific exception occurs.
   * @throws IOException when some sort of generic IO exception occurs.
   */
  public void takeSnapshot(SnapshotDescription snapshot) throws IOException {
    // check to see if we already completed the snapshot
    if (isSnapshotCompleted(snapshot)) {
      throw new SnapshotExistsException("Snapshot '" + snapshot.getName()
          + "' already stored on the filesystem.", snapshot);
    }

    LOG.debug("No existing snapshot, attempting snapshot...");

    // stop tracking "abandoned" handlers
    cleanupSentinels();

    // check to see if the table exists
    HTableDescriptor desc = null;
    try {
      desc = master.getTableDescriptors().get(
          TableName.valueOf(snapshot.getTable()));
    } catch (FileNotFoundException e) {
      String msg = "Table:" + snapshot.getTable() + " info doesn't exist!";
      LOG.error(msg);
      throw new SnapshotCreationException(msg, e, snapshot);
    } catch (IOException e) {
      throw new SnapshotCreationException("Error while geting table description for table "
          + snapshot.getTable(), e, snapshot);
    }
    if (desc == null) {
      throw new SnapshotCreationException("Table '" + snapshot.getTable()
          + "' doesn't exist, can't take snapshot.", snapshot);
    }
    SnapshotDescription.Builder builder = snapshot.toBuilder();
    // if not specified, set the snapshot format
    if (!snapshot.hasVersion()) {
      builder.setVersion(SnapshotDescriptionUtils.SNAPSHOT_LAYOUT_VERSION);
    }
    User user = RpcServer.getRequestUser();
    if (User.isHBaseSecurityEnabled(master.getConfiguration()) && user != null) {
      builder.setOwner(user.getShortName());
    }
    snapshot = builder.build();

    // call pre coproc hook
    MasterCoprocessorHost cpHost = master.getMasterCoprocessorHost();
    if (cpHost != null) {
      cpHost.preSnapshot(snapshot, desc);
    }

    // if the table is enabled, then have the RS run actually the snapshot work
    TableName snapshotTable = TableName.valueOf(snapshot.getTable());
    AssignmentManager assignmentMgr = master.getAssignmentManager();
    if (assignmentMgr.getTableStateManager().isTableState(snapshotTable,
        ZooKeeperProtos.Table.State.ENABLED)) {
      LOG.debug("Table enabled, starting distributed snapshot.");
      snapshotEnabledTable(snapshot);
      LOG.debug("Started snapshot: " + ClientSnapshotDescriptionUtils.toString(snapshot));
    }
    // For disabled table, snapshot is created by the master
    else if (assignmentMgr.getTableStateManager().isTableState(snapshotTable,
        ZooKeeperProtos.Table.State.DISABLED)) {
      LOG.debug("Table is disabled, running snapshot entirely on master.");
      snapshotDisabledTable(snapshot);
      LOG.debug("Started snapshot: " + ClientSnapshotDescriptionUtils.toString(snapshot));
    } else {
      LOG.error("Can't snapshot table '" + snapshot.getTable()
          + "', isn't open or closed, we don't know what to do!");
      TablePartiallyOpenException tpoe = new TablePartiallyOpenException(snapshot.getTable()
          + " isn't fully open.");
      throw new SnapshotCreationException("Table is not entirely open or closed", tpoe, snapshot);
    }

    // call post coproc hook
    if (cpHost != null) {
      cpHost.postSnapshot(snapshot, desc);
    }
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
   * Clone the specified snapshot into a new table.
   * The operation will fail if the destination table has a snapshot or restore in progress.
   *
   * @param snapshot Snapshot Descriptor
   * @param hTableDescriptor Table Descriptor of the table to create
   */
  synchronized void cloneSnapshot(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor) throws HBaseSnapshotException {
    TableName tableName = hTableDescriptor.getTableName();

    // make sure we aren't running a snapshot on the same table
    if (isTakingSnapshot(tableName)) {
      throw new RestoreSnapshotException("Snapshot in progress on the restore table=" + tableName);
    }

    // make sure we aren't running a restore on the same table
    if (isRestoringTable(tableName)) {
      throw new RestoreSnapshotException("Restore already in progress on the table=" + tableName);
    }

    try {
      CloneSnapshotHandler handler =
        new CloneSnapshotHandler(master, snapshot, hTableDescriptor).prepare();
      this.executorService.submit(handler);
      this.restoreHandlers.put(tableName, handler);
    } catch (Exception e) {
      String msg = "Couldn't clone the snapshot=" + ClientSnapshotDescriptionUtils.toString(snapshot) +
        " on table=" + tableName;
      LOG.error(msg, e);
      throw new RestoreSnapshotException(msg, e);
    }
  }

  /**
   * Restore the specified snapshot
   * @param reqSnapshot
   * @throws IOException
   */
  public void restoreSnapshot(SnapshotDescription reqSnapshot) throws IOException {
    FileSystem fs = master.getMasterFileSystem().getFileSystem();
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(reqSnapshot, rootDir);
    MasterCoprocessorHost cpHost = master.getMasterCoprocessorHost();

    // check if the snapshot exists
    if (!fs.exists(snapshotDir)) {
      LOG.error("A Snapshot named '" + reqSnapshot.getName() + "' does not exist.");
      throw new SnapshotDoesNotExistException(reqSnapshot);
    }

    // Get snapshot info from file system. The reqSnapshot is a "fake" snapshotInfo with
    // just the snapshot "name" and table name to restore. It does not contains the "real" snapshot
    // information.
    SnapshotDescription snapshot = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    SnapshotManifest manifest = SnapshotManifest.open(master.getConfiguration(), fs,
        snapshotDir, snapshot);
    HTableDescriptor snapshotTableDesc = manifest.getTableDescriptor();
    TableName tableName = TableName.valueOf(reqSnapshot.getTable());

    // stop tracking "abandoned" handlers
    cleanupSentinels();

    // Verify snapshot validity
    SnapshotReferenceUtil.verifySnapshot(master.getConfiguration(), fs, manifest);

    // Execute the restore/clone operation
    if (MetaTableAccessor.tableExists(master.getConnection(), tableName)) {
      if (master.getAssignmentManager().getTableStateManager().isTableState(
          TableName.valueOf(snapshot.getTable()), ZooKeeperProtos.Table.State.ENABLED)) {
        throw new UnsupportedOperationException("Table '" +
            TableName.valueOf(snapshot.getTable()) + "' must be disabled in order to " +
            "perform a restore operation" +
            ".");
      }

      // call coproc pre hook
      if (cpHost != null) {
        cpHost.preRestoreSnapshot(reqSnapshot, snapshotTableDesc);
      }

      int tableRegionCount = -1;
      try {
        // Table already exist. Check and update the region quota for this table namespace.
        // The region quota may not be updated correctly if there are concurrent restore snapshot
        // requests for the same table

        tableRegionCount = getRegionCountOfTable(tableName);
        int snapshotRegionCount = manifest.getRegionManifestsMap().size();

        // Update region quota when snapshotRegionCount is larger. If we updated the region count
        // to a smaller value before retoreSnapshot and the retoreSnapshot fails, we may fail to
        // reset the region count to its original value if the region quota is consumed by other
        // tables in the namespace
        if (tableRegionCount > 0 && tableRegionCount < snapshotRegionCount) {
          checkAndUpdateNamespaceRegionQuota(snapshotRegionCount, tableName);
        }
        restoreSnapshot(snapshot, snapshotTableDesc);
        // Update the region quota if snapshotRegionCount is smaller. This step should not fail
        // because we have reserved enough region quota before hand
        if (tableRegionCount > 0 && tableRegionCount > snapshotRegionCount) {
          checkAndUpdateNamespaceRegionQuota(snapshotRegionCount, tableName);
        }
      } catch (QuotaExceededException e) {
        LOG.error("Region quota exceeded while restoring the snapshot " + snapshot.getName()
          + " as table " + tableName.getNameAsString(), e);
        // If QEE is thrown before restoreSnapshot, quota information is not updated, so we
        // should throw the exception directly. If QEE is thrown after restoreSnapshot, there
        // must be unexpected reasons, we also throw the exception directly
        throw e;
      } catch (IOException e) {
        if (tableRegionCount > 0) {
          // reset the region count for table
          checkAndUpdateNamespaceRegionQuota(tableRegionCount, tableName);
        }
        LOG.error("Exception occurred while restoring the snapshot " + snapshot.getName()
            + " as table " + tableName.getNameAsString(), e);
        throw e;
      }
      LOG.info("Restore snapshot=" + snapshot.getName() + " as table=" + tableName);

      if (cpHost != null) {
        cpHost.postRestoreSnapshot(reqSnapshot, snapshotTableDesc);
      }
    } else {
      HTableDescriptor htd = new HTableDescriptor(tableName, snapshotTableDesc);
      if (cpHost != null) {
        cpHost.preCloneSnapshot(reqSnapshot, htd);
      }
      try {
        checkAndUpdateNamespaceQuota(manifest, tableName);
        cloneSnapshot(snapshot, htd);
      } catch (IOException e) {
        this.master.getMasterQuotaManager().removeTableFromNamespaceQuota(tableName);
        LOG.error("Exception occurred while cloning the snapshot " + snapshot.getName()
            + " as table " + tableName.getNameAsString(), e);
        throw e;
      }
      LOG.info("Clone snapshot=" + snapshot.getName() + " as table=" + tableName);

      if (cpHost != null) {
        cpHost.postCloneSnapshot(reqSnapshot, htd);
      }
    }
  }

  private void checkAndUpdateNamespaceQuota(SnapshotManifest manifest, TableName tableName)
      throws IOException {
    if (this.master.getMasterQuotaManager().isQuotaEnabled()) {
      this.master.getMasterQuotaManager().checkNamespaceTableAndRegionQuota(tableName,
        manifest.getRegionManifestsMap().size());
    }
  }

  private void checkAndUpdateNamespaceRegionQuota(int updatedRegionCount, TableName tableName)
      throws IOException {
    if (this.master.getMasterQuotaManager().isQuotaEnabled()) {
      this.master.getMasterQuotaManager().checkAndUpdateNamespaceRegionQuota(tableName,
        updatedRegionCount);
    }
  }

  /**
   * @return cached region count, or -1 if quota manager is disabled or table status not found
  */
  private int getRegionCountOfTable(TableName tableName) throws IOException {
    if (this.master.getMasterQuotaManager().isQuotaEnabled()) {
      return this.master.getMasterQuotaManager().getRegionCountOfTable(tableName);
    }
    return -1;
  }

  /**
   * Restore the specified snapshot.
   * The restore will fail if the destination table has a snapshot or restore in progress.
   *
   * @param snapshot Snapshot Descriptor
   * @param hTableDescriptor Table Descriptor
   */
  private synchronized void restoreSnapshot(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor) throws HBaseSnapshotException {
    TableName tableName = hTableDescriptor.getTableName();

    // make sure we aren't running a snapshot on the same table
    if (isTakingSnapshot(tableName)) {
      throw new RestoreSnapshotException("Snapshot in progress on the restore table=" + tableName);
    }

    // make sure we aren't running a restore on the same table
    if (isRestoringTable(tableName)) {
      throw new RestoreSnapshotException("Restore already in progress on the table=" + tableName);
    }

    try {
      RestoreSnapshotHandler handler =
        new RestoreSnapshotHandler(master, snapshot, hTableDescriptor).prepare();
      this.executorService.submit(handler);
      restoreHandlers.put(tableName, handler);
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
    SnapshotSentinel sentinel = this.restoreHandlers.get(tableName);
    return(sentinel != null && !sentinel.isFinished());
  }

  /**
   * Returns the status of a restore operation.
   * If the in-progress restore is failed throws the exception that caused the failure.
   *
   * @param snapshot
   * @return false if in progress, true if restore is completed or not requested.
   * @throws IOException if there was a failure during the restore
   */
  public boolean isRestoreDone(final SnapshotDescription snapshot) throws IOException {
    // check to see if the sentinel exists,
    // and if the task is complete removes it from the in-progress restore map.
    SnapshotSentinel sentinel = removeSentinelIfFinished(this.restoreHandlers, snapshot);

    // stop tracking "abandoned" handlers
    cleanupSentinels();

    if (sentinel == null) {
      // there is no sentinel so restore is not in progress.
      return true;
    }

    LOG.debug("Verify snapshot=" + snapshot.getName() + " against="
        + sentinel.getSnapshot().getName() + " table=" +
        TableName.valueOf(snapshot.getTable()));

    // If the restore is failed, rethrow the exception
    sentinel.rethrowExceptionIfFailed();

    // check to see if we are done
    if (sentinel.isFinished()) {
      LOG.debug("Restore snapshot=" + ClientSnapshotDescriptionUtils.toString(snapshot) +
          " has completed. Notifying the client.");
      return true;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Sentinel is not yet finished with restoring snapshot=" +
          ClientSnapshotDescriptionUtils.toString(snapshot));
    }
    return false;
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
    cleanupSentinels(this.restoreHandlers);
  }

  /**
   * Remove the sentinels that are marked as finished and the completion time
   * has exceeded the removal timeout.
   * @param sentinels map of sentinels to clean
   */
  private synchronized void cleanupSentinels(final Map<TableName, SnapshotSentinel> sentinels) {
    long currentTime = EnvironmentEdgeManager.currentTime();
    Iterator<Map.Entry<TableName, SnapshotSentinel>> it =
        sentinels.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<TableName, SnapshotSentinel> entry = it.next();
      SnapshotSentinel sentinel = entry.getValue();
      if (sentinel.isFinished() &&
          (currentTime - sentinel.getCompletionTimestamp()) > SNAPSHOT_SENTINELS_CLEANUP_TIMEOUT)
      {
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

    // pass the stop onto all the restore handlers
    for (SnapshotSentinel restoreHandler: this.restoreHandlers.values()) {
      restoreHandler.cancel(why);
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
    Set<String> hfileCleaners = new HashSet<String>();
    String[] cleaners = conf.getStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS);
    if (cleaners != null) Collections.addAll(hfileCleaners, cleaners);

    Set<String> logCleaners = new HashSet<String>();
    cleaners = conf.getStrings(HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS);
    if (cleaners != null) Collections.addAll(logCleaners, cleaners);

    // check if an older version of snapshot directory was present
    Path oldSnapshotDir = new Path(mfs.getRootDir(), HConstants.OLD_SNAPSHOT_DIR_NAME);
    FileSystem fs = mfs.getFileSystem();
    List<SnapshotDescription> ss = getCompletedSnapshots(new Path(rootDir, oldSnapshotDir));
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
      logCleaners.add(SnapshotLogCleaner.class.getName());

      // Set cleaners conf
      conf.setStrings(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS,
        hfileCleaners.toArray(new String[hfileCleaners.size()]));
      conf.setStrings(HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS,
        logCleaners.toArray(new String[logCleaners.size()]));
    } else {
      // Verify if cleaners are present
      snapshotEnabled = logCleaners.contains(SnapshotLogCleaner.class.getName()) &&
        hfileCleaners.contains(SnapshotHFileCleaner.class.getName()) &&
        hfileCleaners.contains(HFileLinkCleaner.class.getName());

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
        FileStatus[] snapshots = FSUtils.listStatus(fs, snapshotDir,
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
    long timeoutMillis = Math.max(conf.getLong(SnapshotDescriptionUtils.SNAPSHOT_TIMEOUT_MILLIS_KEY,
                    SnapshotDescriptionUtils.SNAPSHOT_TIMEOUT_MILLIS_DEFAULT),
            conf.getLong(SnapshotDescriptionUtils.MASTER_SNAPSHOT_TIMEOUT_MILLIS,
                    SnapshotDescriptionUtils.DEFAULT_MAX_WAIT_TIME));
    int opThreads = conf.getInt(SNAPSHOT_POOL_THREADS_KEY, SNAPSHOT_POOL_THREADS_DEFAULT);

    // setup the default procedure coordinator
    String name = master.getServerName().toString();
    ThreadPoolExecutor tpool = ProcedureCoordinator.defaultPool(name, opThreads);
    ProcedureCoordinatorRpcs comms = new ZKProcedureCoordinatorRpcs(
        master.getZooKeeper(), SnapshotManager.ONLINE_SNAPSHOT_CONTROLLER_DESCRIPTION, name);

    this.coordinator = new ProcedureCoordinator(comms, tpool, timeoutMillis, wakeFrequency);
    this.executorService = master.getExecutorService();
    resetTempDir();
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
}
