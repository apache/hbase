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
package org.apache.hadoop.hbase.master.snapshot.manage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.SnapshotSentinel;
import org.apache.hadoop.hbase.master.snapshot.CloneSnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.DisabledTableSnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.RestoreSnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.TakeSnapshotHandler;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotDoesNotExistException;
import org.apache.hadoop.hbase.snapshot.exception.SnapshotExistsException;
import org.apache.hadoop.hbase.snapshot.exception.TablePartiallyOpenException;
import org.apache.hadoop.hbase.snapshot.exception.UnknownSnapshotException;
import org.apache.hadoop.hbase.snapshot.restore.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;

import com.google.protobuf.ServiceException;

/**
 * This class manages the procedure of taking and restoring snapshots. There is only one
 * SnapshotManager for the master.
 * <p>
 * The class provides methods for monitoring in-progress snapshot actions.
 * <p>
 * Note: Currently there can only one snapshot being taken at a time over the cluster.  This is a
 * simplification in the current implementation.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SnapshotManager implements Stoppable {
  private static final Log LOG = LogFactory.getLog(SnapshotManager.class);

  /** By default, check to see if the snapshot is complete every WAKE MILLIS (ms) */
  public static final int SNAPSHOT_WAKE_MILLIS_DEFAULT = 500;

  /**
   * Conf key for # of ms elapsed between checks for snapshot errors while waiting for
   * completion.
   */
  public static final String SNAPSHOT_WAKE_MILLIS_KEY = "hbase.snapshot.master.wakeMillis";

  /** By default, check to see if the snapshot is complete (ms) */
  public static final int SNAPSHOT_TIMEOUT_MILLIS_DEFAULT = 5000;

  /**
   * Conf key for # of ms elapsed before injecting a snapshot timeout error when waiting for
   * completion.
   */
  public static final String SNAPSHOT_TIMEMOUT_MILLIS_KEY = "hbase.snapshot.master.timeoutMillis";

  /** Name of the operation to use in the controller */
  public static final String ONLINE_SNAPSHOT_CONTROLLER_DESCRIPTION = "online-snapshot";

  // TODO - enable having multiple snapshots with multiple monitors/threads
  // this needs to be configuration based when running multiple snapshots is implemented
  /** number of current operations running on the master */
  private static final int opThreads = 1;

  private boolean stopped;
  private final long wakeFrequency;
  private final MasterServices master;  // Needed by TableEventHandlers

  // A reference to a handler.  If the handler is non-null, then it is assumed that a snapshot is
  // in progress currently
  // TODO: this is a bad smell;  likely replace with a collection in the future.  Also this gets
  // reset by every operation.
  private TakeSnapshotHandler handler;

  private final Path rootDir;
  private final ExecutorService executorService;

  // Restore Sentinels map, with table name as key
  private Map<String, SnapshotSentinel> restoreHandlers = new HashMap<String, SnapshotSentinel>();

  /**
   * Construct a snapshot manager.
   * @param master
   * @param comms
   */
  public SnapshotManager(final MasterServices master) throws IOException {
    this.master = master;

    // get the configuration for the coordinator
    Configuration conf = master.getConfiguration();
    this.wakeFrequency = conf.getInt(SNAPSHOT_WAKE_MILLIS_KEY, SNAPSHOT_WAKE_MILLIS_DEFAULT);
    this.rootDir = master.getMasterFileSystem().getRootDir();
    this.executorService = master.getExecutorService();
    resetTempDir();
  }

  /**
   * Gets the list of all completed snapshots.
   * @return list of SnapshotDescriptions
   * @throws IOException File system exception
   */
  public List<SnapshotDescription> getCompletedSnapshots() throws IOException {
    List<SnapshotDescription> snapshotDescs = new ArrayList<SnapshotDescription>();
    // first create the snapshot root path and check to see if it exists
    Path snapshotDir = SnapshotDescriptionUtils.getSnapshotsDir(rootDir);
    FileSystem fs = master.getMasterFileSystem().getFileSystem();

    // if there are no snapshots, return an empty list
    if (!fs.exists(snapshotDir)) {
      return snapshotDescs;
    }

    // ignore all the snapshots in progress
    FileStatus[] snapshots = fs.listStatus(snapshotDir,
      new SnapshotDescriptionUtils.CompletedSnaphotDirectoriesFilter(fs));
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
        snapshotDescs.add(desc);
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
    if (master.getMasterFileSystem().getFileSystem().delete(tmpdir, true)) {
      LOG.warn("Couldn't delete working snapshot directory: " + tmpdir);
    }
  }

  /**
   * Delete the specified snapshot
   * @param snapshot
   * @throws SnapshotDoesNotExistException If the specified snapshot does not exist.
   * @throws IOException For filesystem IOExceptions
   */
  public void deleteSnapshot(SnapshotDescription snapshot) throws SnapshotDoesNotExistException, IOException {

    // call coproc pre hook
    MasterCoprocessorHost cpHost = master.getCoprocessorHost();
    if (cpHost != null) {
      cpHost.preDeleteSnapshot(snapshot);
    }

    // check to see if it is completed
    if (!isSnapshotCompleted(snapshot)) {
      throw new SnapshotDoesNotExistException(snapshot);
    }

    String snapshotName = snapshot.getName();
    LOG.debug("Deleting snapshot: " + snapshotName);
    // first create the snapshot description and check to see if it exists
    MasterFileSystem fs = master.getMasterFileSystem();
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);

    // delete the existing snapshot
    if (!fs.getFileSystem().delete(snapshotDir, true)) {
      throw new HBaseSnapshotException("Failed to delete snapshot directory: " + snapshotDir);
    }

    // call coproc post hook
    if (cpHost != null) {
      cpHost.postDeleteSnapshot(snapshot);
    }

  }

  /**
   * Return the handler if it is currently running and has the same snapshot target name.
   * @param snapshot
   * @return null if doesn't match, else a live handler.
   */
  TakeSnapshotHandler getTakeSnapshotHandler(SnapshotDescription snapshot) {
    TakeSnapshotHandler h = this.handler;
    if (h == null) {
      return null;
    }

    if (!h.getSnapshot().getName().equals(snapshot.getName())) {
      // specified snapshot is to the one currently running
      return null;
    }

    return h;
  }

  /**
   * Check if the specified snapshot is done
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

    // check to see if the sentinel exists
    TakeSnapshotHandler handler = getTakeSnapshotHandler(expected);
    if (handler == null) {
      // doesn't exist, check if it is already completely done.
      if (!isSnapshotCompleted(expected)) {
        throw new UnknownSnapshotException("Snapshot:" + expected.getName()
            + " is not currently running or one of the known completed snapshots.");
      }
      // was done, return true;
      return true;
    }

    // pass on any failure we find in the sentinel
    try {
      handler.rethrowException();
    } catch (ForeignException e) {
      throw new HBaseSnapshotException("Snapshot error from RS", e, expected);
    }

    // check to see if we are done
    if (handler.isFinished()) {
      LOG.debug("Snapshot '" + expected.getName() + "' has completed, notifying client.");
      return true;
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Sentinel isn't finished with snapshot '" + expected.getName() + "'!");
    }
    return false;
  }

  /**
   * Check to see if there are any snapshots in progress currently.  Currently we have a
   * limitation only allowing a single snapshot attempt at a time.
   * @return <tt>true</tt> if there any snapshots in progress, <tt>false</tt> otherwise
   * @throws SnapshotCreationException if the snapshot failed
   */
  synchronized boolean isTakingSnapshot() throws SnapshotCreationException {
    // TODO later when we handle multiple there would be a map with ssname to handler.
    return handler != null && !handler.isFinished();
  }

  /**
   * Check to see if the specified table has a snapshot in progress.  Currently we have a
   * limitation only allowing a single snapshot attempt at a time.
   * @param tableName name of the table being snapshotted.
   * @return <tt>true</tt> if there is a snapshot in progress on the specified table.
   */
  private boolean isTakingSnapshot(final String tableName) {
    if (handler != null && handler.getSnapshot().getTable().equals(tableName)) {
      return !handler.isFinished();
    }
    return false;
  }

  /**
   * Check to make sure that we are OK to run the passed snapshot. Checks to make sure that we
   * aren't already running a snapshot.
   * @param snapshot description of the snapshot we want to start
   * @throws HBaseSnapshotException if the filesystem could not be prepared to start the snapshot
   */
  private synchronized void prepareToTakeSnapshot(SnapshotDescription snapshot)
      throws HBaseSnapshotException {
    FileSystem fs = master.getMasterFileSystem().getFileSystem();
    Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir);

    // make sure we aren't already running a snapshot
    if (isTakingSnapshot()) {
      throw new SnapshotCreationException("Already running another snapshot:"
          + this.handler.getSnapshot(), snapshot);
    }

    // make sure we aren't running a restore on the same table
    if (isRestoringTable(snapshot.getTable())) {
      throw new SnapshotCreationException("Restore in progress on the same table snapshot:"
          + this.handler.getSnapshot(), snapshot);
    }

    try {
      // delete the working directory, since we aren't running the snapshot.  Likely leftovers
      // from a failed attempt.
      fs.delete(workingDir, true);

      // recreate the working directory for the snapshot
      if (!fs.mkdirs(workingDir)) {
        throw new SnapshotCreationException("Couldn't create working directory (" + workingDir
            + ") for snapshot.", snapshot);
      }
    } catch (HBaseSnapshotException e) {
      throw e;
    } catch (IOException e) {
      throw new SnapshotCreationException(
          "Exception while checking to see if snapshot could be started.", e, snapshot);
    }
  }

  /**
   * Take a snapshot based on the enabled/disabled state of the table.
   *
   * @param snapshot
   * @throws HBaseSnapshotException when a snapshot specific exception occurs.
   * @throws IOException when some sort of generic IO exception occurs.
   */
  public void takeSnapshot(SnapshotDescription snapshot) throws HBaseSnapshotException, IOException {
    // check to see if we already completed the snapshot
    if (isSnapshotCompleted(snapshot)) {
      throw new SnapshotExistsException("Snapshot '" + snapshot.getName()
          + "' already stored on the filesystem.", snapshot);
    }

    LOG.debug("No existing snapshot, attempting snapshot...");

    // check to see if the table exists
    HTableDescriptor desc = null;
    try {
      desc = master.getTableDescriptors().get(snapshot.getTable());
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

    // set the snapshot version, now that we are ready to take it
    snapshot = snapshot.toBuilder().setVersion(SnapshotDescriptionUtils.SNAPSHOT_LAYOUT_VERSION)
        .build();

    // call pre coproc hook
    MasterCoprocessorHost cpHost = master.getCoprocessorHost();
    if (cpHost != null) {
      cpHost.preSnapshot(snapshot, desc);
    }

    // setup the snapshot
    prepareToTakeSnapshot(snapshot);

    // if the table is enabled, then have the RS run actually the snapshot work
    AssignmentManager assignmentMgr = master.getAssignmentManager();
    if (assignmentMgr.getZKTable().isEnabledTable(snapshot.getTable())) {
      LOG.debug("Table enabled, starting distributed snapshot.");
      throw new UnsupportedOperationException("Snapshots of enabled tables is not yet supported");
    }
    // For disabled table, snapshot is created by the master
    else if (assignmentMgr.getZKTable().isDisabledTable(snapshot.getTable())) {
      LOG.debug("Table is disabled, running snapshot entirely on master.");
      snapshotDisabledTable(snapshot);
      LOG.debug("Started snapshot: " + snapshot);
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
   * Take a snapshot of a disabled table.
   * <p>
   * Ensures the snapshot won't be started if there is another snapshot already running. Does
   * <b>not</b> check to see if another snapshot of the same name already exists.
   * @param snapshot description of the snapshot to take. Modified to be {@link Type#DISABLED}.
   * @throws HBaseSnapshotException if the snapshot could not be started
   */
  private synchronized void snapshotDisabledTable(SnapshotDescription snapshot)
      throws HBaseSnapshotException {

    // set the snapshot to be a disabled snapshot, since the client doesn't know about that
    snapshot = snapshot.toBuilder().setType(Type.DISABLED).build();

    DisabledTableSnapshotHandler handler;
    try {
      handler = new DisabledTableSnapshotHandler(snapshot, this.master);
      this.handler = handler;
      this.executorService.submit(handler);
    } catch (IOException e) {
      // cleanup the working directory by trying to delete it from the fs.
      Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir);
      try {
        if (this.master.getMasterFileSystem().getFileSystem().delete(workingDir, true)) {
          LOG.error("Couldn't delete working directory (" + workingDir + " for snapshot:"
              + snapshot);
        }
      } catch (IOException e1) {
        LOG.error("Couldn't delete working directory (" + workingDir + " for snapshot:" + snapshot);
      }
      // fail the snapshot
      throw new SnapshotCreationException("Could not build snapshot handler", e, snapshot);
    }
  }

  /**
   * Set the handler for the current snapshot
   * <p>
   * Exposed for TESTING
   * @param handler handler the master should use
   *
   * TODO get rid of this if possible, repackaging, modify tests.
   */
  public synchronized void setSnapshotHandlerForTesting(TakeSnapshotHandler handler) {
    this.handler = handler;
  }

  /**
   * Check to see if the snapshot is one of the currently completed snapshots
   * @param expected snapshot to check
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
   * Restore the specified snapshot.
   * The restore will fail if the destination table has a snapshot or restore in progress.
   *
   * @param snapshot Snapshot Descriptor
   * @param hTableDescriptor Table Descriptor of the table to create
   * @param waitTime timeout before considering the clone failed
   */
  synchronized void cloneSnapshot(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor) throws HBaseSnapshotException {
    String tableName = hTableDescriptor.getNameAsString();

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
        new CloneSnapshotHandler(master, snapshot, hTableDescriptor);
      this.executorService.submit(handler);
      restoreHandlers.put(tableName, handler);
    } catch (Exception e) {
      String msg = "Couldn't clone the snapshot=" + snapshot + " on table=" + tableName;
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
    MasterCoprocessorHost cpHost = master.getCoprocessorHost();

    // check if the snapshot exists
    if (!fs.exists(snapshotDir)) {
      LOG.error("A Snapshot named '" + reqSnapshot.getName() + "' does not exist.");
      throw new SnapshotDoesNotExistException(reqSnapshot);
    }

    // read snapshot information
    SnapshotDescription fsSnapshot = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    HTableDescriptor snapshotTableDesc = FSTableDescriptors.getTableDescriptor(fs, snapshotDir);
    String tableName = reqSnapshot.getTable();

    // stop tracking completed restores
    cleanupRestoreSentinels();

    // Execute the restore/clone operation
    if (MetaReader.tableExists(master.getCatalogTracker(), tableName)) {
      if (master.getAssignmentManager().getZKTable().isEnabledTable(fsSnapshot.getTable())) {
        throw new UnsupportedOperationException("Table '" +
          fsSnapshot.getTable() + "' must be disabled in order to perform a restore operation.");
      }

      // call coproc pre hook
      if (cpHost != null) {
        cpHost.preRestoreSnapshot(reqSnapshot, snapshotTableDesc);
      }
      restoreSnapshot(fsSnapshot, snapshotTableDesc);
      LOG.info("Restore snapshot=" + fsSnapshot.getName() + " as table=" + tableName);

      if (cpHost != null) {
        cpHost.postRestoreSnapshot(reqSnapshot, snapshotTableDesc);
      }
    } else {
      HTableDescriptor htd = RestoreSnapshotHelper.cloneTableSchema(snapshotTableDesc,
                                                         Bytes.toBytes(tableName));
      if (cpHost != null) {
        cpHost.preCloneSnapshot(reqSnapshot, htd);
      }
      cloneSnapshot(fsSnapshot, htd);
      LOG.info("Clone snapshot=" + fsSnapshot.getName() + " as table=" + tableName);

      if (cpHost != null) {
        cpHost.postCloneSnapshot(reqSnapshot, htd);
      }
    }
  }

  /**
   * Restore the specified snapshot.
   * The restore will fail if the destination table has a snapshot or restore in progress.
   *
   * @param snapshot Snapshot Descriptor
   * @param hTableDescriptor Table Descriptor
   * @param waitTime timeout before considering the restore failed
   */
  private synchronized void restoreSnapshot(final SnapshotDescription snapshot,
      final HTableDescriptor hTableDescriptor) throws HBaseSnapshotException {
    String tableName = hTableDescriptor.getNameAsString();

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
        new RestoreSnapshotHandler(master, snapshot, hTableDescriptor);
      this.executorService.submit(handler);
      restoreHandlers.put(hTableDescriptor.getNameAsString(), handler);
    } catch (Exception e) {
      String msg = "Couldn't restore the snapshot=" + snapshot + " on table=" + tableName;
      LOG.error(msg, e);
      throw new RestoreSnapshotException(msg, e);
    }
  }

  /**
   * Verify if the the restore of the specified table is in progress.
   *
   * @param tableName table under restore
   * @return <tt>true</tt> if there is a restore in progress of the specified table.
   */
  private boolean isRestoringTable(final String tableName) {
    SnapshotSentinel sentinel = restoreHandlers.get(tableName);
    return(sentinel != null && !sentinel.isFinished());
  }

  /**
   * Returns status of a restore request, specifically comparing source snapshot and target table
   * names.  Throws exception if not a known snapshot.
   * @param snapshot
   * @return true if in progress, false if is not.
   * @throws UnknownSnapshotException if specified source snapshot does not exit.
   * @throws IOException if there was some sort of IO failure
   */
  public boolean isRestoringTable(final SnapshotDescription snapshot) throws IOException {
    // check to see if the snapshot is already on the fs
    if (!isSnapshotCompleted(snapshot)) {
      throw new UnknownSnapshotException("Snapshot:" + snapshot.getName()
          + " is not one of the known completed snapshots.");
    }

    SnapshotSentinel sentinel = getRestoreSnapshotSentinel(snapshot.getTable());
    if (sentinel == null) {
      // there is no sentinel so restore is not in progress.
      return false;
    }
    if (!sentinel.getSnapshot().getName().equals(snapshot.getName())) {
      // another handler is trying to restore to the table, but it isn't the same snapshot source.
      return false;
    }

    LOG.debug("Verify snapshot=" + snapshot.getName() + " against="
        + sentinel.getSnapshot().getName() + " table=" + snapshot.getTable());
    ForeignException e = sentinel.getExceptionIfFailed();
    if (e != null) throw e;

    // check to see if we are done
    if (sentinel.isFinished()) {
      LOG.debug("Restore snapshot=" + snapshot + " has completed. Notifying the client.");
      return false;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Sentinel is not yet finished with restoring snapshot=" + snapshot);
    }
    return true;
  }

  /**
   * Get the restore snapshot sentinel for the specified table
   * @param tableName table under restore
   * @return the restore snapshot handler
   */
  private synchronized SnapshotSentinel getRestoreSnapshotSentinel(final String tableName) {
    try {
      return restoreHandlers.get(tableName);
    } finally {
      cleanupRestoreSentinels();
    }
  }

  /**
   * Scan the restore handlers and remove the finished ones.
   */
  private void cleanupRestoreSentinels() {
    Iterator<Map.Entry<String, SnapshotSentinel>> it = restoreHandlers.entrySet().iterator();
    while (it.hasNext()) {
        Map.Entry<String, SnapshotSentinel> entry = it.next();
        SnapshotSentinel sentinel = entry.getValue();
        if (sentinel.isFinished()) {
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
    if (this.handler != null) this.handler.cancel(why);

    // pass the stop onto all the restore handlers
    for (SnapshotSentinel restoreHandler: this.restoreHandlers.values()) {
      restoreHandler.cancel(why);
    }
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }
}
