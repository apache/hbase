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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionSnare;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MetricsSnapshot;
import org.apache.hadoop.hbase.master.SnapshotSentinel;
import org.apache.hadoop.hbase.master.locking.LockManager;
import org.apache.hadoop.hbase.master.locking.LockManager.MasterLock;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.procedure2.LockType;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

/**
 * A handler for taking snapshots from the master.
 *
 * This is not a subclass of TableEventHandler because using that would incur an extra hbase:meta scan.
 *
 * The {@link #snapshotRegions(List)} call should get implemented for each snapshot flavor.
 */
@InterfaceAudience.Private
public abstract class TakeSnapshotHandler extends EventHandler implements SnapshotSentinel,
    ForeignExceptionSnare {
  private static final Logger LOG = LoggerFactory.getLogger(TakeSnapshotHandler.class);

  private volatile boolean finished;

  // none of these should ever be null
  protected final MasterServices master;
  protected final MetricsSnapshot metricsSnapshot = new MetricsSnapshot();
  protected final SnapshotDescription snapshot;
  protected final Configuration conf;
  protected final FileSystem fs;
  protected final Path rootDir;
  private final Path snapshotDir;
  protected final Path workingDir;
  private final MasterSnapshotVerifier verifier;
  protected final ForeignExceptionDispatcher monitor;
  private final LockManager.MasterLock tableLock;
  protected final MonitoredTask status;
  protected final TableName snapshotTable;
  protected final SnapshotManifest snapshotManifest;
  protected final SnapshotManager snapshotManager;

  protected TableDescriptor htd;

  /**
   * @param snapshot descriptor of the snapshot to take
   * @param masterServices master services provider
   */
  public TakeSnapshotHandler(SnapshotDescription snapshot, final MasterServices masterServices,
                             final SnapshotManager snapshotManager) {
    super(masterServices, EventType.C_M_SNAPSHOT_TABLE);
    assert snapshot != null : "SnapshotDescription must not be nul1";
    assert masterServices != null : "MasterServices must not be nul1";

    this.master = masterServices;
    this.snapshot = snapshot;
    this.snapshotManager = snapshotManager;
    this.snapshotTable = TableName.valueOf(snapshot.getTable());
    this.conf = this.master.getConfiguration();
    this.fs = this.master.getMasterFileSystem().getFileSystem();
    this.rootDir = this.master.getMasterFileSystem().getRootDir();
    this.snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
    this.workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir);
    this.monitor = new ForeignExceptionDispatcher(snapshot.getName());
    this.snapshotManifest = SnapshotManifest.create(conf, fs, workingDir, snapshot, monitor);

    this.tableLock = master.getLockManager().createMasterLock(
        snapshotTable, LockType.EXCLUSIVE,
        this.getClass().getName() + ": take snapshot " + snapshot.getName());

    // prepare the verify
    this.verifier = new MasterSnapshotVerifier(masterServices, snapshot, rootDir);
    // update the running tasks
    this.status = TaskMonitor.get().createStatus(
      "Taking " + snapshot.getType() + " snapshot on table: " + snapshotTable);
  }

  private TableDescriptor loadTableDescriptor()
      throws FileNotFoundException, IOException {
    TableDescriptor htd =
      this.master.getTableDescriptors().get(snapshotTable);
    if (htd == null) {
      throw new IOException("TableDescriptor missing for " + snapshotTable);
    }
    return htd;
  }

  @Override
  public TakeSnapshotHandler prepare() throws Exception {
    super.prepare();
    // after this, you should ensure to release this lock in case of exceptions
    this.tableLock.acquire();
    try {
      this.htd = loadTableDescriptor(); // check that .tableinfo is present
    } catch (Exception e) {
      this.tableLock.release();
      throw e;
    }
    return this;
  }

  /**
   * Execute the core common portions of taking a snapshot. The {@link #snapshotRegions(List)}
   * call should get implemented for each snapshot flavor.
   */
  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="REC_CATCH_EXCEPTION",
    justification="Intentional")
  public void process() {
    String msg = "Running " + snapshot.getType() + " table snapshot " + snapshot.getName() + " "
        + eventType + " on table " + snapshotTable;
    LOG.info(msg);
    ReentrantLock lock = snapshotManager.getLocks().acquireLock(snapshot.getName());
    MasterLock tableLockToRelease = this.tableLock;
    status.setStatus(msg);
    try {
      if (downgradeToSharedTableLock()) {
        // release the exclusive lock and hold the shared lock instead
        tableLockToRelease = master.getLockManager().createMasterLock(snapshotTable,
          LockType.SHARED, this.getClass().getName() + ": take snapshot " + snapshot.getName());
        tableLock.release();
        tableLockToRelease.acquire();
      }
      // If regions move after this meta scan, the region specific snapshot should fail, triggering
      // an external exception that gets captured here.

      // write down the snapshot info in the working directory
      SnapshotDescriptionUtils.writeSnapshotInfo(snapshot, workingDir, fs);
      snapshotManifest.addTableDescriptor(this.htd);
      monitor.rethrowException();

      List<Pair<RegionInfo, ServerName>> regionsAndLocations;
      if (TableName.META_TABLE_NAME.equals(snapshotTable)) {
        regionsAndLocations = new MetaTableLocator().getMetaRegionsAndLocations(
          server.getZooKeeper());
      } else {
        regionsAndLocations = MetaTableAccessor.getTableRegionsAndLocations(
          server.getConnection(), snapshotTable, false);
      }

      // run the snapshot
      snapshotRegions(regionsAndLocations);
      monitor.rethrowException();

      // extract each pair to separate lists
      Set<String> serverNames = new HashSet<>();
      for (Pair<RegionInfo, ServerName> p : regionsAndLocations) {
        if (p != null && p.getFirst() != null && p.getSecond() != null) {
          RegionInfo hri = p.getFirst();
          if (hri.isOffline() && (hri.isSplit() || hri.isSplitParent())) continue;
          serverNames.add(p.getSecond().toString());
        }
      }

      // flush the in-memory state, and write the single manifest
      status.setStatus("Consolidate snapshot: " + snapshot.getName());
      snapshotManifest.consolidate();

      // verify the snapshot is valid
      status.setStatus("Verifying snapshot: " + snapshot.getName());
      verifier.verifySnapshot(this.workingDir, serverNames);

      // complete the snapshot, atomically moving from tmp to .snapshot dir.
      completeSnapshot(this.snapshotDir, this.workingDir, this.fs);
      msg = "Snapshot " + snapshot.getName() + " of table " + snapshotTable + " completed";
      status.markComplete(msg);
      LOG.info(msg);
      metricsSnapshot.addSnapshot(status.getCompletionTimestamp() - status.getStartTime());
    } catch (Exception e) { // FindBugs: REC_CATCH_EXCEPTION
      status.abort("Failed to complete snapshot " + snapshot.getName() + " on table " +
          snapshotTable + " because " + e.getMessage());
      String reason = "Failed taking snapshot " + ClientSnapshotDescriptionUtils.toString(snapshot)
          + " due to exception:" + e.getMessage();
      LOG.error(reason, e);
      ForeignException ee = new ForeignException(reason, e);
      monitor.receive(ee);
      // need to mark this completed to close off and allow cleanup to happen.
      cancel(reason);
    } finally {
      LOG.debug("Launching cleanup of working dir:" + workingDir);
      try {
        // if the working dir is still present, the snapshot has failed.  it is present we delete
        // it.
        if (fs.exists(workingDir) && !this.fs.delete(workingDir, true)) {
          LOG.error("Couldn't delete snapshot working directory:" + workingDir);
        }
      } catch (IOException e) {
        LOG.error("Couldn't delete snapshot working directory:" + workingDir);
      }
      lock.unlock();
      tableLockToRelease.release();
    }
  }

  /**
   * Reset the manager to allow another snapshot to proceed
   *
   * @param snapshotDir final path of the snapshot
   * @param workingDir directory where the in progress snapshot was built
   * @param fs {@link FileSystem} where the snapshot was built
   * @throws SnapshotCreationException if the snapshot could not be moved
   * @throws IOException the filesystem could not be reached
   */
  public void completeSnapshot(Path snapshotDir, Path workingDir, FileSystem fs)
      throws SnapshotCreationException, IOException {
    LOG.debug("Sentinel is done, just moving the snapshot from " + workingDir + " to "
        + snapshotDir);
    if (!fs.rename(workingDir, snapshotDir)) {
      throw new SnapshotCreationException("Failed to move working directory(" + workingDir
          + ") to completed directory(" + snapshotDir + ").");
    }
    finished = true;
  }

  /**
   * When taking snapshot, first we must acquire the exclusive table lock to confirm that there are
   * no ongoing merge/split procedures. But later, we should try our best to release the exclusive
   * lock as this may hurt the availability, because we need to hold the shared lock when assigning
   * regions.
   * <p/>
   * See HBASE-21480 for more details.
   */
  protected abstract boolean downgradeToSharedTableLock();

  /**
   * Snapshot the specified regions
   */
  protected abstract void snapshotRegions(List<Pair<RegionInfo, ServerName>> regions)
      throws IOException, KeeperException;

  /**
   * Take a snapshot of the specified disabled region
   */
  protected void snapshotDisabledRegion(final RegionInfo regionInfo)
      throws IOException {
    snapshotManifest.addRegion(FSUtils.getTableDir(rootDir, snapshotTable), regionInfo);
    monitor.rethrowException();
    status.setStatus("Completed referencing HFiles for offline region " + regionInfo.toString() +
        " of table: " + snapshotTable);
  }

  @Override
  public void cancel(String why) {
    if (finished) return;

    this.finished = true;
    LOG.info("Stop taking snapshot=" + ClientSnapshotDescriptionUtils.toString(snapshot) +
        " because: " + why);
    CancellationException ce = new CancellationException(why);
    monitor.receive(new ForeignException(master.getServerName().toString(), ce));
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public long getCompletionTimestamp() {
    return this.status.getCompletionTimestamp();
  }

  @Override
  public SnapshotDescription getSnapshot() {
    return snapshot;
  }

  @Override
  public ForeignException getExceptionIfFailed() {
    return monitor.getException();
  }

  @Override
  public void rethrowExceptionIfFailed() throws ForeignException {
    monitor.rethrowException();
  }

  @Override
  public void rethrowException() throws ForeignException {
    monitor.rethrowException();
  }

  @Override
  public boolean hasException() {
    return monitor.hasException();
  }

  @Override
  public ForeignException getException() {
    return monitor.getException();
  }

}
