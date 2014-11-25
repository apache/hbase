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

package org.apache.hadoop.hbase.master.snapshot;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CancellationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MetricsSnapshot;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.SnapshotSentinel;
import org.apache.hadoop.hbase.master.handler.TableEventHandler;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;

/**
 * Handler to Restore a snapshot.
 *
 * <p>Uses {@link RestoreSnapshotHelper} to replace the table content with the
 * data available in the snapshot.
 */
@InterfaceAudience.Private
public class RestoreSnapshotHandler extends TableEventHandler implements SnapshotSentinel {
  private static final Log LOG = LogFactory.getLog(RestoreSnapshotHandler.class);

  private final HTableDescriptor hTableDescriptor;
  private final SnapshotDescription snapshot;

  private final ForeignExceptionDispatcher monitor;
  private final MetricsSnapshot metricsSnapshot = new MetricsSnapshot();
  private final MonitoredTask status;

  private volatile boolean stopped = false;

  public RestoreSnapshotHandler(final MasterServices masterServices,
      final SnapshotDescription snapshot, final HTableDescriptor htd) throws IOException {
    super(EventType.C_M_RESTORE_SNAPSHOT, htd.getTableName(), masterServices, masterServices);

    // Snapshot information
    this.snapshot = snapshot;

    // Monitor
    this.monitor = new ForeignExceptionDispatcher();

    // Check table exists.
    getTableDescriptor();

    // This is the new schema we are going to write out as this modification.
    this.hTableDescriptor = htd;

    this.status = TaskMonitor.get().createStatus(
      "Restoring  snapshot '" + snapshot.getName() + "' to table "
          + hTableDescriptor.getTableName());
  }

  @Override
  public RestoreSnapshotHandler prepare() throws IOException {
    return (RestoreSnapshotHandler) super.prepare();
  }

  /**
   * The restore table is executed in place.
   *  - The on-disk data will be restored - reference files are put in place without moving data
   *  -  [if something fail here: you need to delete the table and re-run the restore]
   *  - hbase:meta will be updated
   *  -  [if something fail here: you need to run hbck to fix hbase:meta entries]
   * The passed in list gets changed in this method
   */
  @Override
  protected void handleTableOperation(List<HRegionInfo> hris) throws IOException {
    MasterFileSystem fileSystemManager = masterServices.getMasterFileSystem();
    Connection conn = masterServices.getConnection();
    FileSystem fs = fileSystemManager.getFileSystem();
    Path rootDir = fileSystemManager.getRootDir();
    TableName tableName = hTableDescriptor.getTableName();

    try {
      // 1. Update descriptor
      this.masterServices.getTableDescriptors().add(hTableDescriptor);

      // 2. Execute the on-disk Restore
      LOG.debug("Starting restore snapshot=" + ClientSnapshotDescriptionUtils.toString(snapshot));
      Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
      SnapshotManifest manifest = SnapshotManifest.open(masterServices.getConfiguration(), fs,
                                                        snapshotDir, snapshot);
      RestoreSnapshotHelper restoreHelper = new RestoreSnapshotHelper(
          masterServices.getConfiguration(), fs, manifest,
          this.hTableDescriptor, rootDir, monitor, status);
      RestoreSnapshotHelper.RestoreMetaChanges metaChanges = restoreHelper.restoreHdfsRegions();

      // 3. Forces all the RegionStates to be offline
      //
      // The AssignmentManager keeps all the region states around
      // with no possibility to remove them, until the master is restarted.
      // This means that a region marked as SPLIT before the restore will never be assigned again.
      // To avoid having all states around all the regions are switched to the OFFLINE state,
      // which is the same state that the regions will be after a delete table.
      forceRegionsOffline(metaChanges);

      // 4. Applies changes to hbase:meta
      status.setStatus("Preparing to restore each region");

      // 4.1 Removes the current set of regions from META
      //
      // By removing also the regions to restore (the ones present both in the snapshot
      // and in the current state) we ensure that no extra fields are present in META
      // e.g. with a simple add addRegionToMeta() the splitA and splitB attributes
      // not overwritten/removed, so you end up with old informations
      // that are not correct after the restore.
      List<HRegionInfo> hrisToRemove = new LinkedList<HRegionInfo>();
      if (metaChanges.hasRegionsToRemove()) hrisToRemove.addAll(metaChanges.getRegionsToRemove());
      MetaTableAccessor.deleteRegions(conn, hrisToRemove);

      // 4.2 Add the new set of regions to META
      //
      // At this point the old regions are no longer present in META.
      // and the set of regions present in the snapshot will be written to META.
      // All the information in hbase:meta are coming from the .regioninfo of each region present
      // in the snapshot folder.
      hris.clear();
      if (metaChanges.hasRegionsToAdd()) hris.addAll(metaChanges.getRegionsToAdd());
      MetaTableAccessor.addRegionsToMeta(conn, hris);
      if (metaChanges.hasRegionsToRestore()) {
        MetaTableAccessor.overwriteRegions(conn, metaChanges.getRegionsToRestore());
      }
      metaChanges.updateMetaParentRegions(this.server.getConnection(), hris);

      // At this point the restore is complete. Next step is enabling the table.
      LOG.info("Restore snapshot=" + ClientSnapshotDescriptionUtils.toString(snapshot) +
        " on table=" + tableName + " completed!");
    } catch (IOException e) {
      String msg = "restore snapshot=" + ClientSnapshotDescriptionUtils.toString(snapshot)
          + " failed. Try re-running the restore command.";
      LOG.error(msg, e);
      monitor.receive(new ForeignException(masterServices.getServerName().toString(), e));
      throw new RestoreSnapshotException(msg, e);
    }
  }

  private void forceRegionsOffline(final RestoreSnapshotHelper.RestoreMetaChanges metaChanges) {
    forceRegionsOffline(metaChanges.getRegionsToAdd());
    forceRegionsOffline(metaChanges.getRegionsToRestore());
    forceRegionsOffline(metaChanges.getRegionsToRemove());
  }

  private void forceRegionsOffline(final List<HRegionInfo> hris) {
    AssignmentManager am = this.masterServices.getAssignmentManager();
    RegionStates states = am.getRegionStates();
    if (hris != null) {
      for (HRegionInfo hri: hris) {
        states.regionOffline(hri);
      }
    }
  }

  @Override
  protected void completed(final Throwable exception) {
    this.stopped = true;
    if (exception != null) {
      status.abort("Restore snapshot '" + snapshot.getName() + "' failed because " +
          exception.getMessage());
    } else {
      status.markComplete("Restore snapshot '"+ snapshot.getName() +"'!");
    }
    metricsSnapshot.addSnapshotRestore(status.getCompletionTimestamp() - status.getStartTime());
    super.completed(exception);
  }

  @Override
  public boolean isFinished() {
    return this.stopped;
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
  public void cancel(String why) {
    if (this.stopped) return;
    this.stopped = true;
    String msg = "Stopping restore snapshot=" + ClientSnapshotDescriptionUtils.toString(snapshot)
        + " because: " + why;
    LOG.info(msg);
    CancellationException ce = new CancellationException(why);
    this.monitor.receive(new ForeignException(masterServices.getServerName().toString(), ce));
  }

  @Override
  public ForeignException getExceptionIfFailed() {
    return this.monitor.getException();
  }

  @Override
  public void rethrowExceptionIfFailed() throws ForeignException {
    monitor.rethrowException();
  }
}
