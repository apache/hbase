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
import java.util.List;
import java.util.concurrent.CancellationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.SnapshotSentinel;
import org.apache.hadoop.hbase.master.handler.TableEventHandler;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.Bytes;

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
  private volatile boolean stopped = false;

  public RestoreSnapshotHandler(final MasterServices masterServices,
      final SnapshotDescription snapshot, final HTableDescriptor htd)
      throws IOException {
    super(EventType.C_M_RESTORE_SNAPSHOT, htd.getName(), masterServices, masterServices);

    // Snapshot information
    this.snapshot = snapshot;

    // Monitor
    this.monitor = new ForeignExceptionDispatcher();

    // Check table exists.
    getTableDescriptor();

    // This is the new schema we are going to write out as this modification.
    this.hTableDescriptor = htd;
  }

  /**
   * The restore table is executed in place.
   *  - The on-disk data will be restored
   *  - [if something fail here: you need to delete the table and re-run the restore]
   *  - META will be updated
   *  - [if something fail here: you need to run hbck to fix META entries]
   */
  @Override
  protected void handleTableOperation(List<HRegionInfo> hris) throws IOException {
    MasterFileSystem fileSystemManager = masterServices.getMasterFileSystem();
    CatalogTracker catalogTracker = masterServices.getCatalogTracker();
    FileSystem fs = fileSystemManager.getFileSystem();
    Path rootDir = fileSystemManager.getRootDir();
    byte[] tableName = hTableDescriptor.getName();
    Path tableDir = HTableDescriptor.getTableDir(rootDir, tableName);

    try {
      // 1. Update descriptor
      this.masterServices.getTableDescriptors().add(hTableDescriptor);

      // 2. Execute the on-disk Restore
      LOG.debug("Starting restore snapshot=" + SnapshotDescriptionUtils.toString(snapshot));
      Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
      RestoreSnapshotHelper restoreHelper = new RestoreSnapshotHelper(
          masterServices.getConfiguration(), fs,
          snapshot, snapshotDir, hTableDescriptor, tableDir, monitor);
      RestoreSnapshotHelper.RestoreMetaChanges metaChanges = restoreHelper.restoreHdfsRegions();

      // 3. Applies changes to .META.
      hris.clear();
      if (metaChanges.hasRegionsToAdd()) hris.addAll(metaChanges.getRegionsToAdd());
      if (metaChanges.hasRegionsToRestore()) hris.addAll(metaChanges.getRegionsToRestore());
      List<HRegionInfo> hrisToRemove = metaChanges.getRegionsToRemove();
      MetaEditor.mutateRegions(catalogTracker, hrisToRemove, hris);

      // At this point the restore is complete. Next step is enabling the table.
      LOG.info("Restore snapshot=" + SnapshotDescriptionUtils.toString(snapshot) + " on table=" +
        Bytes.toString(tableName) + " completed!");
    } catch (IOException e) {
      String msg = "restore snapshot=" + SnapshotDescriptionUtils.toString(snapshot)
          + " failed. Try re-running the restore command.";
      LOG.error(msg, e);
      monitor.receive(new ForeignException(masterServices.getServerName().toString(), e));
      throw new RestoreSnapshotException(msg, e);
    } finally {
      this.stopped = true;
    }
  }

  @Override
  public boolean isFinished() {
    return this.stopped;
  }

  @Override
  public SnapshotDescription getSnapshot() {
    return snapshot;
  }

  @Override
  public void cancel(String why) {
    if (this.stopped) return;
    this.stopped = true;
    String msg = "Stopping restore snapshot=" + SnapshotDescriptionUtils.toString(snapshot)
        + " because: " + why;
    LOG.info(msg);
    CancellationException ce = new CancellationException(why);
    this.monitor.receive(new ForeignException(masterServices.getServerName().toString(), ce));
  }

  public ForeignException getExceptionIfFailed() {
    return this.monitor.getException();
  }
}
