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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MetricsSnapshot;
import org.apache.hadoop.hbase.master.SnapshotSentinel;
import org.apache.hadoop.hbase.master.handler.CreateTableHandler;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;

import com.google.common.base.Preconditions;

/**
 * Handler to Clone a snapshot.
 *
 * <p>Uses {@link RestoreSnapshotHelper} to create a new table with the same
 * content of the specified snapshot.
 */
@InterfaceAudience.Private
public class CloneSnapshotHandler extends CreateTableHandler implements SnapshotSentinel {
  private static final Log LOG = LogFactory.getLog(CloneSnapshotHandler.class);

  private final static String NAME = "Master CloneSnapshotHandler";

  private final SnapshotDescription snapshot;

  private final ForeignExceptionDispatcher monitor;
  private final MetricsSnapshot metricsSnapshot = new MetricsSnapshot();
  private final MonitoredTask status;

  private RestoreSnapshotHelper.RestoreMetaChanges metaChanges;

  private volatile boolean stopped = false;

  public CloneSnapshotHandler(final MasterServices masterServices,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor) {
    super(masterServices, masterServices.getMasterFileSystem(), hTableDescriptor,
      masterServices.getConfiguration(), null, masterServices);

    // Snapshot information
    this.snapshot = snapshot;

    // Monitor
    this.monitor = new ForeignExceptionDispatcher();
    this.status = TaskMonitor.get().createStatus("Cloning  snapshot '" + snapshot.getName() +
      "' to table " + hTableDescriptor.getTableName());
  }

  @Override
  public CloneSnapshotHandler prepare() throws NotAllMetaRegionsOnlineException,
      TableExistsException, IOException {
    return (CloneSnapshotHandler) super.prepare();
  }

  /**
   * Create the on-disk regions, using the tableRootDir provided by the CreateTableHandler.
   * The cloned table will be created in a temp directory, and then the CreateTableHandler
   * will be responsible to add the regions returned by this method to hbase:meta and do the assignment.
   */
  @Override
  protected List<HRegionInfo> handleCreateHdfsRegions(final Path tableRootDir,
      final TableName tableName) throws IOException {
    status.setStatus("Creating regions for table: " + tableName);
    FileSystem fs = fileSystemManager.getFileSystem();
    Path rootDir = fileSystemManager.getRootDir();

    try {
      // 1. Execute the on-disk Clone
      Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
      SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshot);
      RestoreSnapshotHelper restoreHelper = new RestoreSnapshotHelper(conf, fs,
          manifest, hTableDescriptor, tableRootDir, monitor, status);
      metaChanges = restoreHelper.restoreHdfsRegions();

      // Clone operation should not have stuff to restore or remove
      Preconditions.checkArgument(!metaChanges.hasRegionsToRestore(),
          "A clone should not have regions to restore");
      Preconditions.checkArgument(!metaChanges.hasRegionsToRemove(),
          "A clone should not have regions to remove");

      // At this point the clone is complete. Next step is enabling the table.
      String msg = "Clone snapshot="+ snapshot.getName() +" on table=" + tableName + " completed!";
      LOG.info(msg);
      status.setStatus(msg + " Waiting for table to be enabled...");

      // 2. let the CreateTableHandler add the regions to meta
      return metaChanges.getRegionsToAdd();
    } catch (Exception e) {
      String msg = "clone snapshot=" + ClientSnapshotDescriptionUtils.toString(snapshot) +
        " failed because " + e.getMessage();
      LOG.error(msg, e);
      IOException rse = new RestoreSnapshotException(msg, e, snapshot);

      // these handlers aren't futures so we need to register the error here.
      this.monitor.receive(new ForeignException(NAME, rse));
      throw rse;
    }
  }

  @Override
  protected void addRegionsToMeta(final List<HRegionInfo> regionInfos,
      int regionReplication)
      throws IOException {
    super.addRegionsToMeta(regionInfos, regionReplication);
    metaChanges.updateMetaParentRegions(this.server.getConnection(), regionInfos);
  }

  @Override
  protected void completed(final Throwable exception) {
    this.stopped = true;
    if (exception != null) {
      status.abort("Snapshot '" + snapshot.getName() + "' clone failed because " +
          exception.getMessage());
    } else {
      status.markComplete("Snapshot '"+ snapshot.getName() +"' clone completed and table enabled!");
    }
    metricsSnapshot.addSnapshotClone(status.getCompletionTimestamp() - status.getStartTime());
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
    String msg = "Stopping clone snapshot=" + snapshot + " because: " + why;
    LOG.info(msg);
    status.abort(msg);
    this.monitor.receive(new ForeignException(NAME, new CancellationException(why)));
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
