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
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.SnapshotSentinel;
import org.apache.hadoop.hbase.master.handler.CreateTableHandler;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.Bytes;

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

  private volatile boolean stopped = false;

  public CloneSnapshotHandler(final MasterServices masterServices,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws NotAllMetaRegionsOnlineException, TableExistsException, IOException {
    super(masterServices, masterServices.getMasterFileSystem(), 
      masterServices.getServerManager(), hTableDescriptor,
      masterServices.getConfiguration(), null, masterServices.getCatalogTracker(),
      masterServices.getAssignmentManager());

    // Snapshot information
    this.snapshot = snapshot;

    // Monitor
    this.monitor = new ForeignExceptionDispatcher();
  }

  /**
   * Create the on-disk regions, using the tableRootDir provided by the CreateTableHandler.
   * The cloned table will be created in a temp directory, and then the CreateTableHandler
   * will be responsible to add the regions returned by this method to META and do the assignment.
   */
  @Override
  protected List<HRegionInfo> handleCreateHdfsRegions(final Path tableRootDir, final String tableName)
      throws IOException {
    FileSystem fs = fileSystemManager.getFileSystem();
    Path rootDir = fileSystemManager.getRootDir();
    Path tableDir = new Path(tableRootDir, tableName);

    try {
      // 1. Execute the on-disk Clone
      Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
      RestoreSnapshotHelper restoreHelper = new RestoreSnapshotHelper(conf, fs,
          snapshot, snapshotDir, hTableDescriptor, tableDir, monitor);
      RestoreSnapshotHelper.RestoreMetaChanges metaChanges = restoreHelper.restoreHdfsRegions();

      // Clone operation should not have stuff to restore or remove
      Preconditions.checkArgument(!metaChanges.hasRegionsToRestore(),
          "A clone should not have regions to restore");
      Preconditions.checkArgument(!metaChanges.hasRegionsToRemove(),
          "A clone should not have regions to remove");

      // At this point the clone is complete. Next step is enabling the table.
      LOG.info("Clone snapshot=" + snapshot.getName() + " on table=" + tableName + " completed!");

      // 2. let the CreateTableHandler add the regions to meta
      return metaChanges.getRegionsToAdd();
    } catch (Exception e) {
      String msg = "clone snapshot=" + SnapshotDescriptionUtils.toString(snapshot) + " failed";
      LOG.error(msg, e);
      IOException rse = new RestoreSnapshotException(msg, e, snapshot);

      // these handlers aren't futures so we need to register the error here.
      this.monitor.receive(new ForeignException(NAME, rse));
      throw rse;
    }
  }

  @Override
  protected void completed(final Throwable exception) {
    this.stopped = true;
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
    LOG.info("Stopping clone snapshot=" + snapshot + " because: " + why);
    this.monitor.receive(new ForeignException(NAME, new CancellationException(why)));
  }

  @Override
  public ForeignException getExceptionIfFailed() {
    return this.monitor.getException();
  }
}
