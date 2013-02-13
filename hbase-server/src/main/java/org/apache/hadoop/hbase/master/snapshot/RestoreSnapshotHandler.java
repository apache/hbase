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
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.SnapshotSentinel;
import org.apache.hadoop.hbase.master.handler.TableEventHandler;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.exception.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.restore.RestoreSnapshotHelper;
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

  @Override
  protected void handleTableOperation(List<HRegionInfo> hris) throws IOException {
    MasterFileSystem fileSystemManager = masterServices.getMasterFileSystem();
    CatalogTracker catalogTracker = masterServices.getCatalogTracker();
    FileSystem fs = fileSystemManager.getFileSystem();
    Path rootDir = fileSystemManager.getRootDir();
    byte[] tableName = hTableDescriptor.getName();
    Path tableDir = HTableDescriptor.getTableDir(rootDir, tableName);

    try {
      // Update descriptor
      this.masterServices.getTableDescriptors().add(hTableDescriptor);

      // Execute the Restore
      LOG.debug("Starting restore snapshot=" + snapshot);
      Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
      RestoreSnapshotHelper restoreHelper = new RestoreSnapshotHelper(
          masterServices.getConfiguration(), fs, catalogTracker,
          snapshot, snapshotDir, hTableDescriptor, tableDir, monitor);
      restoreHelper.restore();

      // At this point the restore is complete. Next step is enabling the table.
      LOG.info("Restore snapshot=" + snapshot.getName() + " on table=" +
        Bytes.toString(tableName) + " completed!");

      hris.clear();
      hris.addAll(MetaReader.getTableRegions(catalogTracker, tableName));
    } catch (IOException e) {
      String msg = "restore snapshot=" + snapshot + " failed";
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
    String msg = "Stopping restore snapshot=" + snapshot + " because: " + why;
    LOG.info(msg);
    CancellationException ce = new CancellationException(why);
    this.monitor.receive(new ForeignException(masterServices.getServerName().toString(), ce));
  }

  public ForeignException getExceptionIfFailed() {
    return this.monitor.getException();
  }
}
