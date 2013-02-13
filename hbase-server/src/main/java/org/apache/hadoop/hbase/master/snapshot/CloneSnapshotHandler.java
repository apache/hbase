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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.SnapshotSentinel;
import org.apache.hadoop.hbase.master.handler.CreateTableHandler;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotExceptionSnare;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.restore.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.exception.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.exception.RestoreSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;

/**
 * Handler to Clone a snapshot.
 *
 * <p>Uses {@link RestoreSnapshotHelper} to create a new table with the same
 * content of the specified snapshot.
 */
@InterfaceAudience.Private
public class CloneSnapshotHandler extends CreateTableHandler implements SnapshotSentinel {
  private static final Log LOG = LogFactory.getLog(CloneSnapshotHandler.class);

  private final SnapshotDescription snapshot;

  private final SnapshotExceptionSnare monitor;

  private volatile boolean stopped = false;

  public CloneSnapshotHandler(final MasterServices masterServices,
      final SnapshotDescription snapshot, final HTableDescriptor hTableDescriptor)
      throws NotAllMetaRegionsOnlineException, TableExistsException, IOException {
    super(masterServices, masterServices.getMasterFileSystem(), hTableDescriptor,
      masterServices.getConfiguration(), null, masterServices.getCatalogTracker(),
      masterServices.getAssignmentManager());

    // Snapshot information
    this.snapshot = snapshot;

    // Monitor
    this.monitor = new SnapshotExceptionSnare(snapshot);
  }

  @Override
  protected List<HRegionInfo> handleCreateRegions(String tableName) throws IOException {
    FileSystem fs = fileSystemManager.getFileSystem();
    Path rootDir = fileSystemManager.getRootDir();
    Path tableDir = HTableDescriptor.getTableDir(rootDir, Bytes.toBytes(tableName));

    try {
      // Execute the Clone
      Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
      RestoreSnapshotHelper restoreHelper = new RestoreSnapshotHelper(conf, fs,
          catalogTracker, snapshot, snapshotDir, hTableDescriptor, tableDir, monitor);
      restoreHelper.restore();

      // At this point the clone is complete. Next step is enabling the table.
      LOG.info("Clone snapshot=" + snapshot.getName() + " on table=" + tableName + " completed!");

      return MetaReader.getTableRegions(catalogTracker, Bytes.toBytes(tableName));
    } catch (Exception e) {
      String msg = "clone snapshot=" + snapshot + " failed";
      LOG.error(msg, e);
      monitor.snapshotFailure("Failed due to exception:" + e.getMessage(), snapshot, e);
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
  public void stop(String why) {
    if (this.stopped) return;
    this.stopped = true;
    LOG.info("Stopping clone snapshot=" + snapshot + " because: " + why);
    this.monitor.snapshotFailure("Failing clone snapshot because server is stopping.", snapshot);
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  @Override
  public HBaseSnapshotException getExceptionIfFailed() {
    try {
      this.monitor.failOnError();
    } catch (HBaseSnapshotException e) {
      return e;
    }
    return null;
  }
}
