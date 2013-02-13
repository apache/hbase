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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionSnare;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.SnapshotSentinel;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.TableInfoCopyTask;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.KeeperException;

/**
 * A handler for taking snapshots from the master.
 *
 * This is not a subclass of TableEventHandler because using that would incur an extra META scan.
 */
@InterfaceAudience.Private
public abstract class TakeSnapshotHandler extends EventHandler implements SnapshotSentinel,
    ForeignExceptionSnare {
  private static final Log LOG = LogFactory.getLog(TakeSnapshotHandler.class);

  private volatile boolean finished;

  // none of these should ever be null
  protected final MasterServices master;
  protected final SnapshotDescription snapshot;
  protected final Configuration conf;
  protected final FileSystem fs;
  protected final Path rootDir;
  private final Path snapshotDir;
  protected final Path workingDir;
  private final MasterSnapshotVerifier verifier;
  protected final ForeignExceptionDispatcher monitor;

  /**
   * @param snapshot descriptor of the snapshot to take
   * @param masterServices master services provider
   * @throws IOException on unexpected error
   */
  public TakeSnapshotHandler(SnapshotDescription snapshot,
      final MasterServices masterServices) throws IOException {
    super(masterServices, EventType.C_M_SNAPSHOT_TABLE);
    assert snapshot != null : "SnapshotDescription must not be nul1";
    assert masterServices != null : "MasterServices must not be nul1";

    this.master = masterServices;
    this.snapshot = snapshot;
    this.conf = this.master.getConfiguration();
    this.fs = this.master.getMasterFileSystem().getFileSystem();
    this.rootDir = this.master.getMasterFileSystem().getRootDir();
    this.snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
    this.workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir);
    this.monitor =  new ForeignExceptionDispatcher();

    // prepare the verify
    this.verifier = new MasterSnapshotVerifier(masterServices, snapshot, rootDir);
  }

  private HTableDescriptor loadTableDescriptor()
      throws FileNotFoundException, IOException {
    final String name = snapshot.getTable();
    HTableDescriptor htd =
      this.master.getTableDescriptors().get(name);
    if (htd == null) {
      throw new IOException("HTableDescriptor missing for " + name);
    }
    return htd;
  }

  /**
   * Execute the core common portions of taking a snapshot.  the {@link #snapshotRegions(List)}
   * call should get implemented for each snapshot flavor.
   */
  @Override
  public void process() {
    LOG.info("Running table snapshot operation " + eventType + " on table " + snapshot.getTable());
    try {
      loadTableDescriptor(); // check that .tableinfo is present

      byte[] ssbytes = Bytes.toBytes(snapshot.getTable());
      List<Pair<HRegionInfo, ServerName>> regionsAndLocations = MetaReader.getTableRegionsAndLocations(
        this.server.getCatalogTracker(), ssbytes, true);

      // If regions move after this meta scan, the region specific snapshot should fail, triggering
      // an external exception that gets captured here.

      // write down the snapshot info in the working directory
      SnapshotDescriptionUtils.writeSnapshotInfo(snapshot, workingDir, this.fs);
      new TableInfoCopyTask(monitor, snapshot, fs, rootDir).call();
      monitor.rethrowException();

      // run the snapshot
      snapshotRegions(regionsAndLocations);

      // extract each pair to separate lists
      Set<String> serverNames = new HashSet<String>();
      for (Pair<HRegionInfo, ServerName> p : regionsAndLocations) {
        serverNames.add(p.getSecond().toString());
      }

      // verify the snapshot is valid
      verifier.verifySnapshot(this.workingDir, serverNames);

      // complete the snapshot, atomically moving from tmp to .snapshot dir.
      completeSnapshot(this.snapshotDir, this.workingDir, this.fs);
    } catch (Exception e) {
      String reason = "Failed taking snapshot " + SnapshotDescriptionUtils.toString(snapshot)
          + " due to exception:" + e.getMessage();
      LOG.error(reason, e);
      ForeignException ee = new ForeignException(reason, e);
      monitor.receive(ee);
      // need to mark this completed to close off and allow cleanup to happen.
      cancel("Failed to take snapshot '" + SnapshotDescriptionUtils.toString(snapshot)
          + "' due to exception");
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
   * Snapshot the specified regions
   */
  protected abstract void snapshotRegions(List<Pair<HRegionInfo, ServerName>> regions)
      throws IOException, KeeperException;

  @Override
  public void cancel(String why) {
    if (finished) return;

    this.finished = true;
    LOG.info("Stop taking snapshot=" + SnapshotDescriptionUtils.toString(snapshot) + " because: "
        + why);
    CancellationException ce = new CancellationException(why);
    monitor.receive(new ForeignException(master.getServerName().toString(), ce));
  }

  @Override
  public boolean isFinished() {
    return finished;
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
