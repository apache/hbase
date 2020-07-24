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

package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;

/**
 * A Scanner which performs a scan over snapshot files. Using this class requires copying the
 * snapshot to a temporary empty directory, which will copy the snapshot reference files into that
 * directory. Actual data files are not copied.
 *
 * <p>
 * This also allows one to run the scan from an
 * online or offline hbase cluster. The snapshot files can be exported by using the
 * org.apache.hadoop.hbase.snapshot.ExportSnapshot tool,
 * to a pure-hdfs cluster, and this scanner can be used to
 * run the scan directly over the snapshot files. The snapshot should not be deleted while there
 * are open scanners reading from snapshot files.
 *
 * <p>
 * An internal RegionScanner is used to execute the {@link Scan} obtained
 * from the user for each region in the snapshot.
 * <p>
 * HBase owns all the data and snapshot files on the filesystem. Only the HBase user can read from
 * snapshot files and data files. HBase also enforces security because all the requests are handled
 * by the server layer, and the user cannot read from the data files directly. To read from snapshot
 * files directly from the file system, the user who is running the MR job must have sufficient
 * permissions to access snapshot and reference files. This means that to run mapreduce over
 * snapshot files, the job has to be run as the HBase user or the user must have group or other
 * priviledges in the filesystem (See HBASE-8369). Note that, given other users access to read from
 * snapshot/data files will completely circumvent the access control enforced by HBase.
 * See org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat.
 */
@InterfaceAudience.Private
public class TableSnapshotScanner extends AbstractClientScanner {

  private static final Logger LOG = LoggerFactory.getLogger(TableSnapshotScanner.class);

  private Configuration conf;
  private String snapshotName;
  private FileSystem fs;
  private Path rootDir;
  private Path restoreDir;
  private Scan scan;
  private ArrayList<RegionInfo> regions;
  private TableDescriptor htd;
  private final boolean snapshotAlreadyRestored;

  private ClientSideRegionScanner currentRegionScanner  = null;
  private int currentRegion = -1;

  private int numOfCompleteRows = 0;
  /**
   * Creates a TableSnapshotScanner.
   * @param conf the configuration
   * @param restoreDir a temporary directory to copy the snapshot files into. Current user should
   *          have write permissions to this directory, and this should not be a subdirectory of
   *          rootDir. The scanner deletes the contents of the directory once the scanner is closed.
   * @param snapshotName the name of the snapshot to read from
   * @param scan a Scan representing scan parameters
   * @throws IOException in case of error
   */
  public TableSnapshotScanner(Configuration conf, Path restoreDir, String snapshotName, Scan scan)
      throws IOException {
    this(conf, CommonFSUtils.getRootDir(conf), restoreDir, snapshotName, scan);
  }

  public TableSnapshotScanner(Configuration conf, Path rootDir, Path restoreDir,
      String snapshotName, Scan scan) throws IOException {
    this(conf, rootDir, restoreDir, snapshotName, scan, false);
  }

  /**
   * Creates a TableSnapshotScanner.
   * @param conf the configuration
   * @param rootDir root directory for HBase.
   * @param restoreDir a temporary directory to copy the snapshot files into. Current user should
   *          have write permissions to this directory, and this should not be a subdirectory of
   *          rootdir. The scanner deletes the contents of the directory once the scanner is closed.
   * @param snapshotName the name of the snapshot to read from
   * @param scan a Scan representing scan parameters
   * @param snapshotAlreadyRestored true to indicate that snapshot has been restored.
   * @throws IOException in case of error
   */
  public TableSnapshotScanner(Configuration conf, Path rootDir, Path restoreDir,
      String snapshotName, Scan scan, boolean snapshotAlreadyRestored) throws IOException {
    this.conf = conf;
    this.snapshotName = snapshotName;
    this.rootDir = rootDir;
    this.scan = scan;
    this.snapshotAlreadyRestored = snapshotAlreadyRestored;
    this.fs = rootDir.getFileSystem(conf);

    if (snapshotAlreadyRestored) {
      this.restoreDir = restoreDir;
      openWithoutRestoringSnapshot();
    } else {
      // restoreDir will be deleted in close(), use a unique sub directory
      this.restoreDir = new Path(restoreDir, UUID.randomUUID().toString());
      openWithRestoringSnapshot();
    }

    initScanMetrics(scan);
  }

  private void openWithoutRestoringSnapshot() throws IOException {
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    SnapshotProtos.SnapshotDescription snapshotDesc =
        SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);

    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);
    List<SnapshotRegionManifest> regionManifests = manifest.getRegionManifests();
    if (regionManifests == null) {
      throw new IllegalArgumentException("Snapshot seems empty, snapshotName: " + snapshotName);
    }

    regions = new ArrayList<>(regionManifests.size());
    regionManifests.stream().map(r -> HRegionInfo.convert(r.getRegionInfo()))
        .filter(this::isValidRegion).sorted().forEach(r -> regions.add(r));
    htd = manifest.getTableDescriptor();
  }

  private boolean isValidRegion(RegionInfo hri) {
    // An offline split parent region should be excluded.
    if (hri.isOffline() && (hri.isSplit() || hri.isSplitParent())) {
      return false;
    }
    return PrivateCellUtil.overlappingKeys(scan.getStartRow(), scan.getStopRow(), hri.getStartKey(),
      hri.getEndKey());
  }

  private void openWithRestoringSnapshot() throws IOException {
    final RestoreSnapshotHelper.RestoreMetaChanges meta =
        RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotName);
    final List<RegionInfo> restoredRegions = meta.getRegionsToAdd();

    htd = meta.getTableDescriptor();
    regions = new ArrayList<>(restoredRegions.size());
    restoredRegions.stream().filter(this::isValidRegion).sorted().forEach(r -> regions.add(r));
  }

  @Override
  public Result next() throws IOException {
    Result result = null;
    while (true) {
      if (currentRegionScanner == null) {
        currentRegion++;
        if (currentRegion >= regions.size()) {
          return null;
        }

        RegionInfo hri = regions.get(currentRegion);
        currentRegionScanner = new ClientSideRegionScanner(conf, fs,
          restoreDir, htd, hri, scan, scanMetrics);
        if (this.scanMetrics != null) {
          this.scanMetrics.countOfRegions.incrementAndGet();
        }
      }

      try {
        result = currentRegionScanner.next();
        if (result != null) {
          if (scan.getLimit() > 0 && ++this.numOfCompleteRows > scan.getLimit()) {
            result = null;
          }
          return result;
        }
      } finally {
        if (result == null) {
          currentRegionScanner.close();
          currentRegionScanner = null;
        }
      }
    }
  }

  private void cleanup() {
    try {
      if (fs.exists(this.restoreDir)) {
        if (!fs.delete(this.restoreDir, true)) {
          LOG.warn(
            "Delete restore directory for the snapshot failed. restoreDir: " + this.restoreDir);
        }
      }
    } catch (IOException ex) {
      LOG.warn(
        "Could not delete restore directory for the snapshot. restoreDir: " + this.restoreDir, ex);
    }
  }

  @Override
  public void close() {
    if (currentRegionScanner != null) {
      currentRegionScanner.close();
    }
    // if snapshotAlreadyRestored is true, then we should invoke cleanup() method by hand.
    if (!this.snapshotAlreadyRestored) {
      cleanup();
    }
  }

  @Override
  public boolean renewLease() {
    throw new UnsupportedOperationException();
  }

}
