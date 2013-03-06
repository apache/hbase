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

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription.Type;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.snapshot.CorruptedSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.TakeSnapshotUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;

/**
 * General snapshot verification on the master.
 * <p>
 * This is a light-weight verification mechanism for all the files in a snapshot. It doesn't
 * attempt to verify that the files are exact copies (that would be paramount to taking the
 * snapshot again!), but instead just attempts to ensure that the files match the expected
 * files and are the same length.
 * <p>
 * Taking an online snapshots can race against other operations and this is an last line of
 * defense.  For example, if meta changes between when snapshots are taken not all regions of a
 * table may be present.  This can be caused by a region split (daughters present on this scan,
 * but snapshot took parent), or move (snapshots only checks lists of region servers, a move could
 * have caused a region to be skipped or done twice).
 * <p>
 * Current snapshot files checked:
 * <ol>
 * <li>SnapshotDescription is readable</li>
 * <li>Table info is readable</li>
 * <li>Regions</li>
 * <ul>
 * <li>Matching regions in the snapshot as currently in the table</li>
 * <li>{@link HRegionInfo} matches the current and stored regions</li>
 * <li>All referenced hfiles have valid names</li>
 * <li>All the hfiles are present (either in .archive directory in the region)</li>
 * <li>All recovered.edits files are present (by name) and have the correct file size</li>
 * </ul>
 * </ol>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class MasterSnapshotVerifier {

  private SnapshotDescription snapshot;
  private FileSystem fs;
  private Path rootDir;
  private String tableName;
  private MasterServices services;

  /**
   * @param services services for the master
   * @param snapshot snapshot to check
   * @param rootDir root directory of the hbase installation.
   */
  public MasterSnapshotVerifier(MasterServices services, SnapshotDescription snapshot, Path rootDir) {
    this.fs = services.getMasterFileSystem().getFileSystem();
    this.services = services;
    this.snapshot = snapshot;
    this.rootDir = rootDir;
    this.tableName = snapshot.getTable();
  }

  /**
   * Verify that the snapshot in the directory is a valid snapshot
   * @param snapshotDir snapshot directory to check
   * @param snapshotServers {@link ServerName} of the servers that are involved in the snapshot
   * @throws CorruptedSnapshotException if the snapshot is invalid
   * @throws IOException if there is an unexpected connection issue to the filesystem
   */
  public void verifySnapshot(Path snapshotDir, Set<String> snapshotServers)
      throws CorruptedSnapshotException, IOException {
    // verify snapshot info matches
    verifySnapshotDescription(snapshotDir);

    // check that tableinfo is a valid table description
    verifyTableInfo(snapshotDir);

    // check that each region is valid
    verifyRegions(snapshotDir);
  }

  /**
   * Check that the snapshot description written in the filesystem matches the current snapshot
   * @param snapshotDir snapshot directory to check
   */
  private void verifySnapshotDescription(Path snapshotDir) throws CorruptedSnapshotException {
    SnapshotDescription found = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    if (!this.snapshot.equals(found)) {
      throw new CorruptedSnapshotException("Snapshot read (" + found
          + ") doesn't equal snapshot we ran (" + snapshot + ").", snapshot);
    }
  }

  /**
   * Check that the table descriptor for the snapshot is a valid table descriptor
   * @param snapshotDir snapshot directory to check
   */
  private void verifyTableInfo(Path snapshotDir) throws IOException {
    FSTableDescriptors.getTableDescriptor(fs, snapshotDir);
  }

  /**
   * Check that all the regions in the snapshot are valid, and accounted for.
   * @param snapshotDir snapshot directory to check
   * @throws IOException if we can't reach .META. or read the files from the FS
   */
  private void verifyRegions(Path snapshotDir) throws IOException {
    List<HRegionInfo> regions = MetaReader.getTableRegions(this.services.getCatalogTracker(),
      Bytes.toBytes(tableName));
    for (HRegionInfo region : regions) {
      // if offline split parent, skip it
      if (region.isOffline() && (region.isSplit() || region.isSplitParent())) {
        continue;
      }

      verifyRegion(fs, snapshotDir, region);
    }
  }

  /**
   * Verify that the region (regioninfo, hfiles) are valid
   * @param fs the FileSystem instance
   * @param snapshotDir snapshot directory to check
   * @param region the region to check
   */
  private void verifyRegion(FileSystem fs, Path snapshotDir, HRegionInfo region) throws IOException {
    // make sure we have region in the snapshot
    Path regionDir = new Path(snapshotDir, region.getEncodedName());
    if (!fs.exists(regionDir)) {
      // could happen due to a move or split race.
      throw new CorruptedSnapshotException("No region directory found for region:" + region,
          snapshot);
    }
    // make sure we have the region info in the snapshot
    Path regionInfo = new Path(regionDir, HRegion.REGIONINFO_FILE);
    // make sure the file exists
    if (!fs.exists(regionInfo)) {
      throw new CorruptedSnapshotException("No region info found for region:" + region, snapshot);
    }
    FSDataInputStream in = fs.open(regionInfo);
    HRegionInfo found = new HRegionInfo();
    try {
      found.readFields(in);
      if (!region.equals(found)) {
        throw new CorruptedSnapshotException("Found region info (" + found
           + ") doesn't match expected region:" + region, snapshot);
      }
    } finally {
      in.close();
    }

    // make sure we have the expected recovered edits files
    TakeSnapshotUtils.verifyRecoveredEdits(fs, snapshotDir, found, snapshot);

    // check for the existance of each hfile
    PathFilter familiesDirs = new FSUtils.FamilyDirFilter(fs);
    FileStatus[] columnFamilies = FSUtils.listStatus(fs, regionDir, familiesDirs);
    // should we do some checking here to make sure the cfs are correct?
    if (columnFamilies == null) return;

    // setup the suffixes for the snapshot directories
    Path tableNameSuffix = new Path(tableName);
    Path regionNameSuffix = new Path(tableNameSuffix, region.getEncodedName());

    // get the potential real paths
    Path archivedRegion = new Path(HFileArchiveUtil.getArchivePath(services.getConfiguration()),
        regionNameSuffix);
    Path realRegion = new Path(rootDir, regionNameSuffix);

    // loop through each cf and check we can find each of the hfiles
    for (FileStatus cf : columnFamilies) {
      FileStatus[] hfiles = FSUtils.listStatus(fs, cf.getPath(), null);
      // should we check if there should be hfiles?
      if (hfiles == null || hfiles.length == 0) continue;

      Path realCfDir = new Path(realRegion, cf.getPath().getName());
      Path archivedCfDir = new Path(archivedRegion, cf.getPath().getName());
      for (FileStatus hfile : hfiles) {
        // make sure the name is correct
        if (!StoreFile.validateStoreFileName(hfile.getPath().getName())) {
          throw new CorruptedSnapshotException("HFile: " + hfile.getPath()
              + " is not a valid hfile name.", snapshot);
        }

        // check to see if hfile is present in the real table
        String fileName = hfile.getPath().getName();
        Path file = new Path(realCfDir, fileName);
        Path archived = new Path(archivedCfDir, fileName);
        if (!fs.exists(file) && !file.equals(archived)) {
          throw new CorruptedSnapshotException("Can't find hfile: " + hfile.getPath()
              + " in the real (" + realCfDir + ") or archive (" + archivedCfDir
              + ") directory for the primary table.", snapshot);
        }
      }
    }
  }

  /**
   * Check that the logs stored in the log directory for the snapshot are valid - it contains all
   * the expected logs for all servers involved in the snapshot.
   * @param snapshotDir snapshot directory to check
   * @param snapshotServers list of the names of servers involved in the snapshot.
   * @throws CorruptedSnapshotException if the hlogs in the snapshot are not correct
   * @throws IOException if we can't reach the filesystem
   */
  private void verifyLogs(Path snapshotDir, Set<String> snapshotServers)
      throws CorruptedSnapshotException, IOException {
    Path snapshotLogDir = new Path(snapshotDir, HConstants.HREGION_LOGDIR_NAME);
    Path logsDir = new Path(rootDir, HConstants.HREGION_LOGDIR_NAME);
    TakeSnapshotUtils.verifyAllLogsGotReferenced(fs, logsDir, snapshotServers, snapshot,
      snapshotLogDir);
  }
}
