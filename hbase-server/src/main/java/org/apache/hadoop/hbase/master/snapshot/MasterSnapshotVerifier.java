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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.CorruptedSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.snapshot.TakeSnapshotUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSVisitor;
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
  private static final Log LOG = LogFactory.getLog(MasterSnapshotVerifier.class);

  private SnapshotDescription snapshot;
  private FileSystem fs;
  private Path rootDir;
  private TableName tableName;
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
    this.tableName = TableName.valueOf(snapshot.getTable());
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
    FSTableDescriptors.getTableDescriptorFromFs(fs, snapshotDir);
  }

  /**
   * Check that all the regions in the snapshot are valid, and accounted for.
   * @param snapshotDir snapshot directory to check
   * @throws IOException if we can't reach hbase:meta or read the files from the FS
   */
  private void verifyRegions(Path snapshotDir) throws IOException {
    List<HRegionInfo> regions = MetaReader.getTableRegions(this.services.getCatalogTracker(),
        tableName);

    Set<String> snapshotRegions = SnapshotReferenceUtil.getSnapshotRegionNames(fs, snapshotDir);
    if (snapshotRegions == null) {
      String msg = "Snapshot " + ClientSnapshotDescriptionUtils.toString(snapshot) + " looks empty";
      LOG.error(msg);
      throw new CorruptedSnapshotException(msg);
    }

    if (snapshotRegions.size() != regions.size()) {
      String msg = "Regions moved during the snapshot '" + 
                   ClientSnapshotDescriptionUtils.toString(snapshot) + "'. expected=" +
                   regions.size() + " snapshotted=" + snapshotRegions.size();
      LOG.error(msg);
      throw new CorruptedSnapshotException(msg);
    }

    for (HRegionInfo region : regions) {
      if (!snapshotRegions.contains(region.getEncodedName())) {
        // could happen due to a move or split race.
        String msg = "No region directory found for region:" + region;
        LOG.error(msg);
        throw new CorruptedSnapshotException(msg, snapshot);
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
  private void verifyRegion(final FileSystem fs, final Path snapshotDir, final HRegionInfo region)
      throws IOException {
    // make sure we have region in the snapshot
    Path regionDir = new Path(snapshotDir, region.getEncodedName());

    // make sure we have the region info in the snapshot
    Path regionInfo = new Path(regionDir, HRegionFileSystem.REGION_INFO_FILE);
    // make sure the file exists
    if (!fs.exists(regionInfo)) {
      throw new CorruptedSnapshotException("No region info found for region:" + region, snapshot);
    }

    HRegionInfo found = HRegionFileSystem.loadRegionInfoFileContent(fs, regionDir);
    if (!region.equals(found)) {
      throw new CorruptedSnapshotException("Found region info (" + found
        + ") doesn't match expected region:" + region, snapshot);
    }

    // make sure we have the expected recovered edits files
    TakeSnapshotUtils.verifyRecoveredEdits(fs, snapshotDir, found, snapshot);

     // make sure we have all the expected store files
    SnapshotReferenceUtil.visitRegionStoreFiles(fs, regionDir, new FSVisitor.StoreFileVisitor() {
      public void storeFile(final String regionNameSuffix, final String family,
          final String hfileName) throws IOException {
        verifyStoreFile(snapshotDir, region, family, hfileName);
      }
    });
  }

  private void verifyStoreFile(final Path snapshotDir, final HRegionInfo regionInfo,
      final String family, final String fileName) throws IOException {
    Path refPath = null;
    if (StoreFileInfo.isReference(fileName)) {
      // If is a reference file check if the parent file is present in the snapshot
      Path snapshotHFilePath = new Path(new Path(
          new Path(snapshotDir, regionInfo.getEncodedName()), family), fileName);
      refPath = StoreFileInfo.getReferredToFile(snapshotHFilePath);
      if (!fs.exists(refPath)) {
        throw new CorruptedSnapshotException("Missing parent hfile for: " + fileName, snapshot);
      }
    }

    Path linkPath;
    if (refPath != null && HFileLink.isHFileLink(refPath)) {
      linkPath = new Path(family, refPath.getName());
    } else if (HFileLink.isHFileLink(fileName)) {
      linkPath = new Path(family, fileName);
    } else {
      linkPath = new Path(family, HFileLink.createHFileLinkName(tableName,
        regionInfo.getEncodedName(), fileName));
    }

    // check if the linked file exists (in the archive, or in the table dir)
    HFileLink link = new HFileLink(services.getConfiguration(), linkPath);
    if (!link.exists(fs)) {
      throw new CorruptedSnapshotException("Can't find hfile: " + fileName
          + " in the real (" + link.getOriginPath() + ") or archive (" + link.getArchivePath()
          + ") directory for the primary table.", snapshot);
    }
  }
}
