/*
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

package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;

/**
 * General snapshot verification.
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
 * </ol>
 * <ul>
 * <li>Matching regions in the snapshot as currently in the table</li>
 * <li>{@link RegionInfo} matches the current and stored regions</li>
 * <li>All referenced hfiles have valid names</li>
 * <li>All the hfiles are present (either in .archive directory in the region)</li>
 * <li>All recovered.edits files are present (by name) and have the correct file size</li>
 * </ul>
 */
@InterfaceAudience.Private
public final class SnapshotVerifyUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotVerifyUtil.class);

  private SnapshotVerifyUtil() {
  }

  /**
   *  Check that the snapshot description written in the filesystem matches the current snapshot
   * @param conf configuration of service
   * @param snapshot the snapshot need to be verified
   * @param tableName the table of snapshot
   * @param regions the regions whose region info and store files need to be verified. If we use
   *                master to verify snapshot, this will be the whole regions of table. If we use
   *                SnapshotVerifyProcedure to verify snapshot, this will be part of the whole
   *                regions.
   * @param expectedNumRegion total num region of table taking snapshot, both include online
   *                          regions and offline regions
   */
  public static void verifySnapshot(Configuration conf, SnapshotDescription snapshot,
    TableName tableName, List<RegionInfo> regions, int expectedNumRegion) throws IOException {
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir, conf);
    FileSystem workingDirFS = workingDir.getFileSystem(conf);
    SnapshotManifest manifest = SnapshotManifest.open(conf, workingDirFS, workingDir, snapshot);

    // verify snapshot info matches
    verifySnapshotDescription(workingDirFS, workingDir, snapshot);

    // check that tableinfo is a valid table description
    verifyTableInfo(manifest, snapshot);

    // check that each region is valid
    verifyRegions(manifest, regions, snapshot, tableName, expectedNumRegion);

    // check that each store file is valid
    verifyStoreFiles(conf, manifest, regions, CommonFSUtils.getRootDirFileSystem(conf),
      snapshot, workingDir);
  }

  /**
   * Check that the snapshot description written in the filesystem matches the current snapshot
   * @param snapshotDir snapshot directory to check
   */
  private static void verifySnapshotDescription(FileSystem fs, Path snapshotDir,
    SnapshotDescription snapshot) throws CorruptedSnapshotException {
    SnapshotDescription
      found = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    if (!snapshot.equals(found)) {
      throw new CorruptedSnapshotException(
        "Snapshot read (" + found + ") doesn't equal snapshot we ran (" + snapshot + ").",
        ProtobufUtil.createSnapshotDesc(snapshot));
    }
  }

  /**
   * Check that the table descriptor for the snapshot is a valid table descriptor
   * @param manifest snapshot manifest to inspect
   */
  private static void verifyTableInfo(SnapshotManifest manifest,
    SnapshotDescription snapshot) throws IOException {
    TableDescriptor htd = manifest.getTableDescriptor();
    if (htd == null) {
      throw new CorruptedSnapshotException("Missing Table Descriptor",
        ProtobufUtil.createSnapshotDesc(snapshot));
    }

    if (!htd.getTableName().getNameAsString().equals(snapshot.getTable())) {
      throw new CorruptedSnapshotException(
        "Invalid Table Descriptor. Expected " + snapshot.getTable() + " name, got "
          + htd.getTableName().getNameAsString(), ProtobufUtil.createSnapshotDesc(snapshot));
    }
  }

  /**
   * Check that all the regions in the snapshot are valid, and accounted for.
   * @param manifest snapshot manifest to inspect
   */
  private static void verifyRegions(SnapshotManifest manifest, List<RegionInfo> regions,
    SnapshotDescription snapshot, TableName tableName, int expectedNumRegion)
    throws CorruptedSnapshotException {
    Map<String, SnapshotRegionManifest> regionManifests = manifest.getRegionManifestsMap();
    if (regionManifests == null) {
      throw new CorruptedSnapshotException("Snapshot " +
        ClientSnapshotDescriptionUtils.toString(snapshot) + " looks empty");
    }

    // Verify Region Count
    int realRegionCount = regionManifests.size();
    if (regionManifests.get(MobUtils.getMobRegionInfo(tableName).getEncodedName()) != null) {
      // the mob region is a dummy region, it's not a real region in HBase.
      // the mob region has a special name, it could be found by the region name.
      realRegionCount --;
    }
    if (realRegionCount != expectedNumRegion) {
      throw new CorruptedSnapshotException("number of region didn't match for snapshot '" +
        ClientSnapshotDescriptionUtils.toString(snapshot) + "', expected=" +
        expectedNumRegion + ", snapshotted=" + realRegionCount);
    }

    // Verify RegionInfo
    for (RegionInfo region : regions) {
      SnapshotRegionManifest regionManifest = regionManifests.get(region.getEncodedName());
      if (regionManifest == null) {
        LOG.warn("No snapshot region directory found for {}", region.getRegionNameAsString());
        continue;
      }
      verifyRegionInfo(region, snapshot, regionManifest);
    }
  }

  /**
   * Verify that the regionInfo is valid
   * @param region the region to check
   * @param manifest snapshot manifest to inspect
   */
  private static void verifyRegionInfo(final RegionInfo region, final SnapshotDescription snapshot,
    final SnapshotRegionManifest manifest) throws CorruptedSnapshotException {
    RegionInfo manifestRegionInfo = ProtobufUtil.toRegionInfo(manifest.getRegionInfo());
    if (RegionInfo.COMPARATOR.compare(region, manifestRegionInfo) != 0) {
      String msg = "Manifest region info " + manifestRegionInfo +
        "doesn't match expected region:" + region;
      throw new CorruptedSnapshotException(msg, ProtobufUtil.createSnapshotDesc(snapshot));
    }
  }

  /**
   *  Verify that store files are valid
   */
  private static void verifyStoreFiles(final Configuration conf, final SnapshotManifest manifest,
    final List<RegionInfo> regions, final FileSystem fs, final SnapshotDescription snapshot,
    final Path snapshotDir) throws IOException {
    // Verify Snapshot HFiles
    // Requires the root directory file system as HFiles are stored in the root directory
    SnapshotReferenceUtil.verifySnapshot(conf, CommonFSUtils.getRootDirFileSystem(conf), manifest,
      new SnapshotReferenceUtil.StoreFileVisitor() {
        @Override
        public void storeFile(RegionInfo regionInfo, String familyName,
          SnapshotRegionManifest.StoreFile storeFile) throws IOException {
          if (regions.contains(regionInfo)) {
            SnapshotReferenceUtil.verifyStoreFile(conf, fs, snapshotDir, snapshot,
              regionInfo, familyName, storeFile);
          }
        }
      });
  }
}
