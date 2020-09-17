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
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.CorruptedSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;

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
@InterfaceStability.Unstable
public final class MasterSnapshotVerifier {
  private static final Logger LOG = LoggerFactory.getLogger(MasterSnapshotVerifier.class);

  private SnapshotDescription snapshot;
  private FileSystem workingDirFs;
  private TableName tableName;
  private MasterServices services;

  /**
   * @param services services for the master
   * @param snapshot snapshot to check
   * @param workingDirFs the file system containing the temporary snapshot information
   */
  public MasterSnapshotVerifier(MasterServices services,
      SnapshotDescription snapshot, FileSystem workingDirFs) {
    this.workingDirFs = workingDirFs;
    this.services = services;
    this.snapshot = snapshot;
    this.tableName = TableName.valueOf(snapshot.getTable());
  }

  /**
   * Verify that the snapshot in the directory is a valid snapshot
   * @param snapshotDir snapshot directory to check
   * @param snapshotServers {@link org.apache.hadoop.hbase.ServerName} of the servers
   *        that are involved in the snapshot
   * @throws CorruptedSnapshotException if the snapshot is invalid
   * @throws IOException if there is an unexpected connection issue to the filesystem
   */
  public void verifySnapshot(Path snapshotDir, Set<String> snapshotServers)
      throws CorruptedSnapshotException, IOException {
    SnapshotManifest manifest = SnapshotManifest.open(services.getConfiguration(), workingDirFs,
                                                      snapshotDir, snapshot);
    // verify snapshot info matches
    verifySnapshotDescription(snapshotDir);

    // check that tableinfo is a valid table description
    verifyTableInfo(manifest);

    // check that each region is valid
    verifyRegions(manifest);
  }

  /**
   * Check that the snapshot description written in the filesystem matches the current snapshot
   * @param snapshotDir snapshot directory to check
   */
  private void verifySnapshotDescription(Path snapshotDir) throws CorruptedSnapshotException {
    SnapshotDescription found = SnapshotDescriptionUtils.readSnapshotInfo(workingDirFs,
        snapshotDir);
    if (!this.snapshot.equals(found)) {
      throw new CorruptedSnapshotException(
          "Snapshot read (" + found + ") doesn't equal snapshot we ran (" + snapshot + ").",
          ProtobufUtil.createSnapshotDesc(snapshot));
    }
  }

  /**
   * Check that the table descriptor for the snapshot is a valid table descriptor
   * @param manifest snapshot manifest to inspect
   */
  private void verifyTableInfo(final SnapshotManifest manifest) throws IOException {
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
   * @throws IOException if we can't reach hbase:meta or read the files from the FS
   */
  private void verifyRegions(final SnapshotManifest manifest) throws IOException {
    List<RegionInfo> regions;
    if (TableName.META_TABLE_NAME.equals(tableName)) {
      regions = MetaTableLocator.getMetaRegions(services.getZooKeeper());
    } else {
      regions = MetaTableAccessor.getTableRegions(services.getConnection(), tableName);
    }
    // Remove the non-default regions
    RegionReplicaUtil.removeNonDefaultRegions(regions);

    Map<String, SnapshotRegionManifest> regionManifests = manifest.getRegionManifestsMap();
    if (regionManifests == null) {
      String msg = "Snapshot " + ClientSnapshotDescriptionUtils.toString(snapshot) + " looks empty";
      LOG.error(msg);
      throw new CorruptedSnapshotException(msg);
    }

    String errorMsg = "";
    boolean hasMobStore = false;
    // the mob region is a dummy region, it's not a real region in HBase.
    // the mob region has a special name, it could be found by the region name.
    if (regionManifests.get(MobUtils.getMobRegionInfo(tableName).getEncodedName()) != null) {
      hasMobStore = true;
    }
    int realRegionCount = hasMobStore ? regionManifests.size() - 1 : regionManifests.size();
    if (realRegionCount != regions.size()) {
      errorMsg = "Regions moved during the snapshot '" +
                   ClientSnapshotDescriptionUtils.toString(snapshot) + "'. expected=" +
                   regions.size() + " snapshotted=" + realRegionCount + ".";
      LOG.error(errorMsg);
    }

    // Verify RegionInfo
    for (RegionInfo region : regions) {
      SnapshotRegionManifest regionManifest = regionManifests.get(region.getEncodedName());
      if (regionManifest == null) {
        // could happen due to a move or split race.
        String mesg = " No snapshot region directory found for region:" + region;
        if (errorMsg.isEmpty()) errorMsg = mesg;
        LOG.error(mesg);
        continue;
      }

      verifyRegionInfo(region, regionManifest);
    }

    if (!errorMsg.isEmpty()) {
      throw new CorruptedSnapshotException(errorMsg);
    }

    // Verify Snapshot HFiles
    // Requires the root directory file system as HFiles are stored in the root directory
    SnapshotReferenceUtil.verifySnapshot(services.getConfiguration(),
      CommonFSUtils.getRootDirFileSystem(services.getConfiguration()), manifest);
  }

  /**
   * Verify that the regionInfo is valid
   * @param region the region to check
   * @param manifest snapshot manifest to inspect
   */
  private void verifyRegionInfo(final RegionInfo region,
      final SnapshotRegionManifest manifest) throws IOException {
    RegionInfo manifestRegionInfo = ProtobufUtil.toRegionInfo(manifest.getRegionInfo());
    if (RegionInfo.COMPARATOR.compare(region, manifestRegionInfo) != 0) {
      String msg = "Manifest region info " + manifestRegionInfo +
                   "doesn't match expected region:" + region;
      throw new CorruptedSnapshotException(msg, ProtobufUtil.createSnapshotDesc(snapshot));
    }
  }
}
