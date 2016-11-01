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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.fs.MasterStorage;
import org.apache.hadoop.hbase.fs.StorageContext;
import org.apache.hadoop.hbase.fs.StorageIdentifier;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.CorruptedSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;

/**
 * General snapshot verification on the master.
 * <p>
 * This is a light-weight verification mechanism for verifying snapshot artifacts like snapshot
 * description, table, regions and store files. It doesn't attempt to verify that the artifacts
 * are exact copies (that would be paramount to taking the snapshot again!), but instead just
 * attempts to ensure that the artifacts match the expected artifacts including the length etc.
 * <p>
 * Taking an online snapshots can race against other operations and this is an last line of
 * defense.  For example, if meta changes between when snapshots are taken not all regions of a
 * table may be present.  This can be caused by a region split (daughters present on this scan,
 * but snapshot took parent), or move (snapshots only checks lists of region servers, a move could
 * have caused a region to be skipped or done twice).
 * <p>
 * Current snapshot artifacts checked:
 * <ol>
 * <li>SnapshotDescription is readable</li>
 * <li>Table info is readable</li>
 * <li>Regions</li>
 * </ol>
 * <ul>
 * <li>Matching regions in the snapshot as currently in the table</li>
 * <li>{@link HRegionInfo} matches the current and stored regions</li>
 * <li>All referenced hfiles have valid names</li>
 * <li>All the hfiles are present (either in .archive directory in the region)</li>
 * <li>All recovered.edits files are present (by name) and have the correct file size</li>
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class MasterSnapshotVerifier {
  private static final Log LOG = LogFactory.getLog(MasterSnapshotVerifier.class);

  private SnapshotDescription snapshot;
  private TableName tableName;
  private MasterStorage<? extends StorageIdentifier> masterStorage;
  private MasterServices services;
  private StorageContext ctx;

  /**
   * @param services services for the master
   * @param snapshot snapshot to check
   */
  public MasterSnapshotVerifier(MasterServices services, SnapshotDescription snapshot,
      StorageContext ctx) {
    this.services = services;
    this.masterStorage = services.getMasterStorage();
    this.snapshot = snapshot;
    this.ctx = ctx;
    this.tableName = TableName.valueOf(snapshot.getTable());
  }

  /**
   * Verify that the snapshot persisted on a storage is a valid snapshot
   * @param ctx {@link StorageContext} for a given snapshot
   * @throws CorruptedSnapshotException if the snapshot is invalid
   * @throws IOException if there is an unexpected connection issue to the storage
   */
  public void verifySnapshot(StorageContext ctx)
      throws CorruptedSnapshotException, IOException {
    // verify snapshot info matches
    verifySnapshotDescription(ctx);

    // check that table info is a valid table description
    verifyTableInfo(ctx);

    // check that each region is valid
    verifyRegions(ctx);
  }

  /**
   * Check that the snapshot description written to storage matches the current snapshot
   * @param ctx {@link StorageContext} for a given snapshot
   * @throws CorruptedSnapshotException if verification fails
   */
  private void verifySnapshotDescription(StorageContext ctx) throws CorruptedSnapshotException {
    boolean match = false;
    SnapshotDescription found = null;
    try {
      found = masterStorage.getSnapshot(snapshot.getName(), ctx);
      match = this.snapshot.equals(found);
    } catch (IOException e) {
      LOG.warn("Failed to read snapshot '" + snapshot.getName() + "' from storage.", e);
    }
    if (!match) {
      throw new CorruptedSnapshotException(
          "Snapshot read (" + found + ") doesn't equal snapshot we ran (" + snapshot + ").",
          ProtobufUtil.createSnapshotDesc(snapshot));
    }
  }

  /**
   * Check that the table descriptor written to storage for the snapshot is valid
   * @param ctx {@link StorageContext} for a given snapshot
   * @throws IOException if fails to read table descriptor from storage
   */
  private void verifyTableInfo(StorageContext ctx) throws IOException {
    HTableDescriptor htd = masterStorage.getTableDescriptorForSnapshot(snapshot, ctx);
    if (htd == null) {
      throw new CorruptedSnapshotException("Missing Table Descriptor",
        ProtobufUtil.createSnapshotDesc(snapshot));
    }

    if (!htd.getNameAsString().equals(snapshot.getTable())) {
      throw new CorruptedSnapshotException(
          "Invalid Table Descriptor. Expected " + snapshot.getTable() + " name, got "
              + htd.getNameAsString(), ProtobufUtil.createSnapshotDesc(snapshot));
    }
  }

  /**
   * Check that all the regions in the snapshot are valid, and accounted for.
   * @param ctx {@link StorageContext} for a given snapshot
   * @throws IOException if fails to read region info for a snapshot from storage
   */
  private void verifyRegions(StorageContext ctx) throws IOException {
    List<HRegionInfo> regions;
    if (TableName.META_TABLE_NAME.equals(tableName)) {
      regions = new MetaTableLocator().getMetaRegions(services.getZooKeeper());
    } else {
      regions = MetaTableAccessor.getTableRegions(services.getConnection(), tableName);
    }
    // Remove the non-default regions
    RegionReplicaUtil.removeNonDefaultRegions(regions);

    Map<String, HRegionInfo> snapshotRegions = masterStorage.getSnapshotRegions(snapshot, ctx);
    if (snapshotRegions == null) {
      String msg = "Snapshot " + ClientSnapshotDescriptionUtils.toString(snapshot) + " looks empty";
      LOG.error(msg);
      throw new CorruptedSnapshotException(msg);
    }

    String errorMsg = "";
    boolean hasMobStore = false;
    // the mob region is a dummy region, it's not a real region in HBase.
    // the mob region has a special name, it could be found by the region name.
    if (snapshotRegions.get(MobUtils.getMobRegionInfo(tableName).getEncodedName()) != null) {
      hasMobStore = true;
    }
    int realRegionCount = hasMobStore ? snapshotRegions.size() - 1 : snapshotRegions.size();
    if (realRegionCount != regions.size()) {
      errorMsg = "Regions moved during the snapshot '" +
                   ClientSnapshotDescriptionUtils.toString(snapshot) + "'. expected=" +
                   regions.size() + " snapshotted=" + realRegionCount + ".";
      LOG.error(errorMsg);
    } else {
      // Verify HRegionInfo
      for (HRegionInfo region : regions) {
        HRegionInfo snapshotRegion = snapshotRegions.get(region.getEncodedName());
        if (snapshotRegion == null) {
          // could happen due to a move or split race.
          errorMsg = "No snapshot region directory found for region '" + region + "'";
          LOG.error(errorMsg);
          break;
        } else if (!region.equals(snapshotRegion)) {
          errorMsg = "Snapshot region info '" + snapshotRegion + "' doesn't match expected region'"
              + region + "'.";
          LOG.error(errorMsg);
          break;
        }
      }
    }

    if (!errorMsg.isEmpty()) {
      throw new CorruptedSnapshotException(errorMsg);
    }

    // Verify Snapshot HFiles
    SnapshotReferenceUtil.verifySnapshot(services.getMasterStorage(), snapshot, ctx);
  }
}
