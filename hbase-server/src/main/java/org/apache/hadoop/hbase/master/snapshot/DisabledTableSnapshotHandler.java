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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.KeeperException;

/**
 * Take a snapshot of a disabled table.
 * <p>
 * Table must exist when taking the snapshot, or results are undefined.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DisabledTableSnapshotHandler extends TakeSnapshotHandler {
  private static final Log LOG = LogFactory.getLog(DisabledTableSnapshotHandler.class);

  /**
   * @param snapshot descriptor of the snapshot to take
   * @param masterServices master services provider
   */
  public DisabledTableSnapshotHandler(SnapshotDescription snapshot,
      final MasterServices masterServices, final SnapshotManager snapshotManager) {
    super(snapshot, masterServices, snapshotManager);
  }

  @Override
  public DisabledTableSnapshotHandler prepare() throws Exception {
    return (DisabledTableSnapshotHandler) super.prepare();
  }

  // TODO consider parallelizing these operations since they are independent. Right now its just
  // easier to keep them serial though
  @Override
  public void snapshotRegions(List<Pair<HRegionInfo, ServerName>> regionsAndLocations)
      throws IOException, KeeperException {
    try {
      // 1. get all the regions hosting this table.

      // extract each pair to separate lists
      Set<HRegionInfo> regions = new HashSet<HRegionInfo>();
      for (Pair<HRegionInfo, ServerName> p : regionsAndLocations) {
        // Don't include non-default regions
        HRegionInfo hri = p.getFirst();
        if (RegionReplicaUtil.isDefaultReplica(hri)) {
          regions.add(hri);
        }
        // if it's the first region, add the mob region
        if (Bytes.equals(hri.getStartKey(), HConstants.EMPTY_START_ROW)) {
          HRegionInfo mobRegion = MobUtils.getMobRegionInfo(hri.getTable());
          regions.add(mobRegion);
        }
      }

      // 2. for each region, write all the info to disk
      String msg = "Starting to write region info and WALs for regions for offline snapshot:"
          + ClientSnapshotDescriptionUtils.toString(snapshot);
      LOG.info(msg);
      status.setStatus(msg);

      ThreadPoolExecutor exec = SnapshotManifest.createExecutor(conf, "DisabledTableSnapshot");
      try {
        ModifyRegionUtils.editRegions(exec, regions, new ModifyRegionUtils.RegionEditTask() {
          @Override
          public void editRegion(final HRegionInfo regionInfo) throws IOException {
            snapshotManifest.addRegion(FSUtils.getTableDir(rootDir, snapshotTable), regionInfo);
          }
        });
      } finally {
        exec.shutdown();
      }
    } catch (Exception e) {
      // make sure we capture the exception to propagate back to the client later
      String reason = "Failed snapshot " + ClientSnapshotDescriptionUtils.toString(snapshot)
          + " due to exception:" + e.getMessage();
      ForeignException ee = new ForeignException(reason, e);
      monitor.receive(ee);
      status.abort("Snapshot of table: "+ snapshotTable + " failed because " + e.getMessage());
    } finally {
      LOG.debug("Marking snapshot" + ClientSnapshotDescriptionUtils.toString(snapshot)
          + " as finished.");
    }
  }
}
