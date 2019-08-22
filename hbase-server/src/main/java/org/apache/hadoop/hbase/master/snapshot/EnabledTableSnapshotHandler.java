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

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.procedure.Procedure;
import org.apache.hadoop.hbase.procedure.ProcedureCoordinator;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

/**
 * Handle the master side of taking a snapshot of an online table, regardless of snapshot type.
 * Uses a {@link Procedure} to run the snapshot across all the involved region servers.
 * @see ProcedureCoordinator
 */
@InterfaceAudience.Private
public class EnabledTableSnapshotHandler extends TakeSnapshotHandler {

  private static final Logger LOG = LoggerFactory.getLogger(EnabledTableSnapshotHandler.class);
  private final ProcedureCoordinator coordinator;

  public EnabledTableSnapshotHandler(SnapshotDescription snapshot, MasterServices master,
      final SnapshotManager manager) throws IOException {
    super(snapshot, master, manager);
    this.coordinator = manager.getCoordinator();
  }

  @Override
  public EnabledTableSnapshotHandler prepare() throws Exception {
    return (EnabledTableSnapshotHandler) super.prepare();
  }

  // TODO consider switching over to using regionnames, rather than server names. This would allow
  // regions to migrate during a snapshot, and then be involved when they are ready. Still want to
  // enforce a snapshot time constraints, but lets us be potentially a bit more robust.

  /**
   * This method kicks off a snapshot procedure.  Other than that it hangs around for various
   * phases to complete.
   */
  @Override
  protected void snapshotRegions(List<Pair<RegionInfo, ServerName>> regions) throws IOException {
    Set<String> regionServers = new HashSet<>(regions.size());
    for (Pair<RegionInfo, ServerName> region : regions) {
      if (region != null && region.getFirst() != null && region.getSecond() != null) {
        RegionInfo hri = region.getFirst();
        if (hri.isOffline() && (hri.isSplit() || hri.isSplitParent())) continue;
        regionServers.add(region.getSecond().toString());
      }
    }

    // start the snapshot on the RS
    Procedure proc = coordinator.startProcedure(this.monitor, this.snapshot.getName(),
      this.snapshot.toByteArray(), Lists.newArrayList(regionServers));
    if (proc == null) {
      String msg = "Failed to submit distributed procedure for snapshot '"
          + snapshot.getName() + "'";
      LOG.error(msg);
      throw new HBaseSnapshotException(msg);
    }

    try {
      // wait for the snapshot to complete.  A timer thread is kicked off that should cancel this
      // if it takes too long.
      proc.waitForCompleted();
      LOG.info("Done waiting - online snapshot for " + this.snapshot.getName());

      // Take the offline regions as disabled
      for (Pair<RegionInfo, ServerName> region : regions) {
        RegionInfo regionInfo = region.getFirst();
        if (regionInfo.isOffline() && (regionInfo.isSplit() || regionInfo.isSplitParent()) &&
            RegionReplicaUtil.isDefaultReplica(regionInfo)) {
          LOG.info("Take disabled snapshot of offline region=" + regionInfo);
          snapshotDisabledRegion(regionInfo);
        }
      }
      // handle the mob files if any.
      boolean mobEnabled = MobUtils.hasMobColumns(htd);
      if (mobEnabled) {
        LOG.info("Taking snapshot for mob files in table " + htd.getTableName());
        // snapshot the mob files as a offline region.
        RegionInfo mobRegionInfo = MobUtils.getMobRegionInfo(htd.getTableName());
        snapshotMobRegion(mobRegionInfo);
      }
    } catch (InterruptedException e) {
      ForeignException ee =
          new ForeignException("Interrupted while waiting for snapshot to finish", e);
      monitor.receive(ee);
      Thread.currentThread().interrupt();
    } catch (ForeignException e) {
      monitor.receive(e);
    }
  }

  /**
   * Takes a snapshot of the mob region
   */
  private void snapshotMobRegion(final RegionInfo regionInfo)
      throws IOException {
    snapshotManifest.addMobRegion(regionInfo);
    monitor.rethrowException();
    status.setStatus("Completed referencing HFiles for the mob region of table: " + snapshotTable);
  }

  @Override
  protected boolean downgradeToSharedTableLock() {
    // return true here to change from exclusive lock to shared lock, so we can still assign regions
    // while taking snapshots. This is important, as region server crash can happen at any time, if
    // we can not assign regions then the cluster will be in trouble as the regions can not online.
    return true;
  }
}
