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
package org.apache.hadoop.hbase.mob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodic MOB compaction chore.
 * <p/>
 * It runs MOB compaction on region servers in parallel, thus
 * utilizing distributed cluster resources. To avoid possible major
 * compaction storms, one can specify maximum number regions to be compacted
 * in parallel by setting configuration parameter: <br>
 * 'hbase.mob.major.compaction.region.batch.size', which by default is 0 (unlimited).
 */
@InterfaceAudience.Private
public class MobFileCompactionChore extends ScheduledChore {

  private static final Logger LOG = LoggerFactory.getLogger(MobFileCompactionChore.class);
  private HMaster master;
  private int regionBatchSize = 0;// not set - compact all

  public MobFileCompactionChore(HMaster master) {
    super(master.getServerName() + "-MobFileCompactionChore", master,
        master.getConfiguration().getInt(MobConstants.MOB_COMPACTION_CHORE_PERIOD,
          MobConstants.DEFAULT_MOB_COMPACTION_CHORE_PERIOD),
        master.getConfiguration().getInt(MobConstants.MOB_COMPACTION_CHORE_PERIOD,
          MobConstants.DEFAULT_MOB_COMPACTION_CHORE_PERIOD),
        TimeUnit.SECONDS);
    this.master = master;
    this.regionBatchSize =
        master.getConfiguration().getInt(MobConstants.MOB_MAJOR_COMPACTION_REGION_BATCH_SIZE,
          MobConstants.DEFAULT_MOB_MAJOR_COMPACTION_REGION_BATCH_SIZE);

  }

  public MobFileCompactionChore(Configuration conf, int batchSize) {
    this.regionBatchSize = batchSize;
  }

  @Override
  protected void chore() {
    boolean reported = false;

    try (Admin admin = master.getConnection().getAdmin()) {
      TableDescriptors htds = master.getTableDescriptors();
      Map<String, TableDescriptor> map = htds.getAll();
      for (TableDescriptor htd : map.values()) {
        if (!master.getTableStateManager().isTableState(htd.getTableName(),
          TableState.State.ENABLED)) {
          LOG.info("Skipping MOB compaction on table {} because it is not ENABLED",
            htd.getTableName());
          continue;
        } else {
          LOG.info("Starting MOB compaction on table {}, checking {} column families",
            htd.getTableName(), htd.getColumnFamilyCount());
        }
        for (ColumnFamilyDescriptor hcd : htd.getColumnFamilies()) {
          try {
            if (hcd.isMobEnabled()) {
              if (!reported) {
                master.reportMobCompactionStart(htd.getTableName());
                reported = true;
              }
              LOG.info("Major MOB compacting table={} cf={}", htd.getTableName(),
                hcd.getNameAsString());
              if (regionBatchSize == MobConstants.DEFAULT_MOB_MAJOR_COMPACTION_REGION_BATCH_SIZE) {
                LOG.debug("Table={} cf ={}: batch MOB compaction is disabled, {}=0 -"+
                  " all regions will be compacted in parallel", htd.getTableName(),
                  hcd.getNameAsString(), "hbase.mob.compaction.batch.size");
                admin.majorCompact(htd.getTableName(), hcd.getName());
              } else {
                LOG.info("Table={} cf={}: performing MOB major compaction in batches "+
                    "'hbase.mob.compaction.batch.size'={}", htd.getTableName(),
                    hcd.getNameAsString(), regionBatchSize);
                performMajorCompactionInBatches(admin, htd, hcd);
              }
            } else {
              LOG.debug("Skipping table={} column family={} because it is not MOB-enabled",
                htd.getTableName(), hcd.getNameAsString());
            }
          } catch (IOException e) {
            LOG.error("Failed to compact table={} cf={}",
              htd.getTableName(), hcd.getNameAsString(), e);
          } catch (InterruptedException ee) {
            Thread.currentThread().interrupt();
            master.reportMobCompactionEnd(htd.getTableName());
            LOG.warn("Failed to compact table={} cf={}",
              htd.getTableName(), hcd.getNameAsString(), ee);
            // Quit the chore
            return;
          }
        }
        if (reported) {
          master.reportMobCompactionEnd(htd.getTableName());
          reported = false;
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to compact", e);
    }
  }

  public void performMajorCompactionInBatches(Admin admin, TableDescriptor htd,
      ColumnFamilyDescriptor hcd) throws IOException, InterruptedException {

    List<RegionInfo> regions = admin.getRegions(htd.getTableName());
    if (regions.size() <= this.regionBatchSize) {
      LOG.debug(
        "Table={} cf={} - performing major MOB compaction in non-batched mode,"
            + "regions={}, batch size={}",
        htd.getTableName(), hcd.getNameAsString(), regions.size(), regionBatchSize);
      admin.majorCompact(htd.getTableName(), hcd.getName());
      return;
    }
    // Shuffle list of regions in case if they come ordered by region server
    Collections.shuffle(regions);
    // Create first batch
    List<RegionInfo> toCompact = new ArrayList<RegionInfo>(this.regionBatchSize);
    for (int i = 0; i < this.regionBatchSize; i++) {
      toCompact.add(regions.remove(0));
    }

    // Start compaction now
    for (RegionInfo ri : toCompact) {
      startCompaction(admin, htd.getTableName(), ri, hcd.getName());
    }

    List<RegionInfo> compacted = new ArrayList<RegionInfo>(toCompact.size());
    List<RegionInfo> failed = new ArrayList<RegionInfo>();
    int totalCompacted = 0;
    while (!toCompact.isEmpty()) {
      // Check status of active compactions
      for (RegionInfo ri : toCompact) {
        try {
          if (admin.getCompactionStateForRegion(ri.getRegionName()) == CompactionState.NONE) {
            totalCompacted++;
            LOG.info(
              "Finished major MOB compaction: table={} cf={} region={} compacted regions={}",
              htd.getTableName(), hcd.getNameAsString(), ri.getRegionNameAsString(),
              totalCompacted);
            compacted.add(ri);
          }
        } catch (IOException e) {
          LOG.error("Could not get compaction state for table={} cf={} region={}, compaction will"+
            " aborted for the region.",
            htd.getTableName(), hcd.getNameAsString(), ri.getEncodedName());
          LOG.error("Because of:", e);
          failed.add(ri);
        }
      }
      // Remove failed regions to avoid
      // endless compaction loop
      toCompact.removeAll(failed);
      failed.clear();
      // Update batch: remove compacted regions and add new ones
      for (RegionInfo ri : compacted) {
        toCompact.remove(ri);
        if (regions.size() > 0) {
          RegionInfo region = regions.remove(0);
          toCompact.add(region);
          startCompaction(admin, htd.getTableName(), region, hcd.getName());
        }
      }
      compacted.clear();

      LOG.debug(
        "Table={}  cf={}. Wait for 10 sec, toCompact size={} regions left={}"
        + " compacted so far={}", htd.getTableName(), hcd.getNameAsString(), toCompact.size(),
        regions.size(), totalCompacted);
      Thread.sleep(10000);
    }
    LOG.info("Finished major MOB compacting table={}. cf={}", htd.getTableName(),
      hcd.getNameAsString());

  }

  private void startCompaction(Admin admin, TableName table, RegionInfo region, byte[] cf)
      throws IOException, InterruptedException {
    LOG.info("Started major compaction: table={} cf={} region={}", table,
      Bytes.toString(cf), region.getRegionNameAsString());
    admin.majorCompactRegion(region.getRegionName(), cf);
    // Wait until it really starts
    // but with finite timeout
    long waitTime = 300000; // 5 min
    long startTime = EnvironmentEdgeManager.currentTime();
    while (admin.getCompactionStateForRegion(region.getRegionName()) == CompactionState.NONE) {
      // Is 1 second too aggressive?
      Thread.sleep(1000);
      if (EnvironmentEdgeManager.currentTime() - startTime > waitTime) {
        LOG.warn(
          "Waited for {} ms to start major MOB compaction on table={} cf={} region={}."
            + " Stopped waiting for request confirmation. This is not an ERROR,"
            + " continue next region.",
          waitTime,
          table.getNameAsString(),
          Bytes.toString(cf),
          region.getRegionNameAsString());
        break;
      }
    }
  }
}
