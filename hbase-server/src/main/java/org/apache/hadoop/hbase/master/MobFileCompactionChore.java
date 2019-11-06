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
package org.apache.hadoop.hbase.master;

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
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * Periodic MOB compaction chore. 
 * It runs MOB compaction on region servers in parallel, thus
 * utilizing distributed cluster resources. To avoid possible major
 * compaction storms, one can specify maximum number regions to be compacted 
 * in parallel by setting configuration parameter: <br>
 * 'hbase.mob.major.compaction.region.batch.size', which by default is 0 (unlimited). 
 * 
 */
@InterfaceAudience.Private
public class MobFileCompactionChore extends ScheduledChore {

  private static final Logger LOG = LoggerFactory.getLogger(MobFileCompactionChore.class);
  private Configuration conf;
  private HMaster master;
  private int regionBatchSize = 0;// not set - compact all

  public MobFileCompactionChore(HMaster master) {
    super(master.getServerName() + "-MobFileCompactionChore", master, master.getConfiguration()
      .getInt(MobConstants.MOB_COMPACTION_CHORE_PERIOD,
        MobConstants.DEFAULT_MOB_COMPACTION_CHORE_PERIOD), master
      .getConfiguration().getInt(MobConstants.MOB_COMPACTION_CHORE_PERIOD,
        MobConstants.DEFAULT_MOB_COMPACTION_CHORE_PERIOD), TimeUnit.SECONDS);
    this.master = master;
    this.conf = master.getConfiguration();
    this.regionBatchSize =
        master.getConfiguration().getInt(MobConstants.MOB_MAJOR_COMPACTION_REGION_BATCH_SIZE,
          MobConstants.DEFAULT_MOB_MAJOR_COMPACTION_REGION_BATCH_SIZE);

  }

  @VisibleForTesting
  public MobFileCompactionChore(Configuration conf, int batchSize) {
    this.conf = conf;
    this.regionBatchSize = batchSize;
  }
  
  @Override
  protected void chore() {

    boolean reported = false;

    try (Connection conn = ConnectionFactory.createConnection(conf);
         Admin admin = conn.getAdmin(); ) {

      TableDescriptors htds = master.getTableDescriptors();
      Map<String, TableDescriptor> map = htds.getAll();
      for (TableDescriptor htd : map.values()) {
        if (!master.getTableStateManager().isTableState(htd.getTableName(),
          TableState.State.ENABLED)) {
          LOG.debug("Skipping MOB compaction on table {} because it is not ENABLED", 
            htd.getTableName());
          continue;
        } else {
          LOG.debug("Starting MOB compaction on table {}", htd.getTableName());
        } 
        for (ColumnFamilyDescriptor hcd : htd.getColumnFamilies()) {
          try {
            if (hcd.isMobEnabled()) {
              if (!reported) {
                master.reportMobCompactionStart(htd.getTableName());
                reported = true;
              }
              LOG.info(" Major compacting {} cf={}", htd.getTableName(), hcd.getNameAsString());
              if (regionBatchSize == MobConstants.DEFAULT_MOB_MAJOR_COMPACTION_REGION_BATCH_SIZE) {
                LOG.debug("Batch compaction is disabled, {}=0", "hbase.mob.compaction.batch.size");
                admin.majorCompact(htd.getTableName(), hcd.getName());
              } else {
                LOG.debug("Performing compaction in batches, {}={}",
                  "hbase.mob.compaction.batch.size", regionBatchSize);
                performMajorCompactionInBatches(admin, htd, hcd);
              }
            } else {
              LOG.debug("Skipping column family {} because it is not MOB-enabled",
                hcd.getNameAsString());
            }
          } catch (IOException e) {
            LOG.error("Failed to compact table="+ htd.getTableName() +" cf="+ hcd.getNameAsString(),
              e);
          } catch (InterruptedException ee) {
            Thread.currentThread().interrupt();
            master.reportMobCompactionEnd(htd.getTableName());
            LOG.warn("Failed to compact table="+ htd.getTableName() +" cf="+ hcd.getNameAsString(),
              ee);
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

  @VisibleForTesting
  public void performMajorCompactionInBatches(Admin admin, TableDescriptor htd,
      ColumnFamilyDescriptor hcd) throws IOException, InterruptedException {

    List<RegionInfo> regions = admin.getRegions(htd.getTableName());
    if (regions.size() <= this.regionBatchSize) {
      LOG.debug("Performing compaction in non-batched mode, regions={}, batch size={}",
        regions.size(), regionBatchSize);
      admin.majorCompact(htd.getTableName(), hcd.getName());
      return;
    }
    // Shuffle list of regions in case if they come ordered by region server
    Collections.shuffle(regions);
    // Create first batch
    List<RegionInfo> toCompact = new ArrayList<RegionInfo>();
    for (int i=0; i < this.regionBatchSize; i++) {
      toCompact.add(regions.remove(0));
    }

    // Start compaction now
    for(RegionInfo ri: toCompact) {
      startCompaction(admin, htd.getTableName(), ri, hcd.getName());
    }

    List<RegionInfo> compacted = new ArrayList<RegionInfo>();
    int totalCompacted = 0;
    while(!toCompact.isEmpty()) {
      // Check status of active compactions
      for (RegionInfo ri: toCompact) {
        try {
          if (admin.getCompactionStateForRegion(ri.getRegionName()) == CompactionState.NONE) {
            totalCompacted++;
            LOG.info("Finished major compaction: table={} region={}, compacted regions={}", 
              htd.getTableName(),ri.getRegionNameAsString(), totalCompacted);
            compacted.add(ri);
          }
        } catch (IOException e) {
          LOG.warn("Could not get compaction state for region {}", ri.getEncodedName());
        }
      }
      // Update batch: remove compacted regions and add new ones
      for (RegionInfo ri: compacted) {
        toCompact.remove(ri);
        if (regions.size() > 0) {
          RegionInfo region = regions.remove(0);
          startCompaction(admin, htd.getTableName(),region, hcd.getName());
          toCompact.add(region);
        }
      }
      compacted.clear();
      LOG.debug("Wait for 10 sec, toCompact size={} regions left={} compacted so far={}", 
        toCompact.size(), regions.size(), totalCompacted);
      Thread.sleep(10000);
    }
    LOG.info("Finished major compacting {}. cf={}", htd.getTableName(), hcd.getNameAsString());

  }

  private void startCompaction(Admin admin, TableName table,  RegionInfo region, byte[] cf)
      throws IOException, InterruptedException {

    LOG.info("Started major compaction: table={} region={}", table,
      region.getRegionNameAsString());
    admin.majorCompactRegion(region.getRegionName());
    // Wait until it really starts
    // but with finite timeout
    long waitTime = 300000; // 5 min
    long startTime = EnvironmentEdgeManager.currentTime();
    while(admin.getCompactionStateForRegion(region.getRegionName()) == CompactionState.NONE) {
      // Is 1 second too aggressive?
      Thread.sleep(1000);
      if (EnvironmentEdgeManager.currentTime() - startTime > waitTime) {
        LOG.warn("Waited for {} ms to start major compaction on table: {} region: {}. Aborted.", 
          waitTime, table.getNameAsString(), region.getRegionNameAsString());
        break;
      }
    }
  }
}
