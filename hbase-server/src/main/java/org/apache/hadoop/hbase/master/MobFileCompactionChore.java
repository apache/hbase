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
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class MobFileCompactionChore extends ScheduledChore {

  private static final Logger LOG = LoggerFactory.getLogger(MobFileCompactionChore.class);
  private final Configuration conf;
  private final HMaster master;
  private volatile boolean running = false;
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

  @Override
  protected void chore() {

    boolean reported = false;

    try (Connection conn = ConnectionFactory.createConnection(conf);
         Admin admin = conn.getAdmin(); ) {

      if (running) {
        LOG.warn(getName() +" is running already, skipping this attempt.");
        return;
      }
      running = true;
      TableDescriptors htds = master.getTableDescriptors();
      Map<String, TableDescriptor> map = htds.getAll();
      for (TableDescriptor htd : map.values()) {
        if (!master.getTableStateManager().isTableState(htd.getTableName(),
          TableState.State.ENABLED)) {
          continue;
        }
        for (ColumnFamilyDescriptor hcd : htd.getColumnFamilies()) {
          if (hcd.isMobEnabled()) {
            if (!reported) {
              master.reportMobCompactionStart(htd.getTableName());
              reported = true;
            }
            LOG.info(" Major compacting "+ htd.getTableName() + " cf=" + hcd.getNameAsString());
            if (regionBatchSize == MobConstants.DEFAULT_MOB_MAJOR_COMPACTION_REGION_BATCH_SIZE) {
              admin.majorCompact(htd.getTableName(), hcd.getName());
            } else {
              performMajorCompactionInBatches(admin, htd, hcd);
            }
          }
        }
        if (reported) {
          master.reportMobCompactionEnd(htd.getTableName());
          reported = false;
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to compact", e);
    } finally {
      running = false;
    }
  }

  private void performMajorCompactionInBatches(Admin admin, TableDescriptor htd,
      ColumnFamilyDescriptor hcd) throws IOException {

    List<RegionInfo> regions = admin.getRegions(htd.getTableName());
    if (regions.size() <= this.regionBatchSize) {
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
      startCompaction(admin, htd.getTableName(), ri);
    }

    List<RegionInfo> compacted = new ArrayList<RegionInfo>();
    while(!toCompact.isEmpty()) {
      // Check status of active compactions
      for (RegionInfo ri: toCompact) {
        if (admin.getCompactionStateForRegion(ri.getRegionName()) == CompactionState.NONE) {
          LOG.info("Finished major compaction: table={} region={}", htd.getTableName(),
            ri.getRegionNameAsString());
          compacted.add(ri);
        }
      }
      // Update batch: remove compacted regions and add new ones
      for (RegionInfo ri: compacted) {
        toCompact.remove(ri);
        if (regions.size() > 0) {
          RegionInfo region = regions.remove(0);
          startCompaction(admin, htd.getTableName(),region);
          toCompact.add(region);
        }
      }
      compacted.clear();
      try {
        Thread.sleep(1000);
      } catch(InterruptedException e) {
        // swallow
      }
    }
    LOG.info(" Finished major compacting "+ htd.getTableName() + " cf=" + hcd.getNameAsString());

  }

  private void startCompaction(Admin admin, TableName table,  RegionInfo region)
      throws IOException {

    LOG.info("Started major compaction: table={} region={}", table,
      region.getRegionNameAsString());
    admin.majorCompactRegion(region.getRegionName());
    // Wait until it really starts
    while(admin.getCompactionStateForRegion(region.getRegionName()) == CompactionState.NONE) {
      //TODO: what if we stuck here?
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
      }
    }
  }
}
