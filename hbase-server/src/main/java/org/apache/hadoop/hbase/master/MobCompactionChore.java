/**
 *
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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.locking.LockManager;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.procedure2.LockType;

/**
 * The Class MobCompactChore for running compaction regularly to merge small mob files.
 */
@InterfaceAudience.Private
public class MobCompactionChore extends ScheduledChore {

  private static final Logger LOG = LoggerFactory.getLogger(MobCompactionChore.class);
  private HMaster master;
  private ExecutorService pool;

  public MobCompactionChore(HMaster master, int period) {
    // use the period as initial delay.
    super(master.getServerName() + "-MobCompactionChore", master, period, period, TimeUnit.SECONDS);
    this.master = master;
    this.pool = MobUtils.createMobCompactorThreadPool(master.getConfiguration());
  }

  @Override
  protected void chore() {
    try {
      TableDescriptors htds = master.getTableDescriptors();
      Map<String, TableDescriptor> map = htds.getAll();
      for (TableDescriptor htd : map.values()) {
        if (!master.getTableStateManager().isTableState(htd.getTableName(),
          TableState.State.ENABLED)) {
          continue;
        }
        boolean reported = false;
        try {
          final LockManager.MasterLock lock = master.getLockManager().createMasterLock(
              MobUtils.getTableLockName(htd.getTableName()), LockType.EXCLUSIVE,
              this.getClass().getName() + ": mob compaction");
          for (ColumnFamilyDescriptor hcd : htd.getColumnFamilies()) {
            if (!hcd.isMobEnabled()) {
              continue;
            }
            if (!reported) {
              master.reportMobCompactionStart(htd.getTableName());
              reported = true;
            }
            MobUtils.doMobCompaction(master.getConfiguration(), master.getFileSystem(),
                htd.getTableName(), hcd, pool, false, lock);
          }
        } finally {
          if (reported) {
            master.reportMobCompactionEnd(htd.getTableName());
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to compact mob files", e);
    }
  }

  @Override
  protected synchronized void cleanup() {
    super.cleanup();
    pool.shutdown();
  }
}
