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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.locking.LockManager;
import org.apache.hadoop.hbase.mob.ExpiredMobFileCleaner;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.procedure2.LockType;

/**
 * The Class ExpiredMobFileCleanerChore for running cleaner regularly to remove the expired
 * mob files.
 */
@InterfaceAudience.Private
public class ExpiredMobFileCleanerChore extends ScheduledChore {

  private static final Logger LOG = LoggerFactory.getLogger(ExpiredMobFileCleanerChore.class);
  private final HMaster master;
  private ExpiredMobFileCleaner cleaner;

  public ExpiredMobFileCleanerChore(HMaster master) {
    super(master.getServerName() + "-ExpiredMobFileCleanerChore", master, master.getConfiguration()
      .getInt(MobConstants.MOB_CLEANER_PERIOD, MobConstants.DEFAULT_MOB_CLEANER_PERIOD), master
      .getConfiguration().getInt(MobConstants.MOB_CLEANER_PERIOD,
        MobConstants.DEFAULT_MOB_CLEANER_PERIOD), TimeUnit.SECONDS);
    this.master = master;
    cleaner = new ExpiredMobFileCleaner();
    cleaner.setConf(master.getConfiguration());
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="REC_CATCH_EXCEPTION",
    justification="Intentional")
  protected void chore() {
    try {
      TableDescriptors htds = master.getTableDescriptors();
      Map<String, TableDescriptor> map = htds.getAll();
      for (TableDescriptor htd : map.values()) {
        for (ColumnFamilyDescriptor hcd : htd.getColumnFamilies()) {
          if (hcd.isMobEnabled() && hcd.getMinVersions() == 0) {
            // clean only for mob-enabled column.
            // obtain a read table lock before cleaning, synchronize with MobFileCompactionChore.
            final LockManager.MasterLock lock = master.getLockManager().createMasterLock(
                MobUtils.getTableLockName(htd.getTableName()), LockType.SHARED,
                this.getClass().getSimpleName() + ": Cleaning expired mob files");
            try {
              lock.acquire();
              cleaner.cleanExpiredMobFiles(htd.getTableName().getNameAsString(), hcd);
            } finally {
              lock.release();
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Fail to clean the expired mob files", e);
    }
  }

}
