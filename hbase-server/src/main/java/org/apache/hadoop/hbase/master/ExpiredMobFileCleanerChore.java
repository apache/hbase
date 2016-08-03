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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.LockTimeoutException;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.hadoop.hbase.mob.ExpiredMobFileCleaner;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;

/**
 * The Class ExpiredMobFileCleanerChore for running cleaner regularly to remove the expired
 * mob files.
 */
@InterfaceAudience.Private
public class ExpiredMobFileCleanerChore extends ScheduledChore {

  private static final Log LOG = LogFactory.getLog(ExpiredMobFileCleanerChore.class);
  private final HMaster master;
  private TableLockManager tableLockManager;
  private ExpiredMobFileCleaner cleaner;

  public ExpiredMobFileCleanerChore(HMaster master) {
    super(master.getServerName() + "-ExpiredMobFileCleanerChore", master, master.getConfiguration()
      .getInt(MobConstants.MOB_CLEANER_PERIOD, MobConstants.DEFAULT_MOB_CLEANER_PERIOD), master
      .getConfiguration().getInt(MobConstants.MOB_CLEANER_PERIOD,
        MobConstants.DEFAULT_MOB_CLEANER_PERIOD), TimeUnit.SECONDS);
    this.master = master;
    this.tableLockManager = master.getTableLockManager();
    cleaner = new ExpiredMobFileCleaner();
    cleaner.setConf(master.getConfiguration());
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="REC_CATCH_EXCEPTION",
    justification="Intentional")
  protected void chore() {
    try {
      TableDescriptors htds = master.getTableDescriptors();
      Map<String, HTableDescriptor> map = htds.getAll();
      for (HTableDescriptor htd : map.values()) {
        for (HColumnDescriptor hcd : htd.getColumnFamilies()) {
          if (hcd.isMobEnabled() && hcd.getMinVersions() == 0) {
            // clean only for mob-enabled column.
            // obtain a read table lock before cleaning, synchronize with MobFileCompactionChore.
            boolean tableLocked = false;
            TableLock lock = null;
            try {
              // the tableLockManager might be null in testing. In that case, it is lock-free.
              if (tableLockManager != null) {
                lock = tableLockManager.readLock(MobUtils.getTableLockName(htd.getTableName()),
                  "Run ExpiredMobFileCleanerChore");
                lock.acquire();
              }
              tableLocked = true;
              cleaner.cleanExpiredMobFiles(htd.getTableName().getNameAsString(), hcd);
            } catch (LockTimeoutException e) {
              LOG.info("Fail to acquire the lock because of timeout, maybe a"
                + " MobCompactor is running", e);
            } catch (IOException e) {
              LOG.error(
                "Fail to clean the expired mob files for the column " + hcd.getNameAsString()
                  + " in the table " + htd.getNameAsString(), e);
            } finally {
              if (lock != null && tableLocked) {
                try {
                  lock.release();
                } catch (IOException e) {
                  LOG.error(
                    "Fail to release the read lock for the table " + htd.getNameAsString(), e);
                }
              }
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Fail to clean the expired mob files", e);
    }
  }

}
