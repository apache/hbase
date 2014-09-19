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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.mob.ExpiredMobFileCleaner;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.Threads;

/**
 * The Class ExpiredMobFileCleanerChore for running cleaner regularly to remove the expired
 * mob files.
 */
@InterfaceAudience.Private
public class ExpiredMobFileCleanerChore extends Chore {

  private static final Log LOG = LogFactory.getLog(ExpiredMobFileCleanerChore.class);
  private final HMaster master;
  private ExpiredMobFileCleaner cleaner;

  public ExpiredMobFileCleanerChore(HMaster master) {
    super(master.getServerName() + "-ExpiredMobFileCleanerChore", master.getConfiguration().getInt(
        MobConstants.MOB_CLEANER_PERIOD, MobConstants.DEFAULT_MOB_CLEANER_PERIOD), master);
    this.master = master;
    cleaner = new ExpiredMobFileCleaner();
  }

  @Override
  protected void chore() {
    try {
      TableDescriptors htds = master.getTableDescriptors();
      Map<String, HTableDescriptor> map = htds.getAll();
      for (HTableDescriptor htd : map.values()) {
        for (HColumnDescriptor hcd : htd.getColumnFamilies()) {
          if (MobUtils.isMobFamily(hcd) && hcd.getMinVersions() == 0) {
            cleaner.cleanExpiredMobFiles(htd.getTableName().getNameAsString(), hcd);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Fail to clean the expired mob files", e);
    }
  }

}
