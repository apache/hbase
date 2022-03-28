/*
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

import com.google.errorprone.annotations.RestrictedApi;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Config manager for RegionsRecovery Chore - Dynamically reload config and update chore accordingly
 */
@InterfaceAudience.Private
public class RegionsRecoveryConfigManager implements ConfigurationObserver {

  private static final Logger LOG = LoggerFactory.getLogger(RegionsRecoveryConfigManager.class);

  private final HMaster hMaster;
  private RegionsRecoveryChore chore;
  private int prevMaxStoreFileRefCount;
  private int prevRegionsRecoveryInterval;

  RegionsRecoveryConfigManager(final HMaster hMaster) {
    this.hMaster = hMaster;
    Configuration conf = hMaster.getConfiguration();
    this.prevMaxStoreFileRefCount = getMaxStoreFileRefCount(conf);
    this.prevRegionsRecoveryInterval = getRegionsRecoveryChoreInterval(conf);
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    final int newMaxStoreFileRefCount = getMaxStoreFileRefCount(conf);
    final int newRegionsRecoveryInterval = getRegionsRecoveryChoreInterval(conf);

    if (prevMaxStoreFileRefCount == newMaxStoreFileRefCount &&
      prevRegionsRecoveryInterval == newRegionsRecoveryInterval) {
      // no need to re-schedule the chore with updated config
      // as there is no change in desired configs
      return;
    }

    LOG.info(
      "Config Reload for RegionsRecovery Chore. prevMaxStoreFileRefCount: {}," +
        " newMaxStoreFileRefCount: {}, prevRegionsRecoveryInterval: {}, " +
        "newRegionsRecoveryInterval: {}",
      prevMaxStoreFileRefCount, newMaxStoreFileRefCount, prevRegionsRecoveryInterval,
      newRegionsRecoveryInterval);

    RegionsRecoveryChore regionsRecoveryChore =
      new RegionsRecoveryChore(this.hMaster, conf, this.hMaster);
    ChoreService choreService = this.hMaster.getChoreService();

    // Regions Reopen based on very high storeFileRefCount is considered enabled
    // only if hbase.regions.recovery.store.file.ref.count has value > 0
    synchronized (this) {
      if (chore != null) {
        chore.shutdown();
        chore = null;
      }
      if (newMaxStoreFileRefCount > 0) {
        // schedule the new chore
        choreService.scheduleChore(regionsRecoveryChore);
        chore = regionsRecoveryChore;
      }
      this.prevMaxStoreFileRefCount = newMaxStoreFileRefCount;
      this.prevRegionsRecoveryInterval = newRegionsRecoveryInterval;
    }
  }

  private int getMaxStoreFileRefCount(Configuration configuration) {
    return configuration.getInt(HConstants.STORE_FILE_REF_COUNT_THRESHOLD,
      HConstants.DEFAULT_STORE_FILE_REF_COUNT_THRESHOLD);
  }

  private int getRegionsRecoveryChoreInterval(Configuration configuration) {
    return configuration.getInt(HConstants.REGIONS_RECOVERY_INTERVAL,
      HConstants.DEFAULT_REGIONS_RECOVERY_INTERVAL);
  }

  @RestrictedApi(explanation = "Only visible for testing", link = "",
    allowedOnPath = ".*/src/test/.*")
  RegionsRecoveryChore getChore() {
    return chore;
  }
}
