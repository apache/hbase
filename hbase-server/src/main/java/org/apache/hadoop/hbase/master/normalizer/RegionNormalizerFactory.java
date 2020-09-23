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
package org.apache.hadoop.hbase.master.normalizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.zookeeper.RegionNormalizerTracker;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Factory to create instance of {@link RegionNormalizer} as configured.
 */
@InterfaceAudience.Private
public final class RegionNormalizerFactory {

  private RegionNormalizerFactory() {
  }

  public static RegionNormalizerManager createNormalizerManager(
    final Configuration conf,
    final ZKWatcher zkWatcher,
    final HMaster master // TODO: consolidate this down to MasterServices
  ) {
    final RegionNormalizer regionNormalizer = getRegionNormalizer(conf);
    regionNormalizer.setMasterServices(master);
    final RegionNormalizerTracker tracker = new RegionNormalizerTracker(zkWatcher, master);
    final RegionNormalizerChore chore =
      master.isInMaintenanceMode() ? null : new RegionNormalizerChore(master);
    final RegionNormalizerWorkQueue<TableName> workQueue =
      master.isInMaintenanceMode() ? null : new RegionNormalizerWorkQueue<>();
    final RegionNormalizerWorker worker = master.isInMaintenanceMode()
      ? null
      : new RegionNormalizerWorker(conf, master, regionNormalizer, workQueue);
    return new RegionNormalizerManager(tracker, chore, workQueue, worker);
  }

  /**
   * Create a region normalizer from the given conf.
   * @param conf configuration
   * @return {@link RegionNormalizer} implementation
   */
  private static RegionNormalizer getRegionNormalizer(Configuration conf) {
    // Create instance of Region Normalizer
    Class<? extends RegionNormalizer> balancerKlass =
      conf.getClass(HConstants.HBASE_MASTER_NORMALIZER_CLASS, SimpleRegionNormalizer.class,
        RegionNormalizer.class);
    return ReflectionUtils.newInstance(balancerKlass, conf);
  }
}
