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
package org.apache.hadoop.hbase.regionserver;

import java.util.List;

import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A chore service that periodically cleans up the compacted files when there are no active readers
 * using those compacted files and also helps in clearing the block cache of these compacted
 * file entries.
 */
@InterfaceAudience.Private
public class CompactedHFilesDischarger extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(CompactedHFilesDischarger.class);
  private RegionServerServices regionServerServices;
  // Default is to use executor
  private boolean useExecutor = true;

  /**
   * @param period the period of time to sleep between each run
   * @param stopper the stopper
   * @param regionServerServices the region server that starts this chore
   */
  public CompactedHFilesDischarger(final int period, final Stoppable stopper,
      final RegionServerServices regionServerServices) {
    // Need to add the config classes
    super("CompactedHFilesCleaner", stopper, period);
    this.regionServerServices = regionServerServices;
  }

  /**
   * @param period the period of time to sleep between each run
   * @param stopper the stopper
   * @param regionServerServices the region server that starts this chore
   * @param useExecutor true if to use the region server's executor service, false otherwise
   */
  public CompactedHFilesDischarger(final int period, final Stoppable stopper,
      final RegionServerServices regionServerServices, boolean useExecutor) {
    // Need to add the config classes
    this(period, stopper, regionServerServices);
    this.useExecutor = useExecutor;
  }

  /**
   * CompactedHFilesDischarger runs asynchronously by default using the hosting
   * RegionServer's Executor. In tests it can be useful to force a synchronous
   * cleanup. Use this method to set no-executor before you call run.
   * @return The old setting for <code>useExecutor</code>
   */
  boolean setUseExecutor(final boolean useExecutor) {
    boolean oldSetting = this.useExecutor;
    this.useExecutor = useExecutor;
    return oldSetting;
  }

  @Override
  public void chore() {
    // Noop if rss is null. This will never happen in a normal condition except for cases
    // when the test case is not spinning up a cluster
    if (regionServerServices == null) return;
    List<HRegion> onlineRegions = (List<HRegion>) regionServerServices.getRegions();
    if (onlineRegions == null) return;
    for (HRegion region : onlineRegions) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Started compacted hfiles cleaner on " + region.getRegionInfo());
      }
      for (HStore store : region.getStores()) {
        try {
          if (useExecutor && regionServerServices != null) {
            CompactedHFilesDischargeHandler handler = new CompactedHFilesDischargeHandler(
                (Server) regionServerServices, EventType.RS_COMPACTED_FILES_DISCHARGER, store);
            regionServerServices.getExecutorService().submit(handler);
          } else {
            // call synchronously if the RegionServerServices are not
            // available
            store.closeAndArchiveCompactedFiles();
          }
          if (LOG.isTraceEnabled()) {
            LOG.trace("Completed archiving the compacted files for the region "
                + region.getRegionInfo() + " under the store " + store.getColumnFamilyName());
          }
        } catch (Exception e) {
          LOG.error("Exception while trying to close and archive the compacted store "
              + "files of the store  " + store.getColumnFamilyName() + " in the" + " region "
              + region.getRegionInfo(), e);
        }
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace(
            "Completed the compacted hfiles cleaner for the region " + region.getRegionInfo());
      }
    }
  }
}
