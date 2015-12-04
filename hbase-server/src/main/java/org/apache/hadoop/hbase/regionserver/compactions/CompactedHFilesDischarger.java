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
package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;

/**
 * A chore service that periodically cleans up the compacted files when there are no active readers
 * using those compacted files and also helps in clearing the block cache with these compacted
 * file entries
 */
@InterfaceAudience.Private
public class CompactedHFilesDischarger extends ScheduledChore {
  private static final Log LOG = LogFactory.getLog(CompactedHFilesDischarger.class);
  private Region region;

  /**
   * @param period the period of time to sleep between each run
   * @param stopper the stopper
   * @param region the store to identify the family name
   */
  public CompactedHFilesDischarger(final int period, final Stoppable stopper, final Region region) {
    // Need to add the config classes
    super("CompactedHFilesCleaner", stopper, period);
    this.region = region;
  }

  @Override
  public void chore() {
    if (LOG.isTraceEnabled()) {
      LOG.trace(
        "Started the compacted hfiles cleaner for the region " + this.region.getRegionInfo());
    }
    for (Store store : region.getStores()) {
      try {
        store.closeAndArchiveCompactedFiles();
        if (LOG.isTraceEnabled()) {
          LOG.trace("Completed archiving the compacted files for the region "
              + this.region.getRegionInfo() + " under the store " + store.getColumnFamilyName());
        }
      } catch (Exception e) {
        LOG.error(
          "Exception while trying to close and archive the comapcted store files of the store  "
              + store.getColumnFamilyName() + " in the region " + this.region.getRegionInfo(),
          e);
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace(
        "Completed the compacted hfiles cleaner for the region " + this.region.getRegionInfo());
    }
  }
}
