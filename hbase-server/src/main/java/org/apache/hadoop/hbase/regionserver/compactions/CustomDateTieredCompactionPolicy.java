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
package org.apache.hadoop.hbase.regionserver.compactions;

import static org.apache.hadoop.hbase.regionserver.CustomTieringMultiFileWriter.CUSTOM_TIERING_TIME_RANGE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom implementation of DateTieredCompactionPolicy that calculates compaction boundaries based
 * on the <b>hbase.hstore.compaction.date.tiered.custom.age.limit.millis</b> configuration property
 * and the TIERING_CELL_MIN/TIERING_CELL_MAX stored on metadata of each store file. This policy
 * would produce either one or two tiers: - One tier if either all files data age are older than the
 * configured age limit or all files data age are younger than the configured age limit. - Two tiers
 * if files have both younger and older data than the configured age limit.
 */
@InterfaceAudience.Private
public class CustomDateTieredCompactionPolicy extends DateTieredCompactionPolicy {

  public static final String AGE_LIMIT_MILLIS =
    "hbase.hstore.compaction.date.tiered.custom.age.limit.millis";

  // Defaults to 10 years
  public static final long DEFAULT_AGE_LIMIT_MILLIS =
    (long) (10L * 365.25 * 24L * 60L * 60L * 1000L);

  private static final Logger LOG = LoggerFactory.getLogger(CustomDateTieredCompactionPolicy.class);

  private long cutOffTimestamp;

  public CustomDateTieredCompactionPolicy(Configuration conf,
    StoreConfigInformation storeConfigInfo) throws IOException {
    super(conf, storeConfigInfo);
    cutOffTimestamp = EnvironmentEdgeManager.currentTime()
      - conf.getLong(AGE_LIMIT_MILLIS, DEFAULT_AGE_LIMIT_MILLIS);

  }

  @Override
  protected List<Long> getCompactBoundariesForMajor(Collection<HStoreFile> filesToCompact,
    long now) {
    MutableLong min = new MutableLong(Long.MAX_VALUE);
    MutableLong max = new MutableLong(0);
    filesToCompact.forEach(f -> {
      byte[] timeRangeBytes = f.getMetadataValue(CUSTOM_TIERING_TIME_RANGE);
      long minCurrent = Long.MAX_VALUE;
      long maxCurrent = 0;
      if (timeRangeBytes != null) {
        try {
          TimeRangeTracker timeRangeTracker = TimeRangeTracker.parseFrom(timeRangeBytes);
          timeRangeTracker.getMin();
          minCurrent = timeRangeTracker.getMin();
          maxCurrent = timeRangeTracker.getMax();
        } catch (IOException e) {
          LOG.warn("Got TIERING_CELL_TIME_RANGE info from file, but failed to parse it:", e);
        }
      }
      if (minCurrent < min.getValue()) {
        min.setValue(minCurrent);
      }
      if (maxCurrent > max.getValue()) {
        max.setValue(maxCurrent);
      }
    });

    List<Long> boundaries = new ArrayList<>();
    boundaries.add(Long.MIN_VALUE);
    if (min.getValue() < cutOffTimestamp) {
      boundaries.add(min.getValue());
      if (max.getValue() > cutOffTimestamp) {
        boundaries.add(cutOffTimestamp);
      }
    }
    return boundaries;
  }

  @Override
  public CompactionRequestImpl selectMinorCompaction(ArrayList<HStoreFile> candidateSelection,
    boolean mayUseOffPeak, boolean mayBeStuck) throws IOException {
    ArrayList<HStoreFile> filteredByPolicy = this.compactionPolicyPerWindow
      .applyCompactionPolicy(candidateSelection, mayUseOffPeak, mayBeStuck);
    return selectMajorCompaction(filteredByPolicy);
  }

  @Override
  public boolean shouldPerformMajorCompaction(Collection<HStoreFile> filesToCompact)
    throws IOException {
    long lowTimestamp = StoreUtils.getLowestTimestamp(filesToCompact);
    long now = EnvironmentEdgeManager.currentTime();
    if (isMajorCompactionTime(filesToCompact, now, lowTimestamp)) {
      long cfTTL = this.storeConfigInfo.getStoreFileTtl();
      int countLower = 0;
      int countHigher = 0;
      HDFSBlocksDistribution hdfsBlocksDistribution = new HDFSBlocksDistribution();
      for (HStoreFile f : filesToCompact) {
        if (checkForTtl(cfTTL, f)) {
          return true;
        }
        if (isMajorOrBulkloadResult(f, now - lowTimestamp)) {
          return true;
        }
        byte[] timeRangeBytes = f.getMetadataValue(CUSTOM_TIERING_TIME_RANGE);
        TimeRangeTracker timeRangeTracker = TimeRangeTracker.parseFrom(timeRangeBytes);
        if (timeRangeTracker.getMin() < cutOffTimestamp) {
          if (timeRangeTracker.getMax() > cutOffTimestamp) {
            // Found at least one file crossing the cutOffTimestamp
            return true;
          } else {
            countLower++;
          }
        } else {
          countHigher++;
        }
        hdfsBlocksDistribution.add(f.getHDFSBlockDistribution());
      }
      // If we haven't found any files crossing the cutOffTimestamp, we have to check
      // if there are at least more than one file on each tier and if so, perform compaction
      if (countLower > 1 || countHigher > 1) {
        return true;
      }
      return checkBlockLocality(hdfsBlocksDistribution);
    }
    return false;
  }

}
