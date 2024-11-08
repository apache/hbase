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

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import static org.apache.hadoop.hbase.regionserver.CustomTieringMultiFileWriter.TIERING_CELL_TIME_RANGE;

/**
 * Custom implementation of DateTieredCompactionPolicy that calculates compaction boundaries based
 * on the <b>hbase.hstore.compaction.date.tiered.custom.age.limit.millis</b> configuration property
 * and the TIERING_CELL_MIN/TIERING_CELL_MAX stored on metadata of each store file.
 *
 * This policy would produce either one or two tiers:
 *  - One tier if either all files data age are older than the configured age limit or all files
 *  data age are younger than the configured age limit.
 *  - Two tiers if files have both younger and older data than the configured age limit.
 *
 */
@InterfaceAudience.Private
public class CustomCellDateTieredCompactionPolicy extends DateTieredCompactionPolicy {

  public static final String AGE_LIMIT_MILLIS =
    "hbase.hstore.compaction.date.tiered.custom.age.limit.millis";

  public static final String TIERING_CELL_QUALIFIER = "TIERING_CELL_QUALIFIER";

  private long cutOffTimestamp;

  public CustomCellDateTieredCompactionPolicy(Configuration conf,
    StoreConfigInformation storeConfigInfo) throws IOException {
    super(conf, storeConfigInfo);
    cutOffTimestamp = EnvironmentEdgeManager.currentTime() -
      conf.getLong(AGE_LIMIT_MILLIS, (long) (10L*365.25*24L*60L*60L*1000L));

  }

  @Override
  protected List<Long> getCompactBoundariesForMajor(Collection<HStoreFile> filesToCompact, long now) {
    MutableLong min = new MutableLong(Long.MAX_VALUE);
    MutableLong max = new MutableLong(0);
    filesToCompact.forEach(f -> {
      byte[] timeRangeBytes = f.getMetadataValue(TIERING_CELL_TIME_RANGE);
      long minCurrent = Long.MAX_VALUE;
      long maxCurrent = 0;
      if(timeRangeBytes!=null) {
        try {
          TimeRangeTracker timeRangeTracker = TimeRangeTracker.parseFrom(timeRangeBytes);
          timeRangeTracker.getMin();
          minCurrent = timeRangeTracker.getMin();
          maxCurrent = timeRangeTracker.getMax();
        } catch (IOException e) {
              //TODO debug this
        }
      }
      if(minCurrent < min.getValue()) {
        min.setValue(minCurrent);
      }
      if(maxCurrent > max.getValue()) {
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

}
