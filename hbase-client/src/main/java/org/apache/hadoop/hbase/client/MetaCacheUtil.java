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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.client.ConnectionUtils.isEmptyStopRow;

import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util class to DRY common logic between AsyncRegionLocationCache and MetaCache
 */
@InterfaceAudience.Private
final class MetaCacheUtil {

  private static final Logger LOG = LoggerFactory.getLogger(MetaCacheUtil.class);

  private MetaCacheUtil() {
  }

  /**
   * When caching a location, the region may have been the result of a merge. Check to see if the
   * region's boundaries overlap any other cached locations in a problematic way. Those would have
   * been merge parents which no longer exist. We need to proactively clear them out to avoid a case
   * where a merged region which receives no requests never gets cleared. This causes requests to
   * other merged regions after it to see the wrong cached location.
   * <p>
   * For example, if we have Start_New < Start_Old < End_Old < End_New, then if we only access
   * within range [End_Old, End_New], then it will always return the old region but it will then
   * find out the row is not in the range, and try to get the new region, and then we get
   * [Start_New, End_New), still fall into the same situation.
   * <p>
   * If Start_Old is less than Start_New, even if we have overlap, it is not a problem, as when the
   * row is greater than Start_New, we will locate to the new region, and if the row is less than
   * Start_New, it will fall into the old region's range and we will try to access the region and
   * get a NotServing exception, and then we will clean the cache.
   * <p>
   * See HBASE-27650
   * @param locations the new location that was just cached
   */
  static void cleanProblematicOverlappedRegions(RegionLocations locations,
    ConcurrentNavigableMap<byte[], RegionLocations> cache) {
    RegionInfo region = locations.getRegionLocation().getRegion();

    boolean isLast = isEmptyStopRow(region.getEndKey());

    while (true) {
      Map.Entry<byte[], RegionLocations> overlap =
        isLast ? cache.lastEntry() : cache.lowerEntry(region.getEndKey());
      if (
        overlap == null || overlap.getValue() == locations
          || Bytes.equals(overlap.getKey(), region.getStartKey())
      ) {
        break;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "Removing cached location {} (endKey={}) because it overlaps with "
            + "new location {} (endKey={})",
          overlap.getValue(),
          Bytes.toStringBinary(overlap.getValue().getRegionLocation().getRegion().getEndKey()),
          locations, Bytes.toStringBinary(locations.getRegionLocation().getRegion().getEndKey()));
      }

      cache.remove(overlap.getKey());
    }
  }
}
