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
package org.apache.hadoop.hbase.snapshot;

import java.util.Collections;
import java.util.Map;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Holds the size of each region in a snapshot.
 */
@InterfaceAudience.Private
public class RegionSizes {

  private final Map<String, Long> regionSizeMap;

  RegionSizes(Map<String, Long> regionSizeMap) {
    this.regionSizeMap = regionSizeMap;
  }

  /**
   * Returns the total size of the given region in bytes.
   * @param encodedRegionName the encoded region name
   * @return the size of the region, or 0 if the region is not present in the snapshot
   */
  public long getRegionSize(String encodedRegionName) {
    return regionSizeMap.getOrDefault(encodedRegionName, 0L);
  }

  /**
   * Returns an unmodifiable view of the region size map.
   * @return a map of region encoded names to their total size in bytes
   */
  public Map<String, Long> getRegionSizeMap() {
    return Collections.unmodifiableMap(regionSizeMap);
  }
}
