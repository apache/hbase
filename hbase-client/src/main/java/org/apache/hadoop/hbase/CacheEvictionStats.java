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
package org.apache.hadoop.hbase;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Public
public final class CacheEvictionStats {

  private final long evictedBlocks;
  private final long maxCacheSize;
  private final Map<byte[], Throwable> exceptions;

  CacheEvictionStats(CacheEvictionStatsBuilder builder) {
    this.evictedBlocks = builder.evictedBlocks;
    this.maxCacheSize = builder.maxCacheSize;
    this.exceptions = builder.exceptions;
  }

  public long getEvictedBlocks() {
    return evictedBlocks;
  }

  public long getMaxCacheSize() {
    return maxCacheSize;
  }

  public Map<byte[], Throwable> getExceptions() {
    return Collections.unmodifiableMap(exceptions);
  }

  public int getExceptionCount() {
    return exceptions.size();
  }

  private String getFailedRegions() {
    return exceptions.keySet().stream()
      .map(regionName -> RegionInfo.prettyPrint(RegionInfo.encodeRegionName(regionName)))
      .collect(Collectors.toList()).toString();
  }

  @InterfaceAudience.Private
  public static CacheEvictionStatsBuilder builder() {
    return new CacheEvictionStatsBuilder();
  }

  @Override
  public String toString() {
    return "CacheEvictionStats{" + "evictedBlocks=" + evictedBlocks + ", maxCacheSize="
      + maxCacheSize + ", failedRegionsSize=" + getExceptionCount() + ", failedRegions="
      + getFailedRegions() + '}';
  }
}
