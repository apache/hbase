/**
 *
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

import java.util.HashMap;
import java.util.Map;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class CacheEvictionStatsBuilder {
  long evictedBlocks = 0;
  long maxCacheSize = 0;
  Map<byte[], Throwable> exceptions = new HashMap<>();

  CacheEvictionStatsBuilder() {
  }

  public CacheEvictionStatsBuilder withEvictedBlocks(long evictedBlocks) {
    this.evictedBlocks = evictedBlocks;
    return this;
  }

  public CacheEvictionStatsBuilder withMaxCacheSize(long maxCacheSize) {
    this.maxCacheSize = maxCacheSize;
    return this;
  }

  public void addException(byte[] regionName, Throwable ie){
    exceptions.put(regionName, ie);
  }

  public CacheEvictionStatsBuilder append(CacheEvictionStats stats) {
    this.evictedBlocks += stats.getEvictedBlocks();
    this.maxCacheSize += stats.getMaxCacheSize();
    this.exceptions.putAll(stats.getExceptions());
    return this;
  }

  public CacheEvictionStats build() {
    return new CacheEvictionStats(this);
  }
}
