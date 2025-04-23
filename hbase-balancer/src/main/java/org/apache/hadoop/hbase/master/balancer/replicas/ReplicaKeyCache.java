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
package org.apache.hadoop.hbase.master.balancer.replicas;

import java.time.Duration;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Suppliers;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hbase.thirdparty.com.google.common.cache.LoadingCache;

@InterfaceAudience.Private
public final class ReplicaKeyCache implements Configurable {
  /**
   * ReplicaKey creation is expensive if you have lots of regions. If your HMaster has adequate
   * memory, and you would like balancing to be faster, then you can turn on this flag to cache
   * ReplicaKey objects.
   */
  public static final String CACHE_REPLICA_KEYS_KEY =
    "hbase.replica.distribution.conditional.cacheReplicaKeys";
  public static final boolean CACHE_REPLICA_KEYS_DEFAULT = false;

  /**
   * If memory is available, then set this to a value greater than your region count to maximize
   * replica distribution performance.
   */
  public static final String REPLICA_KEY_CACHE_SIZE_KEY =
    "hbase.replica.distribution.conditional.replicaKeyCacheSize";
  public static final int REPLICA_KEY_CACHE_SIZE_DEFAULT = 1000;

  private static final Supplier<ReplicaKeyCache> INSTANCE = Suppliers.memoize(ReplicaKeyCache::new);

  private volatile LoadingCache<RegionInfo, ReplicaKey> replicaKeyCache = null;

  private Configuration conf;

  public static ReplicaKeyCache getInstance() {
    return INSTANCE.get();
  }

  private ReplicaKeyCache() {
  }

  public ReplicaKey getReplicaKey(RegionInfo regionInfo) {
    return replicaKeyCache == null
      ? new ReplicaKey(regionInfo)
      : replicaKeyCache.getUnchecked(regionInfo);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    boolean cacheKeys = conf.getBoolean(CACHE_REPLICA_KEYS_KEY, CACHE_REPLICA_KEYS_DEFAULT);
    if (cacheKeys && replicaKeyCache == null) {
      int replicaKeyCacheSize =
        conf.getInt(REPLICA_KEY_CACHE_SIZE_KEY, REPLICA_KEY_CACHE_SIZE_DEFAULT);
      replicaKeyCache = CacheBuilder.newBuilder().maximumSize(replicaKeyCacheSize)
        .expireAfterAccess(Duration.ofMinutes(30)).build(new CacheLoader<RegionInfo, ReplicaKey>() {
          @Override
          public ReplicaKey load(RegionInfo regionInfo) {
            return new ReplicaKey(regionInfo);
          }
        });
    } else if (!cacheKeys) {
      replicaKeyCache = null;
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
