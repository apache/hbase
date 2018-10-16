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
package org.apache.hadoop.hbase.mob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;

/**
 * The cache configuration for the mob.
 */
@InterfaceAudience.Private
public class MobCacheConfig extends CacheConfig {

  private static MobFileCache mobFileCache;

  public MobCacheConfig(Configuration conf, ColumnFamilyDescriptor family) {
    super(conf, family);
    instantiateMobFileCache(conf);
  }

  public MobCacheConfig(Configuration conf) {
    super(conf);
    instantiateMobFileCache(conf);
  }

  public MobCacheConfig(Configuration conf, boolean needBlockCache) {
    super(conf, needBlockCache);
    instantiateMobFileCache(conf);
  }

  /**
   * Instantiates the MobFileCache.
   * @param conf The current configuration.
   * @return The current instance of MobFileCache.
   */
  public static synchronized MobFileCache instantiateMobFileCache(Configuration conf) {
    if (mobFileCache == null) {
      mobFileCache = new MobFileCache(conf);
    }
    return mobFileCache;
  }

  /**
   * Gets the MobFileCache.
   * @return The MobFileCache.
   */
  public MobFileCache getMobFileCache() {
    return mobFileCache;
  }
}
