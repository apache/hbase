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
package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An on heap block cache implementation extended LruBlockCache and only cache index block.
 * This block cache should be only used by
 * {@link org.apache.hadoop.hbase.client.ClientSideRegionScanner} that normally considers to be
 * used by client resides out of the region server, e.g. a container of a map reduce job.
 **/
@InterfaceAudience.Private
public class IndexOnlyLruBlockCache extends LruBlockCache {

  public IndexOnlyLruBlockCache(long maxSize, long blockSize, boolean evictionThread,
    Configuration conf) {
    super(maxSize, blockSize, evictionThread, conf);
  }

  /**
   * Cache only index block with the specified name and buffer
   * @param cacheKey block's cache key
   * @param buf      block buffer
   * @param inMemory if block is in-memory
   */
  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory) {
    if (isMetaBlock(buf.getBlockType())) {
      super.cacheBlock(cacheKey, buf, inMemory);
    }
  }
}
