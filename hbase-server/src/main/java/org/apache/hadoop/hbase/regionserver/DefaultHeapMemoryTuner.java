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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.regionserver.HeapMemoryManager.BLOCK_CACHE_SIZE_MAX_RANGE_KEY;
import static org.apache.hadoop.hbase.regionserver.HeapMemoryManager.BLOCK_CACHE_SIZE_MIN_RANGE_KEY;
import static org.apache.hadoop.hbase.HConstants.HFILE_BLOCK_CACHE_SIZE_KEY;
import static org.apache.hadoop.hbase.regionserver.HeapMemoryManager.MEMSTORE_SIZE_MAX_RANGE_KEY;
import static org.apache.hadoop.hbase.regionserver.HeapMemoryManager.MEMSTORE_SIZE_MIN_RANGE_KEY;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.util.HeapMemorySizeUtil;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager.TunerContext;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager.TunerResult;

/**
 * The default implementation for the HeapMemoryTuner. This will do simple checks to decide
 * whether there should be changes in the heap size of memstore/block cache. When there is no block
 * cache eviction at all but there are flushes because of global heap pressure, it will increase the
 * memstore heap size and decrease block cache size. The step value for this heap size change can be
 * specified using the config <i>hbase.regionserver.heapmemory.autotuner.step</i>. When there is no
 * memstore flushes because of heap pressure but there is block cache evictions it will increase the
 * block cache heap.
 */
@InterfaceAudience.Private
class DefaultHeapMemoryTuner implements HeapMemoryTuner {

  public static final String STEP_KEY = "hbase.regionserver.heapmemory.autotuner.step";
  public static final float DEFAULT_STEP_VALUE = 0.02f; // 2%

  private static final TunerResult TUNER_RESULT = new TunerResult(true);
  private static final TunerResult NO_OP_TUNER_RESULT = new TunerResult(false);

  private Configuration conf;
  private float step = DEFAULT_STEP_VALUE;

  private float globalMemStorePercentMinRange;
  private float globalMemStorePercentMaxRange;
  private float blockCachePercentMinRange;
  private float blockCachePercentMaxRange;

  @Override
  public TunerResult tune(TunerContext context) {
    long blockedFlushCount = context.getBlockedFlushCount();
    long unblockedFlushCount = context.getUnblockedFlushCount();
    long evictCount = context.getEvictCount();
    boolean memstoreSufficient = blockedFlushCount == 0 && unblockedFlushCount == 0;
    boolean blockCacheSufficient = evictCount == 0;
    if (memstoreSufficient && blockCacheSufficient) {
      return NO_OP_TUNER_RESULT;
    }
    float newMemstoreSize;
    float newBlockCacheSize;
    if (memstoreSufficient) {
      // Increase the block cache size and corresponding decrease in memstore size
      newBlockCacheSize = context.getCurBlockCacheSize() + step;
      newMemstoreSize = context.getCurMemStoreSize() - step;
    } else if (blockCacheSufficient) {
      // Increase the memstore size and corresponding decrease in block cache size
      newBlockCacheSize = context.getCurBlockCacheSize() - step;
      newMemstoreSize = context.getCurMemStoreSize() + step;
    } else {
      return NO_OP_TUNER_RESULT;
      // As of now not making any tuning in write/read heavy scenario.
    }
    if (newMemstoreSize > globalMemStorePercentMaxRange) {
      newMemstoreSize = globalMemStorePercentMaxRange;
    } else if (newMemstoreSize < globalMemStorePercentMinRange) {
      newMemstoreSize = globalMemStorePercentMinRange;
    }
    if (newBlockCacheSize > blockCachePercentMaxRange) {
      newBlockCacheSize = blockCachePercentMaxRange;
    } else if (newBlockCacheSize < blockCachePercentMinRange) {
      newBlockCacheSize = blockCachePercentMinRange;
    }
    TUNER_RESULT.setBlockCacheSize(newBlockCacheSize);
    TUNER_RESULT.setMemstoreSize(newMemstoreSize);
    return TUNER_RESULT;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.step = conf.getFloat(STEP_KEY, DEFAULT_STEP_VALUE);
    this.blockCachePercentMinRange = conf.getFloat(BLOCK_CACHE_SIZE_MIN_RANGE_KEY,
        conf.getFloat(HFILE_BLOCK_CACHE_SIZE_KEY, HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT));
    this.blockCachePercentMaxRange = conf.getFloat(BLOCK_CACHE_SIZE_MAX_RANGE_KEY,
        conf.getFloat(HFILE_BLOCK_CACHE_SIZE_KEY, HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT));
    this.globalMemStorePercentMinRange = conf.getFloat(MEMSTORE_SIZE_MIN_RANGE_KEY,
        HeapMemorySizeUtil.getGlobalMemStorePercent(conf, false));
    this.globalMemStorePercentMaxRange = conf.getFloat(MEMSTORE_SIZE_MAX_RANGE_KEY,
        HeapMemorySizeUtil.getGlobalMemStorePercent(conf, false));
  }
}
