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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

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
  public static final int maxNumLookupPeriods = 20;

  private static final TunerResult TUNER_RESULT = new TunerResult(true);
  private static final TunerResult NO_OP_TUNER_RESULT = new TunerResult(false);

  private Configuration conf;
  private float step = DEFAULT_STEP_VALUE;
  private Queue<Long> prevWriteCounts = new LinkedList<Long>();
  private Queue<Long> prevReadCounts = new LinkedList<Long>();
  private int lookupCounts = 0;

  private float globalMemStorePercentMinRange;
  private float globalMemStorePercentMaxRange;
  private float blockCachePercentMinRange;
  private float blockCachePercentMaxRange;

  private boolean stepDirection;  // true if last time tuner increased block cache size 
  private boolean isFirstTuning = true;
  private long prevFlushCount;
  private long prevEvictCount;

  
  @Override
  public TunerResult tune(TunerContext context) {
    long blockedFlushCount = context.getBlockedFlushCount();
    long unblockedFlushCount = context.getUnblockedFlushCount();
    long evictCount = context.getEvictCount();
    long writeRequestCount = context.getWriteRequestCount();
    long readRequestCount = context.getReadRequestCount();
    boolean memstoreSufficient = blockedFlushCount == 0 && unblockedFlushCount == 0;
    boolean blockCacheSufficient = evictCount == 0;
    boolean loadSenario = checkLoadSenario(writeRequestCount,readRequestCount);
    if (memstoreSufficient && blockCacheSufficient) {
      prevFlushCount = blockedFlushCount + unblockedFlushCount;
      prevEvictCount = evictCount;
      return NO_OP_TUNER_RESULT;
    }
    float newMemstoreSize;
    float newBlockCacheSize;
    if (memstoreSufficient) {
      // Increase the block cache size and corresponding decrease in memstore size
      stepDirection = true;
    } else if (blockCacheSufficient) {
      // Increase the memstore size and corresponding decrease in block cache size
      stepDirection = false;
    } else if(!isFirstTuning) {
	  float percentChangeInEvictCount  = (float)(evictCount-prevEvictCount)/(float)(prevEvictCount);
	  float percentChangeInFlushes =
	  (float)(blockedFlushCount + unblockedFlushCount-prevFlushCount)/(float)(prevFlushCount);
	  //Negative is desirable , should repeat previous step
	  //if it is positive , we should move in opposite direction
	  if (percentChangeInEvictCount + percentChangeInFlushes > 0.0) {
		//revert last step if it went wrong
		stepDirection = !stepDirection;
	  } else {
		//last step was useful, taking step based on current stats
		stepDirection = loadSenario;
	  }
    } else {
      stepDirection = loadSenario;
    }
    
    if (stepDirection){
      newBlockCacheSize = context.getCurBlockCacheSize() + step;
      newMemstoreSize = context.getCurMemStoreSize() - step;
    } else {
	  newBlockCacheSize = context.getCurBlockCacheSize() - step;
	  newMemstoreSize = context.getCurMemStoreSize() + step;
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
    prevFlushCount = blockedFlushCount + unblockedFlushCount;
    prevEvictCount = evictCount;
    isFirstTuning = false;
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
  /*
   * @Returns true if read it seems its getting read heavy
   * and need to increase block cache size
   */
  private boolean checkLoadSenario(long writeRequestCount , long readRequestCount) {
	  lookupCounts++;
	  prevWriteCounts.offer(writeRequestCount);
	  prevReadCounts.offer(readRequestCount);
	  Iterator<Long> readCountIterator = prevReadCounts.iterator();
	  Iterator<Long> writeCountIterator = prevWriteCounts.iterator();
	  int loadCount = 0;
	  while(readCountIterator.hasNext() && writeCountIterator.hasNext()){
		  if (readCountIterator.next() > writeCountIterator.next()) {
			 loadCount++;
		  } else {
			loadCount--;
		  }
		}
	  if (lookupCounts > maxNumLookupPeriods){
		 prevWriteCounts.poll();
		 prevReadCounts.poll();
	  }
	  return (loadCount>=0);
  }
}
