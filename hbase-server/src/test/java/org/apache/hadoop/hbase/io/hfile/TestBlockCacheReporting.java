/**
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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.io.hfile.TestCacheConfig.DataCacheEntry;
import org.apache.hadoop.hbase.io.hfile.TestCacheConfig.IndexCacheEntry;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IOTests.class, SmallTests.class})
public class TestBlockCacheReporting {
  private static final Log LOG = LogFactory.getLog(TestBlockCacheReporting.class);
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    CacheConfig.GLOBAL_BLOCK_CACHE_INSTANCE = null;
    this.conf = HBaseConfiguration.create();
  }

  @After
  public void tearDown() throws Exception {
    // Let go of current block cache.
    CacheConfig.GLOBAL_BLOCK_CACHE_INSTANCE = null;
  }

  private void addDataAndHits(final BlockCache bc, final int count) {
    Cacheable dce = new DataCacheEntry();
    Cacheable ice = new IndexCacheEntry();
    for (int i = 0; i < count; i++) {
      BlockCacheKey bckd = new BlockCacheKey("f", i);
      BlockCacheKey bcki = new BlockCacheKey("f", i + count);
      bc.getBlock(bckd, true, false, true);
      bc.cacheBlock(bckd, dce);
      bc.cacheBlock(bcki, ice);
      bc.getBlock(bckd, true, false, true);
      bc.getBlock(bcki, true, false, true);
    }
    assertEquals(2 * count /*Data and Index blocks*/, bc.getStats().getHitCount());
    BlockCacheKey bckd = new BlockCacheKey("f", 0);
    BlockCacheKey bcki = new BlockCacheKey("f", 0 + count);
    bc.evictBlock(bckd);
    bc.evictBlock(bcki);
    bc.getStats().getEvictedCount();
  }

  @Test
  public void testBucketCache() throws JsonGenerationException, JsonMappingException, IOException {
    this.conf.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
    this.conf.setInt(HConstants.BUCKET_CACHE_SIZE_KEY, 100);
    CacheConfig cc = new CacheConfig(this.conf);
    assertTrue(cc.getBlockCache() instanceof CombinedBlockCache);
    logPerBlock(cc.getBlockCache());
    final int count = 3;
    addDataAndHits(cc.getBlockCache(), count);
    // The below has no asserts.  It is just exercising toString and toJSON code.
    LOG.info(cc.getBlockCache().getStats());
    BlockCacheUtil.CachedBlocksByFile cbsbf = logPerBlock(cc.getBlockCache());
    LOG.info(cbsbf);
    logPerFile(cbsbf);
    bucketCacheReport(cc.getBlockCache());
    LOG.info(BlockCacheUtil.toJSON(cbsbf));
  }

  @Test
  public void testLruBlockCache() throws JsonGenerationException, JsonMappingException, IOException {
    CacheConfig cc = new CacheConfig(this.conf);
    assertTrue(cc.isBlockCacheEnabled());
    assertTrue(CacheConfig.DEFAULT_IN_MEMORY == cc.isInMemory());
    assertTrue(cc.getBlockCache() instanceof LruBlockCache);
    logPerBlock(cc.getBlockCache());
    addDataAndHits(cc.getBlockCache(), 3);
    // The below has no asserts.  It is just exercising toString and toJSON code.
    BlockCache bc = cc.getBlockCache();
    LOG.info("count=" + bc.getBlockCount() + ", currentSize=" + bc.getCurrentSize() +
        ", freeSize=" + bc.getFreeSize() );
    LOG.info(cc.getBlockCache().getStats());
    BlockCacheUtil.CachedBlocksByFile cbsbf = logPerBlock(cc.getBlockCache());
    LOG.info(cbsbf);
    logPerFile(cbsbf);
    bucketCacheReport(cc.getBlockCache());
    LOG.info(BlockCacheUtil.toJSON(cbsbf));
  }

  private void bucketCacheReport(final BlockCache bc) {
    LOG.info(bc.getClass().getSimpleName() + ": " + bc.getStats());
    BlockCache [] bcs = bc.getBlockCaches();
    if (bcs != null) {
      for (BlockCache sbc: bc.getBlockCaches()) {
        bucketCacheReport(sbc);
      }
    }
  }

  private void logPerFile(final BlockCacheUtil.CachedBlocksByFile cbsbf)
  throws JsonGenerationException, JsonMappingException, IOException {
    for (Map.Entry<String, NavigableSet<CachedBlock>> e:
        cbsbf.getCachedBlockStatsByFile().entrySet()) {
      int count = 0;
      long size = 0;
      int countData = 0;
      long sizeData = 0;
      for (CachedBlock cb: e.getValue()) {
        count++;
        size += cb.getSize();
        BlockType bt = cb.getBlockType();
        if (bt != null && bt.isData()) {
          countData++;
          sizeData += cb.getSize();
        }
      }
      LOG.info("filename=" + e.getKey() + ", count=" + count + ", countData=" + countData +
          ", size=" + size + ", sizeData=" + sizeData);
      LOG.info(BlockCacheUtil.toJSON(e.getKey(), e.getValue()));
    }
  }

  private BlockCacheUtil.CachedBlocksByFile logPerBlock(final BlockCache bc)
  throws JsonGenerationException, JsonMappingException, IOException {
    BlockCacheUtil.CachedBlocksByFile cbsbf = new BlockCacheUtil.CachedBlocksByFile();
    for (CachedBlock cb: bc) {
      LOG.info(cb.toString());
      LOG.info(BlockCacheUtil.toJSON(bc));
      cbsbf.update(cb);
    }
    return cbsbf;
  }
}