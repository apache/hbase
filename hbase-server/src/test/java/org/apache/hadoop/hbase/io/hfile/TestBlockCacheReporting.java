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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.hfile.TestCacheConfig.DataCacheEntry;
import org.apache.hadoop.hbase.io.hfile.TestCacheConfig.IndexCacheEntry;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({IOTests.class, SmallTests.class})
public class TestBlockCacheReporting {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBlockCacheReporting.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestBlockCacheReporting.class);
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    this.conf = HBaseConfiguration.create();
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
  public void testBucketCache() throws IOException {
    this.conf.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
    this.conf.setInt(HConstants.BUCKET_CACHE_SIZE_KEY, 100);
    BlockCache blockCache = BlockCacheFactory.createBlockCache(this.conf);
    assertTrue(blockCache instanceof CombinedBlockCache);
    logPerBlock(blockCache);
    final int count = 3;
    addDataAndHits(blockCache, count);
    // The below has no asserts.  It is just exercising toString and toJSON code.
    LOG.info(Objects.toString(blockCache.getStats()));
    BlockCacheUtil.CachedBlocksByFile cbsbf = logPerBlock(blockCache);
    LOG.info(Objects.toString(cbsbf));
    logPerFile(cbsbf);
    bucketCacheReport(blockCache);
    LOG.info(BlockCacheUtil.toJSON(cbsbf));
  }

  @Test
  public void testLruBlockCache() throws IOException {
    CacheConfig cc = new CacheConfig(this.conf);
    assertTrue(CacheConfig.DEFAULT_IN_MEMORY == cc.isInMemory());
    BlockCache blockCache = BlockCacheFactory.createBlockCache(this.conf);
    logPerBlock(blockCache);
    addDataAndHits(blockCache, 3);
    // The below has no asserts.  It is just exercising toString and toJSON code.
    LOG.info("count=" + blockCache.getBlockCount() + ", currentSize=" + blockCache.getCurrentSize()
        + ", freeSize=" + blockCache.getFreeSize());
    LOG.info(Objects.toString(blockCache.getStats()));
    BlockCacheUtil.CachedBlocksByFile cbsbf = logPerBlock(blockCache);
    LOG.info(Objects.toString(cbsbf));
    logPerFile(cbsbf);
    bucketCacheReport(blockCache);
    LOG.info(BlockCacheUtil.toJSON(cbsbf));
  }

  private void bucketCacheReport(final BlockCache bc) {
    LOG.info(bc.getClass().getSimpleName() + ": " + bc.getStats());
    BlockCache [] bcs = bc.getBlockCaches();
    if (bcs != null) {
      for (BlockCache sbc: bc.getBlockCaches()) {
        LOG.info(bc.getClass().getSimpleName() + ": " + sbc.getStats());
      }
    }
  }

  private void logPerFile(final BlockCacheUtil.CachedBlocksByFile cbsbf) throws IOException {
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
      //LOG.info(BlockCacheUtil.toJSON(e.getKey(), e.getValue()));
    }
  }

  private BlockCacheUtil.CachedBlocksByFile logPerBlock(final BlockCache bc) throws IOException {
    BlockCacheUtil.CachedBlocksByFile cbsbf = new BlockCacheUtil.CachedBlocksByFile();
    for (CachedBlock cb : bc) {
      LOG.info(cb.toString());
      //LOG.info(BlockCacheUtil.toJSON(bc));
      cbsbf.update(cb);
    }
    return cbsbf;
  }
}
