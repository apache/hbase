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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mortbay.log.Log;

/**
 * Tests that {@link CacheConfig} does as expected.
 */
@Category(SmallTests.class)
public class TestCacheConfig {
  private Configuration conf;

  static class DataCacheEntry implements Cacheable {
    private static final int SIZE = 1;
    private static DataCacheEntry SINGLETON = new DataCacheEntry();

    private final CacheableDeserializer<Cacheable> deserializer =
        new CacheableDeserializer<Cacheable>() {
      @Override
      public int getDeserialiserIdentifier() {
        return 0;
      }
      
      @Override
      public Cacheable deserialize(ByteBuffer b, boolean reuse) throws IOException {
        Log.info("Deserialized " + b + ", reuse=" + reuse);
        return SINGLETON;
      }
      
      @Override
      public Cacheable deserialize(ByteBuffer b) throws IOException {
        Log.info("Deserialized " + b);
        return SINGLETON;
      }
    };

    public String toString() {
      return "size=" + SIZE + ", type=" + getBlockType();
    };
 
    @Override
    public long heapSize() {
      return SIZE;
    }

    @Override
    public int getSerializedLength() {
      return SIZE;
    }

    @Override
    public void serialize(ByteBuffer destination) {
      Log.info("Serialized " + this + " to " + destination);
    }

    @Override
    public CacheableDeserializer<Cacheable> getDeserializer() {
      return this.deserializer;
    }

    @Override
    public BlockType getBlockType() {
      return BlockType.DATA;
    }
  };

  static class MetaCacheEntry extends DataCacheEntry {
    @Override
    public BlockType getBlockType() {
      return BlockType.INTERMEDIATE_INDEX;
    }
  }

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

  /**
   * @param cc
   * @param doubling If true, addition of element ups counter by 2, not 1, because element added
   * to onheap and offheap caches.
   * @param sizing True if we should run sizing test (doesn't always apply).
   */
  private void basicBlockCacheOps(final CacheConfig cc, final boolean doubling,
      final boolean sizing) {
    assertTrue(cc.isBlockCacheEnabled());
    assertTrue(CacheConfig.DEFAULT_IN_MEMORY == cc.isInMemory());
    BlockCache bc = cc.getBlockCache();
    BlockCacheKey bck = new BlockCacheKey("f", 0);
    Cacheable c = new DataCacheEntry();
    // Do asserts on block counting.
    long initialBlockCount = bc.getBlockCount();
    bc.cacheBlock(bck, c);
    assertEquals(doubling? 2: 1, bc.getBlockCount() - initialBlockCount);
    bc.evictBlock(bck);
    assertEquals(initialBlockCount, bc.getBlockCount());
    // Do size accounting.  Do it after the above 'warm-up' because it looks like some
    // buffers do lazy allocation so sizes are off on first go around.
    if (sizing) {
      long originalSize = bc.getCurrentSize();
      bc.cacheBlock(bck, c);
      long size = bc.getCurrentSize();
      assertTrue(bc.getCurrentSize() > originalSize);
      bc.evictBlock(bck);
      size = bc.getCurrentSize();
      assertEquals(originalSize, size);
    }
  }

  @Test
  public void testCacheConfigDefaultLRUBlockCache() {
    CacheConfig cc = new CacheConfig(this.conf);
    assertTrue(cc.isBlockCacheEnabled());
    assertTrue(CacheConfig.DEFAULT_IN_MEMORY == cc.isInMemory());
    basicBlockCacheOps(cc, false, true);
    assertTrue(cc.getBlockCache() instanceof LruBlockCache);
  }

  @Test
  public void testSlabCacheConfig() {
    this.conf.setFloat(CacheConfig.SLAB_CACHE_OFFHEAP_PERCENTAGE_KEY, 0.1f);
    CacheConfig cc = new CacheConfig(this.conf);
    basicBlockCacheOps(cc, true, true);
    assertTrue(cc.getBlockCache() instanceof DoubleBlockCache);
    // TODO Assert sizes allocated are right.
  }

  @Test
  public void testBucketCacheConfig() {
    this.conf.set(CacheConfig.BUCKET_CACHE_IOENGINE_KEY, "offheap");
    this.conf.setInt(CacheConfig.BUCKET_CACHE_SIZE_KEY, 100);
    this.conf.setFloat(CacheConfig.BUCKET_CACHE_COMBINED_PERCENTAGE_KEY, 0.8f);
    CacheConfig cc = new CacheConfig(this.conf);
    basicBlockCacheOps(cc, false, false);
    assertTrue(cc.getBlockCache() instanceof CombinedBlockCache);
    // TODO: Assert sizes allocated are right and proportions.
  }
}
