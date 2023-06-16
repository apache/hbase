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

import static org.junit.Assert.assertEquals;

import com.thimbleware.jmemcached.CacheElement;
import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap.EvictionPolicy;
import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.io.hfile.CacheTestUtils.HFileBlockPair;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ IOTests.class, SmallTests.class })
public class TestMemcachedBlockCache {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMemcachedBlockCache.class);

  static MemCacheDaemon<? extends CacheElement> MEMCACHED;
  static MemcachedBlockCache CACHE;

  @Before
  public void before() throws Exception {
    MEMCACHED.getCache().flush_all();
    assertEquals("Memcache is not empty", MEMCACHED.getCache().getCurrentItems(), 0);
  }

  @BeforeClass
  public static void setup() throws Exception {
    int port = HBaseTestingUtil.randomFreePort();
    MEMCACHED = createDaemon(port);
    Configuration conf = new Configuration();
    conf.set("hbase.cache.memcached.servers", "localhost:" + port);
    CACHE = new MemcachedBlockCache(conf);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (MEMCACHED != null) {
      MEMCACHED.stop();
    }
  }

  @Test
  public void testCache() throws Exception {
    final int NUM_BLOCKS = 10;
    HFileBlockPair[] blocks =
      CacheTestUtils.generateHFileBlocks(HConstants.DEFAULT_BLOCKSIZE, NUM_BLOCKS);
    for (int i = 0; i < NUM_BLOCKS; i++) {
      CACHE.cacheBlock(blocks[i].getBlockName(), blocks[i].getBlock());
    }
    Waiter.waitFor(new Configuration(), 10000,
      () -> MEMCACHED.getCache().getCurrentItems() == NUM_BLOCKS);
  }

  @Test
  public void testEviction() throws Exception {
    final int NUM_BLOCKS = 10;
    HFileBlockPair[] blocks =
      CacheTestUtils.generateHFileBlocks(HConstants.DEFAULT_BLOCKSIZE, NUM_BLOCKS);
    for (int i = 0; i < NUM_BLOCKS; i++) {
      CACHE.cacheBlock(blocks[i].getBlockName(), blocks[i].getBlock());
    }
    Waiter.waitFor(new Configuration(), 10000,
      () -> MEMCACHED.getCache().getCurrentItems() == NUM_BLOCKS);
    for (int i = 0; i < NUM_BLOCKS; i++) {
      CACHE.evictBlock(blocks[i].getBlockName());
    }
    Waiter.waitFor(new Configuration(), 10000, () -> MEMCACHED.getCache().getCurrentItems() == 0);
  }

  private static MemCacheDaemon<? extends CacheElement> createDaemon(int port) {
    InetSocketAddress addr = new InetSocketAddress("localhost", port);
    MemCacheDaemon<LocalCacheElement> daemon = new MemCacheDaemon<LocalCacheElement>();
    ConcurrentLinkedHashMap<Key, LocalCacheElement> cacheStorage =
      ConcurrentLinkedHashMap.create(EvictionPolicy.LRU, 1000, 1024 * 1024);
    daemon.setCache(new CacheImpl(cacheStorage));
    daemon.setAddr(addr);
    daemon.setVerbose(true);
    daemon.start();
    while (!daemon.isRunning()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    return daemon;
  }

}
