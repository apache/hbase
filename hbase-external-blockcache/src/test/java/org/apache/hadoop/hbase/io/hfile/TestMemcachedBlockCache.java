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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import net.spy.memcached.CachedData;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.transcoders.Transcoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.io.hfile.CacheTestUtils.HFileBlockPair;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ IOTests.class, SmallTests.class })
public class TestMemcachedBlockCache {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMemcachedBlockCache.class);

  private MemcachedBlockCache cache;

  private ConcurrentMap<String, CachedData> backingMap;

  @Before
  public void setup() throws Exception {
    int port = ThreadLocalRandom.current().nextInt(1024, 65536);
    Configuration conf = new Configuration();
    conf.set("hbase.cache.memcached.servers", "localhost:" + port);
    backingMap = new ConcurrentHashMap<>();
    cache = new MemcachedBlockCache(conf) {

      private <T> OperationFuture<T> createFuture(String key, long opTimeout, T result) {
        OperationFuture<T> future =
          new OperationFuture<>(key, new CountDownLatch(0), opTimeout, ForkJoinPool.commonPool());
        Operation op = mock(Operation.class);
        when(op.getState()).thenReturn(OperationState.COMPLETE);
        future.setOperation(op);
        future.set(result, new OperationStatus(true, ""));

        return future;
      }

      @Override
      protected MemcachedClient createMemcachedClient(ConnectionFactory factory,
        List<InetSocketAddress> serverAddresses) throws IOException {
        assertEquals(FailureMode.Redistribute, factory.getFailureMode());
        assertTrue(factory.isDaemon());
        assertFalse(factory.useNagleAlgorithm());
        assertEquals(MAX_SIZE, factory.getReadBufSize());
        assertEquals(1, serverAddresses.size());
        assertEquals("localhost", serverAddresses.get(0).getHostName());
        assertEquals(port, serverAddresses.get(0).getPort());
        MemcachedClient client = mock(MemcachedClient.class);
        when(client.set(anyString(), anyInt(), any(), any())).then(inv -> {
          String key = inv.getArgument(0);
          HFileBlock block = inv.getArgument(2);
          Transcoder<HFileBlock> tc = inv.getArgument(3);
          CachedData cd = tc.encode(block);
          backingMap.put(key, cd);
          return createFuture(key, factory.getOperationTimeout(), true);
        });
        when(client.delete(anyString())).then(inv -> {
          String key = inv.getArgument(0);
          backingMap.remove(key);
          return createFuture(key, factory.getOperationTimeout(), true);
        });
        when(client.get(anyString(), any())).then(inv -> {
          String key = inv.getArgument(0);
          Transcoder<HFileBlock> tc = inv.getArgument(1);
          CachedData cd = backingMap.get(key);
          return tc.decode(cd);
        });
        return client;
      }
    };
  }

  @Test
  public void testCache() throws Exception {
    final int numBlocks = 10;
    HFileBlockPair[] blocks =
      CacheTestUtils.generateHFileBlocks(HConstants.DEFAULT_BLOCKSIZE, numBlocks);
    for (int i = 0; i < numBlocks; i++) {
      cache.cacheBlock(blocks[i].getBlockName(), blocks[i].getBlock());
    }
    Waiter.waitFor(new Configuration(), 10000, () -> backingMap.size() == numBlocks);
    for (int i = 0; i < numBlocks; i++) {
      HFileBlock actual = (HFileBlock) cache.getBlock(blocks[i].getBlockName(), false, false, true);
      HFileBlock expected = blocks[i].getBlock();
      assertEquals(expected.getBlockType(), actual.getBlockType());
      assertEquals(expected.getSerializedLength(), actual.getSerializedLength());
    }

    CacheTestUtils.testConvertToJSON(cache);
  }

  @Test
  public void testEviction() throws Exception {
    final int numBlocks = 10;
    HFileBlockPair[] blocks =
      CacheTestUtils.generateHFileBlocks(HConstants.DEFAULT_BLOCKSIZE, numBlocks);
    for (int i = 0; i < numBlocks; i++) {
      cache.cacheBlock(blocks[i].getBlockName(), blocks[i].getBlock());
    }
    Waiter.waitFor(new Configuration(), 10000, () -> backingMap.size() == numBlocks);
    for (int i = 0; i < numBlocks; i++) {
      cache.evictBlock(blocks[i].getBlockName());
    }
    Waiter.waitFor(new Configuration(), 10000, () -> backingMap.size() == 0);
  }

}
