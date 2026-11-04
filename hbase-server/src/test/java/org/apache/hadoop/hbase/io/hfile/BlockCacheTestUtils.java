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

import static org.apache.hadoop.hbase.io.ByteBuffAllocator.HEAP;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.hfile.cache.CacheEngine;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BlockCacheTestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(BlockCacheTestUtils.class);

  private BlockCacheTestUtils() {
  }

  static void testMultiThreadGetAndEvictBlockInternal(BlockCache cache) throws Exception {
    int size = 100;
    int length = HConstants.HFILEBLOCK_HEADER_SIZE + size;
    byte[] byteArr = new byte[length];
    HFileContext meta = new HFileContextBuilder().build();
    BlockCacheKey key = new BlockCacheKey("key1", 0);
    HFileBlock blk = new HFileBlock(BlockType.DATA, size, size, -1,
      ByteBuff.wrap(ByteBuffer.wrap(byteArr, 0, size)), HFileBlock.FILL_HEADER, -1, 52, -1, meta,
      HEAP);
    AtomicBoolean err1 = new AtomicBoolean(false);
    Thread t1 = new Thread(() -> {
      for (int i = 0; i < 10000 && !err1.get(); i++) {
        try {
          cache.getBlock(key, false, false, true);
        } catch (Exception e) {
          err1.set(true);
          LOG.info("Cache block or get block failure: ", e);
        }
      }
    });

    AtomicBoolean err2 = new AtomicBoolean(false);
    Thread t2 = new Thread(() -> {
      for (int i = 0; i < 10000 && !err2.get(); i++) {
        try {
          cache.evictBlock(key);
        } catch (Exception e) {
          err2.set(true);
          LOG.info("Evict block failure: ", e);
        }
      }
    });

    AtomicBoolean err3 = new AtomicBoolean(false);
    Thread t3 = new Thread(() -> {
      for (int i = 0; i < 10000 && !err3.get(); i++) {
        try {
          cache.cacheBlock(key, blk);
        } catch (Exception e) {
          err3.set(true);
          LOG.info("Cache block failure: ", e);
        }
      }
    });
    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();
    assertFalse(err1.get());
    assertFalse(err2.get());
    assertFalse(err3.get());
  }

  public static void testMultiThreadGetAndEvictBlockInternal(CacheEngine cache) throws Exception {
    int size = 100;
    int length = HConstants.HFILEBLOCK_HEADER_SIZE + size;
    byte[] byteArr = new byte[length];
    HFileContext meta = new HFileContextBuilder().build();
    BlockCacheKey key = new BlockCacheKey("key1", 0);
    HFileBlock blk = new HFileBlock(BlockType.DATA, size, size, -1,
      ByteBuff.wrap(ByteBuffer.wrap(byteArr, 0, size)), HFileBlock.FILL_HEADER, -1, 52, -1, meta,
      HEAP);
    AtomicBoolean err1 = new AtomicBoolean(false);
    Thread t1 = new Thread(() -> {
      for (int i = 0; i < 10000 && !err1.get(); i++) {
        try {
          cache.getBlock(key, false, false, true);
        } catch (Exception e) {
          err1.set(true);
          LOG.info("Cache block or get block failure: ", e);
        }
      }
    });

    AtomicBoolean err2 = new AtomicBoolean(false);
    Thread t2 = new Thread(() -> {
      for (int i = 0; i < 10000 && !err2.get(); i++) {
        try {
          cache.evictBlock(key);
        } catch (Exception e) {
          err2.set(true);
          LOG.info("Evict block failure: ", e);
        }
      }
    });

    AtomicBoolean err3 = new AtomicBoolean(false);
    Thread t3 = new Thread(() -> {
      for (int i = 0; i < 10000 && !err3.get(); i++) {
        try {
          cache.cacheBlock(key, blk);
        } catch (Exception e) {
          err3.set(true);
          LOG.info("Cache block failure: ", e);
        }
      }
    });
    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();
    assertFalse(err1.get());
    assertFalse(err2.get());
    assertFalse(err3.get());
  }

}
