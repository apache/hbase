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
package org.apache.hadoop.hbase.io.hfile.bucket;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.RAMCache;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.RAMQueueEntry;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ IOTests.class, SmallTests.class })
public class TestRAMCache {
  private static final Logger LOG = LoggerFactory.getLogger(TestRAMCache.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRAMCache.class);

  // Define a mock HFileBlock.
  private static class MockHFileBlock extends HFileBlock {

    private volatile CountDownLatch latch;

    MockHFileBlock(BlockType blockType, int onDiskSizeWithoutHeader,
        int uncompressedSizeWithoutHeader, long prevBlockOffset, ByteBuffer b, boolean fillHeader,
        long offset, int nextBlockOnDiskSize, int onDiskDataSizeWithHeader,
        HFileContext fileContext, ByteBuffAllocator allocator) {
      super(blockType, onDiskSizeWithoutHeader, uncompressedSizeWithoutHeader, prevBlockOffset,
          ByteBuff.wrap(b), fillHeader, offset, nextBlockOnDiskSize, onDiskDataSizeWithHeader,
          fileContext, allocator);
    }

    public void setLatch(CountDownLatch latch) {
      this.latch = latch;
    }

    public MockHFileBlock retain() {
      try {
        if (latch != null) {
          latch.await();
        }
      } catch (InterruptedException e) {
        LOG.info("Interrupted exception error: ", e);
      }
      super.retain();
      return this;
    }
  }

  @Test
  public void testAtomicRAMCache() throws Exception {
    int size = 100;
    int length = HConstants.HFILEBLOCK_HEADER_SIZE + size;
    byte[] byteArr = new byte[length];

    RAMCache cache = new RAMCache();
    BlockCacheKey key = new BlockCacheKey("file-1", 1);
    MockHFileBlock blk = new MockHFileBlock(BlockType.DATA, size, size, -1,
        ByteBuffer.wrap(byteArr, 0, size), HFileBlock.FILL_HEADER, -1, 52, -1,
        new HFileContextBuilder().build(), ByteBuffAllocator.HEAP);
    RAMQueueEntry re = new RAMQueueEntry(key, blk, 1, false);

    Assert.assertNull(cache.putIfAbsent(key, re));
    Assert.assertEquals(cache.putIfAbsent(key, re), re);

    CountDownLatch latch = new CountDownLatch(1);
    blk.setLatch(latch);

    AtomicBoolean error = new AtomicBoolean(false);
    Thread t1 = new Thread(() -> {
      try {
        cache.get(key);
      } catch (Exception e) {
        error.set(true);
      }
    });
    t1.start();
    Thread.sleep(200);

    AtomicBoolean removed = new AtomicBoolean(false);
    Thread t2 = new Thread(() -> {
      cache.remove(key);
      removed.set(true);
    });
    t2.start();
    Thread.sleep(200);
    Assert.assertFalse(removed.get());

    latch.countDown();
    Thread.sleep(200);
    Assert.assertTrue(removed.get());
    Assert.assertFalse(error.get());
  }
}
