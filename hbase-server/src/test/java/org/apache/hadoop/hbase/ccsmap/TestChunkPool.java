/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.ccsmap;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ccsmap.ChunkPool.ChunkPoolParameters;
import org.apache.hadoop.hbase.testclassification.SmallTests;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestChunkPool {

  private Configuration conf = HBaseConfiguration.create();

  @Test
  public void testNormal() {
    conf.setLong(CCSMapUtils.CHUNK_CAPACITY_KEY, 8 * 1024 * 1024);
    conf.setInt(CCSMapUtils.CHUNK_SIZE_KEY, 4 * 1024);
    conf.setInt(CCSMapUtils.INITIAL_CHUNK_COUNT_KEY, Integer.MAX_VALUE);
    conf.setBoolean(CCSMapUtils.USE_OFFHEAP, true);
    ChunkPoolParameters parameters = new ChunkPoolParameters(conf);
    ChunkPool chunkPool = new ChunkPool(parameters);

    int numberOfChunk = chunkPool.getChunkQueue().size();
    Assert.assertEquals(2 * 1024, numberOfChunk);
    Assert.assertEquals(numberOfChunk, chunkPool.getChunkArray().length);
    Assert.assertEquals(2 * 1024, chunkPool.getCurrentChunkCounter());
    Assert.assertEquals(2 * 1024, chunkPool.getMaxChunkCount());

    AbstractHeapChunk chunk = chunkPool.allocate(4 * 1024 - 1);
    Assert.assertTrue(chunk.isPooledChunk());
    Assert.assertEquals(HeapMode.OFF_HEAP, chunk.getHeapMode());
    Assert.assertEquals(4 * 1024, chunk.getLimit());
    Assert.assertEquals(0, chunk.getChunkID());
    Assert.assertEquals(0, chunk.getPosition());
    Assert.assertEquals(2 * 1024 - 1, chunkPool.getChunkQueue().size());

    for (int i = 0; i < chunkPool.getChunkArray().length; i++) {
      Assert.assertEquals(i, chunkPool.getChunkArray()[i].getChunkID());
    }

    Assert.assertEquals(2 * 1024, chunkPool.getCurrentChunkCounter());
    Assert.assertEquals(0, chunkPool.getUnpooledChunkUsed());

    int unpooledSize = 4 * 1024 + 1;
    AbstractHeapChunk unpooledChunk = chunkPool.allocate(unpooledSize);
    Assert.assertTrue(unpooledChunk instanceof OnHeapChunk);
    Assert.assertFalse(unpooledChunk.isPooledChunk());
    Assert.assertEquals(HeapMode.ON_HEAP, unpooledChunk.getHeapMode());
    Assert.assertEquals(4 * 1024 + 1, unpooledChunk.getLimit());
    long maxChunkCount = chunkPool.getMaxChunkCount();
    Assert.assertEquals(maxChunkCount, unpooledChunk.getChunkID());
    // Nothing changed in chunks pool
    Assert.assertEquals(2 * 1024 - 1, chunkPool.getChunkQueue().size());
    Assert.assertEquals(2 * 1024, chunkPool.getChunkArray().length);
    Assert.assertEquals(2 * 1024, chunkPool.getCurrentChunkCounter());
    Assert.assertEquals(unpooledSize, chunkPool.getUnpooledChunkUsed());
    Map<Long, AbstractHeapChunk> unpooledChunks = chunkPool.getUnpooledChunksMap();
    Assert.assertEquals(1, unpooledChunks.size());
    Assert.assertEquals(unpooledChunk, unpooledChunks.get(unpooledChunk.getChunkID()));

    System.out.println("chunk position=" + chunk.getPosition());
    System.out.println("chunk limit=" + chunk.getLimit());
    System.out.println("chunk BB position=" + chunk.getByteBuffer().position());
    System.out.println("chunk BB limit=" + chunk.getByteBuffer().limit());
    System.out.println("chunk BB capacity=" + chunk.getByteBuffer().capacity());

    chunkPool.reclaimChunk(chunk);
    Assert.assertEquals(2 * 1024, chunkPool.getChunkQueue().size());
    Assert.assertEquals(2 * 1024, chunkPool.getChunkArray().length);
    Assert.assertEquals(2 * 1024, chunkPool.getCurrentChunkCounter());
    Assert.assertEquals(0, chunk.getPosition());
    Assert.assertEquals(0, chunk.getByteBuffer().position());
    Assert.assertEquals(4 * 1024, chunk.getByteBuffer().limit());
    Assert.assertEquals(4 * 1024, chunk.getLimit());
    Assert.assertEquals(0, chunk.getChunkID());
    System.out.println("chunk position=" + chunk.getPosition());
    System.out.println("chunk limit=" + chunk.getLimit());
    System.out.println("chunk BB position=" + chunk.getByteBuffer().position());
    System.out.println("chunk BB limit=" + chunk.getByteBuffer().limit());
    System.out.println("chunk BB capacity=" + chunk.getByteBuffer().capacity());

    chunkPool.reclaimChunk(unpooledChunk);
    Assert.assertEquals(2 * 1024, chunkPool.getChunkQueue().size());
    Assert.assertEquals(2 * 1024, chunkPool.getChunkArray().length);
    Assert.assertEquals(2 * 1024, chunkPool.getCurrentChunkCounter());
    Assert.assertEquals(0, unpooledChunks.size());
    Assert.assertEquals(0, chunkPool.getUnpooledChunkUsed());
  }

  @Test
  public void testExaustedNormalChunk() {
    conf.setLong(CCSMapUtils.CHUNK_CAPACITY_KEY, 8 * 1024);
    conf.setInt(CCSMapUtils.CHUNK_SIZE_KEY, 4 * 1024);
    conf.setInt(CCSMapUtils.INITIAL_CHUNK_COUNT_KEY, Integer.MAX_VALUE);
    conf.setBoolean(CCSMapUtils.USE_OFFHEAP, true);
    ChunkPoolParameters parameters = new ChunkPoolParameters(conf);
    ChunkPool chunkPool = new ChunkPool(parameters);
    Assert.assertEquals(2, chunkPool.getChunkQueue().size());
    Assert.assertEquals(chunkPool.getChunkArray().length, chunkPool.getChunkQueue().size());
    Assert.assertEquals(2, chunkPool.getCurrentChunkCounter());
    Assert.assertEquals(2, chunkPool.getMaxChunkCount());
    Assert.assertEquals(0, chunkPool.getUnpooledChunksMap().size());

    AbstractHeapChunk chunk1 = chunkPool.allocate(4 * 1024 - 1);
    Assert.assertTrue(chunk1.isPooledChunk());
    Assert.assertEquals(HeapMode.OFF_HEAP, chunk1.getHeapMode());
    Assert.assertEquals(4 * 1024, chunk1.getLimit());
    Assert.assertEquals(0, chunk1.getChunkID());
    Assert.assertEquals(0, chunk1.getPosition());
    Assert.assertEquals(1, chunkPool.getChunkQueue().size());
    Assert.assertEquals(2, chunkPool.getChunkArray().length);

    AbstractHeapChunk chunk2 = chunkPool.allocate(4 * 1024 - 2);
    Assert.assertTrue(chunk2.isPooledChunk());
    Assert.assertEquals(HeapMode.OFF_HEAP, chunk2.getHeapMode());
    Assert.assertEquals(4 * 1024, chunk2.getLimit());
    Assert.assertEquals(1, chunk2.getChunkID());
    Assert.assertEquals(0, chunk2.getPosition());
    Assert.assertEquals(0, chunkPool.getChunkQueue().size());
    Assert.assertEquals(2, chunkPool.getChunkArray().length);

    // Exhausted
    AbstractHeapChunk chunk3 = chunkPool.allocate(4 * 1024 - 3);
    Assert.assertEquals(HeapMode.ON_HEAP, chunk3.getHeapMode());
    Assert.assertFalse(chunk3.isPooledChunk());
    Assert.assertEquals(4 * 1024, chunk3.getLimit());
    Assert.assertEquals(2, chunk3.getChunkID());
    Assert.assertEquals(1, chunkPool.getUnpooledChunksMap().size());
    Assert.assertEquals(0, chunkPool.getChunkQueue().size());
    Assert.assertEquals(2, chunkPool.getChunkArray().length);

    AbstractHeapChunk chunk4 = chunkPool.allocate(4 * 1024 - 4);
    Assert.assertEquals(HeapMode.ON_HEAP, chunk3.getHeapMode());
    Assert.assertFalse(chunk4.isPooledChunk());
    Assert.assertEquals(4 * 1024, chunk4.getLimit());
    Assert.assertEquals(3, chunk4.getChunkID());
    Assert.assertEquals(2, chunkPool.getUnpooledChunksMap().size());
    Assert.assertEquals(0, chunkPool.getChunkQueue().size());
    Assert.assertEquals(2, chunkPool.getChunkArray().length);

    chunkPool.reclaimChunk(chunk4);
    Assert.assertEquals(1, chunkPool.getUnpooledChunksMap().size());
    Assert.assertEquals(0, chunkPool.getChunkQueue().size());
    Assert.assertEquals(2, chunkPool.getChunkArray().length);

    chunk4 = chunkPool.allocate(4 * 1024 - 4);
    Assert.assertEquals(HeapMode.ON_HEAP, chunk3.getHeapMode());
    Assert.assertFalse(chunk4.isPooledChunk());
    Assert.assertEquals(4 * 1024, chunk4.getLimit());
    Assert.assertEquals(4, chunk4.getChunkID());
    Assert.assertEquals(2, chunkPool.getUnpooledChunksMap().size());
    Assert.assertEquals(0, chunkPool.getChunkQueue().size());
    Assert.assertEquals(2, chunkPool.getChunkArray().length);

    // A chunk larger than specified
    AbstractHeapChunk chunk5 = chunkPool.allocate(4 * 1024 + 1);
    Assert.assertEquals(HeapMode.ON_HEAP, chunk5.getHeapMode());
    Assert.assertFalse(chunk5.isPooledChunk());
    Assert.assertEquals(4 * 1024 + 1, chunk5.getLimit());
    Assert.assertEquals(5, chunk5.getChunkID());
    Assert.assertEquals(3, chunkPool.getUnpooledChunksMap().size());
    Assert.assertEquals(0, chunkPool.getChunkQueue().size());
    Assert.assertEquals(2, chunkPool.getChunkArray().length);
  }

}
