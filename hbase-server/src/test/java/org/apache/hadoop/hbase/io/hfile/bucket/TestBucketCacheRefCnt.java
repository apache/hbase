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
package org.apache.hadoop.hbase.io.hfile.bucket;

import static org.apache.hadoop.hbase.io.hfile.CacheConfig.BUCKETCACHE_PERSIST_INTERVAL_KEY;
import static org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.DEFAULT_ERROR_TOLERATION_DURATION;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockCacheUtil;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.WriterThread;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.RefCnt;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import org.apache.hadoop.hbase.shaded.protobuf.generated.BucketCacheProtos;

@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestBucketCacheRefCnt {

  private static final String IO_ENGINE = "offheap";
  private static final long CAPACITY_SIZE = 32 * 1024 * 1024;
  private static final int BLOCK_SIZE = 1024;
  private static final int[] BLOCK_SIZE_ARRAY =
    new int[] { 64, 128, 256, 512, 1024, 2048, 4096, 8192 };
  private static final String PERSISTENCE_PATH = null;
  private static final HFileContext CONTEXT = new HFileContextBuilder().build();

  private BucketCache cache;

  private static BucketCache create(int writerSize, int queueSize) throws IOException {
    return new BucketCache(IO_ENGINE, CAPACITY_SIZE, BLOCK_SIZE, BLOCK_SIZE_ARRAY, writerSize,
      queueSize, PERSISTENCE_PATH);
  }

  private static MyBucketCache createMyBucketCache(int writerSize, int queueSize)
    throws IOException {
    return new MyBucketCache(IO_ENGINE, CAPACITY_SIZE, BLOCK_SIZE, BLOCK_SIZE_ARRAY, writerSize,
      queueSize, PERSISTENCE_PATH);
  }

  private static MyBucketCache2 createMyBucketCache2(int writerSize, int queueSize)
    throws IOException {
    return new MyBucketCache2(IO_ENGINE, CAPACITY_SIZE, BLOCK_SIZE, BLOCK_SIZE_ARRAY, writerSize,
      queueSize, PERSISTENCE_PATH);
  }

  private static HFileBlock createBlock(int offset, int size) {
    return createBlock(offset, size, ByteBuffAllocator.HEAP);
  }

  private static HFileBlock createBlock(int offset, int size, ByteBuffAllocator alloc) {
    return new HFileBlock(BlockType.DATA, size, size, -1, ByteBuff.wrap(ByteBuffer.allocate(size)),
      HFileBlock.FILL_HEADER, offset, 52, size, CONTEXT, alloc);
  }

  private static BlockCacheKey createKey(String hfileName, long offset) {
    return new BlockCacheKey(hfileName, offset);
  }

  private void disableWriter() {
    if (cache != null) {
      for (WriterThread wt : cache.writerThreads) {
        wt.disableWriter();
        wt.interrupt();
      }
    }
  }

  @Disabled
  @Test // Disabled by HBASE-24079. Reenable issue HBASE-24082
  // Flakey TestBucketCacheRefCnt.testBlockInRAMCache:121 expected:<3> but was:<2>
  public void testBlockInRAMCache() throws IOException {
    cache = create(1, 1000);
    disableWriter();
    final String prefix = "testBlockInRamCache";
    try {
      for (int i = 0; i < 10; i++) {
        HFileBlock blk = createBlock(i, 1020);
        BlockCacheKey key = createKey(prefix, i);
        assertEquals(1, blk.refCnt());
        cache.cacheBlock(key, blk);
        assertEquals(i + 1, cache.getBlockCount());
        assertEquals(2, blk.refCnt());

        Cacheable block = cache.getBlock(key, false, false, false);
        try {
          assertEquals(3, blk.refCnt());
          assertEquals(3, block.refCnt());
          assertEquals(blk, block);
        } finally {
          block.release();
        }
        assertEquals(2, blk.refCnt());
        assertEquals(2, block.refCnt());
      }

      for (int i = 0; i < 10; i++) {
        BlockCacheKey key = createKey(prefix, i);
        Cacheable blk = cache.getBlock(key, false, false, false);
        assertEquals(3, blk.refCnt());
        assertFalse(blk.release());
        assertEquals(2, blk.refCnt());

        assertTrue(cache.evictBlock(key));
        assertEquals(1, blk.refCnt());
        assertTrue(blk.release());
        assertEquals(0, blk.refCnt());
      }
    } finally {
      cache.shutdown();
    }
  }

  private static void waitUntilFlushedToCache(BucketCache bucketCache, BlockCacheKey blockCacheKey)
    throws InterruptedException {
    while (
      !bucketCache.backingMap.containsKey(blockCacheKey)
        || bucketCache.ramCache.containsKey(blockCacheKey)
    ) {
      Thread.sleep(100);
    }
    Thread.sleep(1000);
  }

  @Test
  public void testBlockInBackingMap() throws Exception {
    ByteBuffAllocator alloc = ByteBuffAllocator.create(HBaseConfiguration.create(), true);
    cache = create(1, 1000);
    try {
      HFileBlock blk = createBlock(200, 1020, alloc);
      BlockCacheKey key = createKey("testHFile-00", 200);
      cache.cacheBlock(key, blk);
      waitUntilFlushedToCache(cache, key);
      assertEquals(1, blk.refCnt());

      Cacheable block = cache.getBlock(key, false, false, false);
      assertTrue(block instanceof HFileBlock);
      assertTrue(((HFileBlock) block).getByteBuffAllocator() == alloc);
      assertEquals(2, block.refCnt());

      block.retain();
      assertEquals(3, block.refCnt());

      Cacheable newBlock = cache.getBlock(key, false, false, false);
      assertTrue(newBlock instanceof HFileBlock);
      assertTrue(((HFileBlock) newBlock).getByteBuffAllocator() == alloc);
      assertEquals(4, newBlock.refCnt());

      // release the newBlock
      assertFalse(newBlock.release());
      assertEquals(3, newBlock.refCnt());
      assertEquals(3, block.refCnt());

      // Evict the key
      cache.evictBlock(key);
      assertEquals(2, block.refCnt());

      // Evict again, shouldn't change the refCnt.
      cache.evictBlock(key);
      assertEquals(2, block.refCnt());

      assertFalse(block.release());
      assertEquals(1, block.refCnt());

      /**
       * The key was evicted from {@link BucketCache#backingMap} and {@link BucketCache#ramCache},
       * so {@link BucketCache#getBlock} return null.
       */
      Cacheable newestBlock = cache.getBlock(key, false, false, false);
      assertNull(newestBlock);
      assertEquals(1, block.refCnt());
      assertTrue(((HFileBlock) newBlock).getByteBuffAllocator() == alloc);

      // Release the block
      assertTrue(block.release());
      assertEquals(0, block.refCnt());
      assertEquals(0, newBlock.refCnt());
    } finally {
      cache.shutdown();
    }
  }

  @Test
  public void testInBucketCache() throws IOException {
    ByteBuffAllocator alloc = ByteBuffAllocator.create(HBaseConfiguration.create(), true);
    cache = create(1, 1000);
    try {
      HFileBlock blk = createBlock(200, 1020, alloc);
      BlockCacheKey key = createKey("testHFile-00", 200);
      cache.cacheBlock(key, blk);
      assertTrue(blk.refCnt() == 1 || blk.refCnt() == 2);

      // wait for block to move to backing map because refCnt get refreshed once block moves to
      // backing map
      Waiter.waitFor(HBaseConfiguration.create(), 12000, () -> isRamCacheDrained(key, cache));

      Cacheable block1 = cache.getBlock(key, false, false, false);
      assertTrue(block1.refCnt() >= 2);
      assertTrue(((HFileBlock) block1).getByteBuffAllocator() == alloc);

      Cacheable block2 = cache.getBlock(key, false, false, false);
      assertTrue(((HFileBlock) block2).getByteBuffAllocator() == alloc);
      assertTrue(block2.refCnt() >= 3);

      cache.evictBlock(key);
      assertTrue(blk.refCnt() >= 1);
      assertTrue(block1.refCnt() >= 2);
      assertTrue(block2.refCnt() >= 2);

      // Get key again
      Cacheable block3 = cache.getBlock(key, false, false, false);
      if (block3 != null) {
        assertTrue(((HFileBlock) block3).getByteBuffAllocator() == alloc);
        assertTrue(block3.refCnt() >= 3);
        assertFalse(block3.release());
      }

      blk.release();
      boolean ret1 = block1.release();
      boolean ret2 = block2.release();
      assertTrue(ret1 || ret2);
      assertEquals(0, blk.refCnt());
      assertEquals(0, block1.refCnt());
      assertEquals(0, block2.refCnt());
    } finally {
      cache.shutdown();
    }
  }

  private boolean isRamCacheDrained(BlockCacheKey key, BucketCache cache) {
    return cache.backingMap.containsKey(key) && !cache.ramCache.containsKey(key);
  }

  @Test
  public void testMarkStaleAsEvicted() throws Exception {
    cache = create(1, 1000);
    try {
      HFileBlock blk = createBlock(200, 1020);
      BlockCacheKey key = createKey("testMarkStaleAsEvicted", 200);
      cache.cacheBlock(key, blk);
      waitUntilFlushedToCache(cache, key);
      assertEquals(1, blk.refCnt());
      assertNotNull(cache.backingMap.get(key));
      assertEquals(1, cache.backingMap.get(key).refCnt());

      // RPC reference this cache.
      Cacheable block1 = cache.getBlock(key, false, false, false);
      assertEquals(2, block1.refCnt());
      BucketEntry be1 = cache.backingMap.get(key);
      assertNotNull(be1);
      assertEquals(2, be1.refCnt());

      // We've some RPC reference, so it won't have any effect.
      assertFalse(cache.evictBucketEntryIfNoRpcReferenced(key, be1));
      assertEquals(2, block1.refCnt());
      assertEquals(2, cache.backingMap.get(key).refCnt());

      // Release the RPC reference.
      block1.release();
      assertEquals(1, block1.refCnt());
      assertEquals(1, cache.backingMap.get(key).refCnt());

      // Mark the stale as evicted again, it'll do the de-allocation.
      assertTrue(cache.evictBucketEntryIfNoRpcReferenced(key, be1));
      assertEquals(0, block1.refCnt());
      assertNull(cache.backingMap.get(key));
      assertEquals(0, cache.size());
    } finally {
      cache.shutdown();
    }
  }

  @Test
  public void testShutdownReleasesOnlyBackingMapReference() throws Exception {
    ByteBuffAllocator allocator = ByteBuffAllocator.create(HBaseConfiguration.create(), true);
    HFileBlock blockToCache = createBlock(200, 1020, allocator);
    HFileBlock retainedBlock = null;
    try {
      cache = create(1, 1000);
      BlockCacheKey key = createKey("testShutdownReleasesOnlyBackingMapReference", 200);
      cache.cacheBlock(key, blockToCache);
      waitUntilFlushedToCache(cache, key);

      retainedBlock = (HFileBlock) cache.getBlock(key, false, false, false);
      assertNotNull(retainedBlock);
      assertEquals(2, retainedBlock.refCnt());

      HFileBlock callerReference = retainedBlock;
      cache.shutdown();
      cache.shutdown();

      Waiter.waitFor(HBaseConfiguration.create(), 10000,
        () -> cache.backingMap.isEmpty() && callerReference.refCnt() == 1);
      assertEquals(1, retainedBlock.refCnt());
      assertTrue(retainedBlock.release());
      assertEquals(0, retainedBlock.refCnt());
    } finally {
      if (cache != null) {
        cache.shutdown();
        cache = null;
      }
      if (retainedBlock != null) {
        while (retainedBlock.refCnt() > 0) {
          retainedBlock.release();
        }
      }
      while (blockToCache.refCnt() > 0) {
        blockToCache.release();
      }
      allocator.clean();
    }
  }

  @Test
  public void testShutdownPersistsEmptyMapOverPreviousCheckpoint(@TempDir File testDir)
    throws Exception {
    HFileBlock blockToCache = createBlock(200, 1020);
    BucketCache bucketCache = null;
    BucketCache recoveredCache = null;
    String cachePath = new File(testDir, "bucket.cache").getAbsolutePath();
    String persistencePath = new File(testDir, "bucket.persistence").getAbsolutePath();
    BlockCacheKey key = createKey("testShutdownPersistsEmptyMapOverPreviousCheckpoint", 200);
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(BUCKETCACHE_PERSIST_INTERVAL_KEY, Long.MAX_VALUE);
    try {
      bucketCache = new BucketCache("file:" + cachePath, CAPACITY_SIZE, BLOCK_SIZE,
        BLOCK_SIZE_ARRAY, 1, 1000, persistencePath, DEFAULT_ERROR_TOLERATION_DURATION, conf);
      assertTrue(bucketCache.waitForCacheInitialization(10000));
      bucketCache.cacheBlock(key, blockToCache);
      waitUntilFlushedToCache(bucketCache, key);

      bucketCache.persistToFile();
      assertTrue(new File(persistencePath).isFile());
      assertTrue(bucketCache.backingMap.containsKey(key));
      assertTrue(bucketCache.evictBlock(key));
      assertTrue(bucketCache.backingMap.isEmpty());
      bucketCache.shutdown();
      try (
        DataInputStream in = new DataInputStream(new FileInputStream(new File(persistencePath)))) {
        byte[] magic = new byte[BucketProtoUtils.PB_MAGIC_V2.length];
        in.readFully(magic);
        assertArrayEquals(BucketProtoUtils.PB_MAGIC_V2, magic);
        assertNotNull(BucketCacheProtos.BucketCacheEntry.parseDelimitedFrom(in));
        assertEquals(-1, in.read());
      }
      bucketCache = null;

      recoveredCache = new BucketCache("file:" + cachePath, CAPACITY_SIZE, BLOCK_SIZE,
        BLOCK_SIZE_ARRAY, 1, 1000, persistencePath, DEFAULT_ERROR_TOLERATION_DURATION, conf);
      assertTrue(recoveredCache.waitForCacheInitialization(10000));
      assertTrue(recoveredCache.backingMap.isEmpty());
      assertEquals(0, recoveredCache.getAllocator().getUsedSize());
      assertEquals(0, recoveredCache.getBlockCount());
      Cacheable staleBlock = recoveredCache.getBlock(key, false, false, false);
      if (staleBlock != null) {
        staleBlock.release();
      }
      assertNull(staleBlock);
    } finally {
      if (bucketCache != null) {
        bucketCache.shutdown();
      }
      if (recoveredCache != null) {
        recoveredCache.shutdown();
      }
      while (blockToCache.refCnt() > 0) {
        blockToCache.release();
      }
    }
  }

  @Test
  public void testFinalPersistFailureReleasesEntriesAndKeepsPreviousCheckpoint(
    @TempDir File testDir) throws Exception {
    HFileBlock firstBlock = createBlock(200, 1020);
    HFileBlock secondBlock = createBlock(400, 1020);
    BucketCache bucketCache = null;
    BucketCache recoveredCache = null;
    String cachePath = new File(testDir, "bucket.cache").getAbsolutePath();
    String persistencePath = new File(testDir, "bucket.persistence").getAbsolutePath();
    BlockCacheKey firstKey = createKey("first", 200);
    BlockCacheKey secondKey = createKey("second", 400);
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(BUCKETCACHE_PERSIST_INTERVAL_KEY, Long.MAX_VALUE);
    try {
      bucketCache = new BucketCache("file:" + cachePath, CAPACITY_SIZE, BLOCK_SIZE,
        BLOCK_SIZE_ARRAY, 1, 1000, persistencePath, DEFAULT_ERROR_TOLERATION_DURATION, conf);
      assertTrue(bucketCache.waitForCacheInitialization(10000));
      bucketCache.cacheBlock(firstKey, firstBlock);
      waitUntilFlushedToCache(bucketCache, firstKey);
      bucketCache.persistToFile();
      byte[] previousCheckpoint = Files.readAllBytes(new File(persistencePath).toPath());

      bucketCache.cacheBlock(secondKey, secondBlock);
      waitUntilFlushedToCache(bucketCache, secondKey);
      BucketEntry firstEntry = bucketCache.backingMap.get(firstKey);
      BucketEntry secondEntry = bucketCache.backingMap.get(secondKey);
      assertNotNull(firstEntry);
      assertNotNull(secondEntry);
      assertEquals(1, firstEntry.refCnt());
      assertEquals(1, secondEntry.refCnt());

      long failureTime = 123456789L;
      File tempPersistencePath = new File(persistencePath + failureTime);
      assertTrue(tempPersistencePath.mkdir());
      ManualEnvironmentEdge edge = new ManualEnvironmentEdge();
      edge.setValue(failureTime);
      EnvironmentEdgeManager.injectEdge(edge);
      try {
        bucketCache.shutdown();
      } finally {
        EnvironmentEdgeManager.reset();
      }

      assertTrue(bucketCache.backingMap.isEmpty());
      assertEquals(0, firstEntry.refCnt());
      assertEquals(0, secondEntry.refCnt());
      assertArrayEquals(previousCheckpoint, Files.readAllBytes(new File(persistencePath).toPath()));
      bucketCache = null;

      recoveredCache = new BucketCache("file:" + cachePath, CAPACITY_SIZE, BLOCK_SIZE,
        BLOCK_SIZE_ARRAY, 1, 1000, persistencePath, DEFAULT_ERROR_TOLERATION_DURATION, conf);
      assertTrue(recoveredCache.waitForCacheInitialization(10000));
      BucketCache cacheToValidate = recoveredCache;
      Waiter.waitFor(HBaseConfiguration.create(), 10000,
        () -> cacheToValidate.getBackingMapValidated().get());
      assertEquals(1, recoveredCache.backingMap.size());
      Cacheable recoveredBlock = recoveredCache.getBlock(firstKey, false, false, false);
      assertNotNull(recoveredBlock);
      recoveredBlock.release();
      Cacheable uncheckpointedBlock = recoveredCache.getBlock(secondKey, false, false, false);
      if (uncheckpointedBlock != null) {
        uncheckpointedBlock.release();
      }
      assertNull(uncheckpointedBlock);
    } finally {
      EnvironmentEdgeManager.reset();
      if (bucketCache != null) {
        bucketCache.shutdown();
      }
      if (recoveredCache != null) {
        recoveredCache.shutdown();
      }
      while (firstBlock.refCnt() > 0) {
        firstBlock.release();
      }
      while (secondBlock.refCnt() > 0) {
        secondBlock.release();
      }
    }
  }

  @Test
  public void testHBaseIOExceptionThroughReferenceEvictsStoredEntry(@TempDir File testDir)
    throws Exception {
    HFileBlock blockToCache = createBlock(200, 1020);
    BucketEntry bucketEntry = null;
    String cachePath = new File(testDir, "bucket.cache").getAbsolutePath();
    String persistencePath = new File(testDir, "bucket.persistence").getAbsolutePath();
    BucketCache bucketCache = new BucketCache("file:" + cachePath, CAPACITY_SIZE, BLOCK_SIZE,
      BLOCK_SIZE_ARRAY, 1, 1000, persistencePath);
    try {
      assertTrue(bucketCache.waitForCacheInitialization(10000));
      String hfileName = "0123456789abcdef";
      String regionName = "region";
      BlockCacheKey storedKey =
        new BlockCacheKey(hfileName, "cf", regionName, 200, true, BlockType.DATA, false);
      BlockCacheKey referenceKey = createKey(hfileName + ".parent", 200);
      bucketCache.cacheBlock(storedKey, blockToCache);
      waitUntilFlushedToCache(bucketCache, storedKey);
      bucketCache.fileCacheCompleted(new Path("/table/" + regionName + "/cf/" + hfileName), 1020);

      bucketEntry = bucketCache.backingMap.get(storedKey);
      assertNotNull(bucketEntry);
      assertTrue(bucketCache.regionCachedSize.containsKey(regionName));
      assertTrue(bucketCache.fullyCachedFiles.containsKey(hfileName));

      ByteBuffer invalidCachedTime = ByteBuffer.allocate(Long.BYTES);
      invalidCachedTime.putLong(bucketEntry.getCachedTime() + 1).flip();
      bucketCache.ioEngine.write(invalidCachedTime, bucketEntry.offset());
      bucketCache.ioEngine.sync();

      assertNull(bucketCache.getBlock(referenceKey, false, false, false));
      assertFalse(bucketCache.backingMap.containsKey(storedKey));
      assertFalse(bucketCache.blocksByHFile.contains(storedKey));
      assertFalse(bucketCache.regionCachedSize.containsKey(regionName));
      assertFalse(bucketCache.fullyCachedFiles.containsKey(hfileName));
      assertEquals(0, bucketEntry.refCnt());
      assertEquals(0, bucketCache.getAllocator().getUsedSize());
    } finally {
      bucketCache.shutdown();
      if (bucketEntry != null && bucketEntry.refCnt() > 0) {
        bucketEntry.markAsEvicted();
      }
      while (blockToCache.refCnt() > 0) {
        blockToCache.release();
      }
    }
  }

  @Test
  public void testPlainIOExceptionKeepsEntryAndCacheMetadata(@TempDir File testDir)
    throws Exception {
    HFileBlock blockToCache = createBlock(200, 1020);
    String cachePath = new File(testDir, "bucket.cache").getAbsolutePath();
    String persistencePath = new File(testDir, "bucket.persistence").getAbsolutePath();
    BucketCache bucketCache = new BucketCache("file:" + cachePath, CAPACITY_SIZE, BLOCK_SIZE,
      BLOCK_SIZE_ARRAY, 1, 1000, persistencePath);
    FileChannel originalChannel = null;
    try {
      assertTrue(bucketCache.waitForCacheInitialization(10000));
      String hfileName = "testPlainIOExceptionKeepsEntryAndCacheMetadata";
      String regionName = "region";
      BlockCacheKey key =
        new BlockCacheKey(hfileName, "cf", regionName, 200, true, BlockType.DATA, false);
      bucketCache.cacheBlock(key, blockToCache);
      waitUntilFlushedToCache(bucketCache, key);
      bucketCache.fileCacheCompleted(new Path("/table/" + regionName + "/cf/" + hfileName), 1020);

      BucketEntry bucketEntry = bucketCache.backingMap.get(key);
      assertNotNull(bucketEntry);
      FileIOEngine fileIOEngine = (FileIOEngine) bucketCache.ioEngine;
      originalChannel = fileIOEngine.getFileChannels()[0];
      FileChannel failingChannel = Mockito.mock(FileChannel.class);
      Mockito.when(failingChannel.read(Mockito.any(ByteBuffer.class), Mockito.anyLong()))
        .thenThrow(new IOException("Injected read failure"));
      fileIOEngine.getFileChannels()[0] = failingChannel;

      assertNull(bucketCache.getBlock(key, false, false, false));
      assertTrue(bucketCache.isCacheEnabled());
      assertEquals(bucketEntry, bucketCache.backingMap.get(key));
      assertTrue(bucketCache.blocksByHFile.contains(key));
      assertTrue(bucketCache.regionCachedSize.containsKey(regionName));
      assertTrue(bucketCache.fullyCachedFiles.containsKey(hfileName));
      assertEquals(1, bucketEntry.refCnt());
    } finally {
      if (originalChannel != null) {
        ((FileIOEngine) bucketCache.ioEngine).getFileChannels()[0] = originalChannel;
      }
      bucketCache.shutdown();
      while (blockToCache.refCnt() > 0) {
        blockToCache.release();
      }
    }
  }

  @Test
  public void testInitialPublicationDoesNotRestoreIndexAfterConcurrentEviction() throws Exception {
    BucketCache bucketCache = create(1, 1000);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch entryPublished = new CountDownLatch(1);
    CountDownLatch continuePublication = new CountDownLatch(1);
    BucketEntry publishedEntry = null;
    try {
      BlockCacheKey key =
        createKey("testInitialPublicationDoesNotRestoreIndexAfterConcurrentEviction", 200);
      publishedEntry = new BucketEntry(8192, 1020, 1020, 0, false, entry -> ByteBuffAllocator.NONE,
        ByteBuffAllocator.HEAP);
      BucketEntry entryToPublish = publishedEntry;
      bucketCache.backingMap = Mockito.spy(bucketCache.backingMap);
      Mockito.doAnswer(invocation -> {
        Object previousEntry = invocation.callRealMethod();
        entryPublished.countDown();
        if (!continuePublication.await(10, TimeUnit.SECONDS)) {
          throw new AssertionError("Timed out waiting to resume publication");
        }
        return previousEntry;
      }).when(bucketCache.backingMap).put(key, entryToPublish);
      Future<?> publication =
        executor.submit(() -> bucketCache.putIntoBackingMap(key, entryToPublish));

      assertTrue(entryPublished.await(10, TimeUnit.SECONDS));
      assertTrue(entryToPublish.withWriteLock(bucketCache.offsetLock, () -> {
        if (bucketCache.backingMap.remove(key, entryToPublish)) {
          bucketCache.blockEvicted(key, entryToPublish, false, false);
          return true;
        }
        return false;
      }));
      continuePublication.countDown();
      publication.get(10, TimeUnit.SECONDS);

      assertFalse(bucketCache.backingMap.containsKey(key));
      assertFalse(bucketCache.blocksByHFile.contains(key));
      assertEquals(0, publishedEntry.refCnt());
    } finally {
      continuePublication.countDown();
      executor.shutdownNow();
      try {
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      } finally {
        bucketCache.shutdown();
        if (publishedEntry != null && publishedEntry.refCnt() > 0) {
          publishedEntry.markAsEvicted();
        }
      }
    }
  }

  /**
   * <pre>
   * This test is for HBASE-26281,
   * test two threads for replacing Block and getting Block execute concurrently.
   * The threads sequence is:
   * 1. Block1 was cached successfully,the {@link RefCnt} of Block1 is 1.
   * 2. Thread1 caching the same {@link BlockCacheKey} with Block2 satisfied
   *    {@link BlockCacheUtil#shouldReplaceExistingCacheBlock}, so Block2 would
   *    replace Block1, but thread1 stopping before {@link BucketCache#cacheBlockWithWaitInternal}
   * 3. Thread2 invoking {@link BucketCache#getBlock} with the same {@link BlockCacheKey},
   *    which returned Block1, the {@link RefCnt} of Block1 is 2.
   * 4. Thread1 continues caching Block2, in {@link BucketCache.WriterThread#putIntoBackingMap},
   *    the old Block1 is freed directly which {@link RefCnt} is 2, but the Block1 is still used
   *    by Thread2 and the content of Block1 would be overwritten after it is freed, which may
   *    cause a serious error.
   * </pre>
   */
  @Test
  public void testReplacingBlockAndGettingBlockConcurrently() throws Exception {
    ByteBuffAllocator byteBuffAllocator =
      ByteBuffAllocator.create(HBaseConfiguration.create(), true);
    final MyBucketCache myBucketCache = createMyBucketCache(1, 1000);
    try {
      HFileBlock hfileBlock = createBlock(200, 1020, byteBuffAllocator);
      final BlockCacheKey blockCacheKey = createKey("testTwoThreadConcurrent", 200);
      myBucketCache.cacheBlock(blockCacheKey, hfileBlock);
      waitUntilFlushedToCache(myBucketCache, blockCacheKey);
      assertEquals(1, hfileBlock.refCnt());

      assertTrue(!myBucketCache.ramCache.containsKey(blockCacheKey));
      final AtomicReference<Throwable> exceptionRef = new AtomicReference<Throwable>();
      Thread cacheBlockThread = new Thread(() -> {
        try {
          HFileBlock newHFileBlock = createBlock(200, 1020, byteBuffAllocator);
          myBucketCache.cacheBlock(blockCacheKey, newHFileBlock);
          waitUntilFlushedToCache(myBucketCache, blockCacheKey);

        } catch (Throwable exception) {
          exceptionRef.set(exception);
        }
      });
      cacheBlockThread.setName(MyBucketCache.CACHE_BLOCK_THREAD_NAME);
      cacheBlockThread.start();

      String oldThreadName = Thread.currentThread().getName();
      HFileBlock gotHFileBlock = null;
      try {

        Thread.currentThread().setName(MyBucketCache.GET_BLOCK_THREAD_NAME);

        gotHFileBlock = (HFileBlock) (myBucketCache.getBlock(blockCacheKey, false, false, false));
        assertTrue(gotHFileBlock.equals(hfileBlock));
        assertTrue(gotHFileBlock.getByteBuffAllocator() == byteBuffAllocator);
        assertEquals(2, gotHFileBlock.refCnt());
        /**
         * Release the second cyclicBarrier.await in
         * {@link MyBucketCache#cacheBlockWithWaitInternal}
         */
        myBucketCache.cyclicBarrier.await();

      } finally {
        Thread.currentThread().setName(oldThreadName);
      }

      cacheBlockThread.join();
      assertTrue(exceptionRef.get() == null);
      assertTrue(myBucketCache.blocksByHFile.contains(blockCacheKey));
      assertEquals(1, gotHFileBlock.refCnt());
      assertTrue(gotHFileBlock.equals(hfileBlock));
      assertTrue(myBucketCache.overwiteByteBuff == null);
      assertTrue(myBucketCache.freeBucketEntryCounter.get() == 0);

      gotHFileBlock.release();
      assertEquals(0, gotHFileBlock.refCnt());
      assertTrue(myBucketCache.overwiteByteBuff != null);
      assertTrue(myBucketCache.freeBucketEntryCounter.get() == 1);
      assertTrue(myBucketCache.replaceCounter.get() == 1);
      assertTrue(myBucketCache.blockEvictCounter.get() == 1);
    } finally {
      myBucketCache.shutdown();
    }

  }

  /**
   * <pre>
   * This test also is for HBASE-26281,
   * test three threads for evicting Block,caching Block and getting Block
   * execute concurrently.
   * 1. Thread1 caching Block1, stopping after {@link BucketCache.WriterThread#putIntoBackingMap},
   *    the {@link RefCnt} of Block1 is 1.
   * 2. Thread2 invoking {@link BucketCache#evictBlock} with the same {@link BlockCacheKey},
   *    but stopping after {@link BucketCache#removeFromRamCache}.
   * 3. Thread3 invoking {@link BucketCache#getBlock} with the same {@link BlockCacheKey},
   *    which returned Block1, the {@link RefCnt} of Block1 is 2.
   * 4. Thread1 continues caching block1,but finding that {@link BucketCache.RAMCache#remove}
   *    returning false, so invoking {@link BucketCache#blockEvicted} to free the the Block1
   *    directly which {@link RefCnt} is 2 and the Block1 is still used by Thread3.
   * </pre>
   */
  @Test
  public void testEvictingBlockCachingBlockGettingBlockConcurrently() throws Exception {
    ByteBuffAllocator byteBuffAllocator =
      ByteBuffAllocator.create(HBaseConfiguration.create(), true);
    final MyBucketCache2 myBucketCache2 = createMyBucketCache2(1, 1000);
    try {
      final HFileBlock hfileBlock = createBlock(200, 1020, byteBuffAllocator);
      final BlockCacheKey blockCacheKey = createKey("testThreeThreadConcurrent", 200);
      final AtomicReference<Throwable> cacheBlockThreadExceptionRef =
        new AtomicReference<Throwable>();
      Thread cacheBlockThread = new Thread(() -> {
        try {
          myBucketCache2.cacheBlock(blockCacheKey, hfileBlock);
          /**
           * Wait for Caching Block completed.
           */
          myBucketCache2.writeThreadDoneCyclicBarrier.await();
        } catch (Throwable exception) {
          cacheBlockThreadExceptionRef.set(exception);
        }
      });
      cacheBlockThread.setName(MyBucketCache2.CACHE_BLOCK_THREAD_NAME);
      cacheBlockThread.start();

      final AtomicReference<Throwable> evictBlockThreadExceptionRef =
        new AtomicReference<Throwable>();
      Thread evictBlockThread = new Thread(() -> {
        try {
          myBucketCache2.evictBlock(blockCacheKey);
        } catch (Throwable exception) {
          evictBlockThreadExceptionRef.set(exception);
        }
      });
      evictBlockThread.setName(MyBucketCache2.EVICT_BLOCK_THREAD_NAME);
      evictBlockThread.start();

      String oldThreadName = Thread.currentThread().getName();
      HFileBlock gotHFileBlock = null;
      try {
        Thread.currentThread().setName(MyBucketCache2.GET_BLOCK_THREAD_NAME);
        gotHFileBlock = (HFileBlock) (myBucketCache2.getBlock(blockCacheKey, false, false, false));
        assertTrue(gotHFileBlock.equals(hfileBlock));
        assertTrue(gotHFileBlock.getByteBuffAllocator() == byteBuffAllocator);
        assertEquals(2, gotHFileBlock.refCnt());
        try {
          /**
           * Release the second cyclicBarrier.await in {@link MyBucketCache2#putIntoBackingMap} for
           * {@link BucketCache.WriterThread},getBlock completed,{@link BucketCache.WriterThread}
           * could continue.
           */
          myBucketCache2.putCyclicBarrier.await();
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }

      } finally {
        Thread.currentThread().setName(oldThreadName);
      }

      cacheBlockThread.join();
      evictBlockThread.join();
      assertTrue(cacheBlockThreadExceptionRef.get() == null);
      assertTrue(evictBlockThreadExceptionRef.get() == null);

      assertTrue(gotHFileBlock.equals(hfileBlock));
      assertEquals(1, gotHFileBlock.refCnt());
      assertTrue(myBucketCache2.overwiteByteBuff == null);
      assertTrue(myBucketCache2.freeBucketEntryCounter.get() == 0);

      gotHFileBlock.release();
      assertEquals(0, gotHFileBlock.refCnt());
      assertTrue(myBucketCache2.overwiteByteBuff != null);
      assertTrue(myBucketCache2.freeBucketEntryCounter.get() == 1);
      assertTrue(myBucketCache2.blockEvictCounter.get() == 1);
    } finally {
      myBucketCache2.shutdown();
    }

  }

  static class MyBucketCache extends BucketCache {
    private static final String GET_BLOCK_THREAD_NAME = "_getBlockThread";
    private static final String CACHE_BLOCK_THREAD_NAME = "_cacheBlockThread";

    private final CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
    private final AtomicInteger replaceCounter = new AtomicInteger(0);
    private final AtomicInteger blockEvictCounter = new AtomicInteger(0);
    private final AtomicInteger freeBucketEntryCounter = new AtomicInteger(0);
    private ByteBuff overwiteByteBuff = null;

    public MyBucketCache(String ioEngineName, long capacity, int blockSize, int[] bucketSizes,
      int writerThreadNum, int writerQLen, String persistencePath) throws IOException {
      super(ioEngineName, capacity, blockSize, bucketSizes, writerThreadNum, writerQLen,
        persistencePath);
    }

    /**
     * Simulate the Block could be replaced.
     */
    @Override
    protected boolean shouldReplaceExistingCacheBlock(BlockCacheKey cacheKey, Cacheable newBlock) {
      replaceCounter.incrementAndGet();
      return true;
    }

    @Override
    public Cacheable getBlock(BlockCacheKey key, boolean caching, boolean repeat,
      boolean updateCacheMetrics) {
      if (Thread.currentThread().getName().equals(GET_BLOCK_THREAD_NAME)) {
        /**
         * Wait the first cyclicBarrier.await() in {@link MyBucketCache#cacheBlockWithWaitInternal},
         * so the {@link BucketCache#getBlock} is executed after the {@link BucketEntry#isRpcRef}
         * checking.
         */
        try {
          cyclicBarrier.await();
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }
      Cacheable result = super.getBlock(key, caching, repeat, updateCacheMetrics);
      return result;
    }

    @Override
    protected void cacheBlockWithWaitInternal(BlockCacheKey cacheKey, Cacheable cachedItem,
      boolean inMemory, boolean wait) {
      if (Thread.currentThread().getName().equals(CACHE_BLOCK_THREAD_NAME)) {
        /**
         * Wait the cyclicBarrier.await() in {@link MyBucketCache#getBlock}
         */
        try {
          cyclicBarrier.await();
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }
      if (Thread.currentThread().getName().equals(CACHE_BLOCK_THREAD_NAME)) {
        /**
         * Wait the cyclicBarrier.await() in
         * {@link TestBucketCacheRefCnt#testReplacingBlockAndGettingBlockConcurrently} for
         * {@link MyBucketCache#getBlock} and Assert completed.
         */
        try {
          cyclicBarrier.await();
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }
      super.cacheBlockWithWaitInternal(cacheKey, cachedItem, inMemory, wait);
    }

    @Override
    void blockEvicted(BlockCacheKey cacheKey, BucketEntry bucketEntry, boolean decrementBlockNumber,
      boolean evictedByEvictionProcess) {
      blockEvictCounter.incrementAndGet();
      super.blockEvicted(cacheKey, bucketEntry, decrementBlockNumber, evictedByEvictionProcess);
    }

    /**
     * Overwrite 0xff to the {@link BucketEntry} content to simulate it would be overwrite after the
     * {@link BucketEntry} is freed.
     */
    @Override
    void freeBucketEntry(BucketEntry bucketEntry) {
      freeBucketEntryCounter.incrementAndGet();
      super.freeBucketEntry(bucketEntry);
      this.overwiteByteBuff = getOverwriteByteBuff(bucketEntry);
      try {
        this.ioEngine.write(this.overwiteByteBuff, bucketEntry.offset());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static class MyBucketCache2 extends BucketCache {
    private static final String GET_BLOCK_THREAD_NAME = "_getBlockThread";
    private static final String CACHE_BLOCK_THREAD_NAME = "_cacheBlockThread";
    private static final String EVICT_BLOCK_THREAD_NAME = "_evictBlockThread";

    private final CyclicBarrier getCyclicBarrier = new CyclicBarrier(2);
    private final CyclicBarrier evictCyclicBarrier = new CyclicBarrier(2);
    private final CyclicBarrier putCyclicBarrier = new CyclicBarrier(2);
    /**
     * This is used for {@link BucketCache.WriterThread},{@link #CACHE_BLOCK_THREAD_NAME} and
     * {@link #EVICT_BLOCK_THREAD_NAME},waiting for caching block completed.
     */
    private final CyclicBarrier writeThreadDoneCyclicBarrier = new CyclicBarrier(3);
    private final AtomicInteger blockEvictCounter = new AtomicInteger(0);
    private final AtomicInteger removeRamCounter = new AtomicInteger(0);
    private final AtomicInteger freeBucketEntryCounter = new AtomicInteger(0);
    private ByteBuff overwiteByteBuff = null;

    public MyBucketCache2(String ioEngineName, long capacity, int blockSize, int[] bucketSizes,
      int writerThreadNum, int writerQLen, String persistencePath) throws IOException {
      super(ioEngineName, capacity, blockSize, bucketSizes, writerThreadNum, writerQLen,
        persistencePath);
    }

    @Override
    protected void putIntoBackingMap(BlockCacheKey key, BucketEntry bucketEntry) {
      super.putIntoBackingMap(key, bucketEntry);
      /**
       * The {@link BucketCache.WriterThread} wait for evictCyclicBarrier.await before
       * {@link MyBucketCache2#removeFromRamCache} for {@link #EVICT_BLOCK_THREAD_NAME}
       */
      try {
        evictCyclicBarrier.await();
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }

      /**
       * Wait the cyclicBarrier.await() in
       * {@link TestBucketCacheRefCnt#testEvictingBlockCachingBlockGettingBlockConcurrently} for
       * {@link MyBucketCache#getBlock} and Assert completed.
       */
      try {
        putCyclicBarrier.await();
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    void doDrain(List<RAMQueueEntry> entries, ByteBuffer metaBuff) throws InterruptedException {
      super.doDrain(entries, metaBuff);
      if (entries.size() > 0) {
        /**
         * Caching Block completed,release {@link #GET_BLOCK_THREAD_NAME} and
         * {@link #EVICT_BLOCK_THREAD_NAME}.
         */
        try {
          writeThreadDoneCyclicBarrier.await();
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }

    }

    @Override
    public Cacheable getBlock(BlockCacheKey key, boolean caching, boolean repeat,
      boolean updateCacheMetrics) {
      if (Thread.currentThread().getName().equals(GET_BLOCK_THREAD_NAME)) {
        /**
         * Wait for second getCyclicBarrier.await in {@link MyBucketCache2#removeFromRamCache} after
         * {@link BucketCache#removeFromRamCache}.
         */
        try {
          getCyclicBarrier.await();
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }
      Cacheable result = super.getBlock(key, caching, repeat, updateCacheMetrics);
      return result;
    }

    @Override
    protected boolean removeFromRamCache(BlockCacheKey cacheKey) {
      boolean firstTime = false;
      if (Thread.currentThread().getName().equals(EVICT_BLOCK_THREAD_NAME)) {
        int count = this.removeRamCounter.incrementAndGet();
        firstTime = (count == 1);
        if (firstTime) {
          /**
           * The {@link #EVICT_BLOCK_THREAD_NAME} wait for evictCyclicBarrier.await after
           * {@link BucketCache#putIntoBackingMap}.
           */
          try {
            evictCyclicBarrier.await();
          } catch (Throwable e) {
            throw new RuntimeException(e);
          }
        }
      }
      boolean result = super.removeFromRamCache(cacheKey);
      if (Thread.currentThread().getName().equals(EVICT_BLOCK_THREAD_NAME)) {
        if (firstTime) {
          /**
           * Wait for getCyclicBarrier.await before {@link BucketCache#getBlock}.
           */
          try {
            getCyclicBarrier.await();
          } catch (Throwable e) {
            throw new RuntimeException(e);
          }
          /**
           * Wait for Caching Block completed, after Caching Block completed, evictBlock could
           * continue.
           */
          try {
            writeThreadDoneCyclicBarrier.await();
          } catch (Throwable e) {
            throw new RuntimeException(e);
          }
        }
      }

      return result;
    }

    @Override
    void blockEvicted(BlockCacheKey cacheKey, BucketEntry bucketEntry, boolean decrementBlockNumber,
      boolean evictedByEvictionProcess) {
      /**
       * This is only invoked by {@link BucketCache.WriterThread}. {@link MyMyBucketCache2} create
       * only one {@link BucketCache.WriterThread}.
       */
      assertTrue(Thread.currentThread() == this.writerThreads[0]);

      blockEvictCounter.incrementAndGet();
      super.blockEvicted(cacheKey, bucketEntry, decrementBlockNumber, evictedByEvictionProcess);
    }

    /**
     * Overwrite 0xff to the {@link BucketEntry} content to simulate it would be overwrite after the
     * {@link BucketEntry} is freed.
     */
    @Override
    void freeBucketEntry(BucketEntry bucketEntry) {
      freeBucketEntryCounter.incrementAndGet();
      super.freeBucketEntry(bucketEntry);
      this.overwiteByteBuff = getOverwriteByteBuff(bucketEntry);
      try {
        this.ioEngine.write(this.overwiteByteBuff, bucketEntry.offset());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static ByteBuff getOverwriteByteBuff(BucketEntry bucketEntry) {
    int byteSize = bucketEntry.getLength();
    byte[] data = new byte[byteSize];
    Arrays.fill(data, (byte) 0xff);
    return ByteBuff.wrap(ByteBuffer.wrap(data));
  }
}
