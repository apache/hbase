/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.hfile.bucket;

import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.BucketEntry;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.RAMQueueEntry;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@Category(SmallTests.class)
public class TestBucketWriterThread {
  private BucketCache bc;
  private BucketCache.WriterThread wt;
  private BlockingQueue<RAMQueueEntry> q;
  private Cacheable plainCacheable;
  private BlockCacheKey plainKey;

  /** A BucketCache that does not start its writer threads. */
  private static class MockBucketCache extends BucketCache {

    public MockBucketCache(String ioEngineName, long capacity, int blockSize, int[] bucketSizes,
      int writerThreadNum, int writerQLen, String persistencePath, int ioErrorsTolerationDuration)
      throws FileNotFoundException, IOException {
      super(ioEngineName, capacity, blockSize, bucketSizes, writerThreadNum, writerQLen,
        persistencePath, ioErrorsTolerationDuration);
    }

    @Override
    protected void startWriterThreads() {
      // intentional noop
    }
  }

  /**
   * Set up variables and get BucketCache and WriterThread into state where tests can  manually
   * control the running of WriterThread and BucketCache is empty.
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {
    // Arbitrary capacity.
    final int capacity = 16;
    // Run with one writer thread only. Means there will be one writer queue only too.  We depend
    // on this in below.
    final int writerThreadsCount = 1;
    this.bc = new MockBucketCache("heap", capacity, 1, new int [] {1}, writerThreadsCount,
      capacity, null, 100/*Tolerate ioerrors for 100ms*/);
    assertEquals(writerThreadsCount, bc.writerThreads.length);
    assertEquals(writerThreadsCount, bc.writerQueues.size());
    // Get reference to our single WriterThread instance.
    this.wt = bc.writerThreads[0];
    this.q = bc.writerQueues.get(0);

    wt.disableWriter();
    this.plainKey = new BlockCacheKey("f", 0);
    this.plainCacheable = Mockito.mock(Cacheable.class);

    assertThat(bc.ramCache.isEmpty(), is(true));
    assertTrue(q.isEmpty());
  }

  @After
  public void tearDown() throws Exception {
    if (this.bc != null) this.bc.shutdown();
  }

  /**
   * Test non-error case just works.
   * @throws FileNotFoundException
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=30000)
  public void testNonErrorCase() throws IOException, InterruptedException {
    bc.cacheBlock(this.plainKey, this.plainCacheable);
    doDrainOfOneEntry(this.bc, this.wt, this.q);
  }

  /**
   * Pass through a too big entry and ensure it is cleared from queues and ramCache.
   * Manually run the WriterThread.
   * @throws InterruptedException
   */
  @Test
  public void testTooBigEntry() throws InterruptedException {
    Cacheable tooBigCacheable = Mockito.mock(Cacheable.class);
    Mockito.when(tooBigCacheable.getSerializedLength()).thenReturn(Integer.MAX_VALUE);
    this.bc.cacheBlock(this.plainKey, tooBigCacheable);
    doDrainOfOneEntry(this.bc, this.wt, this.q);
  }

  /**
   * Do IOE. Take the RAMQueueEntry that was on the queue, doctor it to throw exception, then
   * put it back and process it.
   * @throws IOException
   * @throws InterruptedException
   */
  @SuppressWarnings("unchecked")
  @Test (timeout=30000)
  public void testIOE() throws IOException, InterruptedException {
    this.bc.cacheBlock(this.plainKey, plainCacheable);
    RAMQueueEntry rqe = q.remove();
    RAMQueueEntry spiedRqe = Mockito.spy(rqe);
    Mockito.doThrow(new IOException("Mocked!")).when(spiedRqe).
      writeToCache((IOEngine)Mockito.any(), (BucketAllocator)Mockito.any(),
        (UniqueIndexMap<Integer>)Mockito.any(), (AtomicLong)Mockito.any());
    this.q.add(spiedRqe);
    doDrainOfOneEntry(bc, wt, q);
    // Cache disabled when ioes w/o ever healing.
    assertTrue(!bc.isCacheEnabled());
  }

  /**
   * Do Cache full exception
   * @throws IOException
   * @throws InterruptedException
   */
  @Test (timeout=30000)
  public void testCacheFullException()
      throws IOException, InterruptedException {
    this.bc.cacheBlock(this.plainKey, plainCacheable);
    RAMQueueEntry rqe = q.remove();
    RAMQueueEntry spiedRqe = Mockito.spy(rqe);
    final CacheFullException cfe = new CacheFullException(0, 0);
    BucketEntry mockedBucketEntry = Mockito.mock(BucketEntry.class);
    Mockito.doThrow(cfe).
      doReturn(mockedBucketEntry).
      when(spiedRqe).writeToCache((IOEngine)Mockito.any(), (BucketAllocator)Mockito.any(),
        (UniqueIndexMap<Integer>)Mockito.any(), (AtomicLong)Mockito.any());
    this.q.add(spiedRqe);
    doDrainOfOneEntry(bc, wt, q);
  }

  private static void doDrainOfOneEntry(final BucketCache bc, final BucketCache.WriterThread wt,
      final BlockingQueue<RAMQueueEntry> q)
  throws InterruptedException {
    List<RAMQueueEntry> rqes = BucketCache.getRAMQueueEntries(q, new ArrayList<RAMQueueEntry>(1));
    wt.doDrain(rqes);
    assertTrue(q.isEmpty());
    assertTrue(bc.ramCache.isEmpty());
    assertEquals(0, bc.heapSize());
  }
}
