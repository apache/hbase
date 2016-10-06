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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.MultithreadedTestUtil;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestThread;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;

import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestMemStoreLAB {

  private static final byte[] rk = Bytes.toBytes("r1");
  private static final byte[] cf = Bytes.toBytes("f");
  private static final byte[] q = Bytes.toBytes("q");

  /**
   * Test a bunch of random allocations
   */
  @Test
  public void testLABRandomAllocation() {
    Random rand = new Random();
    MemStoreLAB mslab = new HeapMemStoreLAB();
    int expectedOff = 0;
    byte[] lastBuffer = null;
    // 100K iterations by 0-1K alloc -> 50MB expected
    // should be reasonable for unit test and also cover wraparound
    // behavior
    for (int i = 0; i < 100000; i++) {
      int valSize = rand.nextInt(1000);
      KeyValue kv = new KeyValue(rk, cf, q, new byte[valSize]);
      int size = KeyValueUtil.length(kv);
      KeyValue newKv = (KeyValue) mslab.copyCellInto(kv);
      if (newKv.getBuffer() != lastBuffer) {
        expectedOff = 0;
        lastBuffer = newKv.getBuffer();
      }
      assertEquals(expectedOff, newKv.getOffset());
      assertTrue("Allocation overruns buffer",
          newKv.getOffset() + size <= newKv.getBuffer().length);
      expectedOff += size;
    }
  }

  @Test
  public void testLABLargeAllocation() {
    MemStoreLAB mslab = new HeapMemStoreLAB();
    KeyValue kv = new KeyValue(rk, cf, q, new byte[2 * 1024 * 1024]);
    Cell newCell = mslab.copyCellInto(kv);
    assertNull("2MB allocation shouldn't be satisfied by LAB.", newCell);
  }

  /**
   * Test allocation from lots of threads, making sure the results don't
   * overlap in any way
   */
  @Test
  public void testLABThreading() throws Exception {
    Configuration conf = new Configuration();
    MultithreadedTestUtil.TestContext ctx =
      new MultithreadedTestUtil.TestContext(conf);
    
    final AtomicInteger totalAllocated = new AtomicInteger();
    
    final MemStoreLAB mslab = new HeapMemStoreLAB();
    List<List<AllocRecord>> allocations = Lists.newArrayList();
    
    for (int i = 0; i < 10; i++) {
      final List<AllocRecord> allocsByThisThread = Lists.newLinkedList();
      allocations.add(allocsByThisThread);
      
      TestThread t = new MultithreadedTestUtil.RepeatingTestThread(ctx) {
        private Random r = new Random();
        @Override
        public void doAnAction() throws Exception {
          int valSize = r.nextInt(1000);
          KeyValue kv = new KeyValue(rk, cf, q, new byte[valSize]);
          int size = KeyValueUtil.length(kv);
          KeyValue newKv = (KeyValue) mslab.copyCellInto(kv);
          totalAllocated.addAndGet(size);
          allocsByThisThread.add(new AllocRecord(newKv.getBuffer(), newKv.getOffset(), size));
        }
      };
      ctx.addThread(t);
    }
    
    ctx.startThreads();
    while (totalAllocated.get() < 50*1024*1024 && ctx.shouldRun()) {
      Thread.sleep(10);
    }
    ctx.stop();
    
    // Partition the allocations by the actual byte[] they point into,
    // make sure offsets are unique for each chunk
    Map<byte[], Map<Integer, AllocRecord>> mapsByChunk =
      Maps.newHashMap();
    
    int sizeCounted = 0;
    for (AllocRecord rec : Iterables.concat(allocations)) {
      sizeCounted += rec.size;
      if (rec.size == 0) continue;
      
      Map<Integer, AllocRecord> mapForThisByteArray =
        mapsByChunk.get(rec.alloc);
      if (mapForThisByteArray == null) {
        mapForThisByteArray = Maps.newTreeMap();
        mapsByChunk.put(rec.alloc, mapForThisByteArray);
      }
      AllocRecord oldVal = mapForThisByteArray.put(rec.offset, rec);
      assertNull("Already had an entry " + oldVal + " for allocation " + rec,
          oldVal);
    }
    assertEquals("Sanity check test", sizeCounted, totalAllocated.get());
    
    // Now check each byte array to make sure allocations don't overlap
    for (Map<Integer, AllocRecord> allocsInChunk : mapsByChunk.values()) {
      int expectedOff = 0;
      for (AllocRecord alloc : allocsInChunk.values()) {
        assertEquals(expectedOff, alloc.offset);
        assertTrue("Allocation overruns buffer",
            alloc.offset + alloc.size <= alloc.alloc.length);
        expectedOff += alloc.size;
      }
    }

  }

  /**
   * Test frequent chunk retirement with chunk pool triggered by lots of threads, making sure
   * there's no memory leak (HBASE-16195)
   * @throws Exception if any error occurred
   */
  @Test
  public void testLABChunkQueue() throws Exception {
    HeapMemStoreLAB mslab = new HeapMemStoreLAB();
    // by default setting, there should be no chunk queue initialized
    assertNull(mslab.getChunkQueue());
    // reset mslab with chunk pool
    Configuration conf = HBaseConfiguration.create();
    conf.setDouble(MemStoreChunkPool.CHUNK_POOL_MAXSIZE_KEY, 0.1);
    // set chunk size to default max alloc size, so we could easily trigger chunk retirement
    conf.setLong(HeapMemStoreLAB.CHUNK_SIZE_KEY, HeapMemStoreLAB.MAX_ALLOC_DEFAULT);
    // reconstruct mslab
    MemStoreChunkPool.clearDisableFlag();
    mslab = new HeapMemStoreLAB(conf);
    // launch multiple threads to trigger frequent chunk retirement
    List<Thread> threads = new ArrayList<Thread>();
    final KeyValue kv = new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("q"),
        new byte[HeapMemStoreLAB.MAX_ALLOC_DEFAULT - 24]);
    for (int i = 0; i < 10; i++) {
      threads.add(getChunkQueueTestThread(mslab, "testLABChunkQueue-" + i, kv));
    }
    for (Thread thread : threads) {
      thread.start();
    }
    // let it run for some time
    Thread.sleep(1000);
    for (Thread thread : threads) {
      thread.interrupt();
    }
    boolean threadsRunning = true;
    while (threadsRunning) {
      for (Thread thread : threads) {
        if (thread.isAlive()) {
          threadsRunning = true;
          break;
        }
      }
      threadsRunning = false;
    }
    // close the mslab
    mslab.close();
    // make sure all chunks reclaimed or removed from chunk queue
    int queueLength = mslab.getChunkQueue().size();
    assertTrue("All chunks in chunk queue should be reclaimed or removed"
        + " after mslab closed but actually: " + queueLength, queueLength == 0);
  }

  private Thread getChunkQueueTestThread(final HeapMemStoreLAB mslab, String threadName,
      Cell cellToCopyInto) {
    Thread thread = new Thread() {
      boolean stopped = false;

      @Override
      public void run() {
        while (!stopped) {
          // keep triggering chunk retirement
          mslab.copyCellInto(cellToCopyInto);
        }
      }

      @Override
      public void interrupt() {
        this.stopped = true;
      }
    };
    thread.setName(threadName);
    thread.setDaemon(true);
    return thread;
  }

  private static class AllocRecord implements Comparable<AllocRecord>{
    private final byte[] alloc;
    private final int offset;
    private final int size;

    public AllocRecord(byte[] alloc, int offset, int size) {
      super();
      this.alloc = alloc;
      this.offset = offset;
      this.size = size;
    }

    @Override
    public int compareTo(AllocRecord e) {
      if (alloc != e.alloc) {
        throw new RuntimeException("Can only compare within a particular array");
      }
      return Ints.compare(this.offset, e.offset);
    }
    
    @Override
    public String toString() {
      return "AllocRecord(offset=" + this.offset + ", size=" + size + ")";
    }
  }
}

