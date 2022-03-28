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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Ignore // See HBASE-19742 for issue on reenabling.
@Category({RegionServerTests.class, SmallTests.class})
public class TestMemstoreLABWithoutPool {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMemstoreLABWithoutPool.class);

  private final static Configuration conf = new Configuration();

  private static final byte[] rk = Bytes.toBytes("r1");
  private static final byte[] cf = Bytes.toBytes("f");
  private static final byte[] q = Bytes.toBytes("q");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    long globalMemStoreLimit = (long) (ManagementFactory.getMemoryMXBean().getHeapMemoryUsage()
        .getMax() * 0.8);
    // disable pool
    ChunkCreator.initialize(MemStoreLABImpl.CHUNK_SIZE_DEFAULT + Bytes.SIZEOF_LONG,
      false, globalMemStoreLimit, 0.0f, MemStoreLAB.POOL_INITIAL_SIZE_DEFAULT,
      null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
  }

  /**
   * Test a bunch of random allocations
   */
  @Test
  public void testLABRandomAllocation() {
    MemStoreLAB mslab = new MemStoreLABImpl();
    int expectedOff = 0;
    ByteBuffer lastBuffer = null;
    int lastChunkId = -1;
    // 100K iterations by 0-1K alloc -> 50MB expected
    // should be reasonable for unit test and also cover wraparound
    // behavior
    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < 100000; i++) {
      int valSize = rand.nextInt(1000);
      KeyValue kv = new KeyValue(rk, cf, q, new byte[valSize]);
      int size = kv.getSerializedSize();
      ByteBufferKeyValue newKv = (ByteBufferKeyValue) mslab.copyCellInto(kv);
      if (newKv.getBuffer() != lastBuffer) {
        // since we add the chunkID at the 0th offset of the chunk and the
        // chunkid is an int we need to account for those 4 bytes
        expectedOff = Bytes.SIZEOF_INT;
        lastBuffer = newKv.getBuffer();
        int chunkId = newKv.getBuffer().getInt(0);
        assertTrue("chunkid should be different", chunkId != lastChunkId);
        lastChunkId = chunkId;
      }
      assertEquals(expectedOff, newKv.getOffset());
      assertTrue("Allocation overruns buffer",
          newKv.getOffset() + size <= newKv.getBuffer().capacity());
      expectedOff += size;
    }
  }

  /**
   * Test frequent chunk retirement with chunk pool triggered by lots of threads, making sure
   * there's no memory leak (HBASE-16195)
   * @throws Exception if any error occurred
   */
  @Test
  public void testLABChunkQueueWithMultipleMSLABs() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    MemStoreLABImpl[] mslab = new MemStoreLABImpl[10];
    for (int i = 0; i < 10; i++) {
      mslab[i] = new MemStoreLABImpl(conf);
    }
    // launch multiple threads to trigger frequent chunk retirement
    List<Thread> threads = new ArrayList<>();
    // create smaller sized kvs
    final KeyValue kv = new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("q"),
        new byte[0]);
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        threads.add(getChunkQueueTestThread(mslab[i], "testLABChunkQueue-" + j, kv));
      }
    }
    for (Thread thread : threads) {
      thread.start();
    }
    // let it run for some time
    Thread.sleep(3000);
    for (Thread thread : threads) {
      thread.interrupt();
    }
    boolean threadsRunning = true;
    boolean alive = false;
    while (threadsRunning) {
      alive = false;
      for (Thread thread : threads) {
        if (thread.isAlive()) {
          alive = true;
          break;
        }
      }
      if (!alive) {
        threadsRunning = false;
      }
    }
    // close the mslab
    for (int i = 0; i < 10; i++) {
      mslab[i].close();
    }
    // all of the chunkIds would have been returned back
    assertTrue("All the chunks must have been cleared",
        ChunkCreator.instance.numberOfMappedChunks() == 0);
  }

  private Thread getChunkQueueTestThread(final MemStoreLABImpl mslab, String threadName,
      Cell cellToCopyInto) {
    Thread thread = new Thread() {
      volatile boolean stopped = false;

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
}
