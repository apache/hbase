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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test the {@link MemStoreChunkPool} class
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestMemStoreChunkPool {
  private final static Configuration conf = new Configuration();
  private static MemStoreChunkPool chunkPool;
  private static boolean chunkPoolDisabledBeforeTest;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf.setBoolean(MemStoreLAB.USEMSLAB_KEY, true);
    conf.setFloat(MemStoreLAB.CHUNK_POOL_MAXSIZE_KEY, 0.2f);
    chunkPoolDisabledBeforeTest = MemStoreChunkPool.chunkPoolDisabled;
    MemStoreChunkPool.chunkPoolDisabled = false;
    long globalMemStoreLimit = (long) (ManagementFactory.getMemoryMXBean().getHeapMemoryUsage()
        .getMax() * MemorySizeUtil.getGlobalMemStoreHeapPercent(conf, false));
    chunkPool = MemStoreChunkPool.initialize(globalMemStoreLimit, 0.2f,
        MemStoreLAB.POOL_INITIAL_SIZE_DEFAULT, MemStoreLABImpl.CHUNK_SIZE_DEFAULT, false);
    assertTrue(chunkPool != null);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MemStoreChunkPool.chunkPoolDisabled = chunkPoolDisabledBeforeTest;
  }

  @Before
  public void tearDown() throws Exception {
    chunkPool.clearChunks();
  }

  @Test
  public void testReusingChunks() {
    Random rand = new Random();
    MemStoreLAB mslab = new MemStoreLABImpl(conf);
    int expectedOff = 0;
    byte[] lastBuffer = null;
    final byte[] rk = Bytes.toBytes("r1");
    final byte[] cf = Bytes.toBytes("f");
    final byte[] q = Bytes.toBytes("q");
    // Randomly allocate some bytes
    for (int i = 0; i < 100; i++) {
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
    // chunks will be put back to pool after close
    mslab.close();
    int chunkCount = chunkPool.getPoolSize();
    assertTrue(chunkCount > 0);
    // reconstruct mslab
    mslab = new MemStoreLABImpl(conf);
    // chunk should be got from the pool, so we can reuse it.
    KeyValue kv = new KeyValue(rk, cf, q, new byte[10]);
    mslab.copyCellInto(kv);
    assertEquals(chunkCount - 1, chunkPool.getPoolSize());
  }

  @Test
  public void testPuttingBackChunksAfterFlushing() throws UnexpectedStateException {
    byte[] row = Bytes.toBytes("testrow");
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf1 = Bytes.toBytes("testqualifier1");
    byte[] qf2 = Bytes.toBytes("testqualifier2");
    byte[] qf3 = Bytes.toBytes("testqualifier3");
    byte[] qf4 = Bytes.toBytes("testqualifier4");
    byte[] qf5 = Bytes.toBytes("testqualifier5");
    byte[] val = Bytes.toBytes("testval");

    DefaultMemStore memstore = new DefaultMemStore();

    // Setting up memstore
    memstore.add(new KeyValue(row, fam, qf1, val), null);
    memstore.add(new KeyValue(row, fam, qf2, val), null);
    memstore.add(new KeyValue(row, fam, qf3, val), null);

    // Creating a snapshot
    MemStoreSnapshot snapshot = memstore.snapshot();
    assertEquals(3, memstore.getSnapshot().getCellsCount());

    // Adding value to "new" memstore
    assertEquals(0, memstore.getActive().getCellsCount());
    memstore.add(new KeyValue(row, fam, qf4, val), null);
    memstore.add(new KeyValue(row, fam, qf5, val), null);
    assertEquals(2, memstore.getActive().getCellsCount());
    // close the scanner - this is how the snapshot will be used
    snapshot.getScanner().close();
    memstore.clearSnapshot(snapshot.getId());

    int chunkCount = chunkPool.getPoolSize();
    assertTrue(chunkCount > 0);

  }

  @Test
  public void testPuttingBackChunksWithOpeningScanner()
      throws IOException {
    byte[] row = Bytes.toBytes("testrow");
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf1 = Bytes.toBytes("testqualifier1");
    byte[] qf2 = Bytes.toBytes("testqualifier2");
    byte[] qf3 = Bytes.toBytes("testqualifier3");
    byte[] qf4 = Bytes.toBytes("testqualifier4");
    byte[] qf5 = Bytes.toBytes("testqualifier5");
    byte[] qf6 = Bytes.toBytes("testqualifier6");
    byte[] qf7 = Bytes.toBytes("testqualifier7");
    byte[] val = Bytes.toBytes("testval");

    DefaultMemStore memstore = new DefaultMemStore();

    // Setting up memstore
    memstore.add(new KeyValue(row, fam, qf1, val), null);
    memstore.add(new KeyValue(row, fam, qf2, val), null);
    memstore.add(new KeyValue(row, fam, qf3, val), null);

    // Creating a snapshot
    MemStoreSnapshot snapshot = memstore.snapshot();
    assertEquals(3, memstore.getSnapshot().getCellsCount());

    // Adding value to "new" memstore
    assertEquals(0, memstore.getActive().getCellsCount());
    memstore.add(new KeyValue(row, fam, qf4, val), null);
    memstore.add(new KeyValue(row, fam, qf5, val), null);
    assertEquals(2, memstore.getActive().getCellsCount());

    // opening scanner before clear the snapshot
    List<KeyValueScanner> scanners = memstore.getScanners(0);
    // Shouldn't putting back the chunks to pool,since some scanners are opening
    // based on their data
    // close the snapshot scanner
    snapshot.getScanner().close();
    memstore.clearSnapshot(snapshot.getId());

    assertTrue(chunkPool.getPoolSize() == 0);

    // Chunks will be put back to pool after close scanners;
    for (KeyValueScanner scanner : scanners) {
      scanner.close();
    }
    assertTrue(chunkPool.getPoolSize() > 0);

    // clear chunks
    chunkPool.clearChunks();

    // Creating another snapshot
    snapshot = memstore.snapshot();
    // Adding more value
    memstore.add(new KeyValue(row, fam, qf6, val), null);
    memstore.add(new KeyValue(row, fam, qf7, val), null);
    // opening scanners
    scanners = memstore.getScanners(0);
    // close scanners before clear the snapshot
    for (KeyValueScanner scanner : scanners) {
      scanner.close();
    }
    // Since no opening scanner, the chunks of snapshot should be put back to
    // pool
    // close the snapshot scanner
    snapshot.getScanner().close();
    memstore.clearSnapshot(snapshot.getId());
    assertTrue(chunkPool.getPoolSize() > 0);
  }

  @Test
  public void testPutbackChunksMultiThreaded() throws Exception {
    MemStoreChunkPool oldPool = MemStoreChunkPool.GLOBAL_INSTANCE;
    final int maxCount = 10;
    final int initialCount = 5;
    final int chunkSize = 30;
    final int valSize = 7;
    MemStoreChunkPool pool = new MemStoreChunkPool(chunkSize, maxCount, initialCount, 1, false);
    assertEquals(initialCount, pool.getPoolSize());
    assertEquals(maxCount, pool.getMaxCount());
    MemStoreChunkPool.GLOBAL_INSTANCE = pool;// Replace the global ref with the new one we created.
                                             // Used it for the testing. Later in finally we put
                                             // back the original
    final KeyValue kv = new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("q"),
        new byte[valSize]);
    try {
      Runnable r = new Runnable() {
        @Override
        public void run() {
          MemStoreLAB memStoreLAB = new MemStoreLABImpl(conf);
          for (int i = 0; i < maxCount; i++) {
            memStoreLAB.copyCellInto(kv);// Try allocate size = chunkSize. Means every
                                         // allocate call will result in a new chunk
          }
          // Close MemStoreLAB so that all chunks will be tried to be put back to pool
          memStoreLAB.close();
        }
      };
      Thread t1 = new Thread(r);
      Thread t2 = new Thread(r);
      Thread t3 = new Thread(r);
      t1.start();
      t2.start();
      t3.start();
      t1.join();
      t2.join();
      t3.join();
      assertTrue(pool.getPoolSize() <= maxCount);
    } finally {
      MemStoreChunkPool.GLOBAL_INSTANCE = oldPool;
    }
  }
}
