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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.rmi.UnexpectedException;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB.Allocation;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the {@link MemStoreChunkPool} class
 */
@Category(SmallTests.class)
public class TestMemStoreChunkPool {
  private final static Configuration conf = new Configuration();
  private static MemStoreChunkPool chunkPool;
  private static boolean chunkPoolDisabledBeforeTest;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf.setBoolean(MemStore.USEMSLAB_KEY, true);
    conf.setFloat(MemStoreChunkPool.CHUNK_POOL_MAXSIZE_KEY, 0.2f);
    chunkPoolDisabledBeforeTest = MemStoreChunkPool.chunkPoolDisabled;
    MemStoreChunkPool.chunkPoolDisabled = false;
    chunkPool = MemStoreChunkPool.getPool(conf);
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
    MemStoreLAB mslab = new MemStoreLAB(conf, chunkPool);
    int expectedOff = 0;
    byte[] lastBuffer = null;
    // Randomly allocate some bytes
    for (int i = 0; i < 100; i++) {
      int size = rand.nextInt(1000);
      Allocation alloc = mslab.allocateBytes(size);

      if (alloc.getData() != lastBuffer) {
        expectedOff = 0;
        lastBuffer = alloc.getData();
      }
      assertEquals(expectedOff, alloc.getOffset());
      assertTrue("Allocation " + alloc + " overruns buffer", alloc.getOffset()
          + size <= alloc.getData().length);
      expectedOff += size;
    }
    // chunks will be put back to pool after close
    mslab.close();
    int chunkCount = chunkPool.getPoolSize();
    assertTrue(chunkCount > 0);
    // reconstruct mslab
    mslab = new MemStoreLAB(conf, chunkPool);
    // chunk should be got from the pool, so we can reuse it.
    mslab.allocateBytes(1000);
    assertEquals(chunkCount - 1, chunkPool.getPoolSize());
  }

  @Test
  public void testPuttingBackChunksAfterFlushing() throws UnexpectedException {
    byte[] row = Bytes.toBytes("testrow");
    byte[] fam = Bytes.toBytes("testfamily");
    byte[] qf1 = Bytes.toBytes("testqualifier1");
    byte[] qf2 = Bytes.toBytes("testqualifier2");
    byte[] qf3 = Bytes.toBytes("testqualifier3");
    byte[] qf4 = Bytes.toBytes("testqualifier4");
    byte[] qf5 = Bytes.toBytes("testqualifier5");
    byte[] val = Bytes.toBytes("testval");

    MemStore memstore = new MemStore();

    // Setting up memstore
    memstore.add(new KeyValue(row, fam, qf1, val));
    memstore.add(new KeyValue(row, fam, qf2, val));
    memstore.add(new KeyValue(row, fam, qf3, val));

    // Creating a snapshot
    memstore.snapshot();
    KeyValueSkipListSet snapshot = memstore.getSnapshot();
    assertEquals(3, memstore.snapshot.size());

    // Adding value to "new" memstore
    assertEquals(0, memstore.kvset.size());
    memstore.add(new KeyValue(row, fam, qf4, val));
    memstore.add(new KeyValue(row, fam, qf5, val));
    assertEquals(2, memstore.kvset.size());
    memstore.clearSnapshot(snapshot);

    int chunkCount = chunkPool.getPoolSize();
    assertTrue(chunkCount > 0);

  }

  @Test
  public void testPuttingBackChunksWithOpeningScanner()
      throws UnexpectedException {
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

    MemStore memstore = new MemStore();

    // Setting up memstore
    memstore.add(new KeyValue(row, fam, qf1, val));
    memstore.add(new KeyValue(row, fam, qf2, val));
    memstore.add(new KeyValue(row, fam, qf3, val));

    // Creating a snapshot
    memstore.snapshot();
    KeyValueSkipListSet snapshot = memstore.getSnapshot();
    assertEquals(3, memstore.snapshot.size());

    // Adding value to "new" memstore
    assertEquals(0, memstore.kvset.size());
    memstore.add(new KeyValue(row, fam, qf4, val));
    memstore.add(new KeyValue(row, fam, qf5, val));
    assertEquals(2, memstore.kvset.size());

    // opening scanner before clear the snapshot
    List<KeyValueScanner> scanners = memstore.getScanners(0);
    // Shouldn't putting back the chunks to pool,since some scanners are opening
    // based on their data
    memstore.clearSnapshot(snapshot);

    assertTrue(chunkPool.getPoolSize() == 0);

    // Chunks will be put back to pool after close scanners;
    for (KeyValueScanner scanner : scanners) {
      scanner.close();
    }
    assertTrue(chunkPool.getPoolSize() > 0);

    // clear chunks
    chunkPool.clearChunks();

    // Creating another snapshot
    memstore.snapshot();
    snapshot = memstore.getSnapshot();
    // Adding more value
    memstore.add(new KeyValue(row, fam, qf6, val));
    memstore.add(new KeyValue(row, fam, qf7, val));
    // opening scanners
    scanners = memstore.getScanners(0);
    // close scanners before clear the snapshot
    for (KeyValueScanner scanner : scanners) {
      scanner.close();
    }
    // Since no opening scanner, the chunks of snapshot should be put back to
    // pool
    memstore.clearSnapshot(snapshot);
    assertTrue(chunkPool.getPoolSize() > 0);
  }

}
