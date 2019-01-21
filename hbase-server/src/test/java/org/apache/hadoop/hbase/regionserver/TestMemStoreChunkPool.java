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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the {@link MemStoreChunkPool} class
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestMemStoreChunkPool {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMemStoreChunkPool.class);

  private final static Configuration conf = new Configuration();
  private static ChunkCreator chunkCreator;
  private static boolean chunkPoolDisabledBeforeTest;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf.setBoolean(MemStoreLAB.USEMSLAB_KEY, true);
    conf.setFloat(MemStoreLAB.CHUNK_POOL_MAXSIZE_KEY, 0.2f);
    chunkPoolDisabledBeforeTest = ChunkCreator.chunkPoolDisabled;
    ChunkCreator.chunkPoolDisabled = false;
    long globalMemStoreLimit = (long) (ManagementFactory.getMemoryMXBean().getHeapMemoryUsage()
        .getMax() * MemorySizeUtil.getGlobalMemStoreHeapPercent(conf, false));
    chunkCreator = ChunkCreator.initialize(MemStoreLABImpl.CHUNK_SIZE_DEFAULT, false,
      globalMemStoreLimit, 0.2f, MemStoreLAB.POOL_INITIAL_SIZE_DEFAULT, null);
    assertTrue(chunkCreator != null);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    ChunkCreator.chunkPoolDisabled = chunkPoolDisabledBeforeTest;
  }

  @After
  public void tearDown() throws Exception {
    chunkCreator.clearChunksInPool();
  }

  @Test
  public void testReusingChunks() {
    Random rand = new Random();
    MemStoreLAB mslab = new MemStoreLABImpl(conf);
    int expectedOff = 0;
    ByteBuffer lastBuffer = null;
    final byte[] rk = Bytes.toBytes("r1");
    final byte[] cf = Bytes.toBytes("f");
    final byte[] q = Bytes.toBytes("q");
    // Randomly allocate some bytes
    for (int i = 0; i < 100; i++) {
      int valSize = rand.nextInt(1000);
      KeyValue kv = new KeyValue(rk, cf, q, new byte[valSize]);
      int size = kv.getSerializedSize();
      ByteBufferKeyValue newKv = (ByteBufferKeyValue) mslab.copyCellInto(kv);
      if (newKv.getBuffer() != lastBuffer) {
        expectedOff = 4;
        lastBuffer = newKv.getBuffer();
      }
      assertEquals(expectedOff, newKv.getOffset());
      assertTrue("Allocation overruns buffer",
          newKv.getOffset() + size <= newKv.getBuffer().capacity());
      expectedOff += size;
    }
    // chunks will be put back to pool after close
    mslab.close();
    int chunkCount = chunkCreator.getPoolSize();
    assertTrue(chunkCount > 0);
    // reconstruct mslab
    mslab = new MemStoreLABImpl(conf);
    // chunk should be got from the pool, so we can reuse it.
    KeyValue kv = new KeyValue(rk, cf, q, new byte[10]);
    mslab.copyCellInto(kv);
    assertEquals(chunkCount - 1, chunkCreator.getPoolSize());
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
    for(KeyValueScanner scanner : snapshot.getScanners()) {
      scanner.close();
    }
    memstore.clearSnapshot(snapshot.getId());

    int chunkCount = chunkCreator.getPoolSize();
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
    for(KeyValueScanner scanner : snapshot.getScanners()) {
      scanner.close();
    }
    memstore.clearSnapshot(snapshot.getId());

    assertTrue(chunkCreator.getPoolSize() == 0);

    // Chunks will be put back to pool after close scanners;
    for (KeyValueScanner scanner : scanners) {
      scanner.close();
    }
    assertTrue(chunkCreator.getPoolSize() > 0);

    // clear chunks
    chunkCreator.clearChunksInPool();

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
    // close the snapshot scanners
    for(KeyValueScanner scanner : snapshot.getScanners()) {
      scanner.close();
    }
    memstore.clearSnapshot(snapshot.getId());
    assertTrue(chunkCreator.getPoolSize() > 0);
  }

  @Test
  public void testPutbackChunksMultiThreaded() throws Exception {
    final int maxCount = 10;
    final int initialCount = 5;
    final int chunkSize = 40;
    final int valSize = 7;
    ChunkCreator oldCreator = ChunkCreator.getInstance();
    ChunkCreator newCreator = new ChunkCreator(chunkSize, false, 400, 1, 0.5f, null, 0);
    assertEquals(initialCount, newCreator.getPoolSize());
    assertEquals(maxCount, newCreator.getMaxCount());
    ChunkCreator.instance = newCreator;// Replace the global ref with the new one we created.
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
      assertTrue(newCreator.getPoolSize() <= maxCount);
    } finally {
      ChunkCreator.instance = oldCreator;
    }
  }
}
