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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.regionserver.ChunkCreator.ChunkType;
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
 * Test the {@link org.apache.hadoop.hbase.regionserver.ChunkCreator.MemStoreChunkPool} class
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
    chunkCreator = ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false,
      globalMemStoreLimit, 0.2f, MemStoreLAB.POOL_INITIAL_SIZE_DEFAULT,
      null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    assertNotNull(chunkCreator);
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
    MemStoreLAB mslab = new MemStoreLABImpl(conf);
    int expectedOff = 0;
    ByteBuffer lastBuffer = null;
    final byte[] rk = Bytes.toBytes("r1");
    final byte[] cf = Bytes.toBytes("f");
    final byte[] q = Bytes.toBytes("q");
    // Randomly allocate some bytes
    final Random rand = ThreadLocalRandom.current();
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
    // Replace the global ref with the new one we created.
    // Used it for the testing. Later in finally we put
    // back the original
    ChunkCreator.instance = newCreator;

    final KeyValue kv = new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("q"),
        new byte[valSize]);
    final AtomicReference<Throwable> exceptionRef = new AtomicReference<Throwable>();
    try {
      Runnable r = new Runnable() {
        @Override
        public void run() {
          try {
            MemStoreLAB memStoreLAB = new MemStoreLABImpl(conf);
            for (int i = 0; i < maxCount; i++) {
              // Try allocate size = chunkSize. Means every
              // allocate call will result in a new chunk
              memStoreLAB.copyCellInto(kv);
            }
            // Close MemStoreLAB so that all chunks will be tried to be put back to pool
            memStoreLAB.close();
          } catch (Throwable execption) {
            exceptionRef.set(execption);
          }
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
      assertTrue(exceptionRef.get() == null);
      assertTrue(newCreator.getPoolSize() <= maxCount && newCreator.getPoolSize() > 0);
    } finally {
      ChunkCreator.instance = oldCreator;
    }
  }

  // This test is for HBASE-26142, which throws NPE when indexChunksPool is null.
  @Test
  public void testNoIndexChunksPoolOrNoDataChunksPool() throws Exception {
    final int maxCount = 10;
    final int initialCount = 5;
    final int newChunkSize = 40;
    final int valSize = 7;

    ChunkCreator oldCreator = ChunkCreator.getInstance();
    try {
      // Test dataChunksPool is not null and indexChunksPool is null
      ChunkCreator newCreator = new ChunkCreator(newChunkSize, false, 400, 1, 0.5f, null, 0);
      assertEquals(initialCount, newCreator.getPoolSize());
      assertEquals(0, newCreator.getPoolSize(ChunkType.INDEX_CHUNK));
      assertEquals(maxCount, newCreator.getMaxCount());
      assertEquals(0, newCreator.getMaxCount(ChunkType.INDEX_CHUNK));
      assertTrue(newCreator.getDataChunksPool() != null);
      assertTrue(newCreator.getIndexChunksPool() == null);
      ChunkCreator.instance = newCreator;
      final KeyValue kv = new KeyValue(Bytes.toBytes("r"), Bytes.toBytes("f"), Bytes.toBytes("q"),
          new byte[valSize]);

      MemStoreLAB memStoreLAB = new MemStoreLABImpl(conf);
      memStoreLAB.copyCellInto(kv);
      memStoreLAB.close();
      assertEquals(initialCount, newCreator.getPoolSize());
      assertEquals(0, newCreator.getPoolSize(ChunkType.INDEX_CHUNK));

      Chunk dataChunk = newCreator.getChunk(CompactingMemStore.IndexType.CHUNK_MAP);
      assertTrue(dataChunk.isDataChunk());
      assertTrue(dataChunk.isFromPool());
      assertEquals(initialCount - 1, newCreator.getPoolSize());
      assertEquals(0, newCreator.getPoolSize(ChunkType.INDEX_CHUNK));
      newCreator.putbackChunks(Collections.singleton(dataChunk.getId()));
      assertEquals(initialCount, newCreator.getPoolSize());
      assertEquals(0, newCreator.getPoolSize(ChunkType.INDEX_CHUNK));

      // We set ChunkCreator.indexChunkSize to 0, but we want to get a IndexChunk
      try {
        newCreator.getChunk(CompactingMemStore.IndexType.CHUNK_MAP, ChunkType.INDEX_CHUNK);
        fail();
      } catch (IllegalArgumentException e) {
      }

      Chunk jumboChunk = newCreator.getJumboChunk(newChunkSize + 10);
      assertTrue(jumboChunk.isJumbo());
      assertTrue(!jumboChunk.isFromPool());
      assertEquals(initialCount, newCreator.getPoolSize());
      assertEquals(0, newCreator.getPoolSize(ChunkType.INDEX_CHUNK));

      // Test both dataChunksPool and indexChunksPool are null
      newCreator = new ChunkCreator(newChunkSize, false, 400, 0, 0.5f, null, 0);
      assertEquals(0, newCreator.getPoolSize());
      assertEquals(0, newCreator.getPoolSize(ChunkType.INDEX_CHUNK));
      assertEquals(0, newCreator.getMaxCount());
      assertEquals(0, newCreator.getMaxCount(ChunkType.INDEX_CHUNK));
      assertTrue(newCreator.getDataChunksPool() == null);
      assertTrue(newCreator.getIndexChunksPool() == null);
      ChunkCreator.instance = newCreator;

      memStoreLAB = new MemStoreLABImpl(conf);
      memStoreLAB.copyCellInto(kv);
      memStoreLAB.close();
      assertEquals(0, newCreator.getPoolSize());
      assertEquals(0, newCreator.getPoolSize(ChunkType.INDEX_CHUNK));

      dataChunk = newCreator.getChunk(CompactingMemStore.IndexType.CHUNK_MAP);
      assertTrue(dataChunk.isDataChunk());
      assertTrue(!dataChunk.isFromPool());
      assertEquals(0, newCreator.getPoolSize());
      assertEquals(0, newCreator.getPoolSize(ChunkType.INDEX_CHUNK));

      try {
        // We set ChunkCreator.indexChunkSize to 0, but we want to get a IndexChunk
        newCreator.getChunk(CompactingMemStore.IndexType.CHUNK_MAP, ChunkType.INDEX_CHUNK);
        fail();
      } catch (IllegalArgumentException e) {
      }

      jumboChunk = newCreator.getJumboChunk(newChunkSize + 10);
      assertTrue(jumboChunk.isJumbo());
      assertTrue(!jumboChunk.isFromPool());
      assertEquals(0, newCreator.getPoolSize());
      assertEquals(0, newCreator.getPoolSize(ChunkType.INDEX_CHUNK));

      // Test dataChunksPool is null and indexChunksPool is not null
      newCreator = new ChunkCreator(newChunkSize, false, 400, 1, 0.5f, null, 1);
      assertEquals(0, newCreator.getPoolSize());
      assertEquals(initialCount, newCreator.getPoolSize(ChunkType.INDEX_CHUNK));
      assertEquals(0, newCreator.getMaxCount());
      assertEquals(maxCount, newCreator.getMaxCount(ChunkType.INDEX_CHUNK));
      assertTrue(newCreator.getDataChunksPool() == null);
      assertTrue(newCreator.getIndexChunksPool() != null);
      assertEquals(newCreator.getChunkSize(ChunkType.DATA_CHUNK),
        newCreator.getChunkSize(ChunkType.INDEX_CHUNK));
      ChunkCreator.instance = newCreator;

      memStoreLAB = new MemStoreLABImpl(conf);
      memStoreLAB.copyCellInto(kv);
      memStoreLAB.close();
      assertEquals(0, newCreator.getPoolSize());
      assertEquals(initialCount, newCreator.getPoolSize(ChunkType.INDEX_CHUNK));

      dataChunk = newCreator.getChunk(CompactingMemStore.IndexType.CHUNK_MAP);
      assertTrue(dataChunk.isDataChunk());
      assertTrue(!dataChunk.isFromPool());
      assertEquals(0, newCreator.getPoolSize());
      assertEquals(initialCount, newCreator.getPoolSize(ChunkType.INDEX_CHUNK));

      Chunk indexChunk =
          newCreator.getChunk(CompactingMemStore.IndexType.CHUNK_MAP, ChunkType.INDEX_CHUNK);
      assertEquals(0, newCreator.getPoolSize());
      assertEquals(initialCount - 1, newCreator.getPoolSize(ChunkType.INDEX_CHUNK));
      assertTrue(indexChunk.isIndexChunk());
      assertTrue(indexChunk.isFromPool());
      newCreator.putbackChunks(Collections.singleton(indexChunk.getId()));
      assertEquals(0, newCreator.getPoolSize());
      assertEquals(initialCount, newCreator.getPoolSize(ChunkType.INDEX_CHUNK));

      jumboChunk = newCreator.getJumboChunk(newChunkSize + 10);
      assertTrue(jumboChunk.isJumbo());
      assertTrue(!jumboChunk.isFromPool());
      assertEquals(0, newCreator.getPoolSize());
      assertEquals(initialCount, newCreator.getPoolSize(ChunkType.INDEX_CHUNK));
    } finally {
      ChunkCreator.instance = oldCreator;
    }

    // Test both dataChunksPool and indexChunksPool are not null
    assertTrue(ChunkCreator.getInstance().getDataChunksPool() != null);
    assertTrue(ChunkCreator.getInstance().getIndexChunksPool() != null);
    Chunk dataChunk = ChunkCreator.getInstance().getChunk(CompactingMemStore.IndexType.CHUNK_MAP);
    assertTrue(dataChunk.isDataChunk());
    assertTrue(dataChunk.isFromPool());
    Chunk indexChunk = ChunkCreator.getInstance().getChunk(CompactingMemStore.IndexType.CHUNK_MAP,
      ChunkType.INDEX_CHUNK);
    assertTrue(indexChunk.isIndexChunk());
    assertTrue(indexChunk.isFromPool());
    Chunk jumboChunk =
        ChunkCreator.getInstance().getJumboChunk(ChunkCreator.getInstance().getChunkSize() + 10);
    assertTrue(jumboChunk.isJumbo());
    assertTrue(!jumboChunk.isFromPool());
  }
}
