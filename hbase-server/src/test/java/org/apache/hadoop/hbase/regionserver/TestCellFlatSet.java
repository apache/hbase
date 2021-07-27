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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.regionserver.ChunkCreator.ChunkType;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category({RegionServerTests.class, SmallTests.class})
@RunWith(Parameterized.class)
public class TestCellFlatSet {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCellFlatSet.class);

  @Parameterized.Parameters
  public static Object[] data() {
    return new Object[] { "SMALL_CHUNKS", "NORMAL_CHUNKS" }; // test with different chunk sizes
  }
  private static final int NUM_OF_CELLS = 4;
  private static final int SMALL_CHUNK_SIZE = 64;
  private Cell ascCells[];
  private CellArrayMap ascCbOnHeap;
  private Cell descCells[];
  private CellArrayMap descCbOnHeap;
  private final static Configuration CONF = new Configuration();
  private KeyValue lowerOuterCell;
  private KeyValue upperOuterCell;


  private CellChunkMap ascCCM;   // for testing ascending CellChunkMap with one chunk in array
  private CellChunkMap descCCM;  // for testing descending CellChunkMap with one chunk in array
  private final boolean smallChunks;
  private static ChunkCreator chunkCreator;


  public TestCellFlatSet(String chunkType){
    long globalMemStoreLimit = (long) (ManagementFactory.getMemoryMXBean().getHeapMemoryUsage()
        .getMax() * MemorySizeUtil.getGlobalMemStoreHeapPercent(CONF, false));
    if (chunkType.equals("NORMAL_CHUNKS")) {
      chunkCreator = ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false,
        globalMemStoreLimit, 0.2f, MemStoreLAB.POOL_INITIAL_SIZE_DEFAULT,
        null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
      assertNotNull(chunkCreator);
      smallChunks = false;
    } else {
      // chunkCreator with smaller chunk size, so only 3 cell-representations can accommodate a chunk
      chunkCreator = ChunkCreator.initialize(SMALL_CHUNK_SIZE, false,
        globalMemStoreLimit, 0.2f, MemStoreLAB.POOL_INITIAL_SIZE_DEFAULT,
        null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
      assertNotNull(chunkCreator);
      smallChunks = true;
    }
  }

  @Before
  public void setUp() throws Exception {
    // create array of Cells to bass to the CellFlatMap under CellSet
    final byte[] one = Bytes.toBytes(15);
    final byte[] two = Bytes.toBytes(25);
    final byte[] three = Bytes.toBytes(35);
    final byte[] four = Bytes.toBytes(45);

    final byte[] f = Bytes.toBytes("f");
    final byte[] q = Bytes.toBytes("q");
    final byte[] v = Bytes.toBytes(4);

    final KeyValue kv1 = new KeyValue(one, f, q, 10, v);
    final KeyValue kv2 = new KeyValue(two, f, q, 20, v);
    final KeyValue kv3 = new KeyValue(three, f, q, 30, v);
    final KeyValue kv4 = new KeyValue(four, f, q, 40, v);
    lowerOuterCell = new KeyValue(Bytes.toBytes(10), f, q, 10, v);
    upperOuterCell = new KeyValue(Bytes.toBytes(50), f, q, 10, v);
    ascCells = new Cell[] {kv1,kv2,kv3,kv4};
    ascCbOnHeap = new CellArrayMap(CellComparator.getInstance(), ascCells,0, NUM_OF_CELLS,false);
    descCells = new Cell[] {kv4,kv3,kv2,kv1};
    descCbOnHeap = new CellArrayMap(CellComparator.getInstance(), descCells,0, NUM_OF_CELLS,true);

    CONF.setBoolean(MemStoreLAB.USEMSLAB_KEY, true);
    CONF.setFloat(MemStoreLAB.CHUNK_POOL_MAXSIZE_KEY, 0.2f);
    ChunkCreator.chunkPoolDisabled = false;

    // create ascending and descending CellChunkMaps
    // according to parameter, once built with normal chunks and at second with small chunks
    ascCCM = setUpCellChunkMap(true);
    descCCM = setUpCellChunkMap(false);

    if (smallChunks) {    // check jumbo chunks as well
      ascCCM = setUpJumboCellChunkMap(true);
    }
  }

  /* Create and test ascending CellSet based on CellArrayMap */
  @Test
  public void testCellArrayMapAsc() throws Exception {
    CellSet cs = new CellSet(ascCbOnHeap);
    testCellBlocks(cs);
    testIterators(cs);
  }

  /* Create and test ascending and descending CellSet based on CellChunkMap */
  @Test
  public void testCellChunkMap() throws Exception {
    CellSet cs = new CellSet(ascCCM);
    testCellBlocks(cs);
    testIterators(cs);
    testSubSet(cs);
    cs = new CellSet(descCCM);
    testSubSet(cs);
//    cs = new CellSet(ascMultCCM);
//    testCellBlocks(cs);
//    testSubSet(cs);
//    cs = new CellSet(descMultCCM);
//    testSubSet(cs);
  }

  @Test
  public void testAsc() throws Exception {
    CellSet ascCs = new CellSet(ascCbOnHeap);
    assertEquals(NUM_OF_CELLS, ascCs.size());
    testSubSet(ascCs);
  }
  @Test
  public void testDesc() throws Exception {
    CellSet descCs = new CellSet(descCbOnHeap);
    assertEquals(NUM_OF_CELLS, descCs.size());
    testSubSet(descCs);
  }

  private void testSubSet(CellSet cs) throws Exception {
    for (int i = 0; i != ascCells.length; ++i) {
      NavigableSet<Cell> excludeTail = cs.tailSet(ascCells[i], false);
      NavigableSet<Cell> includeTail = cs.tailSet(ascCells[i], true);
      assertEquals(ascCells.length - 1 - i, excludeTail.size());
      assertEquals(ascCells.length - i, includeTail.size());
      Iterator<Cell> excludeIter = excludeTail.iterator();
      Iterator<Cell> includeIter = includeTail.iterator();
      for (int j = 1 + i; j != ascCells.length; ++j) {
        assertEquals(true, CellUtil.equals(excludeIter.next(), ascCells[j]));
      }
      for (int j = i; j != ascCells.length; ++j) {
        assertEquals(true, CellUtil.equals(includeIter.next(), ascCells[j]));
      }
    }
    assertEquals(NUM_OF_CELLS, cs.tailSet(lowerOuterCell, false).size());
    assertEquals(0, cs.tailSet(upperOuterCell, false).size());
    for (int i = 0; i != ascCells.length; ++i) {
      NavigableSet<Cell> excludeHead = cs.headSet(ascCells[i], false);
      NavigableSet<Cell> includeHead = cs.headSet(ascCells[i], true);
      assertEquals(i, excludeHead.size());
      assertEquals(i + 1, includeHead.size());
      Iterator<Cell> excludeIter = excludeHead.iterator();
      Iterator<Cell> includeIter = includeHead.iterator();
      for (int j = 0; j != i; ++j) {
        assertEquals(true, CellUtil.equals(excludeIter.next(), ascCells[j]));
      }
      for (int j = 0; j != i + 1; ++j) {
        assertEquals(true, CellUtil.equals(includeIter.next(), ascCells[j]));
      }
    }
    assertEquals(0, cs.headSet(lowerOuterCell, false).size());
    assertEquals(NUM_OF_CELLS, cs.headSet(upperOuterCell, false).size());

    NavigableMap<Cell, Cell> sub = cs.getDelegatee().subMap(lowerOuterCell, true, upperOuterCell, true);
    assertEquals(NUM_OF_CELLS, sub.size());
    Iterator<Cell> iter = sub.values().iterator();
    for (int i = 0; i != ascCells.length; ++i) {
      assertEquals(true, CellUtil.equals(iter.next(), ascCells[i]));
    }
  }

  /* Generic basic test for immutable CellSet */
  private void testCellBlocks(CellSet cs) throws Exception {
    final byte[] oneAndHalf = Bytes.toBytes(20);
    final byte[] f = Bytes.toBytes("f");
    final byte[] q = Bytes.toBytes("q");
    final byte[] v = Bytes.toBytes(4);
    final KeyValue outerCell = new KeyValue(oneAndHalf, f, q, 10, v);

    assertEquals(NUM_OF_CELLS, cs.size());          // check size
    assertFalse(cs.contains(outerCell));            // check outer cell

    assertTrue(cs.contains(ascCells[0]));           // check existence of the first
    Cell first = cs.first();
    assertTrue(ascCells[0].equals(first));

    assertTrue(cs.contains(ascCells[NUM_OF_CELLS - 1]));  // check last
    Cell last = cs.last();
    assertTrue(ascCells[NUM_OF_CELLS - 1].equals(last));

    SortedSet<Cell> tail = cs.tailSet(ascCells[1]);    // check tail abd head sizes
    assertEquals(NUM_OF_CELLS - 1, tail.size());
    SortedSet<Cell> head = cs.headSet(ascCells[1]);
    assertEquals(1, head.size());

    SortedSet<Cell> tailOuter = cs.tailSet(outerCell);  // check tail starting from outer cell
    assertEquals(NUM_OF_CELLS - 1, tailOuter.size());

    Cell tailFirst = tail.first();
    assertTrue(ascCells[1].equals(tailFirst));
    Cell tailLast = tail.last();
    assertTrue(ascCells[NUM_OF_CELLS - 1].equals(tailLast));

    Cell headFirst = head.first();
    assertTrue(ascCells[0].equals(headFirst));
    Cell headLast = head.last();
    assertTrue(ascCells[0].equals(headLast));
  }

  /* Generic iterators test for immutable CellSet */
  private void testIterators(CellSet cs) throws Exception {

    // Assert that we have NUM_OF_CELLS values and that they are in order
    int count = 0;
    for (Cell kv: cs) {
      assertEquals("\n\n-------------------------------------------------------------------\n"
              + "Comparing iteration number " + (count + 1) + " the returned cell: " + kv
              + ", the first Cell in the CellBlocksMap: " + ascCells[count]
              + ", and the same transformed to String: " + ascCells[count].toString()
              + "\n-------------------------------------------------------------------\n",
              ascCells[count], kv);
      count++;
    }
    assertEquals(NUM_OF_CELLS, count);

    // Test descending iterator
    count = 0;
    for (Iterator<Cell> i = cs.descendingIterator(); i.hasNext();) {
      Cell kv = i.next();
      assertEquals(ascCells[NUM_OF_CELLS - (count + 1)], kv);
      count++;
    }
    assertEquals(NUM_OF_CELLS, count);
  }

  /* Create CellChunkMap with four cells inside the index chunk */
  private CellChunkMap setUpCellChunkMap(boolean asc) {

    // allocate new chunks and use the data chunk to hold the full data of the cells
    // and the index chunk to hold the cell-representations
    Chunk dataChunk = chunkCreator.getChunk(CompactingMemStore.IndexType.CHUNK_MAP);
    Chunk idxChunk  = chunkCreator.getChunk(CompactingMemStore.IndexType.CHUNK_MAP);
    // the array of index chunks to be used as a basis for CellChunkMap
    Chunk chunkArray[] = new Chunk[8];  // according to test currently written 8 is way enough
    int chunkArrayIdx = 0;
    chunkArray[chunkArrayIdx++] = idxChunk;

    ByteBuffer idxBuffer = idxChunk.getData();  // the buffers of the chunks
    ByteBuffer dataBuffer = dataChunk.getData();
    int dataOffset = ChunkCreator.SIZEOF_CHUNK_HEADER;        // offset inside data buffer
    int idxOffset = ChunkCreator.SIZEOF_CHUNK_HEADER;         // skip the space for chunk ID

    Cell[] cellArray = asc ? ascCells : descCells;

    for (Cell kv: cellArray) {
      // do we have enough space to write the cell data on the data chunk?
      if (dataOffset + kv.getSerializedSize() > chunkCreator.getChunkSize()) {
        // allocate more data chunks if needed
        dataChunk = chunkCreator.getChunk(CompactingMemStore.IndexType.CHUNK_MAP);
        dataBuffer = dataChunk.getData();
        dataOffset = ChunkCreator.SIZEOF_CHUNK_HEADER;
      }
      int dataStartOfset = dataOffset;
      dataOffset = KeyValueUtil.appendTo(kv, dataBuffer, dataOffset, false); // write deep cell data

      // do we have enough space to write the cell-representation on the index chunk?
      if (idxOffset + ClassSize.CELL_CHUNK_MAP_ENTRY > chunkCreator.getChunkSize()) {
        // allocate more index chunks if needed
        idxChunk = chunkCreator.getChunk(CompactingMemStore.IndexType.CHUNK_MAP);
        idxBuffer = idxChunk.getData();
        idxOffset = ChunkCreator.SIZEOF_CHUNK_HEADER;
        chunkArray[chunkArrayIdx++] = idxChunk;
      }
      idxOffset = ByteBufferUtils.putInt(idxBuffer, idxOffset, dataChunk.getId()); // write data chunk id
      idxOffset = ByteBufferUtils.putInt(idxBuffer, idxOffset, dataStartOfset);          // offset
      idxOffset = ByteBufferUtils.putInt(idxBuffer, idxOffset, kv.getSerializedSize()); // length
      idxOffset = ByteBufferUtils.putLong(idxBuffer, idxOffset, kv.getSequenceId());     // seqId
    }

    return new CellChunkMap(CellComparator.getInstance(),chunkArray,0,NUM_OF_CELLS,!asc);
  }

  /* Create CellChunkMap with four cells inside the data jumbo chunk. This test is working only
  ** with small chunks sized SMALL_CHUNK_SIZE (64) bytes */
  private CellChunkMap setUpJumboCellChunkMap(boolean asc) {
    int smallChunkSize = SMALL_CHUNK_SIZE+8;
    // allocate new chunks and use the data JUMBO chunk to hold the full data of the cells
    // and the normal index chunk to hold the cell-representations
    Chunk dataJumboChunk =
        chunkCreator.getChunk(CompactingMemStore.IndexType.CHUNK_MAP, ChunkType.JUMBO_CHUNK,
          smallChunkSize);
    assertTrue(dataJumboChunk.isJumbo());
    Chunk idxChunk  = chunkCreator.getChunk(CompactingMemStore.IndexType.CHUNK_MAP);
    // the array of index chunks to be used as a basis for CellChunkMap
    Chunk[] chunkArray = new Chunk[8];  // according to test currently written 8 is way enough
    int chunkArrayIdx = 0;
    chunkArray[chunkArrayIdx++] = idxChunk;

    ByteBuffer idxBuffer = idxChunk.getData();  // the buffers of the chunks
    ByteBuffer dataBuffer = dataJumboChunk.getData();
    int dataOffset = ChunkCreator.SIZEOF_CHUNK_HEADER;          // offset inside data buffer
    int idxOffset = ChunkCreator.SIZEOF_CHUNK_HEADER;           // skip the space for chunk ID

    Cell[] cellArray = asc ? ascCells : descCells;

    for (Cell kv: cellArray) {
      int dataStartOfset = dataOffset;
      dataOffset = KeyValueUtil.appendTo(kv, dataBuffer, dataOffset, false); // write deep cell data

      // do we have enough space to write the cell-representation on the index chunk?
      if (idxOffset + ClassSize.CELL_CHUNK_MAP_ENTRY > chunkCreator.getChunkSize()) {
        // allocate more index chunks if needed
        idxChunk = chunkCreator.getChunk(CompactingMemStore.IndexType.CHUNK_MAP);
        idxBuffer = idxChunk.getData();
        idxOffset = ChunkCreator.SIZEOF_CHUNK_HEADER;
        chunkArray[chunkArrayIdx++] = idxChunk;
      }
      // write data chunk id
      idxOffset = ByteBufferUtils.putInt(idxBuffer, idxOffset, dataJumboChunk.getId());
      idxOffset = ByteBufferUtils.putInt(idxBuffer, idxOffset, dataStartOfset);          // offset
      idxOffset = ByteBufferUtils.putInt(idxBuffer, idxOffset, kv.getSerializedSize()); // length
      idxOffset = ByteBufferUtils.putLong(idxBuffer, idxOffset, kv.getSequenceId());     // seqId

      // Jumbo chunks are working only with one cell per chunk, thus always allocate a new jumbo
      // data chunk for next cell
      dataJumboChunk =
          chunkCreator.getChunk(CompactingMemStore.IndexType.CHUNK_MAP, ChunkType.JUMBO_CHUNK,
            smallChunkSize);
      assertTrue(dataJumboChunk.isJumbo());
      dataBuffer = dataJumboChunk.getData();
      dataOffset = ChunkCreator.SIZEOF_CHUNK_HEADER;
    }

    return new CellChunkMap(CellComparator.getInstance(),chunkArray,0,NUM_OF_CELLS,!asc);
  }
}
