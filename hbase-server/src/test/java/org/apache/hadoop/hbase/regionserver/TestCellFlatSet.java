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

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.SortedSet;
import static org.junit.Assert.assertTrue;

@Category({RegionServerTests.class, SmallTests.class})
public class TestCellFlatSet extends TestCase {

  private static final int NUM_OF_CELLS = 4;

  private Cell cells[];
  private CellArrayMap cbOnHeap;

  private final static Configuration conf = new Configuration();
  private HeapMemStoreLAB mslab;


  protected void setUp() throws Exception {
    super.setUp();

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

    cells = new Cell[] {kv1,kv2,kv3,kv4};
    cbOnHeap = new CellArrayMap(CellComparator.COMPARATOR,cells,0,NUM_OF_CELLS,false);

    conf.setBoolean(SegmentFactory.USEMSLAB_KEY, true);
    conf.setFloat(MemStoreChunkPool.CHUNK_POOL_MAXSIZE_KEY, 0.2f);
    MemStoreChunkPool.chunkPoolDisabled = false;
    mslab = new HeapMemStoreLAB(conf);
  }

  /* Create and test CellSet based on CellArrayMap */
  public void testCellBlocksOnHeap() throws Exception {
    CellSet cs = new CellSet(cbOnHeap);
    testCellBlocks(cs);
    testIterators(cs);
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

    assertTrue(cs.contains(cells[0]));              // check existence of the first
    Cell first = cs.first();
    assertTrue(cells[0].equals(first));

    assertTrue(cs.contains(cells[NUM_OF_CELLS - 1]));  // check last
    Cell last = cs.last();
    assertTrue(cells[NUM_OF_CELLS - 1].equals(last));

    SortedSet<Cell> tail = cs.tailSet(cells[1]);    // check tail abd head sizes
    assertEquals(NUM_OF_CELLS - 1, tail.size());
    SortedSet<Cell> head = cs.headSet(cells[1]);
    assertEquals(1, head.size());

    SortedSet<Cell> tailOuter = cs.tailSet(outerCell);  // check tail starting from outer cell
    assertEquals(NUM_OF_CELLS - 1, tailOuter.size());

    Cell tailFirst = tail.first();
    assertTrue(cells[1].equals(tailFirst));
    Cell tailLast = tail.last();
    assertTrue(cells[NUM_OF_CELLS - 1].equals(tailLast));

    Cell headFirst = head.first();
    assertTrue(cells[0].equals(headFirst));
    Cell headLast = head.last();
    assertTrue(cells[0].equals(headLast));
  }

  /* Generic iterators test for immutable CellSet */
  private void testIterators(CellSet cs) throws Exception {

    // Assert that we have NUM_OF_CELLS values and that they are in order
    int count = 0;
    for (Cell kv: cs) {
      assertEquals("\n\n-------------------------------------------------------------------\n"
              + "Comparing iteration number " + (count + 1) + " the returned cell: " + kv
              + ", the first Cell in the CellBlocksMap: " + cells[count]
              + ", and the same transformed to String: " + cells[count].toString()
              + "\n-------------------------------------------------------------------\n",
              cells[count], kv);
      count++;
    }
    assertEquals(NUM_OF_CELLS, count);

    // Test descending iterator
    count = 0;
    for (Iterator<Cell> i = cs.descendingIterator(); i.hasNext();) {
      Cell kv = i.next();
      assertEquals(cells[NUM_OF_CELLS - (count + 1)], kv);
      count++;
    }
    assertEquals(NUM_OF_CELLS, count);
  }
}
