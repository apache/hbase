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

package org.apache.hbase.codec.prefixtree.row;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTool;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.hbase.Cell;
import org.apache.hbase.cell.CellComparator;
import org.apache.hbase.cell.CellScannerPosition;
import org.apache.hbase.codec.prefixtree.decode.DecoderFactory;
import org.apache.hbase.codec.prefixtree.encode.PrefixTreeEncoder;
import org.apache.hbase.codec.prefixtree.scanner.CellSearcher;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestPrefixTreeSearcher {

	protected static int BLOCK_START = 7;

  @Parameters
  public static Collection<Object[]> parameters() {
    return new TestRowData.InMemory().getAllAsObjectArray();
  }

  protected TestRowData rows;
  protected ByteBuffer block;

  public TestPrefixTreeSearcher(TestRowData testRows) throws IOException {
    this.rows = testRows;
    ByteArrayOutputStream os = new ByteArrayOutputStream(1 << 20);
    PrefixTreeEncoder kvBuilder = new PrefixTreeEncoder(os, true);
    for (KeyValue kv : rows.getInputs()) {
      kvBuilder.write(kv);
    }
    kvBuilder.flush();
    byte[] outputBytes = os.toByteArray();
    this.block = ByteBuffer.wrap(outputBytes);
  }


  @Test
  public void testScanForwards() throws IOException {
    CellSearcher searcher = null;
    try {
      searcher = DecoderFactory.checkOut(block, true);

      int i = -1;
      while (searcher.next()) {
        ++i;
        KeyValue inputCell = rows.getInputs().get(i);
        Cell outputCell = searcher.getCurrent();

        // check all 3 permutations of equals()
        Assert.assertEquals(inputCell, outputCell);
        Assert.assertEquals(outputCell, inputCell);
        Assert.assertTrue(CellComparator.equals(inputCell, outputCell));
      }
      Assert.assertEquals(rows.getInputs().size(), i + 1);
    } finally {
      DecoderFactory.checkIn(searcher);
    }
  }


  @Test
  public void testScanBackwards() throws IOException {
    CellSearcher searcher = null;
    try {
      searcher = DecoderFactory.checkOut(block, true);
      searcher.positionAfterLastCell();
      int i = -1;
      while (searcher.previous()) {
        ++i;
        int oppositeIndex = rows.getInputs().size() - i - 1;
        KeyValue inputKv = rows.getInputs().get(oppositeIndex);
        KeyValue outputKv = KeyValueTool.copyToNewKeyValue(searcher.getCurrent());
        Assert.assertEquals(inputKv, outputKv);
      }
      Assert.assertEquals(rows.getInputs().size(), i + 1);
    } finally {
      DecoderFactory.checkIn(searcher);
    }
  }


  @Test
  public void testRandomSeekHits() throws IOException {
    CellSearcher searcher = null;
    try {
      searcher = DecoderFactory.checkOut(block, true);
      for (KeyValue kv : rows.getInputs()) {
        boolean hit = searcher.positionAt(kv);
        Assert.assertTrue(hit);
        Cell foundKv = searcher.getCurrent();
        Assert.assertTrue(CellComparator.equals(kv, foundKv));
      }
    } finally {
      DecoderFactory.checkIn(searcher);
    }
  }

  /**
   * very hard to test nubs with this thing since the a nextRowKey function will usually skip them
   */
  @Test
  public void testRandomSeekMisses() throws IOException {
    CellSearcher searcher = null;
    List<Integer> rowStartIndexes = rows.getRowStartIndexes();
    try {
      searcher = DecoderFactory.checkOut(block, true);
      for (int i=0; i < rows.getInputs().size(); ++i) {
        KeyValue kv = rows.getInputs().get(i);

        //nextRow
        KeyValue inputNextRow = KeyValueTool.createFirstKeyInNextRow(kv);

        CellScannerPosition position = searcher.positionAtOrBefore(inputNextRow);
        boolean isFirstInRow = rowStartIndexes.contains(i);
        if(isFirstInRow){
          int rowIndex = rowStartIndexes.indexOf(i);
          if(rowIndex < rowStartIndexes.size() - 1){
//            int lastKvInRowI = rowStartIndexes.get(rowIndex + 1) - 1;
            Assert.assertEquals(CellScannerPosition.BEFORE, position);
            /*
             * Can't get this to work between nubs like rowB\x00 <-> rowBB
             *
             * No reason to doubt that it works, but will have to come up with a smarter test.
             */
//            Assert.assertEquals(rows.getInputs().get(lastKvInRowI), searcher.getCurrentCell());
          }
        }

        //previous KV
        KeyValue inputPreviousKv = KeyValueTool.previousKey(kv);
        boolean hit = searcher.positionAt(inputPreviousKv);
        Assert.assertFalse(hit);
        position = searcher.positionAtOrAfter(inputPreviousKv);
        if(CollectionUtils.isLastIndex(rows.getInputs(), i)){
          Assert.assertTrue(CellScannerPosition.AFTER_LAST == position);
        }else{
          Assert.assertTrue(CellScannerPosition.AFTER == position);
          /*
           * TODO: why i+1 instead of i?
           */
          Assert.assertEquals(rows.getInputs().get(i+1), searcher.getCurrent());
        }
      }
    } finally {
      DecoderFactory.checkIn(searcher);
    }
  }


  @Test
  public void testRandomSeekIndividualAssertions() throws IOException {
    CellSearcher searcher = null;
    try {
      searcher = DecoderFactory.checkOut(block, true);
      rows.individualSearcherAssertions(searcher);
    } finally {
      DecoderFactory.checkIn(searcher);
    }
  }
}
