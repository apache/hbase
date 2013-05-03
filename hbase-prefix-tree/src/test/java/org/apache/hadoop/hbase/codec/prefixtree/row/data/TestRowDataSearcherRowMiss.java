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

package org.apache.hadoop.hbase.codec.prefixtree.row.data;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.codec.prefixtree.row.BaseTestRowData;
import org.apache.hadoop.hbase.codec.prefixtree.scanner.CellScannerPosition;
import org.apache.hadoop.hbase.codec.prefixtree.scanner.CellSearcher;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

import com.google.common.collect.Lists;

public class TestRowDataSearcherRowMiss extends BaseTestRowData{

  static byte[]
      //don't let the rows share any common prefix bytes
      A = Bytes.toBytes("A"),
      AA = Bytes.toBytes("AA"),
      AAA = Bytes.toBytes("AAA"),
      B = Bytes.toBytes("B"),
      cf = Bytes.toBytes("fam"),
      cq = Bytes.toBytes("cq0"),
      v = Bytes.toBytes("v0");

  static long
    ts = 55L;

  static List<KeyValue> d = Lists.newArrayList();
  static{
    d.add(new KeyValue(A, cf, cq, ts, v));
    d.add(new KeyValue(AA, cf, cq, ts, v));
    d.add(new KeyValue(AAA, cf, cq, ts, v));
    d.add(new KeyValue(B, cf, cq, ts, v));
  }

	@Override
	public List<KeyValue> getInputs() {
		return d;
	}

	@Override
	public void individualSearcherAssertions(CellSearcher searcher) {
    assertRowOffsetsCorrect();

    searcher.resetToBeforeFirstEntry();

    //test first cell
    try {
      searcher.advance();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Cell first = searcher.current();
    Assert.assertTrue(CellComparator.equals(d.get(0), first));

    //test first cell in second row
    Assert.assertTrue(searcher.positionAt(d.get(1)));
    Assert.assertTrue(CellComparator.equals(d.get(1), searcher.current()));

    testBetween1and2(searcher);
    testBetween2and3(searcher);
  }

	/************ private methods, call from above *******************/

	private void assertRowOffsetsCorrect(){
	  Assert.assertEquals(4, getRowStartIndexes().size());
	}

	private void testBetween1and2(CellSearcher searcher){
    CellScannerPosition p;//reuse
    Cell betweenAAndAAA = new KeyValue(AA, cf, cq, ts-2, v);

    //test exact
    Assert.assertFalse(searcher.positionAt(betweenAAndAAA));

    //test atOrBefore
    p = searcher.positionAtOrBefore(betweenAAndAAA);
    Assert.assertEquals(CellScannerPosition.BEFORE, p);
    Assert.assertTrue(CellComparator.equals(searcher.current(), d.get(1)));

    //test atOrAfter
    p = searcher.positionAtOrAfter(betweenAAndAAA);
    Assert.assertEquals(CellScannerPosition.AFTER, p);
    Assert.assertTrue(CellComparator.equals(searcher.current(), d.get(2)));
	}

  private void testBetween2and3(CellSearcher searcher){
    CellScannerPosition p;//reuse
    Cell betweenAAAndB = new KeyValue(AAA, cf, cq, ts-2, v);

    //test exact
    Assert.assertFalse(searcher.positionAt(betweenAAAndB));

    //test atOrBefore
    p = searcher.positionAtOrBefore(betweenAAAndB);
    Assert.assertEquals(CellScannerPosition.BEFORE, p);
    Assert.assertTrue(CellComparator.equals(searcher.current(), d.get(2)));

    //test atOrAfter
    p = searcher.positionAtOrAfter(betweenAAAndB);
    Assert.assertEquals(CellScannerPosition.AFTER, p);
    Assert.assertTrue(CellComparator.equals(searcher.current(), d.get(3)));
  }

}
