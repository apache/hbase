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
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.junit.Assert;

import com.google.common.collect.Lists;

public class TestRowDataSimple extends BaseTestRowData {

  static byte[]
  // don't let the rows share any common prefix bytes
      rowA = Bytes.toBytes("Arow"),
      rowB = Bytes.toBytes("Brow"), cf = Bytes.toBytes("fam"),
      cq0 = Bytes.toBytes("cq0"),
      cq1 = Bytes.toBytes("cq1tail"),// make sure tail does not come back as liat
      cq2 = Bytes.toBytes("dcq2"),// start with a different character
      v0 = Bytes.toBytes("v0");

  static long ts = 55L;

  static List<KeyValue> d = Lists.newArrayList();
  static {
    d.add(new KeyValue(rowA, cf, cq0, ts, v0));
    d.add(new KeyValue(rowA, cf, cq1, ts, v0));
    d.add(new KeyValue(rowA, cf, cq2, ts, v0));
    d.add(new KeyValue(rowB, cf, cq0, ts, v0));
    d.add(new KeyValue(rowB, cf, cq1, ts, v0));
    d.add(new KeyValue(rowB, cf, cq2, ts, v0));
  }

  @Override
  public List<KeyValue> getInputs() {
    return d;
  }

  @Override
  public void individualSearcherAssertions(CellSearcher searcher) {
    CellScannerPosition p;// reuse
    searcher.resetToBeforeFirstEntry();

    // test first cell
    try {
      searcher.advance();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Cell first = searcher.current();
    Assert.assertTrue(CellComparator.equals(d.get(0), first));

    // test first cell in second row
    Assert.assertTrue(searcher.positionAt(d.get(3)));
    Assert.assertTrue(CellComparator.equals(d.get(3), searcher.current()));

    Cell between4And5 = new KeyValue(rowB, cf, cq1, ts - 2, v0);

    // test exact
    Assert.assertFalse(searcher.positionAt(between4And5));

    // test atOrBefore
    p = searcher.positionAtOrBefore(between4And5);
    Assert.assertEquals(CellScannerPosition.BEFORE, p);
    Assert.assertTrue(CellComparator.equals(searcher.current(), d.get(4)));

    // test atOrAfter
    p = searcher.positionAtOrAfter(between4And5);
    Assert.assertEquals(CellScannerPosition.AFTER, p);
    Assert.assertTrue(CellComparator.equals(searcher.current(), d.get(5)));

    // test when key falls before first key in block
    Cell beforeFirst = new KeyValue(Bytes.toBytes("A"), cf, cq0, ts, v0);
    Assert.assertFalse(searcher.positionAt(beforeFirst));
    p = searcher.positionAtOrBefore(beforeFirst);
    Assert.assertEquals(CellScannerPosition.BEFORE_FIRST, p);
    p = searcher.positionAtOrAfter(beforeFirst);
    Assert.assertEquals(CellScannerPosition.AFTER, p);
    Assert.assertTrue(CellComparator.equals(searcher.current(), d.get(0)));
    Assert.assertEquals(d.get(0), searcher.current());

    // test when key falls after last key in block
    Cell afterLast = new KeyValue(Bytes.toBytes("z"), cf, cq0, ts, v0);// must be lower case z
    Assert.assertFalse(searcher.positionAt(afterLast));
    p = searcher.positionAtOrAfter(afterLast);
    Assert.assertEquals(CellScannerPosition.AFTER_LAST, p);
    p = searcher.positionAtOrBefore(afterLast);
    Assert.assertEquals(CellScannerPosition.BEFORE, p);
    Assert.assertTrue(CellComparator.equals(searcher.current(), CollectionUtils.getLast(d)));
  }

}
