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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;
@Category({MiscTests.class, SmallTests.class})
public class TestCellComparator {

  byte[] row1 = Bytes.toBytes("row1");
  byte[] row2 = Bytes.toBytes("row2");
  byte[] row_1_0 = Bytes.toBytes("row10");

  byte[] fam1 = Bytes.toBytes("fam1");
  byte[] fam2 = Bytes.toBytes("fam2");
  byte[] fam_1_2 = Bytes.toBytes("fam12");

  byte[] qual1 = Bytes.toBytes("qual1");
  byte[] qual2 = Bytes.toBytes("qual2");

  byte[] val = Bytes.toBytes("val");

  @Test
  public void testCompareCells() {
    KeyValue kv1 = new KeyValue(row1, fam1, qual1, val);
    KeyValue kv2 = new KeyValue(row2, fam1, qual1, val);
    assertTrue((CellComparator.compare(kv1, kv2, false)) < 0);

    kv1 = new KeyValue(row1, fam2, qual1, val);
    kv2 = new KeyValue(row1, fam1, qual1, val);
    assertTrue((CellComparator.compareFamilies(kv1, kv2) > 0));

    kv1 = new KeyValue(row1, fam1, qual1, 1l, val);
    kv2 = new KeyValue(row1, fam1, qual1, 2l, val);
    assertTrue((CellComparator.compare(kv1, kv2, false) > 0));

    kv1 = new KeyValue(row1, fam1, qual1, 1l, Type.Put);
    kv2 = new KeyValue(row1, fam1, qual1, 1l, Type.Maximum);
    assertTrue((CellComparator.compare(kv1, kv2, false) > 0));

    kv1 = new KeyValue(row1, fam1, qual1, 1l, Type.Put);
    kv2 = new KeyValue(row1, fam_1_2, qual1, 1l, Type.Maximum);
    assertTrue((CellComparator.compareCommonFamilyPrefix(kv1, kv2, 4) < 0));

    kv1 = new KeyValue(row1, fam1, qual1, 1l, Type.Put);
    kv2 = new KeyValue(row_1_0, fam_1_2, qual1, 1l, Type.Maximum);
    assertTrue((CellComparator.compareCommonRowPrefix(kv1, kv2, 4) < 0));

    kv1 = new KeyValue(row1, fam1, qual2, 1l, Type.Put);
    kv2 = new KeyValue(row1, fam1, qual1, 1l, Type.Maximum);
    assertTrue((CellComparator.compareCommonQualifierPrefix(kv1, kv2, 4) > 0));

    kv1 = new KeyValue(row1, fam1, qual1, 1l, Type.Put);
    kv2 = new KeyValue(row1, fam1, qual1, 1l, Type.Put);
    assertTrue((CellComparator.equals(kv1, kv2)));
  }

  @Test
  public void testGetShortMidpoint() {
    KeyValue.KVComparator comparator = new KeyValue.KVComparator();

    Cell left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    Cell right = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    Cell mid = CellComparator.getMidpoint(comparator, left, right);
    assertTrue(CellComparator.compare(left, mid, true) <= 0);
    assertTrue(CellComparator.compare(mid, right, true) <= 0);

    left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("b"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    mid = CellComparator.getMidpoint(comparator, left, right);
    assertTrue(CellComparator.compare(left, mid, true) < 0);
    assertTrue(CellComparator.compare(mid, right, true) <= 0);

    left = CellUtil.createCell(Bytes.toBytes("g"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("i"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    mid = CellComparator.getMidpoint(comparator, left, right);
    assertTrue(CellComparator.compare(left, mid, true) < 0);
    assertTrue(CellComparator.compare(mid, right, true) <= 0);

    left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("bbbbbbb"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    mid = CellComparator.getMidpoint(comparator, left, right);
    assertTrue(CellComparator.compare(left, mid, true) < 0);
    assertTrue(CellComparator.compare(mid, right, true) < 0);
    assertEquals(1, (int)mid.getRowLength());

    left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("a"));
    mid = CellComparator.getMidpoint(comparator, left, right);
    assertTrue(CellComparator.compare(left, mid, true) < 0);
    assertTrue(CellComparator.compare(mid, right, true) <= 0);

    left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("aaaaaaaa"), Bytes.toBytes("b"));
    mid = CellComparator.getMidpoint(comparator, left, right);
    assertTrue(CellComparator.compare(left, mid, true) < 0);
    assertTrue(CellComparator.compare(mid, right, true) < 0);
    assertEquals(2, (int)mid.getFamilyLength());

    left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("aaaaaaaaa"));
    mid = CellComparator.getMidpoint(comparator, left, right);
    assertTrue(CellComparator.compare(left, mid, true) < 0);
    assertTrue(CellComparator.compare(mid, right, true) < 0);
    assertEquals(2, (int)mid.getQualifierLength());

    left = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("a"), Bytes.toBytes("a"), Bytes.toBytes("b"));
    mid = CellComparator.getMidpoint(comparator, left, right);
    assertTrue(CellComparator.compare(left, mid, true) < 0);
    assertTrue(CellComparator.compare(mid, right, true) <= 0);
    assertEquals(1, (int)mid.getQualifierLength());

    // Assert that if meta comparator, it returns the right cell -- i.e.  no optimization done.
    left = CellUtil.createCell(Bytes.toBytes("g"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    right = CellUtil.createCell(Bytes.toBytes("i"), Bytes.toBytes("a"), Bytes.toBytes("a"));
    mid = CellComparator.getMidpoint(new KeyValue.MetaComparator(), left, right);
    assertTrue(CellComparator.compare(left, mid, true) < 0);
    assertTrue(CellComparator.compare(mid, right, true) == 0);
  }
}