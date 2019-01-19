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

import java.util.Iterator;
import java.util.SortedSet;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestCellSkipListSet extends TestCase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCellSkipListSet.class);

  private final CellSet csls =
    new CellSet(CellComparatorImpl.COMPARATOR);

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    this.csls.clear();
  }

  public void testAdd() throws Exception {
    byte[] bytes = Bytes.toBytes(getName());
    KeyValue kv = new KeyValue(bytes, bytes, bytes, bytes);
    this.csls.add(kv);
    assertTrue(this.csls.contains(kv));
    assertEquals(1, this.csls.getDelegatee().size());
    Cell first = this.csls.first();
    assertTrue(kv.equals(first));
    assertTrue(Bytes.equals(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength(),
      first.getValueArray(), first.getValueOffset(), first.getValueLength()));
    // Now try overwritting
    byte[] overwriteValue = Bytes.toBytes("overwrite");
    KeyValue overwrite = new KeyValue(bytes, bytes, bytes, overwriteValue);
    this.csls.add(overwrite);
    assertEquals(1, this.csls.getDelegatee().size());
    first = this.csls.first();
    assertTrue(Bytes.equals(overwrite.getValueArray(), overwrite.getValueOffset(),
      overwrite.getValueLength(), first.getValueArray(), first.getValueOffset(),
      first.getValueLength()));
    assertFalse(Bytes.equals(CellUtil.cloneValue(overwrite), CellUtil.cloneValue(kv)));
  }

  public void testIterator() throws Exception {
    byte [] bytes = Bytes.toBytes(getName());
    byte [] value1 = Bytes.toBytes("1");
    byte [] value2 = Bytes.toBytes("2");
    final int total = 3;
    for (int i = 0; i < total; i++) {
      this.csls.add(new KeyValue(bytes, bytes, Bytes.toBytes("" + i), value1));
    }
    // Assert that we added 'total' values and that they are in order
    int count = 0;
    for (Cell kv: this.csls) {
      assertEquals("" + count,
        Bytes.toString(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength()));
      assertTrue(Bytes.equals(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength(), value1,
        0, value1.length));
      count++;
    }
    assertEquals(total, count);
    // Now overwrite with a new value.
    for (int i = 0; i < total; i++) {
      this.csls.add(new KeyValue(bytes, bytes, Bytes.toBytes("" + i), value2));
    }
    // Assert that we added 'total' values and that they are in order and that
    // we are getting back value2
    count = 0;
    for (Cell kv : this.csls) {
      assertEquals("" + count,
        Bytes.toString(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength()));
      assertTrue(Bytes.equals(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength(), value2,
        0, value2.length));
      count++;
    }
    assertEquals(total, count);
  }

  public void testDescendingIterator() throws Exception {
    byte [] bytes = Bytes.toBytes(getName());
    byte [] value1 = Bytes.toBytes("1");
    byte [] value2 = Bytes.toBytes("2");
    final int total = 3;
    for (int i = 0; i < total; i++) {
      this.csls.add(new KeyValue(bytes, bytes, Bytes.toBytes("" + i), value1));
    }
    // Assert that we added 'total' values and that they are in order
    int count = 0;
    for (Iterator<Cell> i = this.csls.descendingIterator(); i.hasNext();) {
      Cell kv = i.next();
      assertEquals("" + (total - (count + 1)),
        Bytes.toString(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength()));
      assertTrue(Bytes.equals(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength(), value1,
        0, value1.length));
      count++;
    }
    assertEquals(total, count);
    // Now overwrite with a new value.
    for (int i = 0; i < total; i++) {
      this.csls.add(new KeyValue(bytes, bytes, Bytes.toBytes("" + i), value2));
    }
    // Assert that we added 'total' values and that they are in order and that
    // we are getting back value2
    count = 0;
    for (Iterator<Cell> i = this.csls.descendingIterator(); i.hasNext();) {
      Cell kv = i.next();
      assertEquals("" + (total - (count + 1)),
        Bytes.toString(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength()));
      assertTrue(Bytes.equals(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength(), value2,
        0, value2.length));
      count++;
    }
    assertEquals(total, count);
  }

  public void testHeadTail() throws Exception {
    byte [] bytes = Bytes.toBytes(getName());
    byte [] value1 = Bytes.toBytes("1");
    byte [] value2 = Bytes.toBytes("2");
    final int total = 3;
    KeyValue splitter = null;
    for (int i = 0; i < total; i++) {
      KeyValue kv = new KeyValue(bytes, bytes, Bytes.toBytes("" + i), value1);
      if (i == 1) splitter = kv;
      this.csls.add(kv);
    }
    SortedSet<Cell> tail = this.csls.tailSet(splitter);
    assertEquals(2, tail.size());
    SortedSet<Cell> head = this.csls.headSet(splitter);
    assertEquals(1, head.size());
    // Now ensure that we get back right answer even when we do tail or head.
    // Now overwrite with a new value.
    for (int i = 0; i < total; i++) {
      this.csls.add(new KeyValue(bytes, bytes, Bytes.toBytes("" + i), value2));
    }
    tail = this.csls.tailSet(splitter);
    assertTrue(Bytes.equals(tail.first().getValueArray(), tail.first().getValueOffset(),
      tail.first().getValueLength(), value2, 0, value2.length));
    head = this.csls.headSet(splitter);
    assertTrue(Bytes.equals(head.first().getValueArray(), head.first().getValueOffset(),
      head.first().getValueLength(), value2, 0, value2.length));
  }
}
