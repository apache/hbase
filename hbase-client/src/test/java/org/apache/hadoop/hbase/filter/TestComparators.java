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
package org.apache.hadoop.hbase.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.ByteBufferKeyValue;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestComparators {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestComparators.class);

  @Test
  public void testCellFieldsCompare() throws Exception {
    byte[] r0 = Bytes.toBytes("row0");
    byte[] r1 = Bytes.toBytes("row1");
    byte[] r2 = Bytes.toBytes("row2");
    byte[] f = Bytes.toBytes("cf1");
    byte[] q1 = Bytes.toBytes("qual1");
    byte[] q2 = Bytes.toBytes("qual2");
    byte[] q3 = Bytes.toBytes("r");
    long l1 = 1234L;
    byte[] v1 = Bytes.toBytes(l1);
    long l2 = 2000L;
    byte[] v2 = Bytes.toBytes(l2);
    // Row compare
    KeyValue kv = new KeyValue(r1, f, q1, v1);
    ByteBuffer buffer = ByteBuffer.wrap(kv.getBuffer());
    Cell bbCell = new ByteBufferKeyValue(buffer, 0, buffer.remaining());
    ByteArrayComparable comparable = new BinaryComparator(r1);
    assertEquals(0, PrivateCellUtil.compareRow(bbCell, comparable));
    assertEquals(0, PrivateCellUtil.compareRow(kv, comparable));
    kv = new KeyValue(r0, f, q1, v1);
    buffer = ByteBuffer.wrap(kv.getBuffer());
    bbCell = new ByteBufferKeyValue(buffer, 0, buffer.remaining());
    assertTrue(PrivateCellUtil.compareRow(bbCell, comparable) > 0);
    assertTrue(PrivateCellUtil.compareRow(kv, comparable) > 0);
    kv = new KeyValue(r2, f, q1, v1);
    buffer = ByteBuffer.wrap(kv.getBuffer());
    bbCell = new ByteBufferKeyValue(buffer, 0, buffer.remaining());
    assertTrue(PrivateCellUtil.compareRow(bbCell, comparable) < 0);
    assertTrue(PrivateCellUtil.compareRow(kv, comparable) < 0);
    // Qualifier compare
    comparable = new BinaryPrefixComparator(Bytes.toBytes("qual"));
    assertEquals(0, PrivateCellUtil.compareQualifier(bbCell, comparable));
    assertEquals(0, PrivateCellUtil.compareQualifier(kv, comparable));
    kv = new KeyValue(r2, f, q2, v1);
    buffer = ByteBuffer.wrap(kv.getBuffer());
    bbCell = new ByteBufferKeyValue(buffer, 0, buffer.remaining());
    assertEquals(0, PrivateCellUtil.compareQualifier(bbCell, comparable));
    assertEquals(0, PrivateCellUtil.compareQualifier(kv, comparable));
    kv = new KeyValue(r2, f, q3, v1);
    buffer = ByteBuffer.wrap(kv.getBuffer());
    bbCell = new ByteBufferKeyValue(buffer, 0, buffer.remaining());
    assertTrue(PrivateCellUtil.compareQualifier(bbCell, comparable) < 0);
    assertTrue(PrivateCellUtil.compareQualifier(kv, comparable) < 0);
    // Value compare
    comparable = new LongComparator(l1);
    assertEquals(0, PrivateCellUtil.compareValue(bbCell, comparable));
    assertEquals(0, PrivateCellUtil.compareValue(kv, comparable));
    kv = new KeyValue(r1, f, q1, v2);
    buffer = ByteBuffer.wrap(kv.getBuffer());
    bbCell = new ByteBufferKeyValue(buffer, 0, buffer.remaining());
    assertTrue(PrivateCellUtil.compareValue(bbCell, comparable) < 0);
    assertTrue(PrivateCellUtil.compareValue(kv, comparable) < 0);
    // Family compare
    comparable = new SubstringComparator("cf");
    assertEquals(0, PrivateCellUtil.compareFamily(bbCell, comparable));
    assertEquals(0, PrivateCellUtil.compareFamily(kv, comparable));
    // Qualifier starts with
    kv = new KeyValue(r1, f, q1, v2);
    assertTrue(PrivateCellUtil.qualifierStartsWith(kv, Bytes.toBytes("q")));
    assertTrue(PrivateCellUtil.qualifierStartsWith(kv, q1));
    assertFalse(PrivateCellUtil.qualifierStartsWith(kv, q2));
    assertFalse(PrivateCellUtil.qualifierStartsWith(kv, Bytes.toBytes("longerthanthequalifier")));

    //Binary component comparisons
    byte[] val = Bytes.toBytes("abcd");
    kv = new KeyValue(r0, f, q1, val);
    buffer = ByteBuffer.wrap(kv.getBuffer());
    bbCell = new ByteBufferKeyValue(buffer, 0, buffer.remaining());

    //equality check
    //row comparison
    //row is "row0"(set by variable r0)
    //and we are checking for equality to 'o' at position 1
    //'r' is at position 0.
    byte[] component = Bytes.toBytes("o");
    comparable = new BinaryComponentComparator(component, 1);
    assertEquals(0, PrivateCellUtil.compareRow(bbCell, comparable));
    assertEquals(0, PrivateCellUtil.compareRow(kv, comparable));
    //value comparison
    //value is "abcd"(set by variable val).
    //and we are checking for equality to 'c' at position 2.
    //'a' is at position 0.
    component = Bytes.toBytes("c");
    comparable = new BinaryComponentComparator(component, 2);
    assertEquals(0,PrivateCellUtil.compareValue(bbCell, comparable));
    assertEquals(0,PrivateCellUtil.compareValue(kv, comparable));

    //greater than
    component = Bytes.toBytes("z");
    //checking for greater than at position 1.
    //for both row("row0") and value("abcd")
    //'z' > 'r'
    comparable = new BinaryComponentComparator(component, 1);
    //row comparison
    assertTrue(PrivateCellUtil.compareRow(bbCell, comparable) > 0);
    assertTrue(PrivateCellUtil.compareRow(kv, comparable) > 0);
    //value comparison
    //'z' > 'a'
    assertTrue(PrivateCellUtil.compareValue(bbCell, comparable) > 0);
    assertTrue(PrivateCellUtil.compareValue(kv, comparable) > 0);

    //less than
    component = Bytes.toBytes("a");
    //checking for less than at position 1 for row ("row0")
    comparable = new BinaryComponentComparator(component, 1);
    //row comparison
    //'a' < 'r'
    assertTrue(PrivateCellUtil.compareRow(bbCell, comparable) < 0);
    assertTrue(PrivateCellUtil.compareRow(kv, comparable) < 0);
    //value comparison
    //checking for less than at position 2 for value("abcd")
    //'a' < 'c'
    comparable = new BinaryComponentComparator(component, 2);
    assertTrue(PrivateCellUtil.compareValue(bbCell, comparable) < 0);
    assertTrue(PrivateCellUtil.compareValue(kv, comparable) < 0);
  }

}
