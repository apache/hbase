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
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TestCellUtil.ByteBufferCellImpl;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestComparators {

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
    Cell bbCell = new ByteBufferCellImpl(buffer, 0, buffer.remaining());
    ByteArrayComparable comparable = new BinaryComparator(r1);
    assertEquals(0, CellComparator.compareRow(bbCell, comparable));
    assertEquals(0, CellComparator.compareRow(kv, comparable));
    kv = new KeyValue(r0, f, q1, v1);
    buffer = ByteBuffer.wrap(kv.getBuffer());
    bbCell = new ByteBufferCellImpl(buffer, 0, buffer.remaining());
    assertTrue(CellComparator.compareRow(bbCell, comparable) > 0);
    assertTrue(CellComparator.compareRow(kv, comparable) > 0);
    kv = new KeyValue(r2, f, q1, v1);
    buffer = ByteBuffer.wrap(kv.getBuffer());
    bbCell = new ByteBufferCellImpl(buffer, 0, buffer.remaining());
    assertTrue(CellComparator.compareRow(bbCell, comparable) < 0);
    assertTrue(CellComparator.compareRow(kv, comparable) < 0);
    // Qualifier compare
    comparable = new BinaryPrefixComparator(Bytes.toBytes("qual"));
    assertEquals(0, CellComparator.compareQualifier(bbCell, comparable));
    assertEquals(0, CellComparator.compareQualifier(kv, comparable));
    kv = new KeyValue(r2, f, q2, v1);
    buffer = ByteBuffer.wrap(kv.getBuffer());
    bbCell = new ByteBufferCellImpl(buffer, 0, buffer.remaining());
    assertEquals(0, CellComparator.compareQualifier(bbCell, comparable));
    assertEquals(0, CellComparator.compareQualifier(kv, comparable));
    kv = new KeyValue(r2, f, q3, v1);
    buffer = ByteBuffer.wrap(kv.getBuffer());
    bbCell = new ByteBufferCellImpl(buffer, 0, buffer.remaining());
    assertTrue(CellComparator.compareQualifier(bbCell, comparable) < 0);
    assertTrue(CellComparator.compareQualifier(kv, comparable) < 0);
    // Value compare
    comparable = new LongComparator(l1);
    assertEquals(0, CellComparator.compareValue(bbCell, comparable));
    assertEquals(0, CellComparator.compareValue(kv, comparable));
    kv = new KeyValue(r1, f, q1, v2);
    buffer = ByteBuffer.wrap(kv.getBuffer());
    bbCell = new ByteBufferCellImpl(buffer, 0, buffer.remaining());
    assertTrue(CellComparator.compareValue(bbCell, comparable) < 0);
    assertTrue(CellComparator.compareValue(kv, comparable) < 0);
    // Family compare
    comparable = new SubstringComparator("cf");
    assertEquals(0, CellComparator.compareFamily(bbCell, comparable));
    assertEquals(0, CellComparator.compareFamily(kv, comparable));
  }
}
