/*
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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.ConcurrentModificationException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that I can Iterate Client Actions that hold Cells (Get does not have Cells).
 */
@Category({SmallTests.class, ClientTests.class})
public class TestPutDeleteEtcCellIteration {
  private static final byte [] ROW = new byte [] {'r'};
  private static final long TIMESTAMP = System.currentTimeMillis();
  private static final int COUNT = 10;

  @Test
  public void testPutIteration() throws IOException {
    Put p = new Put(ROW);
    for (int i = 0; i < COUNT; i++) {
      byte [] bytes = Bytes.toBytes(i);
      p.addColumn(bytes, bytes, TIMESTAMP, bytes);
    }
    int index = 0;
    for (CellScanner cellScanner = p.cellScanner(); cellScanner.advance();) {
      Cell cell = cellScanner.current();
      byte [] bytes = Bytes.toBytes(index++);
      cell.equals(new KeyValue(ROW, bytes, bytes, TIMESTAMP, bytes));
    }
    assertEquals(COUNT, index);
  }

  @Test (expected = ConcurrentModificationException.class)
  public void testPutConcurrentModificationOnIteration() throws IOException {
    Put p = new Put(ROW);
    for (int i = 0; i < COUNT; i++) {
      byte [] bytes = Bytes.toBytes(i);
      p.addColumn(bytes, bytes, TIMESTAMP, bytes);
    }
    int index = 0;
    int trigger = 3;
    for (CellScanner cellScanner = p.cellScanner(); cellScanner.advance();) {
      Cell cell = cellScanner.current();
      byte [] bytes = Bytes.toBytes(index++);
      // When we hit the trigger, try inserting a new KV; should trigger exception
      if (trigger == 3) p.addColumn(bytes, bytes, TIMESTAMP, bytes);
      cell.equals(new KeyValue(ROW, bytes, bytes, TIMESTAMP, bytes));
    }
    assertEquals(COUNT, index);
  }

  @Test
  public void testDeleteIteration() throws IOException {
    Delete d = new Delete(ROW);
    for (int i = 0; i < COUNT; i++) {
      byte [] bytes = Bytes.toBytes(i);
      d.addColumn(bytes, bytes, TIMESTAMP);
    }
    int index = 0;
    for (CellScanner cellScanner = d.cellScanner(); cellScanner.advance();) {
      Cell cell = cellScanner.current();
      byte [] bytes = Bytes.toBytes(index++);
      cell.equals(new KeyValue(ROW, bytes, bytes, TIMESTAMP, KeyValue.Type.DeleteColumn));
    }
    assertEquals(COUNT, index);
  }

  @Test
  public void testAppendIteration() throws IOException {
    Append a = new Append(ROW);
    for (int i = 0; i < COUNT; i++) {
      byte [] bytes = Bytes.toBytes(i);
      a.add(bytes, bytes, bytes);
    }
    int index = 0;
    for (CellScanner cellScanner = a.cellScanner(); cellScanner.advance();) {
      Cell cell = cellScanner.current();
      byte [] bytes = Bytes.toBytes(index++);
      KeyValue kv = (KeyValue)cell;
      assertTrue(Bytes.equals(CellUtil.cloneFamily(kv), bytes));
      assertTrue(Bytes.equals(CellUtil.cloneValue(kv), bytes));
    }
    assertEquals(COUNT, index);
  }

  @Test
  public void testIncrementIteration() throws IOException {
    Increment increment = new Increment(ROW);
    for (int i = 0; i < COUNT; i++) {
      byte [] bytes = Bytes.toBytes(i);
      increment.addColumn(bytes, bytes, i);
    }
    int index = 0;
    for (CellScanner cellScanner = increment.cellScanner(); cellScanner.advance();) {
      Cell cell = cellScanner.current();
      int value = index;
      byte [] bytes = Bytes.toBytes(index++);
      KeyValue kv = (KeyValue)cell;
      assertTrue(Bytes.equals(CellUtil.cloneFamily(kv), bytes));
      long a = Bytes.toLong(CellUtil.cloneValue(kv));
      assertEquals(value, a);
    }
    assertEquals(COUNT, index);
  }

  @Test
  public void testResultIteration() throws IOException {
    Cell [] cells = new Cell[COUNT];
    for(int i = 0; i < COUNT; i++) {
      byte [] bytes = Bytes.toBytes(i);
      cells[i] = new KeyValue(ROW, bytes, bytes, TIMESTAMP, bytes);
    }
    Result r = Result.create(Arrays.asList(cells));
    int index = 0;
    for (CellScanner cellScanner = r.cellScanner(); cellScanner.advance();) {
      Cell cell = cellScanner.current();
      byte [] bytes = Bytes.toBytes(index++);
      cell.equals(new KeyValue(ROW, bytes, bytes, TIMESTAMP, bytes));
    }
    assertEquals(COUNT, index);
  }
}