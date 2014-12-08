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
package org.apache.hadoop.hbase.protobuf;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(SmallTests.class)
public class TestReplicationProtobuf {
  /**
   * Little test to check we can basically convert list of a list of KVs into a CellScanner
   * @throws IOException
   */
  @Test
  public void testGetCellScanner() throws IOException {
    List<Cell> a = new ArrayList<Cell>();
    KeyValue akv = new KeyValue(Bytes.toBytes("a"), -1L);
    a.add(akv);
    // Add a few just to make it less regular.
    a.add(new KeyValue(Bytes.toBytes("aa"), -1L));
    a.add(new KeyValue(Bytes.toBytes("aaa"), -1L));
    List<Cell> b = new ArrayList<Cell>();
    KeyValue bkv = new KeyValue(Bytes.toBytes("b"), -1L);
    a.add(bkv);
    List<Cell> c = new ArrayList<Cell>();
    KeyValue ckv = new KeyValue(Bytes.toBytes("c"), -1L);
    c.add(ckv);
    List<List<? extends Cell>> all = new ArrayList<List<? extends Cell>>();
    all.add(a);
    all.add(b);
    all.add(c);
    CellScanner scanner = ReplicationProtbufUtil.getCellScanner(all, 0);
    testAdvancetHasSameRow(scanner, akv);
    // Skip over aa
    scanner.advance();
    // Skip over aaa
    scanner.advance();
    testAdvancetHasSameRow(scanner, bkv);
    testAdvancetHasSameRow(scanner, ckv);
    assertFalse(scanner.advance());
  }

  private void testAdvancetHasSameRow(CellScanner scanner, final KeyValue kv) throws IOException {
    scanner.advance();
    assertTrue(Bytes.equals(scanner.current().getRowArray(), scanner.current().getRowOffset(),
        scanner.current().getRowLength(),
      kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()));
  }
}
