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
package org.apache.hbase;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestCellUtil {
  @Test
  public void testCreateCellScannerCellList() {
    final int count = 3;
    Cell [] cs = getCells(count, Bytes.toBytes(0));
    List<Cell> cells = Arrays.asList(cs);
    CellScanner scanner = CellUtil.createCellScanner(cells);
    int i = 0;
    while (scanner.advance()) {
      i++;
    }
    assertEquals(count, i);
  }

  @Test
  public void testCreateCellScannerFamilyMap() {
    final int count = 3;
    final NavigableMap<byte [], List<? extends Cell>> map =
      new TreeMap<byte [], List<? extends Cell>>(Bytes.BYTES_COMPARATOR);
    for (int i = 0; i < count; i++) {
      byte [] key = Bytes.toBytes(i);
      KeyValue [] cs = getCells(count, key);
      map.put(key, Arrays.asList(cs));
    }
    CellScanner scanner = CellUtil.createCellScanner(map);
    int i = 0;
    while (scanner.advance()) {
      i++;
    }
    assertEquals(count * count, i);
  }

  static KeyValue [] getCells(final int howMany, final byte [] family) {
    KeyValue [] cells = new KeyValue[howMany];
    for (int i = 0; i < howMany; i++) {
      byte [] index = Bytes.toBytes(i);
      KeyValue kv = new KeyValue(index, family, index, index);
      cells[i] = kv;
    }
    return cells;
  }
}