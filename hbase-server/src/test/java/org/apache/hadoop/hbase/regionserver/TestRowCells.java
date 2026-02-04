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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestRowCells {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRowCells.class);

  @Test
  public void testDeepClone() throws CloneNotSupportedException {
    List<Cell> cells = new ArrayList<>();
    KeyValue kv1 = KeyValueTestUtil.create("row", "CF", "q1", 1, "v1");
    cells.add(kv1);
    KeyValue kv2 = KeyValueTestUtil.create("row", "CF", "q12", 2, "v22");
    cells.add(kv2);
    RowCells rowCells = new RowCells(cells);

    // Ensure deep clone happened
    assertNotSame(kv1, rowCells.getCells().get(0));
    assertEquals(kv1, rowCells.getCells().get(0));
    assertNotSame(kv2, rowCells.getCells().get(1));
    assertEquals(kv2, rowCells.getCells().get(1));
  }

  @Test
  public void testHeapSize() throws CloneNotSupportedException {
    List<Cell> cells;
    RowCells rowCells;

    cells = new ArrayList<>();
    rowCells = new RowCells(cells);
    assertEquals(RowCells.FIXED_OVERHEAD, rowCells.heapSize());

    cells = new ArrayList<>();
    KeyValue kv1 = KeyValueTestUtil.create("row", "CF", "q1", 1, "v1");
    cells.add(kv1);
    KeyValue kv2 = KeyValueTestUtil.create("row", "CF", "q22", 2, "v22");
    cells.add(kv2);
    rowCells = new RowCells(cells);
    assertEquals(RowCells.FIXED_OVERHEAD + kv1.heapSize() + kv2.heapSize(), rowCells.heapSize());
  }
}
