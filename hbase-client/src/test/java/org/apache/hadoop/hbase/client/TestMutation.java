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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class, ClientTests.class })
public class TestMutation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMutation.class);

  @Test
  public void testAppendCopyConstructor() throws IOException {
    Append origin = new Append(Bytes.toBytes("ROW-01"));
    origin.setPriority(100);
    byte[] family = Bytes.toBytes("CF-01");

    origin.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
      .setRow(origin.getRow())
      .setFamily(family)
      .setQualifier(Bytes.toBytes("q"))
      .setType(Type.Put)
      .setValue(Bytes.toBytes(100))
      .build());
    origin.addColumn(family, Bytes.toBytes("q0"), Bytes.toBytes("value"));
    origin.setTimeRange(100, 1000);
    Append clone = new Append(origin);
    assertEquals(origin, clone);
    origin.addColumn(family, Bytes.toBytes("q1"), Bytes.toBytes("value"));

    //They should have different cell lists
    assertNotEquals(origin.getCellList(family), clone.getCellList(family));
  }

  @Test
  public void testIncrementCopyConstructor() throws IOException {
    Increment origin = new Increment(Bytes.toBytes("ROW-01"));
    origin.setPriority(100);
    byte[] family = Bytes.toBytes("CF-01");

    origin.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
      .setRow(origin.getRow())
      .setFamily(family)
      .setQualifier(Bytes.toBytes("q"))
      .setType(Cell.Type.Put)
      .setValue(Bytes.toBytes(100))
      .build());
    origin.addColumn(family, Bytes.toBytes("q0"), 4);
    origin.setTimeRange(100, 1000);
    Increment clone = new Increment(origin);
    assertEquals(origin, clone);
    origin.addColumn(family, Bytes.toBytes("q1"), 3);

    //They should have different cell lists
    assertNotEquals(origin.getCellList(family), clone.getCellList(family));
  }

  @Test
  public void testDeleteCopyConstructor() throws IOException {
    Delete origin = new Delete(Bytes.toBytes("ROW-01"));
    origin.setPriority(100);
    byte[] family = Bytes.toBytes("CF-01");

    origin.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
      .setRow(origin.getRow())
      .setFamily(family)
      .setQualifier(Bytes.toBytes("q"))
      .setType(Type.Delete)
      .build());
    origin.addColumn(family, Bytes.toBytes("q0"));
    origin.addColumns(family, Bytes.toBytes("q1"));
    origin.addFamily(family);
    origin.addColumns(family, Bytes.toBytes("q2"), 100);
    origin.addFamilyVersion(family, 1000);
    Delete clone = new Delete(origin);
    assertEquals(origin, clone);
    origin.addColumn(family, Bytes.toBytes("q3"));

    //They should have different cell lists
    assertNotEquals(origin.getCellList(family), clone.getCellList(family));
  }

  @Test
  public void testPutCopyConstructor() throws IOException {
    Put origin = new Put(Bytes.toBytes("ROW-01"));
    origin.setPriority(100);
    byte[] family = Bytes.toBytes("CF-01");

    origin.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
      .setRow(origin.getRow())
      .setFamily(family)
      .setQualifier(Bytes.toBytes("q"))
      .setType(Cell.Type.Put)
      .setValue(Bytes.toBytes("value"))
      .build());
    origin.addColumn(family, Bytes.toBytes("q0"), Bytes.toBytes("V-01"));
    origin.addColumn(family, Bytes.toBytes("q1"), 100, Bytes.toBytes("V-01"));
    Put clone = new Put(origin);
    assertEquals(origin, clone);
    origin.addColumn(family, Bytes.toBytes("q2"), Bytes.toBytes("V-02"));

    //They should have different cell lists
    assertNotEquals(origin.getCellList(family), clone.getCellList(family));
  }

  private void assertEquals(Mutation origin, Mutation clone) {
    Assert.assertEquals(origin.getFamilyCellMap().size(), clone.getFamilyCellMap().size());
    for (byte[] family : origin.getFamilyCellMap().keySet()) {
      List<Cell> originCells = origin.getCellList(family);
      List<Cell> cloneCells = clone.getCellList(family);
      Assert.assertEquals(originCells.size(), cloneCells.size());
      for (int i = 0; i != cloneCells.size(); ++i) {
        Cell originCell = originCells.get(i);
        Cell cloneCell = cloneCells.get(i);
        assertTrue(CellUtil.equals(originCell, cloneCell));
        assertTrue(CellUtil.matchingValue(originCell, cloneCell));
      }
    }
    Assert.assertEquals(origin.getAttributesMap().size(), clone.getAttributesMap().size());
    for (String name : origin.getAttributesMap().keySet()) {
      byte[] originValue = origin.getAttributesMap().get(name);
      byte[] cloneValue = clone.getAttributesMap().get(name);
      assertTrue(Bytes.equals(originValue, cloneValue));
    }
    Assert.assertEquals(origin.getTimestamp(), clone.getTimestamp());
    Assert.assertEquals(origin.getPriority(), clone.getPriority());
    if (origin instanceof Append) {
      assertEquals(((Append)origin).getTimeRange(), ((Append)clone).getTimeRange());
    }
    if (origin instanceof Increment) {
      assertEquals(((Increment)origin).getTimeRange(), ((Increment)clone).getTimeRange());
    }
  }

  private static void assertEquals(TimeRange origin, TimeRange clone) {
    Assert.assertEquals(origin.getMin(), clone.getMin());
    Assert.assertEquals(origin.getMax(), clone.getMax());
  }

  // HBASE-14881
  @Test
  public void testRowIsImmutableOrNot() {
    byte[] rowKey = Bytes.toBytes("immutable");

    // Test when row key is immutable
    Put putRowIsImmutable = new Put(rowKey, true);
    assertTrue(rowKey == putRowIsImmutable.getRow());  // No local copy is made

    // Test when row key is not immutable
    Put putRowIsNotImmutable = new Put(rowKey, 1000L, false);
    assertTrue(rowKey != putRowIsNotImmutable.getRow());  // A local copy is made
  }

  // HBASE-14882
  @Test
  public void testAddImmutableToPut() throws IOException {
    byte[] row        = Bytes.toBytes("immutable-row");
    byte[] family     = Bytes.toBytes("immutable-family");

    byte[] qualifier0 = Bytes.toBytes("immutable-qualifier-0");
    byte[] value0     = Bytes.toBytes("immutable-value-0");

    byte[] qualifier1 = Bytes.toBytes("immutable-qualifier-1");
    byte[] value1     = Bytes.toBytes("immutable-value-1");
    long   ts1        = 5000L;

    // "true" indicates that the input row is immutable
    Put put = new Put(row, true);
    put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
            .setRow(row)
            .setFamily(family)
            .setQualifier(qualifier0)
            .setTimestamp(put.getTimestamp())
            .setType(Type.Put)
            .setValue(value0)
            .build())
        .add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
            .setRow(row)
            .setFamily(family)
            .setQualifier(qualifier1)
            .setTimestamp(ts1)
            .setType(Type.Put)
            .setValue(value1)
            .build());

    // Verify the cell of family:qualifier0
    Cell cell0 = put.get(family, qualifier0).get(0);

    // Verify no local copy is made for family, qualifier or value
    assertTrue(cell0.getFamilyArray()    == family);
    assertTrue(cell0.getQualifierArray() == qualifier0);
    assertTrue(cell0.getValueArray()     == value0);

    // Verify timestamp
    assertTrue(cell0.getTimestamp()      == put.getTimestamp());

    // Verify the cell of family:qualifier1
    Cell cell1 = put.get(family, qualifier1).get(0);

    // Verify no local copy is made for family, qualifier or value
    assertTrue(cell1.getFamilyArray()    == family);
    assertTrue(cell1.getQualifierArray() == qualifier1);
    assertTrue(cell1.getValueArray()     == value1);

    // Verify timestamp
    assertTrue(cell1.getTimestamp()      == ts1);
  }
}
