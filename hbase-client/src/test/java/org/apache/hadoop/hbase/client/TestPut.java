/**
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class, ClientTests.class })
public class TestPut {
  @Test
  public void testCopyConstructor() {
    Put origin = new Put(Bytes.toBytes("ROW-01"));
    byte[] family = Bytes.toBytes("CF-01");
    byte[] qualifier = Bytes.toBytes("Q-01");

    origin.addColumn(family, qualifier, Bytes.toBytes("V-01"));
    Put clone = new Put(origin);

    assertEquals(origin.getCellList(family), clone.getCellList(family));
    origin.addColumn(family, qualifier, Bytes.toBytes("V-02"));

    //They should have different cell lists
    assertNotEquals(origin.getCellList(family), clone.getCellList(family));

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
  public void testAddImmutable() throws IOException {
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
            .setTimestamp(put.getTimeStamp())
            .setType(CellBuilder.DataType.Put)
            .setValue(value0)
            .build())
        .add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
            .setRow(row)
            .setFamily(family)
            .setQualifier(qualifier1)
            .setTimestamp(ts1)
            .setType(CellBuilder.DataType.Put)
            .setValue(value1)
            .build());

    // Verify the cell of family:qualifier0
    Cell cell0 = put.get(family, qualifier0).get(0);

    // Verify no local copy is made for family, qualifier or value
    assertTrue(cell0.getFamilyArray()    == family);
    assertTrue(cell0.getQualifierArray() == qualifier0);
    assertTrue(cell0.getValueArray()     == value0);

    // Verify timestamp
    assertTrue(cell0.getTimestamp()      == put.getTimeStamp());

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
