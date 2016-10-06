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

import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

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
}
