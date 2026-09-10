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
package org.apache.hadoop.hbase.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(ClientTests.TAG)
@Tag(SmallTests.TAG)
public class TestClientDataStructureMisc {

  @Test
  public void testAddKeyValue() {
    final byte[] CONTENTS_FAMILY = Bytes.toBytes("contents");
    final byte[] value = Bytes.toBytes("abcd");
    final byte[] row1 = Bytes.toBytes("row1");
    final byte[] row2 = Bytes.toBytes("row2");
    byte[] qualifier = Bytes.toBytes("qf1");
    Put put = new Put(row1);

    // Adding KeyValue with the same row
    KeyValue kv = new KeyValue(row1, CONTENTS_FAMILY, qualifier, value);
    boolean ok = true;
    try {
      put.add(kv);
    } catch (IOException e) {
      ok = false;
    }
    assertTrue(ok);

    // Adding KeyValue with the different row
    kv = new KeyValue(row2, CONTENTS_FAMILY, qualifier, value);
    ok = false;
    try {
      put.add(kv);
    } catch (IOException e) {
      ok = true;
    }
    assertTrue(ok);
  }

  /**
   * For HBASE-2156
   */
  @Test
  public void testScanVariableReuse() {
    byte[] family = Bytes.toBytes("family");
    byte[] qual = Bytes.toBytes("qual");
    Scan scan = new Scan();
    scan.addFamily(family);
    scan.addColumn(family, qual);

    assertEquals(1, scan.getFamilyMap().get(family).size());

    scan = new Scan();
    scan.addFamily(family);

    assertNull(scan.getFamilyMap().get(family));
    assertTrue(scan.getFamilyMap().containsKey(family));
  }

  @Test
  public void testNegativeTimestamp() throws IOException {
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
      () -> new Put(Bytes.toBytes("row"), -1), "Negative timestamps should not have been allowed");
    assertThat(ex.getMessage(), containsString("negative"));

    ex = assertThrows(
      IllegalArgumentException.class, () -> new Put(Bytes.toBytes("row"))
        .addColumn(Bytes.toBytes("f"), Bytes.toBytes("q"), -1, Bytes.toBytes("v")),
      "Negative timestamps should not have been allowed");
    assertThat(ex.getMessage(), containsString("negative"));

    ex = assertThrows(IllegalArgumentException.class, () -> new Delete(Bytes.toBytes("row"), -1),
      "Negative timestamps should not have been allowed");
    assertThat(ex.getMessage(), containsString("negative"));

    ex = assertThrows(IllegalArgumentException.class,
      () -> new Delete(Bytes.toBytes("row")).addFamily(Bytes.toBytes("f"), -1),
      "Negative timestamps should not have been allowed");
    assertThat(ex.getMessage(), containsString("negative"));

    ex = assertThrows(IllegalArgumentException.class, () -> new Scan().setTimeRange(-1, 1),
      "Negative timestamps should not have been allowed");
    assertThat(ex.getMessage(), containsString("negative"));

    // KeyValue should allow negative timestamps for backwards compat. Otherwise, if the user
    // already has negative timestamps in cluster data, HBase won't be able to handle that
    new KeyValue(Bytes.toBytes(42), Bytes.toBytes(42), Bytes.toBytes(42), -1, Bytes.toBytes(42));
  }
}
