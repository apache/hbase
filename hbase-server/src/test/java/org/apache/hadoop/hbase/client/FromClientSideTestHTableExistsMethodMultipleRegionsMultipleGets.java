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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FromClientSideTestHTableExistsMethodMultipleRegionsMultipleGets
  extends FromClientSideTestBase {

  private static final Logger LOG =
    LoggerFactory.getLogger(FromClientSideTestHTableExistsMethodMultipleRegionsMultipleGets.class);

  private static byte[] ANOTHERROW = Bytes.toBytes("anotherrow");

  protected FromClientSideTestHTableExistsMethodMultipleRegionsMultipleGets(
    Class<? extends ConnectionRegistry> registryImpl, int numHedgedReqs) {
    super(registryImpl, numHedgedReqs);
  }

  @TestTemplate
  public void testHTableExistsMethodMultipleRegionsMultipleGets() throws Exception {
    TEST_UTIL.createTable(tableName, new byte[][] { FAMILY }, 1, new byte[] { 0x00 },
      new byte[] { (byte) 0xff }, 255);
    TEST_UTIL.waitTableAvailable(tableName, 10_000);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);

      List<Get> gets = new ArrayList<>();
      gets.add(new Get(ANOTHERROW));
      gets.add(new Get(Bytes.add(ROW, new byte[] { 0x00 })));
      gets.add(new Get(ROW));
      gets.add(new Get(Bytes.add(ANOTHERROW, new byte[] { 0x00 })));

      LOG.info("Calling exists");
      boolean[] results = table.exists(gets);
      assertFalse(results[0]);
      assertFalse(results[1]);
      assertTrue(results[2]);
      assertFalse(results[3]);

      // Test with the first region.
      put = new Put(new byte[] { 0x00 });
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);

      gets = new ArrayList<>();
      gets.add(new Get(new byte[] { 0x00 }));
      gets.add(new Get(new byte[] { 0x00, 0x00 }));
      results = table.exists(gets);
      assertTrue(results[0]);
      assertFalse(results[1]);

      // Test with the last region
      put = new Put(new byte[] { (byte) 0xff, (byte) 0xff });
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);

      gets = new ArrayList<>();
      gets.add(new Get(new byte[] { (byte) 0xff }));
      gets.add(new Get(new byte[] { (byte) 0xff, (byte) 0xff }));
      gets.add(new Get(new byte[] { (byte) 0xff, (byte) 0xff, (byte) 0xff }));
      results = table.exists(gets);
      assertFalse(results[0]);
      assertTrue(results[1]);
      assertFalse(results[2]);
    }
  }
}
