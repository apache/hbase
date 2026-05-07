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

import org.junit.jupiter.api.TestTemplate;

public class FromClientSideTestHTableExistsMethodMultipleRegionsSingleGet
  extends FromClientSideTestBase {

  protected FromClientSideTestHTableExistsMethodMultipleRegionsSingleGet(
    Class<? extends ConnectionRegistry> registryImpl, int numHedgedReqs) {
    super(registryImpl, numHedgedReqs);
  }

  @TestTemplate
  public void testHTableExistsMethodMultipleRegionsSingleGet() throws Exception {
    TEST_UTIL.createTable(tableName, new byte[][] { FAMILY }, 1, new byte[] { 0x00 },
      new byte[] { (byte) 0xff }, 255);
    TEST_UTIL.waitTableAvailable(tableName, 10_000);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);

      Get get = new Get(ROW);

      boolean exist = table.exists(get);
      assertFalse(exist);

      table.put(put);

      exist = table.exists(get);
      assertTrue(exist);
    }
  }
}
