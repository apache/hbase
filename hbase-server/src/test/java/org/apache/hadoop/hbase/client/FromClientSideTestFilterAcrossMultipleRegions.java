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

import static org.apache.hadoop.hbase.HBaseTestingUtil.countRows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HRegionLocation;
import org.junit.jupiter.api.TestTemplate;

public class FromClientSideTestFilterAcrossMultipleRegions extends FromClientSideTestBase {

  protected FromClientSideTestFilterAcrossMultipleRegions(
    Class<? extends ConnectionRegistry> registryImpl, int numHedgedReqs) {
    super(registryImpl, numHedgedReqs);
  }

  /**
   * Test filters when multiple regions. It does counts. Needs eye-balling of logs to ensure that
   * we're not scanning more regions that we're supposed to. Related to the TestFilterAcrossRegions
   * over in the o.a.h.h.filter package.
   */
  @TestTemplate
  public void testFilterAcrossMultipleRegions() throws IOException {
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table t = conn.getTable(tableName)) {
      int rowCount = TEST_UTIL.loadTable(t, FAMILY, false);
      assertRowCount(t, rowCount);
      // Split the table. Should split on a reasonable key; 'lqj'
      List<HRegionLocation> regions = splitTable(t);
      assertRowCount(t, rowCount);
      // Get end key of first region.
      byte[] endKey = regions.get(0).getRegion().getEndKey();
      // Count rows with a filter that stops us before passed 'endKey'.
      // Should be count of rows in first region.
      int endKeyCount = countRows(t, createScanWithRowFilter(endKey));
      assertTrue(endKeyCount < rowCount);

      // How do I know I did not got to second region? Thats tough. Can't really
      // do that in client-side region test. I verified by tracing in debugger.
      // I changed the messages that come out when set to DEBUG so should see
      // when scanner is done. Says "Finished with scanning..." with region name.
      // Check that its finished in right region.

      // New test. Make it so scan goes into next region by one and then two.
      // Make sure count comes out right.
      byte[] key = new byte[] { endKey[0], endKey[1], (byte) (endKey[2] + 1) };
      int plusOneCount = countRows(t, createScanWithRowFilter(key));
      assertEquals(endKeyCount + 1, plusOneCount);
      key = new byte[] { endKey[0], endKey[1], (byte) (endKey[2] + 2) };
      int plusTwoCount = countRows(t, createScanWithRowFilter(key));
      assertEquals(endKeyCount + 2, plusTwoCount);

      // New test. Make it so I scan one less than endkey.
      key = new byte[] { endKey[0], endKey[1], (byte) (endKey[2] - 1) };
      int minusOneCount = countRows(t, createScanWithRowFilter(key));
      assertEquals(endKeyCount - 1, minusOneCount);
      // For above test... study logs. Make sure we do "Finished with scanning.."
      // in first region and that we do not fall into the next region.

      key = new byte[] { 'a', 'a', 'a' };
      int countBBB = countRows(t, createScanWithRowFilter(key, null, CompareOperator.EQUAL));
      assertEquals(1, countBBB);

      int countGreater =
        countRows(t, createScanWithRowFilter(endKey, null, CompareOperator.GREATER_OR_EQUAL));
      // Because started at start of table.
      assertEquals(0, countGreater);
      countGreater =
        countRows(t, createScanWithRowFilter(endKey, endKey, CompareOperator.GREATER_OR_EQUAL));
      assertEquals(rowCount - endKeyCount, countGreater);
    }
  }
}
