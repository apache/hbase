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

import java.io.IOException;

import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestScanCursor extends AbstractTestScanCursor {

  @Test
  public void testHeartbeatWithSparseFilter() throws Exception {
    try (ResultScanner scanner =
        TEST_UTIL.getConnection().getTable(TABLE_NAME).getScanner(createScanWithSparseFilter())) {
      int num = 0;
      Result r;
      while ((r = scanner.next()) != null) {
        if (num < (NUM_ROWS - 1) * NUM_FAMILIES * NUM_QUALIFIERS) {
          Assert.assertTrue(r.isCursor());
          Assert.assertArrayEquals(ROWS[num / NUM_FAMILIES / NUM_QUALIFIERS],
            r.getCursor().getRow());
        } else {
          Assert.assertFalse(r.isCursor());
          Assert.assertArrayEquals(ROWS[num / NUM_FAMILIES / NUM_QUALIFIERS], r.getRow());
        }
        num++;
      }
    }
  }

  @Test
  public void testHeartbeatWithSparseFilterReversed() throws Exception {
    try (ResultScanner scanner = TEST_UTIL.getConnection().getTable(TABLE_NAME)
        .getScanner(createReversedScanWithSparseFilter())) {
      int num = 0;
      Result r;
      while ((r = scanner.next()) != null) {
        if (num < (NUM_ROWS - 1) * NUM_FAMILIES * NUM_QUALIFIERS) {
          Assert.assertTrue(r.isCursor());
          Assert.assertArrayEquals(ROWS[NUM_ROWS - 1 - num / NUM_FAMILIES / NUM_QUALIFIERS],
            r.getCursor().getRow());
        } else {
          Assert.assertFalse(r.isCursor());
          Assert.assertArrayEquals(ROWS[0], r.getRow());
        }
        num++;
      }
    }
  }

  @Test
  public void testSizeLimit() throws IOException {
    try (ResultScanner scanner =
        TEST_UTIL.getConnection().getTable(TABLE_NAME).getScanner(createScanWithSizeLimit())) {
      int num = 0;
      Result r;
      while ((r = scanner.next()) != null) {
        if (num % (NUM_FAMILIES * NUM_QUALIFIERS) != (NUM_FAMILIES * NUM_QUALIFIERS) - 1) {
          Assert.assertTrue(r.isCursor());
          Assert.assertArrayEquals(ROWS[num / NUM_FAMILIES / NUM_QUALIFIERS],
            r.getCursor().getRow());
        } else {
          Assert.assertFalse(r.isCursor());
          Assert.assertArrayEquals(ROWS[num / NUM_FAMILIES / NUM_QUALIFIERS], r.getRow());
        }
        num++;
      }
    }
  }
}
