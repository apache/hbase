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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

public abstract class AbstractTestResultScannerCursor extends AbstractTestScanCursor {

  protected abstract ResultScanner getScanner(Scan scan) throws Exception;

  @Test
  public void testHeartbeatWithSparseFilter() throws Exception {
    try (ResultScanner scanner = getScanner(createScanWithSparseFilter())) {
      int num = 0;
      Result r;
      while ((r = scanner.next()) != null) {
        if (num < (NUM_ROWS - 1) * NUM_FAMILIES * NUM_QUALIFIERS) {
          assertTrue(r.isCursor());
          assertArrayEquals(ROWS[num / NUM_FAMILIES / NUM_QUALIFIERS],
            r.getCursor().getRow());
        } else {
          assertFalse(r.isCursor());
          assertArrayEquals(ROWS[num / NUM_FAMILIES / NUM_QUALIFIERS], r.getRow());
        }
        num++;
      }
    }
  }

  @Test
  public void testHeartbeatWithSparseFilterReversed() throws Exception {
    try (ResultScanner scanner = getScanner(createReversedScanWithSparseFilter())) {
      int num = 0;
      Result r;
      while ((r = scanner.next()) != null) {
        if (num < (NUM_ROWS - 1) * NUM_FAMILIES * NUM_QUALIFIERS) {
          assertTrue(r.isCursor());
          assertArrayEquals(ROWS[NUM_ROWS - 1 - num / NUM_FAMILIES / NUM_QUALIFIERS],
            r.getCursor().getRow());
        } else {
          assertFalse(r.isCursor());
          assertArrayEquals(ROWS[0], r.getRow());
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
          assertTrue(r.isCursor());
          assertArrayEquals(ROWS[num / NUM_FAMILIES / NUM_QUALIFIERS],
            r.getCursor().getRow());
        } else {
          assertFalse(r.isCursor());
          assertArrayEquals(ROWS[num / NUM_FAMILIES / NUM_QUALIFIERS], r.getRow());
        }
        num++;
      }
    }
  }
}
