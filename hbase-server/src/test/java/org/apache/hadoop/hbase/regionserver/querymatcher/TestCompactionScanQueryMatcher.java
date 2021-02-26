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
package org.apache.hadoop.hbase.regionserver.querymatcher;

import static org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher.MatchCode.INCLUDE;
import static org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher.MatchCode.SKIP;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher.MatchCode;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestCompactionScanQueryMatcher extends AbstractTestScanQueryMatcher {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionScanQueryMatcher.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCompactionScanQueryMatcher.class);

  @Test
  public void testMatch_PartialRangeDropDeletes() throws Exception {
    // Some ranges.
    testDropDeletes(row2, row3, new byte[][] { row1, row2, row2, row3 }, INCLUDE, SKIP, SKIP,
      INCLUDE);
    testDropDeletes(row2, row3, new byte[][] { row1, row1, row2 }, INCLUDE, INCLUDE, SKIP);
    testDropDeletes(row2, row3, new byte[][] { row2, row3, row3 }, SKIP, INCLUDE, INCLUDE);
    testDropDeletes(row1, row3, new byte[][] { row1, row2, row3 }, SKIP, SKIP, INCLUDE);
    // Open ranges.
    testDropDeletes(HConstants.EMPTY_START_ROW, row3, new byte[][] { row1, row2, row3 }, SKIP, SKIP,
      INCLUDE);
    testDropDeletes(row2, HConstants.EMPTY_END_ROW, new byte[][] { row1, row2, row3 }, INCLUDE,
      SKIP, SKIP);
    testDropDeletes(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
      new byte[][] { row1, row2, row3, row3 }, SKIP, SKIP, SKIP, SKIP);

    // No KVs in range.
    testDropDeletes(row2, row3, new byte[][] { row1, row1, row3 }, INCLUDE, INCLUDE, INCLUDE);
    testDropDeletes(row2, row3, new byte[][] { row3, row3 }, INCLUDE, INCLUDE);
    testDropDeletes(row2, row3, new byte[][] { row1, row1 }, INCLUDE, INCLUDE);
  }

  private void testDropDeletes(byte[] from, byte[] to, byte[][] rows, MatchCode... expected)
      throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    // Set time to purge deletes to negative value to avoid it ever happening.
    ScanInfo scanInfo = new ScanInfo(this.conf, fam2, 0, 1, ttl, KeepDeletedCells.FALSE,
        HConstants.DEFAULT_BLOCKSIZE, -1L, rowComparator, false);

    CompactionScanQueryMatcher qm = CompactionScanQueryMatcher.create(scanInfo,
      ScanType.COMPACT_RETAIN_DELETES, Long.MAX_VALUE, HConstants.OLDEST_TIMESTAMP,
      HConstants.OLDEST_TIMESTAMP, now, from, to, null);
    List<ScanQueryMatcher.MatchCode> actual = new ArrayList<>(rows.length);
    byte[] prevRow = null;
    for (byte[] row : rows) {
      if (prevRow == null || !Bytes.equals(prevRow, row)) {
        qm.setToNewRow(KeyValueUtil.createFirstOnRow(row));
        prevRow = row;
      }
      actual.add(qm.match(new KeyValue(row, fam2, null, now, Type.Delete)));
    }

    assertEquals(expected.length, actual.size());
    for (int i = 0; i < expected.length; i++) {
      LOG.debug("expected " + expected[i] + ", actual " + actual.get(i));
      assertEquals(expected[i], actual.get(i));
    }
  }
}
