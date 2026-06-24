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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;

@Tag(RegionServerTests.TAG)
@Tag(LargeTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: compType={0}")
public class TestMajorCompaction extends MajorCompactionTestBase {

  public TestMajorCompaction(String compType) {
    super(compType);
  }

  /**
   * Run compaction and flushing memstore Assert deletes get cleaned up.
   */
  @TestTemplate
  public void testMajorCompaction() throws Exception {
    majorCompaction();
  }

  /**
   * Test that on a major compaction, if all cells are expired or deleted, then we'll end up with no
   * product. Make sure scanner over region returns right answer in this case - and that it just
   * basically works.
   */
  @TestTemplate
  public void testMajorCompactingToNoOutputWithReverseScan() throws IOException {
    createStoreFile(r);
    for (int i = 0; i < compactionThreshold; i++) {
      createStoreFile(r);
    }
    // Now delete everything.
    Scan scan = new Scan();
    scan.setReversed(true);
    try (InternalScanner s = r.getScanner(scan)) {
      do {
        List<Cell> results = new ArrayList<>();
        boolean result = s.next(results);
        assertTrue(!results.isEmpty());
        r.delete(new Delete(CellUtil.cloneRow(results.get(0))));
        if (!result) {
          break;
        }
      } while (true);
    }
    // Flush
    r.flush(true);
    // Major compact.
    r.compact(true);
    scan = new Scan();
    scan.setReversed(true);
    int counter = 0;
    try (InternalScanner s = r.getScanner(scan)) {
      do {
        List<Cell> results = new ArrayList<>();
        boolean result = s.next(results);
        if (!result) {
          break;
        }
        counter++;
      } while (true);
    }
    assertEquals(0, counter);
  }
}
