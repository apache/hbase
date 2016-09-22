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

package org.apache.hadoop.hbase.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotFileInfo;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test Export Snapshot Tool helpers
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestExportSnapshotHelpers {
  private static final Log LOG = LogFactory.getLog(TestExportSnapshotHelpers.class);

  /**
   * Verfy the result of getBalanceSplits() method.
   * The result are groups of files, used as input list for the "export" mappers.
   * All the groups should have similar amount of data.
   *
   * The input list is a pair of file path and length.
   * The getBalanceSplits() function sort it by length,
   * and assign to each group a file, going back and forth through the groups.
   */
  @Test
  public void testBalanceSplit() throws Exception {
    // Create a list of files
    List<Pair<SnapshotFileInfo, Long>> files = new ArrayList<Pair<SnapshotFileInfo, Long>>();
    for (long i = 0; i <= 20; i++) {
      SnapshotFileInfo fileInfo = SnapshotFileInfo.newBuilder()
        .setType(SnapshotFileInfo.Type.HFILE)
        .setHfile("file-" + i)
        .build();
      files.add(new Pair<SnapshotFileInfo, Long>(fileInfo, i));
    }

    // Create 5 groups (total size 210)
    //    group 0: 20, 11, 10,  1 (total size: 42)
    //    group 1: 19, 12,  9,  2 (total size: 42)
    //    group 2: 18, 13,  8,  3 (total size: 42)
    //    group 3: 17, 12,  7,  4 (total size: 42)
    //    group 4: 16, 11,  6,  5 (total size: 42)
    List<List<Pair<SnapshotFileInfo, Long>>> splits = ExportSnapshot.getBalancedSplits(files, 5);
    assertEquals(5, splits.size());

    String[] split0 = new String[] {"file-20", "file-11", "file-10", "file-1", "file-0"};
    verifyBalanceSplit(splits.get(0), split0, 42);
    String[] split1 = new String[] {"file-19", "file-12", "file-9",  "file-2"};
    verifyBalanceSplit(splits.get(1), split1, 42);
    String[] split2 = new String[] {"file-18", "file-13", "file-8",  "file-3"};
    verifyBalanceSplit(splits.get(2), split2, 42);
    String[] split3 = new String[] {"file-17", "file-14", "file-7",  "file-4"};
    verifyBalanceSplit(splits.get(3), split3, 42);
    String[] split4 = new String[] {"file-16", "file-15", "file-6",  "file-5"};
    verifyBalanceSplit(splits.get(4), split4, 42);
  }

  private void verifyBalanceSplit(final List<Pair<SnapshotFileInfo, Long>> split,
      final String[] expected, final long expectedSize) {
    assertEquals(expected.length, split.size());
    long totalSize = 0;
    for (int i = 0; i < expected.length; ++i) {
      Pair<SnapshotFileInfo, Long> fileInfo = split.get(i);
      assertEquals(expected[i], fileInfo.getFirst().getHfile());
      totalSize += fileInfo.getSecond();
    }
    assertEquals(expectedSize, totalSize);
  }
}
