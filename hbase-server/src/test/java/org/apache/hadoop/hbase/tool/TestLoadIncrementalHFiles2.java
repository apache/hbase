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
package org.apache.hadoop.hbase.tool;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Split from {@link TestLoadIncrementalHFiles}.
 */
@Category({ MiscTests.class, LargeTests.class })
public class TestLoadIncrementalHFiles2 extends TestLoadIncrementalHFilesBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLoadIncrementalHFiles2.class);

  /**
   * Test case that creates some regions and loads HFiles that cross the boundaries and have
   * different region boundaries than the table pre-split.
   */
  @Test
  public void testRegionCrossingHFileSplit() throws Exception {
    testRegionCrossingHFileSplit(BloomType.NONE);
  }

  /**
   * Test case that creates some regions and loads HFiles that cross the boundaries have a ROW bloom
   * filter and a different region boundaries than the table pre-split.
   */
  @Test
  public void testRegionCrossingHFileSplitRowBloom() throws Exception {
    testRegionCrossingHFileSplit(BloomType.ROW);
  }

  /**
   * Test case that creates some regions and loads HFiles that cross the boundaries have a ROWCOL
   * bloom filter and a different region boundaries than the table pre-split.
   */
  @Test
  public void testRegionCrossingHFileSplitRowColBloom() throws Exception {
    testRegionCrossingHFileSplit(BloomType.ROWCOL);
  }

  private void testRegionCrossingHFileSplit(BloomType bloomType) throws Exception {
    runTest("testHFileSplit" + bloomType + "Bloom", bloomType,
        new byte[][] { Bytes.toBytes("aaa"), Bytes.toBytes("fff"), Bytes.toBytes("jjj"),
            Bytes.toBytes("ppp"), Bytes.toBytes("uuu"), Bytes.toBytes("zzz"), },
        new byte[][][] { new byte[][] { Bytes.toBytes("aaaa"), Bytes.toBytes("eee") },
            new byte[][] { Bytes.toBytes("fff"), Bytes.toBytes("zzz") }, });
  }

  /**
   * Test case that creates some regions and loads HFiles that cross the boundaries of those regions
   */
  @Test
  public void testRegionCrossingLoad() throws Exception {
    runTest("testRegionCrossingLoad", BloomType.NONE,
        new byte[][][] { new byte[][] { Bytes.toBytes("aaaa"), Bytes.toBytes("eee") },
            new byte[][] { Bytes.toBytes("fff"), Bytes.toBytes("zzz") }, });
  }

  /**
   * Test loading into a column family that has a ROW bloom filter.
   */
  @Test
  public void testRegionCrossingRowBloom() throws Exception {
    runTest("testRegionCrossingLoadRowBloom", BloomType.ROW,
        new byte[][][] { new byte[][] { Bytes.toBytes("aaaa"), Bytes.toBytes("eee") },
            new byte[][] { Bytes.toBytes("fff"), Bytes.toBytes("zzz") }, });
  }

  /**
   * Test loading into a column family that has a ROWCOL bloom filter.
   */
  @Test
  public void testRegionCrossingRowColBloom() throws Exception {
    runTest("testRegionCrossingLoadRowColBloom", BloomType.ROWCOL,
        new byte[][][] { new byte[][] { Bytes.toBytes("aaaa"), Bytes.toBytes("eee") },
            new byte[][] { Bytes.toBytes("fff"), Bytes.toBytes("zzz") }, });
  }
}
