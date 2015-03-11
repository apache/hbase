/**
 *
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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * TestTableInputFormatScan part 1.
 * @see TestTableInputFormatScanBase
 */
@Category(LargeTests.class)
public class TestTableInputFormatScan1 extends TestTableInputFormatScanBase {

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanEmptyToEmpty()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan(null, null, null);
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanEmptyToAPP()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan(null, "app", "apo");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanEmptyToBBA()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan(null, "bba", "baz");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanEmptyToBBB()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan(null, "bbb", "bba");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanEmptyToOPP()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan(null, "opp", "opo");
  }

  /**
   * Tests a MR scan using specific number of mappers. The test table has 25 regions,
   * and all region sizes are set as 0 as default. The average region size is 1 (the smallest
   * positive). When we set hbase.mapreduce.input.ratio as -1, all regions will be cut into two
   * MapRedcue input splits, the number of MR input splits should be 50; when we set hbase
   * .mapreduce.input.ratio as 100, the sum of all region sizes is less then the average region
   * size, all regions will be combined into 1 MapRedcue input split.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testGetSplits() throws IOException, InterruptedException, ClassNotFoundException {
    testNumOfSplits("-1", 50);
    testNumOfSplits("100", 1);
  }

  /**
   * Tests the getSplitKey() method in TableInputFormatBase.java
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testGetSplitsPoint() throws IOException, InterruptedException,
  ClassNotFoundException {
    // Test Case 1: "aaabcdef" and "aaaff", split point is "aaad".
    byte[] start1 = { 'a', 'a', 'a', 'b', 'c', 'd', 'e', 'f' };
    byte[] end1 = { 'a', 'a', 'a', 'f', 'f' };
    byte[] splitPoint1 = { 'a', 'a', 'a', 'd' };
    testGetSplitKey(start1, end1, splitPoint1, true);

    // Test Case 2: "111000" and "1125790", split point is "111b".
    byte[] start2 = { '1', '1', '1', '0', '0', '0' };
    byte[] end2 = { '1', '1', '2', '5', '7', '9', '0' };
    byte[] splitPoint2 = { '1', '1', '1', 'b' };
    testGetSplitKey(start2, end2, splitPoint2, true);

    // Test Case 3: "aaaaaa" and "aab", split point is "aaap".
    byte[] start3 = { 'a', 'a', 'a', 'a', 'a', 'a' };
    byte[] end3 = { 'a', 'a', 'b' };
    byte[] splitPoint3 = { 'a', 'a', 'a', 'p' };
    testGetSplitKey(start3, end3, splitPoint3, true);

    // Test Case 4: "aaa" and "aaaz", split point is "aaaM".
    byte[] start4 = { 'a', 'a', 'a' };
    byte[] end4 = { 'a', 'a', 'a', 'z' };
    byte[] splitPoint4 = { 'a', 'a', 'a', 'M' };
    testGetSplitKey(start4, end4, splitPoint4, true);

    // Test Case 5: "aaa" and "aaba", split point is "aaap".
    byte[] start5 = { 'a', 'a', 'a' };
    byte[] end5 = { 'a', 'a', 'b', 'a' };
    byte[] splitPoint5 = { 'a', 'a', 'a', 'p' };
    testGetSplitKey(start5, end5, splitPoint5, true);

    // Test Case 6: empty key and "hhhqqqwww", split point is "h"
    byte[] start6 = {};
    byte[] end6 = { 'h', 'h', 'h', 'q', 'q', 'q', 'w', 'w' };
    byte[] splitPoint6 = { 'h' };
    testGetSplitKey(start6, end6, splitPoint6, true);

    // Test Case 7: "ffffaaa" and empty key, split point depends on the mode we choose(text key or
    // binary key).
    byte[] start7 = { 'f', 'f', 'f', 'f', 'a', 'a', 'a' };
    byte[] end7 = {};
    byte[] splitPointText7 = { 'f', '~', '~', '~', '~', '~', '~'  };
    byte[] splitPointBinary7 = { 'f', 127, 127, 127, 127, 127, 127  };
    testGetSplitKey(start7, end7, splitPointText7, true);
    testGetSplitKey(start7, end7, splitPointBinary7, false);

    // Test Case 8: both start key and end key are empty. Split point depends on the mode we
    // choose (text key or binary key).
    byte[] start8 = {};
    byte[] end8 = {};
    byte[] splitPointText8 = { 'O' };
    byte[] splitPointBinary8 = { 0 };
    testGetSplitKey(start8, end8, splitPointText8, true);
    testGetSplitKey(start8, end8, splitPointBinary8, false);

    // Test Case 9: Binary Key example
    byte[] start9 = { 13, -19, 126, 127 };
    byte[] end9 = { 13, -19, 127, 0 };
    byte[] splitPoint9 = { 13, -19, 127, -64 };
    testGetSplitKey(start9, end9, splitPoint9, false);
  }
}
