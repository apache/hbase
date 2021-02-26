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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MapReduceTests.class, LargeTests.class })
public class TestTableInputFormatScan extends TestTableInputFormatScanBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTableInputFormatScan.class);

  /**
   * Tests a MR scan using specific number of mappers. The test table has 26 regions,
   */
  @Test
  public void testGetSplits() throws IOException, InterruptedException, ClassNotFoundException {
    testNumOfSplits(1, 26);
    testNumOfSplits(3, 78);
  }

  /**
   * Runs a MR to test TIF using specific number of mappers. The test table has 26 regions,
   */
  @Test
  public void testSpecifiedNumOfMappersMR()
      throws InterruptedException, IOException, ClassNotFoundException {
    testNumOfSplitsMR(2, 52);
    testNumOfSplitsMR(4, 104);
  }

  /**
   * Test if autoBalance create correct splits
   */
  @Test
  public void testAutoBalanceSplits() throws IOException {
    testAutobalanceNumOfSplit();
  }

  @Test
  public void testScanFromConfiguration()
      throws IOException, InterruptedException, ClassNotFoundException {
    testScanFromConfiguration("bba", "bbd", "bbc");
  }
}
