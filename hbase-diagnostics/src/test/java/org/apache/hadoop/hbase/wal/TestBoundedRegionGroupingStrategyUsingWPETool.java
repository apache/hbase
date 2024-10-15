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
package org.apache.hadoop.hbase.wal;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for TestBoundedRegionGroupingStrategy which use WALPerformanceEvaluation for WAL data
 * creation. This class was created as part of refactoring for hbase-diagnostics module creation in
 * HBASE-28432 to break cyclic dependency.
 */
@RunWith(Parameterized.class)
@Category({ RegionServerTests.class, MediumTests.class })
public class TestBoundedRegionGroupingStrategyUsingWPETool
  extends TestBoundedRegionGroupingStrategy {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBoundedRegionGroupingStrategyUsingWPETool.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestBoundedRegionGroupingStrategyUsingWPETool.class);

  /**
   * Write to a log file with three concurrent threads and verifying all data is written.
   */
  @Test
  public void testConcurrentWrites() throws Exception {
    // Run the WPE tool with three threads writing 3000 edits each concurrently.
    // When done, verify that all edits were written.
    int errCode = WALPerformanceEvaluation.innerMain(new Configuration(CONF),
      new String[] { "-threads", "3", "-verify", "-noclosefs", "-iterations", "3000" });
    assertEquals(0, errCode);
  }

  /**
   * Make sure we can successfully run with more regions then our bound.
   */
  @Test
  public void testMoreRegionsThanBound() throws Exception {
    final String parallelism =
      Integer.toString(BoundedGroupingStrategy.DEFAULT_NUM_REGION_GROUPS * 2);
    int errCode =
      WALPerformanceEvaluation.innerMain(new Configuration(CONF), new String[] { "-threads",
        parallelism, "-verify", "-noclosefs", "-iterations", "3000", "-regions", parallelism });
    assertEquals(0, errCode);
  }

  @Test
  public void testBoundsGreaterThanDefault() throws Exception {
    final int temp = CONF.getInt(BoundedGroupingStrategy.NUM_REGION_GROUPS,
      BoundedGroupingStrategy.DEFAULT_NUM_REGION_GROUPS);
    try {
      CONF.setInt(BoundedGroupingStrategy.NUM_REGION_GROUPS, temp * 4);
      final String parallelism = Integer.toString(temp * 4);
      int errCode =
        WALPerformanceEvaluation.innerMain(new Configuration(CONF), new String[] { "-threads",
          parallelism, "-verify", "-noclosefs", "-iterations", "3000", "-regions", parallelism });
      assertEquals(0, errCode);
    } finally {
      CONF.setInt(BoundedGroupingStrategy.NUM_REGION_GROUPS, temp);
    }
  }

  @Test
  public void testMoreRegionsThanBoundWithBoundsGreaterThanDefault() throws Exception {
    final int temp = CONF.getInt(BoundedGroupingStrategy.NUM_REGION_GROUPS,
      BoundedGroupingStrategy.DEFAULT_NUM_REGION_GROUPS);
    try {
      CONF.setInt(BoundedGroupingStrategy.NUM_REGION_GROUPS, temp * 4);
      final String parallelism = Integer.toString(temp * 4 * 2);
      int errCode =
        WALPerformanceEvaluation.innerMain(new Configuration(CONF), new String[] { "-threads",
          parallelism, "-verify", "-noclosefs", "-iterations", "3000", "-regions", parallelism });
      assertEquals(0, errCode);
    } finally {
      CONF.setInt(BoundedGroupingStrategy.NUM_REGION_GROUPS, temp);
    }
  }
}
