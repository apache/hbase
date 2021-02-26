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
package org.apache.hadoop.hbase.master.assignment;

import static org.junit.Assert.fail;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Testcase for HBASE-23682.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestDeadServerMetricRegionChore {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDeadServerMetricRegionChore.class);

  protected HBaseTestingUtility util;

  @Before
  public void setUp() throws Exception {
    util = new HBaseTestingUtility();
    // Disable DeadServerMetricRegionChore
    util.getConfiguration()
      .setInt(AssignmentManager.DEAD_REGION_METRIC_CHORE_INTERVAL_MSEC_CONF_KEY, -1);
  }

  @After
  public void tearDown() throws Exception {
    this.util.shutdownMiniCluster();
  }

  @Test
  public void testDeadServerMetricRegionChore() throws Exception {
    try {
      this.util.startMiniCluster();
    } catch (Exception e) {
      fail("Start cluster failed");
    }
  }

}
