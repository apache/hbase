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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({RegionServerTests.class, SmallTests.class})
public class TestCurrentHourProvider {
  private static final Logger LOG = LoggerFactory.getLogger(TestCurrentHourProvider.class);
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCurrentHourProvider.class);

  /**
   * In timezone GMT+08:00, the unix time of 2020-08-20 11:52:41 is 1597895561000
   * and the unix time of 2020-08-20 15:04:00 is 1597907081000,
   * by calculating the delta time to get expected time in current timezone,
   * then we can get special hour no matter which timezone it runs.
   */
  @Test
  public void testWithEnvironmentEdge() {
    // set a time represent hour 11
    long deltaFor11 = TimeZone.getDefault().getRawOffset() - 28800000;
    long timeFor11 = 1597895561000L - deltaFor11;
    EnvironmentEdgeManager.injectEdge(() -> timeFor11);
    assertEquals(11, CurrentHourProvider.getCurrentHour());

    // set a time represent hour 15
    long deltaFor15 = TimeZone.getDefault().getRawOffset() - 28800000;
    long timeFor15 = 1597907081000L - deltaFor15;
    EnvironmentEdgeManager.injectEdge(() -> timeFor15);
    assertEquals(15, CurrentHourProvider.getCurrentHour());
  }
}
