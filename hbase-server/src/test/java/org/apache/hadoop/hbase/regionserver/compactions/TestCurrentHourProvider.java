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

import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestCurrentHourProvider {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCurrentHourProvider.class);

  private static final List<String> ZONE_IDS = Lists.newArrayList("UTC", "US/Pacific", "Etc/GMT+8");

  /**
   * In timezone GMT+08:00, the unix time of 2020-08-20 11:52:41 is 1597895561000 and the unix time
   * of 2020-08-20 15:04:00 is 1597907081000, by calculating the delta time to get expected time in
   * current timezone, then we can get special hour no matter which timezone it runs.
   * <p/>
   * In addition, we should consider the Daylight Saving Time. If in DaylightTime, we need reduce
   * one hour.
   */
  @Test
  public void testWithEnvironmentEdge() {
    // test for all available zoneID
    for (String zoneID : ZONE_IDS) {
      TimeZone timezone = TimeZone.getTimeZone(zoneID);
      TimeZone.setDefault(timezone);

      // set a time represent hour 11
      long deltaFor11 = TimeZone.getDefault().getRawOffset() - 28800000;
      long timeFor11 = 1597895561000L - deltaFor11;
      EnvironmentEdgeManager.injectEdge(() -> timeFor11);
      CurrentHourProvider.advanceTick();
      int hour11 = CurrentHourProvider.getCurrentHour();
      if (TimeZone.getDefault().inDaylightTime(new Date(timeFor11))) {
        hour11 = CurrentHourProvider.getCurrentHour() - 1;
      }
      assertEquals(11, hour11);

      // set a time represent hour 15
      long deltaFor15 = TimeZone.getDefault().getRawOffset() - 28800000;
      long timeFor15 = 1597907081000L - deltaFor15;
      EnvironmentEdgeManager.injectEdge(() -> timeFor15);
      CurrentHourProvider.advanceTick();
      int hour15 = CurrentHourProvider.getCurrentHour();
      if (TimeZone.getDefault().inDaylightTime(new Date(timeFor15))) {
        hour15 = CurrentHourProvider.getCurrentHour() - 1;
      }
      assertEquals(15, hour15);
    }
  }
}
