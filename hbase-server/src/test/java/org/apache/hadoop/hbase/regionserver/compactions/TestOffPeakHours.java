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
package org.apache.hadoop.hbase.regionserver.compactions;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestOffPeakHours {
  private static HBaseTestingUtility testUtil;

  @BeforeClass
  public static void setUpClass() {
    testUtil = new HBaseTestingUtility();
  }

  private int hourOfDay;
  private int hourPlusOne;
  private int hourMinusOne;
  private int hourMinusTwo;
  private Configuration conf;

  @Before
  public void setUp() {
    hourOfDay = 15;
    hourPlusOne = ((hourOfDay+1)%24);
    hourMinusOne = ((hourOfDay-1+24)%24);
    hourMinusTwo = ((hourOfDay-2+24)%24);
    conf = testUtil.getConfiguration();
  }

  @Test
  public void testWithoutSettings() {
    Configuration conf = testUtil.getConfiguration();
    OffPeakHours target = OffPeakHours.getInstance(conf);
    assertFalse(target.isOffPeakHour(hourOfDay));
  }

  @Test
  public void testSetPeakHourToTargetTime() {
    conf.setLong("hbase.offpeak.start.hour", hourMinusOne);
    conf.setLong("hbase.offpeak.end.hour", hourPlusOne);
    OffPeakHours target = OffPeakHours.getInstance(conf);
    assertTrue(target.isOffPeakHour(hourOfDay));
  }

  @Test
  public void testSetPeakHourOutsideCurrentSelection() {
    conf.setLong("hbase.offpeak.start.hour", hourMinusTwo);
    conf.setLong("hbase.offpeak.end.hour", hourMinusOne);
    OffPeakHours target = OffPeakHours.getInstance(conf);
    assertFalse(target.isOffPeakHour(hourOfDay));
  }
}
