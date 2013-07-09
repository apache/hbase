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

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.SmallTests;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestPeakCompactionsThrottle {
  private static HBaseTestingUtility testUtil;
  private Configuration conf;

  @BeforeClass
  public static void setUpClass() {
    testUtil = new HBaseTestingUtility();
  }

  @Before
  public void setUp() {
    conf = testUtil.getConfiguration();
  }

  @Test
  public void testSetPeakHourToTargetTime() throws IOException {
    conf.set(PeakCompactionsThrottle.PEAK_START_HOUR, "0");
    conf.set(PeakCompactionsThrottle.PEAK_END_HOUR, "23");
    PeakCompactionsThrottle peakCompactionsThrottle = new PeakCompactionsThrottle(conf);
    peakCompactionsThrottle.startCompaction();
    long numOfBytes = 60 * 1024 * 1024;
    peakCompactionsThrottle.throttle(numOfBytes);
    peakCompactionsThrottle.finishCompaction("region", "family");
    assertTrue(peakCompactionsThrottle.getSleepNumber() > 0);
  }

  @Test
  public void testSetPeakHourOutsideCurrentSelection() throws IOException {
    conf.set(PeakCompactionsThrottle.PEAK_START_HOUR, "-1");
    conf.set(PeakCompactionsThrottle.PEAK_END_HOUR, "-1");
    PeakCompactionsThrottle peakCompactionsThrottle = new PeakCompactionsThrottle(conf);
    peakCompactionsThrottle.startCompaction();
    long numOfBytes = 30 * 1024 * 1024;
    peakCompactionsThrottle.throttle(numOfBytes);
    peakCompactionsThrottle.finishCompaction("region", "family");
    assertTrue(peakCompactionsThrottle.getSleepNumber() == 0);
  }
}
