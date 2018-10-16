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
package org.apache.hadoop.hbase.metrics.impl;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.metrics.Timer;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test class for {@link TimerImpl}
 */
@Category(SmallTests.class)
public class TestTimerImpl {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTimerImpl.class);

  private Timer timer;

  @Before
  public void setup() {
    this.timer = new TimerImpl();
  }

  @Test
  public void testUpdate() {
    timer.update(40, TimeUnit.MILLISECONDS);
    timer.update(41, TimeUnit.MILLISECONDS);
    timer.update(42, TimeUnit.MILLISECONDS);
    assertEquals(3, timer.getHistogram().getCount());
    assertEquals(3, timer.getMeter().getCount());

    assertEquals(TimeUnit.MILLISECONDS.toMicros(41), timer.getHistogram().snapshot().getMedian());
  }
}
