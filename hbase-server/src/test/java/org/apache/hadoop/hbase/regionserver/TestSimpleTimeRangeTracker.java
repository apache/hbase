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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestSimpleTimeRangeTracker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSimpleTimeRangeTracker.class);

  protected TimeRangeTracker getTimeRangeTracker() {
    return TimeRangeTracker.create(TimeRangeTracker.Type.NON_SYNC);
  }

  protected TimeRangeTracker getTimeRangeTracker(long min, long max) {
    return TimeRangeTracker.create(TimeRangeTracker.Type.NON_SYNC, min, max);
  }

  @Test
  public void testExtreme() {
    TimeRange tr = TimeRange.allTime();
    assertTrue(tr.includesTimeRange(TimeRange.allTime()));
    TimeRangeTracker trt = getTimeRangeTracker();
    assertFalse(trt.includesTimeRange(TimeRange.allTime()));
    trt.includeTimestamp(1);
    trt.includeTimestamp(10);
    assertTrue(trt.includesTimeRange(TimeRange.allTime()));
  }

  @Test
  public void testTimeRangeInitialized() {
    TimeRangeTracker src = getTimeRangeTracker();
    TimeRange tr = new TimeRange(System.currentTimeMillis());
    assertFalse(src.includesTimeRange(tr));
  }

  @Test
  public void testTimeRangeTrackerNullIsSameAsTimeRangeNull() throws IOException {
    TimeRangeTracker src = getTimeRangeTracker(1, 2);
    byte[] bytes = TimeRangeTracker.toByteArray(src);
    TimeRange tgt = TimeRangeTracker.parseFrom(bytes).toTimeRange();
    assertEquals(src.getMin(), tgt.getMin());
    assertEquals(src.getMax(), tgt.getMax());
  }

  @Test
  public void testSerialization() throws IOException {
    TimeRangeTracker src = getTimeRangeTracker(1, 2);
    TimeRangeTracker tgt = TimeRangeTracker.parseFrom(TimeRangeTracker.toByteArray(src));
    assertEquals(src.getMin(), tgt.getMin());
    assertEquals(src.getMax(), tgt.getMax());
  }

  @Test
  public void testLegacySerialization() throws IOException {
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(data);
    output.writeLong(100);
    output.writeLong(200);
    TimeRangeTracker tgt = TimeRangeTracker.parseFrom(data.toByteArray());
    assertEquals(100, tgt.getMin());
    assertEquals(200, tgt.getMax());
  }

  @Test
  public void testAlwaysDecrementingSetsMaximum() {
    TimeRangeTracker trr = getTimeRangeTracker();
    trr.includeTimestamp(3);
    trr.includeTimestamp(2);
    trr.includeTimestamp(1);
    assertTrue(trr.getMin() != TimeRangeTracker.INITIAL_MIN_TIMESTAMP);
    assertTrue(trr.getMax() != -1 /*The initial max value*/);
  }

  @Test
  public void testSimpleInRange() {
    TimeRangeTracker trr = getTimeRangeTracker();
    trr.includeTimestamp(0);
    trr.includeTimestamp(2);
    assertTrue(trr.includesTimeRange(new TimeRange(1)));
  }

  @Test
  public void testRangeConstruction() throws IOException {
    TimeRange defaultRange = TimeRange.allTime();
    assertEquals(0L, defaultRange.getMin());
    assertEquals(Long.MAX_VALUE, defaultRange.getMax());
    assertTrue(defaultRange.isAllTime());

    TimeRange oneArgRange = new TimeRange(0L);
    assertEquals(0L, oneArgRange.getMin());
    assertEquals(Long.MAX_VALUE, oneArgRange.getMax());
    assertTrue(oneArgRange.isAllTime());

    TimeRange oneArgRange2 = new TimeRange(1);
    assertEquals(1, oneArgRange2.getMin());
    assertEquals(Long.MAX_VALUE, oneArgRange2.getMax());
    assertFalse(oneArgRange2.isAllTime());

    TimeRange twoArgRange = new TimeRange(0L, Long.MAX_VALUE);
    assertEquals(0L, twoArgRange.getMin());
    assertEquals(Long.MAX_VALUE, twoArgRange.getMax());
    assertTrue(twoArgRange.isAllTime());

    TimeRange twoArgRange2 = new TimeRange(0L, Long.MAX_VALUE - 1);
    assertEquals(0L, twoArgRange2.getMin());
    assertEquals(Long.MAX_VALUE - 1, twoArgRange2.getMax());
    assertFalse(twoArgRange2.isAllTime());

    TimeRange twoArgRange3 = new TimeRange(1, Long.MAX_VALUE);
    assertEquals(1, twoArgRange3.getMin());
    assertEquals(Long.MAX_VALUE, twoArgRange3.getMax());
    assertFalse(twoArgRange3.isAllTime());
  }

}
