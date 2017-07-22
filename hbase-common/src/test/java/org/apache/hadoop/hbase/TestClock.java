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

package org.apache.hadoop.hbase;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@Category(SmallTests.class)
public class TestClock {

  static final Clock MOCK_CLOCK = mock(Clock.class);
  static final long MAX_CLOCK_SKEW_IN_MS = 1000;

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  /**
   * Wrapper class around any Clock.
   * On calls to now() and update(), timestamps returned by the underlying clock are stored in an
   * array. Call assertMonotonic() to assert that the timestamps returned were monotonic or not.
   */
  private class MonotonicityCheckerClock {
    final boolean strictlyIncreasing;
    final Clock clock;
    final ArrayList<Long> timestamps = new ArrayList<>();

    MonotonicityCheckerClock(Clock clock, boolean strictlyIncreasing) {
      this.clock = clock;
      this.strictlyIncreasing = strictlyIncreasing;
    }

    long now() {
      long ts = clock.now();
      timestamps.add(ts);
      return ts;
    }

    long update(long timestamp) {
      long ts = clock.update(timestamp);
      timestamps.add(ts);
      return ts;
    }

    void assertMonotonic() {
      assertTrue(timestamps.size() > 0);
      long prev = 0;
      for (long timestamp : timestamps) {
        if (strictlyIncreasing) {
          // This simple comparison works correctly for all types of clocks we have currently.
          assertTrue(timestamps.toString(), timestamp > prev);
        } else {
          assertTrue(timestamps.toString(), timestamp >= prev);
        }
        prev = timestamp;
      }
    }
  }

  @Before
  public void setUp() {
    when(MOCK_CLOCK.getTimeUnit()).thenReturn(TimeUnit.MILLISECONDS);
  }

  // All Clocks Tests

  @Test
  public void testSystemMonotonicNow() {
    MonotonicityCheckerClock systemMonotonic =
        new MonotonicityCheckerClock(new Clock.SystemMonotonic(MOCK_CLOCK), false);

    // case 1: Set time and assert
    when(MOCK_CLOCK.now()).thenReturn(100L);
    assertEquals(100, systemMonotonic.now());

    // case 2: Go back in time and check monotonic property.
    when(MOCK_CLOCK.now()).thenReturn(99L);
    assertEquals(100, systemMonotonic.now());

    // case 3: system time goes ahead compared to previous timestamp.
    when(MOCK_CLOCK.now()).thenReturn(101L);
    assertEquals(101, systemMonotonic.now());

    systemMonotonic.assertMonotonic();
  }

  /**
   * Tests that
   * - Progressing mock clock progresses SystemMonotonic clock.
   * - Skews in the clock are correctly updated/not changed on call to update(), depending on
   * target time and clock's own time ( = system time + current skew).
   */
  @Test
  public void testSystemMonotonicUpdate() {
    Clock.SystemMonotonic systemMonotonicClock = new Clock.SystemMonotonic(MOCK_CLOCK);
    MonotonicityCheckerClock systemMonotonic =
        new MonotonicityCheckerClock(systemMonotonicClock, false);

    // Sets internal time
    when(MOCK_CLOCK.now()).thenReturn(99L);
    assertEquals(99, systemMonotonic.now());

    // case 1: Message timestamp is greater than current System Monotonic Time,
    // physical time at 100 still.
    when(MOCK_CLOCK.now()).thenReturn(100L);
    assertEquals(102, systemMonotonic.update(102));

    // case 2: Message timestamp is greater than current System Monotonic Time,
    // physical time at 100 still.
    when(MOCK_CLOCK.now()).thenReturn(100L);
    assertEquals(103, systemMonotonic.update(103));

    // case 3: Message timestamp is less than current System Monotonic Time, greater than current
    // physical time which is 100.
    assertEquals(103, systemMonotonic.update(101));

    // case 4: Message timestamp is less than current System Monotonic Time, less than current
    // physical time which is 100.
    assertEquals(103, systemMonotonic.update(99));

    // case 5: Message timestamp<System monotonic time and both less than current Physical Time
    when(MOCK_CLOCK.now()).thenReturn(106L);
    assertEquals(106, systemMonotonic.update(102));

    // case 6: Message timestamp>System monotonic time and both less than current Physical Time
    when(MOCK_CLOCK.now()).thenReturn(109L);
    assertEquals(109, systemMonotonic.update(108));

    systemMonotonic.assertMonotonic();
  }

  @Test
  public void testSystemMonotonicUpdateMaxClockSkew() throws Clock.ClockException {
    final long time = 100L;
    Clock.SystemMonotonic systemMonotonic =
        new Clock.SystemMonotonic(MOCK_CLOCK, MAX_CLOCK_SKEW_IN_MS);

    // Set Current Time.
    when(MOCK_CLOCK.now()).thenReturn(time);
    systemMonotonic.now();

    // Shouldn't throw ClockException
    systemMonotonic.update(time + MAX_CLOCK_SKEW_IN_MS - 1);

    exception.expect(Clock.ClockException.class);
    systemMonotonic.update(time + MAX_CLOCK_SKEW_IN_MS + 1);
  }

  // All Hybrid Logical Clock Tests

  private void assertHLCTime(long hlcTime, long expectedPhysicalTime, long expectedLogicalTime) {
    assertEquals(expectedPhysicalTime, TimestampType.HYBRID.getPhysicalTime(hlcTime));
    assertEquals(expectedLogicalTime, TimestampType.HYBRID.getLogicalTime(hlcTime));
  }

  @Test
  public void testHLCNow() throws Clock.ClockException {
    MonotonicityCheckerClock hybridLogicalClock = new MonotonicityCheckerClock(
        new Clock.HLC(new Clock.SystemMonotonic(MOCK_CLOCK)),
        true);  // true for strict monotonicity

    // case 1: Test if it returns correct time based on current physical time.
    //         Remember, initially logical time = 0
    when(MOCK_CLOCK.now()).thenReturn(100L);
    assertHLCTime(hybridLogicalClock.now(), 100, 0);

    // case 2: physical time doesn't change, logical time should increment.
    when(MOCK_CLOCK.now()).thenReturn(100L);
    assertHLCTime(hybridLogicalClock.now(), 100, 1);

    // case 3: physical time doesn't change still, logical time should increment again
    when(MOCK_CLOCK.now()).thenReturn(100L);
    assertHLCTime(hybridLogicalClock.now(), 100, 2);

    // case 4: physical time moves forward, logical time should reset to 0.
    when(MOCK_CLOCK.now()).thenReturn(101L);
    assertHLCTime(hybridLogicalClock.now(), 101, 0);

    // case 5: Monotonic increasing check, physical time goes back.
    when(MOCK_CLOCK.now()).thenReturn(99L);
    assertHLCTime(hybridLogicalClock.now(), 101, 1);

    hybridLogicalClock.assertMonotonic();
  }

  @Test
  public void testHLCNowLogicalTimeOverFlow() {
    Clock.HLC hybridLogicalClock = new Clock.HLC(new Clock.SystemMonotonic(MOCK_CLOCK));

    when(MOCK_CLOCK.now()).thenReturn(100L);
    hybridLogicalClock.now();  // current time (100, 0)
    for (int i = 0; i < TimestampType.HYBRID.getMaxLogicalTime() - 1; i++) {
      hybridLogicalClock.now();
    }
    exception.expect(Clock.ClockException.class);
    hybridLogicalClock.now();
  }

  // No need to check skews in this test, since they are member of SystemMonotonic and not HLC.
  @Test
  public void testHLCUpdate() throws Clock.ClockException {
    long messageTimestamp;
    MonotonicityCheckerClock hybridLogicalClock = new MonotonicityCheckerClock(
        new Clock.HLC(new Clock.SystemMonotonic(MOCK_CLOCK)),
        true);  // true for strictly increasing check

    // Set Current Time.
    when(MOCK_CLOCK.now()).thenReturn(100L);
    hybridLogicalClock.now();

    // case 1: Message physical timestamp is lower than current physical time.
    messageTimestamp = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, 99, 1);
    when(MOCK_CLOCK.now()).thenReturn(101L);
    assertHLCTime(hybridLogicalClock.update(messageTimestamp), 101, 0);

    // case 2: Message physical timestamp is greater than HLC physical time.
    messageTimestamp = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, 105 , 3);
    when(MOCK_CLOCK.now()).thenReturn(102L);
    assertHLCTime(hybridLogicalClock.update(messageTimestamp), 105, 4);

    // case 3: Message timestamp is less than HLC timestamp
    messageTimestamp = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, 104 , 4);
    when(MOCK_CLOCK.now()).thenReturn(103L);
    assertHLCTime(hybridLogicalClock.update(messageTimestamp), 105, 5);

    //case 4: Message timestamp with same physical time as HLC, but lower logical time
    messageTimestamp = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, 105 , 2);
    when(MOCK_CLOCK.now()).thenReturn(101L);
    assertHLCTime(hybridLogicalClock.update(messageTimestamp), 105, 6);

    //case 5: Message timestamp with same physical time as HLC, but higher logical time
    messageTimestamp = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, 105 , 8);
    when(MOCK_CLOCK.now()).thenReturn(102L);
    assertHLCTime(hybridLogicalClock.update(messageTimestamp), 105, 9);

    //case 6: Actual Physical Time greater than message physical timestamp and HLC physical time.
    messageTimestamp = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, 105 , 10);
    when(MOCK_CLOCK.now()).thenReturn(110L);
    assertHLCTime(hybridLogicalClock.update(messageTimestamp), 110, 0);

    hybridLogicalClock.assertMonotonic();
  }

  @Test
  public void testHLCUpdateLogicalTimeOverFlow() throws Clock.ClockException {
    long messageTimestamp;
    Clock.HLC hybridLogicalClock = new Clock.HLC(new Clock.SystemMonotonic(MOCK_CLOCK));

    // Set Current Time.
    when(MOCK_CLOCK.now()).thenReturn(100L);
    hybridLogicalClock.now();

    messageTimestamp = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, 100,
        TimestampType.HYBRID.getMaxLogicalTime());
    exception.expect(Clock.ClockException.class);
    hybridLogicalClock.update(messageTimestamp);
  }

  @Test
  public void testHLCUpdateMaxClockSkew() throws Clock.ClockException {
    final long time = 100;
    long messageTimestamp;
    Clock.HLC hybridLogicalClock = new Clock.HLC(
        new Clock.SystemMonotonic(MOCK_CLOCK, MAX_CLOCK_SKEW_IN_MS));

    // Set Current Time.
    when(MOCK_CLOCK.now()).thenReturn(time);
    hybridLogicalClock.now();

    messageTimestamp = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS,
        time + MAX_CLOCK_SKEW_IN_MS - 1, 0);
    hybridLogicalClock.update(messageTimestamp);

    messageTimestamp = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS,
        time + MAX_CLOCK_SKEW_IN_MS + 1, 0);
    exception.expect(Clock.ClockException.class);
    hybridLogicalClock.update(messageTimestamp);
  }
}
