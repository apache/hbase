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

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.testclassification.SmallTests;

import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import org.apache.hadoop.hbase.TimestampType;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@Category(SmallTests.class)
public class TestClock {

  // utils
  private void assertTimestampsMonotonic(List<Long> timestamps, boolean
      strictlyIncreasing) {
    assertTrue(timestamps.size() > 0);

    long prev = 0;
    for (long timestamp : timestamps) {
      if (strictlyIncreasing) {
        assertTrue(timestamps.toString(), timestamp > prev);
      } else {
        assertTrue(timestamps.toString(), timestamp >= prev);
      }
      prev = timestamp;
    }
  }

  // All Clocks Tests

  /**
   * Remove this test if moving away from millis resolution for physical time. Be sure to change
   * {@link TimestampType} methods which assume millisecond resolution.
   */
  @Test public void TestClocksPhysicalTimeResolution() {
    Clock.System systemClock = new Clock.System();
    Clock.SystemMonotonic systemMonotonicClock = new Clock.SystemMonotonic();
    Clock.HLC hybridLogicalClock = new Clock.HLC();
    assertTrue(systemClock.getTimeUnit() == systemMonotonicClock.getTimeUnit()
        && systemClock.getTimeUnit() == hybridLogicalClock.getTimeUnit()
        && TimeUnit.MILLISECONDS == systemClock.getTimeUnit());
  }

  // All System Clock Tests
  @Test public void TestSystemClockIsMonotonic() {
    Clock.System systemClock = new Clock.System();
    assertFalse(systemClock.isMonotonic());
  }

  @Test public void testSystemClockIsMonotonicallyIncreasing() {
    Clock.System systemClock = new Clock.System();
    assertFalse(systemClock.isMonotonicallyIncreasing());
  }

  // All System Monotonic Clock Tests

  @Test public void testSystemMonotonicClockIsMonotonic() {
    Clock.SystemMonotonic systemMonotonicClock = new Clock.SystemMonotonic();
    assertTrue(systemMonotonicClock.isMonotonic());
  }

  @Test public void testSystemMonotonicClockIsMonotonicallyIncreasing() {
    Clock.SystemMonotonic systemMonotonicClock = new Clock.SystemMonotonic();
    assertFalse(systemMonotonicClock.isMonotonicallyIncreasing());
  }

  @Test public void testSystemMonotonicNow() {
    ArrayList<Long> timestamps = new ArrayList<Long>(3);
    long timestamp;
    Clock.PhysicalClock physicalClock = mock(Clock.PhysicalClock.class);
    when(physicalClock.getTimeUnit()).thenReturn(TimeUnit.MILLISECONDS);
    Clock.SystemMonotonic systemMonotonic = new Clock.SystemMonotonic(physicalClock, 30000);

    // case 1: Set time and assert
    when(physicalClock.now()).thenReturn(100L);
    timestamp = systemMonotonic.now();
    timestamps.add(timestamp);

    assertEquals(100, timestamp);

    // case 2: Go back in time and check monotonic property.
    when(physicalClock.now()).thenReturn(99L);
    timestamp = systemMonotonic.now();
    timestamps.add(timestamp);

    assertEquals(100, timestamp);

    // case 3: system time goes ahead compared to previous timestamp.
    when(physicalClock.now()).thenReturn(101L);
    timestamp = systemMonotonic.now();
    timestamps.add(timestamp);

    assertEquals(101, timestamp);

    assertTimestampsMonotonic(timestamps, false);
  }

  @Test public void testSystemMonotonicUpdate() {
    ArrayList<Long> timestamps = new ArrayList<Long>(7);
    long timestamp;
    Clock.PhysicalClock physicalClock = mock(Clock.PhysicalClock.class);
    when(physicalClock.getTimeUnit()).thenReturn(TimeUnit.MILLISECONDS);
    Clock.SystemMonotonic systemMonotonic = new Clock.SystemMonotonic(physicalClock, 30000);

    // Set Time
    when(physicalClock.now()).thenReturn(99L);
    timestamp = systemMonotonic.now();
    timestamps.add(timestamp);

    // case 1: Message timestamp is greater than current System Monotonic Time,
    // physical time at 100 still.
    when(physicalClock.now()).thenReturn(100L);
    timestamp = systemMonotonic.update(102);
    timestamps.add(timestamp);

    assertEquals(102, timestamp);

    // case 2: Message timestamp is greater than current System Monotonic Time,
    // physical time at 100 still.
    when(physicalClock.now()).thenReturn(100L);
    timestamp = systemMonotonic.update(103);
    timestamps.add(timestamp);

    assertEquals(103, timestamp);

    // case 3: Message timestamp is less than current System Monotonic Time, greater than current
    // physical time which is 100.
    timestamp = systemMonotonic.update(101);
    timestamps.add(timestamp);

    assertEquals(103, timestamp);

    // case 4: Message timestamp is less than current System Monotonic Time, less than current
    // physical time which is 100.
    timestamp = systemMonotonic.update(99);
    timestamps.add(timestamp);

    assertEquals(103, timestamp);

    // case 5: Message timestamp<System monotonic time and both less than current Physical Time
    when(physicalClock.now()).thenReturn(106L);
    timestamp = systemMonotonic.update(102);
    timestamps.add(timestamp);

    assertEquals(106, timestamp);

    // case 6: Message timestamp>System monotonic time and both less than current Physical Time
    when(physicalClock.now()).thenReturn(109L);
    timestamp = systemMonotonic.update(108);
    timestamps.add(timestamp);

    assertEquals(109, timestamp);

    assertTimestampsMonotonic(timestamps, false);
  }

  @Test public void testSystemMonotonicUpdateMaxClockSkew() throws Clock.ClockException {
    long maxClockSkew = 1000;
    Clock.PhysicalClock physicalClock = mock(Clock.PhysicalClock.class);
    Clock.SystemMonotonic systemMonotonic = new Clock.SystemMonotonic(physicalClock, maxClockSkew);
    when(physicalClock.getTimeUnit()).thenReturn(TimeUnit.MILLISECONDS);

    // Set Current Time.
    when(physicalClock.now()).thenReturn(100L);
    systemMonotonic.now();

    systemMonotonic.update(maxClockSkew+100-1);

    try{
      systemMonotonic.update(maxClockSkew+101);
      fail("Should have thrown Clock Exception");
    } catch (Clock.ClockException e){
      assertTrue(true);
    }
  }


  // All Hybrid Logical Clock Tests
  @Test public void testHLCIsMonotonic() {
    Clock.HLC hybridLogicalClock = new Clock.HLC();
    assertTrue(hybridLogicalClock.isMonotonic());
  }

  @Test public void testHLCIsMonotonicallyIncreasing() {
    Clock.HLC hybridLogicalClock = new Clock.HLC();
    assertTrue(hybridLogicalClock.isMonotonicallyIncreasing());
  }

  @Test public void testHLCNow() throws Clock.ClockException {
    ArrayList<Long> timestamps = new ArrayList<Long>(5);
    long timestamp;
    Clock.PhysicalClock physicalClock = mock(Clock.PhysicalClock.class);
    when(physicalClock.getTimeUnit()).thenReturn(TimeUnit.MILLISECONDS);
    Clock.HLC hybridLogicalClock = new Clock.HLC(physicalClock, 30000);


    // case 1: Test if it returns correct time based on current physical time.
    //         Remember, initially logical time = 0
    when(physicalClock.now()).thenReturn(100L);
    timestamp = hybridLogicalClock.now();
    timestamps.add(timestamp);

    assertEquals(100, hybridLogicalClock.getTimestampType().getPhysicalTime(timestamp));
    assertEquals(0, hybridLogicalClock.getTimestampType().getLogicalTime(timestamp));

    // case 2: physical time does'nt change, logical time should increment.
    when(physicalClock.now()).thenReturn(100L);
    timestamp = hybridLogicalClock.now();
    timestamps.add(timestamp);

    assertEquals(100, hybridLogicalClock.getTimestampType().getPhysicalTime(timestamp));
    assertEquals(1, hybridLogicalClock.getTimestampType().getLogicalTime(timestamp));

    // case 3: physical time does'nt change still, logical time should increment again
    when(physicalClock.now()).thenReturn(100L);
    timestamp = hybridLogicalClock.now();
    timestamps.add(timestamp);

    assertEquals(100, hybridLogicalClock.getTimestampType().getPhysicalTime(timestamp));
    assertEquals(2, hybridLogicalClock.getTimestampType().getLogicalTime(timestamp));

    // case 4: physical time moves forward, logical time should reset to 0.
    when(physicalClock.now()).thenReturn(101L);
    timestamp = hybridLogicalClock.now();
    timestamps.add(timestamp);

    assertEquals(101, hybridLogicalClock.getTimestampType().getPhysicalTime(timestamp));
    assertEquals(0, hybridLogicalClock.getTimestampType().getLogicalTime(timestamp));

    // case 5: Monotonic increasing check, physical time goes back.
    when(physicalClock.now()).thenReturn(99L);
    timestamp = hybridLogicalClock.now();
    timestamps.add(timestamp);

    assertEquals(101, hybridLogicalClock.getTimestampType().getPhysicalTime(timestamp));
    assertEquals(1, hybridLogicalClock.getTimestampType().getLogicalTime(timestamp));

    // Check if all timestamps generated in the process are strictly monotonic.
    assertTimestampsMonotonic(timestamps, true);
  }

  @Test public void testHLCUNowLogicalTimeOverFlow() throws Clock.ClockException {
    Clock.PhysicalClock physicalClock = mock(Clock.PhysicalClock.class);
    Clock.HLC hybridLogicalClock = new Clock.HLC(physicalClock, 100);
    when(physicalClock.getTimeUnit()).thenReturn(TimeUnit.MILLISECONDS);

    // Set Current Time.
    when(physicalClock.now()).thenReturn(100L);
    hybridLogicalClock.setPhysicalTime(100);
    hybridLogicalClock.setLogicalTime(TimestampType.HYBRID.getMaxLogicalTime());

    try{
      hybridLogicalClock.now();
      fail("Should have thrown Clock Exception");
    } catch (Clock.ClockException e){
      assertTrue(true);
    }
  }

  @Test public void testHLCUpdate() throws Clock.ClockException {
    ArrayList<Long> timestamps = new ArrayList<Long>(5);
    long timestamp, messageTimestamp;
    Clock.PhysicalClock physicalClock = mock(Clock.PhysicalClock.class);
    Clock.HLC hybridLogicalClock = new Clock.HLC(physicalClock, 100);
    when(physicalClock.getTimeUnit()).thenReturn(TimeUnit.MILLISECONDS);

    // Set Current Time.
    when(physicalClock.now()).thenReturn(100L);
    timestamp = hybridLogicalClock.now();
    timestamps.add(timestamp);

    // case 1: Message physical timestamp is lower than current physical time.
    messageTimestamp = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, 99, 1);
    when(physicalClock.now()).thenReturn(101L);
    timestamp = hybridLogicalClock.update(messageTimestamp);
    timestamps.add(timestamp);

    assertEquals(101, hybridLogicalClock.getTimestampType().getPhysicalTime(timestamp));
    assertEquals(0, hybridLogicalClock.getTimestampType().getLogicalTime(timestamp));

    // case 2: Message physical timestamp is greater than HLC physical time.
    messageTimestamp = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, 105 , 3);
    when(physicalClock.now()).thenReturn(102L);
    timestamp = hybridLogicalClock.update(messageTimestamp);
    timestamps.add(timestamp);

    assertEquals(105, hybridLogicalClock.getTimestampType().getPhysicalTime(timestamp));
    assertEquals(4, hybridLogicalClock.getTimestampType().getLogicalTime(timestamp));

    // case 3: Message timestamp is less than HLC timestamp
    messageTimestamp = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, 104 , 4);
    when(physicalClock.now()).thenReturn(103L);
    timestamp = hybridLogicalClock.update(messageTimestamp);
    timestamps.add(timestamp);

    assertEquals(105, hybridLogicalClock.getTimestampType().getPhysicalTime(timestamp));
    assertEquals(5, hybridLogicalClock.getTimestampType().getLogicalTime(timestamp));

    //case 4: Message timestamp with same physical time as HLC, but lower logical time
    messageTimestamp = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, 105 , 2);
    when(physicalClock.now()).thenReturn(101L);
    timestamp = hybridLogicalClock.update(messageTimestamp);
    timestamps.add(timestamp);

    assertEquals(105, hybridLogicalClock.getTimestampType().getPhysicalTime(timestamp));
    assertEquals(6, hybridLogicalClock.getTimestampType().getLogicalTime(timestamp));

    //case 5: Message timestamp with same physical time as HLC, but higher logical time
    messageTimestamp = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, 105 , 8);
    when(physicalClock.now()).thenReturn(102L);
    timestamp = hybridLogicalClock.update(messageTimestamp);
    timestamps.add(timestamp);

    assertEquals(105, hybridLogicalClock.getTimestampType().getPhysicalTime(timestamp));
    assertEquals(9, hybridLogicalClock.getTimestampType().getLogicalTime(timestamp));

    //case 6: Actual Physical Time greater than message physical timestamp and HLC physical time.
    messageTimestamp = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, 105 , 10);
    when(physicalClock.now()).thenReturn(110L);
    timestamp = hybridLogicalClock.update(messageTimestamp);
    timestamps.add(timestamp);

    assertEquals(110, hybridLogicalClock.getTimestampType().getPhysicalTime(timestamp));
    assertEquals(0, hybridLogicalClock.getTimestampType().getLogicalTime(timestamp));

    // Check if all timestamps generated in the process are strictly monotonic.
    assertTimestampsMonotonic(timestamps, true);
  }

  @Test public void testHLCUpdateLogicalTimeOverFlow() throws Clock.ClockException {
    long messageTimestamp;
    Clock.PhysicalClock physicalClock = mock(Clock.PhysicalClock.class);
    Clock.HLC hybridLogicalClock = new Clock.HLC(physicalClock, 100);
    when(physicalClock.getTimeUnit()).thenReturn(TimeUnit.MILLISECONDS);

    // Set Current Time.
    when(physicalClock.now()).thenReturn(100L);
    hybridLogicalClock.now();

    try{
      messageTimestamp = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, 100,
          TimestampType.HYBRID.getMaxLogicalTime());
      hybridLogicalClock.update(messageTimestamp);
      fail("Should have thrown Clock Exception");
    } catch (Clock.ClockException e){
      assertTrue(true);
    }
  }

  @Test public void testHLCUpdateMaxClockSkew() throws Clock.ClockException {
    long messageTimestamp, maxClockSkew = 1000;
    Clock.PhysicalClock physicalClock = mock(Clock.PhysicalClock.class);
    Clock.HLC hybridLogicalClock = new Clock.HLC(physicalClock, maxClockSkew);
    when(physicalClock.getTimeUnit()).thenReturn(TimeUnit.MILLISECONDS);

    // Set Current Time.
    when(physicalClock.now()).thenReturn(100L);
    hybridLogicalClock.now();
    messageTimestamp = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS,
        maxClockSkew-100, 0);
    hybridLogicalClock.update(messageTimestamp);

    try{
      messageTimestamp = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS,
          maxClockSkew+101, 0);
      hybridLogicalClock.update(messageTimestamp);
      fail("Should have thrown Clock Exception");
    } catch (Clock.ClockException e){
      assertTrue(true);
    }
  }
}
