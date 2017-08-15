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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Tests for TimestampType enum.
 */
@Category(SmallTests.class)
public class TestTimestampType {
  private static final Log LOG = LogFactory.getLog(TestTimestampType.class);

  private static final long PHYSICAL_TIME = 1234567890123L;
  private static final long LOGICAL_TIME = 12;

  private static final long HLC_TIME = TimestampType.HYBRID.toTimestamp(
      TimeUnit.MILLISECONDS, PHYSICAL_TIME, LOGICAL_TIME);
  private static final long PHYSICAL_CLOCK_TIME = TimestampType.PHYSICAL.toTimestamp(
      TimeUnit.MILLISECONDS, PHYSICAL_TIME, LOGICAL_TIME);

  @Test
  public void testFromToEpoch() {
    for (TimestampType timestamp : TimestampType.values()) {
      long wallTime = System.currentTimeMillis();
      long converted = timestamp.toEpochTimeMillisFromTimestamp(
          timestamp.fromEpochTimeMillisToTimestamp(wallTime));

      assertEquals(wallTime, converted);
    }
  }

  @Test
  public void testHybridMaxValues() {
    // assert 44-bit Physical Time with signed comparison (actual 43 bits)
    assertEquals(
        (1L << (63-TimestampType.HYBRID.getBitsForLogicalTime())) - 1,
        TimestampType.HYBRID.getMaxPhysicalTime());

    // assert 20-bit Logical Time
    assertEquals(
        (1L << TimestampType.HYBRID.getBitsForLogicalTime()) - 1,
        TimestampType.HYBRID.getMaxLogicalTime());

    // assert that maximum representable timestamp is Long.MAX_VALUE (assuming signed comparison).
    assertEquals(
        Long.MAX_VALUE,
        TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS,
            TimestampType.HYBRID.getMaxPhysicalTime(),
            TimestampType.HYBRID.getMaxLogicalTime())
    );
  }

  @Test
  public void testHybridTimeComponents() {
    assertEquals(PHYSICAL_TIME, TimestampType.HYBRID.getPhysicalTime(HLC_TIME));
    assertEquals(LOGICAL_TIME, TimestampType.HYBRID.getLogicalTime(HLC_TIME));
  }

  @Test
  public void testHybridToString() {
    assertEquals("2009-02-13T23:31:30:123(1234567890123), 12",
        TimestampType.HYBRID.toString(HLC_TIME));
  }

  @Test
  public void testHybridToTimestamp() {
    long expected = (PHYSICAL_TIME << TimestampType.HYBRID.getBitsForLogicalTime()) + LOGICAL_TIME;
    // test millisecond
    assertEquals(HLC_TIME, expected);

    // test nanosecond
    long ts = TimestampType.HYBRID.toTimestamp(TimeUnit.NANOSECONDS,
        TimeUnit.MILLISECONDS.toNanos(PHYSICAL_TIME), LOGICAL_TIME);
    assertEquals(ts, expected);
  }

  @Test
  public void testHybridIsLikelyOfType() throws ParseException {
    ZonedDateTime date = LocalDateTime.of(1970, 1, 1, 1, 1).atZone(ZoneId.of("UTC"));
    // test timestamps of Hybrid type from year 1971 to 2248 where lt = 0
    for (int year = 1971; year <= 2248; year += 1) {
      date = date.withYear(year);
      // Hybrid type ts with pt = date and lt = 0
      long ts = TimestampType.HYBRID.toTimestamp(TimeUnit.SECONDS, date.toEpochSecond(), 0);
      assertTrue("Year = " + year, TimestampType.HYBRID.isLikelyOfType(ts));
    }

    // test timestamps of Hybrid type from year 2016 to 2348 where lt > 0
    for (int year = 2016; year <= 2248; year += 1) {
      // Timestamps from first half of the first day of 1970 fail the HYBRID#isLikelyOfType check
      // since the result after converting to a hybrid timestamp is very small so we offset by the
      // second day
      date = date.withDayOfYear(2).withYear(year);

      // Hybrid type ts with pt = date and lt = 123
      long ts = TimestampType.HYBRID.toTimestamp(TimeUnit.SECONDS, date.toEpochSecond(), 123);
      assertTrue("Year = " + year, TimestampType.HYBRID.isLikelyOfType(ts));
    }

    // test that timestamps from different years are not Hybrid type
    for (int year = 1970; year <= 10000 ;year += 10) {
      date = date.withYear(year);
      final long ts = date.toEpochSecond() * 1000;
      assertFalse("Year = " + year, TimestampType.HYBRID.isLikelyOfType(ts));
    }

    // test that timestamps up to 2016 are not Hybrid even if lt = 0
    for (int year = 1970; year <= 2016; year += 1) {
      date = date.withYear(year);

      // reset lt = 0
      long ts = (((date.toEpochSecond() * 1000)
          >> TimestampType.HYBRID.getBitsForLogicalTime())
          << TimestampType.HYBRID.getBitsForLogicalTime());
      assertFalse("Year = " + year, TimestampType.HYBRID.isLikelyOfType(ts));
    }

    // test that timestamps from currentTime epoch are not Hybrid type
    long systemTimeNow = System.currentTimeMillis();
    LOG.info(TimestampType.PHYSICAL.toString(systemTimeNow));
    LOG.info(TimestampType.PHYSICAL.toString((TimestampType.HYBRID.getPhysicalTime(systemTimeNow))));
    assertFalse(TimestampType.HYBRID.isLikelyOfType(systemTimeNow));
  }


  @Test
  public void testPhysicalMaxValues() {
    assertEquals( (1L << 63)- 1, TimestampType.PHYSICAL.getMaxPhysicalTime());

    assertEquals(0, TimestampType.PHYSICAL.getMaxLogicalTime());
  }

  @Test
  public void testPhysicalClockPhysicalAndLogicalTime() {
    assertEquals(PHYSICAL_TIME, TimestampType.PHYSICAL.getPhysicalTime(PHYSICAL_CLOCK_TIME));
    assertEquals(0, TimestampType.PHYSICAL.getLogicalTime(PHYSICAL_CLOCK_TIME));
  }

  @Test
  public void testPhysicalToString() {
    assertEquals("2009-02-13T23:31:30:123(1234567890123), 0",
        TimestampType.PHYSICAL.toString(PHYSICAL_CLOCK_TIME));
  }

  @Test
  public void testPhysicalToTimestamp() {
    // test millisecond
    assertEquals(PHYSICAL_CLOCK_TIME, PHYSICAL_TIME);

    // test nanosecond
    long ts = TimestampType.PHYSICAL.toTimestamp(TimeUnit.NANOSECONDS,
        TimeUnit.MILLISECONDS.toNanos(PHYSICAL_TIME), LOGICAL_TIME);
    assertEquals(ts, PHYSICAL_TIME);
  }

  @Test
  public void testPhysicalIsLikelyOfType() throws ParseException {
    ZonedDateTime date = LocalDateTime.of(1970, 1, 1, 1, 1).atZone(ZoneId.of("UTC"));

    // test that timestamps from 1970 to 3K epoch are of Physical type
    for (int year = 1970; year < 3000 ;year += 10) {
      // Start date 1970 to 10000
      date = date.withYear(year);

      final long ts = date.toEpochSecond() * 1000;
      assertTrue("Year = " + year, TimestampType.PHYSICAL.isLikelyOfType(ts));
    }

    // test that timestamps from currentTime epoch are of Physical type
    long systemTimeNow = System.currentTimeMillis();
    assertTrue(TimestampType.PHYSICAL.isLikelyOfType(systemTimeNow));

    // test timestamps of Hybrid type from year 1970 to 2248 are not of Physical type
    for (int year = 1970; year <= 2248; year += 1) {
      // Timestamps from first half of the first day of 1970 fail the HYBRID#isLikelyOfType check
      // since the result after converting to a hybrid timestamp is very small so we offset by the
      // second day
      date = date.withDayOfYear(2).withYear(year);
      // Hybrid type ts with pt = date and lt = 0
      long ts = TimestampType.HYBRID.toTimestamp(TimeUnit.SECONDS, date.toEpochSecond(), 0);
      assertFalse(TimestampType.PHYSICAL.isLikelyOfType(ts));
    }
  }
}