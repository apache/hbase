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

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@Category(SmallTests.class)
public class TestTimestampType {

  private static long testPhysicalTime = 1234567890123L;
  private static long testLogicalTime = 12;

  /*
   * Tests for TimestampType enum
   */

  @Test
  public void testFromToEpoch() {
    for (TimestampType timestamp : TimestampType.values()) {
      long wallTime = System.currentTimeMillis();
      long converted = timestamp.toEpochTimeMillisFromTimestamp(
          timestamp.fromEpochTimeMillisToTimestamp(wallTime));

      assertEquals(wallTime, converted);
    }
  }

  /* Tests for HL Clock */
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
  public void testHybridGetPhysicalTime() {
    long ts = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, testPhysicalTime, testLogicalTime);
    assertEquals(testPhysicalTime, TimestampType.HYBRID.getPhysicalTime(ts));
  }

  @Test
  public void testHybridGetLogicalTime() {
    long ts = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, testPhysicalTime, testLogicalTime);
    assertEquals(testLogicalTime, TimestampType.HYBRID.getLogicalTime(ts));
  }

  @Test
  public void testHybridToString() {
    long ts = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, testPhysicalTime, testLogicalTime);

    assertEquals("2009-02-13T23:31:30:123(1234567890123), 12", TimestampType.HYBRID.toString(ts));
  }

  @Test
  public void testHybridToTimestamp() {
    long expected = (testPhysicalTime << TimestampType.HYBRID.getBitsForLogicalTime()) + testLogicalTime;
    // test millisecond
    long ts = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, testPhysicalTime, testLogicalTime);
    assertEquals(ts, expected);

    // test nanosecond
    ts = TimestampType.HYBRID.toTimestamp(TimeUnit.NANOSECONDS, TimeUnit.MILLISECONDS.toNanos(testPhysicalTime), testLogicalTime);
    assertEquals(ts, expected);
  }

  @Test
  public void testHybridIsLikelyOfType() throws ParseException {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSS Z");

    // test timestamps of Hybrid type from year 1971 to 2248 where lt = 0
    for (int year = 1971; year <= 2248; year += 1) {
      Date date = dateFormat.parse(year + "-01-01T11:22:33:444 UTC");

      // Hybrid type ts with pt = date and lt = 0
      long ts = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, date.getTime(), 0);
      System.out.println(TimestampType.HYBRID.toString(ts));

      assertTrue(TimestampType.HYBRID.isLikelyOfType(ts, true));
    }

    // test timestamps of Hybrid type from year 2016 to 2348 where lt > 0
    for (int year = 2016; year <= 2248; year += 1) {
      Date date = dateFormat.parse(year + "-01-01T11:22:33:444 UTC");

      // Hybrid type ts with pt = date and lt = 123
      long ts = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, date.getTime(), 123);
      System.out.println(TimestampType.HYBRID.toString(ts));

      assertTrue(TimestampType.HYBRID.isLikelyOfType(ts, true));
    }

    // test that timestamps from different years are not Hybrid type
    for (int year = 1970; year <= 10000 ;year += 10) {
      // Stardate 1970 to 10000
      Date date = dateFormat.parse(year + "-01-01T00:00:00:000 UTC");
      long ts = date.getTime();
      System.out.println(TimestampType.PHYSICAL.toString(ts));
      System.out.println(TimestampType.PHYSICAL.toString(TimestampType.HYBRID.getPhysicalTime(ts)));

      assertFalse(TimestampType.HYBRID.isLikelyOfType(ts, true));
    }

    // test that timestamps up to 2016 are not Hybrid even if lt = 0
    for (int year = 1970; year <= 2016; year += 1) {
      Date date = dateFormat.parse(year + "-01-01T11:22:33:444 UTC");

      // reset lt = 0
      long ts = ((date.getTime()
          >> TimestampType.HYBRID.getBitsForLogicalTime()) << TimestampType.HYBRID.getBitsForLogicalTime());
      System.out.println(Long.toHexString(ts));

      System.out.println(TimestampType.PHYSICAL.toString(ts));
      System.out.println(TimestampType.PHYSICAL.toString(TimestampType.HYBRID.getPhysicalTime(ts)));

      assertFalse(TimestampType.HYBRID.isLikelyOfType(ts, true));
    }

    // test that timestamps from currentTime epoch are not Hybrid type
    long systemTimeNow = System.currentTimeMillis();
    System.out.println(TimestampType.PHYSICAL.toString(systemTimeNow));
    System.out.println(TimestampType.PHYSICAL.toString((TimestampType.HYBRID.getPhysicalTime(systemTimeNow))));
    assertFalse(TimestampType.HYBRID.isLikelyOfType(systemTimeNow, true));
  }


  @Test
  public void testPhysicalMaxValues() {
    assertEquals(
        (1L << 63) - 1,
        TimestampType.PHYSICAL.getMaxPhysicalTime());

    assertEquals(0, TimestampType.PHYSICAL.getMaxLogicalTime());
  }

  @Test
  public void testPhysicalGetPhysicalTime() {
    long ts = TimestampType.PHYSICAL.toTimestamp(TimeUnit.MILLISECONDS, testPhysicalTime, testLogicalTime);
    assertEquals(testPhysicalTime, TimestampType.PHYSICAL.getPhysicalTime(ts));
  }

  @Test
  public void testPhysicalGetLogicalTime() {
    long ts = TimestampType.PHYSICAL.toTimestamp(TimeUnit.MILLISECONDS, testPhysicalTime, testLogicalTime);
    assertEquals(0, TimestampType.PHYSICAL.getLogicalTime(ts));
  }

  @Test
  public void testPhysicalToString() {
    long ts = TimestampType.PHYSICAL.toTimestamp(TimeUnit.MILLISECONDS, testPhysicalTime, testLogicalTime);

    assertEquals("2009-02-13T23:31:30:123(1234567890123), 0", TimestampType.PHYSICAL.toString(ts));
  }

  @Test
  public void testPhysicalToTimestamp() {
    // test millisecond
    long ts = TimestampType.PHYSICAL.toTimestamp(TimeUnit.MILLISECONDS, testPhysicalTime, testLogicalTime);
    assertEquals(ts, testPhysicalTime);

    // test nanosecond
    ts = TimestampType.PHYSICAL.toTimestamp(TimeUnit.NANOSECONDS, TimeUnit.MILLISECONDS.toNanos(testPhysicalTime), testLogicalTime);
    assertEquals(ts, testPhysicalTime);
  }

  @Test
  public void testPhysicalIsLikelyOfType() throws ParseException {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSS Z");

    // test that timestamps from 1970 to 3K epoch are of Physical type
    for (int year = 1970; year < 3000 ;year += 10) {
      // Start date 1970 to 10000
      Date date = dateFormat.parse(year + "-01-01T00:00:00:000 UTC");
      long ts = date.getTime();
      System.out.println(TimestampType.PHYSICAL.toString(ts));
      System.out.println(TimestampType.PHYSICAL.toString(TimestampType.HYBRID.getPhysicalTime(ts)));

      assertTrue(TimestampType.PHYSICAL.isLikelyOfType(ts, true));
    }

    // test that timestamps from currentTime epoch are of Physical type
    long systemTimeNow = System.currentTimeMillis();
    System.out.println(TimestampType.PHYSICAL.toString(systemTimeNow));
    assertTrue(TimestampType.PHYSICAL.isLikelyOfType(systemTimeNow, true));

    // test timestamps of Hybrid type from year 1970 to 2248 are not of Physical type
    for (int year = 1970; year <= 2248; year += 1) {
      Date date = dateFormat.parse(year + "-01-01T11:22:33:444 UTC");

      // Hybrid type ts with pt = date and lt = 0
      long ts = TimestampType.HYBRID.toTimestamp(TimeUnit.MILLISECONDS, date.getTime(), 0);
      System.out.println(TimestampType.HYBRID.toString(ts));
      System.out.println(TimestampType.PHYSICAL.toString(ts));

      assertFalse(TimestampType.PHYSICAL.isLikelyOfType(ts, true));
    }
  }
}