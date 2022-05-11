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
package org.apache.hadoop.hbase.io;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Represents an interval of version timestamps. Presumes timestamps between
 * {@link #INITIAL_MIN_TIMESTAMP} and {@link #INITIAL_MAX_TIMESTAMP} only. Gets freaked out if
 * passed a timestamp that is < {@link #INITIAL_MIN_TIMESTAMP},
 * <p>
 * Evaluated according to minStamp &lt;= timestamp &lt; maxStamp or [minStamp,maxStamp) in interval
 * notation.
 * <p>
 * Can be returned and read by clients. Should not be directly created by clients. Thus, all
 * constructors are purposely @InterfaceAudience.Private.
 * <p>
 * Immutable. Thread-safe.
 */
@InterfaceAudience.Public
public class TimeRange {
  public static final long INITIAL_MIN_TIMESTAMP = 0L;
  public static final long INITIAL_MAX_TIMESTAMP = Long.MAX_VALUE;
  private static final TimeRange ALL_TIME =
    new TimeRange(INITIAL_MIN_TIMESTAMP, INITIAL_MAX_TIMESTAMP);

  public static TimeRange allTime() {
    return ALL_TIME;
  }

  public static TimeRange at(long ts) {
    if (ts < 0 || ts == Long.MAX_VALUE) {
      throw new IllegalArgumentException("invalid ts:" + ts);
    }
    return new TimeRange(ts, ts + 1);
  }

  /**
   * Represents the time interval [minStamp, Long.MAX_VALUE)
   * @param minStamp the minimum timestamp value, inclusive
   */
  public static TimeRange from(long minStamp) {
    check(minStamp, INITIAL_MAX_TIMESTAMP);
    return new TimeRange(minStamp, INITIAL_MAX_TIMESTAMP);
  }

  /**
   * Represents the time interval [0, maxStamp)
   * @param maxStamp the minimum timestamp value, exclusive
   */
  public static TimeRange until(long maxStamp) {
    check(INITIAL_MIN_TIMESTAMP, maxStamp);
    return new TimeRange(INITIAL_MIN_TIMESTAMP, maxStamp);
  }

  /**
   * Represents the time interval [minStamp, maxStamp)
   * @param minStamp the minimum timestamp, inclusive
   * @param maxStamp the maximum timestamp, exclusive
   */
  public static TimeRange between(long minStamp, long maxStamp) {
    check(minStamp, maxStamp);
    return new TimeRange(minStamp, maxStamp);
  }

  private final long minStamp;
  private final long maxStamp;
  private final boolean allTime;

  /**
   * Default constructor. Represents interval [0, Long.MAX_VALUE) (allTime)
   * @deprecated This is made @InterfaceAudience.Private in the 2.0 line and above and may be
   *             changed to private or removed in 3.0.
   */
  @Deprecated
  @InterfaceAudience.Private
  public TimeRange() {
    this(INITIAL_MIN_TIMESTAMP, INITIAL_MAX_TIMESTAMP);
  }

  /**
   * Represents interval [minStamp, Long.MAX_VALUE)
   * @param minStamp the minimum timestamp value, inclusive
   * @deprecated This is made @InterfaceAudience.Private in the 2.0 line and above and may be
   *             changed to private or removed in 3.0.
   */
  @Deprecated
  @InterfaceAudience.Private
  public TimeRange(long minStamp) {
    this(minStamp, INITIAL_MAX_TIMESTAMP);
  }

  /**
   * Represents interval [minStamp, Long.MAX_VALUE)
   * @param minStamp the minimum timestamp value, inclusive
   * @deprecated This is made @InterfaceAudience.Private in the 2.0 line and above and may be
   *             changed to private or removed in 3.0.
   */
  @Deprecated
  @InterfaceAudience.Private
  public TimeRange(byte[] minStamp) {
    this(Bytes.toLong(minStamp));
  }

  /**
   * Represents interval [minStamp, maxStamp)
   * @param minStamp the minimum timestamp, inclusive
   * @param maxStamp the maximum timestamp, exclusive
   * @deprecated This is made @InterfaceAudience.Private in the 2.0 line and above and may be
   *             changed to private or removed in 3.0.
   */
  @Deprecated
  @InterfaceAudience.Private
  public TimeRange(byte[] minStamp, byte[] maxStamp) {
    this(Bytes.toLong(minStamp), Bytes.toLong(maxStamp));
  }

  /**
   * Represents interval [minStamp, maxStamp)
   * @param minStamp the minimum timestamp, inclusive
   * @param maxStamp the maximum timestamp, exclusive
   * @throws IllegalArgumentException if either <0,
   * @deprecated This is made @InterfaceAudience.Private in the 2.0 line and above and may be
   *             changed to private or removed in 3.0.
   */
  @Deprecated
  @InterfaceAudience.Private
  public TimeRange(long minStamp, long maxStamp) {
    check(minStamp, maxStamp);
    this.minStamp = minStamp;
    this.maxStamp = maxStamp;
    this.allTime = isAllTime(minStamp, maxStamp);
  }

  private static boolean isAllTime(long minStamp, long maxStamp) {
    return minStamp == INITIAL_MIN_TIMESTAMP && maxStamp == INITIAL_MAX_TIMESTAMP;
  }

  private static void check(long minStamp, long maxStamp) {
    if (minStamp < 0 || maxStamp < 0) {
      throw new IllegalArgumentException(
        "Timestamp cannot be negative. minStamp:" + minStamp + ", maxStamp:" + maxStamp);
    }
    if (maxStamp < minStamp) {
      throw new IllegalArgumentException("maxStamp is smaller than minStamp");
    }
  }

  /**
   * @return the smallest timestamp that should be considered
   */
  public long getMin() {
    return minStamp;
  }

  /**
   * @return the biggest timestamp that should be considered
   */
  public long getMax() {
    return maxStamp;
  }

  /**
   * Check if it is for all time
   * @return true if it is for all time
   */
  public boolean isAllTime() {
    return allTime;
  }

  /**
   * Check if the specified timestamp is within this TimeRange.
   * <p>
   * Returns true if within interval [minStamp, maxStamp), false if not.
   * @param bytes  timestamp to check
   * @param offset offset into the bytes
   * @return true if within TimeRange, false if not
   * @deprecated This is made @InterfaceAudience.Private in the 2.0 line and above and may be
   *             changed to private or removed in 3.0. Use {@link #withinTimeRange(long)} instead
   */
  @Deprecated
  public boolean withinTimeRange(byte[] bytes, int offset) {
    if (allTime) {
      return true;
    }
    return withinTimeRange(Bytes.toLong(bytes, offset));
  }

  /**
   * Check if the specified timestamp is within this TimeRange.
   * <p>
   * Returns true if within interval [minStamp, maxStamp), false if not.
   * @param timestamp timestamp to check
   * @return true if within TimeRange, false if not
   */
  public boolean withinTimeRange(long timestamp) {
    assert timestamp >= 0;
    if (this.allTime) {
      return true;
    }
    // check if >= minStamp
    return (minStamp <= timestamp && timestamp < maxStamp);
  }

  /**
   * Check if the range has any overlap with TimeRange
   * @param tr TimeRange
   * @return True if there is overlap, false otherwise
   */
  // This method came from TimeRangeTracker. We used to go there for this function but better
  // to come here to the immutable, unsynchronized datastructure at read time.
  public boolean includesTimeRange(final TimeRange tr) {
    if (this.allTime) {
      return true;
    }
    assert tr.getMin() >= 0;
    return getMin() < tr.getMax() && getMax() >= tr.getMin();
  }

  /**
   * Check if the specified timestamp is within or after this TimeRange.
   * <p>
   * Returns true if greater than minStamp, false if not.
   * @param timestamp timestamp to check
   * @return true if within or after TimeRange, false if not
   */
  public boolean withinOrAfterTimeRange(long timestamp) {
    assert timestamp >= 0;
    if (allTime) {
      return true;
    }
    // check if >= minStamp
    return timestamp >= minStamp;
  }

  /**
   * Compare the timestamp to timerange.
   * @return -1 if timestamp is less than timerange, 0 if timestamp is within timerange, 1 if
   *         timestamp is greater than timerange
   */
  public int compare(long timestamp) {
    assert timestamp >= 0;
    if (this.allTime) {
      return 0;
    }
    if (timestamp < minStamp) {
      return -1;
    }
    return timestamp >= maxStamp ? 1 : 0;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("maxStamp=");
    sb.append(this.maxStamp);
    sb.append(", minStamp=");
    sb.append(this.minStamp);
    return sb.toString();
  }
}
