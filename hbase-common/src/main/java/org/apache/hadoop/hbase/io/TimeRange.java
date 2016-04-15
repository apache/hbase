/*
 *
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

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Represents an interval of version timestamps.
 * <p>
 * Evaluated according to minStamp <= timestamp < maxStamp
 * or [minStamp,maxStamp) in interval notation.
 * <p>
 * Only used internally; should not be accessed directly by clients.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TimeRange {
  static final long INITIAL_MIN_TIMESTAMP = 0L;
  private static final long MIN_TIME = INITIAL_MIN_TIMESTAMP;
  static final long INITIAL_MAX_TIMESTAMP = Long.MAX_VALUE;
  static final long MAX_TIME = INITIAL_MAX_TIMESTAMP;
  private long minStamp = MIN_TIME;
  private long maxStamp = MAX_TIME;
  private final boolean allTime;

  /**
   * Default constructor.
   * Represents interval [0, Long.MAX_VALUE) (allTime)
   */
  public TimeRange() {
    allTime = true;
  }

  /**
   * Represents interval [minStamp, Long.MAX_VALUE)
   * @param minStamp the minimum timestamp value, inclusive
   */
  public TimeRange(long minStamp) {
    this.minStamp = minStamp;
    this.allTime = this.minStamp == MIN_TIME;
  }

  /**
   * Represents interval [minStamp, Long.MAX_VALUE)
   * @param minStamp the minimum timestamp value, inclusive
   */
  public TimeRange(byte [] minStamp) {
  	this.minStamp = Bytes.toLong(minStamp);
  	this.allTime = false;
  }

  /**
   * Represents interval [minStamp, maxStamp)
   * @param minStamp the minimum timestamp, inclusive
   * @param maxStamp the maximum timestamp, exclusive
   * @throws IOException
   */
  public TimeRange(long minStamp, long maxStamp)
  throws IOException {
    if (minStamp < 0 || maxStamp < 0) {
      throw new IllegalArgumentException("Timestamp cannot be negative. minStamp:" + minStamp
        + ", maxStamp" + maxStamp);
    }
    if (maxStamp < minStamp) {
      throw new IOException("maxStamp is smaller than minStamp");
    }
    this.minStamp = minStamp;
    this.maxStamp = maxStamp;
    this.allTime = this.minStamp == MIN_TIME && this.maxStamp == MAX_TIME;
  }

  /**
   * Represents interval [minStamp, maxStamp)
   * @param minStamp the minimum timestamp, inclusive
   * @param maxStamp the maximum timestamp, exclusive
   * @throws IOException
   */
  public TimeRange(byte [] minStamp, byte [] maxStamp)
  throws IOException {
    this(Bytes.toLong(minStamp), Bytes.toLong(maxStamp));
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
   * Returns true if within interval [minStamp, maxStamp), false
   * if not.
   * @param bytes timestamp to check
   * @param offset offset into the bytes
   * @return true if within TimeRange, false if not
   */
  public boolean withinTimeRange(byte [] bytes, int offset) {
  	if (allTime) {
  	  return true;
  	}
  	return withinTimeRange(Bytes.toLong(bytes, offset));
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
    return getMin() < tr.getMax() && getMax() >= tr.getMin();
  }

  /**
   * Check if the specified timestamp is within this TimeRange.
   * <p>
   * Returns true if within interval [minStamp, maxStamp), false
   * if not.
   * @param timestamp timestamp to check
   * @return true if within TimeRange, false if not
   */
  public boolean withinTimeRange(long timestamp) {
  	if(allTime) return true;
  	// check if >= minStamp
  	return (minStamp <= timestamp && timestamp < maxStamp);
  }

  /**
   * Check if the specified timestamp is within this TimeRange.
   * <p>
   * Returns true if within interval [minStamp, maxStamp), false
   * if not.
   * @param timestamp timestamp to check
   * @return true if within TimeRange, false if not
   */
  public boolean withinOrAfterTimeRange(long timestamp) {
    if(allTime) return true;
    // check if >= minStamp
    return (timestamp >= minStamp);
  }

  /**
   * Compare the timestamp to timerange
   * @param timestamp
   * @return -1 if timestamp is less than timerange,
   * 0 if timestamp is within timerange,
   * 1 if timestamp is greater than timerange
   */
  public int compare(long timestamp) {
    if (timestamp < minStamp) {
      return -1;
    } else if (timestamp >= maxStamp) {
      return 1;
    } else {
      return 0;
    }
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
