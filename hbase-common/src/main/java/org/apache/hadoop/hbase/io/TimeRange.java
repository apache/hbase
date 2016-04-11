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
 * Evaluated according to minStamp &lt;= timestamp &lt; maxStamp
 * or [minStamp,maxStamp) in interval notation.
 * <p>
 * Only used internally; should not be accessed directly by clients.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TimeRange {
  private static final long MIN_TIME = 0L;
  private static final long MAX_TIME = Long.MAX_VALUE;
  private long minStamp = MIN_TIME;
  private long maxStamp = MAX_TIME;
  private boolean allTime = false;

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
    if (this.minStamp == MIN_TIME){
      this.allTime = true;
    }
  }

  /**
   * Represents interval [minStamp, Long.MAX_VALUE)
   * @param minStamp the minimum timestamp value, inclusive
   */
  public TimeRange(byte [] minStamp) {
    this.minStamp = Bytes.toLong(minStamp);
  }

  /**
   * Represents interval [minStamp, maxStamp)
   * @param minStamp the minimum timestamp, inclusive
   * @param maxStamp the maximum timestamp, exclusive
   * @throws IllegalArgumentException
   */
  public TimeRange(long minStamp, long maxStamp) {
    if (minStamp < 0 || maxStamp < 0) {
      throw new IllegalArgumentException("Timestamp cannot be negative. minStamp:" + minStamp
        + ", maxStamp:" + maxStamp);
    }
    if(maxStamp < minStamp) {
      throw new IllegalArgumentException("maxStamp is smaller than minStamp");
    }
    this.minStamp = minStamp;
    this.maxStamp = maxStamp;
    if (this.minStamp == MIN_TIME && this.maxStamp == MAX_TIME){
      this.allTime = true;
    }
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
    if(allTime) return true;
    return withinTimeRange(Bytes.toLong(bytes, offset));
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
    if (allTime) return 0;
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
