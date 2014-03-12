/*
 * Copyright 2009 The Apache Software Foundation
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import org.apache.hadoop.hbase.util.Bytes;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

/**
 * Represents an interval of version timestamps.
 * <p>
 * Evaluated according to minStamp <= timestamp < maxStamp
 * or [minStamp,maxStamp) in interval notation.
 * <p>
 * Only used internally; should not be accessed directly by clients.
 */
@ThriftStruct
public class TimeRange implements Writable {
  @ThriftField(1)
  public boolean allTime = false;

  @ThriftField(2)
  public long minStamp = 0L;

  @ThriftField(3)
  public long maxStamp = Long.MAX_VALUE;

  /**
   * Default constructor.
   * Represents interval [0, Long.MAX_VALUE) (allTime)
   */
  public TimeRange() {
  }

  /**
   * Represents interval [minStamp, Long.MAX_VALUE)
   * @param minStamp the minimum timestamp value, inclusive
   */
  public TimeRange(long minStamp) {
    this.minStamp = minStamp;
  }

  /**
   * Represents interval [minStamp, Long.MAX_VALUE)
   * @param minStamp the minimum timestamp value, inclusive
   */
  public TimeRange(byte [] minStamp) {
    this.minStamp = Bytes.toLong(minStamp);
  }

  public TimeRange(final long minStamp, final long maxStamp) throws IOException {
    this(false, minStamp, maxStamp);
  }


  /**
   * Thrift Constructor
   * Represents interval [minStamp, maxStamp)
   * @param minStamp the minimum timestamp, inclusive
   * @param maxStamp the maximum timestamp, exclusive
   * @throws IOException
   */
  @ThriftConstructor
  public TimeRange(
    @ThriftField(1) final boolean allTime,
    @ThriftField(2) final long minStamp,
    @ThriftField(3) final long maxStamp
    )
  throws IOException {
    if(maxStamp < minStamp) {
      throw new IOException("maxStamp is smaller than minStamp");
    }
    this.allTime = allTime;
    this.minStamp = minStamp;
    this.maxStamp = maxStamp;
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

  //Writable
  public void readFields(final DataInput in) throws IOException {
    this.minStamp = in.readLong();
    this.maxStamp = in.readLong();
    this.allTime = in.readBoolean();
  }

  public void write(final DataOutput out) throws IOException {
    out.writeLong(minStamp);
    out.writeLong(maxStamp);
    out.writeBoolean(this.allTime);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (allTime ? 1231 : 1237);
    result = prime * result + (int) (maxStamp ^ (maxStamp >>> 32));
    result = prime * result + (int) (minStamp ^ (minStamp >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    TimeRange other = (TimeRange) obj;
    if (allTime != other.allTime)
      return false;
    if (maxStamp != other.maxStamp)
      return false;
    if (minStamp != other.minStamp)
      return false;
    return true;
  }
}
