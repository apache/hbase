/**
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
package org.apache.hadoop.hbase.regionserver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;

/**
 * Stores minimum and maximum timestamp values. Both timestamps are inclusive.
 * Use this class at write-time ONLY. Too much synchronization to use at read time
 * (TODO: there are two scenarios writing, once when lots of concurrency as part of memstore
 * updates but then later we can make one as part of a compaction when there is only one thread
 * involved -- consider making different version, the synchronized and the unsynchronized).
 * Use {@link TimeRange} at read time instead of this. See toTimeRange() to make TimeRange to use.
 * MemStores use this class to track minimum and maximum timestamps. The TimeRangeTracker made by
 * the MemStore is passed to the StoreFile for it to write out as part a flush in the the file
 * metadata. If no memstore involved -- i.e. a compaction -- then the StoreFile will calculate its
 * own TimeRangeTracker as it appends. The StoreFile serialized TimeRangeTracker is used
 * at read time via an instance of {@link TimeRange} to test if Cells fit the StoreFile TimeRange.
 */
@InterfaceAudience.Private
public class TimeRangeTracker implements Writable {
  static final long INITIAL_MIN_TIMESTAMP = Long.MAX_VALUE;
  long minimumTimestamp = INITIAL_MIN_TIMESTAMP;
  static final long INITIAL_MAX_TIMESTAMP = -1;
  long maximumTimestamp = INITIAL_MAX_TIMESTAMP;

  /**
   * Default constructor.
   * Initializes TimeRange to be null
   */
  public TimeRangeTracker() {

  }

  /**
   * Copy Constructor
   * @param trt source TimeRangeTracker
   */
  public TimeRangeTracker(final TimeRangeTracker trt) {
    this.minimumTimestamp = trt.getMin();
    this.maximumTimestamp = trt.getMax();
  }

  public TimeRangeTracker(long minimumTimestamp, long maximumTimestamp) {
    this.minimumTimestamp = minimumTimestamp;
    this.maximumTimestamp = maximumTimestamp;
  }

  /**
   * Update the current TimestampRange to include the timestamp from KeyValue
   * If the Key is of type DeleteColumn or DeleteFamily, it includes the
   * entire time range from 0 to timestamp of the key.
   * @param kv the KeyValue to include
   */
  public void includeTimestamp(final KeyValue kv) {
    includeTimestamp(kv.getTimestamp());
    if (kv.isDeleteColumnOrFamily()) {
      includeTimestamp(0);
    }
  }

  /**
   * Update the current TimestampRange to include the timestamp from Key.
   * If the Key is of type DeleteColumn or DeleteFamily, it includes the
   * entire time range from 0 to timestamp of the key.
   * @param key
   */
  public void includeTimestamp(final byte[] key) {
    includeTimestamp(Bytes.toLong(key,key.length-KeyValue.TIMESTAMP_TYPE_SIZE));
    int type = key[key.length - 1];
    if (type == Type.DeleteColumn.getCode() ||
        type == Type.DeleteFamily.getCode()) {
      includeTimestamp(0);
    }
  }

  /**
   * If required, update the current TimestampRange to include timestamp
   * @param timestamp the timestamp value to include
   */
  synchronized void includeTimestamp(final long timestamp) {
    if (maximumTimestamp == -1) {
      minimumTimestamp = timestamp;
      maximumTimestamp = timestamp;
    }
    else if (minimumTimestamp > timestamp) {
      minimumTimestamp = timestamp;
    }
    else if (maximumTimestamp < timestamp) {
      maximumTimestamp = timestamp;
    }
    return;
  }

  /**
   * Check if the range has any overlap with TimeRange
   * @param tr TimeRange
   * @return True if there is overlap, false otherwise
   */
  public synchronized boolean includesTimeRange(final TimeRange tr) {
    return (this.minimumTimestamp < tr.getMax() &&
        this.maximumTimestamp >= tr.getMin());
  }

  /**
   * @return the minimumTimestamp
   */
  public synchronized long getMin() {
    return minimumTimestamp;
  }

  /**
   * @return the maximumTimestamp
   */
  public synchronized long getMax() {
    return maximumTimestamp;
  }

  public synchronized void write(final DataOutput out) throws IOException {
    out.writeLong(minimumTimestamp);
    out.writeLong(maximumTimestamp);
  }

  public synchronized void readFields(final DataInput in) throws IOException {
    this.minimumTimestamp = in.readLong();
    this.maximumTimestamp = in.readLong();
  }

  @Override
  public synchronized String toString() {
    return "[" + minimumTimestamp + "," + maximumTimestamp + "]";
  }

  /**
   * @return An instance of TimeRangeTracker filled w/ the content of serialized
   * TimeRangeTracker in <code>timeRangeTrackerBytes</code>.
   * @throws IOException
   */
  public static TimeRangeTracker getTimeRangeTracker(final byte [] timeRangeTrackerBytes)
      throws IOException {
    if (timeRangeTrackerBytes == null) return null;
    TimeRangeTracker trt = new TimeRangeTracker();
    Writables.copyWritable(timeRangeTrackerBytes, trt);
    return trt;
  }

  /**
   * @return An instance of a TimeRange made from the serialized TimeRangeTracker passed in
   * <code>timeRangeTrackerBytes</code>.
   * @throws IOException
   */
  static TimeRange getTimeRange(final byte [] timeRangeTrackerBytes) throws IOException {
    TimeRangeTracker trt = getTimeRangeTracker(timeRangeTrackerBytes);
    return trt == null? null: trt.toTimeRange();
  }

  private boolean isFreshInstance() {
    return getMin() == INITIAL_MIN_TIMESTAMP && getMax() == INITIAL_MAX_TIMESTAMP;
  }

  /**
   * @return Make a TimeRange from current state of <code>this</code>.
   */
  TimeRange toTimeRange() throws IOException {
    long min = getMin();
    long max = getMax();
    // Check for the case where the TimeRangeTracker is fresh. In that case it has
    // initial values that are antithetical to a TimeRange... Return an uninitialized TimeRange
    // if passed an uninitialized TimeRangeTracker.
    if (isFreshInstance()) {
      return new TimeRange();
    }
    return new TimeRange(min, max);
  }
}
