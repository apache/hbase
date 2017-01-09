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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;
/**
 * Stores minimum and maximum timestamp values, it is [minimumTimestamp, maximumTimestamp] in
 * interval notation.
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
  static final long INITIAL_MAX_TIMESTAMP = -1L;

  AtomicLong minimumTimestamp = new AtomicLong(INITIAL_MIN_TIMESTAMP);
  AtomicLong maximumTimestamp = new AtomicLong(INITIAL_MAX_TIMESTAMP);

  /**
   * Default constructor.
   * Initializes TimeRange to be null
   */
  public TimeRangeTracker() {}

  /**
   * Copy Constructor
   * @param trt source TimeRangeTracker
   */
  public TimeRangeTracker(final TimeRangeTracker trt) {
    minimumTimestamp.set(trt.getMin());
    maximumTimestamp.set(trt.getMax());
  }

  public TimeRangeTracker(long minimumTimestamp, long maximumTimestamp) {
    this.minimumTimestamp.set(minimumTimestamp);
    this.maximumTimestamp.set(maximumTimestamp);
  }

  /**
   * Update the current TimestampRange to include the timestamp from <code>cell</code>.
   * If the Key is of type DeleteColumn or DeleteFamily, it includes the
   * entire time range from 0 to timestamp of the key.
   * @param cell the Cell to include
   */
  public void includeTimestamp(final Cell cell) {
    includeTimestamp(cell.getTimestamp());
    if (CellUtil.isDeleteColumnOrFamily(cell)) {
      includeTimestamp(0);
    }
  }

  /**
   * If required, update the current TimestampRange to include timestamp
   * @param timestamp the timestamp value to include
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="MT_CORRECTNESS",
      justification="Intentional")
  void includeTimestamp(final long timestamp) {
    long initialMinTimestamp = this.minimumTimestamp.get();
    if (timestamp < initialMinTimestamp) {
      long curMinTimestamp = initialMinTimestamp;
      while (timestamp < curMinTimestamp) {
        if (!this.minimumTimestamp.compareAndSet(curMinTimestamp, timestamp)) {
          curMinTimestamp = this.minimumTimestamp.get();
        } else {
          // successfully set minimumTimestamp, break.
          break;
        }
      }

      // When it reaches here, there are two possibilities:
      //  1). timestamp >= curMinTimestamp, someone already sets the minimumTimestamp. In this case,
      //      it still needs to check if initialMinTimestamp == INITIAL_MIN_TIMESTAMP to see
      //      if it needs to update minimumTimestamp. Someone may already set both
      //      minimumTimestamp/minimumTimestamp to the same value(curMinTimestamp),
      //      need to check if maximumTimestamp needs to be updated.
      //  2). timestamp < curMinTimestamp, it sets the minimumTimestamp successfully.
      //      In this case,it still needs to check if initialMinTimestamp == INITIAL_MIN_TIMESTAMP
      //      to see if it needs to set maximumTimestamp.
      if (initialMinTimestamp != INITIAL_MIN_TIMESTAMP) {
        // Someone already sets minimumTimestamp and timestamp is less than minimumTimestamp.
        // In this case, no need to set maximumTimestamp as it will be set to at least
        // initialMinTimestamp.
        return;
      }
    }

    long curMaxTimestamp = this.maximumTimestamp.get();

    if (timestamp > curMaxTimestamp) {
      while (timestamp > curMaxTimestamp) {
        if (!this.maximumTimestamp.compareAndSet(curMaxTimestamp, timestamp)) {
          curMaxTimestamp = this.maximumTimestamp.get();
        } else {
          // successfully set maximumTimestamp, break
          break;
        }
      }
    }
  }

  /**
   * Check if the range has ANY overlap with TimeRange
   * @param tr TimeRange, it expects [minStamp, maxStamp)
   * @return True if there is overlap, false otherwise
   */
  public boolean includesTimeRange(final TimeRange tr) {
    return (this.minimumTimestamp.get() < tr.getMax() && this.maximumTimestamp.get() >= tr.getMin());
  }

  /**
   * @return the minimumTimestamp
   */
  public long getMin() {
    return minimumTimestamp.get();
  }

  /**
   * @return the maximumTimestamp
   */
  public long getMax() {
    return maximumTimestamp.get();
  }

  public void write(final DataOutput out) throws IOException {
    out.writeLong(minimumTimestamp.get());
    out.writeLong(maximumTimestamp.get());
  }

  public void readFields(final DataInput in) throws IOException {

    this.minimumTimestamp.set(in.readLong());
    this.maximumTimestamp.set(in.readLong());
  }

  @Override
  public String toString() {
    return "[" + minimumTimestamp.get() + "," + maximumTimestamp.get() + "]";
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

  /**
   * @return Make a TimeRange from current state of <code>this</code>.
   */
  TimeRange toTimeRange() throws IOException {
    long min = getMin();
    long max = getMax();
    // Initial TimeRangeTracker timestamps are the opposite of what you want for a TimeRange. Fix!
    if (min == INITIAL_MIN_TIMESTAMP) {
      min = TimeRange.INITIAL_MIN_TIMESTAMP;
    }
    if (max == INITIAL_MAX_TIMESTAMP) {
      max = TimeRange.INITIAL_MAX_TIMESTAMP;
    }
    return new TimeRange(min, max);
  }
}