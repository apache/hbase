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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.io.Writable;

/**
 * Stores the minimum and maximum timestamp values (both are inclusive).
 * Can be used to find if any given time range overlaps with its time range
 * MemStores use this class to track its minimum and maximum timestamps.
 * When writing StoreFiles, this information is stored in meta blocks and used
 * at read time to match against the required TimeRange.
 */
@InterfaceAudience.Private
public class TimeRangeTracker implements Writable {
  static final long INITIAL_MINIMUM_TIMESTAMP = Long.MAX_VALUE;
  long minimumTimestamp = INITIAL_MINIMUM_TIMESTAMP;
  long maximumTimestamp = -1;

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
    set(trt.getMinimumTimestamp(), trt.getMaximumTimestamp());
  }

  public TimeRangeTracker(long minimumTimestamp, long maximumTimestamp) {
    set(minimumTimestamp, maximumTimestamp);
  }

  private void set(final long min, final long max) {
    this.minimumTimestamp = min;
    this.maximumTimestamp = max;
  }

  /**
   * @param l
   * @return True if we initialized values
   */
  private boolean init(final long l) {
    if (this.minimumTimestamp != INITIAL_MINIMUM_TIMESTAMP) return false;
    set(l, l);
    return true;
  }

  /**
   * Update the current TimestampRange to include the timestamp from Cell
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
    // Do test outside of synchronization block.  Synchronization in here can be problematic
    // when many threads writing one Store -- they can all pile up trying to add in here.
    // Happens when doing big write upload where we are hammering on one region.
    if (timestamp < this.minimumTimestamp) {
      synchronized (this) {
        if (!init(timestamp)) {
          if (timestamp < this.minimumTimestamp) {
            this.minimumTimestamp = timestamp;
          }
        }
      }
    } else if (timestamp > this.maximumTimestamp) {
      synchronized (this) {
        if (!init(timestamp)) {
          if (this.maximumTimestamp < timestamp) {
            this.maximumTimestamp =  timestamp;
          }
        }
      }
    }
  }

  /**
   * Check if the range has any overlap with TimeRange
   * @param tr TimeRange
   * @return True if there is overlap, false otherwise
   */
  public synchronized boolean includesTimeRange(final TimeRange tr) {
    return (this.minimumTimestamp < tr.getMax() && this.maximumTimestamp >= tr.getMin());
  }

  /**
   * @return the minimumTimestamp
   */
  public synchronized long getMinimumTimestamp() {
    return minimumTimestamp;
  }

  /**
   * @return the maximumTimestamp
   */
  public synchronized long getMaximumTimestamp() {
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
}