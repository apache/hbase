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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

/**
 * Stores minimum and maximum timestamp values, it is [minimumTimestamp, maximumTimestamp] in
 * interval notation.
 * Use this class at write-time ONLY. Too much synchronization to use at read time
 * Use {@link TimeRange} at read time instead of this. See toTimeRange() to make TimeRange to use.
 * MemStores use this class to track minimum and maximum timestamps. The TimeRangeTracker made by
 * the MemStore is passed to the StoreFile for it to write out as part a flush in the the file
 * metadata. If no memstore involved -- i.e. a compaction -- then the StoreFile will calculate its
 * own TimeRangeTracker as it appends. The StoreFile serialized TimeRangeTracker is used
 * at read time via an instance of {@link TimeRange} to test if Cells fit the StoreFile TimeRange.
 */
@InterfaceAudience.Private
public abstract class TimeRangeTracker {

  public enum Type {
    // thread-unsafe
    NON_SYNC,
    // thread-safe
    SYNC
  }

  static final long INITIAL_MIN_TIMESTAMP = Long.MAX_VALUE;
  static final long INITIAL_MAX_TIMESTAMP = -1L;

  public static TimeRangeTracker create(Type type) {
    switch (type) {
      case NON_SYNC:
        return new NonSyncTimeRangeTracker();
      case SYNC:
        return new SyncTimeRangeTracker();
      default:
        throw new UnsupportedOperationException("The type:" + type + " is unsupported");
    }
  }

  public static TimeRangeTracker create(Type type, TimeRangeTracker trt) {
    switch (type) {
      case NON_SYNC:
        return new NonSyncTimeRangeTracker(trt);
      case SYNC:
        return new SyncTimeRangeTracker(trt);
      default:
        throw new UnsupportedOperationException("The type:" + type + " is unsupported");
    }
  }

  public static TimeRangeTracker create(Type type, long minimumTimestamp, long maximumTimestamp) {
    switch (type) {
      case NON_SYNC:
        return new NonSyncTimeRangeTracker(minimumTimestamp, maximumTimestamp);
      case SYNC:
        return new SyncTimeRangeTracker(minimumTimestamp, maximumTimestamp);
      default:
        throw new UnsupportedOperationException("The type:" + type + " is unsupported");
    }
  }

  protected abstract void setMax(long ts);
  protected abstract void setMin(long ts);
  protected abstract boolean compareAndSetMin(long expect, long update);
  protected abstract boolean compareAndSetMax(long expect, long update);
  /**
   * Update the current TimestampRange to include the timestamp from <code>cell</code>.
   * If the Key is of type DeleteColumn or DeleteFamily, it includes the
   * entire time range from 0 to timestamp of the key.
   * @param cell the Cell to include
   */
  public void includeTimestamp(final Cell cell) {
    includeTimestamp(cell.getTimestamp());
    if (PrivateCellUtil.isDeleteColumnOrFamily(cell)) {
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
    long initialMinTimestamp = getMin();
    if (timestamp < initialMinTimestamp) {
      long curMinTimestamp = initialMinTimestamp;
      while (timestamp < curMinTimestamp) {
        if (!compareAndSetMin(curMinTimestamp, timestamp)) {
          curMinTimestamp = getMin();
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

    long curMaxTimestamp = getMax();

    if (timestamp > curMaxTimestamp) {
      while (timestamp > curMaxTimestamp) {
        if (!compareAndSetMax(curMaxTimestamp, timestamp)) {
          curMaxTimestamp = getMax();
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
    return (getMin() < tr.getMax() && getMax() >= tr.getMin());
  }

  /**
   * @return the minimumTimestamp
   */
  public abstract long getMin();

  /**
   * @return the maximumTimestamp
   */
  public abstract long getMax();

  @Override
  public String toString() {
    return "[" + getMin() + "," + getMax() + "]";
  }

  /**
   * @param data the serialization data. It can't be null!
   * @return An instance of NonSyncTimeRangeTracker filled w/ the content of serialized
   * NonSyncTimeRangeTracker in <code>timeRangeTrackerBytes</code>.
   * @throws IOException
   */
  public static TimeRangeTracker parseFrom(final byte[] data) throws IOException {
    return parseFrom(data, Type.NON_SYNC);
  }

  public static TimeRangeTracker parseFrom(final byte[] data, Type type) throws IOException {
    Preconditions.checkNotNull(data, "input data is null!");
    if (ProtobufUtil.isPBMagicPrefix(data)) {
      int pblen = ProtobufUtil.lengthOfPBMagic();
      HBaseProtos.TimeRangeTracker.Builder builder = HBaseProtos.TimeRangeTracker.newBuilder();
      ProtobufUtil.mergeFrom(builder, data, pblen, data.length - pblen);
      return TimeRangeTracker.create(type, builder.getFrom(), builder.getTo());
    } else {
      try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data))) {
        return TimeRangeTracker.create(type, in.readLong(), in.readLong());
      }
    }
  }

  /**
   * This method used to serialize TimeRangeTracker (TRT) by protobuf while this breaks the
   * forward compatibility on HFile.(See HBASE-21008) In previous hbase version ( < 2.0.0 ) we use
   * DataOutput to serialize TRT, these old versions don't have capability to deserialize TRT
   * which is serialized by protobuf. So we need to revert the change of serializing
   * TimeRangeTracker back to DataOutput. For more information, please check HBASE-21012.
   * @param tracker TimeRangeTracker needed to be serialized.
   * @return byte array filled with serialized TimeRangeTracker.
   * @throws IOException if something goes wrong in writeLong.
   */
  public static byte[] toByteArray(TimeRangeTracker tracker) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      try (DataOutputStream dos = new DataOutputStream(bos)) {
        dos.writeLong(tracker.getMin());
        dos.writeLong(tracker.getMax());
        return bos.toByteArray();
      }
    }
  }

  /**
   * @return Make a TimeRange from current state of <code>this</code>.
   */
  TimeRange toTimeRange() {
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

  //In order to estimate the heap size, this inner class need to be accessible to TestHeapSize.
  public static class NonSyncTimeRangeTracker extends TimeRangeTracker {
    private long minimumTimestamp = INITIAL_MIN_TIMESTAMP;
    private long maximumTimestamp = INITIAL_MAX_TIMESTAMP;

    NonSyncTimeRangeTracker() {
    }

    NonSyncTimeRangeTracker(final TimeRangeTracker trt) {
      this.minimumTimestamp = trt.getMin();
      this.maximumTimestamp = trt.getMax();
    }

    NonSyncTimeRangeTracker(long minimumTimestamp, long maximumTimestamp) {
      this.minimumTimestamp = minimumTimestamp;
      this.maximumTimestamp = maximumTimestamp;
    }

    @Override
    protected void setMax(long ts) {
      maximumTimestamp = ts;
    }

    @Override
    protected void setMin(long ts) {
      minimumTimestamp = ts;
    }

    @Override
    protected boolean compareAndSetMin(long expect, long update) {
      if (minimumTimestamp != expect) {
        return false;
      }
      minimumTimestamp = update;
      return true;
    }

    @Override
    protected boolean compareAndSetMax(long expect, long update) {
      if (maximumTimestamp != expect) {
        return false;
      }
      maximumTimestamp = update;
      return true;
    }

    @Override
    public long getMin() {
      return minimumTimestamp;
    }

    @Override
    public long getMax() {
      return maximumTimestamp;
    }
  }

  //In order to estimate the heap size, this inner class need to be accessible to TestHeapSize.
  public static class SyncTimeRangeTracker extends TimeRangeTracker {
    private final AtomicLong minimumTimestamp = new AtomicLong(INITIAL_MIN_TIMESTAMP);
    private final AtomicLong maximumTimestamp = new AtomicLong(INITIAL_MAX_TIMESTAMP);

    private SyncTimeRangeTracker() {
    }

    SyncTimeRangeTracker(final TimeRangeTracker trt) {
      this.minimumTimestamp.set(trt.getMin());
      this.maximumTimestamp.set(trt.getMax());
    }

    SyncTimeRangeTracker(long minimumTimestamp, long maximumTimestamp) {
      this.minimumTimestamp.set(minimumTimestamp);
      this.maximumTimestamp.set(maximumTimestamp);
    }

    @Override
    protected void setMax(long ts) {
      maximumTimestamp.set(ts);
    }

    @Override
    protected void setMin(long ts) {
      minimumTimestamp.set(ts);
    }

    @Override
    protected boolean compareAndSetMin(long expect, long update) {
      return minimumTimestamp.compareAndSet(expect, update);
    }

    @Override
    protected boolean compareAndSetMax(long expect, long update) {
      return maximumTimestamp.compareAndSet(expect, update);
    }

    @Override
    public long getMin() {
      return minimumTimestamp.get();
    }

    @Override
    public long getMax() {
      return maximumTimestamp.get();
    }
  }
}
