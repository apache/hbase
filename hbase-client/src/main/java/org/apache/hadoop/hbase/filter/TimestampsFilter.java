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
package org.apache.hadoop.hbase.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Filter that returns only cells whose timestamp (version) is
 * in the specified list of timestamps (versions).
 * <p>
 * Note: Use of this filter overrides any time range/time stamp
 * options specified using {@link org.apache.hadoop.hbase.client.Get#setTimeRange(long, long)},
 * {@link org.apache.hadoop.hbase.client.Scan#setTimeRange(long, long)}, {@link org.apache.hadoop.hbase.client.Get#setTimeStamp(long)},
 * or {@link org.apache.hadoop.hbase.client.Scan#setTimeStamp(long)}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TimestampsFilter extends FilterBase {

  TreeSet<Long> timestamps;
  private static final int MAX_LOG_TIMESTAMPS = 5;

  // Used during scans to hint the scan to stop early
  // once the timestamps fall below the minTimeStamp.
  long minTimeStamp = Long.MAX_VALUE;

  /**
   * Constructor for filter that retains only those
   * cells whose timestamp (version) is in the specified
   * list of timestamps.
   *
   * @param timestamps
   */
  public TimestampsFilter(List<Long> timestamps) {
    for (Long timestamp : timestamps) {
      Preconditions.checkArgument(timestamp >= 0, "must be positive %s", timestamp);
    }
    this.timestamps = new TreeSet<Long>(timestamps);
    init();
  }

  /**
   * @return the list of timestamps
   */
  public List<Long> getTimestamps() {
    List<Long> list = new ArrayList<Long>(timestamps.size());
    list.addAll(timestamps);
    return list;
  }

  private void init() {
    if (this.timestamps.size() > 0) {
      minTimeStamp = this.timestamps.first();
    }
  }

  /**
   * Gets the minimum timestamp requested by filter.
   * @return  minimum timestamp requested by filter.
   */
  public long getMin() {
    return minTimeStamp;
  }

  @Override
  public ReturnCode filterKeyValue(Cell v) {
    if (this.timestamps.contains(v.getTimestamp())) {
      return ReturnCode.INCLUDE;
    } else if (v.getTimestamp() < minTimeStamp) {
      // The remaining versions of this column are guaranteed
      // to be lesser than all of the other values.
      return ReturnCode.NEXT_COL;
    }
    return ReturnCode.SKIP;
  }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    ArrayList<Long> timestamps = new ArrayList<Long>();
    for (int i = 0; i<filterArguments.size(); i++) {
      long timestamp = ParseFilter.convertByteArrayToLong(filterArguments.get(i));
      timestamps.add(timestamp);
    }
    return new TimestampsFilter(timestamps);
  }

  /**
   * @return The filter serialized using pb
   */
  public byte [] toByteArray() {
    FilterProtos.TimestampsFilter.Builder builder =
      FilterProtos.TimestampsFilter.newBuilder();
    builder.addAllTimestamps(this.timestamps);
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link TimestampsFilter} instance
   * @return An instance of {@link TimestampsFilter} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static TimestampsFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    FilterProtos.TimestampsFilter proto;
    try {
      proto = FilterProtos.TimestampsFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new TimestampsFilter(proto.getTimestampsList());
  }

  /**
   * @param other
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof TimestampsFilter)) return false;

    TimestampsFilter other = (TimestampsFilter)o;
    return this.getTimestamps().equals(other.getTimestamps());
  }

  @Override
  public String toString() {
    return toString(MAX_LOG_TIMESTAMPS);
  }

  protected String toString(int maxTimestamps) {
    StringBuilder tsList = new StringBuilder();

    int count = 0;
    for (Long ts : this.timestamps) {
      if (count >= maxTimestamps) {
        break;
      }
      ++count;
      tsList.append(ts.toString());
      if (count < this.timestamps.size() && count < maxTimestamps) {
        tsList.append(", ");
      }
    }

    return String.format("%s (%d/%d): [%s]", this.getClass().getSimpleName(),
        count, this.timestamps.size(), tsList.toString());
  }
}
