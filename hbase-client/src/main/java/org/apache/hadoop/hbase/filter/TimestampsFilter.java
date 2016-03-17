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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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

  private final boolean canHint;
  TreeSet<Long> timestamps;
  private static final int MAX_LOG_TIMESTAMPS = 5;

  // Used during scans to hint the scan to stop early
  // once the timestamps fall below the minTimeStamp.
  long minTimeStamp = Long.MAX_VALUE;

  /**
   * Constructor for filter that retains only the specified timestamps in the list.
   * @param timestamps
   */
  public TimestampsFilter(List<Long> timestamps) {
    this(timestamps, false);
  }

  /**
   * Constructor for filter that retains only those
   * cells whose timestamp (version) is in the specified
   * list of timestamps.
   *
   * @param timestamps list of timestamps that are wanted.
   * @param canHint should the filter provide a seek hint? This can skip
   *                past delete tombstones, so it should only be used when that
   *                is not an issue ( no deletes, or don't care if data
   *                becomes visible)
   */
  public TimestampsFilter(List<Long> timestamps, boolean canHint) {
    for (Long timestamp : timestamps) {
      Preconditions.checkArgument(timestamp >= 0, "must be positive %s", timestamp);
    }
    this.canHint = canHint;
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
  public boolean filterRowKey(Cell cell) throws IOException {
    // Impl in FilterBase might do unnecessary copy for Off heap backed Cells.
    return false;
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
    return canHint ? ReturnCode.SEEK_NEXT_USING_HINT : ReturnCode.SKIP;
  }


  /**
   * Pick the next cell that the scanner should seek to. Since this can skip any number of cells
   * any of which can be a delete this can resurect old data.
   *
   * The method will only be used if canHint was set to true while creating the filter.
   *
   * @throws IOException This will never happen.
   */
  public Cell getNextCellHint(Cell currentCell) throws IOException {
    if (!canHint) {
      return null;
    }

    Long nextTimestampObject = timestamps.lower(currentCell.getTimestamp());

    if (nextTimestampObject == null) {
      // This should only happen if the current column's
      // timestamp is below the last one in the list.
      //
      // It should never happen as the filterKeyValue should return NEXT_COL
      // but it's always better to be extra safe and protect against future
      // behavioral changes.

      return CellUtil.createLastOnRowCol(currentCell);
    }

    // Since we know the nextTimestampObject isn't null here there must still be
    // timestamps that can be included. Cast the Long to a long and return the
    // a cell with the current row/cf/col and the next found timestamp.
    long nextTimestamp = nextTimestampObject;
    return CellUtil.createFirstOnRowColTS(currentCell, nextTimestamp);
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
  public byte[] toByteArray() {
    FilterProtos.TimestampsFilter.Builder builder =
        FilterProtos.TimestampsFilter.newBuilder();
    builder.addAllTimestamps(this.timestamps);
    builder.setCanHint(canHint);
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link TimestampsFilter} instance
   *
   * @return An instance of {@link TimestampsFilter} made from <code>bytes</code>
   * @see #toByteArray
   */
  public static TimestampsFilter parseFrom(final byte[] pbBytes)
      throws DeserializationException {
    FilterProtos.TimestampsFilter proto;
    try {
      proto = FilterProtos.TimestampsFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new TimestampsFilter(proto.getTimestampsList(),
        proto.hasCanHint() && proto.getCanHint());
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

    return String.format("%s (%d/%d): [%s] canHint: [%b]", this.getClass().getSimpleName(),
        count, this.timestamps.size(), tsList.toString(), canHint);
  }
}
