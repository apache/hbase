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
package org.apache.hadoop.hbase.filter;

import java.util.ArrayList;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A filter, based on the ColumnCountGetFilter, takes two arguments: limit and offset.
 * This filter can be used for row-based indexing, where references to other tables are stored across many columns,
 * in order to efficient lookups and paginated results for end users. Only most recent versions are considered
 * for pagination.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ColumnPaginationFilter extends FilterBase
{
  private int limit = 0;
  private int offset = -1;
  private byte[] columnOffset = null;
  private int count = 0;

  /**
   * Initializes filter with an integer offset and limit. The offset is arrived at
   * scanning sequentially and skipping entries. @limit number of columns are
   * then retrieved. If multiple column families are involved, the columns may be spread
   * across them.
   *
   * @param limit Max number of columns to return.
   * @param offset The integer offset where to start pagination.
   */
  public ColumnPaginationFilter(final int limit, final int offset)
  {
    Preconditions.checkArgument(limit >= 0, "limit must be positive %s", limit);
    Preconditions.checkArgument(offset >= 0, "offset must be positive %s", offset);
    this.limit = limit;
    this.offset = offset;
  }

  /**
   * Initializes filter with a string/bookmark based offset and limit. The offset is arrived
   * at, by seeking to it using scanner hints. If multiple column families are involved,
   * pagination starts at the first column family which contains @columnOffset. Columns are
   * then retrieved sequentially upto @limit number of columns which maybe spread across
   * multiple column families, depending on how the scan is setup.
   *
   * @param limit Max number of columns to return.
   * @param columnOffset The string/bookmark offset on where to start pagination.
   */
  public ColumnPaginationFilter(final int limit, final byte[] columnOffset) {
    Preconditions.checkArgument(limit >= 0, "limit must be positive %s", limit);
    Preconditions.checkArgument(columnOffset != null,
                                "columnOffset must be non-null %s",
                                columnOffset);
    this.limit = limit;
    this.columnOffset = columnOffset;
  }

  /**
   * @return limit
   */
  public int getLimit() {
    return limit;
  }

  /**
   * @return offset
   */
  public int getOffset() {
    return offset;
  }

  /**
   * @return columnOffset
   */
  public byte[] getColumnOffset() {
    return columnOffset;
  }

  @Override
  public ReturnCode filterKeyValue(Cell v)
  {
    if (columnOffset != null) {
      if (count >= limit) {
        return ReturnCode.NEXT_ROW;
      }
      byte[] buffer = v.getQualifierArray();
      if (buffer == null) {
        return ReturnCode.SEEK_NEXT_USING_HINT;
      }
      int cmp = 0;
      // Only compare if no KV's have been seen so far.
      if (count == 0) {
        cmp = Bytes.compareTo(buffer,
                              v.getQualifierOffset(),
                              v.getQualifierLength(),
                              this.columnOffset,
                              0,
                              this.columnOffset.length);
      }
      if (cmp < 0) {
        return ReturnCode.SEEK_NEXT_USING_HINT;
      } else {
        count++;
        return ReturnCode.INCLUDE_AND_NEXT_COL;
      }
    } else {
      if (count >= offset + limit) {
        return ReturnCode.NEXT_ROW;
      }

      ReturnCode code = count < offset ? ReturnCode.NEXT_COL :
                                         ReturnCode.INCLUDE_AND_NEXT_COL;
      count++;
      return code;
    }
  }

  @Override
  public Cell getNextCellHint(Cell cell) {
    return KeyValueUtil.createFirstOnRow(
        cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), cell.getFamilyArray(),
        cell.getFamilyOffset(), cell.getFamilyLength(), columnOffset, 0, columnOffset.length);
  }

  @Override
  public void reset()
  {
    this.count = 0;
  }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    Preconditions.checkArgument(filterArguments.size() == 2,
                                "Expected 2 but got: %s", filterArguments.size());
    int limit = ParseFilter.convertByteArrayToInt(filterArguments.get(0));
    int offset = ParseFilter.convertByteArrayToInt(filterArguments.get(1));
    return new ColumnPaginationFilter(limit, offset);
  }

  /**
   * @return The filter serialized using pb
   */
  public byte [] toByteArray() {
    FilterProtos.ColumnPaginationFilter.Builder builder =
      FilterProtos.ColumnPaginationFilter.newBuilder();
    builder.setLimit(this.limit);
    if (this.offset >= 0) {
      builder.setOffset(this.offset);
    }
    if (this.columnOffset != null) {
      builder.setColumnOffset(ByteStringer.wrap(this.columnOffset));
    }
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link ColumnPaginationFilter} instance
   * @return An instance of {@link ColumnPaginationFilter} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static ColumnPaginationFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    FilterProtos.ColumnPaginationFilter proto;
    try {
      proto = FilterProtos.ColumnPaginationFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    if (proto.hasColumnOffset()) {
      return new ColumnPaginationFilter(proto.getLimit(),
                                        proto.getColumnOffset().toByteArray());
    }
    return new ColumnPaginationFilter(proto.getLimit(),proto.getOffset());
  }

  /**
   * @param other
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof ColumnPaginationFilter)) return false;

    ColumnPaginationFilter other = (ColumnPaginationFilter)o;
    if (this.columnOffset != null) {
      return this.getLimit() == this.getLimit() &&
          Bytes.equals(this.getColumnOffset(), other.getColumnOffset());
    }
    return this.getLimit() == other.getLimit() && this.getOffset() == other.getOffset();
  }

  @Override
  public String toString() {
    if (this.columnOffset != null) {
      return (this.getClass().getSimpleName() + "(" + this.limit + ", " +
          Bytes.toStringBinary(this.columnOffset) + ")");
    }
    return String.format("%s (%d, %d)", this.getClass().getSimpleName(),
        this.limit, this.offset);
  }
}
