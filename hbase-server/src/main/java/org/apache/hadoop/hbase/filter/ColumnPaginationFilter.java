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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.DeserializationException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;

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
  private int offset = 0;
  private int count = 0;

  public ColumnPaginationFilter(final int limit, final int offset)
  {
    Preconditions.checkArgument(limit >= 0, "limit must be positive %s", limit);
    Preconditions.checkArgument(offset >= 0, "offset must be positive %s", offset);
    this.limit = limit;
    this.offset = offset;
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

  @Override
  public ReturnCode filterKeyValue(KeyValue v)
  {
    if(count >= offset + limit)
    {
      return ReturnCode.NEXT_ROW;
    }

    ReturnCode code = count < offset ? ReturnCode.NEXT_COL :
                                       ReturnCode.INCLUDE_AND_NEXT_COL;
    count++;
    return code;
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
    builder.setOffset(this.offset);
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link ColumnPaginationFilter} instance
   * @return An instance of {@link ColumnPaginationFilter} made from <code>bytes</code>
   * @throws DeserializationException
   * @see {@link #toByteArray()}
   */
  public static ColumnPaginationFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    FilterProtos.ColumnPaginationFilter proto;
    try {
      proto = FilterProtos.ColumnPaginationFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
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
    return this.getLimit() == other.getLimit() && this.getOffset() == other.getOffset();
  }

  @Override
  public String toString() {
    return String.format("%s (%d, %d)", this.getClass().getSimpleName(),
        this.limit, this.offset);
  }
}
