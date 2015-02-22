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
package org.apache.hadoop.hbase.filter;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
/**
 * Implementation of Filter interface that limits results to a specific page
 * size. It terminates scanning once the number of filter-passed rows is >
 * the given page size.
 * <p>
 * Note that this filter cannot guarantee that the number of results returned
 * to a client are <= page size. This is because the filter is applied
 * separately on different region servers. It does however optimize the scan of
 * individual HRegions by making sure that the page size is never exceeded
 * locally.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class PageFilter extends FilterBase {
  private long pageSize = Long.MAX_VALUE;
  private int rowsAccepted = 0;

  /**
   * Constructor that takes a maximum page size.
   *
   * @param pageSize Maximum result size.
   */
  public PageFilter(final long pageSize) {
    Preconditions.checkArgument(pageSize >= 0, "must be positive %s", pageSize);
    this.pageSize = pageSize;
  }

  public long getPageSize() {
    return pageSize;
  }

  @Override
  public ReturnCode filterKeyValue(Cell ignored) throws IOException {
    return ReturnCode.INCLUDE;
  }
  
  public boolean filterAllRemaining() {
    return this.rowsAccepted >= this.pageSize;
  }

  public boolean filterRow() {
    this.rowsAccepted++;
    return this.rowsAccepted > this.pageSize;
  }
  
  public boolean hasFilterRow() {
    return true;
  }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    Preconditions.checkArgument(filterArguments.size() == 1,
                                "Expected 1 but got: %s", filterArguments.size());
    long pageSize = ParseFilter.convertByteArrayToLong(filterArguments.get(0));
    return new PageFilter(pageSize);
  }

  /**
   * @return The filter serialized using pb
   */
  public byte [] toByteArray() {
    FilterProtos.PageFilter.Builder builder =
      FilterProtos.PageFilter.newBuilder();
    builder.setPageSize(this.pageSize);
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link PageFilter} instance
   * @return An instance of {@link PageFilter} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static PageFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    FilterProtos.PageFilter proto;
    try {
      proto = FilterProtos.PageFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new PageFilter(proto.getPageSize());
  }

  /**
   * @param other
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof PageFilter)) return false;

    PageFilter other = (PageFilter)o;
    return this.getPageSize() == other.getPageSize();
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " " + this.pageSize;
  }
}
