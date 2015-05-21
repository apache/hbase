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

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A wrapper filter that returns true from {@link #filterAllRemaining()} as soon
 * as the wrapped filters {@link Filter#filterRowKey(byte[], int, int)},
 * {@link Filter#filterKeyValue(org.apache.hadoop.hbase.Cell)},
 * {@link org.apache.hadoop.hbase.filter.Filter#filterRow()} or
 * {@link org.apache.hadoop.hbase.filter.Filter#filterAllRemaining()} methods
 * returns true.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class WhileMatchFilter extends FilterBase {
  private boolean filterAllRemaining = false;
  private Filter filter;

  public WhileMatchFilter(Filter filter) {
    this.filter = filter;
  }

  public Filter getFilter() {
    return filter;
  }

  public void reset() throws IOException {
    this.filter.reset();
  }

  private void changeFAR(boolean value) {
    filterAllRemaining = filterAllRemaining || value;
  }

  @Override
  public boolean filterAllRemaining() throws IOException {
    return this.filterAllRemaining || this.filter.filterAllRemaining();
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
    boolean value = filter.filterRowKey(buffer, offset, length);
    changeFAR(value);
    return value;
  }

  @Override
  public boolean filterRowKey(Cell cell) throws IOException {
    boolean value = filter.filterRowKey(cell);
    changeFAR(value);
    return value;
  }

  @Override
  public ReturnCode filterKeyValue(Cell v) throws IOException {
    ReturnCode c = filter.filterKeyValue(v);
    changeFAR(c != ReturnCode.INCLUDE);
    return c;
  }

  @Override
  public Cell transformCell(Cell v) throws IOException {
    return filter.transformCell(v);
  }

  @Override
  public boolean filterRow() throws IOException {
    boolean filterRow = this.filter.filterRow();
    changeFAR(filterRow);
    return filterRow;
  }
  
  @Override
  public boolean hasFilterRow() {
    return true;
  }

  /**
   * @return The filter serialized using pb
   */
  public byte[] toByteArray() throws IOException {
    FilterProtos.WhileMatchFilter.Builder builder =
      FilterProtos.WhileMatchFilter.newBuilder();
    builder.setFilter(ProtobufUtil.toFilter(this.filter));
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link WhileMatchFilter} instance
   * @return An instance of {@link WhileMatchFilter} made from <code>bytes</code>
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   * @see #toByteArray
   */
  public static WhileMatchFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    FilterProtos.WhileMatchFilter proto;
    try {
      proto = FilterProtos.WhileMatchFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    try {
      return new WhileMatchFilter(ProtobufUtil.toFilter(proto.getFilter()));
    } catch (IOException ioe) {
      throw new DeserializationException(ioe);
    }
  }

  /**
   * @param other
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof WhileMatchFilter)) return false;

    WhileMatchFilter other = (WhileMatchFilter)o;
    return getFilter().areSerializedFieldsEqual(other.getFilter());
  }

  public boolean isFamilyEssential(byte[] name) throws IOException {
    return filter.isFamilyEssential(name);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " " + this.filter.toString();
  }
}
