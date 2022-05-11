/*
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
import java.util.Objects;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;

/**
 * A wrapper filter that filters an entire row if any of the Cell checks do not pass.
 * <p>
 * For example, if all columns in a row represent weights of different things, with the values being
 * the actual weights, and we want to filter out the entire row if any of its weights are zero. In
 * this case, we want to prevent rows from being emitted if a single key is filtered. Combine this
 * filter with a {@link ValueFilter}:
 * </p>
 * <p>
 * <code>
 * scan.setFilter(new SkipFilter(new ValueFilter(CompareOp.NOT_EQUAL,
 *     new BinaryComparator(Bytes.toBytes(0))));
 * </code> Any row which contained a column whose value was 0 will be filtered out (since
 * ValueFilter will not pass that Cell). Without this filter, the other non-zero valued columns in
 * the row would still be emitted.
 * </p>
 */
@InterfaceAudience.Public
public class SkipFilter extends FilterBase {
  private boolean filterRow = false;
  private Filter filter;

  public SkipFilter(Filter filter) {
    this.filter = filter;
  }

  public Filter getFilter() {
    return filter;
  }

  @Override
  public void reset() throws IOException {
    filter.reset();
    filterRow = false;
  }

  private void changeFR(boolean value) {
    filterRow = filterRow || value;
  }

  @Override
  public boolean filterRowKey(Cell cell) throws IOException {
    // Impl in FilterBase might do unnecessary copy for Off heap backed Cells.
    return false;
  }

  @Deprecated
  @Override
  public ReturnCode filterKeyValue(final Cell c) throws IOException {
    return filterCell(c);
  }

  @Override
  public ReturnCode filterCell(final Cell c) throws IOException {
    ReturnCode rc = filter.filterCell(c);
    changeFR(rc != ReturnCode.INCLUDE);
    return rc;
  }

  @Override
  public Cell transformCell(Cell v) throws IOException {
    return filter.transformCell(v);
  }

  @Override
  public boolean filterRow() {
    return filterRow;
  }

  @Override
  public boolean hasFilterRow() {
    return true;
  }

  /**
   * @return The filter serialized using pb
   */
  @Override
  public byte[] toByteArray() throws IOException {
    FilterProtos.SkipFilter.Builder builder = FilterProtos.SkipFilter.newBuilder();
    builder.setFilter(ProtobufUtil.toFilter(this.filter));
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link SkipFilter} instance
   * @return An instance of {@link SkipFilter} made from <code>bytes</code> n * @see #toByteArray
   */
  public static SkipFilter parseFrom(final byte[] pbBytes) throws DeserializationException {
    FilterProtos.SkipFilter proto;
    try {
      proto = FilterProtos.SkipFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    try {
      return new SkipFilter(ProtobufUtil.toFilter(proto.getFilter()));
    } catch (IOException ioe) {
      throw new DeserializationException(ioe);
    }
  }

  /**
   * @param o the other filter to compare with
   * @return true if and only if the fields of the filter that are serialized are equal to the
   *         corresponding fields in other. Used for testing.
   */
  @Override
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof SkipFilter)) return false;

    SkipFilter other = (SkipFilter) o;
    return getFilter().areSerializedFieldsEqual(other.getFilter());
  }

  @Override
  public boolean isFamilyEssential(byte[] name) throws IOException {
    return filter.isFamilyEssential(name);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " " + this.filter.toString();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Filter && areSerializedFieldsEqual((Filter) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.filter);
  }
}
