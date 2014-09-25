/*
 * Copyright The Apache Software Foundation
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
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * This is a Filter wrapper class which is used in the server side. Some filter
 * related hooks can be defined in this wrapper. The only way to create a
 * FilterWrapper instance is passing a client side Filter instance through
 * {@link org.apache.hadoop.hbase.client.Scan#getFilter()}.
 * 
 */
@InterfaceAudience.Private
final public class FilterWrapper extends Filter {
  Filter filter = null;

  public FilterWrapper( Filter filter ) {
    if (null == filter) {
      // ensure the filter instance is not null
      throw new NullPointerException("Cannot create FilterWrapper with null Filter");
    }
    this.filter = filter;
  }

  /**
   * @return The filter serialized using pb
   */
  public byte[] toByteArray() throws IOException {
    FilterProtos.FilterWrapper.Builder builder =
      FilterProtos.FilterWrapper.newBuilder();
    builder.setFilter(ProtobufUtil.toFilter(this.filter));
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link FilterWrapper} instance
   * @return An instance of {@link FilterWrapper} made from <code>bytes</code>
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   * @see #toByteArray
   */
  public static FilterWrapper parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    FilterProtos.FilterWrapper proto;
    try {
      proto = FilterProtos.FilterWrapper.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    try {
      return new FilterWrapper(ProtobufUtil.toFilter(proto.getFilter()));
    } catch (IOException ioe) {
      throw new DeserializationException(ioe);
    }
  }

  @Override
  public void reset() throws IOException {
    this.filter.reset();
  }

  @Override
  public boolean filterAllRemaining() throws IOException {
    return this.filter.filterAllRemaining();
  }

  @Override
  public boolean filterRow() throws IOException {
    return this.filter.filterRow();
  }

  /**
   * This method is deprecated and you should override Cell getNextKeyHint(Cell) instead.
   */
  @Override
  @Deprecated
  public KeyValue getNextKeyHint(KeyValue currentKV) throws IOException {
    return KeyValueUtil.ensureKeyValue(this.filter.getNextCellHint((Cell)currentKV));
  }

  /**
   * Old filter wrapper descendants will implement KV getNextKeyHint(KV) so we should call it.
   */
  @Override
  public Cell getNextCellHint(Cell currentKV) throws IOException {
    // Old filters based off of this class will override KeyValue getNextKeyHint(KeyValue).
    // Thus to maintain compatibility we need to call the old version.
    return this.getNextKeyHint(KeyValueUtil.ensureKeyValue(currentKV));
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
    return this.filter.filterRowKey(buffer, offset, length);
  }

  @Override
  public ReturnCode filterKeyValue(Cell v) throws IOException {
    return this.filter.filterKeyValue(v);
  }

  @Override
  public Cell transformCell(Cell v) throws IOException {
    // Old filters based off of this class will override KeyValue transform(KeyValue).
    // Thus to maintain compatibility we need to call the old version.
    return transform(KeyValueUtil.ensureKeyValue(v));
  }

  /**
   * WARNING: please to not override this method.  Instead override {@link #transformCell(Cell)}.
   *
   * This is for transition from 0.94 -> 0.96
   */
  @Override
  @Deprecated
  public KeyValue transform(KeyValue currentKV) throws IOException {
    return KeyValueUtil.ensureKeyValue(this.filter.transformCell(currentKV));
  }

  @Override
  public boolean hasFilterRow() {
    return this.filter.hasFilterRow();
  }

  @Override
  public void filterRowCells(List<Cell> kvs) throws IOException {
    filterRowCellsWithRet(kvs);
  }

  public enum FilterRowRetCode {
    NOT_CALLED,
    INCLUDE,     // corresponds to filter.filterRow() returning false
    EXCLUDE      // corresponds to filter.filterRow() returning true
  }
  public FilterRowRetCode filterRowCellsWithRet(List<Cell> kvs) throws IOException {
    //To fix HBASE-6429,
    //Filter with filterRow() returning true is incompatible with scan with limit
    //1. hasFilterRow() returns true, if either filterRow() or filterRow(kvs) is implemented.
    //2. filterRow() is merged with filterRow(kvs),
    //so that to make all those row related filtering stuff in the same function.
    this.filter.filterRowCells(kvs);
    if (!kvs.isEmpty()) {
      if (this.filter.filterRow()) {
        kvs.clear();
        return FilterRowRetCode.EXCLUDE;
      }
      return FilterRowRetCode.INCLUDE;
    }
    return FilterRowRetCode.NOT_CALLED;
  }

  /**
   * WARNING: please to not override this method.  Instead override {@link #transformCell(Cell)}.
   *
   * This is for transition from 0.94 -> 0.96
   */
  @Override
  @Deprecated
  public void filterRow(List<KeyValue> kvs) throws IOException {
    // This is only used internally, marked InterfaceAudience.private, and not used anywhere.
    // We can get away with not implementing this.
    throw new UnsupportedOperationException("filterRow(List<KeyValue>) should never be called");
  }

  @Override
  public boolean isFamilyEssential(byte[] name) throws IOException {
    return filter.isFamilyEssential(name);
  }

  /**
   * @param other
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof FilterWrapper)) return false;

    FilterWrapper other = (FilterWrapper)o;
    return this.filter.areSerializedFieldsEqual(other.filter);
  }
}
