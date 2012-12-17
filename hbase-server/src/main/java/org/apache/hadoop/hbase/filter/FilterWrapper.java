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
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.hbase.DeserializationException;
import org.apache.hadoop.hbase.KeyValue;
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
@InterfaceStability.Evolving
public class FilterWrapper extends Filter {
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
  public byte [] toByteArray() {
    FilterProtos.FilterWrapper.Builder builder =
      FilterProtos.FilterWrapper.newBuilder();
    builder.setFilter(ProtobufUtil.toFilter(this.filter));
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link FilterWrapper} instance
   * @return An instance of {@link FilterWrapper} made from <code>bytes</code>
   * @throws DeserializationException
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
  public void reset() {
    this.filter.reset();
  }

  @Override
  public boolean filterAllRemaining() {
    return this.filter.filterAllRemaining();
  }

  @Override
  public boolean filterRow() {
    return this.filter.filterRow();
  }

  @Override
  public KeyValue getNextKeyHint(KeyValue currentKV) {
    return this.filter.getNextKeyHint(currentKV);
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    return this.filter.filterRowKey(buffer, offset, length);
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    return this.filter.filterKeyValue(v);
  }

  @Override
  public KeyValue transform(KeyValue v) {
    return this.filter.transform(v);
  }

  @Override
  public boolean hasFilterRow() {
    return this.filter.hasFilterRow();
  }

  @Override
  public void filterRow(List<KeyValue> kvs) {
    //To fix HBASE-6429, 
    //Filter with filterRow() returning true is incompatible with scan with limit
    //1. hasFilterRow() returns true, if either filterRow() or filterRow(kvs) is implemented.
    //2. filterRow() is merged with filterRow(kvs),
    //so that to make all those row related filtering stuff in the same function.
    this.filter.filterRow(kvs);
    if (!kvs.isEmpty() && this.filter.filterRow()) {
      kvs.clear();
    }
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
