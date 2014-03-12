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

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.google.common.base.Objects;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@ThriftStruct
public class TFilter implements Filter {
  private final Log LOG = LogFactory.getLog(TFilter.class);
  private byte[] filterBytes;
  private Filter filter;

  /**
   * Empty constructor for hadoop rpc
   */
  public TFilter() {
  }

  /**
   * Constructor used by thrift.
   * Use {@link TFilter#getTFilter(Filter)} to construct a TFilter object from
   * a Filter object. We serialize the Filter object in the older method and
   * deserailize on the server side as well.
   * @param filterBytes
   * @throws IOException
   */
  @ThriftConstructor
  public TFilter(@ThriftField(1) byte[] filterBytes) throws IOException {
    this.filterBytes = filterBytes;
    this.filter = getFilter();
  }

  public TFilter(byte[] filterBytes, Filter filter) {
    this.filterBytes = filterBytes;
    this.filter = filter;
  }

  @ThriftField(1)
  public byte[] getFilterBytes() {
    return this.filterBytes;
  }

  /**
   * Constructs the Filter using the
   * @return
   * @throws IOException
   */
  public Filter getFilter() throws IOException {
    if (filter != null) return filter;
    if (this.filterBytes == null || this.filterBytes.length == 0) return null;
    DataInput in = new DataInputStream(
        new ByteArrayInputStream(this.filterBytes));
    return filter = (Filter)HbaseObjectWritable.readObject(in, null);
  }

  /**
   * Constructs the thrift serialized representation of Filter using
   * HBaseObjectWritable helper methods.
   * @param filter
   * @return
   * @throws IOException
   */
  public static TFilter getTFilter(Filter filter) throws IOException {
    if (filter == null) return null;
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(stream);
    HbaseObjectWritable.writeObject(out, filter, filter.getClass(), null);
    out.close();
    return new TFilter(stream.toByteArray(), filter);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.filterBytes = Bytes.readByteArray(in);
    this.filter = getFilter();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.filterBytes);
  }

  @Override
  public void reset() {
    this.filter.reset();
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    return this.filter.filterRowKey(buffer, offset, length);
  }

  @Override
  public boolean filterAllRemaining() {
    return this.filter.filterAllRemaining();
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    return this.filter.filterKeyValue(v);
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v, List<KeyValueScanner> scanners) {
    return this.filter.filterKeyValue(v, scanners);
  }

  @Override
  public void filterRow(List<KeyValue> kvs) {
    this.filter.filterRow(kvs);
  }

  @Override
  public boolean hasFilterRow() {
    return this.filter.hasFilterRow();
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
  public int hashCode() {
    return Objects.hashCode(this.filterBytes);
  }

  @Override
  public String toString() {
    if (this.filter == null) {
      return null;
    }

    return this.filter.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TFilter other = (TFilter) obj;
    if (!Arrays.equals(filterBytes, other.filterBytes)) {
      return false;
    }
    return true;
  }
}
