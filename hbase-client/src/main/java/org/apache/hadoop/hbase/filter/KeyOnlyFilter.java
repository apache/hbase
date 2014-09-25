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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A filter that will only return the key component of each KV (the value will
 * be rewritten as empty).
 * <p>
 * This filter can be used to grab all of the keys without having to also grab
 * the values.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class KeyOnlyFilter extends FilterBase {

  boolean lenAsVal;
  public KeyOnlyFilter() { this(false); }
  public KeyOnlyFilter(boolean lenAsVal) { this.lenAsVal = lenAsVal; }

  @Override
  public Cell transformCell(Cell kv) {
    // TODO Move to KeyValueUtil

    // TODO make matching Column a cell method or CellUtil method.
    KeyValue v = KeyValueUtil.ensureKeyValue(kv);

    return v.createKeyOnly(this.lenAsVal);
  }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    Preconditions.checkArgument((filterArguments.size() == 0 || filterArguments.size() == 1),
                                "Expected: 0 or 1 but got: %s", filterArguments.size());
    KeyOnlyFilter filter = new KeyOnlyFilter();
    if (filterArguments.size() == 1) {
      filter.lenAsVal = ParseFilter.convertByteArrayToBoolean(filterArguments.get(0));
    }
    return filter;
  }

  /**
   * @return The filter serialized using pb
   */
  public byte [] toByteArray() {
    FilterProtos.KeyOnlyFilter.Builder builder =
      FilterProtos.KeyOnlyFilter.newBuilder();
    builder.setLenAsVal(this.lenAsVal);
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link KeyOnlyFilter} instance
   * @return An instance of {@link KeyOnlyFilter} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static KeyOnlyFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    FilterProtos.KeyOnlyFilter proto;
    try {
      proto = FilterProtos.KeyOnlyFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new KeyOnlyFilter(proto.getLenAsVal());
  }

  /**
   * @param other
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof KeyOnlyFilter)) return false;

    KeyOnlyFilter other = (KeyOnlyFilter)o;
    return this.lenAsVal == other.lenAsVal;
  }
}
