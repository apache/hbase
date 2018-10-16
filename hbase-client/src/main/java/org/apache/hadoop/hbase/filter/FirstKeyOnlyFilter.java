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
import java.util.ArrayList;
import java.util.Objects;

import org.apache.hadoop.hbase.Cell;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

/**
 * A filter that will only return the first KV from each row.
 * <p>
 * This filter can be used to more efficiently perform row count operations.
 */
@InterfaceAudience.Public
public class FirstKeyOnlyFilter extends FilterBase {
  private boolean foundKV = false;

  public FirstKeyOnlyFilter() {
  }

  @Override
  public void reset() {
    foundKV = false;
  }

  @Override
  public boolean filterRowKey(Cell cell) throws IOException {
    // Impl in FilterBase might do unnecessary copy for Off heap backed Cells.
    return false;
  }

  @Deprecated
  @Override
  public ReturnCode filterKeyValue(final Cell c) {
    return filterCell(c);
  }

  @Override
  public ReturnCode filterCell(final Cell c) {
    if(foundKV) return ReturnCode.NEXT_ROW;
    foundKV = true;
    return ReturnCode.INCLUDE;
  }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    Preconditions.checkArgument(filterArguments.isEmpty(),
                                "Expected 0 but got: %s", filterArguments.size());
    return new FirstKeyOnlyFilter();
  }

  /**
   * @return true if first KV has been found.
   */
  protected boolean hasFoundKV() {
    return this.foundKV;
  }

  /**
   *
   * @param value update {@link #foundKV} flag with value.
   */
  protected void setFoundKV(boolean value) {
    this.foundKV = value;
  }

  /**
   * @return The filter serialized using pb
   */
  @Override
  public byte [] toByteArray() {
    FilterProtos.FirstKeyOnlyFilter.Builder builder =
      FilterProtos.FirstKeyOnlyFilter.newBuilder();
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link FirstKeyOnlyFilter} instance
   * @return An instance of {@link FirstKeyOnlyFilter} made from <code>bytes</code>
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   * @see #toByteArray
   */
  public static FirstKeyOnlyFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    // There is nothing to deserialize.  Why do this at all?
    try {
      FilterProtos.FirstKeyOnlyFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    // Just return a new instance.
    return new FirstKeyOnlyFilter();
  }

  /**
   * @param o the other filter to compare with
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  @Override
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof FirstKeyOnlyFilter)) return false;

    return true;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Filter && areSerializedFieldsEqual((Filter) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(foundKV);
  }
}
