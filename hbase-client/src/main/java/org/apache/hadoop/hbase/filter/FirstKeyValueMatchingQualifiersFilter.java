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

import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * The filter looks for the given columns in KeyValue. Once there is a match for
 * any one of the columns, it returns ReturnCode.NEXT_ROW for remaining
 * KeyValues in the row.
 * <p>
 * Note : It may emit KVs which do not have the given columns in them, if
 * these KVs happen to occur before a KV which does have a match. Given this
 * caveat, this filter is only useful for special cases
 * like {@link org.apache.hadoop.hbase.mapreduce.RowCounter}.
 * <p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FirstKeyValueMatchingQualifiersFilter extends FirstKeyOnlyFilter {

  private Set<byte []> qualifiers;

  /**
   * Constructor which takes a set of columns. As soon as first KeyValue
   * matching any of these columns is found, filter moves to next row.
   * 
   * @param qualifiers the set of columns to me matched.
   */
  public FirstKeyValueMatchingQualifiersFilter(Set<byte []> qualifiers) {
    this.qualifiers = qualifiers;
  }

  @Override
  public ReturnCode filterKeyValue(Cell v) {
    if (hasFoundKV()) {
      return ReturnCode.NEXT_ROW;
    } else if (hasOneMatchingQualifier(v)) {
      setFoundKV(true);
    }
    return ReturnCode.INCLUDE;
  }

  private boolean hasOneMatchingQualifier(Cell v) {
    for (byte[] q : qualifiers) {
      if (CellUtil.matchingQualifier(v, q)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return The filter serialized using pb
   */
  public byte [] toByteArray() {
    FilterProtos.FirstKeyValueMatchingQualifiersFilter.Builder builder =
      FilterProtos.FirstKeyValueMatchingQualifiersFilter.newBuilder();
    for (byte[] qualifier : qualifiers) {
      if (qualifier != null) builder.addQualifiers(ByteStringer.wrap(qualifier));
    }
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link FirstKeyValueMatchingQualifiersFilter} instance
   * @return An instance of {@link FirstKeyValueMatchingQualifiersFilter} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static FirstKeyValueMatchingQualifiersFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    FilterProtos.FirstKeyValueMatchingQualifiersFilter proto;
    try {
      proto = FilterProtos.FirstKeyValueMatchingQualifiersFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }

    TreeSet<byte []> qualifiers = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
    for (ByteString qualifier : proto.getQualifiersList()) {
      qualifiers.add(qualifier.toByteArray());
    }
    return new FirstKeyValueMatchingQualifiersFilter(qualifiers);
  }

  /**
   * @param other
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof FirstKeyValueMatchingQualifiersFilter)) return false;

    FirstKeyValueMatchingQualifiersFilter other = (FirstKeyValueMatchingQualifiersFilter)o;
    return this.qualifiers.equals(other.qualifiers);
  }
}
