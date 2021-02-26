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

import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

/**
 * The filter looks for the given columns in KeyValue. Once there is a match for
 * any one of the columns, it returns ReturnCode.NEXT_ROW for remaining
 * KeyValues in the row.
 * <p>
 * Note : It may emit KVs which do not have the given columns in them, if
 * these KVs happen to occur before a KV which does have a match. Given this
 * caveat, this filter is only useful for special cases
 * like org.apache.hadoop.hbase.mapreduce.RowCounter.
 * <p>
 * @deprecated Deprecated in 2.0.0 and will be removed in 3.0.0.
 * @see <a href="https://issues.apache.org/jira/browse/HBASE-13347">HBASE-13347</a>
 */
@InterfaceAudience.Public
@Deprecated
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

  @Deprecated
  @Override
  public ReturnCode filterKeyValue(final Cell c) {
    return filterCell(c);
  }

  @Override
  public ReturnCode filterCell(final Cell c) {
    if (hasFoundKV()) {
      return ReturnCode.NEXT_ROW;
    } else if (hasOneMatchingQualifier(c)) {
      setFoundKV(true);
    }
    return ReturnCode.INCLUDE;
  }

  private boolean hasOneMatchingQualifier(Cell c) {
    for (byte[] q : qualifiers) {
      if (CellUtil.matchingQualifier(c, q)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return The filter serialized using pb
   */
  @Override
  public byte [] toByteArray() {
    FilterProtos.FirstKeyValueMatchingQualifiersFilter.Builder builder =
      FilterProtos.FirstKeyValueMatchingQualifiersFilter.newBuilder();
    for (byte[] qualifier : qualifiers) {
      if (qualifier != null) builder.addQualifiers(UnsafeByteOperations.unsafeWrap(qualifier));
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

    TreeSet<byte []> qualifiers = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    for (ByteString qualifier : proto.getQualifiersList()) {
      qualifiers.add(qualifier.toByteArray());
    }
    return new FirstKeyValueMatchingQualifiersFilter(qualifiers);
  }

  /**
   * @param o the other filter to compare with
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  @Override
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof FirstKeyValueMatchingQualifiersFilter)) return false;

    FirstKeyValueMatchingQualifiersFilter other = (FirstKeyValueMatchingQualifiersFilter)o;
    return this.qualifiers.equals(other.qualifiers);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Filter && areSerializedFieldsEqual((Filter) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.qualifiers);
  }
}
