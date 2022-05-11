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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Objects;
import java.util.TreeSet;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;

/**
 * This filter is used for selecting only those keys with columns that matches a particular prefix.
 * For example, if prefix is 'an', it will pass keys will columns like 'and', 'anti' but not keys
 * with columns like 'ball', 'act'.
 */
@InterfaceAudience.Public
public class MultipleColumnPrefixFilter extends FilterBase {
  private static final Logger LOG = LoggerFactory.getLogger(MultipleColumnPrefixFilter.class);
  protected byte[] hint = null;
  protected TreeSet<byte[]> sortedPrefixes = createTreeSet();
  private final static int MAX_LOG_PREFIXES = 5;

  public MultipleColumnPrefixFilter(final byte[][] prefixes) {
    if (prefixes != null) {
      for (byte[] prefix : prefixes) {
        if (!sortedPrefixes.add(prefix)) {
          LOG.error("prefix {} is repeated", Bytes.toString(prefix));
          throw new IllegalArgumentException("prefixes must be distinct");
        }
      }
    }
  }

  public byte[][] getPrefix() {
    int count = 0;
    byte[][] temp = new byte[sortedPrefixes.size()][];
    for (byte[] prefixes : sortedPrefixes) {
      temp[count++] = prefixes;
    }
    return temp;
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
    if (sortedPrefixes.isEmpty()) {
      return ReturnCode.INCLUDE;
    } else {
      return filterColumn(c);
    }
  }

  public ReturnCode filterColumn(Cell cell) {
    byte[] qualifier = CellUtil.cloneQualifier(cell);
    TreeSet<byte[]> lesserOrEqualPrefixes =
      (TreeSet<byte[]>) sortedPrefixes.headSet(qualifier, true);

    if (lesserOrEqualPrefixes.size() != 0) {
      byte[] largestPrefixSmallerThanQualifier = lesserOrEqualPrefixes.last();

      if (Bytes.startsWith(qualifier, largestPrefixSmallerThanQualifier)) {
        return ReturnCode.INCLUDE;
      }

      if (lesserOrEqualPrefixes.size() == sortedPrefixes.size()) {
        return ReturnCode.NEXT_ROW;
      } else {
        hint = sortedPrefixes.higher(largestPrefixSmallerThanQualifier);
        return ReturnCode.SEEK_NEXT_USING_HINT;
      }
    } else {
      hint = sortedPrefixes.first();
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }
  }

  public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
    byte[][] prefixes = new byte[filterArguments.size()][];
    for (int i = 0; i < filterArguments.size(); i++) {
      byte[] columnPrefix = ParseFilter.removeQuotesFromByteArray(filterArguments.get(i));
      prefixes[i] = columnPrefix;
    }
    return new MultipleColumnPrefixFilter(prefixes);
  }

  /**
   * @return The filter serialized using pb
   */
  @Override
  public byte[] toByteArray() {
    FilterProtos.MultipleColumnPrefixFilter.Builder builder =
      FilterProtos.MultipleColumnPrefixFilter.newBuilder();
    for (byte[] element : sortedPrefixes) {
      if (element != null) builder.addSortedPrefixes(UnsafeByteOperations.unsafeWrap(element));
    }
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link MultipleColumnPrefixFilter} instance
   * @return An instance of {@link MultipleColumnPrefixFilter} made from <code>bytes</code> n * @see
   *         #toByteArray
   */
  public static MultipleColumnPrefixFilter parseFrom(final byte[] pbBytes)
    throws DeserializationException {
    FilterProtos.MultipleColumnPrefixFilter proto;
    try {
      proto = FilterProtos.MultipleColumnPrefixFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    int numPrefixes = proto.getSortedPrefixesCount();
    byte[][] prefixes = new byte[numPrefixes][];
    for (int i = 0; i < numPrefixes; ++i) {
      prefixes[i] = proto.getSortedPrefixes(i).toByteArray();
    }

    return new MultipleColumnPrefixFilter(prefixes);
  }

  /**
   * @param o the other filter to compare with
   * @return true if and only if the fields of the filter that are serialized are equal to the
   *         corresponding fields in other. Used for testing.
   */
  @Override
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof MultipleColumnPrefixFilter)) return false;

    MultipleColumnPrefixFilter other = (MultipleColumnPrefixFilter) o;
    return this.sortedPrefixes.equals(other.sortedPrefixes);
  }

  @Override
  public Cell getNextCellHint(Cell cell) {
    return PrivateCellUtil.createFirstOnRowCol(cell, hint, 0, hint.length);
  }

  public TreeSet<byte[]> createTreeSet() {
    return new TreeSet<>(new Comparator<Object>() {
      @Override
      public int compare(Object o1, Object o2) {
        if (o1 == null || o2 == null) throw new IllegalArgumentException("prefixes can't be null");

        byte[] b1 = (byte[]) o1;
        byte[] b2 = (byte[]) o2;
        return Bytes.compareTo(b1, 0, b1.length, b2, 0, b2.length);
      }
    });
  }

  @Override
  public String toString() {
    return toString(MAX_LOG_PREFIXES);
  }

  protected String toString(int maxPrefixes) {
    StringBuilder prefixes = new StringBuilder();

    int count = 0;
    for (byte[] ba : this.sortedPrefixes) {
      if (count >= maxPrefixes) {
        break;
      }
      ++count;
      prefixes.append(Bytes.toStringBinary(ba));
      if (count < this.sortedPrefixes.size() && count < maxPrefixes) {
        prefixes.append(", ");
      }
    }

    return String.format("%s (%d/%d): [%s]", this.getClass().getSimpleName(), count,
      this.sortedPrefixes.size(), prefixes.toString());
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Filter && areSerializedFieldsEqual((Filter) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.sortedPrefixes);
  }
}
