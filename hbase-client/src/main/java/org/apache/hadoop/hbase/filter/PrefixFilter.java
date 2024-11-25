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

import java.util.ArrayList;
import org.apache.hadoop.hbase.ByteBufferExtendedCell;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;

/**
 * Pass results that have same row prefix.
 */
@InterfaceAudience.Public
public class PrefixFilter extends FilterBase implements HintingFilter {
  protected byte[] prefix = null;
  protected boolean passedPrefix = false;
  protected boolean filterRow = true;
  protected boolean provideHint = false;
  protected Cell reversedNextCellHint;
  protected Cell forwardNextCellHint;

  public PrefixFilter(final byte[] prefix) {
    this.prefix = prefix;
    // Pre-compute hints at creation to avoid re-computing them several times in the corner
    // case where there are a lot of cells between the hint and the first real match.
    createCellHints();
  }

  private void createCellHints() {
    if (prefix == null) {
      return;
    }
    // On reversed scan hint should be the prefix with last byte incremented
    byte[] reversedHintBytes = PrivateCellUtil.increaseLastNonMaxByte(this.prefix);
    this.reversedNextCellHint =
      PrivateCellUtil.createFirstOnRow(reversedHintBytes, 0, (short) reversedHintBytes.length);
    // On forward scan hint should be the prefix
    this.forwardNextCellHint = PrivateCellUtil.createFirstOnRow(prefix, 0, (short) prefix.length);
  }

  public byte[] getPrefix() {
    return prefix;
  }

  @Override
  public boolean filterRowKey(Cell firstRowCell) {
    if (firstRowCell == null || this.prefix == null) {
      return true;
    }
    if (filterAllRemaining()) {
      return true;
    }
    // if the cell is before => return false so that getNextCellHint() is invoked.
    // if they are equal, return false => pass row
    // if the cell is after => return true, filter row
    // if we are passed the prefix, set flag
    int cmp;
    if (firstRowCell instanceof ByteBufferExtendedCell) {
      cmp = ByteBufferUtils.compareTo(((ByteBufferExtendedCell) firstRowCell).getRowByteBuffer(),
        ((ByteBufferExtendedCell) firstRowCell).getRowPosition(), this.prefix.length, this.prefix,
        0, this.prefix.length);
    } else {
      cmp = Bytes.compareTo(firstRowCell.getRowArray(), firstRowCell.getRowOffset(),
        this.prefix.length, this.prefix, 0, this.prefix.length);
    }
    if ((!isReversed() && cmp > 0) || (isReversed() && cmp < 0)) {
      passedPrefix = true;
    }
    filterRow = (cmp != 0);
    provideHint = (!isReversed() && cmp < 0) || (isReversed() && cmp > 0);
    return passedPrefix;
  }

  @Deprecated
  @Override
  public ReturnCode filterKeyValue(final Cell c) {
    return filterCell(c);
  }

  @Override
  public ReturnCode filterCell(final Cell c) {
    if (provideHint) {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }
    if (filterRow) {
      return ReturnCode.NEXT_ROW;
    }
    return ReturnCode.INCLUDE;
  }

  @Override
  public boolean filterRow() {
    return filterRow;
  }

  @Override
  public void reset() {
    filterRow = true;
  }

  @Override
  public boolean filterAllRemaining() {
    return passedPrefix;
  }

  @Override
  public Cell getNextCellHint(Cell cell) {
    if (reversed) {
      return reversedNextCellHint;
    } else {
      return forwardNextCellHint;
    }
  }

  public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
    Preconditions.checkArgument(filterArguments.size() == 1, "Expected 1 but got: %s",
      filterArguments.size());
    byte[] prefix = ParseFilter.removeQuotesFromByteArray(filterArguments.get(0));
    return new PrefixFilter(prefix);
  }

  /** Returns The filter serialized using pb */
  @Override
  public byte[] toByteArray() {
    FilterProtos.PrefixFilter.Builder builder = FilterProtos.PrefixFilter.newBuilder();
    if (this.prefix != null) {
      builder.setPrefix(UnsafeByteOperations.unsafeWrap(this.prefix));
    }
    return builder.build().toByteArray();
  }

  /**
   * Parse a serialized representation of {@link PrefixFilter}
   * @param pbBytes A pb serialized {@link PrefixFilter} instance
   * @return An instance of {@link PrefixFilter} made from <code>bytes</code>
   * @throws DeserializationException if an error occurred
   * @see #toByteArray
   */
  public static PrefixFilter parseFrom(final byte[] pbBytes) throws DeserializationException {
    FilterProtos.PrefixFilter proto;
    try {
      proto = FilterProtos.PrefixFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new PrefixFilter(proto.hasPrefix() ? proto.getPrefix().toByteArray() : null);
  }

  /**
   * Returns true if and only if the fields of the filter that are serialized are equal to the
   * corresponding fields in other. Used for testing.
   */
  @Override
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof PrefixFilter)) {
      return false;
    }
    PrefixFilter other = (PrefixFilter) o;
    return Bytes.equals(this.getPrefix(), other.getPrefix());
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " " + Bytes.toStringBinary(this.prefix);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Filter && areSerializedFieldsEqual((Filter) obj);
  }

  @Override
  public int hashCode() {
    return Bytes.hashCode(this.getPrefix());
  }
}
