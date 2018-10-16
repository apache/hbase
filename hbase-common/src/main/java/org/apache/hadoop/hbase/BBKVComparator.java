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
package org.apache.hadoop.hbase;

import java.util.Comparator;

import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hbase.thirdparty.com.google.common.primitives.Longs;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A comparator for case where {@link ByteBufferKeyValue} is prevalent type (BBKV
 * is base-type in hbase2). Takes a general comparator as fallback in case types are NOT the
 * expected ByteBufferKeyValue.
 *
 * <p>This is a tricked-out Comparator at heart of hbase read and write. It is in
 * the HOT path so we try all sorts of ugly stuff so we can go faster. See below
 * in this javadoc comment for the list.
 *
 * <p>Apply this comparator narrowly so it is fed exclusively ByteBufferKeyValues
 * as much as is possible so JIT can settle (e.g. make one per ConcurrentSkipListMap
 * in HStore).
 *
 * <p>Exploits specially added methods in BBKV to save on deserializations of shorts,
 * longs, etc: i.e. calculating the family length requires row length; pass it in
 * rather than recalculate it, and so on.
 *
 * <p>This comparator does static dispatch to private final methods so hotspot is comfortable
 * deciding inline.
 *
 * <p>Measurement has it that we almost have it so all inlines from memstore
 * ConcurrentSkipListMap on down to the (unsafe) intrinisics that do byte compare
 * and deserialize shorts and ints; needs a bit more work.
 *
 * <p>Does not take a Type to compare: i.e. it is not a Comparator&lt;Cell> or
 * CellComparator&lt;Cell> or Comparator&lt;ByteBufferKeyValue> because that adds
 * another method to the hierarchy -- from compare(Object, Object)
 * to dynamic compare(Cell, Cell) to static private compare -- and inlining doesn't happen if
 * hierarchy is too deep (it is the case here).
 *
 * <p>Be careful making changes. Compare perf before and after and look at what
 * hotspot ends up generating before committing change (jitwatch is helpful here).
 * Changing this one class doubled write throughput (HBASE-20483).
 */
@InterfaceAudience.Private
public class BBKVComparator implements Comparator {
  protected static final Logger LOG = LoggerFactory.getLogger(BBKVComparator.class);
  private final Comparator fallback;

  public BBKVComparator(Comparator fallback) {
    this.fallback = fallback;
  }

  @Override
  public int compare(Object l, Object r) {
    // LOG.info("ltype={} rtype={}", l, r);
    if ((l instanceof ByteBufferKeyValue) && (r instanceof ByteBufferKeyValue)) {
      return compare((ByteBufferKeyValue)l, (ByteBufferKeyValue)r, false);
    }
    // Skip calling compare(Object, Object) and go direct to compare(Cell, Cell)
    return this.fallback.compare((Cell)l, (Cell)r);
  }

  // TODO: Come back here. We get a few percentage points extra of throughput if this is a
  // private method.
  static final int compare(ByteBufferKeyValue left, ByteBufferKeyValue right,
      boolean ignoreSequenceid) {
    // NOTE: Same method is in CellComparatorImpl, also private, not shared, intentionally. Not
    // sharing gets us a few percent more throughput in compares. If changes here or there, make
    // sure done in both places.

    // Compare Rows. Cache row length.
    int leftRowLength = left.getRowLength();
    int rightRowLength = right.getRowLength();
    int diff = ByteBufferUtils.compareTo(left.getRowByteBuffer(), left.getRowPosition(),
        leftRowLength,
        right.getRowByteBuffer(), right.getRowPosition(), rightRowLength);
    if (diff != 0) {
      return diff;
    }

    // If the column is not specified, the "minimum" key type appears as latest in the sorted
    // order, regardless of the timestamp. This is used for specifying the last key/value in a
    // given row, because there is no "lexicographically last column" (it would be infinitely long).
    // The "maximum" key type does not need this behavior. Copied from KeyValue. This is bad in that
    // we can't do memcmp w/ special rules like this.
    // TODO: Is there a test for this behavior?
    int leftFamilyLengthPosition = left.getFamilyLengthPosition(leftRowLength);
    int leftFamilyLength = left.getFamilyLength(leftFamilyLengthPosition);
    int leftKeyLength = left.getKeyLength();
    int leftQualifierLength = left.getQualifierLength(leftKeyLength, leftRowLength,
        leftFamilyLength);

    // No need of left row length below here.

    byte leftType = left.getTypeByte(leftKeyLength);
    if (leftFamilyLength + leftQualifierLength == 0 &&
        leftType == KeyValue.Type.Minimum.getCode()) {
      // left is "bigger", i.e. it appears later in the sorted order
      return 1;
    }

    int rightFamilyLengthPosition = right.getFamilyLengthPosition(rightRowLength);
    int rightFamilyLength = right.getFamilyLength(rightFamilyLengthPosition);
    int rightKeyLength = right.getKeyLength();
    int rightQualifierLength = right.getQualifierLength(rightKeyLength, rightRowLength,
        rightFamilyLength);

   // No need of right row length below here.

    byte rightType = right.getTypeByte(rightKeyLength);
    if (rightFamilyLength + rightQualifierLength == 0 &&
        rightType == KeyValue.Type.Minimum.getCode()) {
      return -1;
    }

    // Compare families.
    int leftFamilyPosition = left.getFamilyPosition(leftFamilyLengthPosition);
    int rightFamilyPosition = right.getFamilyPosition(rightFamilyLengthPosition);
    diff = ByteBufferUtils.compareTo(left.getFamilyByteBuffer(), leftFamilyPosition,
        leftFamilyLength,
        right.getFamilyByteBuffer(), rightFamilyPosition, rightFamilyLength);
    if (diff != 0) {
      return diff;
    }

    // Compare qualifiers
    diff = ByteBufferUtils.compareTo(left.getQualifierByteBuffer(),
        left.getQualifierPosition(leftFamilyPosition, leftFamilyLength), leftQualifierLength,
        right.getQualifierByteBuffer(),
        right.getQualifierPosition(rightFamilyPosition, rightFamilyLength),
        rightQualifierLength);
    if (diff != 0) {
      return diff;
    }

    // Timestamps.
    // Swap order we pass into compare so we get DESCENDING order.
    diff = Long.compare(right.getTimestamp(rightKeyLength), left.getTimestamp(leftKeyLength));
    if (diff != 0) {
      return diff;
    }

    // Compare types. Let the delete types sort ahead of puts; i.e. types
    // of higher numbers sort before those of lesser numbers. Maximum (255)
    // appears ahead of everything, and minimum (0) appears after
    // everything.
    diff = (0xff & rightType) - (0xff & leftType);
    if (diff != 0) {
      return diff;
    }

    // Negate following comparisons so later edits show up first mvccVersion: later sorts first
    return ignoreSequenceid ? diff : Longs.compare(right.getSequenceId(), left.getSequenceId());
  }
}
