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

import java.nio.ByteBuffer;
import java.util.Comparator;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * A {@link CellComparatorImpl} for <code>hbase:meta</code> catalog table {@link KeyValue}s.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MetaCellComparator extends CellComparatorImpl {

  /**
   * A {@link MetaCellComparator} for <code>hbase:meta</code> catalog table {@link KeyValue}s.
   */
  public static final MetaCellComparator META_COMPARATOR = new MetaCellComparator();

  @Override
  public int compareRows(final Cell left, final Cell right) {
    if (left instanceof ByteBufferExtendedCell) {
      ByteBufferExtendedCell bbLeft = (ByteBufferExtendedCell) left;
      if (right instanceof ByteBufferExtendedCell) {
        ByteBufferExtendedCell bbRight = (ByteBufferExtendedCell) right;
        return compareBBRows(bbLeft.getRowByteBuffer(), bbLeft.getRowPosition(),
          left.getRowLength(), bbRight.getRowByteBuffer(), bbRight.getRowPosition(),
          right.getRowLength());
      } else {
        return compareBBAndBytesRows(bbLeft.getRowByteBuffer(), bbLeft.getRowPosition(),
          left.getRowLength(), right.getRowArray(), right.getRowOffset(), right.getRowLength());
      }
    } else {
      if (right instanceof ByteBufferExtendedCell) {
        ByteBufferExtendedCell bbRight = (ByteBufferExtendedCell) right;
        return -compareBBAndBytesRows(bbRight.getRowByteBuffer(), bbRight.getRowPosition(),
          right.getRowLength(), left.getRowArray(), left.getRowOffset(), left.getRowLength());
      } else {
        return compareBytesRows(left.getRowArray(), left.getRowOffset(), left.getRowLength(),
          right.getRowArray(), right.getRowOffset(), right.getRowLength());
      }
    }
  }

  @Override
  public int compareRows(Cell left, byte[] right, int roffset, int rlength) {
    if (left instanceof ByteBufferExtendedCell) {
      ByteBufferExtendedCell bbLeft = (ByteBufferExtendedCell) left;
      return compareBBAndBytesRows(bbLeft.getRowByteBuffer(), bbLeft.getRowPosition(),
        left.getRowLength(), right, roffset, rlength);
    } else {
      return compareBytesRows(left.getRowArray(), left.getRowOffset(), left.getRowLength(), right,
        roffset, rlength);
    }
  }

  @Override
  public int compareRows(byte[] leftRow, byte[] rightRow) {
    return compareBytesRows(leftRow, 0, leftRow.length, rightRow, 0, rightRow.length);
  }

  @Override
  public int compare(final Cell a, final Cell b, boolean ignoreSequenceid) {
    int diff = compareRows(a, b);
    if (diff != 0) {
      return diff;
    }

    diff = compareWithoutRow(a, b);
    if (diff != 0) {
      return diff;
    }

    if (ignoreSequenceid) {
      return diff;
    }
    // Negate following comparisons so later edits show up first mvccVersion: later sorts first
    return Long.compare(PrivateCellUtil.getSequenceId(b), PrivateCellUtil.getSequenceId(a));
  }

  @FunctionalInterface
  private interface SearchDelimiter<T> {
    int search(T t, int offset, int length, int delimiter);
  }

  @FunctionalInterface
  private interface SearchDelimiterInReverse<T> {
    int search(T t, int offset, int length, int delimiter);
  }

  @FunctionalInterface
  private interface Compare<L, R> {
    int compareTo(L left, int loffset, int llength, R right, int roffset, int rlength);
  }

  private static <L, R> int compareRows(L left, int loffset, int llength, R right, int roffset,
    int rlength, SearchDelimiter<L> searchLeft, SearchDelimiter<R> searchRight,
    SearchDelimiterInReverse<L> searchInReverseLeft,
    SearchDelimiterInReverse<R> searchInReverseRight, Compare<L, R> comparator) {
    int leftDelimiter = searchLeft.search(left, loffset, llength, HConstants.DELIMITER);
    int rightDelimiter = searchRight.search(right, roffset, rlength, HConstants.DELIMITER);
    // Compare up to the delimiter
    int lpart = (leftDelimiter < 0 ? llength : leftDelimiter - loffset);
    int rpart = (rightDelimiter < 0 ? rlength : rightDelimiter - roffset);
    int result = comparator.compareTo(left, loffset, lpart, right, roffset, rpart);
    if (result != 0) {
      return result;
    } else {
      if (leftDelimiter < 0 && rightDelimiter >= 0) {
        return -1;
      } else if (rightDelimiter < 0 && leftDelimiter >= 0) {
        return 1;
      } else if (leftDelimiter < 0) {
        return 0;
      }
    }
    // Compare middle bit of the row.
    // Move past delimiter
    leftDelimiter++;
    rightDelimiter++;
    int leftFarDelimiter = searchInReverseLeft.search(left, leftDelimiter,
      llength - (leftDelimiter - loffset), HConstants.DELIMITER);
    int rightFarDelimiter = searchInReverseRight.search(right, rightDelimiter,
      rlength - (rightDelimiter - roffset), HConstants.DELIMITER);
    // Now compare middlesection of row.
    lpart = (leftFarDelimiter < 0 ? llength + loffset : leftFarDelimiter) - leftDelimiter;
    rpart = (rightFarDelimiter < 0 ? rlength + roffset : rightFarDelimiter) - rightDelimiter;
    result = comparator.compareTo(left, leftDelimiter, lpart, right, rightDelimiter, rpart);
    if (result != 0) {
      return result;
    } else {
      if (leftDelimiter < 0 && rightDelimiter >= 0) {
        return -1;
      } else if (rightDelimiter < 0 && leftDelimiter >= 0) {
        return 1;
      } else if (leftDelimiter < 0) {
        return 0;
      }
    }
    // Compare last part of row, the rowid.
    leftFarDelimiter++;
    rightFarDelimiter++;
    result = comparator.compareTo(left, leftFarDelimiter, llength - (leftFarDelimiter - loffset),
      right, rightFarDelimiter, rlength - (rightFarDelimiter - roffset));
    return result;
  }

  private static int compareBBRows(ByteBuffer left, int loffset, int llength, ByteBuffer right,
    int roffset, int rlength) {
    if (left.hasArray()) {
      return -compareBBAndBytesRows(right, roffset, rlength, left.array(),
        left.arrayOffset() + loffset, llength);
    }
    if (right.hasArray()) {
      return compareBBAndBytesRows(left, loffset, llength, right.array(),
        right.arrayOffset() + roffset, rlength);
    }
    return compareRows(left, loffset, llength, right, roffset, rlength,
      ByteBufferUtils::searchDelimiterIndex, ByteBufferUtils::searchDelimiterIndex,
      ByteBufferUtils::searchDelimiterIndexInReverse,
      ByteBufferUtils::searchDelimiterIndexInReverse, ByteBufferUtils::compareTo);
  }

  private static int compareBBAndBytesRows(ByteBuffer left, int loffset, int llength, byte[] right,
    int roffset, int rlength) {
    if (left.hasArray()) {
      return compareBytesRows(left.array(), left.arrayOffset() + loffset, llength, right, roffset,
        rlength);
    }
    return compareRows(left, loffset, llength, right, roffset, rlength,
      ByteBufferUtils::searchDelimiterIndex, Bytes::searchDelimiterIndex,
      ByteBufferUtils::searchDelimiterIndexInReverse, Bytes::searchDelimiterIndexInReverse,
      ByteBufferUtils::compareTo);
  }

  private static int compareBytesRows(byte[] left, int loffset, int llength, byte[] right,
    int roffset, int rlength) {
    return compareRows(left, loffset, llength, right, roffset, rlength, Bytes::searchDelimiterIndex,
      Bytes::searchDelimiterIndex, Bytes::searchDelimiterIndexInReverse,
      Bytes::searchDelimiterIndexInReverse, Bytes::compareTo);
  }

  @Override
  public int compareRows(ByteBuffer row, Cell cell) {
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferExtendedCell bbCell = (ByteBufferExtendedCell) cell;
      return compareBBRows(row, row.position(), row.remaining(), bbCell.getRowByteBuffer(),
        bbCell.getRowPosition(), cell.getRowLength());
    } else {
      return compareBBAndBytesRows(row, row.position(), row.remaining(), cell.getRowArray(),
        cell.getRowOffset(), cell.getRowLength());
    }
  }

  @Override
  public Comparator getSimpleComparator() {
    return this;
  }

}
