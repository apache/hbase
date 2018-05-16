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

import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.primitives.Longs;

/**
 * Compare two HBase cells.  Do not use this method comparing <code>-ROOT-</code> or
 * <code>hbase:meta</code> cells.  Cells from these tables need a specialized comparator, one that
 * takes account of the special formatting of the row where we have commas to delimit table from
 * regionname, from row.  See KeyValue for how it has a special comparator to do hbase:meta cells
 * and yet another for -ROOT-.
 * While using this comparator for {{@link #compareRows(Cell, Cell)} et al, the hbase:meta cells
 * format should be taken into consideration, for which the instance of this comparator
 * should be used.  In all other cases the static APIs in this comparator would be enough
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value="UNKNOWN",
    justification="Findbugs doesn't like the way we are negating the result of a compare in below")
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CellComparatorImpl implements CellComparator {
  static final Logger LOG = LoggerFactory.getLogger(CellComparatorImpl.class);
  /**
   * Comparator for plain key/values; i.e. non-catalog table key/values. Works on Key portion
   * of KeyValue only.
   */
  public static final CellComparatorImpl COMPARATOR = new CellComparatorImpl();
  /**
   * A {@link CellComparatorImpl} for <code>hbase:meta</code> catalog table
   * {@link KeyValue}s.
   */
  public static final CellComparatorImpl META_COMPARATOR = new MetaCellComparator();

  @Override
  public int compare(Cell a, Cell b) {
    return compare(a, b, false);
  }

  /**
   * Compare cells.
   * @param ignoreSequenceid True if we are to compare the key portion only and ignore
   *  the sequenceid. Set to false to compare key and consider sequenceid.
   * @return 0 if equal, -1 if a &lt; b, and +1 if a &gt; b.
   */
  @Override
  public int compare(final Cell a, final Cell b, boolean ignoreSequenceid) {
    int diff = 0;
    if (a instanceof ByteBufferKeyValue && b instanceof ByteBufferKeyValue) {
      diff = compareByteBufferKeyValue((ByteBufferKeyValue)a, (ByteBufferKeyValue)b);
      if (diff != 0) {
        return diff;
      }
    } else {
      diff = compareRows(a, b);
      if (diff != 0) {
        return diff;
      }

      diff = compareWithoutRow(a, b);
      if (diff != 0) {
        return diff;
      }
    }

    // Negate following comparisons so later edits show up first mvccVersion: later sorts first
    return ignoreSequenceid? diff: Longs.compare(b.getSequenceId(), a.getSequenceId());
  }

  /**
   * Specialized comparator for the ByteBufferKeyValue type exclusivesly.
   * Caches deserialized lengths of rows and families, etc., and reuses them where it can
   * (ByteBufferKeyValue has been changed to be amenable to our providing pre-made lengths, etc.)
   */
  private static final int compareByteBufferKeyValue(ByteBufferKeyValue left,
      ByteBufferKeyValue right) {
    // Compare Rows. Cache row length.
    int leftRowLength = left.getRowLength();
    int rightRowLength = right.getRowLength();
    int diff = ByteBufferUtils.compareTo(
        left.getRowByteBuffer(), left.getRowPosition(), leftRowLength,
        right.getRowByteBuffer(), right.getRowPosition(), rightRowLength);
    if (diff != 0) {
      return diff;
    }

    // If the column is not specified, the "minimum" key type appears the
    // latest in the sorted order, regardless of the timestamp. This is used
    // for specifying the last key/value in a given row, because there is no
    // "lexicographically last column" (it would be infinitely long). The
    // "maximum" key type does not need this behavior.
    // Copied from KeyValue. This is bad in that we can't do memcmp w/ special rules like this.
    // I tried to get rid of the above but scanners depend on it. TODO.
    int leftFamilyLengthPosition = left.getFamilyLengthPosition(leftRowLength);
    int leftFamilyLength = left.getFamilyLength(leftFamilyLengthPosition);
    int rightFamilyLengthPosition = right.getFamilyLengthPosition(rightRowLength);
    int rightFamilyLength = right.getFamilyLength(rightFamilyLengthPosition);
    int leftKeyLength = left.getKeyLength();
    int leftQualifierLength = left.getQualifierLength(leftKeyLength, leftRowLength,
        leftFamilyLength);
    byte leftType = left.getTypeByte(leftKeyLength);
    if (leftFamilyLength + leftQualifierLength == 0 && leftType == Type.Minimum.getCode()) {
      // left is "bigger", i.e. it appears later in the sorted order
      return 1;
    }
    int rightKeyLength = right.getKeyLength();
    int rightQualifierLength = right.getQualifierLength(rightKeyLength, rightRowLength,
        rightFamilyLength);
    byte rightType = right.getTypeByte(rightKeyLength);
    if (rightFamilyLength + rightQualifierLength == 0 && rightType == Type.Minimum.getCode()) {
      return -1;
    }

    // Compare families.
    int leftFamilyPosition = left.getFamilyPosition(leftFamilyLengthPosition);
    int rightFamilyPosition = right.getFamilyPosition(rightFamilyLengthPosition);
    diff = ByteBufferUtils.compareTo(
        left.getFamilyByteBuffer(), leftFamilyPosition, leftFamilyLength,
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
    diff = compareTimestampsInternal(left.getTimestamp(leftKeyLength),
        right.getTimestamp(rightKeyLength));
    if (diff != 0) {
      return diff;
    }
    // Compare types. Let the delete types sort ahead of puts; i.e. types
    // of higher numbers sort before those of lesser numbers. Maximum (255)
    // appears ahead of everything, and minimum (0) appears after
    // everything.
    return (0xff & rightType) - (0xff & leftType);
  }

  /**
   * Compares the family and qualifier part of the cell
   * @return 0 if both cells are equal, 1 if left cell is bigger than right, -1 otherwise
   */
  public final int compareColumns(final Cell left, final Cell right) {
    int diff = compareFamilies(left, right);
    if (diff != 0) {
      return diff;
    }
    return compareQualifiers(left, right);
  }

  /**
   * Compare the families of left and right cell
   * @return 0 if both cells are equal, 1 if left cell is bigger than right, -1 otherwise
   */
  @Override
  public final int compareFamilies(Cell left, Cell right) {
    if (left instanceof ByteBufferExtendedCell && right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) left).getFamilyPosition(), left.getFamilyLength(),
          ((ByteBufferExtendedCell) right).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) right).getFamilyPosition(), right.getFamilyLength());
    }
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) left).getFamilyPosition(), left.getFamilyLength(),
          right.getFamilyArray(), right.getFamilyOffset(), right.getFamilyLength());
    }
    if (right instanceof ByteBufferExtendedCell) {
      // Notice how we flip the order of the compare here. We used to negate the return value but
      // see what FindBugs says
      // http://findbugs.sourceforge.net/bugDescriptions.html#RV_NEGATING_RESULT_OF_COMPARETO
      // It suggest flipping the order to get same effect and 'safer'.
      return ByteBufferUtils.compareTo(
          left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(),
          ((ByteBufferExtendedCell)right).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell)right).getFamilyPosition(), right.getFamilyLength());
    }
    return Bytes.compareTo(left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(),
        right.getFamilyArray(), right.getFamilyOffset(), right.getFamilyLength());
  }

  /**
   * Compare the qualifiers part of the left and right cells.
   * @return 0 if both cells are equal, 1 if left cell is bigger than right, -1 otherwise
   */
  @Override
  public final int compareQualifiers(Cell left, Cell right) {
    if (left instanceof ByteBufferExtendedCell && right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils
          .compareTo(((ByteBufferExtendedCell) left).getQualifierByteBuffer(),
              ((ByteBufferExtendedCell) left).getQualifierPosition(),
              left.getQualifierLength(), ((ByteBufferExtendedCell) right).getQualifierByteBuffer(),
              ((ByteBufferExtendedCell) right).getQualifierPosition(),
              right.getQualifierLength());
    }
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) left).getQualifierPosition(), left.getQualifierLength(),
          right.getQualifierArray(), right.getQualifierOffset(), right.getQualifierLength());
    }
    if (right instanceof ByteBufferExtendedCell) {
      // Notice how we flip the order of the compare here. We used to negate the return value but
      // see what FindBugs says
      // http://findbugs.sourceforge.net/bugDescriptions.html#RV_NEGATING_RESULT_OF_COMPARETO
      // It suggest flipping the order to get same effect and 'safer'.
      return ByteBufferUtils.compareTo(left.getQualifierArray(),
          left.getQualifierOffset(), left.getQualifierLength(),
          ((ByteBufferExtendedCell)right).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell)right).getQualifierPosition(), right.getQualifierLength());
    }
    return Bytes.compareTo(left.getQualifierArray(), left.getQualifierOffset(),
        left.getQualifierLength(), right.getQualifierArray(), right.getQualifierOffset(),
        right.getQualifierLength());
  }

  /**
   * Compares the rows of the left and right cell.
   * For the hbase:meta case this method is overridden such that it can handle hbase:meta cells.
   * The caller should ensure using the appropriate comparator for hbase:meta.
   * @return 0 if both cells are equal, 1 if left cell is bigger than right, -1 otherwise
   */
  @Override
  public int compareRows(final Cell left, final Cell right) {
    return compareRows(left, left.getRowLength(), right, right.getRowLength());
  }

  int compareRows(final Cell left, int leftRowLength, final Cell right, int rightRowLength) {
    // left and right can be exactly the same at the beginning of a row
    if (left == right) {
      return 0;
    }
    if (left instanceof ByteBufferExtendedCell && right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getRowByteBuffer(),
          ((ByteBufferExtendedCell) left).getRowPosition(), leftRowLength,
          ((ByteBufferExtendedCell) right).getRowByteBuffer(),
          ((ByteBufferExtendedCell) right).getRowPosition(), rightRowLength);
    }
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getRowByteBuffer(),
          ((ByteBufferExtendedCell) left).getRowPosition(), leftRowLength,
          right.getRowArray(), right.getRowOffset(), rightRowLength);
    }
    if (right instanceof ByteBufferExtendedCell) {
      // Notice how we flip the order of the compare here. We used to negate the return value but
      // see what FindBugs says
      // http://findbugs.sourceforge.net/bugDescriptions.html#RV_NEGATING_RESULT_OF_COMPARETO
      // It suggest flipping the order to get same effect and 'safer'.
      return ByteBufferUtils.compareTo(left.getRowArray(), left.getRowOffset(), leftRowLength,
          ((ByteBufferExtendedCell)right).getRowByteBuffer(),
          ((ByteBufferExtendedCell)right).getRowPosition(), rightRowLength);
    }
    return Bytes.compareTo(left.getRowArray(), left.getRowOffset(), left.getRowLength(),
        right.getRowArray(), right.getRowOffset(), right.getRowLength());
  }

  /**
   * Compares the row part of the cell with a simple plain byte[] like the
   * stopRow in Scan. This should be used with context where for hbase:meta
   * cells the {{@link #META_COMPARATOR} should be used
   *
   * @param left
   *          the cell to be compared
   * @param right
   *          the kv serialized byte[] to be compared with
   * @param roffset
   *          the offset in the byte[]
   * @param rlength
   *          the length in the byte[]
   * @return 0 if both cell and the byte[] are equal, 1 if the cell is bigger
   *         than byte[], -1 otherwise
   */
  @Override
  public int compareRows(Cell left, byte[] right, int roffset, int rlength) {
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getRowByteBuffer(),
          ((ByteBufferExtendedCell) left).getRowPosition(), left.getRowLength(), right,
          roffset, rlength);
    }
    return Bytes.compareTo(left.getRowArray(), left.getRowOffset(), left.getRowLength(), right,
        roffset, rlength);
  }

  @Override
  public final int compareWithoutRow(final Cell left, final Cell right) {
    // If the column is not specified, the "minimum" key type appears the
    // latest in the sorted order, regardless of the timestamp. This is used
    // for specifying the last key/value in a given row, because there is no
    // "lexicographically last column" (it would be infinitely long). The
    // "maximum" key type does not need this behavior.
    // Copied from KeyValue. This is bad in that we can't do memcmp w/ special rules like this.
    int lFamLength = left.getFamilyLength();
    int rFamLength = right.getFamilyLength();
    int lQualLength = left.getQualifierLength();
    int rQualLength = right.getQualifierLength();
    if (lFamLength + lQualLength == 0
          && left.getTypeByte() == Type.Minimum.getCode()) {
      // left is "bigger", i.e. it appears later in the sorted order
      return 1;
    }
    if (rFamLength + rQualLength == 0
        && right.getTypeByte() == Type.Minimum.getCode()) {
      return -1;
    }
    if (lFamLength != rFamLength) {
      // comparing column family is enough.
      return compareFamilies(left, right);
    }
    // Compare cf:qualifier
    int diff = compareColumns(left, right);
    if (diff != 0) {
      return diff;
    }

    diff = compareTimestamps(left, right);
    if (diff != 0) {
      return diff;
    }

    // Compare types. Let the delete types sort ahead of puts; i.e. types
    // of higher numbers sort before those of lesser numbers. Maximum (255)
    // appears ahead of everything, and minimum (0) appears after
    // everything.
    return (0xff & right.getTypeByte()) - (0xff & left.getTypeByte());
  }

  @Override
  public int compareTimestamps(final Cell left, final Cell right) {
    return compareTimestampsInternal(left.getTimestamp(), right.getTimestamp());
  }

  @Override
  public int compareTimestamps(final long ltimestamp, final long rtimestamp) {
    return compareTimestampsInternal(ltimestamp, rtimestamp);
  }

  private static final int compareTimestampsInternal(final long ltimestamp, final long rtimestamp) {
    // Swap the times so sort is newest to oldest, descending.
    return Long.compare(rtimestamp, ltimestamp);
  }

  /**
   * A {@link CellComparatorImpl} for <code>hbase:meta</code> catalog table
   * {@link KeyValue}s.
   */
  public static class MetaCellComparator extends CellComparatorImpl {
    @Override
    public int compareRows(final Cell left, final Cell right) {
      return compareRows(left.getRowArray(), left.getRowOffset(), left.getRowLength(),
          right.getRowArray(), right.getRowOffset(), right.getRowLength());
    }

    @Override
    public int compareRows(Cell left, byte[] right, int roffset, int rlength) {
      return compareRows(left.getRowArray(), left.getRowOffset(), left.getRowLength(), right,
          roffset, rlength);
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

      // Negate following comparisons so later edits show up first mvccVersion: later sorts first
      return ignoreSequenceid? diff: Longs.compare(b.getSequenceId(), a.getSequenceId());
    }

    private static int compareRows(byte[] left, int loffset, int llength, byte[] right, int roffset,
        int rlength) {
      int leftDelimiter = Bytes.searchDelimiterIndex(left, loffset, llength, HConstants.DELIMITER);
      int rightDelimiter = Bytes
          .searchDelimiterIndex(right, roffset, rlength, HConstants.DELIMITER);
      // Compare up to the delimiter
      int lpart = (leftDelimiter < 0 ? llength : leftDelimiter - loffset);
      int rpart = (rightDelimiter < 0 ? rlength : rightDelimiter - roffset);
      int result = Bytes.compareTo(left, loffset, lpart, right, roffset, rpart);
      if (result != 0) {
        return result;
      } else {
        if (leftDelimiter < 0 && rightDelimiter >= 0) {
          return -1;
        } else if (rightDelimiter < 0 && leftDelimiter >= 0) {
          return 1;
        } else if (leftDelimiter < 0 && rightDelimiter < 0) {
          return 0;
        }
      }
      // Compare middle bit of the row.
      // Move past delimiter
      leftDelimiter++;
      rightDelimiter++;
      int leftFarDelimiter = Bytes.searchDelimiterIndexInReverse(left, leftDelimiter, llength
          - (leftDelimiter - loffset), HConstants.DELIMITER);
      int rightFarDelimiter = Bytes.searchDelimiterIndexInReverse(right, rightDelimiter, rlength
          - (rightDelimiter - roffset), HConstants.DELIMITER);
      // Now compare middlesection of row.
      lpart = (leftFarDelimiter < 0 ? llength + loffset : leftFarDelimiter) - leftDelimiter;
      rpart = (rightFarDelimiter < 0 ? rlength + roffset : rightFarDelimiter) - rightDelimiter;
      result = Bytes.compareTo(left, leftDelimiter, lpart, right, rightDelimiter, rpart);
      if (result != 0) {
        return result;
      } else {
        if (leftDelimiter < 0 && rightDelimiter >= 0) {
          return -1;
        } else if (rightDelimiter < 0 && leftDelimiter >= 0) {
          return 1;
        } else if (leftDelimiter < 0 && rightDelimiter < 0) {
          return 0;
        }
      }
      // Compare last part of row, the rowid.
      leftFarDelimiter++;
      rightFarDelimiter++;
      result = Bytes.compareTo(left, leftFarDelimiter, llength - (leftFarDelimiter - loffset),
          right, rightFarDelimiter, rlength - (rightFarDelimiter - roffset));
      return result;
    }
  }
}
