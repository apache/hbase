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

import java.io.Serializable;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.primitives.Longs;

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
public class CellComparator implements Comparator<Cell>, Serializable {
  static final Log LOG = LogFactory.getLog(CellComparator.class);
  private static final long serialVersionUID = -8760041766259623329L;

  /**
   * Comparator for plain key/values; i.e. non-catalog table key/values. Works on Key portion
   * of KeyValue only.
   */
  public static final CellComparator COMPARATOR = new CellComparator();
  /**
   * A {@link CellComparator} for <code>hbase:meta</code> catalog table
   * {@link KeyValue}s.
   */
  public static final CellComparator META_COMPARATOR = new MetaCellComparator();

  @Override
  public int compare(Cell a, Cell b) {
    return compare(a, b, false);
  }

  /**
   * Compares only the key portion of a cell. It does not include the sequence id/mvcc of the
   * cell 
   * @param left
   * @param right
   * @return an int greater than 0 if left &gt; than right
   *                lesser than 0 if left &lt; than right
   *                equal to 0 if left is equal to right
   */
  public final int compareKeyIgnoresMvcc(Cell left, Cell right) {
    return compare(left, right, true);
  }

  /**
   * Used when a cell needs to be compared with a key byte[] such as cases of
   * finding the index from the index block, bloom keys from the bloom blocks
   * This byte[] is expected to be serialized in the KeyValue serialization format
   * If the KeyValue (Cell's) serialization format changes this method cannot be used.
   * @param left the cell to be compared
   * @param key the serialized key part of a KeyValue
   * @param offset the offset in the key byte[]
   * @param length the length of the key byte[]
   * @return an int greater than 0 if left is greater than right
   *                lesser than 0 if left is lesser than right
   *                equal to 0 if left is equal to right
   */
  public final int compare(Cell left, byte[] key, int offset, int length) {
    // row
    short rrowlength = Bytes.toShort(key, offset);
    int c = compareRows(left, key, offset + Bytes.SIZEOF_SHORT, rrowlength);
    if (c != 0) return c;

    // Compare the rest of the two KVs without making any assumptions about
    // the common prefix. This function will not compare rows anyway, so we
    // don't need to tell it that the common prefix includes the row.
    return compareWithoutRow(left, key, offset, length, rrowlength);
  }

  /**
   * Compare cells.
   * @param a
   * @param b
   * @param ignoreSequenceid True if we are to compare the key portion only and ignore
   * the sequenceid. Set to false to compare key and consider sequenceid.
   * @return 0 if equal, -1 if a &lt; b, and +1 if a &gt; b.
   */
  private final int compare(final Cell a, final Cell b, boolean ignoreSequenceid) {
    // row
    int c = compareRows(a, b);
    if (c != 0) return c;

    c = compareWithoutRow(a, b);
    if(c != 0) return c;

    if (!ignoreSequenceid) {
      // Negate following comparisons so later edits show up first
      // mvccVersion: later sorts first
      return Longs.compare(b.getSequenceId(), a.getSequenceId());
    } else {
      return c;
    }
  }

  /**
   * Compares the family and qualifier part of the cell
   * @param left the left cell
   * @param right the right cell
   * @return 0 if both cells are equal, 1 if left cell is bigger than right, -1 otherwise
   */
  public final static int compareColumns(final Cell left, final Cell right) {
    int diff = compareFamilies(left, right);
    if (diff != 0) {
      return diff;
    }
    return compareQualifiers(left, right);
  }

  private final static int compareColumns(Cell left, byte[] right, int rfoffset, int rflength,
      int rqoffset, int rqlength) {
    int diff = compareFamilies(left, right, rfoffset, rflength);
    if (diff != 0)
      return diff;
    return compareQualifiers(left, right, rqoffset, rqlength);
  }

  /**
   * Compare the families of left and right cell
   * @param left
   * @param right
   * @return 0 if both cells are equal, 1 if left cell is bigger than right, -1 otherwise
   */
  public final static int compareFamilies(Cell left, Cell right) {
    if (left instanceof ByteBufferedCell && right instanceof ByteBufferedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferedCell) left).getFamilyByteBuffer(),
          ((ByteBufferedCell) left).getFamilyPosition(), left.getFamilyLength(),
          ((ByteBufferedCell) right).getFamilyByteBuffer(),
          ((ByteBufferedCell) right).getFamilyPosition(), right.getFamilyLength());
    }
    if (left instanceof ByteBufferedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferedCell) left).getFamilyByteBuffer(),
          ((ByteBufferedCell) left).getFamilyPosition(), left.getFamilyLength(),
          right.getFamilyArray(), right.getFamilyOffset(), right.getFamilyLength());
    }
    if (right instanceof ByteBufferedCell) {
      return -(ByteBufferUtils.compareTo(((ByteBufferedCell) right).getFamilyByteBuffer(),
          ((ByteBufferedCell) right).getFamilyPosition(), right.getFamilyLength(),
          left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength()));
    }
    return Bytes.compareTo(left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(),
        right.getFamilyArray(), right.getFamilyOffset(), right.getFamilyLength());
  }

  private final static int compareFamilies(Cell left, byte[] right, int roffset, int rlength) {
    if (left instanceof ByteBufferedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferedCell) left).getFamilyByteBuffer(),
          ((ByteBufferedCell) left).getFamilyPosition(), left.getFamilyLength(), right,
          roffset, rlength);
    }
    return Bytes.compareTo(left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(),
        right, roffset, rlength);
  }

  /**
   * Compare the qualifiers part of the left and right cells.
   * @param left
   * @param right
   * @return 0 if both cells are equal, 1 if left cell is bigger than right, -1 otherwise
   */
  public final static int compareQualifiers(Cell left, Cell right) {
    if (left instanceof ByteBufferedCell && right instanceof ByteBufferedCell) {
      return ByteBufferUtils
          .compareTo(((ByteBufferedCell) left).getQualifierByteBuffer(),
              ((ByteBufferedCell) left).getQualifierPosition(),
              left.getQualifierLength(), ((ByteBufferedCell) right).getQualifierByteBuffer(),
              ((ByteBufferedCell) right).getQualifierPosition(),
              right.getQualifierLength());
    }
    if (left instanceof ByteBufferedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferedCell) left).getQualifierByteBuffer(),
          ((ByteBufferedCell) left).getQualifierPosition(), left.getQualifierLength(),
          right.getQualifierArray(), right.getQualifierOffset(), right.getQualifierLength());
    }
    if (right instanceof ByteBufferedCell) {
      return -(ByteBufferUtils.compareTo(((ByteBufferedCell) right).getQualifierByteBuffer(),
          ((ByteBufferedCell) right).getQualifierPosition(),
          right.getQualifierLength(), left.getQualifierArray(), left.getQualifierOffset(),
          left.getQualifierLength()));
    }
    return Bytes.compareTo(left.getQualifierArray(), left.getQualifierOffset(),
        left.getQualifierLength(), right.getQualifierArray(), right.getQualifierOffset(),
        right.getQualifierLength());
  }

  public final static int compareQualifiers(Cell left, byte[] right, int rOffset, int rLength) {
    if (left instanceof ByteBufferedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferedCell) left).getQualifierByteBuffer(),
          ((ByteBufferedCell) left).getQualifierPosition(), left.getQualifierLength(),
          right, rOffset, rLength);
    }
    return Bytes.compareTo(left.getQualifierArray(), left.getQualifierOffset(),
        left.getQualifierLength(), right, rOffset, rLength);
  }

  /**
   * Compare columnFamily, qualifier, timestamp, and key type (everything
   * except the row). This method is used both in the normal comparator and
   * the "same-prefix" comparator. Note that we are assuming that row portions
   * of both KVs have already been parsed and found identical, and we don't
   * validate that assumption here.
   * @param commonPrefix
   *          the length of the common prefix of the two key-values being
   *          compared, including row length and row
   */
  private final int compareWithoutRow(Cell left,
      byte[] right, int roffset, int rlength, short rowlength) {
    /***
     * KeyValue Format and commonLength:
     * |_keyLen_|_valLen_|_rowLen_|_rowKey_|_famiLen_|_fami_|_Quali_|....
     * ------------------|-------commonLength--------|--------------
     */
    int commonLength = KeyValue.ROW_LENGTH_SIZE + KeyValue.FAMILY_LENGTH_SIZE + rowlength;

    // commonLength + TIMESTAMP_TYPE_SIZE
    int commonLengthWithTSAndType = KeyValue.TIMESTAMP_TYPE_SIZE + commonLength;
    // ColumnFamily + Qualifier length.
    int lcolumnlength = left.getFamilyLength() + left.getQualifierLength();
    int rcolumnlength = rlength - commonLengthWithTSAndType;

    byte ltype = left.getTypeByte();
    byte rtype = right[roffset + (rlength - 1)];

    // If the column is not specified, the "minimum" key type appears the
    // latest in the sorted order, regardless of the timestamp. This is used
    // for specifying the last key/value in a given row, because there is no
    // "lexicographically last column" (it would be infinitely long). The
    // "maximum" key type does not need this behavior.
    if (lcolumnlength == 0 && ltype == Type.Minimum.getCode()) {
      // left is "bigger", i.e. it appears later in the sorted order
      return 1;
    }
    if (rcolumnlength == 0 && rtype == Type.Minimum.getCode()) {
      return -1;
    }

    int rfamilyoffset = commonLength + roffset;

    // Column family length.
    int lfamilylength = left.getFamilyLength();
    int rfamilylength = right[rfamilyoffset - 1];
    // If left family size is not equal to right family size, we need not
    // compare the qualifiers.
    boolean sameFamilySize = (lfamilylength == rfamilylength);
    if (!sameFamilySize) {
      // comparing column family is enough.
      return compareFamilies(left, right, rfamilyoffset, rfamilylength);
    }
    // Compare family & qualifier together.
    // Families are same. Compare on qualifiers.
    int comparison = compareColumns(left, right, rfamilyoffset, rfamilylength, rfamilyoffset
        + rfamilylength, (rcolumnlength - rfamilylength));
    if (comparison != 0) {
      return comparison;
    }

    // //
    // Next compare timestamps.
    long rtimestamp = Bytes.toLong(right, roffset + (rlength - KeyValue.TIMESTAMP_TYPE_SIZE));
    int compare = compareTimestamps(left.getTimestamp(), rtimestamp);
    if (compare != 0) {
      return compare;
    }

    // Compare types. Let the delete types sort ahead of puts; i.e. types
    // of higher numbers sort before those of lesser numbers. Maximum (255)
    // appears ahead of everything, and minimum (0) appears after
    // everything.
    return (0xff & rtype) - (0xff & ltype);
  }

  /**
   * Compares the rows of the left and right cell.
   * For the hbase:meta case this method is overridden such that it can handle hbase:meta cells.
   * The caller should ensure using the appropriate comparator for hbase:meta.
   * @param left
   * @param right
   * @return 0 if both cells are equal, 1 if left cell is bigger than right, -1 otherwise
   */
  public int compareRows(final Cell left, final Cell right) {
    // left and right can be exactly the same at the beginning of a row
    if (left == right) {
      return 0;
    }
    if (left instanceof ByteBufferedCell && right instanceof ByteBufferedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferedCell) left).getRowByteBuffer(),
          ((ByteBufferedCell) left).getRowPosition(), left.getRowLength(),
          ((ByteBufferedCell) right).getRowByteBuffer(),
          ((ByteBufferedCell) right).getRowPosition(), right.getRowLength());
    }
    if (left instanceof ByteBufferedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferedCell) left).getRowByteBuffer(),
          ((ByteBufferedCell) left).getRowPosition(), left.getRowLength(),
          right.getRowArray(), right.getRowOffset(), right.getRowLength());
    }
    if (right instanceof ByteBufferedCell) {
      return -(ByteBufferUtils.compareTo(((ByteBufferedCell) right).getRowByteBuffer(),
          ((ByteBufferedCell) right).getRowPosition(), right.getRowLength(),
          left.getRowArray(), left.getRowOffset(), left.getRowLength()));
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
  public int compareRows(Cell left, byte[] right, int roffset, int rlength) {
    if (left instanceof ByteBufferedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferedCell) left).getRowByteBuffer(),
          ((ByteBufferedCell) left).getRowPosition(), left.getRowLength(), right,
          roffset, rlength);
    }
    return Bytes.compareTo(left.getRowArray(), left.getRowOffset(), left.getRowLength(), right,
        roffset, rlength);
  }

  private static int compareWithoutRow(final Cell left, final Cell right) {
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
    if (diff != 0) return diff;

    diff = compareTimestamps(left, right);
    if (diff != 0) return diff;

    // Compare types. Let the delete types sort ahead of puts; i.e. types
    // of higher numbers sort before those of lesser numbers. Maximum (255)
    // appears ahead of everything, and minimum (0) appears after
    // everything.
    return (0xff & right.getTypeByte()) - (0xff & left.getTypeByte());
  }

  /**
   * Compares cell's timestamps in DESCENDING order.
   * The below older timestamps sorting ahead of newer timestamps looks
   * wrong but it is intentional. This way, newer timestamps are first
   * found when we iterate over a memstore and newer versions are the
   * first we trip over when reading from a store file.
   * @return 1 if left's timestamp &lt; right's timestamp
   *         -1 if left's timestamp &gt; right's timestamp
   *         0 if both timestamps are equal
   */
  public static int compareTimestamps(final Cell left, final Cell right) {
    return compareTimestamps(left.getTimestamp(), right.getTimestamp());
  }

  /**
   * Used to compare two cells based on the column hint provided. This is specifically
   * used when we need to optimize the seeks based on the next indexed key. This is an
   * advance usage API specifically needed for some optimizations.
   * @param nextIndexedCell the next indexed cell 
   * @param currentCell the cell to be compared
   * @param foff the family offset of the currentCell
   * @param flen the family length of the currentCell
   * @param colHint the column hint provided - could be null
   * @param coff the offset of the column hint if provided, if not offset of the currentCell's
   * qualifier
   * @param clen the length of the column hint if provided, if not length of the currentCell's
   * qualifier
   * @param ts the timestamp to be seeked
   * @param type the type to be seeked
   * @return an int based on the given column hint
   * TODO : To be moved out of here because this is a special API used in scan
   * optimization.
   */
  // compare a key against row/fam/qual/ts/type
  public final int compareKeyBasedOnColHint(Cell nextIndexedCell, Cell currentCell, int foff,
      int flen, byte[] colHint, int coff, int clen, long ts, byte type) {
    int compare = compareRows(nextIndexedCell, currentCell);
    if (compare != 0) {
      return compare;
    }
    // If the column is not specified, the "minimum" key type appears the
    // latest in the sorted order, regardless of the timestamp. This is used
    // for specifying the last key/value in a given row, because there is no
    // "lexicographically last column" (it would be infinitely long). The
    // "maximum" key type does not need this behavior.
    if (nextIndexedCell.getFamilyLength() + nextIndexedCell.getQualifierLength() == 0
        && nextIndexedCell.getTypeByte() == Type.Minimum.getCode()) {
      // left is "bigger", i.e. it appears later in the sorted order
      return 1;
    }
    if (flen + clen == 0 && type == Type.Minimum.getCode()) {
      return -1;
    }

    compare = compareFamilies(nextIndexedCell, currentCell);
    if (compare != 0) {
      return compare;
    }
    if (colHint == null) {
      compare = compareQualifiers(nextIndexedCell, currentCell);
    } else {
      compare = compareQualifiers(nextIndexedCell, colHint, coff, clen);
    }
    if (compare != 0) {
      return compare;
    }
    // Next compare timestamps.
    compare = compareTimestamps(nextIndexedCell.getTimestamp(), ts);
    if (compare != 0) {
      return compare;
    }

    // Compare types. Let the delete types sort ahead of puts; i.e. types
    // of higher numbers sort before those of lesser numbers. Maximum (255)
    // appears ahead of everything, and minimum (0) appears after
    // everything.
    return (0xff & type) - (0xff & nextIndexedCell.getTypeByte());
  }

  /**
   * Compares timestamps in DESCENDING order.
   * The below older timestamps sorting ahead of newer timestamps looks
   * wrong but it is intentional. This way, newer timestamps are first
   * found when we iterate over a memstore and newer versions are the
   * first we trip over when reading from a store file.
   * @return 1 if left timestamp &lt; right timestamp
   *         -1 if left timestamp &gt; right timestamp
   *         0 if both timestamps are equal
   */
  public static int compareTimestamps(final long ltimestamp, final long rtimestamp) {
    if (ltimestamp < rtimestamp) {
      return 1;
    } else if (ltimestamp > rtimestamp) {
      return -1;
    }
    return 0;
  }

  /**
   * Compare cell's row against given comparator
   * @param cell
   * @param comparator
   * @return result comparing cell's row
   */
  public static int compareRow(Cell cell, ByteArrayComparable comparator) {
    if (cell instanceof ByteBufferedCell) {
      return comparator.compareTo(((ByteBufferedCell) cell).getRowByteBuffer(),
          ((ByteBufferedCell) cell).getRowPosition(), cell.getRowLength());
    }
    return comparator.compareTo(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
  }

  /**
   * Compare cell's column family against given comparator
   * @param cell
   * @param comparator
   * @return result comparing cell's column family
   */
  public static int compareFamily(Cell cell, ByteArrayComparable comparator) {
    if (cell instanceof ByteBufferedCell) {
      return comparator.compareTo(((ByteBufferedCell) cell).getFamilyByteBuffer(),
          ((ByteBufferedCell) cell).getFamilyPosition(), cell.getFamilyLength());
    }
    return comparator.compareTo(cell.getFamilyArray(), cell.getFamilyOffset(),
        cell.getFamilyLength());
  }

  /**
   * Compare cell's qualifier against given comparator
   * @param cell
   * @param comparator
   * @return result comparing cell's qualifier
   */
  public static int compareQualifier(Cell cell, ByteArrayComparable comparator) {
    if (cell instanceof ByteBufferedCell) {
      return comparator.compareTo(((ByteBufferedCell) cell).getQualifierByteBuffer(),
          ((ByteBufferedCell) cell).getQualifierPosition(), cell.getQualifierLength());
    }
    return comparator.compareTo(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength());
  }

  /**
   * Compare cell's value against given comparator
   * @param cell
   * @param comparator
   * @return result comparing cell's value
   */
  public static int compareValue(Cell cell, ByteArrayComparable comparator) {
    if (cell instanceof ByteBufferedCell) {
      return comparator.compareTo(((ByteBufferedCell) cell).getValueByteBuffer(),
          ((ByteBufferedCell) cell).getValuePosition(), cell.getValueLength());
    }
    return comparator.compareTo(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
  }

  /**
   * Comparator that compares row component only of a Cell
   */
  public static class RowComparator extends CellComparator {
    @Override
    public int compare(Cell a, Cell b) {
      return compareRows(a, b);
    }
  }

  /**
   * A {@link CellComparator} for <code>hbase:meta</code> catalog table
   * {@link KeyValue}s.
   */
  public static class MetaCellComparator extends CellComparator {

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

    private int compareRows(byte[] left, int loffset, int llength, byte[] right, int roffset,
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
