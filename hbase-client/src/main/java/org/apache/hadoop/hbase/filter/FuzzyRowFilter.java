/**
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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.BytesBytesPair;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.UnsafeAccess;
import org.apache.hadoop.hbase.util.UnsafeAvailChecker;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * This is optimized version of a standard FuzzyRowFilter Filters data based on fuzzy row key.
 * Performs fast-forwards during scanning. It takes pairs (row key, fuzzy info) to match row keys.
 * Where fuzzy info is a byte array with 0 or 1 as its values:
 * <ul>
 * <li>0 - means that this byte in provided row key is fixed, i.e. row key's byte at same position
 * must match</li>
 * <li>1 - means that this byte in provided row key is NOT fixed, i.e. row key's byte at this
 * position can be different from the one in provided row key</li>
 * </ul>
 * Example: Let's assume row key format is userId_actionId_year_month. Length of userId is fixed and
 * is 4, length of actionId is 2 and year and month are 4 and 2 bytes long respectively. Let's
 * assume that we need to fetch all users that performed certain action (encoded as "99") in Jan of
 * any year. Then the pair (row key, fuzzy info) would be the following: row key = "????_99_????_01"
 * (one can use any value instead of "?") fuzzy info =
 * "\x01\x01\x01\x01\x00\x00\x00\x00\x01\x01\x01\x01\x00\x00\x00" I.e. fuzzy info tells the matching
 * mask is "????_99_????_01", where at ? can be any value.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class FuzzyRowFilter extends FilterBase {
  private static final boolean UNSAFE_UNALIGNED = UnsafeAvailChecker.unaligned();
  private List<Pair<byte[], byte[]>> fuzzyKeysData;
  private boolean done = false;

  /**
   * The index of a last successfully found matching fuzzy string (in fuzzyKeysData). We will start
   * matching next KV with this one. If they do not match then we will return back to the one-by-one
   * iteration over fuzzyKeysData.
   */
  private int lastFoundIndex = -1;

  /**
   * Row tracker (keeps all next rows after SEEK_NEXT_USING_HINT was returned)
   */
  private RowTracker tracker;

  public FuzzyRowFilter(List<Pair<byte[], byte[]>> fuzzyKeysData) {
    Pair<byte[], byte[]> p;
    for (int i = 0; i < fuzzyKeysData.size(); i++) {
      p = fuzzyKeysData.get(i);
      if (p.getFirst().length != p.getSecond().length) {
        Pair<String, String> readable =
            new Pair<String, String>(Bytes.toStringBinary(p.getFirst()), Bytes.toStringBinary(p
                .getSecond()));
        throw new IllegalArgumentException("Fuzzy pair lengths do not match: " + readable);
      }
      // update mask ( 0 -> -1 (0xff), 1 -> 2)
      p.setSecond(preprocessMask(p.getSecond()));
      preprocessSearchKey(p);
    }
    this.fuzzyKeysData = fuzzyKeysData;
    this.tracker = new RowTracker();
  }

  private void preprocessSearchKey(Pair<byte[], byte[]> p) {
    if (!UNSAFE_UNALIGNED) {
      return;
    }
    byte[] key = p.getFirst();
    byte[] mask = p.getSecond();
    for (int i = 0; i < mask.length; i++) {
      // set non-fixed part of a search key to 0.
      if (mask[i] == 2) {
        key[i] = 0;
      }
    }
  }

  /**
   * We need to preprocess mask array, as since we treat 2's as unfixed positions and -1 (0xff) as
   * fixed positions
   * @param mask
   * @return mask array
   */
  private byte[] preprocessMask(byte[] mask) {
    if (!UNSAFE_UNALIGNED) {
      return mask;
    }
    if (isPreprocessedMask(mask)) return mask;
    for (int i = 0; i < mask.length; i++) {
      if (mask[i] == 0) {
        mask[i] = -1; // 0 -> -1
      } else if (mask[i] == 1) {
        mask[i] = 2;// 1 -> 2
      }
    }
    return mask;
  }

  private boolean isPreprocessedMask(byte[] mask) {
    for (int i = 0; i < mask.length; i++) {
      if (mask[i] != -1 && mask[i] != 2) {
        return false;
      }
    }
    return true;
  }

  @Override
  public ReturnCode filterKeyValue(Cell c) {
    final int startIndex = lastFoundIndex >= 0 ? lastFoundIndex : 0;
    final int size = fuzzyKeysData.size();
    for (int i = startIndex; i < size + startIndex; i++) {
      final int index = i % size;
      Pair<byte[], byte[]> fuzzyData = fuzzyKeysData.get(index);
      // This shift is idempotent - always end up with 0 and -1 as mask values.
      for (int j = 0; j < fuzzyData.getSecond().length; j++) {
        fuzzyData.getSecond()[j] >>= 2;
      }
      SatisfiesCode satisfiesCode =
          satisfies(isReversed(), c.getRowArray(), c.getRowOffset(), c.getRowLength(),
            fuzzyData.getFirst(), fuzzyData.getSecond());
      if (satisfiesCode == SatisfiesCode.YES) {
        lastFoundIndex = index;
        return ReturnCode.INCLUDE;
      }
    }
    // NOT FOUND -> seek next using hint
    lastFoundIndex = -1;

    return ReturnCode.SEEK_NEXT_USING_HINT;

  }

  @Override
  public Cell getNextCellHint(Cell currentCell) {
    boolean result = tracker.updateTracker(currentCell);
    if (result == false) {
      done = true;
      return null;
    }
    byte[] nextRowKey = tracker.nextRow();
    return KeyValueUtil.createFirstOnRow(nextRowKey);
  }

  /**
   * If we have multiple fuzzy keys, row tracker should improve overall performance. It calculates
   * all next rows (one per every fuzzy key) and put them (the fuzzy key is bundled) into a priority
   * queue so that the smallest row key always appears at queue head, which helps to decide the
   * "Next Cell Hint". As scanning going on, the number of candidate rows in the RowTracker will
   * remain the size of fuzzy keys until some of the fuzzy keys won't possibly have matches any
   * more.
   */
  private class RowTracker {
    private final PriorityQueue<Pair<byte[], Pair<byte[], byte[]>>> nextRows;
    private boolean initialized = false;

    RowTracker() {
      nextRows =
          new PriorityQueue<Pair<byte[], Pair<byte[], byte[]>>>(fuzzyKeysData.size(),
              new Comparator<Pair<byte[], Pair<byte[], byte[]>>>() {
                @Override
                public int compare(Pair<byte[], Pair<byte[], byte[]>> o1,
                    Pair<byte[], Pair<byte[], byte[]>> o2) {
                  int compare = Bytes.compareTo(o1.getFirst(), o2.getFirst());
                  if (!isReversed()) {
                    return compare;
                  } else {
                    return -compare;
                  }
                }
              });
    }

    byte[] nextRow() {
      if (nextRows.isEmpty()) {
        throw new IllegalStateException(
            "NextRows should not be empty, make sure to call nextRow() after updateTracker() return true");
      } else {
        return nextRows.peek().getFirst();
      }
    }

    boolean updateTracker(Cell currentCell) {
      if (!initialized) {
        for (Pair<byte[], byte[]> fuzzyData : fuzzyKeysData) {
          updateWith(currentCell, fuzzyData);
        }
        initialized = true;
      } else {
        while (!nextRows.isEmpty() && !lessThan(currentCell, nextRows.peek().getFirst())) {
          Pair<byte[], Pair<byte[], byte[]>> head = nextRows.poll();
          Pair<byte[], byte[]> fuzzyData = head.getSecond();
          updateWith(currentCell, fuzzyData);
        }
      }
      return !nextRows.isEmpty();
    }

    boolean lessThan(Cell currentCell, byte[] nextRowKey) {
      int compareResult =
          Bytes.compareTo(currentCell.getRowArray(), currentCell.getRowOffset(),
            currentCell.getRowLength(), nextRowKey, 0, nextRowKey.length);
      return (!isReversed() && compareResult < 0) || (isReversed() && compareResult > 0);
    }

    void updateWith(Cell currentCell, Pair<byte[], byte[]> fuzzyData) {
      byte[] nextRowKeyCandidate =
          getNextForFuzzyRule(isReversed(), currentCell.getRowArray(), currentCell.getRowOffset(),
            currentCell.getRowLength(), fuzzyData.getFirst(), fuzzyData.getSecond());
      if (nextRowKeyCandidate != null) {
        nextRows.add(new Pair<byte[], Pair<byte[], byte[]>>(nextRowKeyCandidate, fuzzyData));
      }
    }

  }

  @Override
  public boolean filterAllRemaining() {
    return done;
  }

  /**
   * @return The filter serialized using pb
   */
  public byte[] toByteArray() {
    FilterProtos.FuzzyRowFilter.Builder builder = FilterProtos.FuzzyRowFilter.newBuilder();
    for (Pair<byte[], byte[]> fuzzyData : fuzzyKeysData) {
      BytesBytesPair.Builder bbpBuilder = BytesBytesPair.newBuilder();
      bbpBuilder.setFirst(ByteStringer.wrap(fuzzyData.getFirst()));
      bbpBuilder.setSecond(ByteStringer.wrap(fuzzyData.getSecond()));
      builder.addFuzzyKeysData(bbpBuilder);
    }
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link FuzzyRowFilter} instance
   * @return An instance of {@link FuzzyRowFilter} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static FuzzyRowFilter parseFrom(final byte[] pbBytes) throws DeserializationException {
    FilterProtos.FuzzyRowFilter proto;
    try {
      proto = FilterProtos.FuzzyRowFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    int count = proto.getFuzzyKeysDataCount();
    ArrayList<Pair<byte[], byte[]>> fuzzyKeysData = new ArrayList<Pair<byte[], byte[]>>(count);
    for (int i = 0; i < count; ++i) {
      BytesBytesPair current = proto.getFuzzyKeysData(i);
      byte[] keyBytes = current.getFirst().toByteArray();
      byte[] keyMeta = current.getSecond().toByteArray();
      fuzzyKeysData.add(new Pair<byte[], byte[]>(keyBytes, keyMeta));
    }
    return new FuzzyRowFilter(fuzzyKeysData);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("FuzzyRowFilter");
    sb.append("{fuzzyKeysData=");
    for (Pair<byte[], byte[]> fuzzyData : fuzzyKeysData) {
      sb.append('{').append(Bytes.toStringBinary(fuzzyData.getFirst())).append(":");
      sb.append(Bytes.toStringBinary(fuzzyData.getSecond())).append('}');
    }
    sb.append("}, ");
    return sb.toString();
  }

  // Utility methods

  static enum SatisfiesCode {
    /** row satisfies fuzzy rule */
    YES,
    /** row doesn't satisfy fuzzy rule, but there's possible greater row that does */
    NEXT_EXISTS,
    /** row doesn't satisfy fuzzy rule and there's no greater row that does */
    NO_NEXT
  }

  @VisibleForTesting
  static SatisfiesCode satisfies(byte[] row, byte[] fuzzyKeyBytes, byte[] fuzzyKeyMeta) {
    return satisfies(false, row, 0, row.length, fuzzyKeyBytes, fuzzyKeyMeta);
  }

  @VisibleForTesting
  static SatisfiesCode satisfies(boolean reverse, byte[] row, byte[] fuzzyKeyBytes,
      byte[] fuzzyKeyMeta) {
    return satisfies(reverse, row, 0, row.length, fuzzyKeyBytes, fuzzyKeyMeta);
  }

  static SatisfiesCode satisfies(boolean reverse, byte[] row, int offset, int length,
      byte[] fuzzyKeyBytes, byte[] fuzzyKeyMeta) {

    if (!UNSAFE_UNALIGNED) {
      return satisfiesNoUnsafe(reverse, row, offset, length, fuzzyKeyBytes, fuzzyKeyMeta);
    }

    if (row == null) {
      // do nothing, let scan to proceed
      return SatisfiesCode.YES;
    }
    length = Math.min(length, fuzzyKeyBytes.length);
    int numWords = length / Bytes.SIZEOF_LONG;
    int offsetAdj = offset + UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;

    int j = numWords << 3; // numWords * SIZEOF_LONG;

    for (int i = 0; i < j; i += Bytes.SIZEOF_LONG) {

      long fuzzyBytes =
          UnsafeAccess.theUnsafe.getLong(fuzzyKeyBytes, UnsafeAccess.BYTE_ARRAY_BASE_OFFSET
              + (long) i);
      long fuzzyMeta =
          UnsafeAccess.theUnsafe.getLong(fuzzyKeyMeta, UnsafeAccess.BYTE_ARRAY_BASE_OFFSET
              + (long) i);
      long rowValue = UnsafeAccess.theUnsafe.getLong(row, offsetAdj + (long) i);
      if ((rowValue & fuzzyMeta) != (fuzzyBytes)) {
        // We always return NEXT_EXISTS
        return SatisfiesCode.NEXT_EXISTS;
      }
    }

    int off = j;

    if (length - off >= Bytes.SIZEOF_INT) {
      int fuzzyBytes =
          UnsafeAccess.theUnsafe.getInt(fuzzyKeyBytes, UnsafeAccess.BYTE_ARRAY_BASE_OFFSET
              + (long) off);
      int fuzzyMeta =
          UnsafeAccess.theUnsafe.getInt(fuzzyKeyMeta, UnsafeAccess.BYTE_ARRAY_BASE_OFFSET
              + (long) off);
      int rowValue = UnsafeAccess.theUnsafe.getInt(row, offsetAdj + (long) off);
      if ((rowValue & fuzzyMeta) != (fuzzyBytes)) {
        // We always return NEXT_EXISTS
        return SatisfiesCode.NEXT_EXISTS;
      }
      off += Bytes.SIZEOF_INT;
    }

    if (length - off >= Bytes.SIZEOF_SHORT) {
      short fuzzyBytes =
          UnsafeAccess.theUnsafe.getShort(fuzzyKeyBytes, UnsafeAccess.BYTE_ARRAY_BASE_OFFSET
              + (long) off);
      short fuzzyMeta =
          UnsafeAccess.theUnsafe.getShort(fuzzyKeyMeta, UnsafeAccess.BYTE_ARRAY_BASE_OFFSET
              + (long) off);
      short rowValue = UnsafeAccess.theUnsafe.getShort(row, offsetAdj + (long) off);
      if ((rowValue & fuzzyMeta) != (fuzzyBytes)) {
        // We always return NEXT_EXISTS
        // even if it does not (in this case getNextForFuzzyRule
        // will return null)
        return SatisfiesCode.NEXT_EXISTS;
      }
      off += Bytes.SIZEOF_SHORT;
    }

    if (length - off >= Bytes.SIZEOF_BYTE) {
      int fuzzyBytes = fuzzyKeyBytes[off] & 0xff;
      int fuzzyMeta = fuzzyKeyMeta[off] & 0xff;
      int rowValue = row[offset + off] & 0xff;
      if ((rowValue & fuzzyMeta) != (fuzzyBytes)) {
        // We always return NEXT_EXISTS
        return SatisfiesCode.NEXT_EXISTS;
      }
    }
    return SatisfiesCode.YES;
  }

  static SatisfiesCode satisfiesNoUnsafe(boolean reverse, byte[] row, int offset, int length,
      byte[] fuzzyKeyBytes, byte[] fuzzyKeyMeta) {
    if (row == null) {
      // do nothing, let scan to proceed
      return SatisfiesCode.YES;
    }

    Order order = Order.orderFor(reverse);
    boolean nextRowKeyCandidateExists = false;

    for (int i = 0; i < fuzzyKeyMeta.length && i < length; i++) {
      // First, checking if this position is fixed and not equals the given one
      boolean byteAtPositionFixed = fuzzyKeyMeta[i] == 0;
      boolean fixedByteIncorrect = byteAtPositionFixed && fuzzyKeyBytes[i] != row[i + offset];
      if (fixedByteIncorrect) {
        // in this case there's another row that satisfies fuzzy rule and bigger than this row
        if (nextRowKeyCandidateExists) {
          return SatisfiesCode.NEXT_EXISTS;
        }

        // If this row byte is less than fixed then there's a byte array bigger than
        // this row and which satisfies the fuzzy rule. Otherwise there's no such byte array:
        // this row is simply bigger than any byte array that satisfies the fuzzy rule
        boolean rowByteLessThanFixed = (row[i + offset] & 0xFF) < (fuzzyKeyBytes[i] & 0xFF);
        if (rowByteLessThanFixed && !reverse) {
          return SatisfiesCode.NEXT_EXISTS;
        } else if (!rowByteLessThanFixed && reverse) {
          return SatisfiesCode.NEXT_EXISTS;
        } else {
          return SatisfiesCode.NO_NEXT;
        }
      }

      // Second, checking if this position is not fixed and byte value is not the biggest. In this
      // case there's a byte array bigger than this row and which satisfies the fuzzy rule. To get
      // bigger byte array that satisfies the rule we need to just increase this byte
      // (see the code of getNextForFuzzyRule below) by one.
      // Note: if non-fixed byte is already at biggest value, this doesn't allow us to say there's
      // bigger one that satisfies the rule as it can't be increased.
      if (fuzzyKeyMeta[i] == 1 && !order.isMax(fuzzyKeyBytes[i])) {
        nextRowKeyCandidateExists = true;
      }
    }
    return SatisfiesCode.YES;
  }

  @VisibleForTesting
  static byte[] getNextForFuzzyRule(byte[] row, byte[] fuzzyKeyBytes, byte[] fuzzyKeyMeta) {
    return getNextForFuzzyRule(false, row, 0, row.length, fuzzyKeyBytes, fuzzyKeyMeta);
  }

  @VisibleForTesting
  static byte[] getNextForFuzzyRule(boolean reverse, byte[] row, byte[] fuzzyKeyBytes,
      byte[] fuzzyKeyMeta) {
    return getNextForFuzzyRule(reverse, row, 0, row.length, fuzzyKeyBytes, fuzzyKeyMeta);
  }

  /** Abstracts directional comparisons based on scan direction. */
  private enum Order {
    ASC {
      public boolean lt(int lhs, int rhs) {
        return lhs < rhs;
      }

      public boolean gt(int lhs, int rhs) {
        return lhs > rhs;
      }

      public byte inc(byte val) {
        // TODO: what about over/underflow?
        return (byte) (val + 1);
      }

      public boolean isMax(byte val) {
        return val == (byte) 0xff;
      }

      public byte min() {
        return 0;
      }
    },
    DESC {
      public boolean lt(int lhs, int rhs) {
        return lhs > rhs;
      }

      public boolean gt(int lhs, int rhs) {
        return lhs < rhs;
      }

      public byte inc(byte val) {
        // TODO: what about over/underflow?
        return (byte) (val - 1);
      }

      public boolean isMax(byte val) {
        return val == 0;
      }

      public byte min() {
        return (byte) 0xFF;
      }
    };

    public static Order orderFor(boolean reverse) {
      return reverse ? DESC : ASC;
    }

    /** Returns true when {@code lhs < rhs}. */
    public abstract boolean lt(int lhs, int rhs);

    /** Returns true when {@code lhs > rhs}. */
    public abstract boolean gt(int lhs, int rhs);

    /** Returns {@code val} incremented by 1. */
    public abstract byte inc(byte val);

    /** Return true when {@code val} is the maximum value */
    public abstract boolean isMax(byte val);

    /** Return the minimum value according to this ordering scheme. */
    public abstract byte min();
  }

  /**
   * @return greater byte array than given (row) which satisfies the fuzzy rule if it exists, null
   *         otherwise
   */
  @VisibleForTesting
  static byte[] getNextForFuzzyRule(boolean reverse, byte[] row, int offset, int length,
      byte[] fuzzyKeyBytes, byte[] fuzzyKeyMeta) {
    // To find out the next "smallest" byte array that satisfies fuzzy rule and "greater" than
    // the given one we do the following:
    // 1. setting values on all "fixed" positions to the values from fuzzyKeyBytes
    // 2. if during the first step given row did not increase, then we increase the value at
    // the first "non-fixed" position (where it is not maximum already)

    // It is easier to perform this by using fuzzyKeyBytes copy and setting "non-fixed" position
    // values than otherwise.
    byte[] result =
        Arrays.copyOf(fuzzyKeyBytes, length > fuzzyKeyBytes.length ? length : fuzzyKeyBytes.length);
    if (reverse && length > fuzzyKeyBytes.length) {
      // we need trailing 0xff's instead of trailing 0x00's
      for (int i = fuzzyKeyBytes.length; i < result.length; i++) {
        result[i] = (byte) 0xFF;
      }
    }
    int toInc = -1;
    final Order order = Order.orderFor(reverse);

    boolean increased = false;
    for (int i = 0; i < result.length; i++) {
      if (i >= fuzzyKeyMeta.length || fuzzyKeyMeta[i] == 0 /* non-fixed */) {
        result[i] = row[offset + i];
        if (!order.isMax(row[offset + i])) {
          // this is "non-fixed" position and is not at max value, hence we can increase it
          toInc = i;
        }
      } else if (i < fuzzyKeyMeta.length && fuzzyKeyMeta[i] == -1 /* fixed */) {
        if (order.lt((row[i + offset] & 0xFF), (fuzzyKeyBytes[i] & 0xFF))) {
          // if setting value for any fixed position increased the original array,
          // we are OK
          increased = true;
          break;
        }

        if (order.gt((row[i + offset] & 0xFF), (fuzzyKeyBytes[i] & 0xFF))) {
          // if setting value for any fixed position makes array "smaller", then just stop:
          // in case we found some non-fixed position to increase we will do it, otherwise
          // there's no "next" row key that satisfies fuzzy rule and "greater" than given row
          break;
        }
      }
    }

    if (!increased) {
      if (toInc < 0) {
        return null;
      }
      result[toInc] = order.inc(result[toInc]);

      // Setting all "non-fixed" positions to zeroes to the right of the one we increased so
      // that found "next" row key is the smallest possible
      for (int i = toInc + 1; i < result.length; i++) {
        if (i >= fuzzyKeyMeta.length || fuzzyKeyMeta[i] == 0 /* non-fixed */) {
          result[i] = order.min();
        }
      }
    }

    return reverse? result: trimTrailingZeroes(result, fuzzyKeyMeta, toInc);
  }

  /**
   * For forward scanner, next cell hint should  not contain any trailing zeroes
   * unless they are part of fuzzyKeyMeta
   * hint = '\x01\x01\x01\x00\x00'
   * will skip valid row '\x01\x01\x01'
   * 
   * @param result
   * @param fuzzyKeyMeta
   * @param toInc - position of incremented byte
   * @return trimmed version of result
   */
  
  private static byte[] trimTrailingZeroes(byte[] result, byte[] fuzzyKeyMeta, int toInc) {
    int off = fuzzyKeyMeta.length >= result.length? result.length -1:
           fuzzyKeyMeta.length -1;  
    for( ; off >= 0; off--){
      if(fuzzyKeyMeta[off] != 0) break;
    }
    if (off < toInc)  off = toInc;
    byte[] retValue = new byte[off+1];
    System.arraycopy(result, 0, retValue, 0, retValue.length);
    return retValue;
  }

  /**
   * @return true if and only if the fields of the filter that are serialized are equal to the
   *         corresponding fields in other. Used for testing.
   */
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof FuzzyRowFilter)) return false;

    FuzzyRowFilter other = (FuzzyRowFilter) o;
    if (this.fuzzyKeysData.size() != other.fuzzyKeysData.size()) return false;
    for (int i = 0; i < fuzzyKeysData.size(); ++i) {
      Pair<byte[], byte[]> thisData = this.fuzzyKeysData.get(i);
      Pair<byte[], byte[]> otherData = other.fuzzyKeysData.get(i);
      if (!(Bytes.equals(thisData.getFirst(), otherData.getFirst()) && Bytes.equals(
        thisData.getSecond(), otherData.getSecond()))) {
        return false;
      }
    }
    return true;
  }
}
