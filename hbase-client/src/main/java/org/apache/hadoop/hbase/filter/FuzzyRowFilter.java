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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.util.ByteStringer;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.BytesBytesPair;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Filters data based on fuzzy row key. Performs fast-forwards during scanning.
 * It takes pairs (row key, fuzzy info) to match row keys. Where fuzzy info is
 * a byte array with 0 or 1 as its values:
 * <ul>
 *   <li>
 *     0 - means that this byte in provided row key is fixed, i.e. row key's byte at same position
 *         must match
 *   </li>
 *   <li>
 *     1 - means that this byte in provided row key is NOT fixed, i.e. row key's byte at this
 *         position can be different from the one in provided row key
 *   </li>
 * </ul>
 *
 *
 * Example:
 * Let's assume row key format is userId_actionId_year_month. Length of userId is fixed
 * and is 4, length of actionId is 2 and year and month are 4 and 2 bytes long respectively.
 *
 * Let's assume that we need to fetch all users that performed certain action (encoded as "99")
 * in Jan of any year. Then the pair (row key, fuzzy info) would be the following:
 * row key = "????_99_????_01" (one can use any value instead of "?")
 * fuzzy info = "\x01\x01\x01\x01\x00\x00\x00\x00\x01\x01\x01\x01\x00\x00\x00"
 *
 * I.e. fuzzy info tells the matching mask is "????_99_????_01", where at ? can be any value.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class FuzzyRowFilter extends FilterBase {
  private List<Pair<byte[], byte[]>> fuzzyKeysData;
  private boolean done = false;

  public FuzzyRowFilter(List<Pair<byte[], byte[]>> fuzzyKeysData) {
    Pair<byte[], byte[]> p;
    for (int i = 0; i < fuzzyKeysData.size(); i++) {
      p = fuzzyKeysData.get(i);
      if (p.getFirst().length != p.getSecond().length) {
        Pair<String, String> readable = new Pair<String, String>(
          Bytes.toStringBinary(p.getFirst()),
          Bytes.toStringBinary(p.getSecond()));
        throw new IllegalArgumentException("Fuzzy pair lengths do not match: " + readable);
      }
    }
    this.fuzzyKeysData = fuzzyKeysData;
  }

  // TODO: possible improvement: save which fuzzy row key to use when providing a hint
  @Override
  public ReturnCode filterKeyValue(Cell kv) {
    // TODO add getRow() equivalent to Cell or change satisfies to take b[],o,l style args.
    KeyValue v = KeyValueUtil.ensureKeyValue(kv);

    byte[] rowKey = v.getRow();
    // assigning "worst" result first and looking for better options
    SatisfiesCode bestOption = SatisfiesCode.NO_NEXT;
    for (Pair<byte[], byte[]> fuzzyData : fuzzyKeysData) {
      SatisfiesCode satisfiesCode =
              satisfies(isReversed(), rowKey, fuzzyData.getFirst(), fuzzyData.getSecond());
      if (satisfiesCode == SatisfiesCode.YES) {
        return ReturnCode.INCLUDE;
      }

      if (satisfiesCode == SatisfiesCode.NEXT_EXISTS) {
        bestOption = SatisfiesCode.NEXT_EXISTS;
      }
    }

    if (bestOption == SatisfiesCode.NEXT_EXISTS) {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }

    // the only unhandled SatisfiesCode is NO_NEXT, i.e. we are done
    done = true;
    return ReturnCode.NEXT_ROW;
  }

  @Override
  public Cell getNextCellHint(Cell currentKV) {
    // TODO make matching Column a cell method or CellUtil method.
    KeyValue v = KeyValueUtil.ensureKeyValue(currentKV);

    byte[] rowKey = v.getRow();
    byte[] nextRowKey = null;
    // Searching for the "smallest" row key that satisfies at least one fuzzy row key
    for (Pair<byte[], byte[]> fuzzyData : fuzzyKeysData) {
      byte[] nextRowKeyCandidate = getNextForFuzzyRule(isReversed(), rowKey,
              fuzzyData.getFirst(), fuzzyData.getSecond());
      if (nextRowKeyCandidate == null) {
        continue;
      }
      if (nextRowKey == null ||
        (reversed && Bytes.compareTo(nextRowKeyCandidate, nextRowKey) > 0) ||
        (!reversed && Bytes.compareTo(nextRowKeyCandidate, nextRowKey) < 0)) {
        nextRowKey = nextRowKeyCandidate;
      }
    }

    if (!reversed && nextRowKey == null) {
      // Should never happen for forward scanners; logic in filterKeyValue should return NO_NEXT.
      // Can happen in reversed scanner when currentKV is just before the next possible match; in
      // this case, fall back on scanner simply calling KeyValueHeap.next()
      // TODO: is there a better way than throw exception? (stop the scanner?)
      throw new IllegalStateException("No next row key that satisfies fuzzy exists when" +
                                         " getNextKeyHint() is invoked." +
                                         " Filter: " + this.toString() +
                                         " currentKV: " + currentKV.toString());
    }

    return nextRowKey == null ? null : KeyValue.createFirstOnRow(nextRowKey);
  }

  @Override
  public boolean filterAllRemaining() {
    return done;
  }

  /**
   * @return The filter serialized using pb
   */
  public byte [] toByteArray() {
    FilterProtos.FuzzyRowFilter.Builder builder =
      FilterProtos.FuzzyRowFilter.newBuilder();
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
  public static FuzzyRowFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    FilterProtos.FuzzyRowFilter proto;
    try {
      proto = FilterProtos.FuzzyRowFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    int count = proto.getFuzzyKeysDataCount();
    ArrayList<Pair<byte[], byte[]>> fuzzyKeysData= new ArrayList<Pair<byte[], byte[]>>(count);
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

  private static SatisfiesCode satisfies(boolean reverse, byte[] row, int offset, int length,
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
      //       bigger one that satisfies the rule as it can't be increased.
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
   * @return greater byte array than given (row) which satisfies the fuzzy rule if it exists,
   *         null otherwise
   */
  private static byte[] getNextForFuzzyRule(boolean reverse, byte[] row, int offset, int length,
                                            byte[] fuzzyKeyBytes, byte[] fuzzyKeyMeta) {
    // To find out the next "smallest" byte array that satisfies fuzzy rule and "greater" than
    // the given one we do the following:
    // 1. setting values on all "fixed" positions to the values from fuzzyKeyBytes
    // 2. if during the first step given row did not increase, then we increase the value at
    //    the first "non-fixed" position (where it is not maximum already)

    // It is easier to perform this by using fuzzyKeyBytes copy and setting "non-fixed" position
    // values than otherwise.
    byte[] result = Arrays.copyOf(fuzzyKeyBytes,
                                  length > fuzzyKeyBytes.length ? length : fuzzyKeyBytes.length);
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
      if (i >= fuzzyKeyMeta.length || fuzzyKeyMeta[i] == 1) {
        result[i] = row[offset + i];
        if (!order.isMax(row[i])) {
          // this is "non-fixed" position and is not at max value, hence we can increase it
          toInc = i;
        }
      } else if (i < fuzzyKeyMeta.length && fuzzyKeyMeta[i] == 0) {
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
        if (i >= fuzzyKeyMeta.length || fuzzyKeyMeta[i] == 1) {
          result[i] = order.min();
        }
      }
    }

    return result;
  }

  /**
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof FuzzyRowFilter)) return false;

    FuzzyRowFilter other = (FuzzyRowFilter)o;
    if (this.fuzzyKeysData.size() != other.fuzzyKeysData.size()) return false;
    for (int i = 0; i < fuzzyKeysData.size(); ++i) {
      Pair<byte[], byte[]> thisData = this.fuzzyKeysData.get(i);
      Pair<byte[], byte[]> otherData = other.fuzzyKeysData.get(i);
      if (!(Bytes.equals(thisData.getFirst(), otherData.getFirst())
        && Bytes.equals(thisData.getSecond(), otherData.getSecond()))) {
        return false;
      }
    }
    return true;
  }
}
