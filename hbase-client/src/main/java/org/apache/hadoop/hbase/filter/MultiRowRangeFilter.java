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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.ClientUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;

/**
 * Filter to support scan multiple row key ranges. It can construct the row key ranges from the
 * passed list which can be accessed by each region server.
 *
 * HBase is quite efficient when scanning only one small row key range. If user needs to specify
 * multiple row key ranges in one scan, the typical solutions are: 1. through FilterList which is a
 * list of row key Filters, 2. using the SQL layer over HBase to join with two table, such as hive,
 * phoenix etc. However, both solutions are inefficient. Both of them can't utilize the range info
 * to perform fast forwarding during scan which is quite time consuming. If the number of ranges
 * are quite big (e.g. millions), join is a proper solution though it is slow. However, there are
 * cases that user wants to specify a small number of ranges to scan (e.g. &lt;1000 ranges). Both
 * solutions can't provide satisfactory performance in such case. MultiRowRangeFilter is to support
 * such usec ase (scan multiple row key ranges), which can construct the row key ranges from user
 * specified list and perform fast-forwarding during scan. Thus, the scan will be quite efficient.
 */
@InterfaceAudience.Public
public class MultiRowRangeFilter extends FilterBase {

  private static final int ROW_BEFORE_FIRST_RANGE = -1;

  private final List<RowRange> rangeList;
  private final RangeIteration ranges;

  private boolean done = false;
  private int index;
  private BasicRowRange range;
  private ReturnCode currentReturnCode;

  /**
   * @param list A list of <code>RowRange</code>
   */
  public MultiRowRangeFilter(List<RowRange> list) {
    // We don't use rangeList anywhere else, but keeping it lets us pay a little
    // memory to avoid touching the serialization logic.
    this.rangeList = Collections.unmodifiableList(sortAndMerge(list));
    this.ranges = new RangeIteration(rangeList);
  }

  /**
   * Constructor for creating a <code>MultiRowRangeFilter</code> from multiple rowkey prefixes.
   *
   * As <code>MultiRowRangeFilter</code> javadoc says (See the solution 1 of the first statement),
   * if you try to create a filter list that scans row keys corresponding to given prefixes (e.g.,
   * <code>FilterList</code> composed of multiple <code>PrefixFilter</code>s), this constructor
   * provides a way to avoid creating an inefficient one.
   *
   * @param rowKeyPrefixes the array of byte array
   */
  public MultiRowRangeFilter(byte[][] rowKeyPrefixes) {
    this(createRangeListFromRowKeyPrefixes(rowKeyPrefixes));
  }

  private static List<RowRange> createRangeListFromRowKeyPrefixes(byte[][] rowKeyPrefixes) {
    if (rowKeyPrefixes == null) {
      throw new IllegalArgumentException("Invalid rowkey prefixes");
    }

    List<RowRange> list = new ArrayList<>();
    for (byte[] rowKeyPrefix: rowKeyPrefixes) {
      byte[] stopRow = ClientUtil.calculateTheClosestNextRowKeyForPrefix(rowKeyPrefix);
      list.add(new RowRange(rowKeyPrefix, true, stopRow, false));
    }
    return list;
  }

  public List<RowRange> getRowRanges() {
    // Used by hbase-rest
    return this.rangeList;
  }

  @Override
  public boolean filterAllRemaining() {
    return done;
  }

  @Override
  public boolean filterRowKey(Cell firstRowCell) {
    if (filterAllRemaining()) return true;

    // N.b. We can only do this after we're iterating over records. If we try to do
    // it before, the Scan (and this filter) may not yet be fully initialized. This is a
    // wart on Filter and something that'd be nice to clean up (like CP's in HBase2.0)
    if (!ranges.isInitialized()) {
      ranges.initialize(isReversed());
    }

    // If it is the first time of running, calculate the current range index for
    // the row key. If index is out of bound which happens when the start row
    // user sets is after the largest stop row of the ranges, stop the scan.
    // If row key is after the current range, find the next range and update index.
    byte[] rowArr = firstRowCell.getRowArray();
    int length = firstRowCell.getRowLength();
    int offset = firstRowCell.getRowOffset();
    if (!ranges.hasFoundFirstRange() || !range.contains(rowArr, offset, length)) {
      byte[] rowkey = CellUtil.cloneRow(firstRowCell);
      index = ranges.getNextRangeIndex(rowkey);
      if (ranges.isIterationComplete(index)) {
        done = true;
        currentReturnCode = ReturnCode.NEXT_ROW;
        return false;
      }
      if(index != ROW_BEFORE_FIRST_RANGE) {
        range = ranges.get(index);
      } else {
        range = ranges.get(0);
      }
      if (ranges.isExclusive()) {
        ranges.resetExclusive();
        currentReturnCode = ReturnCode.NEXT_ROW;
        return false;
      }
      if (!ranges.hasFoundFirstRange()) {
        if(index != ROW_BEFORE_FIRST_RANGE) {
          currentReturnCode = ReturnCode.INCLUDE;
        } else {
          currentReturnCode = ReturnCode.SEEK_NEXT_USING_HINT;
        }
        ranges.setFoundFirstRange();
      } else {
        if (range.contains(rowArr, offset, length)) {
          currentReturnCode = ReturnCode.INCLUDE;
        } else {
          currentReturnCode = ReturnCode.SEEK_NEXT_USING_HINT;
        }
      }
    } else {
      currentReturnCode = ReturnCode.INCLUDE;
    }
    return false;
  }

  @Deprecated
  @Override
  public ReturnCode filterKeyValue(final Cell ignored) {
    return filterCell(ignored);
  }

  @Override
  public ReturnCode filterCell(final Cell ignored) {
    return currentReturnCode;
  }

  @Override
  public Cell getNextCellHint(Cell currentKV) {
    // skip to the next range's start row
    // #getComparisonData lets us avoid the `if (reversed)` branch
    byte[] comparisonData = range.getComparisonData();
    return PrivateCellUtil.createFirstOnRow(comparisonData, 0, (short) comparisonData.length);
  }

  /**
   * @return The filter serialized using pb
   */
  @Override
  public byte[] toByteArray() {
    FilterProtos.MultiRowRangeFilter.Builder builder = FilterProtos.MultiRowRangeFilter
        .newBuilder();
    for (RowRange range : rangeList) {
      if (range != null) {
        FilterProtos.RowRange.Builder rangebuilder = FilterProtos.RowRange.newBuilder();
        if (range.startRow != null)
          rangebuilder.setStartRow(UnsafeByteOperations.unsafeWrap(range.startRow));
        rangebuilder.setStartRowInclusive(range.startRowInclusive);
        if (range.stopRow != null)
          rangebuilder.setStopRow(UnsafeByteOperations.unsafeWrap(range.stopRow));
        rangebuilder.setStopRowInclusive(range.stopRowInclusive);
        builder.addRowRangeList(rangebuilder.build());
      }
    }
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized instance
   * @return An instance of MultiRowRangeFilter
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   */
  public static MultiRowRangeFilter parseFrom(final byte[] pbBytes)
      throws DeserializationException {
    FilterProtos.MultiRowRangeFilter proto;
    try {
      proto = FilterProtos.MultiRowRangeFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    int length = proto.getRowRangeListCount();
    List<FilterProtos.RowRange> rangeProtos = proto.getRowRangeListList();
    List<RowRange> rangeList = new ArrayList<>(length);
    for (FilterProtos.RowRange rangeProto : rangeProtos) {
      RowRange range = new RowRange(rangeProto.hasStartRow() ? rangeProto.getStartRow()
          .toByteArray() : null, rangeProto.getStartRowInclusive(), rangeProto.hasStopRow() ?
              rangeProto.getStopRow().toByteArray() : null, rangeProto.getStopRowInclusive());
      rangeList.add(range);
    }
    return new MultiRowRangeFilter(rangeList);
  }

  /**
   * @param o the filter to compare
   * @return true if and only if the fields of the filter that are serialized are equal to the
   *         corresponding fields in other. Used for testing.
   */
  @Override
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this)
      return true;
    if (!(o instanceof MultiRowRangeFilter))
      return false;

    MultiRowRangeFilter other = (MultiRowRangeFilter) o;
    if (this.rangeList.size() != other.rangeList.size())
      return false;
    for (int i = 0; i < rangeList.size(); ++i) {
      RowRange thisRange = this.rangeList.get(i);
      RowRange otherRange = other.rangeList.get(i);
      if (!(Bytes.equals(thisRange.startRow, otherRange.startRow) && Bytes.equals(
          thisRange.stopRow, otherRange.stopRow) && (thisRange.startRowInclusive ==
          otherRange.startRowInclusive) && (thisRange.stopRowInclusive ==
          otherRange.stopRowInclusive))) {
        return false;
      }
    }
    return true;
  }

  /**
   * sort the ranges and if the ranges with overlap, then merge them.
   *
   * @param ranges the list of ranges to sort and merge.
   * @return the ranges after sort and merge.
   */
  public static List<RowRange> sortAndMerge(List<RowRange> ranges) {
    if (ranges.isEmpty()) {
      throw new IllegalArgumentException("No ranges found.");
    }
    List<RowRange> invalidRanges = new ArrayList<>();
    List<RowRange> newRanges = new ArrayList<>(ranges.size());
    Collections.sort(ranges);
    if(ranges.get(0).isValid()) {
      if (ranges.size() == 1) {
        newRanges.add(ranges.get(0));
      }
    } else {
      invalidRanges.add(ranges.get(0));
    }

    byte[] lastStartRow = ranges.get(0).startRow;
    boolean lastStartRowInclusive = ranges.get(0).startRowInclusive;
    byte[] lastStopRow = ranges.get(0).stopRow;
    boolean lastStopRowInclusive = ranges.get(0).stopRowInclusive;
    int i = 1;
    for (; i < ranges.size(); i++) {
      RowRange range = ranges.get(i);
      if (!range.isValid()) {
        invalidRanges.add(range);
      }
      if(Bytes.equals(lastStopRow, HConstants.EMPTY_BYTE_ARRAY)) {
        newRanges.add(new RowRange(lastStartRow, lastStartRowInclusive, lastStopRow,
            lastStopRowInclusive));
        break;
      }
      // with overlap in the ranges
      if ((Bytes.compareTo(lastStopRow, range.startRow) > 0) ||
          (Bytes.compareTo(lastStopRow, range.startRow) == 0 && !(lastStopRowInclusive == false &&
          range.isStartRowInclusive() == false))) {
        if(Bytes.equals(range.stopRow, HConstants.EMPTY_BYTE_ARRAY)) {
          newRanges.add(new RowRange(lastStartRow, lastStartRowInclusive, range.stopRow,
              range.stopRowInclusive));
          break;
        }
        // if first range contains second range, ignore the second range
        if (Bytes.compareTo(lastStopRow, range.stopRow) >= 0) {
          if((Bytes.compareTo(lastStopRow, range.stopRow) == 0)) {
            if(lastStopRowInclusive == true || range.stopRowInclusive == true) {
              lastStopRowInclusive = true;
            }
          }
          if ((i + 1) == ranges.size()) {
            newRanges.add(new RowRange(lastStartRow, lastStartRowInclusive, lastStopRow,
                lastStopRowInclusive));
          }
        } else {
          lastStopRow = range.stopRow;
          lastStopRowInclusive = range.stopRowInclusive;
          if ((i + 1) < ranges.size()) {
            i++;
            range = ranges.get(i);
            if (!range.isValid()) {
              invalidRanges.add(range);
            }
          } else {
            newRanges.add(new RowRange(lastStartRow, lastStartRowInclusive, lastStopRow,
                lastStopRowInclusive));
            break;
          }
          while ((Bytes.compareTo(lastStopRow, range.startRow) > 0) ||
              (Bytes.compareTo(lastStopRow, range.startRow) == 0 &&
              (lastStopRowInclusive == true || range.startRowInclusive==true))) {
            if(Bytes.equals(range.stopRow, HConstants.EMPTY_BYTE_ARRAY)) {
              break;
            }
            // if this first range contain second range, ignore the second range
            if (Bytes.compareTo(lastStopRow, range.stopRow) >= 0) {
              if(lastStopRowInclusive == true || range.stopRowInclusive == true) {
                lastStopRowInclusive = true;
              }
              i++;
              if (i < ranges.size()) {
                range = ranges.get(i);
                if (!range.isValid()) {
                  invalidRanges.add(range);
                }
              } else {
                break;
              }
            } else {
              lastStopRow = range.stopRow;
              lastStopRowInclusive = range.stopRowInclusive;
              i++;
              if (i < ranges.size()) {
                range = ranges.get(i);
                if (!range.isValid()) {
                  invalidRanges.add(range);
                }
              } else {
                break;
              }
            }
          }
          if(Bytes.equals(range.stopRow, HConstants.EMPTY_BYTE_ARRAY)) {
            if((Bytes.compareTo(lastStopRow, range.startRow) < 0) ||
                (Bytes.compareTo(lastStopRow, range.startRow) == 0 &&
                lastStopRowInclusive == false && range.startRowInclusive == false)) {
              newRanges.add(new RowRange(lastStartRow, lastStartRowInclusive, lastStopRow,
                  lastStopRowInclusive));
              newRanges.add(range);
            } else {
              newRanges.add(new RowRange(lastStartRow, lastStartRowInclusive, range.stopRow,
                  range.stopRowInclusive));
              break;
            }
          }
          newRanges.add(new RowRange(lastStartRow, lastStartRowInclusive, lastStopRow,
              lastStopRowInclusive));
          if ((i + 1) == ranges.size()) {
            newRanges.add(range);
          }
          lastStartRow = range.startRow;
          lastStartRowInclusive = range.startRowInclusive;
          lastStopRow = range.stopRow;
          lastStopRowInclusive = range.stopRowInclusive;
        }
      } else {
        newRanges.add(new RowRange(lastStartRow, lastStartRowInclusive, lastStopRow,
            lastStopRowInclusive));
        if ((i + 1) == ranges.size()) {
          newRanges.add(range);
        }
        lastStartRow = range.startRow;
        lastStartRowInclusive = range.startRowInclusive;
        lastStopRow = range.stopRow;
        lastStopRowInclusive = range.stopRowInclusive;
      }
    }
    // check the remaining ranges
    for(int j=i; j < ranges.size(); j++) {
      if(!ranges.get(j).isValid()) {
        invalidRanges.add(ranges.get(j));
      }
    }
    // if invalid range exists, throw the exception
    if (invalidRanges.size() != 0) {
      throwExceptionForInvalidRanges(invalidRanges, true);
    }
    // If no valid ranges found, throw the exception
    if(newRanges.isEmpty()) {
      throw new IllegalArgumentException("No valid ranges found.");
    }
    return newRanges;
  }

  private static void throwExceptionForInvalidRanges(List<RowRange> invalidRanges,
      boolean details) {
    StringBuilder sb = new StringBuilder();
    sb.append(invalidRanges.size()).append(" invaild ranges.\n");
    if (details) {
      for (RowRange range : invalidRanges) {
        sb.append(
            "Invalid range: start row => " + Bytes.toString(range.startRow) + ", stop row => "
                + Bytes.toString(range.stopRow)).append('\n');
      }
    }
    throw new IllegalArgumentException(sb.toString());
  }

  private static abstract class BasicRowRange implements Comparable<BasicRowRange> {
    protected byte[] startRow;
    protected boolean startRowInclusive = true;
    protected byte[] stopRow;
    protected boolean stopRowInclusive = false;

    public BasicRowRange() {
    }
    /**
     * If the startRow is empty or null, set it to HConstants.EMPTY_BYTE_ARRAY, means begin at the
     * start row of the table. If the stopRow is empty or null, set it to
     * HConstants.EMPTY_BYTE_ARRAY, means end of the last row of table.
     */
    public BasicRowRange(String startRow, boolean startRowInclusive, String stopRow,
        boolean stopRowInclusive) {
      this((startRow == null || startRow.isEmpty()) ? HConstants.EMPTY_BYTE_ARRAY :
        Bytes.toBytes(startRow), startRowInclusive,
        (stopRow == null || stopRow.isEmpty()) ? HConstants.EMPTY_BYTE_ARRAY :
        Bytes.toBytes(stopRow), stopRowInclusive);
    }

    public BasicRowRange(byte[] startRow,  boolean startRowInclusive, byte[] stopRow,
        boolean stopRowInclusive) {
      this.startRow = (startRow == null) ? HConstants.EMPTY_BYTE_ARRAY : startRow;
      this.startRowInclusive = startRowInclusive;
      this.stopRow = (stopRow == null) ? HConstants.EMPTY_BYTE_ARRAY :stopRow;
      this.stopRowInclusive = stopRowInclusive;
    }

    public byte[] getStartRow() {
      return startRow;
    }

    public byte[] getStopRow() {
      return stopRow;
    }

    /**
     * @return if start row is inclusive.
     */
    public boolean isStartRowInclusive() {
      return startRowInclusive;
    }

    /**
     * @return if stop row is inclusive.
     */
    public boolean isStopRowInclusive() {
      return stopRowInclusive;
    }

    public boolean contains(byte[] row) {
      return contains(row, 0, row.length);
    }

    public boolean contains(byte[] buffer, int offset, int length) {
      if(startRowInclusive) {
        if(stopRowInclusive) {
          return Bytes.compareTo(buffer, offset, length, startRow, 0, startRow.length) >= 0
              && (Bytes.equals(stopRow, HConstants.EMPTY_BYTE_ARRAY) ||
                  Bytes.compareTo(buffer, offset, length, stopRow, 0, stopRow.length) <= 0);
        } else {
          return Bytes.compareTo(buffer, offset, length, startRow, 0, startRow.length) >= 0
              && (Bytes.equals(stopRow, HConstants.EMPTY_BYTE_ARRAY) ||
                  Bytes.compareTo(buffer, offset, length, stopRow, 0, stopRow.length) < 0);
        }
      } else {
        if(stopRowInclusive) {
          return Bytes.compareTo(buffer, offset, length, startRow, 0, startRow.length) > 0
              && (Bytes.equals(stopRow, HConstants.EMPTY_BYTE_ARRAY) ||
                  Bytes.compareTo(buffer, offset, length, stopRow, 0, stopRow.length) <= 0);
        } else {
          return Bytes.compareTo(buffer, offset, length, startRow, 0, startRow.length) > 0
              && (Bytes.equals(stopRow, HConstants.EMPTY_BYTE_ARRAY) ||
                  Bytes.compareTo(buffer, offset, length, stopRow, 0, stopRow.length) < 0);
        }
      }
    }

    public boolean isValid() {
      return Bytes.equals(startRow, HConstants.EMPTY_BYTE_ARRAY)
          || Bytes.equals(stopRow, HConstants.EMPTY_BYTE_ARRAY)
          || Bytes.compareTo(startRow, stopRow) < 0
          || (Bytes.compareTo(startRow, stopRow) == 0 && stopRowInclusive == true);
    }

    @Override
    public boolean equals(Object obj){
      if (!(obj instanceof BasicRowRange)) {
        return false;
      }
      if (this == obj) {
        return true;
      }
      BasicRowRange rr = (BasicRowRange) obj;
      return Bytes.equals(this.stopRow, rr.getStopRow()) &&
          Bytes.equals(this.startRow, this.getStartRow()) &&
          this.startRowInclusive == rr.isStartRowInclusive() &&
          this.stopRowInclusive == rr.isStopRowInclusive();
    }

    @Override
    public int hashCode() {
      return Objects.hash(Bytes.hashCode(this.stopRow),
          Bytes.hashCode(this.startRow),
          this.startRowInclusive,
          this.stopRowInclusive);
    }

    /**
     * Returns the data to be used to compare {@code this} to another object.
     */
    public abstract byte[] getComparisonData();

    /**
     * Returns whether the bounding row used for binary-search is inclusive or not.
     *
     * For forward scans, we would check the starRow, but we would check the stopRow for
     * the reverse scan case.
     */
    public abstract boolean isSearchRowInclusive();

    @Override
    public int compareTo(BasicRowRange other) {
      byte[] left;
      byte[] right;
      if (isAscendingOrder()) {
        left = this.getComparisonData();
        right = other.getComparisonData();
      } else {
        left = other.getComparisonData();
        right = this.getComparisonData();
      }
      return Bytes.compareTo(left, right);
    }

    public abstract boolean isAscendingOrder();
  }

  /**
   * Internal RowRange that reverses the sort-order to handle reverse scans.
   */
  @InterfaceAudience.Private
  private static class ReversedRowRange extends BasicRowRange {
    public ReversedRowRange(byte[] startRow,  boolean startRowInclusive, byte[] stopRow,
        boolean stopRowInclusive) {
      super(startRow, startRowInclusive, stopRow, stopRowInclusive);
    }

    @Override
    public byte[] getComparisonData() {
      return this.stopRow;
    }

    @Override
    public boolean isSearchRowInclusive() {
      return this.stopRowInclusive;
    }

    @Override
    public boolean isAscendingOrder() {
      return false;
    }
  }

  @InterfaceAudience.Public
  public static class RowRange extends BasicRowRange {
    public RowRange() {
    }
    /**
     * If the startRow is empty or null, set it to HConstants.EMPTY_BYTE_ARRAY, means begin at the
     * start row of the table. If the stopRow is empty or null, set it to
     * HConstants.EMPTY_BYTE_ARRAY, means end of the last row of table.
     */
    public RowRange(String startRow, boolean startRowInclusive, String stopRow,
        boolean stopRowInclusive) {
      super(startRow, startRowInclusive, stopRow, stopRowInclusive);
    }

    public RowRange(byte[] startRow,  boolean startRowInclusive, byte[] stopRow,
        boolean stopRowInclusive) {
      super(startRow, startRowInclusive, stopRow, stopRowInclusive);
    }

    @Override
    public byte[] getComparisonData() {
      return startRow;
    }

    @Override
    public boolean isSearchRowInclusive() {
      return startRowInclusive;
    }

    @Override
    public boolean isAscendingOrder() {
      return true;
    }
  }

  /**
   * Abstraction over the ranges of rows to return from this filter, regardless of forward or
   * reverse scans being used. This Filter can use this class, agnostic of iteration direction,
   * as the same algorithm can be applied in both cases.
   */
  @InterfaceAudience.Private
  private static class RangeIteration {
    private boolean exclusive = false;
    private boolean initialized = false;
    private boolean foundFirstRange = false;
    private boolean reversed = false;
    private final List<RowRange> sortedAndMergedRanges;
    private List<? extends BasicRowRange> ranges;

    public RangeIteration(List<RowRange> sortedAndMergedRanges) {
      this.sortedAndMergedRanges = sortedAndMergedRanges;
    }

    void initialize(boolean reversed) {
      // Avoid double initialization
      assert !this.initialized;
      this.reversed = reversed;
      if (reversed) {
        // If we are doing a reverse scan, we can reverse the ranges (both the elements in
        // the list as well as their start/stop key), and use the same algorithm.
        this.ranges = flipAndReverseRanges(sortedAndMergedRanges);
      } else {
        this.ranges = sortedAndMergedRanges;
      }
      this.initialized = true;
    }

    /**
     * Rebuilds the sorted ranges (by startKey) into an equivalent sorted list of ranges, only by
     * stopKey instead. Descending order and the ReversedRowRange compareTo implementation make
     * sure that we can use Collections.binarySearch().
     */
    static List<ReversedRowRange> flipAndReverseRanges(List<RowRange> ranges) {
      List<ReversedRowRange> flippedRanges = new ArrayList<>(ranges.size());
      for (int i = ranges.size() - 1; i >= 0; i--) {
        RowRange origRange = ranges.get(i);
        ReversedRowRange newRowRange = new ReversedRowRange(
            origRange.startRow, origRange.startRowInclusive, origRange.stopRow,
            origRange.isStopRowInclusive());
        flippedRanges.add(newRowRange);
      }
      return flippedRanges;
    }

    /**
     * Calculates the position where the given rowkey fits in the ranges list.
     *
     * @param rowKey the row key to calculate
     * @return index the position of the row key
     */
    public int getNextRangeIndex(byte[] rowKey) {
      BasicRowRange temp;
      if (reversed) {
        temp = new ReversedRowRange(null, true, rowKey, true);
      } else {
        temp = new RowRange(rowKey, true, null, true);
      }
      // Because we make sure that `ranges` has the correct natural ordering (given it containing
      // RowRange or ReverseRowRange objects). This keeps us from having to have two different
      // implementations below.
      final int index = Collections.binarySearch(ranges, temp);
      if (index < 0) {
        int insertionPosition = -index - 1;
        // check if the row key in the range before the insertion position
        if (insertionPosition != 0 && ranges.get(insertionPosition - 1).contains(rowKey)) {
          return insertionPosition - 1;
        }
        // check if the row key is before the first range
        if (insertionPosition == 0 && !ranges.get(insertionPosition).contains(rowKey)) {
          return ROW_BEFORE_FIRST_RANGE;
        }
        if (!foundFirstRange) {
          foundFirstRange = true;
        }
        return insertionPosition;
      }
      // the row key equals one of the start keys, and the the range exclude the start key
      if(ranges.get(index).isSearchRowInclusive() == false) {
        exclusive = true;
      }
      return index;
    }

    /**
     * Sets {@link #foundFirstRange} to {@code true}, indicating that we found a matching row range.
     */
    public void setFoundFirstRange() {
      this.foundFirstRange = true;
    }

    /**
     * Gets the RowRange at the given offset.
     */
    @SuppressWarnings("unchecked")
    public <T extends BasicRowRange> T get(int i) {
      return (T) ranges.get(i);
    }

    /**
     * Returns true if the first matching row range was found.
     */
    public boolean hasFoundFirstRange() {
      return foundFirstRange;
    }

    /**
     * Returns true if the current range's key is exclusive
     */
    public boolean isExclusive() {
      return exclusive;
    }

    /**
     * Resets the exclusive flag.
     */
    public void resetExclusive() {
      exclusive = false;
    }

    /**
     * Returns true if this class has been initialized by calling {@link #initialize(boolean)}.
     */
    public boolean isInitialized() {
      return initialized;
    }

    /**
     * Returns true if we exhausted searching all row ranges.
     */
    public boolean isIterationComplete(int index) {
      return index >= ranges.size();
    }
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Filter && areSerializedFieldsEqual((Filter) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.ranges);
  }
}
