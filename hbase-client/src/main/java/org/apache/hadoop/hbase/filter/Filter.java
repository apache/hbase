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
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interface for row and column filters directly applied within the regionserver. A filter can
 * expect the following call sequence:
 * <ul>
 * <li>{@link #reset()} : reset the filter state before filtering a new row.</li>
 * <li>{@link #filterAllRemaining()}: true means row scan is over; false means keep going.</li>
 * <li>{@link #filterRowKey(Cell)}: true means drop this row; false means include.</li>
 * <li>{@link #filterCell(Cell)}: decides whether to include or exclude this Cell. See
 * {@link ReturnCode}.</li>
 * <li>{@link #transformCell(Cell)}: if the Cell is included, let the filter transform the Cell.
 * </li>
 * <li>{@link #filterRowCells(List)}: allows direct modification of the final list to be submitted
 * <li>{@link #filterRow()}: last chance to drop entire row based on the sequence of filter calls.
 * Eg: filter a row if it doesn't contain a specified column.</li>
 * </ul>
 * Filter instances are created one per region/scan. This abstract class replaces the old
 * RowFilterInterface. When implementing your own filters, consider inheriting {@link FilterBase} to
 * help you reduce boilerplate.
 * @see FilterBase
 */
@InterfaceAudience.Public
public abstract class Filter {
  protected transient boolean reversed;

  /**
   * Reset the state of the filter between rows. Concrete implementers can signal a failure
   * condition in their code by throwing an {@link IOException}.
   * @throws IOException in case an I/O or an filter specific failure needs to be signaled.
   */
  abstract public void reset() throws IOException;

  /**
   * Filters a row based on the row key. If this returns true, the entire row will be excluded. If
   * false, each KeyValue in the row will be passed to {@link #filterCell(Cell)} below. If
   * {@link #filterAllRemaining()} returns true, then {@link #filterRowKey(Cell)} should also return
   * true. Concrete implementers can signal a failure condition in their code by throwing an
   * {@link IOException}.
   * @param firstRowCell The first cell coming in the new row
   * @return true, remove entire row, false, include the row (maybe).
   * @throws IOException in case an I/O or an filter specific failure needs to be signaled.
   */
  abstract public boolean filterRowKey(Cell firstRowCell) throws IOException;

  /**
   * If this returns true, the scan will terminate. Concrete implementers can signal a failure
   * condition in their code by throwing an {@link IOException}.
   * @return true to end scan, false to continue.
   * @throws IOException in case an I/O or an filter specific failure needs to be signaled.
   */
  abstract public boolean filterAllRemaining() throws IOException;

  /**
   * A way to filter based on the column family, column qualifier and/or the column value. Return
   * code is described below. This allows filters to filter only certain number of columns, then
   * terminate without matching ever column. If filterRowKey returns true, filterCell needs to be
   * consistent with it. filterCell can assume that filterRowKey has already been called for the
   * row. If your filter returns <code>ReturnCode.NEXT_ROW</code>, it should return
   * <code>ReturnCode.NEXT_ROW</code> until {@link #reset()} is called just in case the caller calls
   * for the next row. Concrete implementers can signal a failure condition in their code by
   * throwing an {@link IOException}.
   * @param c the Cell in question
   * @return code as described below
   * @throws IOException in case an I/O or an filter specific failure needs to be signaled.
   * @see Filter.ReturnCode
   */
  public ReturnCode filterCell(final Cell c) throws IOException {
    return ReturnCode.INCLUDE;
  }

  /**
   * Give the filter a chance to transform the passed Cell. If the Cell is changed a new Cell object
   * must be returned.
   * <p/>
   * <strong>NOTICE:</strong> Filter will be evaluate at server side so the returned {@link Cell}
   * must be an {@link org.apache.hadoop.hbase.ExtendedCell}, although it is marked as IA.Private.
   * @see org.apache.hadoop.hbase.KeyValue#shallowCopy() The transformed KeyValue is what is
   *      eventually returned to the client. Most filters will return the passed KeyValue unchanged.
   * @see org.apache.hadoop.hbase.filter.KeyOnlyFilter#transformCell(Cell) for an example of a
   *      transformation. Concrete implementers can signal a failure condition in their code by
   *      throwing an {@link IOException}.
   * @param v the Cell in question
   * @return the changed Cell
   * @throws IOException in case an I/O or an filter specific failure needs to be signaled.
   */
  abstract public Cell transformCell(final Cell v) throws IOException;

  /**
   * Return codes for filterValue().
   */
  @InterfaceAudience.Public
  public enum ReturnCode {
    /**
     * Include the Cell
     */
    INCLUDE,
    /**
     * Include the Cell and seek to the next column skipping older versions.
     */
    INCLUDE_AND_NEXT_COL,
    /**
     * Skip this Cell
     */
    SKIP,
    /**
     * Skip this column. Go to the next column in this row.
     */
    NEXT_COL,
    /**
     * Seek to next row in current family. It may still pass a cell whose family is different but
     * row is the same as previous cell to {@link #filterCell(Cell)} , even if we get a NEXT_ROW
     * returned for previous cell. For more details see HBASE-18368. <br>
     * Once reset() method was invoked, then we switch to the next row for all family, and you can
     * catch the event by invoking CellUtils.matchingRows(previousCell, currentCell). <br>
     * Note that filterRow() will still be called. <br>
     */
    NEXT_ROW,
    /**
     * Seek to next key which is given as hint by the filter.
     */
    SEEK_NEXT_USING_HINT,
    /**
     * Include KeyValue and done with row, seek to next. See NEXT_ROW.
     */
    INCLUDE_AND_SEEK_NEXT_ROW,
  }

  /**
   * Chance to alter the list of Cells to be submitted. Modifications to the list will carry on
   * Concrete implementers can signal a failure condition in their code by throwing an
   * {@link IOException}.
   * @param kvs the list of Cells to be filtered
   * @throws IOException in case an I/O or an filter specific failure needs to be signaled.
   */
  abstract public void filterRowCells(List<Cell> kvs) throws IOException;

  /**
   * Primarily used to check for conflicts with scans(such as scans that do not read a full row at a
   * time).
   * @return True if this filter actively uses filterRowCells(List) or filterRow().
   */
  abstract public boolean hasFilterRow();

  /**
   * Last chance to veto row based on previous {@link #filterCell(Cell)} calls. The filter needs to
   * retain state then return a particular value for this call if they wish to exclude a row if a
   * certain column is missing (for example). Concrete implementers can signal a failure condition
   * in their code by throwing an {@link IOException}.
   * @return true to exclude row, false to include row.
   * @throws IOException in case an I/O or an filter specific failure needs to be signaled.
   */
  abstract public boolean filterRow() throws IOException;

  /**
   * If the filter returns the match code SEEK_NEXT_USING_HINT, then it should also tell which is
   * the next key it must seek to. After receiving the match code SEEK_NEXT_USING_HINT, the
   * QueryMatcher would call this function to find out which key it must next seek to. Concrete
   * implementers can signal a failure condition in their code by throwing an {@link IOException}.
   * <strong>NOTICE:</strong> Filter will be evaluate at server side so the returned {@link Cell}
   * must be an {@link org.apache.hadoop.hbase.ExtendedCell}, although it is marked as IA.Private.
   * @return KeyValue which must be next seeked. return null if the filter is not sure which key to
   *         seek to next.
   * @throws IOException in case an I/O or an filter specific failure needs to be signaled.
   */
  abstract public Cell getNextCellHint(final Cell currentCell) throws IOException;

  /**
   * Provides a seek hint to bypass row-by-row scanning after {@link #filterRowKey(Cell)} rejects a
   * row. When {@code filterRowKey} returns {@code true} the scan pipeline would normally iterate
   * through every remaining cell in the rejected row one-by-one (via {@code nextRow()}) before
   * moving on. If the filter can determine a better forward position — for example, the next range
   * boundary in a {@code MultiRowRangeFilter} — it should return that target cell here, allowing
   * the scanner to seek directly past the unwanted rows.
   * <p>
   * Contract:
   * <ul>
   * <li>Only called after {@link #filterRowKey(Cell)} has returned {@code true} for the same
   * {@code firstRowCell}.</li>
   * <li>Implementations may use state that was set during {@link #filterRowKey(Cell)} (e.g. an
   * updated range pointer), but <strong>must not</strong> invoke {@link #filterCell(Cell)} logic —
   * the caller guarantees that {@code filterCell} has not been called for this row.</li>
   * <li>The returned {@link Cell}, if non-null, must be an
   * {@link org.apache.hadoop.hbase.ExtendedCell} because filters are evaluated on the server
   * side.</li>
   * <li>Returning {@code null} (the default) falls through to the existing {@code nextRow()}
   * behaviour, preserving full backward compatibility.</li>
   * </ul>
   * @param firstRowCell the first cell encountered in the rejected row; contains the row key that
   *                     was passed to {@code filterRowKey}
   * @return a {@link Cell} representing the earliest position the scanner should seek to, or
   *         {@code null} if this filter cannot provide a better position than a sequential skip
   * @throws IOException in case an I/O or filter-specific failure needs to be signaled
   * @see #filterRowKey(Cell)
   */
  public Cell getHintForRejectedRow(final Cell firstRowCell) throws IOException {
    return null;
  }

  /**
   * Provides a seek hint for cells that are structurally skipped by the scan pipeline
   * <em>before</em> {@link #filterCell(Cell)} is ever reached. The pipeline short-circuits on
   * several criteria — time-range mismatch, column-set exclusion, and version-limit exhaustion —
   * and in each case the filter is bypassed entirely. When an implementation can compute a
   * meaningful forward position purely from the cell's coordinates (without needing the
   * {@code filterCell} call sequence), it should return that position here so the scanner can seek
   * ahead instead of advancing one cell at a time.
   * <p>
   * Contract:
   * <ul>
   * <li>May be called for cells that have <strong>never</strong> been passed to
   * {@link #filterCell(Cell)}.</li>
   * <li>Implementations <strong>must not</strong> modify any filter state; this method is treated
   * as logically stateless. Only filters whose hint computation is based solely on immutable
   * configuration (e.g. a fixed column range or a fuzzy-row pattern) should override this.</li>
   * <li>The returned {@link Cell}, if non-null, must be an
   * {@link org.apache.hadoop.hbase.ExtendedCell} because filters are evaluated on the server
   * side.</li>
   * <li>Returning {@code null} (the default) falls through to the existing structural skip/seek
   * behaviour, preserving full backward compatibility.</li>
   * </ul>
   * @param skippedCell the cell that was rejected by the time-range, column, or version gate before
   *                    {@code filterCell} could be consulted
   * @return a {@link Cell} representing the earliest position the scanner should seek to, or
   *         {@code null} if this filter cannot provide a better position than the structural hint
   * @throws IOException in case an I/O or filter-specific failure needs to be signaled
   * @see #filterCell(Cell)
   * @see #getNextCellHint(Cell)
   */
  public Cell getSkipHint(final Cell skippedCell) throws IOException {
    return null;
  }

  /**
   * Check that given column family is essential for filter to check row. Most filters always return
   * true here. But some could have more sophisticated logic which could significantly reduce
   * scanning process by not even touching columns until we are 100% sure that it's data is needed
   * in result. Concrete implementers can signal a failure condition in their code by throwing an
   * {@link IOException}.
   * @throws IOException in case an I/O or an filter specific failure needs to be signaled.
   */
  abstract public boolean isFamilyEssential(byte[] name) throws IOException;

  /**
   * TODO: JAVADOC Concrete implementers can signal a failure condition in their code by throwing an
   * {@link IOException}.
   * @return The filter serialized using pb
   * @throws IOException in case an I/O or an filter specific failure needs to be signaled.
   */
  abstract public byte[] toByteArray() throws IOException;

  /**
   * Concrete implementers can signal a failure condition in their code by throwing an
   * {@link IOException}.
   * @param pbBytes A pb serialized {@link Filter} instance
   * @return An instance of {@link Filter} made from <code>bytes</code>
   * @throws DeserializationException if an error occurred
   * @see #toByteArray
   */
  public static Filter parseFrom(final byte[] pbBytes) throws DeserializationException {
    throw new DeserializationException(
      "parseFrom called on base Filter, but should be called on derived type");
  }

  /**
   * Concrete implementers can signal a failure condition in their code by throwing an
   * {@link IOException}.
   * @return true if and only if the fields of the filter that are serialized are equal to the
   *         corresponding fields in other. Used for testing.
   */
  abstract boolean areSerializedFieldsEqual(Filter other);

  /**
   * alter the reversed scan flag
   * @param reversed flag
   */
  public void setReversed(boolean reversed) {
    this.reversed = reversed;
  }

  public boolean isReversed() {
    return this.reversed;
  }
}
