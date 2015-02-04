/**
 *
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
package org.apache.hadoop.hbase.regionserver;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Internal scanners differ from client-side scanners in that they operate on
 * HStoreKeys and byte[] instead of RowResults. This is because they are
 * actually close to how the data is physically stored, and therefore it is more
 * convenient to interact with them that way. It is also much easier to merge
 * the results across SortedMaps than RowResults.
 *
 * <p>Additionally, we need to be able to determine if the scanner is doing
 * wildcard column matches (when only a column family is specified or if a
 * column regex is specified) or if multiple members of the same column family
 * were specified. If so, we need to ignore the timestamp to ensure that we get
 * all the family members, as they may have been last updated at different
 * times.
 */
@InterfaceAudience.Private
public interface InternalScanner extends Closeable {
  /**
   * This class encapsulates all the meaningful state information that we would like the know about
   * after a call to {@link InternalScanner#next(List)}. While this is not an enum, a restriction on
   * the possible states is implied through the exposed {@link #makeState(State)} method.
   */
  public static class NextState {
    /**
     * The possible states we want to restrict ourselves to. This enum is not sufficient to
     * encapsulate all of the state information since some of the fields of the state must be
     * dynamic (e.g. resultSize).
     */
    public enum State {
      MORE_VALUES(true),
      NO_MORE_VALUES(false),
      SIZE_LIMIT_REACHED(true),
      BATCH_LIMIT_REACHED(true);

      private boolean moreValues;

      private State(final boolean moreValues) {
        this.moreValues = moreValues;
      }

      /**
       * @return true when the state indicates that more values may follow those that have been
       *         returned
       */
      public boolean hasMoreValues() {
        return this.moreValues;
      }
    }

    /**
     * state variables
     */
    private final State state;
    private long resultSize;

    /**
     * Value to use for resultSize when the size has not been calculated. Must be a negative number
     * so that {@link NextState#hasResultSizeEstimate()} returns false.
     */
    private static final long DEFAULT_RESULT_SIZE = -1;

    private NextState(State state, long resultSize) {
      this.state = state;
      this.resultSize = resultSize;
    }

    /**
     * @param state
     * @return An instance of {@link NextState} where the size of the results returned from the call
     *         to {@link InternalScanner#next(List)} is unknown. It it the responsibility of the
     *         caller of {@link InternalScanner#next(List)} to calculate the result size if needed
     */
    public static NextState makeState(final State state) {
      return makeState(state, DEFAULT_RESULT_SIZE);
    }

    /**
     * @param state
     * @param resultSize
     * @return An instance of {@link NextState} where the size of the values returned from the call
     *         to {@link InternalScanner#next(List)} is known. The caller can avoid recalculating
     *         the result size by using the cached value retrievable via {@link #getResultSize()}
     */
    public static NextState makeState(final State state, long resultSize) {
      switch (state) {
      case MORE_VALUES:
        return createMoreValuesState(resultSize);
      case NO_MORE_VALUES:
        return createNoMoreValuesState(resultSize);
      case BATCH_LIMIT_REACHED:
        return createBatchLimitReachedState(resultSize);
      case SIZE_LIMIT_REACHED:
        return createSizeLimitReachedState(resultSize);
      default:
        // If the state is not recognized, default to no more value state
        return createNoMoreValuesState(resultSize);
      }
    }

    /**
     * Convenience method for creating a state that indicates that more values can be scanned
     * @param resultSize estimate of the size (heap size) of the values returned from the call to
     *          {@link InternalScanner#next(List)}
     */
    private static NextState createMoreValuesState(long resultSize) {
      return new NextState(State.MORE_VALUES, resultSize);
    }

    /**
     * Convenience method for creating a state that indicates that no more values can be scanned.
     * @param resultSize estimate of the size (heap size) of the values returned from the call to
     *          {@link InternalScanner#next(List)}
     */
    private static NextState createNoMoreValuesState(long resultSize) {
      return new NextState(State.NO_MORE_VALUES, resultSize);
    }

    /**
     * Convenience method for creating a state that indicates that the scan stopped because the
     * batch limit was exceeded
     * @param resultSize estimate of the size (heap size) of the values returned from the call to
     *          {@link InternalScanner#next(List)}
     */
    private static NextState createBatchLimitReachedState(long resultSize) {
      return new NextState(State.BATCH_LIMIT_REACHED, resultSize);
    }

    /**
     * Convenience method for creating a state that indicates that the scan stopped due to the size
     * limit
     * @param resultSize estimate of the size (heap size) of the values returned from the call to
     *          {@link InternalScanner#next(List)}
     */
    private static NextState createSizeLimitReachedState(long resultSize) {
      return new NextState(State.SIZE_LIMIT_REACHED, resultSize);
    }

    /**
     * @return true when the scanner has more values to be scanned following the values returned by
     *         the call to {@link InternalScanner#next(List)}
     */
    public boolean hasMoreValues() {
      return this.state.hasMoreValues();
    }

    /**
     * @return true when the scanner had to stop scanning because it reached the batch limit
     */
    public boolean batchLimitReached() {
      return this.state == State.BATCH_LIMIT_REACHED;
    }

    /**
     * @return true when the scanner had to stop scanning because it reached the size limit
     */
    public boolean sizeLimitReached() {
      return this.state == State.SIZE_LIMIT_REACHED;
    }

    /**
     * @return The size (heap size) of the values that were returned from the call to
     *         {@link InternalScanner#next(List)}. This value should only be used if
     *         {@link #hasResultSizeEstimate()} returns true.
     */
    public long getResultSize() {
      return resultSize;
    }

    /**
     * @return true when an estimate for the size of the values returned by
     *         {@link InternalScanner#next(List)} was provided. If false, it is the responsibility
     *         of the caller to calculate the result size
     */
    public boolean hasResultSizeEstimate() {
      return resultSize >= 0;
    }

    /**
     * Helper method to centralize all checks as to whether or not the state is valid.
     * @param state
     * @return true when the state is valid
     */
    public static boolean isValidState(NextState state) {
      return state != null;
    }

    /**
     * @param state
     * @return true when the state is non null and indicates that more values exist
     */
    public static boolean hasMoreValues(NextState state) {
      return state != null && state.hasMoreValues();
    }
  }

  /**
   * Grab the next row's worth of values.
   * @param results return output array
   * @return state where {@link NextState#hasMoreValues()} is true if more rows exist after this
   *         one, false if scanner is done
   * @throws IOException e
   */
  NextState next(List<Cell> results) throws IOException;

  /**
   * Grab the next row's worth of values with a limit on the number of values to return.
   * @param result return output array
   * @param limit limit on row count to get
   * @return state where {@link NextState#hasMoreValues()} is true if more rows exist after this
   *         one, false if scanner is done
   * @throws IOException e
   */
  NextState next(List<Cell> result, int limit) throws IOException;

  /**
   * Grab the next row's worth of values with a limit on the number of values to return as well as a
   * restriction on the size of the list of values that are returned.
   * @param result return output array
   * @param limit limit on row count to get
   * @param remainingResultSize limit on the size of the result being returned
   * @return state where {@link NextState#hasMoreValues()} is true if more rows exist after this
   *         one, false if scanner is done
   * @throws IOException e
   */
  NextState next(List<Cell> result, int limit, long remainingResultSize) throws IOException;

  /**
   * Closes the scanner and releases any resources it has allocated
   * @throws IOException
   */
  void close() throws IOException;
}
