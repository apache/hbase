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
package org.apache.hadoop.hbase.regionserver;

import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.client.metrics.ServerSideScanMetrics;

/**
 * ScannerContext instances encapsulate limit tracking AND progress towards those limits during
 * invocations of {@link InternalScanner#next(java.util.List)} and
 * {@link RegionScanner#next(java.util.List)}.
 * <p>
 * A ScannerContext instance should be updated periodically throughout execution whenever progress
 * towards a limit has been made. Each limit can be checked via the appropriate checkLimit method.
 * <p>
 * Once a limit has been reached, the scan will stop. The invoker of
 * {@link InternalScanner#next(java.util.List)} or {@link RegionScanner#next(java.util.List)} can
 * use the appropriate check*Limit methods to see exactly which limits have been reached.
 * Alternatively, {@link #checkAnyLimitReached(LimitScope)} is provided to see if ANY limit was
 * reached
 * <p>
 * {@link NoLimitScannerContext#NO_LIMIT} is an immutable static definition that can be used
 * whenever a {@link ScannerContext} is needed but limits do not need to be enforced.
 * <p>
 * NOTE: It is important that this class only ever expose setter methods that can be safely skipped
 * when limits should be NOT enforced. This is because of the necessary immutability of the class
 * {@link NoLimitScannerContext}. If a setter cannot be safely skipped, the immutable nature of
 * {@link NoLimitScannerContext} will lead to incorrect behavior.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public class ScannerContext {

  LimitFields limits;
  /**
   * A different set of progress fields. Only include batch, dataSize and heapSize. Compare to
   * LimitFields, ProgressFields doesn't contain time field. As we save a deadline in LimitFields,
   * so use {@link System#currentTimeMillis()} directly when check time limit.
   */
  ProgressFields progress;

  /**
   * The state of the scanner after the invocation of {@link InternalScanner#next(java.util.List)}
   * or {@link RegionScanner#next(java.util.List)}.
   */
  NextState scannerState;
  private static final NextState DEFAULT_STATE = NextState.MORE_VALUES;

  /**
   * Used as an indication to invocations of {@link InternalScanner#next(java.util.List)} and
   * {@link RegionScanner#next(java.util.List)} that, if true, the progress tracked within this
   * {@link ScannerContext} instance should be considered while evaluating the limits. Useful for
   * enforcing a set of limits across multiple calls (i.e. the limit may not be reached in a single
   * invocation, but any progress made should be considered in future invocations)
   * <p>
   * Defaulting this value to false means that, by default, any tracked progress will be wiped clean
   * on invocations to {@link InternalScanner#next(java.util.List)} and
   * {@link RegionScanner#next(java.util.List)} and the call will be treated as though no progress
   * has been made towards the limits so far.
   * <p>
   * This is an important mechanism. Users of Internal/Region scanners expect that they can define
   * some limits and then repeatedly invoke {@link InternalScanner#next(List)} or
   * {@link RegionScanner#next(List)} where each invocation respects these limits separately.
   * <p>
   * For example: <pre> {@code
   * ScannerContext context = new ScannerContext.newBuilder().setBatchLimit(5).build();
   * RegionScanner scanner = ...
   * List<Cell> results = new ArrayList<Cell>();
   * while(scanner.next(results, context)) {
   *   // Do something with a batch of 5 cells
   * }
   * }</pre> However, in the case of RPCs, the server wants to be able to define a set of
   * limits for a particular RPC request and have those limits respected across multiple
   * invocations. This means that the progress made towards the limits in earlier calls will be
   * saved and considered in future invocations
   */
  boolean keepProgress;
  private static boolean DEFAULT_KEEP_PROGRESS = false;

  private Cell lastPeekedCell = null;

  // Set this to true will have the same behavior with reaching the time limit.
  // This is used when you want to make the current RSRpcService.scan returns immediately. For
  // example, when we want to switch from pread to stream, we can only do it after the rpc call is
  // returned.
  private boolean returnImmediately;

  /**
   * Tracks the relevant server side metrics during scans. null when metrics should not be tracked
   */
  final ServerSideScanMetrics metrics;

  ScannerContext(boolean keepProgress, LimitFields limitsToCopy, boolean trackMetrics) {
    this.limits = new LimitFields();
    if (limitsToCopy != null) {
      this.limits.copy(limitsToCopy);
    }

    // Progress fields are initialized to 0
    progress = new ProgressFields(0, 0, 0);

    this.keepProgress = keepProgress;
    this.scannerState = DEFAULT_STATE;
    this.metrics = trackMetrics ? new ServerSideScanMetrics() : null;
  }

  public boolean isTrackingMetrics() {
    return this.metrics != null;
  }

  /**
   * Get the metrics instance. Should only be called after a call to {@link #isTrackingMetrics()}
   * has been made to confirm that metrics are indeed being tracked.
   * @return {@link ServerSideScanMetrics} instance that is tracking metrics for this scan
   */
  public ServerSideScanMetrics getMetrics() {
    assert isTrackingMetrics();
    return this.metrics;
  }

  /**
   * @return true if the progress tracked so far in this instance will be considered during an
   *         invocation of {@link InternalScanner#next(java.util.List)} or
   *         {@link RegionScanner#next(java.util.List)}. false when the progress tracked so far
   *         should not be considered and should instead be wiped away via {@link #clearProgress()}
   */
  boolean getKeepProgress() {
    return keepProgress;
  }

  void setKeepProgress(boolean keepProgress) {
    this.keepProgress = keepProgress;
  }

  /**
   * Progress towards the batch limit has been made. Increment internal tracking of batch progress
   */
  void incrementBatchProgress(int batch) {
    int currentBatch = progress.getBatch();
    progress.setBatch(currentBatch + batch);
  }

  /**
   * Progress towards the size limit has been made. Increment internal tracking of size progress
   */
  void incrementSizeProgress(long dataSize, long heapSize) {
    long curDataSize = progress.getDataSize();
    progress.setDataSize(curDataSize + dataSize);
    long curHeapSize = progress.getHeapSize();
    progress.setHeapSize(curHeapSize + heapSize);
  }

  int getBatchProgress() {
    return progress.getBatch();
  }

  long getDataSizeProgress() {
    return progress.getDataSize();
  }

  long getHeapSizeProgress() {
    return progress.getHeapSize();
  }

  void setProgress(int batchProgress, long sizeProgress, long heapSizeProgress) {
    setBatchProgress(batchProgress);
    setSizeProgress(sizeProgress, heapSizeProgress);
  }

  void setSizeProgress(long dataSizeProgress, long heapSizeProgress) {
    progress.setDataSize(dataSizeProgress);
    progress.setHeapSize(heapSizeProgress);
  }

  void setBatchProgress(int batchProgress) {
    progress.setBatch(batchProgress);
  }

  /**
   * Clear away any progress that has been made so far. All progress fields are reset to initial
   * values
   */
  void clearProgress() {
    progress.setFields(0, 0, 0);
  }

  /**
   * Note that this is not a typical setter. This setter returns the {@link NextState} that was
   * passed in so that methods can be invoked against the new state. Furthermore, this pattern
   * allows the {@link NoLimitScannerContext} to cleanly override this setter and simply return the
   * new state, thus preserving the immutability of {@link NoLimitScannerContext}
   * @param state
   * @return The state that was passed in.
   */
  NextState setScannerState(NextState state) {
    if (!NextState.isValidState(state)) {
      throw new IllegalArgumentException("Cannot set to invalid state: " + state);
    }

    this.scannerState = state;
    return state;
  }

  /**
   * @return true when we have more cells for the current row. This usually because we have reached
   *         a limit in the middle of a row
   */
  boolean mayHaveMoreCellsInRow() {
    return scannerState == NextState.SIZE_LIMIT_REACHED_MID_ROW ||
        scannerState == NextState.TIME_LIMIT_REACHED_MID_ROW ||
        scannerState == NextState.BATCH_LIMIT_REACHED;
  }

  /**
   * @param checkerScope
   * @return true if the batch limit can be enforced in the checker's scope
   */
  boolean hasBatchLimit(LimitScope checkerScope) {
    return limits.canEnforceBatchLimitFromScope(checkerScope) && limits.getBatch() > 0;
  }

  /**
   * @param checkerScope
   * @return true if the size limit can be enforced in the checker's scope
   */
  boolean hasSizeLimit(LimitScope checkerScope) {
    return limits.canEnforceSizeLimitFromScope(checkerScope)
        && (limits.getDataSize() > 0 || limits.getHeapSize() > 0);
  }

  /**
   * @param checkerScope
   * @return true if the time limit can be enforced in the checker's scope
   */
  boolean hasTimeLimit(LimitScope checkerScope) {
    return limits.canEnforceTimeLimitFromScope(checkerScope) &&
      (limits.getTime() > 0 || returnImmediately);
  }

  /**
   * @param checkerScope
   * @return true if any limit can be enforced within the checker's scope
   */
  boolean hasAnyLimit(LimitScope checkerScope) {
    return hasBatchLimit(checkerScope) || hasSizeLimit(checkerScope) || hasTimeLimit(checkerScope);
  }

  /**
   * @param scope The scope in which the size limit will be enforced
   */
  void setSizeLimitScope(LimitScope scope) {
    limits.setSizeScope(scope);
  }

  /**
   * @param scope The scope in which the time limit will be enforced
   */
  void setTimeLimitScope(LimitScope scope) {
    limits.setTimeScope(scope);
  }

  int getBatchLimit() {
    return limits.getBatch();
  }

  long getDataSizeLimit() {
    return limits.getDataSize();
  }

  long getTimeLimit() {
    return limits.getTime();
  }

  /**
   * @param checkerScope The scope that the limit is being checked from
   * @return true when the limit is enforceable from the checker's scope and it has been reached
   */
  boolean checkBatchLimit(LimitScope checkerScope) {
    return hasBatchLimit(checkerScope) && progress.getBatch() >= limits.getBatch();
  }

  /**
   * @param checkerScope The scope that the limit is being checked from
   * @return true when the limit is enforceable from the checker's scope and it has been reached
   */
  boolean checkSizeLimit(LimitScope checkerScope) {
    return hasSizeLimit(checkerScope) && (progress.getDataSize() >= limits.getDataSize()
        || progress.getHeapSize() >= limits.getHeapSize());
  }

  /**
   * @param checkerScope The scope that the limit is being checked from. The time limit is always
   *          checked against {@link System#currentTimeMillis()}
   * @return true when the limit is enforceable from the checker's scope and it has been reached
   */
  boolean checkTimeLimit(LimitScope checkerScope) {
    return hasTimeLimit(checkerScope) &&
      (returnImmediately || System.currentTimeMillis() >= limits.getTime());
  }

  /**
   * @param checkerScope The scope that the limits are being checked from
   * @return true when some limit is enforceable from the checker's scope and it has been reached
   */
  boolean checkAnyLimitReached(LimitScope checkerScope) {
    return checkSizeLimit(checkerScope) || checkBatchLimit(checkerScope)
        || checkTimeLimit(checkerScope);
  }

  Cell getLastPeekedCell() {
    return lastPeekedCell;
  }

  void setLastPeekedCell(Cell lastPeekedCell) {
    this.lastPeekedCell = lastPeekedCell;
  }

  void returnImmediately() {
    this.returnImmediately = true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");

    sb.append("limits:");
    sb.append(limits);

    sb.append(", progress:");
    sb.append(progress);

    sb.append(", keepProgress:");
    sb.append(keepProgress);

    sb.append(", state:");
    sb.append(scannerState);

    sb.append("}");
    return sb.toString();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(boolean keepProgress) {
    return new Builder(keepProgress);
  }

  public static final class Builder {
    boolean keepProgress = DEFAULT_KEEP_PROGRESS;
    boolean trackMetrics = false;
    LimitFields limits = new LimitFields();

    private Builder() {
    }

    private Builder(boolean keepProgress) {
      this.keepProgress = keepProgress;
    }

    public Builder setKeepProgress(boolean keepProgress) {
      this.keepProgress = keepProgress;
      return this;
    }

    public Builder setTrackMetrics(boolean trackMetrics) {
      this.trackMetrics = trackMetrics;
      return this;
    }

    public Builder setSizeLimit(LimitScope sizeScope, long dataSizeLimit, long heapSizeLimit) {
      limits.setDataSize(dataSizeLimit);
      limits.setHeapSize(heapSizeLimit);
      limits.setSizeScope(sizeScope);
      return this;
    }

    public Builder setTimeLimit(LimitScope timeScope, long timeLimit) {
      limits.setTime(timeLimit);
      limits.setTimeScope(timeScope);
      return this;
    }

    public Builder setBatchLimit(int batchLimit) {
      limits.setBatch(batchLimit);
      return this;
    }

    public ScannerContext build() {
      return new ScannerContext(keepProgress, limits, trackMetrics);
    }
  }

  /**
   * The possible states a scanner may be in following a call to {@link InternalScanner#next(List)}
   */
  public enum NextState {
    MORE_VALUES(true, false),
    NO_MORE_VALUES(false, false),
    SIZE_LIMIT_REACHED(true, true),

    /**
     * Special case of size limit reached to indicate that the size limit was reached in the middle
     * of a row and thus a partial results was formed
     */
    SIZE_LIMIT_REACHED_MID_ROW(true, true),
    TIME_LIMIT_REACHED(true, true),

    /**
     * Special case of time limit reached to indicate that the time limit was reached in the middle
     * of a row and thus a partial results was formed
     */
    TIME_LIMIT_REACHED_MID_ROW(true, true),
    BATCH_LIMIT_REACHED(true, true);

    private final boolean moreValues;
    private final boolean limitReached;

    private NextState(boolean moreValues, boolean limitReached) {
      this.moreValues = moreValues;
      this.limitReached = limitReached;
    }

    /**
     * @return true when the state indicates that more values may follow those that have been
     *         returned
     */
    public boolean hasMoreValues() {
      return this.moreValues;
    }

    /**
     * @return true when the state indicates that a limit has been reached and scan should stop
     */
    public boolean limitReached() {
      return this.limitReached;
    }

    public static boolean isValidState(NextState state) {
      return state != null;
    }

    public static boolean hasMoreValues(NextState state) {
      return isValidState(state) && state.hasMoreValues();
    }
  }

  /**
   * The various scopes where a limit can be enforced. Used to differentiate when a limit should be
   * enforced or not.
   */
  public enum LimitScope {
    /**
     * Enforcing a limit between rows means that the limit will not be considered until all the
     * cells for a particular row have been retrieved
     */
    BETWEEN_ROWS(0),

    /**
     * Enforcing a limit between cells means that the limit will be considered after each full cell
     * has been retrieved
     */
    BETWEEN_CELLS(1);

    /**
     * When enforcing a limit, we must check that the scope is appropriate for enforcement.
     * <p>
     * To communicate this concept, each scope has a depth. A limit will be enforced if the depth of
     * the checker's scope is less than or equal to the limit's scope. This means that when checking
     * limits, the checker must know their own scope (i.e. are they checking the limits between
     * rows, between cells, etc...)
     */
    final int depth;

    LimitScope(int depth) {
      this.depth = depth;
    }

    final int depth() {
      return depth;
    }

    /**
     * @param checkerScope The scope in which the limit is being checked
     * @return true when the checker is in a scope that indicates the limit can be enforced. Limits
     *         can be enforced from "higher or equal" scopes (i.e. the checker's scope is at a
     *         lesser depth than the limit)
     */
    boolean canEnforceLimitFromScope(LimitScope checkerScope) {
      return checkerScope != null && checkerScope.depth() <= depth;
    }
  }

  /**
   * The different fields that can be used as limits in calls to
   * {@link InternalScanner#next(java.util.List)} and {@link RegionScanner#next(java.util.List)}
   */
  private static class LimitFields {
    /**
     * Default values of the limit fields. Defined such that if a field does NOT change from its
     * default, it will not be enforced
     */
    private static int DEFAULT_BATCH = -1;
    private static long DEFAULT_SIZE = -1L;
    private static long DEFAULT_TIME = -1L;

    /**
     * Default scope that is assigned to a limit if a scope is not specified.
     */
    private static final LimitScope DEFAULT_SCOPE = LimitScope.BETWEEN_ROWS;

    // The batch limit will always be enforced between cells, thus, there isn't a field to hold the
    // batch scope
    int batch = DEFAULT_BATCH;

    LimitScope sizeScope = DEFAULT_SCOPE;
    // The sum of cell data sizes(key + value). The Cell data might be in on heap or off heap area.
    long dataSize = DEFAULT_SIZE;
    // The sum of heap space occupied by all tracked cells. This includes Cell POJO's overhead as
    // such AND data cells of Cells which are in on heap area.
    long heapSize = DEFAULT_SIZE;

    LimitScope timeScope = DEFAULT_SCOPE;
    long time = DEFAULT_TIME;

    /**
     * Fields keep their default values.
     */
    LimitFields() {
    }

    void copy(LimitFields limitsToCopy) {
      if (limitsToCopy != null) {
        setFields(limitsToCopy.getBatch(), limitsToCopy.getSizeScope(), limitsToCopy.getDataSize(),
            limitsToCopy.getHeapSize(), limitsToCopy.getTimeScope(), limitsToCopy.getTime());
      }
    }

    /**
     * Set all fields together.
     */
    void setFields(int batch, LimitScope sizeScope, long dataSize, long heapSize,
        LimitScope timeScope, long time) {
      setBatch(batch);
      setSizeScope(sizeScope);
      setDataSize(dataSize);
      setHeapSize(heapSize);
      setTimeScope(timeScope);
      setTime(time);
    }

    int getBatch() {
      return this.batch;
    }

    void setBatch(int batch) {
      this.batch = batch;
    }

    /**
     * @param checkerScope
     * @return true when the limit can be enforced from the scope of the checker
     */
    boolean canEnforceBatchLimitFromScope(LimitScope checkerScope) {
      return LimitScope.BETWEEN_CELLS.canEnforceLimitFromScope(checkerScope);
    }

    long getDataSize() {
      return this.dataSize;
    }

    long getHeapSize() {
      return this.heapSize;
    }

    void setDataSize(long dataSize) {
      this.dataSize = dataSize;
    }

    void setHeapSize(long heapSize) {
      this.heapSize = heapSize;
    }

    /**
     * @return {@link LimitScope} indicating scope in which the size limit is enforced
     */
    LimitScope getSizeScope() {
      return this.sizeScope;
    }

    /**
     * Change the scope in which the size limit is enforced
     */
    void setSizeScope(LimitScope scope) {
      this.sizeScope = scope;
    }

    /**
     * @param checkerScope
     * @return true when the limit can be enforced from the scope of the checker
     */
    boolean canEnforceSizeLimitFromScope(LimitScope checkerScope) {
      return this.sizeScope.canEnforceLimitFromScope(checkerScope);
    }

    long getTime() {
      return this.time;
    }

    void setTime(long time) {
      this.time = time;
    }

    /**
     * @return {@link LimitScope} indicating scope in which the time limit is enforced
     */
    LimitScope getTimeScope() {
      return this.timeScope;
    }

    /**
     * Change the scope in which the time limit is enforced
     */
    void setTimeScope(LimitScope scope) {
      this.timeScope = scope;
    }

    /**
     * @param checkerScope
     * @return true when the limit can be enforced from the scope of the checker
     */
    boolean canEnforceTimeLimitFromScope(LimitScope checkerScope) {
      return this.timeScope.canEnforceLimitFromScope(checkerScope);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{");

      sb.append("batch:");
      sb.append(batch);

      sb.append(", dataSize:");
      sb.append(dataSize);

      sb.append(", heapSize:");
      sb.append(heapSize);

      sb.append(", sizeScope:");
      sb.append(sizeScope);

      sb.append(", time:");
      sb.append(time);

      sb.append(", timeScope:");
      sb.append(timeScope);

      sb.append("}");
      return sb.toString();
    }
  }

  private static class ProgressFields {

    private static int DEFAULT_BATCH = -1;
    private static long DEFAULT_SIZE = -1L;

    // The batch limit will always be enforced between cells, thus, there isn't a field to hold the
    // batch scope
    int batch = DEFAULT_BATCH;

    // The sum of cell data sizes(key + value). The Cell data might be in on heap or off heap area.
    long dataSize = DEFAULT_SIZE;
    // The sum of heap space occupied by all tracked cells. This includes Cell POJO's overhead as
    // such AND data cells of Cells which are in on heap area.
    long heapSize = DEFAULT_SIZE;

    ProgressFields(int batch, long size, long heapSize) {
      setFields(batch, size, heapSize);
    }

    /**
     * Set all fields together.
     */
    void setFields(int batch, long dataSize, long heapSize) {
      setBatch(batch);
      setDataSize(dataSize);
      setHeapSize(heapSize);
    }

    int getBatch() {
      return this.batch;
    }

    void setBatch(int batch) {
      this.batch = batch;
    }

    long getDataSize() {
      return this.dataSize;
    }

    long getHeapSize() {
      return this.heapSize;
    }

    void setDataSize(long dataSize) {
      this.dataSize = dataSize;
    }

    void setHeapSize(long heapSize) {
      this.heapSize = heapSize;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{");

      sb.append("batch:");
      sb.append(batch);

      sb.append(", dataSize:");
      sb.append(dataSize);

      sb.append(", heapSize:");
      sb.append(heapSize);

      sb.append("}");
      return sb.toString();
    }
  }
}
