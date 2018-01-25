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

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * This is a special {@link ScannerContext} subclass that is designed to be used globally when
 * limits should not be enforced during invocations of {@link InternalScanner#next(java.util.List)}
 * or {@link RegionScanner#next(java.util.List)}.
 * <p>
 * Instances of {@link NoLimitScannerContext} are immutable after construction. Any attempt to
 * change the limits or progress of a {@link NoLimitScannerContext} will fail silently. The net
 * effect is that all limit checks will return false, thus indicating that a limit has not been
 * reached.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public class NoLimitScannerContext extends ScannerContext {

  public NoLimitScannerContext() {
    super(false, null, false);
  }

  /**
   * Use this instance whenever limits do not need to be enforced.
   */
  private static final ScannerContext NO_LIMIT = new NoLimitScannerContext();

  /**
   * @return The static, immutable instance of {@link NoLimitScannerContext} to be used whenever
   *         limits should not be enforced
   */
  public static final ScannerContext getInstance() {
    return NO_LIMIT;
  }

  @Override
  void setKeepProgress(boolean keepProgress) {
    // Do nothing. NoLimitScannerContext instances are immutable post-construction
  }

  @Override
  void setBatchProgress(int batchProgress) {
    // Do nothing. NoLimitScannerContext instances are immutable post-construction
  }

  @Override
  void setSizeProgress(long sizeProgress, long heapSizeProgress) {
    // Do nothing. NoLimitScannerContext instances are immutable post-construction
  }

  @Override
  void setProgress(int batchProgress, long sizeProgress, long heapSizeProgress) {
    // Do nothing. NoLimitScannerContext instances are immutable post-construction
  }

  @Override
  void clearProgress() {
    // Do nothing. NoLimitScannerContext instances are immutable post-construction
  }

  @Override
  void setSizeLimitScope(LimitScope scope) {
    // Do nothing. NoLimitScannerContext instances are immutable post-construction
  }

  @Override
  void setTimeLimitScope(LimitScope scope) {
    // Do nothing. NoLimitScannerContext instances are immutable post-construction
  }

  @Override
  NextState setScannerState(NextState state) {
    // Do nothing. NoLimitScannerContext instances are immutable post-construction
    return state;
  }

  @Override
  boolean checkBatchLimit(LimitScope checkerScope) {
    // No limits can be specified, thus return false to indicate no limit has been reached.
    return false;
  }

  @Override
  boolean checkSizeLimit(LimitScope checkerScope) {
    // No limits can be specified, thus return false to indicate no limit has been reached.
    return false;
  }

  @Override
  boolean checkTimeLimit(LimitScope checkerScope) {
    // No limits can be specified, thus return false to indicate no limit has been reached.
    return false;
  }

  @Override
  boolean checkAnyLimitReached(LimitScope checkerScope) {
    return false;
  }
}
