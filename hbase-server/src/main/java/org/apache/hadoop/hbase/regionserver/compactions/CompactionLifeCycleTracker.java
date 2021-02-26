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
package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Used to track compaction execution.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface CompactionLifeCycleTracker {

  static CompactionLifeCycleTracker DUMMY = new CompactionLifeCycleTracker() {
  };

  /**
   * Called if the compaction request is failed for some reason.
   */
  default void notExecuted(Store store, String reason) {
  }

  /**
   * Called before compaction is executed by CompactSplitThread.
   * <p>
   * Requesting compaction on a region can lead to multiple compactions on different stores, so we
   * will pass the {@link Store} in to tell you the store we operate on.
   */
  default void beforeExecution(Store store) {
  }

  /**
   * Called after compaction is executed by CompactSplitThread.
   * <p>
   * Requesting compaction on a region can lead to multiple compactions on different stores, so we
   * will pass the {@link Store} in to tell you the store we operate on.
   */
  default void afterExecution(Store store) {
  }

  /**
   * Called after all the requested compactions are completed.
   * <p>
   * The compaction scheduling is per Store so if you request a compaction on a region it may lead
   * to multiple compactions.
   */
  default void completed() {
  }
}
