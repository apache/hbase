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

package org.apache.hadoop.hbase.metrics;

/**
 * Interface for sources that will export JvmPauseMonitor metrics
 */
public interface JvmPauseMonitorSource  {

  String INFO_THRESHOLD_COUNT_KEY = "pauseInfoThresholdExceeded";
  String INFO_THRESHOLD_COUNT_DESC = "Count of INFO level pause threshold alerts";
  String WARN_THRESHOLD_COUNT_KEY = "pauseWarnThresholdExceeded";
  String WARN_THRESHOLD_COUNT_DESC = "Count of WARN level pause threshold alerts";

  String PAUSE_TIME_WITH_GC_KEY = "pauseTimeWithGc";
  String PAUSE_TIME_WITH_GC_DESC = "Histogram for excessive pause times with GC activity detected";

  String PAUSE_TIME_WITHOUT_GC_KEY = "pauseTimeWithoutGc";
  String PAUSE_TIME_WITHOUT_GC_DESC =
      "Histogram for excessive pause times without GC activity detected";

  /**
   * Increment the INFO level threshold exceeded count
   * @param count the count
   */
  void incInfoThresholdExceeded(int count);

  /**
   * Increment the WARN level threshold exceeded count
   * @param count the count
   */
  void incWarnThresholdExceeded(int count);

  /**
   * Update the pause time histogram where GC activity was detected.
   *
   * @param t time it took
   */
  void updatePauseTimeWithGc(long t);

  /**
   * Update the pause time histogram where GC activity was not detected.
   *
   * @param t time it took
   */
  void updatePauseTimeWithoutGc(long t);
}
