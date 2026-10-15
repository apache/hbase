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
package org.apache.hadoop.hbase.mapreduce;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Estimates scan progress based on row key positions within a start/stop range. Custom
 * implementations can be plugged in via {@link RowKeyProgress#PROGRESS_CLASS_KEY}.
 */
@InterfaceAudience.Public
public interface RowKeyProgress {
  String PROGRESS_CLASS_KEY = "hbase.mapreduce.rowkey.progress.class";

  /**
   * Initialize the progress estimator with the start and stop row keys.
   * @param startRow the start row of the scan (inclusive), may be null or empty
   * @param stopRow  the stop row of the scan (exclusive), may be null or empty
   */
  void setStartStopRows(byte[] startRow, byte[] stopRow);

  /**
   * Estimate progress as a fraction between 0.0 and 1.0 based on where {@code currentRow} falls in
   * the range.
   * @param currentRow the last successfully read row key, or null if no row has been read yet
   * @return estimated progress between 0.0 and 1.0, or 0.0 if progress cannot be estimated
   */
  float getProgress(byte[] currentRow);
}
