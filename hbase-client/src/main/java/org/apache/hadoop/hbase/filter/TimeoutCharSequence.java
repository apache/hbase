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

import org.apache.hadoop.hbase.DoNotRetryUncheckedIOException;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils;

/**
 * It checks whether the timeout has been exceeded whenever the charAt method is called.
 */
class TimeoutCharSequence implements CharSequence {
  static final int DEFAULT_CHECK_POINT = 10_000;

  private final CharSequence value;
  private final long startMillis;
  private final long timeoutMillis;
  private final int checkPoint;
  private int numberOfCalls;

  /**
   * Initialize a TimeoutCharSequence.
   * @param value         the original data
   * @param startMillis   time the operation started (ms)
   * @param timeoutMillis the timeout (ms)
   */
  TimeoutCharSequence(CharSequence value, long startMillis, long timeoutMillis) {
    this.value = value;
    this.startMillis = startMillis;
    this.timeoutMillis = timeoutMillis;
    this.checkPoint = DEFAULT_CHECK_POINT;
    this.numberOfCalls = 0;
  }

  /**
   * Initialize a TimeoutCharSequence.
   * @param value         the original data
   * @param startMillis   time the operation started (ms)
   * @param checkPoint    the check point
   * @param timeoutMillis the timeout (ms)
   */
  TimeoutCharSequence(CharSequence value, long startMillis, long timeoutMillis, int checkPoint) {
    this.value = value;
    this.startMillis = startMillis;
    this.timeoutMillis = timeoutMillis;
    this.checkPoint = checkPoint;
    this.numberOfCalls = 0;
  }

  @Override
  public int length() {
    return value.length();
  }

  @Override
  public char charAt(int index) {
    numberOfCalls++;
    if (numberOfCalls % checkPoint == 0) {
      final long diff = EnvironmentEdgeManager.currentTime() - startMillis;
      if (diff > timeoutMillis) {
        throw new DoNotRetryUncheckedIOException(
          String.format("Operation timed out after %s.", StringUtils.formatTime(diff)));
      }
      numberOfCalls = 0;
    }
    return value.charAt(index);
  }

  @Override
  public CharSequence subSequence(int start, int end) {
    return new TimeoutCharSequence(value.subSequence(start, end), startMillis, timeoutMillis);
  }

  @Override
  public String toString() {
    return value.toString();
  }
}
