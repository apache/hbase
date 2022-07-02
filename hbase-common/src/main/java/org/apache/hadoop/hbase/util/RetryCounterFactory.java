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
package org.apache.hadoop.hbase.util;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.util.RetryCounter.ExponentialBackoffPolicyWithLimit;
import org.apache.hadoop.hbase.util.RetryCounter.RetryConfig;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class RetryCounterFactory {
  private final RetryConfig retryConfig;

  public RetryCounterFactory(int sleepIntervalMillis) {
    this(Integer.MAX_VALUE, sleepIntervalMillis);
  }

  public RetryCounterFactory(int maxAttempts, int sleepIntervalMillis) {
    this(maxAttempts, sleepIntervalMillis, -1);
  }

  public RetryCounterFactory(int maxAttempts, int sleepIntervalMillis, int maxSleepTime) {
    this(new RetryConfig(maxAttempts, sleepIntervalMillis, maxSleepTime, TimeUnit.MILLISECONDS,
      new ExponentialBackoffPolicyWithLimit()));
  }

  public RetryCounterFactory(RetryConfig retryConfig) {
    this.retryConfig = retryConfig;
  }

  public RetryCounter create() {
    return new RetryCounter(retryConfig);
  }
}
