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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Modern per-operation interceptor for HBase client operations. Each operation gets a fresh
 * interceptor instance, eliminating thread-safety concerns and making implementation simple.
 * <p>
 * This interceptor tracks both single operations (get, put, delete) and batch operations (multi-row
 * operations). Batch operations are treated as single operations for simplicity.
 * <p>
 * All fields are automatically populated by the HBase client.
 * <p>
 * Usage example:
 *
 * <pre>
 * public class MyInterceptor extends OperationInterceptor {
 *   public MyInterceptor(long operationStartTime) {
 *     super(operationStartTime);
 *   }
 *
 *   public void afterAttemptFailure(RetryingCallable<?> callable, Throwable cause) {
 *     long attemptDuration = System.currentTimeMillis() - getCurrentAttemptStartTime();
 *     long operationDuration = System.currentTimeMillis() - getOperationStartTime();
 *
 *     // Fast-fail after 5 attempts or 30 seconds
 *     if (getAttemptNumber() >= 4 || operationDuration > 30000) {
 *       throw new FastFailException("Operation taking too long");
 *     }
 *
 *     recordMetric("attempt.failure.duration", attemptDuration);
 *     recordMetric("attempt.failure.type", cause.getClass().getSimpleName());
 *   }
 * }
 * </pre>
 */
@InterfaceAudience.Public
public abstract class OperationInterceptor {

  /**
   * Called before each attempt.
   * @param callable the callable about to be executed
   * @throws IOException the implementer may throw if they find issue with the request
   */
  public abstract void beforeAttempt(RetryingCallable<?> callable) throws IOException;

  /**
   * Called after successful attempt completion.
   * @param callable the callable that was executed
   * @param result   the result returned by the attempt (may be null)
   * @throws IOException the implementer may throw if they find issue with the result
   */
  public abstract void afterAttemptSuccess(RetryingCallable<?> callable, Object result)
    throws IOException;

  /**
   * Called after attempt failure, before retry logic.
   * @param callable the callable that failed
   * @param cause    the exception that caused the failure
   * @throws IOException the implementer may throw an alternative exception
   */
  public abstract void afterAttemptFailure(RetryingCallable<?> callable, Throwable cause)
    throws IOException;

  /**
   * Called at the end of the operation, regardless of success or failure. This is guaranteed to be
   * called exactly once per operation.
   */
  public abstract void afterOperation();
}
