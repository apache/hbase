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
package org.apache.hadoop.hbase.client;

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * This class is designed to fit into the RetryingCaller class which forms the
 * central piece of intelligence for the client side retries for most calls.
 * 
 * One can extend this class and intercept the RetryingCaller and add additional
 * logic into the execution of a simple HTable operations like get, delete etc.
 * 
 * Concrete implementations of this calls are supposed to the thread safe. The
 * object is used across threads to identify the fast failing threads.
 * 
 * For a concrete use case see {@link PreemptiveFastFailInterceptor}
 * 
 * Example use case : 
 * try {
 *   interceptor.intercept
 *   doAction()
 * } catch (Exception e) {
 *   interceptor.handleFailure
 * } finally {
 *   interceptor.updateFaulireInfo
 * }
 * 
 * The {@link RetryingCallerInterceptor} also acts as a factory
 * for getting a new {@link RetryingCallerInterceptorContext}.
 * 
 */

@InterfaceAudience.Private
abstract class RetryingCallerInterceptor {

  protected RetryingCallerInterceptor() {
    // Empty constructor protected for NoOpRetryableCallerInterceptor
  }

  /**
   * This returns the context object for the current call.
   * 
   * @return context : the context that needs to be used during this call.
   */
  public abstract RetryingCallerInterceptorContext createEmptyContext();

  /**
   * Call this function in case we caught a failure during retries.
   * 
   * @param context
   *          : The context object that we obtained previously.
   * @param t
   *          : The exception that we caught in this particular try
   * @throws IOException
   */
  public abstract void handleFailure(RetryingCallerInterceptorContext context,
      Throwable t) throws IOException;

  /**
   * Call this function alongside the actual call done on the callable.
   * 
   * @param abstractRetryingCallerInterceptorContext
   * @throws PreemptiveFastFailException
   */
  public abstract void intercept(
      RetryingCallerInterceptorContext abstractRetryingCallerInterceptorContext)
      throws IOException;

  /**
   * Call this function to update at the end of the retry. This is not necessary
   * to happen.
   * 
   * @param context
   */
  public abstract void updateFailureInfo(
      RetryingCallerInterceptorContext context);

  @Override
  public abstract String toString();
}
