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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

import java.io.IOException;

/**
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface RpcRetryingCaller<T> {
  void cancel();

  /**
   * Retries if invocation fails.
   * @param callTimeout Timeout for this call
   * @param callable The {@link RetryingCallable} to run.
   * @return an object of type T
   * @throws IOException if a remote or network exception occurs
   * @throws RuntimeException other unspecified error
   */
  T callWithRetries(RetryingCallable<T> callable, int callTimeout)
  throws IOException, RuntimeException;

  /**
   * Call the server once only.
   * {@link RetryingCallable} has a strange shape so we can do retrys.  Use this invocation if you
   * want to do a single call only (A call to {@link RetryingCallable#call(int)} will not likely
   * succeed).
   * @return an object of type T
   * @throws IOException if a remote or network exception occurs
   * @throws RuntimeException other unspecified error
   */
  T callWithoutRetries(RetryingCallable<T> callable, int callTimeout)
  throws IOException, RuntimeException;
}
