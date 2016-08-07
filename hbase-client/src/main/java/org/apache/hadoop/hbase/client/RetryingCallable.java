/**
 *
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

/**
 * A Callable&lt;T&gt; that will be retried.  If {@link #call(int)} invocation throws exceptions,
 * we will call {@link #throwable(Throwable, boolean)} with whatever the exception was.
 * @param <T> result class from executing <tt>this</tt>
 */
@InterfaceAudience.Private
public interface RetryingCallable<T> extends RetryingCallableBase {
  /**
   * Computes a result, or throws an exception if unable to do so.
   *
   * @param callTimeout - the time available for this call. 0 for infinite.
   * @return computed result
   * @throws Exception if unable to compute a result
   */
  T call(int callTimeout) throws Exception;
}