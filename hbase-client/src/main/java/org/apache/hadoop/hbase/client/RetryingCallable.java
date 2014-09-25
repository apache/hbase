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

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A Callable<T> that will be retried.  If {@link #call()} invocation throws exceptions,
 * we will call {@link #throwable(Throwable, boolean)} with whatever the exception was.
 * @param <T>
 */
@InterfaceAudience.Private
public interface RetryingCallable<T> extends Callable<T> {
  /**
   * Prepare by setting up any connections to servers, etc., ahead of {@link #call()} invocation.
   * @param reload Set this to true if need to requery locations (usually set on second invocation
   * to {@link #call()} or whatever
   * @throws IOException e
   */
  void prepare(final boolean reload) throws IOException;

  /**
   * Called when {@link #call()} throws an exception and we are going to retry; take action to
   * make it so we succeed on next call (clear caches, do relookup of locations, etc.).
   * @param t
   * @param retrying True if we are in retrying mode (we are not in retrying mode when max
   * retries == 1; we ARE in retrying mode if retries > 1 even when we are the last attempt)
   */
  void throwable(final Throwable t, boolean retrying);

  /**
   * @return Some details from the implementation that we would like to add to a terminating
   * exception; i.e. a fatal exception is being thrown ending retries and we might like to add
   * more implementation-specific detail on to the exception being thrown.
   */
  String getExceptionMessageAdditionalDetail();

  /**
   * @param pause
   * @param tries
   * @return Suggestion on how much to sleep between retries
   */
  long sleep(final long pause, final int tries);
}
