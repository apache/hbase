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

import static org.apache.hadoop.hbase.client.ConnectionUtils.retries2Attempts;

import java.util.concurrent.TimeUnit;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * For creating {@link AsyncTable}.
 * <p>
 * The implementation should have default configurations set before returning the builder to user.
 * So users are free to only set the configs they care about to create a new
 * AsyncTable/RawAsyncTable instance.
 * @since 2.0.0
 */
@InterfaceAudience.Public
public interface AsyncTableBuilder<C extends ScanResultConsumerBase> {

  /**
   * Set timeout for a whole operation such as get, put or delete. Notice that scan will not be
   * effected by this value, see scanTimeoutNs.
   * <p>
   * Operation timeout and max attempt times(or max retry times) are both limitations for retrying,
   * we will stop retrying when we reach any of the limitations.
   * @see #setMaxAttempts(int)
   * @see #setMaxRetries(int)
   * @see #setScanTimeout(long, TimeUnit)
   */
  AsyncTableBuilder<C> setOperationTimeout(long timeout, TimeUnit unit);

  /**
   * As now we have heartbeat support for scan, ideally a scan will never timeout unless the RS is
   * crash. The RS will always return something before the rpc timed out or scan timed out to tell
   * the client that it is still alive. The scan timeout is used as operation timeout for every
   * operation in a scan, such as openScanner or next.
   * @see #setScanTimeout(long, TimeUnit)
   */
  AsyncTableBuilder<C> setScanTimeout(long timeout, TimeUnit unit);

  /**
   * Set timeout for each rpc request.
   * <p>
   * Notice that this will <strong>NOT</strong> change the rpc timeout for read(get, scan) request
   * and write request(put, delete).
   */
  AsyncTableBuilder<C> setRpcTimeout(long timeout, TimeUnit unit);

  /**
   * Set timeout for each read(get, scan) rpc request.
   */
  AsyncTableBuilder<C> setReadRpcTimeout(long timeout, TimeUnit unit);

  /**
   * Set timeout for each write(put, delete) rpc request.
   */
  AsyncTableBuilder<C> setWriteRpcTimeout(long timeout, TimeUnit unit);

  /**
   * Set the base pause time for retrying. We use an exponential policy to generate sleep time when
   * retrying.
   * @see #setRetryPauseForCQTBE(long, TimeUnit)
   */
  AsyncTableBuilder<C> setRetryPause(long pause, TimeUnit unit);

  /**
   * Set the base pause time for retrying when we hit {@code CallQueueTooBigException}. We use an
   * exponential policy to generate sleep time when retrying.
   * <p/>
   * This value should be greater than the normal pause value which could be set with the above
   * {@link #setRetryPause(long, TimeUnit)} method, as usually {@code CallQueueTooBigException}
   * means the server is overloaded. We just use the normal pause value for
   * {@code CallQueueTooBigException} if here you specify a smaller value.
   * @see #setRetryPause(long, TimeUnit)
   */
  AsyncTableBuilder<C> setRetryPauseForCQTBE(long pause, TimeUnit unit);

  /**
   * Set the max retry times for an operation. Usually it is the max attempt times minus 1.
   * <p>
   * Operation timeout and max attempt times(or max retry times) are both limitations for retrying,
   * we will stop retrying when we reach any of the limitations.
   * @see #setMaxAttempts(int)
   * @see #setOperationTimeout(long, TimeUnit)
   */
  default AsyncTableBuilder<C> setMaxRetries(int maxRetries) {
    return setMaxAttempts(retries2Attempts(maxRetries));
  }

  /**
   * Set the max attempt times for an operation. Usually it is the max retry times plus 1. Operation
   * timeout and max attempt times(or max retry times) are both limitations for retrying, we will
   * stop retrying when we reach any of the limitations.
   * @see #setMaxRetries(int)
   * @see #setOperationTimeout(long, TimeUnit)
   */
  AsyncTableBuilder<C> setMaxAttempts(int maxAttempts);

  /**
   * Set the number of retries that are allowed before we start to log.
   */
  AsyncTableBuilder<C> setStartLogErrorsCnt(int startLogErrorsCnt);

  /**
   * Create the {@link AsyncTable} instance.
   */
  AsyncTable<C> build();
}
