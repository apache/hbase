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
 * For creating {@link AsyncBufferedMutator}.
 */
@InterfaceAudience.Public
public interface AsyncBufferedMutatorBuilder {

  /**
   * Set timeout for the background flush operation.
   */
  AsyncBufferedMutatorBuilder setOperationTimeout(long timeout, TimeUnit unit);

  /**
   * Set timeout for each rpc request when doing background flush.
   */
  AsyncBufferedMutatorBuilder setRpcTimeout(long timeout, TimeUnit unit);

  /**
   * Set the base pause time for retrying. We use an exponential policy to generate sleep time when
   * retrying.
   */
  AsyncBufferedMutatorBuilder setRetryPause(long pause, TimeUnit unit);

  /**
   * Set the periodical flush interval. If the data in the buffer has not been flush for a long
   * time, i.e, reach this timeout limit, we will flush it automatically.
   * <p/>
   * Notice that, set the timeout to 0 or a negative value means disable periodical flush, not
   * 'flush immediately'. If you want to flush immediately then you should not use this class, as it
   * is designed to be 'buffered'.
   */
  default AsyncBufferedMutatorBuilder setWriteBufferPeriodicFlush(long timeout, TimeUnit unit) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Disable the periodical flush, i.e, set the timeout to 0.
   */
  default AsyncBufferedMutatorBuilder disableWriteBufferPeriodicFlush() {
    return setWriteBufferPeriodicFlush(0, TimeUnit.NANOSECONDS);
  }

  /**
   * Set the max retry times for an operation. Usually it is the max attempt times minus 1.
   * <p>
   * Operation timeout and max attempt times(or max retry times) are both limitations for retrying,
   * we will stop retrying when we reach any of the limitations.
   * @see #setMaxAttempts(int)
   * @see #setOperationTimeout(long, TimeUnit)
   */
  default AsyncBufferedMutatorBuilder setMaxRetries(int maxRetries) {
    return setMaxAttempts(retries2Attempts(maxRetries));
  }

  /**
   * Set the max attempt times for an operation. Usually it is the max retry times plus 1. Operation
   * timeout and max attempt times(or max retry times) are both limitations for retrying, we will
   * stop retrying when we reach any of the limitations.
   * @see #setMaxRetries(int)
   * @see #setOperationTimeout(long, TimeUnit)
   */
  AsyncBufferedMutatorBuilder setMaxAttempts(int maxAttempts);

  /**
   * Set the number of retries that are allowed before we start to log.
   */
  AsyncBufferedMutatorBuilder setStartLogErrorsCnt(int startLogErrorsCnt);

  /**
   * Override the write buffer size specified by the provided {@link AsyncConnection}'s
   * {@link org.apache.hadoop.conf.Configuration} instance, via the configuration key
   * {@code hbase.client.write.buffer}.
   */
  AsyncBufferedMutatorBuilder setWriteBufferSize(long writeBufferSize);

  /**
   * Override the maximum key-value size specified by the provided {@link AsyncConnection}'s
   * {@link org.apache.hadoop.conf.Configuration} instance, via the configuration key
   * {@code hbase.client.keyvalue.maxsize}.
   */
  AsyncBufferedMutatorBuilder setMaxKeyValueSize(int maxKeyValueSize);

  /**
   * Create the {@link AsyncBufferedMutator} instance.
   */
  AsyncBufferedMutator build();
}
