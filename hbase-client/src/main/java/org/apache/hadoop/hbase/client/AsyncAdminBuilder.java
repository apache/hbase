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

import org.apache.hadoop.hbase.HBaseServerException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * For creating {@link AsyncAdmin}. The implementation should have default configurations set before
 * returning the builder to user. So users are free to only set the configs they care about to
 * create a new AsyncAdmin instance.
 * @since 2.0.0
 */
@InterfaceAudience.Public
public interface AsyncAdminBuilder {

  /**
   * Set timeout for a whole admin operation. Operation timeout and max attempt times(or max retry
   * times) are both limitations for retrying, we will stop retrying when we reach any of the
   * limitations.
   * @return this for invocation chaining
   */
  AsyncAdminBuilder setOperationTimeout(long timeout, TimeUnit unit);

  /**
   * Set timeout for each rpc request.
   * @return this for invocation chaining
   */
  AsyncAdminBuilder setRpcTimeout(long timeout, TimeUnit unit);

  /**
   * Set the base pause time for retrying. We use an exponential policy to generate sleep time when
   * retrying.
   * @return this for invocation chaining
   * @see #setRetryPauseForServerOverloaded(long, TimeUnit)
   */
  AsyncAdminBuilder setRetryPause(long timeout, TimeUnit unit);

  /**
   * Set the base pause time for retrying when {@link HBaseServerException#isServerOverloaded()}.
   * We use an exponential policy to generate sleep time from this base when retrying.
   * <p/>
   * This value should be greater than the normal pause value which could be set with the above
   * {@link #setRetryPause(long, TimeUnit)} method, as usually
   * {@link HBaseServerException#isServerOverloaded()} means the server is overloaded. We just use
   * the normal pause value for {@link HBaseServerException#isServerOverloaded()} if here you
   * specify a smaller value.
   *
   * @see #setRetryPause(long, TimeUnit)
   * @deprecated Since 2.5.0, will be removed in 4.0.0. Please use
   *    {@link #setRetryPauseForServerOverloaded(long, TimeUnit)} instead.
   */
  @Deprecated
  default AsyncAdminBuilder setRetryPauseForCQTBE(long pause, TimeUnit unit) {
    return setRetryPauseForServerOverloaded(pause, unit);
  }

  /**
   * Set the base pause time for retrying when {@link HBaseServerException#isServerOverloaded()}.
   * We use an exponential policy to generate sleep time when retrying.
   * <p/>
   * This value should be greater than the normal pause value which could be set with the above
   * {@link #setRetryPause(long, TimeUnit)} method, as usually
   * {@link HBaseServerException#isServerOverloaded()} means the server is overloaded. We just use
   * the normal pause value for {@link HBaseServerException#isServerOverloaded()} if here you
   * specify a smaller value.
   *
   * @see #setRetryPause(long, TimeUnit)
   */
  AsyncAdminBuilder setRetryPauseForServerOverloaded(long pause, TimeUnit unit);

  /**
   * Set the max retry times for an admin operation. Usually it is the max attempt times minus 1.
   * Operation timeout and max attempt times(or max retry times) are both limitations for retrying,
   * we will stop retrying when we reach any of the limitations.
   * @return this for invocation chaining
   */
  default AsyncAdminBuilder setMaxRetries(int maxRetries) {
    return setMaxAttempts(retries2Attempts(maxRetries));
  }

  /**
   * Set the max attempt times for an admin operation. Usually it is the max retry times plus 1.
   * Operation timeout and max attempt times(or max retry times) are both limitations for retrying,
   * we will stop retrying when we reach any of the limitations.
   * @return this for invocation chaining
   */
  AsyncAdminBuilder setMaxAttempts(int maxAttempts);

  /**
   * Set the number of retries that are allowed before we start to log.
   * @return this for invocation chaining
   */
  AsyncAdminBuilder setStartLogErrorsCnt(int startLogErrorsCnt);

  /**
   * Create a {@link AsyncAdmin} instance.
   * @return a {@link AsyncAdmin} instance
   */
  AsyncAdmin build();
}
