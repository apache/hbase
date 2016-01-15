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

import java.lang.reflect.Constructor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Factory implementation to provide the {@link HConnectionImplementation} with
 * the implementation of the {@link RetryingCallerInterceptor} that we would use
 * to intercept the {@link RpcRetryingCaller} during the course of their calls.
 * 
 */

@InterfaceAudience.Private
class RetryingCallerInterceptorFactory {
  private static final Log LOG = LogFactory
      .getLog(RetryingCallerInterceptorFactory.class);
  private Configuration conf;
  private final boolean failFast;
  public static final RetryingCallerInterceptor NO_OP_INTERCEPTOR =
      new NoOpRetryableCallerInterceptor(null);

  public RetryingCallerInterceptorFactory(Configuration conf) {
    this.conf = conf;
    failFast = conf.getBoolean(HConstants.HBASE_CLIENT_FAST_FAIL_MODE_ENABLED,
        HConstants.HBASE_CLIENT_ENABLE_FAST_FAIL_MODE_DEFAULT);
  }

  /**
   * This builds the implementation of {@link RetryingCallerInterceptor} that we
   * specify in the conf and returns the same.
   * 
   * To use {@link PreemptiveFastFailInterceptor}, set HBASE_CLIENT_ENABLE_FAST_FAIL_MODE to true.
   * HBASE_CLIENT_FAST_FAIL_INTERCEPTOR_IMPL is defaulted to {@link PreemptiveFastFailInterceptor}
   * 
   * @return The factory build method which creates the
   *         {@link RetryingCallerInterceptor} object according to the
   *         configuration.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="REC_CATCH_EXCEPTION",
      justification="Convert thrown exception to unchecked")
  public RetryingCallerInterceptor build() {
    RetryingCallerInterceptor ret = NO_OP_INTERCEPTOR;
    if (failFast) {
      try {
        Class<?> c = conf.getClass(
            HConstants.HBASE_CLIENT_FAST_FAIL_INTERCEPTOR_IMPL,
            PreemptiveFastFailInterceptor.class);
        Constructor<?> constructor = c
            .getDeclaredConstructor(Configuration.class);
        constructor.setAccessible(true);
        ret = (RetryingCallerInterceptor) constructor.newInstance(conf);
      } catch (Exception e) {
        ret = new PreemptiveFastFailInterceptor(conf);
      }
    }
    LOG.trace("Using " + ret.toString() + " for intercepting the RpcRetryingCaller");
    return ret;
  }
}
