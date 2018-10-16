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

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.PreemptiveFastFailException;

/**
 * Class that acts as a NoOpInterceptor. This class is used in case the
 * RetryingCallerInterceptor was not configured correctly or an
 * RetryingCallerInterceptor was never configured in the first place.
 *
 */
@InterfaceAudience.Private
class NoOpRetryableCallerInterceptor extends RetryingCallerInterceptor {
  private static final RetryingCallerInterceptorContext NO_OP_CONTEXT =
      new NoOpRetryingInterceptorContext();

  public NoOpRetryableCallerInterceptor() {
  }

  public NoOpRetryableCallerInterceptor(Configuration conf) {
    super();
  }

  @Override
  public void intercept(
      RetryingCallerInterceptorContext abstractRetryingCallerInterceptorContext)
      throws PreemptiveFastFailException {
  }

  @Override
  public void handleFailure(RetryingCallerInterceptorContext context,
      Throwable t) throws IOException {
  }

  @Override
  public void updateFailureInfo(RetryingCallerInterceptorContext context) {
  }

  @Override
  public RetryingCallerInterceptorContext createEmptyContext() {
    return NO_OP_CONTEXT;
  }

  @Override
  public String toString() {
    return "NoOpRetryableCallerInterceptor";
  }
}
