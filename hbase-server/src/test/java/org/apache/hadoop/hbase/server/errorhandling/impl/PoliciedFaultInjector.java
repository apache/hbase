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
package org.apache.hadoop.hbase.server.errorhandling.impl;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.server.errorhandling.FaultInjector;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Fault injector that can take a policy for when to inject a fault
 * @param <E> type of exception that should be returned
 */
public abstract class PoliciedFaultInjector<E extends Exception> implements FaultInjector<E> {

  private static final Log LOG = LogFactory.getLog(PoliciedFaultInjector.class);
  private FaultInjectionPolicy policy;

  public PoliciedFaultInjector(FaultInjectionPolicy policy) {
    this.policy = policy;
  }

  @Override
  public final Pair<E, Object[]> injectFault(StackTraceElement[] trace) {

    if (policy.shouldFault(trace)) {
      return this.getInjectedError(trace);
    }
    LOG.debug("NOT injecting fault, stack:" + Arrays.toString(Arrays.copyOfRange(trace, 3, 6)));
    return null;
  }

  /**
   * Get the error that should be returned to the caller when the {@link FaultInjectionPolicy}
   * determines we need to inject a fault
   * @param trace trace for which the {@link FaultInjectionPolicy} specified we should have an error
   * @return the information about the fault that should be returned if there was a fault, null
   *         otherwise
   */
  protected abstract Pair<E, Object[]> getInjectedError(StackTraceElement[] trace);
}