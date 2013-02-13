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

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionVisitor;
import org.apache.hadoop.hbase.server.errorhandling.FaultInjector;

/**
 * Generic error dispatcher factory that just creates an error dispatcher on request (potentially
 * wrapping with an error injector via the {@link ExceptionOrchestratorFactory}).
 * @param <T> Type of generic error listener the dispatchers should handle
 * @see ExceptionOrchestratorFactory
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ExceptionDispatcherFactory<T> extends
    ExceptionOrchestratorFactory<ExceptionDispatcher<T, Exception>, T> {

  /**
   * @param visitor to use when building an error handler via {@link #createErrorHandler()}.
   */
  public ExceptionDispatcherFactory(ExceptionVisitor<T> visitor) {
    super(visitor);
  }

  @Override
  protected ExceptionDispatcher<T, Exception> buildErrorHandler(ExceptionVisitor<T> visitor) {
    return new ExceptionDispatcher<T, Exception>(visitor);
  }

  @Override
  protected ExceptionDispatcher<T, Exception> wrapWithInjector(
      ExceptionDispatcher<T, Exception> dispatcher,
      List<FaultInjector<?>> injectors) {
    return new InjectingExceptionDispatcher<ExceptionDispatcher<T, Exception>, T, Exception>(dispatcher,
        injectors);
  }
}