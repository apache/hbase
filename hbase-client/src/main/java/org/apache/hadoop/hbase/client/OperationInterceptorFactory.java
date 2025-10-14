/*
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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Factory for creating per-operation interceptors. Each HBase client operation (get, put, batch,
 * etc.) will call createInterceptor() to get a fresh interceptor instance, ensuring thread safety
 * and simplicity.
 * <p>
 * Implementations should be lightweight and thread-safe, as the factory itself may be shared across
 * multiple threads. However, the interceptors created by the factory are used by only one operation
 * at a time.
 * <p>
 * Configuration is injected once during factory construction and can be used to customize
 * interceptor behavior.
 */
@InterfaceAudience.Public
public interface OperationInterceptorFactory {

  /**
   * Configuration key for specifying the OperationInterceptorFactory implementation. The specified
   * class must implement OperationInterceptorFactory and have a no-argument constructor.
   */
  String HBASE_CLIENT_OPERATION_INTERCEPTOR_IMPL = "hbase.client.operation.interceptor.impl";

  /**
   * Create a new interceptor instance for a single operation. This method may be called
   * concurrently from multiple threads.
   * @return a new OperationInterceptor instance, never null
   */
  OperationInterceptor createInterceptor();

  /**
   * A no-op factory that creates interceptors that do nothing. Used as the default when no custom
   * interceptor is configured.
   */
  OperationInterceptorFactory NO_OP = new OperationInterceptorFactory() {
    @Override
    public OperationInterceptor createInterceptor() {
      return new NoOpOperationInterceptor();
    }
  };
}
