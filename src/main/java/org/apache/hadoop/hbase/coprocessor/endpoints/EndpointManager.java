/**
 * Copyright 2014 The Apache Software Foundation
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
package org.apache.hadoop.hbase.coprocessor.endpoints;

import java.util.concurrent.ConcurrentHashMap;

/**
 * The manager holding all endpoint factories in the server.
 *
 */
public class EndpointManager {
  private static EndpointManager instance = new EndpointManager();

  /**
   * Returns the singleton endpoint-manager
   */
  public static EndpointManager get() {
    return instance;
  }

  private ConcurrentHashMap<String, IEndpointFactory<?>> nameFacts = new ConcurrentHashMap<>();

  /**
   * Returns the factory of an endpoint.
   */
  public IEndpointFactory<?> getFactory(String name) {
    return nameFacts.get(name);

  }

  /**
   * Register an endpoint with its factory
   */
  public <T extends IEndpoint> void register(Class<T> iEndpoint,
      IEndpointFactory<T> factory) {
    nameFacts.put(iEndpoint.getName(), factory);
  }
}
