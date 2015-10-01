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

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Get instance of configured Registry.
 */
@InterfaceAudience.Private
final class RegistryFactory {
  static final String REGISTRY_IMPL_CONF_KEY = "hbase.client.registry.impl";

  private RegistryFactory() {}

  /**
   * @return The cluster registry implementation to use.
   * @throws IOException
   */
  static Registry getRegistry(final Connection connection)
      throws IOException {
    String registryClass = connection.getConfiguration().get(REGISTRY_IMPL_CONF_KEY,
      ZooKeeperRegistry.class.getName());
    Registry registry = null;
    try {
      registry = (Registry)Class.forName(registryClass).newInstance();
    } catch (Throwable t) {
      throw new IOException(t);
    }
    registry.init(connection);
    return registry;
  }
}