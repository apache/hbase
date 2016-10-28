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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * Get instance of configured Registry.
 */
@InterfaceAudience.Private
final class ClusterRegistryFactory {

  static final String REGISTRY_IMPL_CONF_KEY = "hbase.client.registry.impl";

  private ClusterRegistryFactory() {
  }

  /**
   * @return The cluster registry implementation to use.
   */
  static AsyncRegistry getRegistry(Configuration conf) {
    Class<? extends AsyncRegistry> clazz =
        conf.getClass(REGISTRY_IMPL_CONF_KEY, ZKAsyncRegistry.class, AsyncRegistry.class);
    return ReflectionUtils.newInstance(clazz, conf);
  }
}