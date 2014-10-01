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
package org.apache.hadoop.hbase.conf;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * This extension to ConfigurationObserver allows the configuration to propagate to the children of
 * the current {@link ConfigurationObserver}. This is the preferred way to make a class online
 * configurable because it allows the user to configure the children in a recursive manner
 * automatically. 
 */
@InterfaceAudience.Private
public interface PropagatingConfigurationObserver extends ConfigurationObserver {

  /**
   * Needs to be called to register the children to the manager. 
   * @param manager : to register to
   */
  void registerChildren(ConfigurationManager manager);

  /**
   * Needs to be called to deregister the children from the manager. 
   * @param manager : to deregister from
   */
  void deregisterChildren(ConfigurationManager manager);
}
