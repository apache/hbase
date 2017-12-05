/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Coprocessor environment state.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface CoprocessorEnvironment<C extends Coprocessor> {

  /** @return the Coprocessor interface version */
  int getVersion();

  /** @return the HBase version as a string (e.g. "0.21.0") */
  String getHBaseVersion();

  /** @return the loaded coprocessor instance */
  C getInstance();

  /** @return the priority assigned to the loaded coprocessor */
  int getPriority();

  /** @return the load sequence number */
  int getLoadSequence();

  /**
   * @return a Read-only Configuration; throws {@link UnsupportedOperationException} if you try
   *   to set a configuration.
   */
  Configuration getConfiguration();

  /**
   * @return the classloader for the loaded coprocessor instance
   */
  ClassLoader getClassLoader();
}
