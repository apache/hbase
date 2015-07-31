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

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Coprocessor interface.
 */
@InterfaceAudience.Private
public interface Coprocessor {
  int VERSION = 1;

  /** Highest installation priority */
  int PRIORITY_HIGHEST = 0;
  /** High (system) installation priority */
  int PRIORITY_SYSTEM = Integer.MAX_VALUE / 4;
  /** Default installation priority for user coprocessors */
  int PRIORITY_USER = Integer.MAX_VALUE / 2;
  /** Lowest installation priority */
  int PRIORITY_LOWEST = Integer.MAX_VALUE;

  /**
   * Lifecycle state of a given coprocessor instance.
   */
  enum State {
    UNINSTALLED,
    INSTALLED,
    STARTING,
    ACTIVE,
    STOPPING,
    STOPPED
  }

  // Interface
  void start(CoprocessorEnvironment env) throws IOException;

  void stop(CoprocessorEnvironment env) throws IOException;

}
