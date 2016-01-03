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
package org.apache.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Simple Service.
 */
// This is a WIP. We have Services throughout hbase. Either have all implement what is here or
// just remove this as an experiment that did not work out.
// TODO: Move on to guava Service after we update our guava version; later guava has nicer
// Service implmentation.
// TODO: Move all Services on to this one Interface.
@InterfaceAudience.Private
public interface Service {
  /**
   * Initiates service startup (if necessary), returning once the service has finished starting.
   * @throws IOException Throws exception if already running and if we fail to start successfully.
   */
  void startAndWait() throws IOException;

  /**
   * @return True if this Service is running.
   */
  boolean isRunning();

  /**
   * Initiates service shutdown (if necessary), returning once the service has finished stopping.
   * @throws IOException Throws exception if not running of if we fail to stop successfully.
   */
  void stopAndWait() throws IOException;
}
