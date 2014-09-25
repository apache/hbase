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
package org.apache.hadoop.hbase.monitoring;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.Private
public interface MonitoredTask extends Cloneable {
  enum State {
    RUNNING,
    WAITING,
    COMPLETE,
    ABORTED;
  }

  long getStartTime();
  String getDescription();
  String getStatus();
  long getStatusTime();
  State getState();
  long getStateTime();
  long getCompletionTimestamp();

  void markComplete(String msg);
  void pause(String msg);
  void resume(String msg);
  void abort(String msg);
  void expireNow();

  void setStatus(String status);
  void setDescription(String description);

  /**
   * Explicitly mark this status as able to be cleaned up,
   * even though it might not be complete.
   */
  void cleanup();

  /**
   * Public exposure of Object.clone() in order to allow clients to easily 
   * capture current state.
   * @return a copy of the object whose references will not change
   */
  MonitoredTask clone();

  /**
   * Creates a string map of internal details for extensible exposure of 
   * monitored tasks.
   * @return A Map containing information for this task.
   */
  Map<String, Object> toMap() throws IOException;

  /**
   * Creates a JSON object for parseable exposure of monitored tasks.
   * @return An encoded JSON object containing information for this task.
   */
  String toJSON() throws IOException;

}
