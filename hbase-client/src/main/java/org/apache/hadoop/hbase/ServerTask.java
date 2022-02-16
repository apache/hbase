/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import org.apache.yetus.audience.InterfaceAudience;

/** Information about active monitored server tasks */
@InterfaceAudience.Public
public interface ServerTask {

  /** Task state */
  enum State {
    RUNNING,
    WAITING,
    COMPLETE,
    ABORTED;
  }

  /**
   * Get the task's description.
   * @return the task's description, typically a name
   */
  String getDescription();

  /**
   * Get the current status of the task.
   * @return the task's current status
   */
  String getStatus();

  /**
   * Get the current state of the task.
   * @return the task's current state
   */
  State getState();

  /**
   * Get the task start time.
   * @return the time when the task started, or 0 if it has not started yet
   */
  long getStartTime();

  /**
   * Get the task completion time.
   * @return the time when the task completed, or 0 if it has not completed yet
   */
  long getCompletionTime();

}
