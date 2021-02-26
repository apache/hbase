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

package org.apache.hadoop.hbase.coordination;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.SplitLogManager.ResubmitDirective;
import org.apache.hadoop.hbase.master.SplitLogManager.Task;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Coordination for SplitLogManager. It creates and works with tasks for split log operations<BR>
 * Manager prepares task by calling {@link #prepareTask} and submit it by
 * {@link #submitTask(String)}. After that it periodically check the number of remaining tasks by
 * {@link #remainingTasksInCoordination()} and waits until it become zero.
 * <P>
 * Methods required for task life circle: <BR>
 * {@link #checkTaskStillAvailable(String)} Check that task is still there <BR>
 * {@link #checkTasks()} check for unassigned tasks and resubmit them
 * @deprecated since 2.4.0 and in 3.0.0, to be removed in 4.0.0, replaced by procedure-based
 *   distributed WAL splitter, see SplitWALManager
 */
@InterfaceAudience.Private
@Deprecated
public interface SplitLogManagerCoordination {
  /**
   * Detail class that shares data between coordination and split log manager
   */
  class SplitLogManagerDetails {
    final private ConcurrentMap<String, Task> tasks;
    final private MasterServices master;
    final private Set<String> failedDeletions;

    public SplitLogManagerDetails(ConcurrentMap<String, Task> tasks, MasterServices master,
        Set<String> failedDeletions) {
      this.tasks = tasks;
      this.master = master;
      this.failedDeletions = failedDeletions;
    }

    /**
     * @return the master value
     */
    public MasterServices getMaster() {
      return master;
    }

    /**
     * @return map of tasks
     */
    public ConcurrentMap<String, Task> getTasks() {
      return tasks;
    }

    /**
     * @return a set of failed deletions
     */
    public Set<String> getFailedDeletions() {
      return failedDeletions;
    }

    /**
     * @return server name
     */
    public ServerName getServerName() {
      return master.getServerName();
    }
  }

  /**
   * Provide the configuration from the SplitLogManager
   */
  void setDetails(SplitLogManagerDetails details);

  /**
   * Returns the configuration that was provided previously
   */
  SplitLogManagerDetails getDetails();

  /**
   * Prepare the new task
   * @param taskName name of the task
   * @return the task id
   */
  String prepareTask(String taskName);

  /**
   * tells Coordination that it should check for new tasks
   */
  void checkTasks();

  /**
   * Return the number of remaining tasks
   */
  int remainingTasksInCoordination();

  /**
   * Check that the task is still there
   * @param task node to check
   */
  void checkTaskStillAvailable(String task);

  /**
   * Resubmit the task in case if found unassigned or failed
   * @param taskName path related to task
   * @param task to resubmit
   * @param force whether it should be forced
   * @return whether it was successful
   */

  boolean resubmitTask(String taskName, Task task, ResubmitDirective force);

  /**
   * @param taskName to be submitted
   */
  void submitTask(String taskName);

  /**
   * @param taskName to be removed
   */
  void deleteTask(String taskName);

  /**
   * Support method to init constants such as timeout. Mostly required for UTs.
   * @throws IOException
   */
  void init() throws IOException;
}
