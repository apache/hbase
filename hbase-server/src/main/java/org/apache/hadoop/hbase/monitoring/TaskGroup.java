/*
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

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link TaskGroup} can be seen as a big {@link MonitoredTask}, which contains a list of sub
 * monitored tasks. The monitored tasks in the group are still be managed by the
 * {@link TaskMonitor}, but whether to clear/expire the monitored tasks in a task group is optional.
 * Since the monitored task already has journals, which mark the phases in a task, we still also
 * need a task group to monitor a big task/process because the journals in a task is serial but the
 * tasks in the task group can be parallel, then we have more flexible ability to monitor the
 * process. Grouping the tasks is not strictly necessary but it is cleaner for presentation to
 * operators. We might want to display the tasks in a group in a list view where each task can be
 * collapsed (probably by default) or expanded.
 */
@InterfaceAudience.Private
public class TaskGroup extends MonitoredTaskImpl {
  private static final Logger LOG = LoggerFactory.getLogger(TaskGroup.class);

  /** Sub-tasks in the group */
  private final ConcurrentLinkedDeque<MonitoredTask> tasks = new ConcurrentLinkedDeque<>();

  /** Whether to ignore to track(e.g. show/clear/expire) in the singleton {@link TaskMonitor} */
  private boolean ignoreSubTasksInTaskMonitor;

  /** Used to track this task group in {@link TaskMonitor} */
  private final MonitoredTask delegate;

  public TaskGroup(boolean ignoreSubTasksInTaskMonitor, String description) {
    super(true, description);
    this.ignoreSubTasksInTaskMonitor = ignoreSubTasksInTaskMonitor;
    this.delegate = TaskMonitor.get().createStatus(description, false, true);
  }

  public synchronized MonitoredTask addTask(String description) {
    return addTask(description, true);
  }

  /**
   * Add a new task to the group, and before that might complete the last task in the group
   * @param description      the description of the new task
   * @param withCompleteLast whether to complete the last task in the group
   * @return the added new task
   */
  public synchronized MonitoredTask addTask(String description, boolean withCompleteLast) {
    if (withCompleteLast) {
      MonitoredTask previousTask = this.tasks.peekLast();
      if (
        previousTask != null && previousTask.getState() != State.COMPLETE
          && previousTask.getState() != State.ABORTED
      ) {
        previousTask.markComplete("Completed");
      }
    }
    MonitoredTask task =
      TaskMonitor.get().createStatus(description, ignoreSubTasksInTaskMonitor, true);
    this.setStatus(description);
    this.tasks.addLast(task);
    delegate.setStatus(description);
    return task;
  }

  public synchronized Collection<MonitoredTask> getTasks() {
    return Collections.unmodifiableCollection(this.tasks);
  }

  @Override
  public synchronized void abort(String msg) {
    setStatus(msg);
    setState(State.ABORTED);
    for (MonitoredTask task : tasks) {
      if (task.getState() != State.COMPLETE && task.getState() != State.ABORTED) {
        task.abort(msg);
      }
    }
    delegate.abort(msg);
  }

  @Override
  public synchronized void markComplete(String msg) {
    setState(State.COMPLETE);
    setStatus(msg);
    if (tasks.getLast() != null) {
      tasks.getLast().markComplete(msg);
    }
    delegate.markComplete(msg);
  }

  @Override
  public synchronized void cleanup() {
    this.tasks.clear();
  }
}
