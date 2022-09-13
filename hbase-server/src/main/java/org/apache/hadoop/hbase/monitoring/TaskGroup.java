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

@InterfaceAudience.Private
public class TaskGroup extends MonitoredTaskImpl {
  private static final Logger LOG = LoggerFactory.getLogger(TaskGroup.class);

  private final ConcurrentLinkedDeque<MonitoredTask> tasks = new ConcurrentLinkedDeque<>();
  private final boolean ignoreClearStatus;

  public TaskGroup(boolean ignoreClearStatus) {
    super(false);
    this.ignoreClearStatus = ignoreClearStatus;
  }

  public TaskGroup() {
    this(false);
  }

  public static TaskGroup createTaskGroup(boolean ignoreClearStatus) {
    return new TaskGroup(ignoreClearStatus);
  }

  public synchronized MonitoredTask addTask(String description) {
    return addTask(description, true);
  }

  public synchronized MonitoredTask addTask(String description, boolean withCompleteLast) {
    if (withCompleteLast) {
      MonitoredTask previousTask = this.tasks.peekLast();
      if (previousTask != null) {
        previousTask.markComplete("Completed");
      }
    }
    MonitoredTask task = TaskMonitor.get().createStatus(description, ignoreClearStatus, true);
    this.setStatus(description);
    this.tasks.addLast(task);
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
      task.abort(msg);
    }
  }

  @Override
  public synchronized void markComplete(String msg) {
    setState(State.COMPLETE);
    setStatus(msg);
    if (tasks.getLast() != null) {
      tasks.getLast().markComplete(msg);
    }
  }

  @Override
  public synchronized void cleanup() {
    this.tasks.clear();
  }
}
