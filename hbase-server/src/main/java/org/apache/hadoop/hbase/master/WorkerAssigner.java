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

package org.apache.hadoop.hbase.master;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureScheduler;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * help assign and release a worker for each remote task.
 * For each worker, concurrent running task should be no more than maxTasks.
 * If a task failed to acquire a worker, it will suspend and wait for workers available.
 */
@InterfaceAudience.Private
public class WorkerAssigner implements ServerListener {
  private final Map<ServerName, Integer> currentWorkers = new HashMap<>();
  private final MasterServices master;
  private final ProcedureEvent<?> event;
  private final int maxTasks;

  public WorkerAssigner(MasterServices master, int maxTasks, ProcedureEvent<?> event) {
    this.maxTasks = maxTasks;
    this.master = master;
    this.event = event;
    // ServerManager might be null in a test context where we are mocking; allow for this
    ServerManager sm = this.master.getServerManager();
    if (sm != null) {
      sm.registerListener(this);
    }
  }

  public synchronized Optional<ServerName> acquire() {
    List<ServerName> serverList = master.getServerManager().getOnlineServersList();
    Collections.shuffle(serverList);
    Optional<ServerName> worker = serverList.stream().filter(
      serverName -> !currentWorkers.containsKey(serverName) || currentWorkers.get(serverName) > 0)
      .findAny();
    worker.ifPresent(name -> currentWorkers.compute(name, (serverName, availableWorker) ->
      availableWorker == null ? maxTasks - 1 : availableWorker - 1));
    return worker;
  }

  public synchronized void release(ServerName serverName) {
    currentWorkers.compute(serverName, (k, v) -> v == null ? null : v + 1);
  }

  public void suspend(Procedure<?> proc) {
    event.suspend();
    event.suspendIfNotReady(proc);
  }

  public void wake(MasterProcedureScheduler scheduler) {
    if (!event.isReady()) {
      event.wake(scheduler);
    }
  }

  @Override
  public void serverAdded(ServerName worker) {
    this.wake(master.getMasterProcedureExecutor().getEnvironment().getProcedureScheduler());
  }

  public synchronized void addUsedWorker(ServerName worker) {
    // load used worker when master restart
    currentWorkers.compute(worker, (serverName,
      availableWorker) -> availableWorker == null ? maxTasks - 1 : availableWorker - 1);
  }

  public Integer getAvailableWorker(ServerName serverName) {
    return currentWorkers.get(serverName);
  }
}
