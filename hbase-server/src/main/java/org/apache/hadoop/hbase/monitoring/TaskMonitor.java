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

import java.io.PrintWriter;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;

import com.google.common.collect.Lists;

/**
 * Singleton which keeps track of tasks going on in this VM.
 * A Task here is anything which takes more than a few seconds
 * and the user might want to inquire about the status
 */
@InterfaceAudience.Private
public class TaskMonitor {
  private static final Log LOG = LogFactory.getLog(TaskMonitor.class);

  public static final String MAX_TASKS_KEY = "hbase.taskmonitor.max.tasks";
  public static final int DEFAULT_MAX_TASKS = 1000;
  public static final String RPC_WARN_TIME_KEY = "hbase.taskmonitor.rpc.warn.time";
  public static final long DEFAULT_RPC_WARN_TIME = 0;
  public static final String EXPIRATION_TIME_KEY = "hbase.taskmonitor.expiration.time";
  public static final long DEFAULT_EXPIRATION_TIME = 60*1000;
  public static final String MONITOR_INTERVAL_KEY = "hbase.taskmonitor.monitor.interval";
  public static final long DEFAULT_MONITOR_INTERVAL = 10*1000;

  private static TaskMonitor instance;

  private final int maxTasks;
  private final long rpcWarnTime;
  private final long expirationTime;
  private final CircularFifoBuffer tasks;
  private final List<TaskAndWeakRefPair> rpcTasks;
  private final long monitorInterval;
  private Thread monitorThread;

  TaskMonitor(Configuration conf) {
    maxTasks = conf.getInt(MAX_TASKS_KEY, DEFAULT_MAX_TASKS);
    expirationTime = conf.getLong(EXPIRATION_TIME_KEY, DEFAULT_EXPIRATION_TIME);
    rpcWarnTime = conf.getLong(RPC_WARN_TIME_KEY, DEFAULT_RPC_WARN_TIME);
    tasks = new CircularFifoBuffer(maxTasks);
    rpcTasks = Lists.newArrayList();
    monitorInterval = conf.getLong(MONITOR_INTERVAL_KEY, DEFAULT_MONITOR_INTERVAL);
    monitorThread = new Thread(new MonitorRunnable());
    Threads.setDaemonThreadRunning(monitorThread, "Monitor thread for TaskMonitor");
  }

  /**
   * Get singleton instance.
   * TODO this would be better off scoped to a single daemon
   */
  public static synchronized TaskMonitor get() {
    if (instance == null) {
      instance = new TaskMonitor(HBaseConfiguration.create());
    }
    return instance;
  }
  
  public synchronized MonitoredTask createStatus(String description) {
    MonitoredTask stat = new MonitoredTaskImpl();
    stat.setDescription(description);
    MonitoredTask proxy = (MonitoredTask) Proxy.newProxyInstance(
        stat.getClass().getClassLoader(),
        new Class<?>[] { MonitoredTask.class },
        new PassthroughInvocationHandler<MonitoredTask>(stat));
    TaskAndWeakRefPair pair = new TaskAndWeakRefPair(stat, proxy);
    if (tasks.isFull()) {
      purgeExpiredTasks();
    }
    tasks.add(pair);
    return proxy;
  }

  public synchronized MonitoredRPCHandler createRPCStatus(String description) {
    MonitoredRPCHandler stat = new MonitoredRPCHandlerImpl();
    stat.setDescription(description);
    MonitoredRPCHandler proxy = (MonitoredRPCHandler) Proxy.newProxyInstance(
        stat.getClass().getClassLoader(),
        new Class<?>[] { MonitoredRPCHandler.class },
        new PassthroughInvocationHandler<MonitoredRPCHandler>(stat));
    TaskAndWeakRefPair pair = new TaskAndWeakRefPair(stat, proxy);
    rpcTasks.add(pair);
    return proxy;
  }

  private synchronized void warnStuckTasks() {
    if (rpcWarnTime > 0) {
      final long now = EnvironmentEdgeManager.currentTime();
      for (Iterator<TaskAndWeakRefPair> it = rpcTasks.iterator();
          it.hasNext();) {
        TaskAndWeakRefPair pair = it.next();
        MonitoredTask stat = pair.get();
        if ((stat.getState() == MonitoredTaskImpl.State.RUNNING) &&
            (now >= stat.getWarnTime() + rpcWarnTime)) {
          LOG.warn("Task may be stuck: " + stat);
          stat.setWarnTime(now);
        }
      }
    }
  }

  private synchronized void purgeExpiredTasks() {
    for (Iterator<TaskAndWeakRefPair> it = tasks.iterator();
         it.hasNext();) {
      TaskAndWeakRefPair pair = it.next();
      MonitoredTask stat = pair.get();
      
      if (pair.isDead()) {
        // The class who constructed this leaked it. So we can
        // assume it's done.
        if (stat.getState() == MonitoredTaskImpl.State.RUNNING) {
          LOG.warn("Status " + stat + " appears to have been leaked");
          stat.cleanup();
        }
      }
      
      if (canPurge(stat)) {
        it.remove();
      }
    }
  }

  /**
   * Produces a list containing copies of the current state of all non-expired 
   * MonitoredTasks handled by this TaskMonitor.
   * @return A complete list of MonitoredTasks.
   */
  public synchronized List<MonitoredTask> getTasks() {
    purgeExpiredTasks();
    ArrayList<MonitoredTask> ret = Lists.newArrayListWithCapacity(tasks.size() + rpcTasks
        .size());
    for (Iterator<TaskAndWeakRefPair> it = tasks.iterator();
         it.hasNext();) {
      TaskAndWeakRefPair pair = it.next();
      MonitoredTask t = pair.get();
      ret.add(t.clone());
    }
    for (Iterator<TaskAndWeakRefPair> it = rpcTasks.iterator();
         it.hasNext();) {
      TaskAndWeakRefPair pair = it.next();
      MonitoredTask t = pair.get();
      ret.add(t.clone());
    }
    return ret;
  }

  private boolean canPurge(MonitoredTask stat) {
    long cts = stat.getCompletionTimestamp();
    return (cts > 0 && EnvironmentEdgeManager.currentTime() - cts > expirationTime);
  }

  public void dumpAsText(PrintWriter out) {
    long now = EnvironmentEdgeManager.currentTime();
    
    List<MonitoredTask> tasks = getTasks();
    for (MonitoredTask task : tasks) {
      out.println("Task: " + task.getDescription());
      out.println("Status: " + task.getState() + ":" + task.getStatus());
      long running = (now - task.getStartTime())/1000;
      if (task.getCompletionTimestamp() != -1) {
        long completed = (now - task.getCompletionTimestamp()) / 1000;
        out.println("Completed " + completed + "s ago");
        out.println("Ran for " +
            (task.getCompletionTimestamp() - task.getStartTime())/1000
            + "s");
      } else {
        out.println("Running for " + running + "s");
      }
      out.println();
    }
  }

  public synchronized void shutdown() {
    if (this.monitorThread != null) {
      monitorThread.interrupt();
    }
  }

  /**
   * This class encapsulates an object as well as a weak reference to a proxy
   * that passes through calls to that object. In art form:
   * <code>
   *     Proxy  <------------------
   *       |                       \
   *       v                        \
   * PassthroughInvocationHandler   |  weak reference
   *       |                       /
   * MonitoredTaskImpl            / 
   *       |                     /
   * StatAndWeakRefProxy  ------/
   *
   * Since we only return the Proxy to the creator of the MonitorableStatus,
   * this means that they can leak that object, and we'll detect it
   * since our weak reference will go null. But, we still have the actual
   * object, so we can log it and display it as a leaked (incomplete) action.
   */
  private static class TaskAndWeakRefPair {
    private MonitoredTask impl;
    private WeakReference<MonitoredTask> weakProxy;
    
    public TaskAndWeakRefPair(MonitoredTask stat,
        MonitoredTask proxy) {
      this.impl = stat;
      this.weakProxy = new WeakReference<MonitoredTask>(proxy);
    }
    
    public MonitoredTask get() {
      return impl;
    }
    
    public boolean isDead() {
      return weakProxy.get() == null;
    }
  }
  
  /**
   * An InvocationHandler that simply passes through calls to the original 
   * object.
   */
  private static class PassthroughInvocationHandler<T> implements InvocationHandler {
    private T delegatee;
    
    public PassthroughInvocationHandler(T delegatee) {
      this.delegatee = delegatee;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      return method.invoke(delegatee, args);
    }    
  }

  private class MonitorRunnable implements Runnable {
    private boolean running = true;

    @Override
    public void run() {
      while (running) {
        try {
          Thread.sleep(monitorInterval);
          if (tasks.isFull()) {
            purgeExpiredTasks();
          }
          warnStuckTasks();
        } catch (InterruptedException e) {
          running = false;
        }
      }
    }
  }
}
