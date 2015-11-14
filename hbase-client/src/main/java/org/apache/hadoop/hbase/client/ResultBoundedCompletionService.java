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
package org.apache.hadoop.hbase.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.htrace.Trace;

/**
 * A completion service for the RpcRetryingCallerFactory.
 * Keeps the list of the futures, and allows to cancel them all.
 * This means as well that it can be used for a small set of tasks only.
 * <br>Implementation is not Thread safe.
 */
@InterfaceAudience.Private
public class ResultBoundedCompletionService<V> {
  private final RpcRetryingCallerFactory retryingCallerFactory;
  private final Executor executor;
  private final QueueingFuture<V>[] tasks; // all the tasks
  private volatile QueueingFuture<V> completed = null;
  private volatile boolean cancelled = false;
  
  class QueueingFuture<T> implements RunnableFuture<T> {
    private final RetryingCallable<T> future;
    private T result = null;
    private ExecutionException exeEx = null;
    private volatile boolean cancelled = false;
    private final int callTimeout;
    private final RpcRetryingCaller<T> retryingCaller;
    private boolean resultObtained = false;


    public QueueingFuture(RetryingCallable<T> future, int callTimeout) {
      this.future = future;
      this.callTimeout = callTimeout;
      this.retryingCaller = retryingCallerFactory.<T>newCaller();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      try {
        if (!cancelled) {
          result = this.retryingCaller.callWithRetries(future, callTimeout);
          resultObtained = true;
        }
      } catch (Throwable t) {
        exeEx = new ExecutionException(t);
      } finally {
        synchronized (tasks) {
          // If this wasn't canceled then store the result.
          if (!cancelled && completed == null) {
            completed = (QueueingFuture<V>) QueueingFuture.this;
          }

          // Notify just in case there was someone waiting and this was canceled.
          // That shouldn't happen but better safe than sorry.
          tasks.notify();
        }
      }
    }
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      if (resultObtained || exeEx != null) return false;
      retryingCaller.cancel();
      if (future instanceof Cancellable) ((Cancellable)future).cancel();
      cancelled = true;
      return true;
    }

    @Override
    public boolean isCancelled() {
      return cancelled;
    }

    @Override
    public boolean isDone() {
      return resultObtained || exeEx != null;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      try {
        return get(1000, TimeUnit.DAYS);
      } catch (TimeoutException e) {
        throw new RuntimeException("You did wait for 1000 days here?", e);
      }
    }

    @Override
    public T get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      synchronized (tasks) {
        if (resultObtained) {
          return result;
        }
        if (exeEx != null) {
          throw exeEx;
        }
        unit.timedWait(tasks, timeout);
      }
      if (resultObtained) {
        return result;
      }
      if (exeEx != null) {
        throw exeEx;
      }

      throw new TimeoutException("timeout=" + timeout + ", " + unit);
    }
  }

  @SuppressWarnings("unchecked")
  public ResultBoundedCompletionService(
      RpcRetryingCallerFactory retryingCallerFactory, Executor executor,
      int maxTasks) {
    this.retryingCallerFactory = retryingCallerFactory;
    this.executor = executor;
    this.tasks = new QueueingFuture[maxTasks];
  }


  public void submit(RetryingCallable<V> task, int callTimeout, int id) {
    QueueingFuture<V> newFuture = new QueueingFuture<V>(task, callTimeout);
    executor.execute(Trace.wrap(newFuture));
    tasks[id] = newFuture;
  }

  public QueueingFuture<V> take() throws InterruptedException {
    synchronized (tasks) {
      while (completed == null && !cancelled) tasks.wait();
    }
    return completed;
  }

  public QueueingFuture<V> poll(long timeout, TimeUnit unit) throws InterruptedException {
    synchronized (tasks) {
      if (completed == null && !cancelled) unit.timedWait(tasks, timeout);
    }
    return completed;
  }

  public void cancelAll() {
    // Grab the lock on tasks so that cancelled is visible everywhere
    synchronized (tasks) {
      cancelled = true;
    }
    for (QueueingFuture<V> future : tasks) {
      if (future != null) future.cancel(true);
    }
  }
}
