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

package org.apache.hadoop.hbase.util;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A completion service, close to the one available in the JDK 1.7
 * However, this ones keeps the list of the future, and allows to cancel them all.
 * This means as well that it can be used for a small set of tasks only.
 * <br>Implementation is not Thread safe.
 */
@InterfaceAudience.Private
public class BoundedCompletionService<V> {
  private final Executor executor;
  private final List<Future<V>> tasks; // alls the tasks
  private final BlockingQueue<Future<V>> completed; // all the tasks that are completed

  class QueueingFuture extends FutureTask<V> {

    public QueueingFuture(Callable<V> callable) {
      super(callable);
    }

    @Override
    protected void done() {
      completed.add(QueueingFuture.this);
    }
  }

  public BoundedCompletionService(Executor executor, int maxTasks) {
    this.executor = executor;
    this.tasks = new ArrayList<Future<V>>(maxTasks);
    this.completed = new ArrayBlockingQueue<Future<V>>(maxTasks);
  }


  public Future<V> submit(Callable<V> task) {
    QueueingFuture newFuture = new QueueingFuture(task);
    executor.execute(newFuture);
    tasks.add(newFuture);
    return newFuture;
  }

  public  Future<V> take() throws InterruptedException{
    return completed.take();
  }

  public Future<V> poll(long timeout, TimeUnit unit) throws InterruptedException{
    return completed.poll(timeout, unit);
  }

  public void cancelAll(boolean interrupt) {
    for (Future<V> future : tasks) {
      future.cancel(interrupt);
    }
  }
}
