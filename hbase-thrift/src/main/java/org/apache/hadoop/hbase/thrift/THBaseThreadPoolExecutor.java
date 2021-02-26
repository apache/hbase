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

package org.apache.hadoop.hbase.thrift;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A ThreadPoolExecutor customized for working with HBase thrift to update metrics before and
 * after the execution of a task.
 */

@InterfaceAudience.Private
public class THBaseThreadPoolExecutor extends ThreadPoolExecutor {

  private ThriftMetrics metrics;

  public THBaseThreadPoolExecutor(int corePoolSize, int maxPoolSize, long keepAliveTime,
      TimeUnit unit, BlockingQueue<Runnable> workQueue, ThriftMetrics metrics) {
    this(corePoolSize, maxPoolSize, keepAliveTime, unit, workQueue, null, metrics);
  }

  public THBaseThreadPoolExecutor(int corePoolSize, int maxPoolSize, long keepAliveTime,
      TimeUnit unit, BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory,ThriftMetrics metrics) {
    super(corePoolSize, maxPoolSize, keepAliveTime, unit, workQueue);
    if (threadFactory != null) {
      setThreadFactory(threadFactory);
    }
    this.metrics = metrics;
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    super.beforeExecute(t, r);
    metrics.incActiveWorkerCount();
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    metrics.decActiveWorkerCount();
    super.afterExecute(r, t);
  }
}
