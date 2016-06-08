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
package org.apache.hadoop.hbase.ipc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DaemonThreadFactory;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A very simple {@code }RpcScheduler} that serves incoming requests in order.
 *
 * This can be used for HMaster, where no prioritization is needed.
 */
public class FifoRpcScheduler extends RpcScheduler {
  private static final Log LOG = LogFactory.getLog(FifoRpcScheduler.class);
  private final int handlerCount;
  private final int maxQueueLength;
  private final AtomicInteger queueSize = new AtomicInteger(0);
  private ThreadPoolExecutor executor;

  public FifoRpcScheduler(Configuration conf, int handlerCount) {
    this.handlerCount = handlerCount;
    this.maxQueueLength = conf.getInt(RpcScheduler.IPC_SERVER_MAX_CALLQUEUE_LENGTH,
        handlerCount * RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER);
    LOG.info("Using " + this.getClass().getSimpleName() + " as user call queue; handlerCount=" +
        handlerCount + "; maxQueueLength=" + maxQueueLength);
  }

  @Override
  public void init(Context context) {
    // no-op
  }

  @Override
  public void start() {
    this.executor = new ThreadPoolExecutor(
        handlerCount,
        handlerCount,
        60,
        TimeUnit.SECONDS,
        new ArrayBlockingQueue<Runnable>(maxQueueLength),
        new DaemonThreadFactory("FifoRpcScheduler.handler"),
        new ThreadPoolExecutor.CallerRunsPolicy());
  }

  @Override
  public void stop() {
    this.executor.shutdown();
  }

  @Override
  public boolean dispatch(final CallRunner task) throws IOException, InterruptedException {
    // Executors provide no offer, so make our own.
    int queued = queueSize.getAndIncrement();
    if (maxQueueLength > 0 && queued >= maxQueueLength) {
      queueSize.decrementAndGet();
      return false;
    }
    executor.submit(new Runnable() {
      @Override
      public void run() {
        task.setStatus(RpcServer.getStatus());
        task.run();
        queueSize.decrementAndGet();
      }
    });
    return true;
  }

  @Override
  public int getGeneralQueueLength() {
    return executor.getQueue().size();
  }

  @Override
  public int getPriorityQueueLength() {
    return 0;
  }

  @Override
  public int getReplicationQueueLength() {
    return 0;
  }

  @Override
  public int getActiveRpcHandlerCount() {
    return executor.getActiveCount();
  }

  @Override
  public long getNumGeneralCallsDropped() {
    return 0;
  }

  @Override
  public long getNumLifoModeSwitches() {
    return 0;
  }
}
