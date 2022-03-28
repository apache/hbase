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
package org.apache.hadoop.hbase.ipc;

import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hbase.thirdparty.io.netty.util.internal.StringUtil;

/**
 * A very simple {@code }RpcScheduler} that serves incoming requests in order.
 *
 * This can be used for HMaster, where no prioritization is needed.
 */
@InterfaceAudience.Private
public class FifoRpcScheduler extends RpcScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(FifoRpcScheduler.class);
  protected final int handlerCount;
  protected final int maxQueueLength;
  protected final AtomicInteger queueSize = new AtomicInteger(0);
  protected ThreadPoolExecutor executor;

  public FifoRpcScheduler(Configuration conf, int handlerCount) {
    this.handlerCount = handlerCount;
    this.maxQueueLength = conf.getInt(RpcScheduler.IPC_SERVER_MAX_CALLQUEUE_LENGTH,
        handlerCount * RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER);
  }

  @Override
  public void init(Context context) {
    // no-op
  }

  @Override
  public void start() {
    LOG.info("Using {} as user call queue; handlerCount={}; maxQueueLength={}",
      this.getClass().getSimpleName(), handlerCount, maxQueueLength);
    this.executor = new ThreadPoolExecutor(handlerCount, handlerCount, 60, TimeUnit.SECONDS,
      new ArrayBlockingQueue<>(maxQueueLength),
      new ThreadFactoryBuilder().setNameFormat("FifoRpcScheduler.handler-pool-%d").setDaemon(true)
        .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build(),
      new ThreadPoolExecutor.CallerRunsPolicy());
  }

  @Override
  public void stop() {
    this.executor.shutdown();
  }

  private static class FifoCallRunner implements Runnable {
    private final CallRunner callRunner;

    FifoCallRunner(CallRunner cr) {
      this.callRunner = cr;
    }

    CallRunner getCallRunner() {
      return callRunner;
    }

    @Override
    public void run() {
      callRunner.run();
    }

  }

  @Override
  public boolean dispatch(final CallRunner task) {
    return executeRpcCall(executor, queueSize, task);
  }

  protected boolean executeRpcCall(final ThreadPoolExecutor executor, final AtomicInteger queueSize,
      final CallRunner task) {
    // Executors provide no offer, so make our own.
    int queued = queueSize.getAndIncrement();
    if (maxQueueLength > 0 && queued >= maxQueueLength) {
      queueSize.decrementAndGet();
      return false;
    }

    executor.execute(new FifoCallRunner(task){
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
  public int getActiveGeneralRpcHandlerCount() {
    return getActiveRpcHandlerCount();
  }

  @Override
  public int getActivePriorityRpcHandlerCount() {
    return 0;
  }

  @Override
  public int getActiveReplicationRpcHandlerCount() {
    return 0;
  }

  @Override
  public int getActiveMetaPriorityRpcHandlerCount() {
    return 0;
  }

  @Override
  public long getNumGeneralCallsDropped() {
    return 0;
  }

  @Override
  public long getNumLifoModeSwitches() {
    return 0;
  }

  @Override
  public int getWriteQueueLength() {
    return 0;
  }

  @Override
  public int getReadQueueLength() {
    return 0;
  }

  @Override
  public int getScanQueueLength() {
    return 0;
  }

  @Override
  public int getActiveWriteRpcHandlerCount() {
    return 0;
  }

  @Override
  public int getActiveReadRpcHandlerCount() {
    return 0;
  }

  @Override
  public int getActiveScanRpcHandlerCount() {
    return 0;
  }

  @Override
  public int getMetaPriorityQueueLength() {
    return 0;
  }

  @Override
  public CallQueueInfo getCallQueueInfo() {
    String queueName = "Fifo Queue";

    HashMap<String, Long> methodCount = new HashMap<>();
    HashMap<String, Long> methodSize = new HashMap<>();

    CallQueueInfo callQueueInfo = new CallQueueInfo();
    callQueueInfo.setCallMethodCount(queueName, methodCount);
    callQueueInfo.setCallMethodSize(queueName, methodSize);

    updateMethodCountAndSizeByQueue(executor.getQueue(), methodCount, methodSize);

    return callQueueInfo;
  }

  protected void updateMethodCountAndSizeByQueue(BlockingQueue<Runnable> queue,
      HashMap<String, Long> methodCount, HashMap<String, Long> methodSize) {
    for (Runnable r : queue) {
      FifoCallRunner mcr = (FifoCallRunner) r;
      RpcCall rpcCall = mcr.getCallRunner().getRpcCall();

      String method = getCallMethod(mcr.getCallRunner());
      if (StringUtil.isNullOrEmpty(method)) {
        method = "Unknown";
      }

      long size = rpcCall.getSize();

      methodCount.put(method, 1 + methodCount.getOrDefault(method, 0L));
      methodSize.put(method, size + methodSize.getOrDefault(method, 0L));
    }
  }

  protected String getCallMethod(final CallRunner task) {
    RpcCall call = task.getRpcCall();
    if (call != null && call.getMethod() != null) {
      return call.getMethod().getName();
    }
    return null;
  }
}
