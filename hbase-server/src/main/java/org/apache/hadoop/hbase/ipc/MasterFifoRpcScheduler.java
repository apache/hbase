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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A special {@code }RpcScheduler} only used for master. This scheduler separates RegionServerReport
 * requests to independent handlers to avoid these requests block other requests. To use this
 * scheduler, please set "hbase.master.rpc.scheduler.factory.class" to
 * "org.apache.hadoop.hbase.ipc.MasterFifoRpcScheduler".
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MasterFifoRpcScheduler extends FifoRpcScheduler {
  private static final Logger LOG = LoggerFactory.getLogger(MasterFifoRpcScheduler.class);

  /**
   * Set RSReport requests handlers count when masters use MasterFifoRpcScheduler. The default value
   * is half of "hbase.regionserver.handler.count" value, but at least 1. The other handlers count
   * is "hbase.regionserver.handler.count" value minus RSReport handlers count, but at least 1 too.
   */
  public static final String MASTER_SERVER_REPORT_HANDLER_COUNT =
      "hbase.master.server.report.handler.count";
  private static final String REGION_SERVER_REPORT = "RegionServerReport";
  private final int rsReportHandlerCount;
  private final int rsRsreportMaxQueueLength;
  private final AtomicInteger rsReportQueueSize = new AtomicInteger(0);
  private ThreadPoolExecutor rsReportExecutor;

  public MasterFifoRpcScheduler(Configuration conf, int callHandlerCount,
      int rsReportHandlerCount) {
    super(conf, callHandlerCount);
    this.rsReportHandlerCount = rsReportHandlerCount;
    this.rsRsreportMaxQueueLength = conf.getInt(RpcScheduler.IPC_SERVER_MAX_CALLQUEUE_LENGTH,
      rsReportHandlerCount * RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER);
  }

  @Override
  public void start() {
    LOG.info(
      "Using {} as call queue; handlerCount={}; maxQueueLength={}; rsReportHandlerCount={}; "
          + "rsReportMaxQueueLength={}",
      this.getClass().getSimpleName(), handlerCount, maxQueueLength, rsReportHandlerCount,
      rsRsreportMaxQueueLength);
    this.executor = new ThreadPoolExecutor(handlerCount, handlerCount, 60, TimeUnit.SECONDS,
      new ArrayBlockingQueue<>(maxQueueLength),
      new ThreadFactoryBuilder().setNameFormat("MasterFifoRpcScheduler.call.handler-pool-%d")
        .setDaemon(true).setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build(),
      new ThreadPoolExecutor.CallerRunsPolicy());
    this.rsReportExecutor = new ThreadPoolExecutor(rsReportHandlerCount, rsReportHandlerCount, 60,
      TimeUnit.SECONDS, new ArrayBlockingQueue<>(rsRsreportMaxQueueLength),
      new ThreadFactoryBuilder().setNameFormat("MasterFifoRpcScheduler.RSReport.handler-pool-%d")
        .setDaemon(true).setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build(),
      new ThreadPoolExecutor.CallerRunsPolicy());
  }

  @Override
  public void stop() {
    this.executor.shutdown();
    this.rsReportExecutor.shutdown();
  }

  @Override
  public boolean dispatch(final CallRunner task) {
    String method = getCallMethod(task);
    if (rsReportExecutor != null && method != null && method.equals(REGION_SERVER_REPORT)) {
      return executeRpcCall(rsReportExecutor, rsReportQueueSize, task);
    } else {
      return executeRpcCall(executor, queueSize, task);
    }
  }

  @Override
  public int getGeneralQueueLength() {
    return executor.getQueue().size() + rsReportExecutor.getQueue().size();
  }

  @Override
  public int getActiveRpcHandlerCount() {
    return executor.getActiveCount() + rsReportExecutor.getActiveCount();
  }

  @Override
  public CallQueueInfo getCallQueueInfo() {
    String queueName = "Master Fifo Queue";

    HashMap<String, Long> methodCount = new HashMap<>();
    HashMap<String, Long> methodSize = new HashMap<>();

    CallQueueInfo callQueueInfo = new CallQueueInfo();
    callQueueInfo.setCallMethodCount(queueName, methodCount);
    callQueueInfo.setCallMethodSize(queueName, methodSize);

    updateMethodCountAndSizeByQueue(executor.getQueue(), methodCount, methodSize);
    updateMethodCountAndSizeByQueue(rsReportExecutor.getQueue(), methodCount, methodSize);

    return callQueueInfo;
  }
}
