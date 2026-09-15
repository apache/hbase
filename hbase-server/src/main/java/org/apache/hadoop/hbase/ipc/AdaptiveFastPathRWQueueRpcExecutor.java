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

import java.util.Deque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is subclass of {@link FastPathRWQueueRpcExecutor}, which has a better utility under various
 * kinds of workloads.
 */
@InterfaceAudience.Private
public class AdaptiveFastPathRWQueueRpcExecutor extends FastPathRWQueueRpcExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(FastPathRWQueueRpcExecutor.class);

  private static final String FASTPATH_ADAPTIVE_RATIO =
    "hbase.ipc.server.callqueue.fastpath.adaptive.ratio";
  private static final float FASTPATH_ADAPTIVE_DEFAULT = 0;
  private static final String FASTPATH_ADAPTIVE_RADIO_WRITE =
    "hbase.ipc.server.callqueue.fastpath.adaptive.ratio.write";
  private static final String FASTPATH_ADAPTIVE_RADIO_READ =
    "hbase.ipc.server.callqueue.fastpath.adaptive.ratio.read";
  private static final String FASTPATH_ADAPTIVE_RADIO_SCAN =
    "hbase.ipc.server.callqueue.fastpath.adaptive.ratio.scan";

  private final int writeSharedHandlers;
  private final int readSharedHandlers;
  private final int scanSharedHandlers;

  protected final Deque<FastPathRpcHandler> sharedHandlerStack = new ConcurrentLinkedDeque<>();

  public AdaptiveFastPathRWQueueRpcExecutor(String name, int handlerCount, int maxQueueLength,
    PriorityFunction priority, Configuration conf, Abortable abortable) {
    super(name, handlerCount, maxQueueLength, priority, conf, abortable);
    float adaptiveRatio = conf.getFloat(FASTPATH_ADAPTIVE_RATIO, FASTPATH_ADAPTIVE_DEFAULT);
    if (!checkAdaptiveRatioRationality(conf)) {
      LOG.warn("The adaptive ratio should be in (0.0, 1.0) but get " + adaptiveRatio
        + " using the default ratio: " + FASTPATH_ADAPTIVE_DEFAULT);
      adaptiveRatio = FASTPATH_ADAPTIVE_DEFAULT;
    }

    float writeRatio = conf.getFloat(FASTPATH_ADAPTIVE_RADIO_WRITE, FASTPATH_ADAPTIVE_DEFAULT);
    float readRatio = conf.getFloat(FASTPATH_ADAPTIVE_RADIO_READ, FASTPATH_ADAPTIVE_DEFAULT);
    float scanRatio = conf.getFloat(FASTPATH_ADAPTIVE_RADIO_SCAN, FASTPATH_ADAPTIVE_DEFAULT);

    writeSharedHandlers = checkRatioRationality(writeRatio)
      ? (int) (writeRatio * writeHandlersCount)
      : (int) (adaptiveRatio * writeHandlersCount);
    readSharedHandlers = checkRatioRationality(readRatio)
      ? (int) (readRatio * readHandlersCount)
      : (int) (adaptiveRatio * readHandlersCount);
    scanSharedHandlers = checkRatioRationality(scanRatio)
      ? (int) (scanRatio * scanHandlersCount)
      : (int) (adaptiveRatio * scanHandlersCount);
  }

  @Override
  public boolean dispatch(CallRunner callTask) {
    RpcCall call = callTask.getRpcCall();
    boolean isWriteRequest = isWriteRequest(call.getHeader(), call.getParam());
    boolean shouldDispatchToScanQueue = shouldDispatchToScanQueue(callTask);
    FastPathRpcHandler handler = isWriteRequest ? writeHandlerStack.poll()
      : shouldDispatchToScanQueue ? scanHandlerStack.poll()
      : readHandlerStack.poll();
    if (handler == null) {
      handler = sharedHandlerStack.poll();
    }

    return handler != null
      ? handler.loadCallRunner(callTask)
      : dispatchTo(isWriteRequest, shouldDispatchToScanQueue, callTask);
  }

  @Override
  protected void startHandlers(final int port) {
    startHandlers(".shared_write", writeSharedHandlers, queues, 0, numWriteQueues, port,
      activeWriteHandlerCount);

    startHandlers(".write", writeHandlersCount - writeSharedHandlers, queues, 0, numWriteQueues,
      port, activeWriteHandlerCount);

    startHandlers(".shared_read", readSharedHandlers, queues, numWriteQueues, numReadQueues, port,
      activeReadHandlerCount);
    startHandlers(".read", readHandlersCount - readSharedHandlers, queues, numWriteQueues,
      numReadQueues, port, activeReadHandlerCount);

    if (numScanQueues > 0) {
      startHandlers(".shared_scan", scanSharedHandlers, queues, numWriteQueues + numReadQueues,
        numScanQueues, port, activeScanHandlerCount);
      startHandlers(".scan", scanHandlersCount - scanSharedHandlers, queues,
        numWriteQueues + numReadQueues, numScanQueues, port, activeScanHandlerCount);
    }
  }

  @Override
  protected RpcHandler getHandler(final String name, final double handlerFailureThreshhold,
    final int handlerCount, final BlockingQueue<CallRunner> q,
    final AtomicInteger activeHandlerCount, final AtomicInteger failedHandlerCount,
    final Abortable abortable) {
    Deque<FastPathRpcHandler> handlerStack = name.contains("shared") ? sharedHandlerStack
      : name.contains("read") ? readHandlerStack
      : name.contains("write") ? writeHandlerStack
      : scanHandlerStack;
    return new FastPathRpcHandler(name, handlerFailureThreshhold, handlerCount, q,
      activeHandlerCount, failedHandlerCount, abortable, handlerStack);
  }

  int getWriteStackLength() {
    return writeHandlerStack.size();
  }

  int getReadStackLength() {
    return readHandlerStack.size();
  }

  int getScanStackLength() {
    return scanHandlerStack.size();
  }

  int getSharedStackLength() {
    return sharedHandlerStack.size();
  }

  static boolean checkAdaptiveRatioRationality(Configuration conf) {
    float ratio = conf.getFloat(FASTPATH_ADAPTIVE_RATIO, FASTPATH_ADAPTIVE_DEFAULT);
    return checkRatioRationality(ratio);
  }

  private static boolean checkRatioRationality(float ratio) {
    return !(ratio <= 0) && !(ratio >= 1.0f);
  }
}
