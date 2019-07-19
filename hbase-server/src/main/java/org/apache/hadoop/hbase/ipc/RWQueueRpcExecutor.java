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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Action;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.util.ReflectionUtils;

import com.google.protobuf.Message;

/**
 * RPC Executor that uses different queues for reads and writes.
 * With the options to use different queues/executors for gets and scans.
 * Each handler has its own queue and there is no stealing.
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public class RWQueueRpcExecutor extends RpcExecutor {
  private static final Log LOG = LogFactory.getLog(RWQueueRpcExecutor.class);

  public static final String CALL_QUEUE_READ_SHARE_CONF_KEY =
      "hbase.ipc.server.callqueue.read.ratio";
  public static final String CALL_QUEUE_SCAN_SHARE_CONF_KEY =
      "hbase.ipc.server.callqueue.scan.ratio";

  private final QueueBalancer writeBalancer;
  private final QueueBalancer readBalancer;
  private final QueueBalancer scanBalancer;
  private final int writeHandlersCount;
  private final int readHandlersCount;
  private final int scanHandlersCount;
  private final int numWriteQueues;
  private final int numReadQueues;
  private final int numScanQueues;

  private final AtomicInteger activeWriteHandlerCount = new AtomicInteger(0);
  private final AtomicInteger activeReadHandlerCount = new AtomicInteger(0);
  private final AtomicInteger activeScanHandlerCount = new AtomicInteger(0);

  public RWQueueRpcExecutor(final String name, final int handlerCount, final int maxQueueLength,
      final PriorityFunction priority, final Configuration conf, final Abortable abortable) {
    super(name, handlerCount, maxQueueLength, priority, conf, abortable);

    float callqReadShare = conf.getFloat(CALL_QUEUE_READ_SHARE_CONF_KEY, 0);
    float callqScanShare = conf.getFloat(CALL_QUEUE_SCAN_SHARE_CONF_KEY, 0);

    numWriteQueues = calcNumWriters(this.numCallQueues, callqReadShare);
    writeHandlersCount = Math.max(numWriteQueues, calcNumWriters(handlerCount, callqReadShare));

    int readQueues = calcNumReaders(this.numCallQueues, callqReadShare);
    int readHandlers = Math.max(readQueues, calcNumReaders(handlerCount, callqReadShare));

    int scanQueues = Math.max(0, (int)Math.floor(readQueues * callqScanShare));
    int scanHandlers = Math.max(0, (int)Math.floor(readHandlers * callqScanShare));

    if ((readQueues - scanQueues) > 0) {
      readQueues -= scanQueues;
      readHandlers -= scanHandlers;
    } else {
      scanQueues = 0;
      scanHandlers = 0;
    }

    numReadQueues = readQueues;
    readHandlersCount = readHandlers;
    numScanQueues = scanQueues;
    scanHandlersCount = scanHandlers;

    this.writeBalancer = getBalancer(numWriteQueues);
    this.readBalancer = getBalancer(numReadQueues);
    this.scanBalancer = numScanQueues > 0 ? getBalancer(numScanQueues) : null;

    initializeQueues(numWriteQueues);
    initializeQueues(numReadQueues);
    initializeQueues(numScanQueues);

    LOG.info(getName() + " writeQueues=" + numWriteQueues + " writeHandlers=" + writeHandlersCount
      + " readQueues=" + numReadQueues + " readHandlers=" + readHandlersCount + " scanQueues="
      + numScanQueues + " scanHandlers=" + scanHandlersCount);
  }

  @Deprecated
  public RWQueueRpcExecutor(final String name, final int handlerCount, final int numQueues,
      final float readShare, final int maxQueueLength,
      final Configuration conf, final Abortable abortable) {
    this(name, handlerCount, numQueues, readShare, maxQueueLength, 0,
      conf, abortable, LinkedBlockingQueue.class);
  }

  @Deprecated
  public RWQueueRpcExecutor(final String name, final int handlerCount, final int numQueues,
      final float readShare, final float scanShare, final int maxQueueLength) {
    this(name, handlerCount, numQueues, readShare, scanShare, maxQueueLength, null, null);
  }

  @Deprecated
  public RWQueueRpcExecutor(final String name, final int handlerCount, final int numQueues,
      final float readShare, final float scanShare, final int maxQueueLength,
      final Configuration conf, final Abortable abortable) {
    this(name, handlerCount, numQueues, readShare, scanShare, maxQueueLength,
      conf, abortable, LinkedBlockingQueue.class);
  }

  @Deprecated
  public RWQueueRpcExecutor(final String name, final int handlerCount, final int numQueues,
      final float readShare, final int maxQueueLength,
      final Configuration conf, final Abortable abortable,
      final Class<? extends BlockingQueue> readQueueClass, Object... readQueueInitArgs) {
    this(name, handlerCount, numQueues, readShare, 0, maxQueueLength, conf, abortable,
      readQueueClass, readQueueInitArgs);
  }

  @Deprecated
  public RWQueueRpcExecutor(final String name, final int handlerCount, final int numQueues,
      final float readShare, final float scanShare, final int maxQueueLength,
      final Configuration conf, final Abortable abortable,
      final Class<? extends BlockingQueue> readQueueClass, Object... readQueueInitArgs) {
    this(name, calcNumWriters(handlerCount, readShare), calcNumReaders(handlerCount, readShare),
      calcNumWriters(numQueues, readShare), calcNumReaders(numQueues, readShare), scanShare,
      LinkedBlockingQueue.class, new Object[] {maxQueueLength},
      readQueueClass, ArrayUtils.addAll(new Object[] {maxQueueLength}, readQueueInitArgs));
  }

  @Deprecated
  public RWQueueRpcExecutor(final String name, final int handlerCount, final int numQueues,
      final float readShare, final float scanShare,
      final Class<? extends BlockingQueue> writeQueueClass, Object[] writeQueueInitArgs,
      final Class<? extends BlockingQueue> readQueueClass, Object[] readQueueInitArgs) {
    this(name, calcNumWriters(handlerCount, readShare), calcNumReaders(handlerCount, readShare),
      calcNumWriters(numQueues, readShare), calcNumReaders(numQueues, readShare), scanShare,
      writeQueueClass, writeQueueInitArgs,
      readQueueClass, readQueueInitArgs);
  }

  @Deprecated
  public RWQueueRpcExecutor(final String name, final int writeHandlers, final int readHandlers,
      final int numWriteQueues, final int numReadQueues,
      final Class<? extends BlockingQueue> writeQueueClass, Object[] writeQueueInitArgs,
      final Class<? extends BlockingQueue> readQueueClass, Object[] readQueueInitArgs) {
    this(name, writeHandlers, readHandlers, numWriteQueues, numReadQueues, 0,
      writeQueueClass, writeQueueInitArgs, readQueueClass, readQueueInitArgs);
  }

  @Deprecated
  public RWQueueRpcExecutor(final String name, int writeHandlers, int readHandlers,
      int numWriteQueues, int numReadQueues, float scanShare,
      final Class<? extends BlockingQueue> writeQueueClass, Object[] writeQueueInitArgs,
      final Class<? extends BlockingQueue> readQueueClass, Object[] readQueueInitArgs) {
    super(name, Math.max(writeHandlers, numWriteQueues) + Math.max(readHandlers, numReadQueues),
        numWriteQueues + numReadQueues);

    this.writeHandlersCount = Math.max(writeHandlers, numWriteQueues);
    this.numWriteQueues = numWriteQueues;

    int numScanQueues = Math.max(0, (int) Math.floor(numReadQueues * scanShare));
    int scanHandlers = Math.max(0, (int) Math.floor(readHandlers * scanShare));
    if ((numReadQueues - numScanQueues) > 0) {
      numReadQueues -= numScanQueues;
      readHandlers -= scanHandlers;
    } else {
      numScanQueues = 0;
      scanHandlers = 0;
    }

    this.readHandlersCount = Math.max(readHandlers, numReadQueues);
    this.scanHandlersCount = Math.max(scanHandlers, numScanQueues);
    this.numReadQueues = numReadQueues;
    this.numScanQueues = numScanQueues;

    this.writeBalancer = getBalancer(numWriteQueues);
    this.readBalancer = getBalancer(numReadQueues);
    this.scanBalancer = numScanQueues > 0 ? getBalancer(numScanQueues) : null;

    LOG.info(name + " writeQueues=" + numWriteQueues + " writeHandlers=" + writeHandlersCount
        + " readQueues=" + numReadQueues + " readHandlers=" + readHandlersCount + " scanQueues="
        + numScanQueues + " scanHandlers=" + scanHandlersCount);

    if (writeQueueInitArgs.length > 0) {
      currentQueueLimit = (int) writeQueueInitArgs[0];
      writeQueueInitArgs[0] = Math.max((int) writeQueueInitArgs[0],
        DEFAULT_CALL_QUEUE_SIZE_HARD_LIMIT);
    }
    for (int i = 0; i < numWriteQueues; ++i) {
      queues.add((BlockingQueue<CallRunner>) ReflectionUtils.newInstance(writeQueueClass,
        writeQueueInitArgs));
    }

    if (readQueueInitArgs.length > 0) {
      currentQueueLimit = (int) readQueueInitArgs[0];
      readQueueInitArgs[0] = Math.max((int) readQueueInitArgs[0],
        DEFAULT_CALL_QUEUE_SIZE_HARD_LIMIT);
    }
    for (int i = 0; i < (numReadQueues + numScanQueues); ++i) {
      queues.add((BlockingQueue<CallRunner>) ReflectionUtils.newInstance(readQueueClass,
        readQueueInitArgs));
    }
  }

  @Override
  protected int computeNumCallQueues(final int handlerCount, final float callQueuesHandlersFactor) {
    // at least 1 read queue and 1 write queue
    return Math.max(2, (int) Math.round(handlerCount * callQueuesHandlersFactor));
  }

  @Override
  protected void startHandlers(final int port) {
    startHandlers(".write", writeHandlersCount, queues, 0, numWriteQueues, port,
      activeWriteHandlerCount);
    startHandlers(".read", readHandlersCount, queues, numWriteQueues, numReadQueues, port,
      activeReadHandlerCount);
    if (numScanQueues > 0) {
      startHandlers(".scan", scanHandlersCount, queues, numWriteQueues + numReadQueues,
        numScanQueues, port, activeScanHandlerCount);
    }
  }

  @Override
  public boolean dispatch(final CallRunner callTask) throws InterruptedException {
    RpcServer.Call call = callTask.getCall();
    int queueIndex;
    if (isWriteRequest(call.getHeader(), call.param)) {
      queueIndex = writeBalancer.getNextQueue();
    } else if (numScanQueues > 0 && isScanRequest(call.getHeader(), call.param)) {
      queueIndex = numWriteQueues + numReadQueues + scanBalancer.getNextQueue();
    } else {
      queueIndex = numWriteQueues + readBalancer.getNextQueue();
    }

    BlockingQueue<CallRunner> queue = queues.get(queueIndex);
    if (queue.size() >= currentQueueLimit) {
      return false;
    }
    return queue.offer(callTask);
  }

  @Override
  public int getWriteQueueLength() {
    int length = 0;
    for (int i = 0; i < numWriteQueues; i++) {
      length += queues.get(i).size();
    }
    return length;
  }

  @Override
  public int getReadQueueLength() {
    int length = 0;
    for (int i = numWriteQueues; i < (numWriteQueues + numReadQueues); i++) {
      length += queues.get(i).size();
    }
    return length;
  }

  @Override
  public int getScanQueueLength() {
    int length = 0;
    for (int i = numWriteQueues + numReadQueues;
        i < (numWriteQueues + numReadQueues + numScanQueues); i++) {
      length += queues.get(i).size();
    }
    return length;
  }

  @Override
  public int getActiveHandlerCount() {
    return activeWriteHandlerCount.get() + activeReadHandlerCount.get()
        + activeScanHandlerCount.get();
  }

  @Override
  public int getActiveWriteHandlerCount() {
    return activeWriteHandlerCount.get();
  }

  @Override
  public int getActiveReadHandlerCount() {
    return activeReadHandlerCount.get();
  }

  @Override
  public int getActiveScanHandlerCount() {
    return activeScanHandlerCount.get();
  }

  private boolean isWriteRequest(final RequestHeader header, final Message param) {
    // TODO: Is there a better way to do this?
    if (param instanceof MultiRequest) {
      MultiRequest multi = (MultiRequest)param;
      for (RegionAction regionAction : multi.getRegionActionList()) {
        for (Action action: regionAction.getActionList()) {
          if (action.hasMutation()) {
            return true;
          }
        }
      }
    }
    if (param instanceof MutateRequest) {
      return true;
    }
    // Below here are methods for master. It's a pretty brittle version of this.
    // Not sure that master actually needs a read/write queue since 90% of requests to
    // master are writing to status or changing the meta table.
    // All other read requests are admin generated and can be processed whenever.
    // However changing that would require a pretty drastic change and should be done for
    // the next major release and not as a fix for HBASE-14239
    if (param instanceof RegionServerStatusProtos.ReportRegionStateTransitionRequest) {
      return true;
    }
    if (param instanceof RegionServerStatusProtos.RegionServerStartupRequest) {
      return true;
    }
    if (param instanceof RegionServerStatusProtos.RegionServerReportRequest) {
      return true;
    }
    return false;
  }

  private boolean isScanRequest(final RequestHeader header, final Message param) {
    return param instanceof ScanRequest;
  }

  /*
   * Calculate the number of writers based on the "total count" and the read share.
   * You'll get at least one writer.
   */
  private static int calcNumWriters(final int count, final float readShare) {
    return Math.max(1, count - Math.max(1, (int)Math.round(count * readShare)));
  }

  /*
   * Calculate the number of readers based on the "total count" and the read share.
   * You'll get at least one reader.
   */
  private static int calcNumReaders(final int count, final float readShare) {
    return count - calcNumWriters(count, readShare);
  }
}
