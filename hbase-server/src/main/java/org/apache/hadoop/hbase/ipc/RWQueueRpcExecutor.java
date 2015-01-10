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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.util.ReflectionUtils;

import com.google.protobuf.Message;

/**
 * RPC Executor that uses different queues for reads and writes.
 * Each handler has its own queue and there is no stealing.
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public class RWQueueRpcExecutor extends RpcExecutor {
  private static final Log LOG = LogFactory.getLog(RWQueueRpcExecutor.class);

  private final List<BlockingQueue<CallRunner>> queues;
  private final QueueBalancer writeBalancer;
  private final QueueBalancer readBalancer;
  private final int writeHandlersCount;
  private final int readHandlersCount;
  private final int numWriteQueues;
  private final int numReadQueues;

  public RWQueueRpcExecutor(final String name, final int handlerCount, final int numQueues,
      final float readShare, final int maxQueueLength) {
    this(name, handlerCount, numQueues, readShare, maxQueueLength, null, null);
  }

  public RWQueueRpcExecutor(final String name, final int handlerCount, final int numQueues,
      final float readShare, final int maxQueueLength, final Configuration conf, final Abortable abortable) {
    this(name, handlerCount, numQueues, readShare, maxQueueLength, conf, abortable, 
      LinkedBlockingQueue.class);
  }

  public RWQueueRpcExecutor(final String name, final int handlerCount, final int numQueues,
      final float readShare, final int maxQueueLength,
      final Configuration conf, final Abortable abortable,
      final Class<? extends BlockingQueue> readQueueClass, Object... readQueueInitArgs) {
    this(name, calcNumWriters(handlerCount, readShare), calcNumReaders(handlerCount, readShare),
      calcNumWriters(numQueues, readShare), calcNumReaders(numQueues, readShare),
      conf, abortable,
      LinkedBlockingQueue.class, new Object[] {maxQueueLength},
      readQueueClass, ArrayUtils.addAll(new Object[] {maxQueueLength}, readQueueInitArgs));
  }

  public RWQueueRpcExecutor(final String name, final int writeHandlers, final int readHandlers,
      final int numWriteQueues, final int numReadQueues,
      final Configuration conf, final Abortable abortable,
      final Class<? extends BlockingQueue> writeQueueClass, Object[] writeQueueInitArgs,
      final Class<? extends BlockingQueue> readQueueClass, Object[] readQueueInitArgs) {
    super(name, Math.max(writeHandlers + readHandlers, numWriteQueues + numReadQueues), conf, abortable);

    this.writeHandlersCount = Math.max(writeHandlers, numWriteQueues);
    this.readHandlersCount = Math.max(readHandlers, numReadQueues);
    this.numWriteQueues = numWriteQueues;
    this.numReadQueues = numReadQueues;
    this.writeBalancer = getBalancer(numWriteQueues);
    this.readBalancer = getBalancer(numReadQueues);

    queues = new ArrayList<BlockingQueue<CallRunner>>(writeHandlersCount + readHandlersCount);
    LOG.debug(name + " writeQueues=" + numWriteQueues + " writeHandlers=" + writeHandlersCount +
              " readQueues=" + numReadQueues + " readHandlers=" + readHandlersCount);

    for (int i = 0; i < numWriteQueues; ++i) {
      queues.add((BlockingQueue<CallRunner>)
        ReflectionUtils.newInstance(writeQueueClass, writeQueueInitArgs));
    }

    for (int i = 0; i < numReadQueues; ++i) {
      queues.add((BlockingQueue<CallRunner>)
        ReflectionUtils.newInstance(readQueueClass, readQueueInitArgs));
    }
  }

  @Override
  protected void startHandlers(final int port) {
    startHandlers(".write", writeHandlersCount, queues, 0, numWriteQueues, port);
    startHandlers(".read", readHandlersCount, queues, numWriteQueues, numReadQueues, port);
  }

  @Override
  public void dispatch(final CallRunner callTask) throws InterruptedException {
    RpcServer.Call call = callTask.getCall();
    int queueIndex;
    if (isWriteRequest(call.getHeader(), call.param)) {
      queueIndex = writeBalancer.getNextQueue();
    } else {
      queueIndex = numWriteQueues + readBalancer.getNextQueue();
    }
    queues.get(queueIndex).put(callTask);
  }

  private boolean isWriteRequest(final RequestHeader header, final Message param) {
    // TODO: Is there a better way to do this?
    String methodName = header.getMethodName();
    if (methodName.equalsIgnoreCase("multi") && param instanceof MultiRequest) {
      MultiRequest multi = (MultiRequest)param;
      for (RegionAction regionAction : multi.getRegionActionList()) {
        for (Action action: regionAction.getActionList()) {
          if (action.hasMutation()) {
            return true;
          }
        }
      }
    }
    return false;
  }

  @Override
  public int getQueueLength() {
    int length = 0;
    for (final BlockingQueue<CallRunner> queue: queues) {
      length += queue.size();
    }
    return length;
  }

  @Override
  protected List<BlockingQueue<CallRunner>> getQueues() {
    return queues;
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
