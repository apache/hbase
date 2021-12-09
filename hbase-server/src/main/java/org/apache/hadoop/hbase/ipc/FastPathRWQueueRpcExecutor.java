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

import java.util.Deque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RPC Executor that extends {@link RWQueueRpcExecutor} with fast-path feature, used in
 * {@link FastPathBalancedQueueRpcExecutor}.
 */
@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public class FastPathRWQueueRpcExecutor extends RWQueueRpcExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(RWQueueRpcExecutor.class);

  private final Deque<FastPathHandler> readHandlerStack = new ConcurrentLinkedDeque<>();
  private final Deque<FastPathHandler> writeHandlerStack = new ConcurrentLinkedDeque<>();
  private final Deque<FastPathHandler> scanHandlerStack = new ConcurrentLinkedDeque<>();

  public FastPathRWQueueRpcExecutor(String name, int handlerCount, int maxQueueLength,
    PriorityFunction priority, Configuration conf, Abortable abortable) {
    super(name, handlerCount, maxQueueLength, priority, conf, abortable);
  }

  @Override
  protected Handler getHandler(final String name, final double handlerFailureThreshhold,
    final BlockingQueue<CallRunner> q, final AtomicInteger activeHandlerCount) {
    Deque<FastPathHandler> handlerStack = name.contains("read") ? readHandlerStack :
      name.contains("write") ? writeHandlerStack : scanHandlerStack;
    return new FastPathHandler(name, handlerFailureThreshhold, q, activeHandlerCount,
      handlerStack);
  }

  @Override
  public boolean dispatch(final CallRunner callTask) throws InterruptedException {
    RpcCall call = callTask.getRpcCall();
    boolean isWriteRequest = isWriteRequest(call.getHeader(), call.getParam());
    boolean shouldDispatchToScanQueue = shouldDispatchToScanQueue(callTask);
    FastPathHandler handler = isWriteRequest ? writeHandlerStack.poll() :
      shouldDispatchToScanQueue ? scanHandlerStack.poll() : readHandlerStack.poll();
    return handler != null ? handler.loadCallRunner(callTask) :
      dispatchTo(isWriteRequest, shouldDispatchToScanQueue, callTask);
  }

  class FastPathHandler extends Handler {
    // Below are for fast-path support. Push this Handler on to the fastPathHandlerStack Deque
    // if an empty queue of CallRunners so we are available for direct handoff when one comes in.
    final Deque<FastPathHandler> fastPathHandlerStack;
    // Semaphore to coordinate loading of fastpathed loadedTask and our running it.
    // UNFAIR synchronization.
    private Semaphore semaphore = new Semaphore(0);
    // The task we get when fast-pathing.
    private CallRunner loadedCallRunner;

    FastPathHandler(String name, double handlerFailureThreshhold, BlockingQueue<CallRunner> q,
      final AtomicInteger activeHandlerCount,
      final Deque<FastPathHandler> fastPathHandlerStack) {
      super(name, handlerFailureThreshhold, q, activeHandlerCount);
      this.fastPathHandlerStack = fastPathHandlerStack;
    }

    @Override
    protected CallRunner getCallRunner() throws InterruptedException {
      // Get a callrunner if one in the Q.
      CallRunner cr = this.q.poll();
      if (cr == null) {
        // Else, if a fastPathHandlerStack present and no callrunner in Q, register ourselves for
        // the fastpath handoff done via fastPathHandlerStack.
        if (this.fastPathHandlerStack != null) {
          this.fastPathHandlerStack.push(this);
          this.semaphore.acquire();
          cr = this.loadedCallRunner;
          this.loadedCallRunner = null;
        } else {
          // No fastpath available. Block until a task comes available.
          cr = super.getCallRunner();
        }
      }
      return cr;
    }

    /**
     * @param cr Task gotten via fastpath.
     * @return True if we successfully loaded our task
     */
    boolean loadCallRunner(final CallRunner cr) {
      this.loadedCallRunner = cr;
      this.semaphore.release();
      return true;
    }
  }
}
