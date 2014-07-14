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

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * RPC Executor that uses a single queue for all the requests.
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public class SingleQueueRpcExecutor extends RpcExecutor {
  private final BlockingQueue<CallRunner> queue;

  public SingleQueueRpcExecutor(final String name, final int handlerCount,
      final int maxQueueLength) {
    this(name, handlerCount, LinkedBlockingQueue.class, maxQueueLength);
  }

  public SingleQueueRpcExecutor(final String name, final int handlerCount,
      final Class<? extends BlockingQueue> queueClass, Object... initargs) {
    super(name, handlerCount);
    queue = (BlockingQueue<CallRunner>)ReflectionUtils.newInstance(queueClass, initargs);
  }

  @Override
  public void dispatch(final CallRunner callTask) throws InterruptedException {
    queue.put(callTask);
  }

  @Override
  public int getQueueLength() {
    return queue.size();
  }

  @Override
  protected List<BlockingQueue<CallRunner>> getQueues() {
    List<BlockingQueue<CallRunner>> list = new ArrayList<BlockingQueue<CallRunner>>(1);
    list.add(queue);
    return list;
  }
}
