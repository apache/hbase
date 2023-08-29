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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX })
@InterfaceStability.Evolving
public class StealReadJobRWQueueRpcExecutor extends FastPathRWQueueRpcExecutor {
  public StealReadJobRWQueueRpcExecutor(String name, int handlerCount, int maxQueueLength,
    PriorityFunction priority, Configuration conf, Abortable abortable) {
    super(name, handlerCount, maxQueueLength, priority, conf, abortable);
  }

  @Override
  public void initQueues() {
    queues = new ArrayList<>(this.numWriteQueues + this.numReadQueues + numScanQueues);
    initializeQueues(numWriteQueues);
    if (numReadQueues > 0 && numScanQueues > 0) {
      int stealQueueCount = Math.min(numReadQueues, numScanQueues);
      List<BlockingQueue<CallRunner>> stealScanQueues = new ArrayList<>(stealQueueCount);
      for (int i = 0; i < stealQueueCount; i++) {
        FIFOStealJobQueue<CallRunner> scanQueue =
          new FIFOStealJobQueue<>(maxQueueLength, maxQueueLength);
        BlockingQueue<CallRunner> readQueue = scanQueue.getStealFromQueue();
        queues.add(readQueue);
        stealScanQueues.add(scanQueue);
      }
      if (numReadQueues > numScanQueues) {
        initializeQueues(numReadQueues - numScanQueues);
      }
      queues.addAll(stealScanQueues);
      if (numScanQueues > numReadQueues) {
        initializeQueues(numScanQueues - numReadQueues);
      }
    } else {
      initializeQueues(Math.max(numReadQueues, numScanQueues));
    }
  }
}
