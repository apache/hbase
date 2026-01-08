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

import static org.apache.hadoop.hbase.ipc.RpcExecutor.CALL_QUEUE_HANDLER_FACTOR_CONF_KEY;
import static org.apache.hadoop.hbase.ipc.RpcExecutor.DEFAULT_CALL_QUEUE_HANDLER_FACTOR;
import static org.apache.hadoop.hbase.ipc.RpcExecutor.DEFAULT_CALL_QUEUE_SIZE_HARD_LIMIT;
import static org.apache.hadoop.hbase.ipc.RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(RPCTests.TAG)
@Tag(MediumTests.TAG)
public class TestRpcExecutor {

  private static Configuration conf;

  @BeforeAll
  public static void setUp() {
    conf = HBaseConfiguration.create();
  }

  /**
   * Test that validates default soft and hard limits when maxQueueLength is not explicitly
   * configured (-1).
   */
  @Test
  public void testDefaultQueueLimits(TestInfo testInfo) {
    PriorityFunction qosFunction = mock(PriorityFunction.class);
    int handlerCount = 100;
    // Pass -1 to use default maxQueueLength calculation
    int defaultMaxQueueLength = -1;

    BalancedQueueRpcExecutor executor = new BalancedQueueRpcExecutor(testInfo.getDisplayName(),
      handlerCount, defaultMaxQueueLength, qosFunction, conf, null);

    List<BlockingQueue<CallRunner>> queues = executor.getQueues();
    int expectedQueueSize = Math.round(handlerCount * DEFAULT_CALL_QUEUE_HANDLER_FACTOR);
    Assertions.assertEquals(expectedQueueSize, queues.size(),
      "Number of queues should be according to default callQueueHandlerFactor");

    // By default, the soft limit depends on number of handler the queue will serve
    int expectedSoftLimit =
      (handlerCount / expectedQueueSize) * DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER;
    Assertions.assertEquals(expectedSoftLimit, executor.currentQueueLimit,
      "Soft limit of queues is wrongly calculated");

    // Hard limit should be maximum of softLimit and DEFAULT_CALL_QUEUE_SIZE_HARD_LIMIT
    int hardQueueLimit = queues.get(0).remainingCapacity() + queues.get(0).size();
    int expectedHardLimit = Math.max(expectedSoftLimit, DEFAULT_CALL_QUEUE_SIZE_HARD_LIMIT);
    Assertions.assertEquals(expectedHardLimit, hardQueueLimit,
      "Default hard limit of queues is wrongly calculated ");
  }

  /**
   * Test that validates configured soft and hard limits when maxQueueLength is explicitly set.
   */
  @Test
  public void testConfiguredQueueLimits(TestInfo testInfo) {
    float callQueueHandlerFactor = 0.2f;
    conf.setFloat(CALL_QUEUE_HANDLER_FACTOR_CONF_KEY, callQueueHandlerFactor);
    PriorityFunction qosFunction = mock(PriorityFunction.class);
    int handlerCount = 100;

    // Test Case 1: Configured soft limit < DEFAULT_CALL_QUEUE_SIZE_HARD_LIMIT
    int maxQueueLength = 150;
    BalancedQueueRpcExecutor executor = new BalancedQueueRpcExecutor(
      testInfo.getDisplayName() + "1", handlerCount, maxQueueLength, qosFunction, conf, null);

    Assertions.assertEquals(maxQueueLength, executor.currentQueueLimit,
      "Configured soft limit is not applied.");

    List<BlockingQueue<CallRunner>> queues1 = executor.getQueues();

    int expectedQueueSize = Math.round(handlerCount * callQueueHandlerFactor);
    Assertions.assertEquals(expectedQueueSize, queues1.size(),
      "Number of queues should be according to callQueueHandlerFactor");

    int hardQueueLimit1 = queues1.get(0).remainingCapacity() + queues1.get(0).size();
    Assertions.assertEquals(DEFAULT_CALL_QUEUE_SIZE_HARD_LIMIT, hardQueueLimit1,
      "Default Hard limit is not applied");

  }
}
