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

import static org.apache.hadoop.hbase.ipc.RWQueueRpcExecutor.CALL_QUEUE_READ_SHARE_CONF_KEY;
import static org.apache.hadoop.hbase.ipc.RWQueueRpcExecutor.CALL_QUEUE_SCAN_SHARE_CONF_KEY;
import static org.apache.hadoop.hbase.ipc.RpcExecutor.CALL_QUEUE_HANDLER_FACTOR_CONF_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ RPCTests.class, MediumTests.class})
public class TestRWQueueRpcExecutor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRWQueueRpcExecutor.class);

  @Rule
  public TestName testName = new TestName();

  private Configuration conf;

  @Before
  public void setUp() {
    conf = HBaseConfiguration.create();
    conf.setFloat(CALL_QUEUE_HANDLER_FACTOR_CONF_KEY, 1.0f);
    conf.setFloat(CALL_QUEUE_SCAN_SHARE_CONF_KEY, 0.5f);
    conf.setFloat(CALL_QUEUE_READ_SHARE_CONF_KEY, 0.5f);
  }

  @Test
  public void itProvidesCorrectQueuesToBalancers() throws InterruptedException {
    PriorityFunction qosFunction = mock(PriorityFunction.class);
    RWQueueRpcExecutor executor =
      new RWQueueRpcExecutor(testName.getMethodName(), 100, 100, qosFunction, conf, null);

    QueueBalancer readBalancer = executor.getReadBalancer();
    QueueBalancer writeBalancer = executor.getWriteBalancer();
    QueueBalancer scanBalancer = executor.getScanBalancer();

    assertTrue(readBalancer instanceof RandomQueueBalancer);
    assertTrue(writeBalancer instanceof RandomQueueBalancer);
    assertTrue(scanBalancer instanceof RandomQueueBalancer);

    List<BlockingQueue<CallRunner>> readQueues = ((RandomQueueBalancer) readBalancer).getQueues();
    List<BlockingQueue<CallRunner>> writeQueues = ((RandomQueueBalancer) writeBalancer).getQueues();
    List<BlockingQueue<CallRunner>> scanQueues = ((RandomQueueBalancer) scanBalancer).getQueues();

    assertEquals(25, readQueues.size());
    assertEquals(50, writeQueues.size());
    assertEquals(25, scanQueues.size());

    verifyDistinct(readQueues, writeQueues, scanQueues);
    verifyDistinct(writeQueues, readQueues, scanQueues);
    verifyDistinct(scanQueues, readQueues, writeQueues);

  }

  private void verifyDistinct(List<BlockingQueue<CallRunner>> queues, List<BlockingQueue<CallRunner>>... others)
    throws InterruptedException {
    CallRunner mock = mock(CallRunner.class);
    for (BlockingQueue<CallRunner> queue : queues) {
      queue.put(mock);
      assertEquals(1, queue.size());
    }

    for (List<BlockingQueue<CallRunner>> other : others) {
      for (BlockingQueue<CallRunner> queue : other) {
        assertEquals(0, queue.size());
      }
    }

    // clear them for next test
    for (BlockingQueue<CallRunner> queue : queues) {
      queue.clear();
    }
  }
}
