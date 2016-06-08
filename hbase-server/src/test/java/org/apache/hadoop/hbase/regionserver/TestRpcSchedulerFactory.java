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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.ipc.SimpleRpcScheduler;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

/**
 * A silly test that does nothing but make sure an rpcscheduler factory makes what it says
 * it is going to make.
 */
@Category(SmallTests.class)
public class TestRpcSchedulerFactory {
  @Rule public TestName testName = new TestName();
  @ClassRule public static TestRule timeout =
      CategoryBasedTimeout.forClass(TestRpcSchedulerFactory.class);
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    this.conf = HBaseConfiguration.create();
  }

  @Test
  public void testRWQ() {
    // Set some configs just to see how it changes the scheduler. Can't assert the settings had
    // an effect. Just eyeball the log.
    this.conf.setDouble(SimpleRpcScheduler.CALL_QUEUE_READ_SHARE_CONF_KEY, 0.5);
    this.conf.setDouble(SimpleRpcScheduler.CALL_QUEUE_HANDLER_FACTOR_CONF_KEY, 0.5);
    this.conf.setDouble(SimpleRpcScheduler.CALL_QUEUE_SCAN_SHARE_CONF_KEY, 0.5);
    RpcSchedulerFactory factory = new SimpleRpcSchedulerFactory();
    RpcScheduler rpcScheduler = factory.create(this.conf, null, null);
    assertTrue(rpcScheduler.getClass().equals(SimpleRpcScheduler.class));
  }

  @Test
  public void testFifo() {
    RpcSchedulerFactory factory = new FifoRpcSchedulerFactory();
    RpcScheduler rpcScheduler = factory.create(this.conf, null, null);
    assertTrue(rpcScheduler.getClass().equals(FifoRpcScheduler.class));
  }
}