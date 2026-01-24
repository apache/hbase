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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.codec.KeyValueCodec;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.ipc.RWQueueRpcExecutor;
import org.apache.hadoop.hbase.ipc.RpcExecutor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class })
public class TestMultiParallel2 extends TestMultiParallel {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMultiParallel2.class);

  public static void beforeClass() throws Exception {
    // Uncomment the following lines if more verbosity is needed for
    // debugging (see HBASE-12285 for details).
    // ((Log4JLogger)RpcServer.LOG).getLogger().setLevel(Level.ALL);
    // ((Log4JLogger)RpcClient.LOG).getLogger().setLevel(Level.ALL);
    // ((Log4JLogger)ScannerCallable.LOG).getLogger().setLevel(Level.ALL);
    UTIL.getConfiguration().set(HConstants.RPC_CODEC_CONF_KEY,
      KeyValueCodec.class.getCanonicalName());
    // Disable table on master for now as the feature is broken
    // UTIL.getConfiguration().setBoolean(LoadBalancer.TABLES_ON_MASTER, true);
    // We used to ask for system tables on Master exclusively but not needed by test and doesn't
    // work anyways -- so commented out.
    // UTIL.getConfiguration().setBoolean(LoadBalancer.SYSTEM_TABLES_ON_MASTER, true);
    UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      MyMasterObserver.class.getName());
    String queueType = RpcExecutor.CALL_QUEUE_TYPE_READ_STEAL_CONF_VALUE;
    UTIL.getConfiguration().set(RpcExecutor.CALL_QUEUE_TYPE_CONF_KEY, queueType);
    UTIL.getConfiguration().setFloat(RWQueueRpcExecutor.CALL_QUEUE_READ_SHARE_CONF_KEY, 0.5f);
    UTIL.getConfiguration().setFloat(RWQueueRpcExecutor.CALL_QUEUE_SCAN_SHARE_CONF_KEY, 1);
    UTIL.startMiniCluster(slaves);
    Table t = UTIL.createMultiRegionTable(TEST_TABLE, Bytes.toBytes(FAMILY));
    UTIL.waitTableEnabled(TEST_TABLE);
    t.close();
    CONNECTION = ConnectionFactory.createConnection(UTIL.getConfiguration());
    assertTrue(MyMasterObserver.start.get());
  }

  @Test
  public void test() throws Exception {
    testBatchWithGet();
  }
}
