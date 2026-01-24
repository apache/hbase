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

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.ipc.RWQueueRpcExecutor;
import org.apache.hadoop.hbase.ipc.RpcExecutor;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ LargeTests.class, ClientTests.class })
public class TestScannersFromClientSide3 extends TestScannersFromClientSide2 {

  @BeforeClass
  public static void setUp() throws Exception {
    String queueType = RpcExecutor.CALL_QUEUE_TYPE_READ_STEAL_CONF_VALUE;
    TEST_UTIL.getConfiguration().set(RpcExecutor.CALL_QUEUE_TYPE_CONF_KEY, queueType);
    TEST_UTIL.getConfiguration().setFloat(RWQueueRpcExecutor.CALL_QUEUE_READ_SHARE_CONF_KEY, 0.5f);
    TEST_UTIL.getConfiguration().setFloat(RWQueueRpcExecutor.CALL_QUEUE_SCAN_SHARE_CONF_KEY, 0.5f);
    TEST_UTIL.startMiniCluster(3);
    byte[][] splitKeys = new byte[8][];
    for (int i = 111; i < 999; i += 111) {
      splitKeys[i / 111 - 1] = Bytes.toBytes(String.format("%03d", i));
    }
    Table table = TEST_UTIL.createTable(TABLE_NAME, FAMILY, splitKeys);
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      puts.add(new Put(Bytes.toBytes(String.format("%03d", i)))
        .addColumn(FAMILY, CQ1, Bytes.toBytes(i)).addColumn(FAMILY, CQ2, Bytes.toBytes(i * i)));
    }
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    table.put(puts);
  }

  @Test
  public void test() throws Exception {
    testScanWithLimit();
    testReversedScanWithLimit();
    testReversedStartRowStopRowInclusive();
    testStartRowStopRowInclusive();
  }
}
