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

import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(RPCTests.TAG)
@Tag(MediumTests.TAG)
public class TestSimpleRpcServer extends AbstractTestRpcServer {

  @SuppressWarnings("deprecation")
  @BeforeAll
  public static void setupClass() throws Exception {
    // Reuse TEST_UTIL if the test already initialized it.
    if (TEST_UTIL == null) {
      TEST_UTIL = new HBaseTestingUtil();
    }
    // Set RPC server impl to SimpleRpcServer
    TEST_UTIL.getConfiguration().set(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY,
      SimpleRpcServer.class.getName());
    TEST_UTIL.startMiniCluster();
  }

  @AfterAll
  public static void tearDownClass() throws Exception {
    if (TEST_UTIL != null) {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testSimpleRpcServer() throws Exception {
    doTest(tableName);
  }
}
