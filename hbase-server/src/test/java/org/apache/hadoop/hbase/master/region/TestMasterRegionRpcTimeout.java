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
package org.apache.hadoop.hbase.master.region;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Make sure that we will not get rpc timeout when updating master region.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestMasterRegionRpcTimeout extends MasterRegionTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterRegionRpcTimeout.class);

  @Test
  public void testRpcTimeout() throws IOException {
    RpcCall call = mock(RpcCall.class);
    // fake a RpcCall which is already timed out
    when(call.getDeadline()).thenReturn(EnvironmentEdgeManager.currentTime() - 10000);
    RpcServer.setCurrentCall(call);
    // make sure that the update operation does not throw timeout exception
    region.update(
      r -> r.put(new Put(Bytes.toBytes("row")).addColumn(CF1, QUALIFIER, Bytes.toBytes("value"))));
    assertSame(call, RpcServer.getCurrentCall().get());
  }
}
