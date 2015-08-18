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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.CoordinatedStateManagerFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

/**
 * Tests to verify correct priority on Master RPC methods.
 */
@Category({MasterTests.class, MediumTests.class})
public class TestMasterPriorityRpc {
  private HMaster master = null;
  private PriorityFunction priority = null;
  private User user = null;

  private final Set<String> ADMIN_METHODS = Sets.newHashSet("GetLastFlushedSequenceId");

  private final Set<String> NORMAL_METHODS = Sets.newHashSet("CreateTable", "DeleteTable",
      "ModifyColumn", "OfflineRegion", "Shutdown",
      "RegionServerReport", "RegionServerStartup", "ReportRSFatalError",
      "ReportRegionStateTransition");

  @Before
  public void setup() {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.testing.nocluster", true); // No need to do ZK
    CoordinatedStateManager cp = CoordinatedStateManagerFactory.getCoordinatedStateManager(conf);
    master = HMaster.constructMaster(HMaster.class, conf, cp);
    priority = master.getMasterRpcServices().getPriority();
    user = User.createUserForTesting(conf, "someuser", new String[]{"somegroup"});
  }

  /**
   * Asserts that the provided method has the given priority.
   *
   * @param methodName
   *          The name of the RPC method.
   * @param expectedPriority
   *          The expected priority.
   */
  private void assertPriority(String methodName, int expectedPriority) {
    assertEquals(methodName + " had unexpected priority", expectedPriority, priority.getPriority(
        RequestHeader.newBuilder().setMethodName(methodName).build(), null, user));
  }

  @Test
  public void testNullMessage() {
    assertPriority("doesnotexist", HConstants.NORMAL_QOS);
  }

  @Test
  public void testAdminPriorityMethods() {
    for (String methodName : ADMIN_METHODS) {
      assertPriority(methodName, HConstants.ADMIN_QOS);
    }
  }

  @Test
  public void testSomeNormalMethods() {
    for (String methodName : NORMAL_METHODS) {
      assertPriority(methodName, HConstants.NORMAL_QOS);
    }
  }
}
