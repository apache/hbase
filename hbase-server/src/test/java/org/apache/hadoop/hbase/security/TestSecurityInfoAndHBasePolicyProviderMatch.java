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
package org.apache.hadoop.hbase.security;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertNotNull;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.security.authorize.Service;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Make sure that all rpc services for master and region server are properly configured in
 * {@link SecurityInfo} and {@link HBasePolicyProvider}.
 */
@Category({ SecurityTests.class, SmallTests.class })
public class TestSecurityInfoAndHBasePolicyProviderMatch {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSecurityInfoAndHBasePolicyProviderMatch.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private void assertServiceMatches(RpcServerInterface rpcServer) {
    HBasePolicyProvider provider = new HBasePolicyProvider();
    Set<Class<?>> serviceClasses =
      Stream.of(provider.getServices()).map(Service::getProtocol).collect(Collectors.toSet());
    for (BlockingServiceAndInterface bsai : ((RpcServer) rpcServer).getServices()) {
      assertNotNull(
        "no security info for " + bsai.getBlockingService().getDescriptorForType().getName(),
        SecurityInfo.getInfo(bsai.getBlockingService().getDescriptorForType().getName()));
      assertThat(serviceClasses, hasItem(bsai.getServiceInterface()));
    }
  }

  @Test
  public void testMatches() {
    assertServiceMatches(
      UTIL.getMiniHBaseCluster().getMaster().getMasterRpcServices().getRpcServer());
    assertServiceMatches(UTIL.getMiniHBaseCluster().getRegionServer(0).getRpcServer());
  }
}
