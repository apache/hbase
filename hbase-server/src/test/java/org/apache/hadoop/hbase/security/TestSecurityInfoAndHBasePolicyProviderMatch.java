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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.security.authorize.Service;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Make sure that all rpc services for master and region server are properly configured in
 * {@link SecurityInfo} and {@link HBasePolicyProvider}.
 */
@Tag(SecurityTests.TAG)
@Tag(SmallTests.TAG)
public class TestSecurityInfoAndHBasePolicyProviderMatch {

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private void assertServiceMatches(RpcServerInterface rpcServer) {
    HBasePolicyProvider provider = new HBasePolicyProvider();
    Set<Class<?>> serviceClasses =
      Stream.of(provider.getServices()).map(Service::getProtocol).collect(Collectors.toSet());
    for (BlockingServiceAndInterface bsai : ((RpcServer) rpcServer).getServices()) {
      assertNotNull(
        SecurityInfo.getInfo(bsai.getBlockingService().getDescriptorForType().getName()),
        "no security info for " + bsai.getBlockingService().getDescriptorForType().getName());
      assertThat(serviceClasses, hasItem(bsai.getServiceInterface()));
    }
  }

  @Test
  public void testMatches() {
    assertServiceMatches(UTIL.getMiniHBaseCluster().getMaster().getRpcServer());
    assertServiceMatches(UTIL.getMiniHBaseCluster().getRegionServer(0).getRpcServer());
  }
}
