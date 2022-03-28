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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test parts of {@link RSRpcServices}
 */
@Category({ RegionServerTests.class, MediumTests.class})
public class TestRSRpcServices {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRSRpcServices.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRSRpcServices.class);

  /**
   * Simple test of the toString on RegionScannerHolder works.
   * Just creates one and calls #toString on it.
   */
  @Test
  public void testRegionScannerHolderToString() throws UnknownHostException {
    RpcCall call = Mockito.mock(RpcCall.class);
    int port = 1234;
    Mockito.when(call.getRemotePort()).thenReturn(port);
    InetAddress address = InetAddress.getLocalHost();
    Mockito.when(call.getRemoteAddress()).thenReturn(address);
    Optional<String> userName = Optional.ofNullable("test");
    Mockito.when(call.getRequestUserName()).thenReturn(userName);
    RpcServer.setCurrentCall(call);
    String clientIpAndPort = RSRpcServices.getRemoteClientIpAndPort();
    String userNameTest = RSRpcServices.getUserName();
    assertEquals("test", userNameTest);
    HRegion region = Mockito.mock(HRegion.class);
    Mockito.when(region.getRegionInfo()).thenReturn(RegionInfoBuilder.FIRST_META_REGIONINFO);
    RSRpcServices.RegionScannerHolder rsh = new RSRpcServices.RegionScannerHolder(null, region,
      null, null, false, false, clientIpAndPort,
      userNameTest);
    LOG.info("rsh: {}", rsh);
  }
}
