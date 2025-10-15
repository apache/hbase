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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test parts of {@link RSRpcServices}
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestRSRpcServices {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRSRpcServices.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRSRpcServices.class);

  /**
   * Simple test of the toString on RegionScannerHolder works. Just creates one and calls #toString
   * on it.
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
      null, null, false, false, clientIpAndPort, userNameTest);
    LOG.info("rsh: {}", rsh);
  }

  /**
   * Test the managedKeysRotateSTK RPC method that is used to rebuild the system key cache
   * on region servers when a system key rotation has occurred.
   */
  @Test
  public void testManagedKeysRotateSTK() throws Exception {
    // Create mocks
    HRegionServer mockServer = mock(HRegionServer.class);
    Configuration conf = HBaseConfiguration.create();
    FileSystem mockFs = mock(FileSystem.class);

    when(mockServer.getConfiguration()).thenReturn(conf);
    when(mockServer.isOnline()).thenReturn(true);
    when(mockServer.isAborted()).thenReturn(false);
    when(mockServer.isStopped()).thenReturn(false);
    when(mockServer.isDataFileSystemOk()).thenReturn(true);
    when(mockServer.getFileSystem()).thenReturn(mockFs);

    // Create RSRpcServices
    RSRpcServices rpcServices = new RSRpcServices(mockServer);

    // Create request
    AdminProtos.ManagedKeysRotateSTKRequest request =
      AdminProtos.ManagedKeysRotateSTKRequest.newBuilder().build();
    RpcController controller = mock(RpcController.class);

    // Call the RPC method
    AdminProtos.ManagedKeysRotateSTKResponse response =
      rpcServices.managedKeysRotateSTK(controller, request);

    // Verify the response
    assertTrue("Response should indicate rotation was successful", response.getRotated());

    // Verify that rebuildSystemKeyCache was called on the server
    verify(mockServer).rebuildSystemKeyCache();

    LOG.info("managedKeysRotateSTK test completed successfully");
  }

  /**
   * Test that managedKeysRotateSTK throws ServiceException when server is not online
   */
  @Test
  public void testManagedKeysRotateSTKWhenServerStopped() throws Exception {
    // Create mocks
    HRegionServer mockServer = mock(HRegionServer.class);
    Configuration conf = HBaseConfiguration.create();
    FileSystem mockFs = mock(FileSystem.class);

    when(mockServer.getConfiguration()).thenReturn(conf);
    when(mockServer.isOnline()).thenReturn(true);
    when(mockServer.isAborted()).thenReturn(false);
    when(mockServer.isStopped()).thenReturn(true); // Server is stopped
    when(mockServer.isDataFileSystemOk()).thenReturn(true);
    when(mockServer.getFileSystem()).thenReturn(mockFs);

    // Create RSRpcServices
    RSRpcServices rpcServices = new RSRpcServices(mockServer);

    // Create request
    AdminProtos.ManagedKeysRotateSTKRequest request =
      AdminProtos.ManagedKeysRotateSTKRequest.newBuilder().build();
    RpcController controller = mock(RpcController.class);

    // Call the RPC method and expect ServiceException
    try {
      rpcServices.managedKeysRotateSTK(controller, request);
      fail("Expected ServiceException when server is stopped");
    } catch (ServiceException e) {
      // Expected
      assertTrue("Exception should mention server stopping",
        e.getCause().getMessage().contains("stopping"));
      LOG.info("Correctly threw ServiceException when server is stopped");
    }
  }

  /**
   * Test that managedKeysRotateSTK throws ServiceException when rebuildSystemKeyCache fails
   */
  @Test
  public void testManagedKeysRotateSTKWhenRebuildFails() throws Exception {
    // Create mocks
    HRegionServer mockServer = mock(HRegionServer.class);
    Configuration conf = HBaseConfiguration.create();
    FileSystem mockFs = mock(FileSystem.class);

    when(mockServer.getConfiguration()).thenReturn(conf);
    when(mockServer.isOnline()).thenReturn(true);
    when(mockServer.isAborted()).thenReturn(false);
    when(mockServer.isStopped()).thenReturn(false);
    when(mockServer.isDataFileSystemOk()).thenReturn(true);
    when(mockServer.getFileSystem()).thenReturn(mockFs);

    // Make rebuildSystemKeyCache throw IOException
    IOException testException = new IOException("Test failure rebuilding cache");
    doThrow(testException).when(mockServer).rebuildSystemKeyCache();

    // Create RSRpcServices
    RSRpcServices rpcServices = new RSRpcServices(mockServer);

    // Create request
    AdminProtos.ManagedKeysRotateSTKRequest request =
      AdminProtos.ManagedKeysRotateSTKRequest.newBuilder().build();
    RpcController controller = mock(RpcController.class);

    // Call the RPC method and expect ServiceException
    try {
      rpcServices.managedKeysRotateSTK(controller, request);
      fail("Expected ServiceException when rebuildSystemKeyCache fails");
    } catch (ServiceException e) {
      // Expected
      assertEquals("Test failure rebuilding cache", e.getCause().getMessage());
      LOG.info("Correctly threw ServiceException when rebuildSystemKeyCache fails");
    }

    // Verify that rebuildSystemKeyCache was called
    verify(mockServer).rebuildSystemKeyCache();
  }
}
