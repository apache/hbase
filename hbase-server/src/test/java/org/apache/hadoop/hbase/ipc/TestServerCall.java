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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;

/**
 * Test for ServerCall IOException handling in setResponse method.
 */
@Category({ RPCTests.class, MediumTests.class })
public class TestServerCall {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestServerCall.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestServerCall.class);

  private Configuration conf;
  private NettyServerRpcConnection mockConnection;
  private RequestHeader header;
  private Message mockParam;
  private ByteBuffAllocator mockAllocator;
  private CellBlockBuilder mockCellBlockBuilder;
  private InetAddress lbAddr;
  private BlockingService mockService;
  private MethodDescriptor mockMethodDescriptor;

  @Before
  public void setUp() throws Exception {
    conf = HBaseConfiguration.create();
    mockConnection = mock(NettyServerRpcConnection.class);
    header = RequestHeader.newBuilder().setCallId(1).setMethodName("testMethod")
      .setRequestParam(true).build();
    mockParam = mock(Message.class);
    mockAllocator = mock(ByteBuffAllocator.class);
    mockCellBlockBuilder = mock(CellBlockBuilder.class);
    lbAddr = InetAddress.getLoopbackAddress();

    mockMethodDescriptor =
      org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService.getDescriptor()
        .getMethods().get(0);

    mockService = mock(BlockingService.class);
    mockConnection.codec = mock(Codec.class);

    when(mockAllocator.isReservoirEnabled()).thenReturn(false);
  }

  /**
   * Test that when IOException occurs during response creation in setResponse, an error response is
   * created and sent to the client instead of leaving the response as null.
   */
  @Test
  public void testSetResponseWithIOException() throws Exception {
    // Create a CellBlockBuilder that throws IOException
    CellBlockBuilder failingCellBlockBuilder = mock(CellBlockBuilder.class);
    doThrow(new IOException("Test IOException during buildCellBlock")).when(failingCellBlockBuilder)
      .buildCellBlock(any(), any(), any());

    // Create NettyServerCall instance
    NettyServerCall call = new NettyServerCall(1, mockService, mockMethodDescriptor, header,
      mockParam, null, mockConnection, 100, lbAddr, System.currentTimeMillis(), 60000,
      mockAllocator, failingCellBlockBuilder, null);

    // Set a successful response, but CellBlockBuilder will fail
    Message mockResponse = mock(Message.class);
    CellScanner mockCellScanner = mock(CellScanner.class);

    LOG.info("Testing setResponse with IOException in buildCellBlock");
    call.setResponse(mockResponse, mockCellScanner, null, null);

    // Verify that response is not null and contains error information
    BufferChain response = call.getResponse();
    assertNotNull("Response should not be null even when IOException occurs", response);
    assertTrue("Call should be marked as error", call.isError);

    // Verify the response buffer is valid
    ByteBuffer[] bufs = response.getBuffers();
    assertNotNull("Response buffers should not be null", bufs);
    assertTrue("Response should have at least one buffer", bufs.length > 0);
  }

  /**
   * Test the case where both normal response creation and error response creation fail with
   * IOException.
   */
  @Test
  public void testSetResponseWithDoubleIOException() throws Exception {

    CellBlockBuilder failingCellBlockBuilder = mock(CellBlockBuilder.class);
    doThrow(new IOException("Test IOException")).when(failingCellBlockBuilder).buildCellBlock(any(),
      any(), any());

    NettyServerCall call = new NettyServerCall(1, mockService, mockMethodDescriptor, header,
      mockParam, null, mockConnection, 100, lbAddr, System.currentTimeMillis(), 60000,
      mockAllocator, failingCellBlockBuilder, null);

    Message mockResponse = mock(Message.class);
    CellScanner mockCellScanner = mock(CellScanner.class);

    // Even if error response creation might fail, the call should still be marked as error
    call.setResponse(mockResponse, mockCellScanner, null, null);
    assertTrue("Call should be marked as error", call.isError);
  }

  /**
   * Test normal response creation to ensure our changes don't break the normal flow.
   */
  @Test
  public void testSetResponseNormalFlow() throws Exception {
    CellBlockBuilder normalCellBlockBuilder = mock(CellBlockBuilder.class);
    when(normalCellBlockBuilder.buildCellBlock(any(), any(), any())).thenReturn(null);

    NettyServerCall call = new NettyServerCall(1, mockService, mockMethodDescriptor, header,
      mockParam, null, mockConnection, 100, lbAddr, System.currentTimeMillis(), 60000,
      mockAllocator, normalCellBlockBuilder, null);

    RPCProtos.CellBlockMeta mockResponse =
      RPCProtos.CellBlockMeta.newBuilder().setLength(0).build();

    LOG.info("Testing normal setResponse flow");
    call.setResponse(mockResponse, null, null, null);

    BufferChain response = call.getResponse();
    assertNotNull("Response should not be null in normal flow", response);
    assertTrue("Call should not be marked as error in normal flow", !call.isError);
  }
}
