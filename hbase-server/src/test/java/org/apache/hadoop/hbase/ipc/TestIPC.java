/**
  *
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

import java.io.IOException;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import javax.net.SocketFactory;
import java.lang.reflect.Method;
import java.util.*;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.mockito.Mockito.*;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.SmallTests;

import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RpcRequestBody;
import com.google.protobuf.Message;
import org.apache.hadoop.hbase.security.User;

import org.apache.commons.logging.*;
import org.apache.log4j.Logger;

@Category(SmallTests.class)
public class TestIPC {
  public static final Log LOG = LogFactory.getLog(TestIPC.class);
  private static final Random RANDOM = new Random();

  private static class TestRpcServer extends HBaseServer {
    TestRpcServer() throws IOException {
      super("0.0.0.0", 0, 1, 1, HBaseConfiguration.create(), "TestRpcServer", 0);
    }

    @Override
    public Message call(Class<? extends VersionedProtocol> protocol,
        RpcRequestBody rpcRequest, 
        long receiveTime, 
        MonitoredRPCHandler status) throws IOException {
      return rpcRequest;
    }
  }

  @Test
  public void testRTEDuringConnectionSetup() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    SocketFactory spyFactory = spy(NetUtils.getDefaultSocketFactory(conf));
    Mockito.doAnswer(new Answer<Socket>() {
      @Override
      public Socket answer(InvocationOnMock invocation) throws Throwable {
        Socket s = spy((Socket)invocation.callRealMethod());
        doThrow(new RuntimeException("Injected fault")).when(s).setSoTimeout(anyInt());
        return s;
      }
    }).when(spyFactory).createSocket();

    TestRpcServer rpcServer = new TestRpcServer();
    rpcServer.start();

    HBaseClient client = new HBaseClient(
        conf,
        spyFactory);
    InetSocketAddress address = rpcServer.getListenerAddress();

    try {
      client.call(RpcRequestBody.getDefaultInstance(), address, User.getCurrent(), 0);
      fail("Expected an exception to have been thrown!");
    } catch (Exception e) {
      LOG.info("Caught expected exception: " + e.toString());
      assertTrue(StringUtils.stringifyException(e).contains("Injected fault"));
    }
  }
}
