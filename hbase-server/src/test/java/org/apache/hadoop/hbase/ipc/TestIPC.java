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

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.net.Socket;

import javax.net.SocketFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.net.NetUtils;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@Category({ RPCTests.class, SmallTests.class })
public class TestIPC extends AbstractTestIPC {

  @Override
  protected RpcClientImpl createRpcClientNoCodec(Configuration conf) {
    return new RpcClientImpl(conf, HConstants.CLUSTER_ID_DEFAULT) {
      @Override
      Codec getCodec() {
        return null;
      }
    };
  }

  @Override
  protected RpcClientImpl createRpcClient(Configuration conf) {
    return new RpcClientImpl(conf, HConstants.CLUSTER_ID_DEFAULT);
  }

  @Override
  protected RpcClientImpl createRpcClientRTEDuringConnectionSetup(Configuration conf)
      throws IOException {
    SocketFactory spyFactory = spy(NetUtils.getDefaultSocketFactory(conf));
    Mockito.doAnswer(new Answer<Socket>() {
      @Override
      public Socket answer(InvocationOnMock invocation) throws Throwable {
        Socket s = spy((Socket) invocation.callRealMethod());
        doThrow(new RuntimeException("Injected fault")).when(s).setSoTimeout(anyInt());
        return s;
      }
    }).when(spyFactory).createSocket();

    return new RpcClientImpl(conf, HConstants.CLUSTER_ID_DEFAULT, spyFactory);
  }
}
