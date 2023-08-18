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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.nio.ByteBuff;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;

public class FailingNettyRpcServer extends NettyRpcServer {

  public FailingNettyRpcServer(Server server, String name,
    List<RpcServer.BlockingServiceAndInterface> services, InetSocketAddress bindAddress,
    Configuration conf, RpcScheduler scheduler) throws IOException {
    super(server, name, services, bindAddress, conf, scheduler, true);
  }

  static final class FailingConnection extends NettyServerRpcConnection {
    private FailingConnection(FailingNettyRpcServer rpcServer, Channel channel) {
      super(rpcServer, channel);
    }

    @Override
    public void processRequest(ByteBuff buf) throws IOException, InterruptedException {
      // this will throw exception after the connection header is read, and an RPC is sent
      // from client
      throw new DoNotRetryIOException("Failing for test");
    }
  }

  @Override
  protected NettyServerRpcConnection createNettyServerRpcConnection(Channel channel) {
    return new FailingConnection(FailingNettyRpcServer.this, channel);
  }
}
