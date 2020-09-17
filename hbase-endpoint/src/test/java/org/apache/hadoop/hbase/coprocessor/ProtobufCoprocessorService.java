/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.coprocessor;


import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.util.Threads;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.AddrResponseProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EmptyRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EmptyResponseProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.PauseRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos;

/**
 * Test implementation of a coprocessor endpoint exposing the
 * {@link org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto}
 * service methods. For internal use by unit tests only.
 */
public class ProtobufCoprocessorService extends TestRpcServiceProtos.TestProtobufRpcProto
        implements MasterCoprocessor, RegionCoprocessor {
  public ProtobufCoprocessorService() {}

  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(this);
  }

  @Override
  public void ping(RpcController controller, TestProtos.EmptyRequestProto request,
          RpcCallback<TestProtos.EmptyResponseProto> done) {
    done.run(TestProtos.EmptyResponseProto.getDefaultInstance());
  }

  @Override
  public void echo(RpcController controller, TestProtos.EchoRequestProto request,
          RpcCallback<TestProtos.EchoResponseProto> done) {
    String message = request.getMessage();
    done.run(TestProtos.EchoResponseProto.newBuilder().setMessage(message).build());
  }

  @Override
  public void error(RpcController controller, TestProtos.EmptyRequestProto request,
          RpcCallback<TestProtos.EmptyResponseProto> done) {
    CoprocessorRpcUtils.setControllerException(controller, new IOException("Test exception"));
    done.run(null);
  }

  @Override
  public void pause(RpcController controller, PauseRequestProto request,
          RpcCallback<EmptyResponseProto> done) {
    Threads.sleepWithoutInterrupt(request.getMs());
    done.run(EmptyResponseProto.getDefaultInstance());
  }

  @Override
  public void addr(RpcController controller, EmptyRequestProto request,
          RpcCallback<AddrResponseProto> done) {
    done.run(AddrResponseProto.newBuilder()
        .setAddr(RpcServer.getRemoteAddress().get().getHostAddress()).build());
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    // To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    // To change body of implemented methods use File | Settings | File Templates.
  }
}
