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
package org.apache.hadoop.hbase.ipc;

import com.google.protobuf.BlockingService;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.AddrResponseProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoResponseProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EmptyRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EmptyResponseProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.PauseRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.Interface;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Threads;

@InterfaceAudience.Private
public class TestProtobufRpcServiceImpl implements BlockingInterface {

  public static final BlockingService SERVICE = TestProtobufRpcProto
      .newReflectiveBlockingService(new TestProtobufRpcServiceImpl());

  public static BlockingInterface newBlockingStub(RpcClient client, InetSocketAddress addr)
      throws IOException {
    return newBlockingStub(client, addr, User.getCurrent());
  }

  public static BlockingInterface newBlockingStub(RpcClient client, InetSocketAddress addr,
      User user) throws IOException {
    return TestProtobufRpcProto.newBlockingStub(client.createBlockingRpcChannel(
      ServerName.valueOf(addr.getHostName(), addr.getPort(), System.currentTimeMillis()), user, 0));
  }

  public static Interface newStub(RpcClient client, InetSocketAddress addr) throws IOException {
    return TestProtobufRpcProto.newStub(client.createRpcChannel(
      ServerName.valueOf(addr.getHostName(), addr.getPort(), System.currentTimeMillis()),
      User.getCurrent(), 0));
  }

  @Override
  public EmptyResponseProto ping(RpcController controller, EmptyRequestProto request)
      throws ServiceException {
    return EmptyResponseProto.getDefaultInstance();
  }

  @Override
  public EchoResponseProto echo(RpcController controller, EchoRequestProto request)
      throws ServiceException {
    if (controller instanceof HBaseRpcController) {
      HBaseRpcController pcrc = (HBaseRpcController) controller;
      // If cells, scan them to check we are able to iterate what we were given and since this is an
      // echo, just put them back on the controller creating a new block. Tests our block building.
      CellScanner cellScanner = pcrc.cellScanner();
      List<Cell> list = null;
      if (cellScanner != null) {
        list = new ArrayList<>();
        try {
          while (cellScanner.advance()) {
            list.add(cellScanner.current());
          }
        } catch (IOException e) {
          throw new ServiceException(e);
        }
      }
      cellScanner = CellUtil.createCellScanner(list);
      pcrc.setCellScanner(cellScanner);
    }
    return EchoResponseProto.newBuilder().setMessage(request.getMessage()).build();
  }

  @Override
  public EmptyResponseProto error(RpcController controller, EmptyRequestProto request)
      throws ServiceException {
    throw new ServiceException(new DoNotRetryIOException("server error!"));
  }

  @Override
  public EmptyResponseProto pause(RpcController controller, PauseRequestProto request)
      throws ServiceException {
    Threads.sleepWithoutInterrupt(request.getMs());
    return EmptyResponseProto.getDefaultInstance();
  }

  @Override
  public AddrResponseProto addr(RpcController controller, EmptyRequestProto request)
      throws ServiceException {
    return AddrResponseProto.newBuilder().setAddr(RpcServer.getRemoteAddress().getHostAddress())
        .build();
  }

}
