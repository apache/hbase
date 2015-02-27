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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.net.NetUtils;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.MethodDescriptor;

@Category({ SmallTests.class })
public class TestIPC extends AbstractTestIPC {

  private static final Log LOG = LogFactory.getLog(TestIPC.class);

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

  public static void main(String[] args) throws IOException, SecurityException,
      NoSuchMethodException, InterruptedException {
    if (args.length != 2) {
      System.out.println("Usage: TestIPC <CYCLES> <CELLS_PER_CYCLE>");
      return;
    }
    // ((Log4JLogger)HBaseServer.LOG).getLogger().setLevel(Level.INFO);
    // ((Log4JLogger)HBaseClient.LOG).getLogger().setLevel(Level.INFO);
    int cycles = Integer.parseInt(args[0]);
    int cellcount = Integer.parseInt(args[1]);
    Configuration conf = HBaseConfiguration.create();
    TestRpcServer rpcServer = new TestRpcServer();
    MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");
    EchoRequestProto param = EchoRequestProto.newBuilder().setMessage("hello").build();
    RpcClientImpl client = new RpcClientImpl(conf, HConstants.CLUSTER_ID_DEFAULT);
    KeyValue kv = BIG_CELL;
    Put p = new Put(CellUtil.cloneRow(kv));
    for (int i = 0; i < cellcount; i++) {
      p.add(kv);
    }
    RowMutations rm = new RowMutations(CellUtil.cloneRow(kv));
    rm.add(p);
    try {
      rpcServer.start();
      InetSocketAddress address = rpcServer.getListenerAddress();
      long startTime = System.currentTimeMillis();
      User user = User.getCurrent();
      for (int i = 0; i < cycles; i++) {
        List<CellScannable> cells = new ArrayList<CellScannable>();
        // Message param = RequestConverter.buildMultiRequest(HConstants.EMPTY_BYTE_ARRAY, rm);
        ClientProtos.RegionAction.Builder builder =
            RequestConverter.buildNoDataRegionAction(HConstants.EMPTY_BYTE_ARRAY, rm, cells,
              RegionAction.newBuilder(), ClientProtos.Action.newBuilder(),
              MutationProto.newBuilder());
        builder.setRegion(RegionSpecifier
            .newBuilder()
            .setType(RegionSpecifierType.REGION_NAME)
            .setValue(
              ByteString.copyFrom(HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes())));
        if (i % 100000 == 0) {
          LOG.info("" + i);
          // Uncomment this for a thread dump every so often.
          // ReflectionUtils.printThreadInfo(new PrintWriter(System.out),
          // "Thread dump " + Thread.currentThread().getName());
        }
        PayloadCarryingRpcController pcrc =
            new PayloadCarryingRpcController(CellUtil.createCellScanner(cells));
        // Pair<Message, CellScanner> response =
        client.call(pcrc, md, builder.build(), param, user, address);
        /*
         * int count = 0; while (p.getSecond().advance()) { count++; } assertEquals(cells.size(),
         * count);
         */
      }
      LOG.info("Cycled " + cycles + " time(s) with " + cellcount + " cell(s) in "
          + (System.currentTimeMillis() - startTime) + "ms");
    } finally {
      client.close();
      rpcServer.stop();
    }
  }

}
