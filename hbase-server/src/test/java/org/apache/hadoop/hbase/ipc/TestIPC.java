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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoResponseProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EmptyRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EmptyResponseProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.BlockingService;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Some basic ipc tests.
 */
@Category(SmallTests.class)
public class TestIPC {
  public static final Log LOG = LogFactory.getLog(TestIPC.class);
  static byte [] CELL_BYTES =  Bytes.toBytes("xyz");
  static Cell CELL = new KeyValue(CELL_BYTES, CELL_BYTES, CELL_BYTES, CELL_BYTES);
  static byte [] BIG_CELL_BYTES = new byte [10 * 1024];
  static Cell BIG_CELL = new KeyValue(CELL_BYTES, CELL_BYTES, CELL_BYTES, BIG_CELL_BYTES);
  private final static Configuration CONF = HBaseConfiguration.create();
  // We are using the test TestRpcServiceProtos generated classes and Service because they are
  // available and basic with methods like 'echo', and ping.  Below we make a blocking service
  // by passing in implementation of blocking interface.  We use this service in all tests that
  // follow.
  private static final BlockingService SERVICE =
   TestRpcServiceProtos.TestProtobufRpcProto.newReflectiveBlockingService(
     new TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface() {

    @Override
    public EmptyResponseProto ping(RpcController controller,
        EmptyRequestProto request) throws ServiceException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public EmptyResponseProto error(RpcController controller,
        EmptyRequestProto request) throws ServiceException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public EchoResponseProto echo(RpcController controller, EchoRequestProto request)
    throws ServiceException {
      if (controller instanceof PayloadCarryingRpcController) {
        PayloadCarryingRpcController pcrc = (PayloadCarryingRpcController)controller;
        // If cells, scan them to check we are able to iterate what we were given and since this is
        // an echo, just put them back on the controller creating a new block.  Tests our block
        // building.
        CellScanner cellScanner = pcrc.cellScanner();
        List<Cell> list = null;
        if (cellScanner != null) {
          list = new ArrayList<Cell>();
          try {
            while(cellScanner.advance()) {
              list.add(cellScanner.current());
            }
          } catch (IOException e) {
            throw new ServiceException(e);
          }
        }
        cellScanner = CellUtil.createCellScanner(list);
        ((PayloadCarryingRpcController)controller).setCellScanner(cellScanner);
      }
      return EchoResponseProto.newBuilder().setMessage(request.getMessage()).build();
    }
  });

  /**
   * Instance of server.  We actually don't do anything speical in here so could just use
   * HBaseRpcServer directly.
   */
  private static class TestRpcServer extends RpcServer {

    TestRpcServer() throws IOException {
      this(new FifoRpcScheduler(CONF, 1));
    }

    TestRpcServer(RpcScheduler scheduler) throws IOException {
      super(null, "testRpcServer",
          Lists.newArrayList(new BlockingServiceAndInterface(SERVICE, null)),
        new InetSocketAddress("localhost", 0), CONF, scheduler);
    }

    @Override
    public Pair<Message, CellScanner> call(BlockingService service,
        MethodDescriptor md, Message param, CellScanner cellScanner,
        long receiveTime, MonitoredRPCHandler status) throws IOException {
      return super.call(service, md, param, cellScanner, receiveTime, status);
    }
  }

  /**
   * Ensure we do not HAVE TO HAVE a codec.
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void testNoCodec() throws InterruptedException, IOException {
    Configuration conf = HBaseConfiguration.create();
    RpcClient client = new RpcClient(conf, HConstants.CLUSTER_ID_DEFAULT) {
      @Override
      Codec getCodec() {
        return null;
      }
    };
    TestRpcServer rpcServer = new TestRpcServer();
    try {
      rpcServer.start();
      InetSocketAddress address = rpcServer.getListenerAddress();
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");
      final String message = "hello";
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage(message).build();
      Pair<Message, CellScanner> r = client.call(md, param, null,
        md.getOutputType().toProto(), User.getCurrent(), address, 0);
      assertTrue(r.getSecond() == null);
      // Silly assertion that the message is in the returned pb.
      assertTrue(r.getFirst().toString().contains(message));
    } finally {
      client.stop();
      rpcServer.stop();
    }
  }

  /**
   * It is hard to verify the compression is actually happening under the wraps.  Hope that if
   * unsupported, we'll get an exception out of some time (meantime, have to trace it manually
   * to confirm that compression is happening down in the client and server).
   * @throws IOException
   * @throws InterruptedException
   * @throws SecurityException
   * @throws NoSuchMethodException
   */
  @Test
  public void testCompressCellBlock()
  throws IOException, InterruptedException, SecurityException, NoSuchMethodException {
    Configuration conf = new Configuration(HBaseConfiguration.create());
    conf.set("hbase.client.rpc.compressor", GzipCodec.class.getCanonicalName());
    doSimpleTest(conf, new RpcClient(conf, HConstants.CLUSTER_ID_DEFAULT));
  }

  private void doSimpleTest(final Configuration conf, final RpcClient client)
  throws InterruptedException, IOException {
    TestRpcServer rpcServer = new TestRpcServer();
    List<Cell> cells = new ArrayList<Cell>();
    int count = 3;
    for (int i = 0; i < count; i++) cells.add(CELL);
    try {
      rpcServer.start();
      InetSocketAddress address = rpcServer.getListenerAddress();
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage("hello").build();
      Pair<Message, CellScanner> r = client.call(md, param, CellUtil.createCellScanner(cells),
        md.getOutputType().toProto(), User.getCurrent(), address, 0);
      int index = 0;
      while (r.getSecond().advance()) {
        assertTrue(CELL.equals(r.getSecond().current()));
        index++;
      }
      assertEquals(count, index);
    } finally {
      client.stop();
      rpcServer.stop();
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
    RpcClient client = new RpcClient(conf, HConstants.CLUSTER_ID_DEFAULT, spyFactory);
    try {
      rpcServer.start();
      InetSocketAddress address = rpcServer.getListenerAddress();
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage("hello").build();
      client.call(md, param, null, null, User.getCurrent(), address, 0);
      fail("Expected an exception to have been thrown!");
    } catch (Exception e) {
      LOG.info("Caught expected exception: " + e.toString());
      assertTrue(StringUtils.stringifyException(e).contains("Injected fault"));
    } finally {
      client.stop();
      rpcServer.stop();
    }
  }

  /** Tests that the rpc scheduler is called when requests arrive. */
  @Test
  public void testRpcScheduler() throws IOException, InterruptedException {
    RpcScheduler scheduler = spy(new FifoRpcScheduler(CONF, 1));
    RpcServer rpcServer = new TestRpcServer(scheduler);
    verify(scheduler).init((RpcScheduler.Context) anyObject());
    RpcClient client = new RpcClient(CONF, HConstants.CLUSTER_ID_DEFAULT);
    try {
      rpcServer.start();
      verify(scheduler).start();
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage("hello").build();
      for (int i = 0; i < 10; i++) {
        client.call(md, param, CellUtil.createCellScanner(ImmutableList.of(CELL)),
            md.getOutputType().toProto(), User.getCurrent(), rpcServer.getListenerAddress(), 0);
      }
      verify(scheduler, times(10)).dispatch((CallRunner) anyObject());
    } finally {
      rpcServer.stop();
      verify(scheduler).stop();
    }
  }

  public static void main(String[] args)
  throws IOException, SecurityException, NoSuchMethodException, InterruptedException {
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
    RpcClient client = new RpcClient(conf, HConstants.CLUSTER_ID_DEFAULT);
    KeyValue kv = KeyValueUtil.ensureKeyValue(BIG_CELL);
    Put p = new Put(kv.getRow());
    for (int i = 0; i < cellcount; i++) {
      p.add(kv);
    }
    RowMutations rm = new RowMutations(kv.getRow());
    rm.add(p);
    try {
      rpcServer.start();
      InetSocketAddress address = rpcServer.getListenerAddress();
      long startTime = System.currentTimeMillis();
      User user = User.getCurrent();
      for (int i = 0; i < cycles; i++) {
        List<CellScannable> cells = new ArrayList<CellScannable>();
        // Message param = RequestConverter.buildMultiRequest(HConstants.EMPTY_BYTE_ARRAY, rm);
        ClientProtos.RegionAction.Builder builder = RequestConverter.buildNoDataRegionAction(
          HConstants.EMPTY_BYTE_ARRAY, rm, cells,
          RegionAction.newBuilder(),
          ClientProtos.Action.newBuilder(),
          MutationProto.newBuilder());
        builder.setRegion(RegionSpecifier.newBuilder().setType(RegionSpecifierType.REGION_NAME).
          setValue(ByteString.copyFrom(HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes())));
        if (i % 100000 == 0) {
          LOG.info("" + i);
          // Uncomment this for a thread dump every so often.
          // ReflectionUtils.printThreadInfo(new PrintWriter(System.out),
          //  "Thread dump " + Thread.currentThread().getName());
        }
        CellScanner cellScanner = CellUtil.createCellScanner(cells);
        Pair<Message, CellScanner> response =
          client.call(md, builder.build(), cellScanner, param, user, address, 0);
        /*
        int count = 0;
        while (p.getSecond().advance()) {
          count++;
        }
        assertEquals(cells.size(), count);*/
      }
      LOG.info("Cycled " + cycles + " time(s) with " + cellcount + " cell(s) in " +
         (System.currentTimeMillis() - startTime) + "ms");
    } finally {
      client.stop();
      rpcServer.stop();
    }
  }
}
