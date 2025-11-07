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

import static org.apache.hadoop.hbase.client.trace.hamcrest.AttributesMatchers.containsEntry;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasAttributes;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasDuration;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasKind;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasName;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasStatusWithCode;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasTraceId;
import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.SERVICE;
import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.newBlockingStub;
import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.newStub;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.junit4.OpenTelemetryRule;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseServerBase;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MatcherPredicate;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoResponseProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EmptyRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EmptyResponseProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.PauseRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.Interface;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.ConnectionRegistryService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetConnectionRegistryRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetConnectionRegistryResponse;

/**
 * Some basic ipc tests.
 */
public abstract class AbstractTestIPC {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTestIPC.class);

  private static final byte[] CELL_BYTES = Bytes.toBytes("xyz");
  private static final KeyValue CELL = new KeyValue(CELL_BYTES, CELL_BYTES, CELL_BYTES, CELL_BYTES);

  protected static final Configuration CONF = HBaseConfiguration.create();

  protected RpcServer createRpcServer(Server server, String name,
    List<BlockingServiceAndInterface> services, InetSocketAddress bindAddress, Configuration conf,
    RpcScheduler scheduler) throws IOException {
    return RpcServerFactory.createRpcServer(server, name, services, bindAddress, conf, scheduler);
  }

  private RpcServer createRpcServer(String name, List<BlockingServiceAndInterface> services,
    InetSocketAddress bindAddress, Configuration conf, RpcScheduler scheduler) throws IOException {
    return createRpcServer(null, name, services, bindAddress, conf, scheduler);
  }

  protected abstract AbstractRpcClient<?> createRpcClientNoCodec(Configuration conf);

  @Rule
  public OpenTelemetryRule traceRule = OpenTelemetryRule.create();

  @Parameter(0)
  public Class<? extends RpcServer> rpcServerImpl;

  @Before
  public void setUpBeforeTest() {
    CONF.setClass(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY, rpcServerImpl, RpcServer.class);
  }

  /**
   * Ensure we do not HAVE TO HAVE a codec.
   */
  @Test
  public void testNoCodec() throws IOException, ServiceException {
    Configuration clientConf = new Configuration(CONF);
    RpcServer rpcServer = createRpcServer("testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try (AbstractRpcClient<?> client = createRpcClientNoCodec(clientConf)) {
      rpcServer.start();
      BlockingInterface stub = newBlockingStub(client, rpcServer.getListenerAddress());
      HBaseRpcController pcrc = new HBaseRpcControllerImpl();
      String message = "hello";
      assertEquals(message,
        stub.echo(pcrc, EchoRequestProto.newBuilder().setMessage(message).build()).getMessage());
      assertNull(pcrc.cellScanner());
    } finally {
      rpcServer.stop();
    }
  }

  protected abstract AbstractRpcClient<?> createRpcClient(Configuration conf);

  /**
   * It is hard to verify the compression is actually happening under the wraps. Hope that if
   * unsupported, we'll get an exception out of some time (meantime, have to trace it manually to
   * confirm that compression is happening down in the client and server).
   */
  @Test
  public void testCompressCellBlock() throws IOException, ServiceException {
    Configuration clientConf = new Configuration(CONF);
    clientConf.set("hbase.client.rpc.compressor", GzipCodec.class.getCanonicalName());
    List<ExtendedCell> cells = new ArrayList<>();
    int count = 3;
    for (int i = 0; i < count; i++) {
      cells.add(CELL);
    }
    RpcServer rpcServer = createRpcServer("testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));

    try (AbstractRpcClient<?> client = createRpcClient(clientConf)) {
      rpcServer.start();
      BlockingInterface stub = newBlockingStub(client, rpcServer.getListenerAddress());
      HBaseRpcController pcrc =
        new HBaseRpcControllerImpl(PrivateCellUtil.createExtendedCellScanner(cells));
      String message = "hello";
      assertEquals(message,
        stub.echo(pcrc, EchoRequestProto.newBuilder().setMessage(message).build()).getMessage());
      int index = 0;
      CellScanner cellScanner = pcrc.cellScanner();
      assertNotNull(cellScanner);
      while (cellScanner.advance()) {
        assertEquals(CELL, cellScanner.current());
        index++;
      }
      assertEquals(count, index);
    } finally {
      rpcServer.stop();
    }
  }

  protected abstract AbstractRpcClient<?>
    createRpcClientRTEDuringConnectionSetup(Configuration conf) throws IOException;

  @Test
  public void testRTEDuringConnectionSetup() throws Exception {
    Configuration clientConf = new Configuration(CONF);
    RpcServer rpcServer = createRpcServer("testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try (AbstractRpcClient<?> client = createRpcClientRTEDuringConnectionSetup(clientConf)) {
      rpcServer.start();
      BlockingInterface stub = newBlockingStub(client, rpcServer.getListenerAddress());
      stub.ping(null, EmptyRequestProto.getDefaultInstance());
      fail("Expected an exception to have been thrown!");
    } catch (Exception e) {
      LOG.info("Caught expected exception: " + e.toString());
      assertTrue(e.toString(), StringUtils.stringifyException(e).contains("Injected fault"));
    } finally {
      rpcServer.stop();
    }
  }

  /**
   * Tests that the rpc scheduler is called when requests arrive.
   */
  @Test
  public void testRpcScheduler() throws IOException, ServiceException, InterruptedException {
    Configuration clientConf = new Configuration(CONF);
    RpcScheduler scheduler = spy(new FifoRpcScheduler(CONF, 1));
    RpcServer rpcServer = createRpcServer("testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, scheduler);
    verify(scheduler).init(any(RpcScheduler.Context.class));
    try (AbstractRpcClient<?> client = createRpcClient(clientConf)) {
      rpcServer.start();
      verify(scheduler).start();
      BlockingInterface stub = newBlockingStub(client, rpcServer.getListenerAddress());
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage("hello").build();
      for (int i = 0; i < 10; i++) {
        stub.echo(null, param);
      }
      verify(scheduler, times(10)).dispatch(any(CallRunner.class));
    } finally {
      rpcServer.stop();
      verify(scheduler).stop();
    }
  }

  /** Tests that the rpc scheduler is called when requests arrive. */
  @Test
  public void testRpcMaxRequestSize() throws IOException, ServiceException {
    Configuration clientConf = new Configuration(CONF);
    clientConf.setInt(RpcServer.MAX_REQUEST_SIZE, 1000);
    RpcServer rpcServer = createRpcServer("testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), clientConf, new FifoRpcScheduler(clientConf, 1));
    try (AbstractRpcClient<?> client = createRpcClient(clientConf)) {
      rpcServer.start();
      BlockingInterface stub = newBlockingStub(client, rpcServer.getListenerAddress());
      StringBuilder message = new StringBuilder(1200);
      for (int i = 0; i < 200; i++) {
        message.append("hello.");
      }
      // set total RPC size bigger than 100 bytes
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage(message.toString()).build();
      stub.echo(new HBaseRpcControllerImpl(
        PrivateCellUtil.createExtendedCellScanner(ImmutableList.<ExtendedCell> of(CELL))), param);
      fail("RPC should have failed because it exceeds max request size");
    } catch (ServiceException e) {
      LOG.info("Caught expected exception: " + e);
      assertTrue(e.toString(),
        StringUtils.stringifyException(e).contains("RequestTooBigException"));
    } finally {
      rpcServer.stop();
    }
  }

  /**
   * Tests that the RpcServer creates & dispatches CallRunner object to scheduler with non-null
   * remoteAddress set to its Call Object
   */
  @Test
  public void testRpcServerForNotNullRemoteAddressInCallObject()
    throws IOException, ServiceException {
    Configuration clientConf = new Configuration(CONF);
    RpcServer rpcServer = createRpcServer("testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    InetSocketAddress localAddr = new InetSocketAddress("localhost", 0);
    try (AbstractRpcClient<?> client = createRpcClient(clientConf)) {
      rpcServer.start();
      BlockingInterface stub = newBlockingStub(client, rpcServer.getListenerAddress());
      assertEquals(localAddr.getAddress().getHostAddress(),
        stub.addr(null, EmptyRequestProto.getDefaultInstance()).getAddr());
    } finally {
      rpcServer.stop();
    }
  }

  @Test
  public void testRemoteError() throws IOException, ServiceException {
    Configuration clientConf = new Configuration(CONF);
    RpcServer rpcServer = createRpcServer("testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try (AbstractRpcClient<?> client = createRpcClient(clientConf)) {
      rpcServer.start();
      BlockingInterface stub = newBlockingStub(client, rpcServer.getListenerAddress());
      stub.error(null, EmptyRequestProto.getDefaultInstance());
    } catch (ServiceException e) {
      LOG.info("Caught expected exception: " + e);
      IOException ioe = ProtobufUtil.handleRemoteException(e);
      assertTrue(ioe instanceof DoNotRetryIOException);
      assertTrue(ioe.getMessage().contains("server error!"));
    } finally {
      rpcServer.stop();
    }
  }

  @Test
  public void testTimeout() throws IOException {
    Configuration clientConf = new Configuration(CONF);
    RpcServer rpcServer = createRpcServer("testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try (AbstractRpcClient<?> client = createRpcClient(clientConf)) {
      rpcServer.start();
      BlockingInterface stub = newBlockingStub(client, rpcServer.getListenerAddress());
      HBaseRpcController pcrc = new HBaseRpcControllerImpl();
      int ms = 1000;
      int timeout = 100;
      for (int i = 0; i < 10; i++) {
        pcrc.reset();
        pcrc.setCallTimeout(timeout);
        long startTime = System.nanoTime();
        try {
          stub.pause(pcrc, PauseRequestProto.newBuilder().setMs(ms).build());
        } catch (ServiceException e) {
          long waitTime = (System.nanoTime() - startTime) / 1000000;
          // expected
          LOG.info("Caught expected exception: " + e);
          IOException ioe = ProtobufUtil.handleRemoteException(e);
          assertTrue(ioe.getCause() instanceof CallTimeoutException);
          // confirm that we got exception before the actual pause.
          assertTrue(waitTime < ms);
        }
      }
    } finally {
      rpcServer.stop();
    }
  }

  private static class FailingSimpleRpcServer extends SimpleRpcServer {

    FailingSimpleRpcServer(Server server, String name,
      List<RpcServer.BlockingServiceAndInterface> services, InetSocketAddress bindAddress,
      Configuration conf, RpcScheduler scheduler) throws IOException {
      super(server, name, services, bindAddress, conf, scheduler, true);
    }

    final class FailingConnection extends SimpleServerRpcConnection {
      private FailingConnection(FailingSimpleRpcServer rpcServer, SocketChannel channel,
        long lastContact) {
        super(rpcServer, channel, lastContact);
      }

      @Override
      public void processRequest(ByteBuff buf) throws IOException, InterruptedException {
        // this will throw exception after the connection header is read, and an RPC is sent
        // from client
        throw new DoNotRetryIOException("Failing for test");
      }
    }

    @Override
    protected SimpleServerRpcConnection getConnection(SocketChannel channel, long time) {
      return new FailingConnection(this, channel, time);
    }
  }

  protected RpcServer createTestFailingRpcServer(final String name,
    final List<BlockingServiceAndInterface> services, final InetSocketAddress bindAddress,
    Configuration conf, RpcScheduler scheduler) throws IOException {
    if (rpcServerImpl.equals(NettyRpcServer.class)) {
      return new FailingNettyRpcServer(null, name, services, bindAddress, conf, scheduler);
    } else {
      return new FailingSimpleRpcServer(null, name, services, bindAddress, conf, scheduler);
    }
  }

  /** Tests that the connection closing is handled by the client with outstanding RPC calls */
  @Test
  public void testConnectionCloseWithOutstandingRPCs() throws InterruptedException, IOException {
    Configuration clientConf = new Configuration(CONF);
    RpcServer rpcServer = createTestFailingRpcServer("testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));

    try (AbstractRpcClient<?> client = createRpcClient(clientConf)) {
      rpcServer.start();
      BlockingInterface stub = newBlockingStub(client, rpcServer.getListenerAddress());
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage("hello").build();
      stub.echo(null, param);
      fail("RPC should have failed because connection closed");
    } catch (ServiceException e) {
      LOG.info("Caught expected exception: " + e.toString());
    } finally {
      rpcServer.stop();
    }
  }

  @Test
  public void testAsyncEcho() throws IOException {
    Configuration clientConf = new Configuration(CONF);
    RpcServer rpcServer = createRpcServer("testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try (AbstractRpcClient<?> client = createRpcClient(clientConf)) {
      rpcServer.start();
      Interface stub = newStub(client, rpcServer.getListenerAddress());
      int num = 10;
      List<HBaseRpcController> pcrcList = new ArrayList<>();
      List<BlockingRpcCallback<EchoResponseProto>> callbackList = new ArrayList<>();
      for (int i = 0; i < num; i++) {
        HBaseRpcController pcrc = new HBaseRpcControllerImpl();
        BlockingRpcCallback<EchoResponseProto> done = new BlockingRpcCallback<>();
        stub.echo(pcrc, EchoRequestProto.newBuilder().setMessage("hello-" + i).build(), done);
        pcrcList.add(pcrc);
        callbackList.add(done);
      }
      for (int i = 0; i < num; i++) {
        EchoResponseProto resp = callbackList.get(i).get();
        HBaseRpcController pcrc = pcrcList.get(i);
        assertEquals("hello-" + i, resp.getMessage());
        assertFalse(pcrc.failed());
        assertNull(pcrc.cellScanner());
      }
    } finally {
      rpcServer.stop();
    }
  }

  @Test
  public void testAsyncRemoteError() throws IOException {
    Configuration clientConf = new Configuration(CONF);
    AbstractRpcClient<?> client = createRpcClient(clientConf);
    RpcServer rpcServer = createRpcServer("testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try {
      rpcServer.start();
      Interface stub = newStub(client, rpcServer.getListenerAddress());
      BlockingRpcCallback<EmptyResponseProto> callback = new BlockingRpcCallback<>();
      HBaseRpcController pcrc = new HBaseRpcControllerImpl();
      stub.error(pcrc, EmptyRequestProto.getDefaultInstance(), callback);
      assertNull(callback.get());
      assertTrue(pcrc.failed());
      LOG.info("Caught expected exception: " + pcrc.getFailed());
      IOException ioe = ProtobufUtil.handleRemoteException(pcrc.getFailed());
      assertTrue(ioe instanceof DoNotRetryIOException);
      assertTrue(ioe.getMessage().contains("server error!"));
    } finally {
      client.close();
      rpcServer.stop();
    }
  }

  @Test
  public void testAsyncTimeout() throws IOException {
    Configuration clientConf = new Configuration(CONF);
    RpcServer rpcServer = createRpcServer("testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try (AbstractRpcClient<?> client = createRpcClient(clientConf)) {
      rpcServer.start();
      Interface stub = newStub(client, rpcServer.getListenerAddress());
      List<HBaseRpcController> pcrcList = new ArrayList<>();
      List<BlockingRpcCallback<EmptyResponseProto>> callbackList = new ArrayList<>();
      int ms = 1000;
      int timeout = 100;
      long startTime = System.nanoTime();
      for (int i = 0; i < 10; i++) {
        HBaseRpcController pcrc = new HBaseRpcControllerImpl();
        pcrc.setCallTimeout(timeout);
        BlockingRpcCallback<EmptyResponseProto> callback = new BlockingRpcCallback<>();
        stub.pause(pcrc, PauseRequestProto.newBuilder().setMs(ms).build(), callback);
        pcrcList.add(pcrc);
        callbackList.add(callback);
      }
      for (BlockingRpcCallback<?> callback : callbackList) {
        assertNull(callback.get());
      }
      long waitTime = (System.nanoTime() - startTime) / 1000000;
      for (HBaseRpcController pcrc : pcrcList) {
        assertTrue(pcrc.failed());
        LOG.info("Caught expected exception: " + pcrc.getFailed());
        IOException ioe = ProtobufUtil.handleRemoteException(pcrc.getFailed());
        assertTrue(ioe.getCause() instanceof CallTimeoutException);
      }
      // confirm that we got exception before the actual pause.
      assertTrue(waitTime < ms);
    } finally {
      rpcServer.stop();
    }
  }

  private SpanData waitSpan(Matcher<SpanData> matcher) {
    Waiter.waitFor(CONF, 1000,
      new MatcherPredicate<>(() -> traceRule.getSpans(), hasItem(matcher)));
    return traceRule.getSpans().stream().filter(matcher::matches).findFirst()
      .orElseThrow(AssertionError::new);
  }

  private static String buildIpcSpanName(final String packageAndService, final String methodName) {
    return packageAndService + "/" + methodName;
  }

  private static Matcher<SpanData> buildIpcClientSpanMatcher(final String packageAndService,
    final String methodName) {
    return allOf(hasName(buildIpcSpanName(packageAndService, methodName)),
      hasKind(SpanKind.CLIENT));
  }

  private static Matcher<SpanData> buildIpcServerSpanMatcher(final String packageAndService,
    final String methodName) {
    return allOf(hasName(buildIpcSpanName(packageAndService, methodName)),
      hasKind(SpanKind.SERVER));
  }

  private static Matcher<SpanData> buildIpcClientSpanAttributesMatcher(
    final String packageAndService, final String methodName, final InetSocketAddress isa) {
    return hasAttributes(allOf(containsEntry("rpc.system", "HBASE_RPC"),
      containsEntry("rpc.service", packageAndService), containsEntry("rpc.method", methodName),
      containsEntry("net.peer.name", isa.getHostName()),
      containsEntry(AttributeKey.longKey("net.peer.port"), (long) isa.getPort())));
  }

  private static Matcher<SpanData>
    buildIpcServerSpanAttributesMatcher(final String packageAndService, final String methodName) {
    return hasAttributes(allOf(containsEntry("rpc.system", "HBASE_RPC"),
      containsEntry("rpc.service", packageAndService), containsEntry("rpc.method", methodName)));
  }

  private void assertRemoteSpan() {
    SpanData data = waitSpan(hasName("RpcServer.process"));
    assertTrue(data.getParentSpanContext().isRemote());
    assertEquals(SpanKind.SERVER, data.getKind());
  }

  @Test
  public void testTracingSuccessIpc() throws IOException, ServiceException {
    Configuration clientConf = new Configuration(CONF);
    RpcServer rpcServer = createRpcServer("testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try (AbstractRpcClient<?> client = createRpcClient(clientConf)) {
      rpcServer.start();
      BlockingInterface stub = newBlockingStub(client, rpcServer.getListenerAddress());
      stub.pause(null, PauseRequestProto.newBuilder().setMs(100).build());
      // use the ISA from the running server so that we can get the port selected.
      final InetSocketAddress isa = rpcServer.getListenerAddress();
      final SpanData pauseClientSpan =
        waitSpan(buildIpcClientSpanMatcher("hbase.test.pb.TestProtobufRpcProto", "pause"));
      assertThat(pauseClientSpan,
        buildIpcClientSpanAttributesMatcher("hbase.test.pb.TestProtobufRpcProto", "pause", isa));
      final SpanData pauseServerSpan =
        waitSpan(buildIpcServerSpanMatcher("hbase.test.pb.TestProtobufRpcProto", "pause"));
      assertThat(pauseServerSpan,
        buildIpcServerSpanAttributesMatcher("hbase.test.pb.TestProtobufRpcProto", "pause"));
      assertRemoteSpan();
      assertFalse("no spans provided", traceRule.getSpans().isEmpty());
      assertThat(traceRule.getSpans(),
        everyItem(allOf(hasStatusWithCode(StatusCode.OK),
          hasTraceId(traceRule.getSpans().iterator().next().getTraceId()),
          hasDuration(greaterThanOrEqualTo(Duration.ofMillis(100L))))));
    } finally {
      rpcServer.stop();
    }
  }

  @Test
  public void testTracingErrorIpc() throws IOException {
    Configuration clientConf = new Configuration(CONF);
    RpcServer rpcServer = createRpcServer("testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try (AbstractRpcClient<?> client = createRpcClient(clientConf)) {
      rpcServer.start();
      BlockingInterface stub = newBlockingStub(client, rpcServer.getListenerAddress());
      // use the ISA from the running server so that we can get the port selected.
      assertThrows(ServiceException.class,
        () -> stub.error(null, EmptyRequestProto.getDefaultInstance()));
      final InetSocketAddress isa = rpcServer.getListenerAddress();
      final SpanData errorClientSpan =
        waitSpan(buildIpcClientSpanMatcher("hbase.test.pb.TestProtobufRpcProto", "error"));
      assertThat(errorClientSpan,
        buildIpcClientSpanAttributesMatcher("hbase.test.pb.TestProtobufRpcProto", "error", isa));
      final SpanData errorServerSpan =
        waitSpan(buildIpcServerSpanMatcher("hbase.test.pb.TestProtobufRpcProto", "error"));
      assertThat(errorServerSpan,
        buildIpcServerSpanAttributesMatcher("hbase.test.pb.TestProtobufRpcProto", "error"));
      assertRemoteSpan();
      assertFalse("no spans provided", traceRule.getSpans().isEmpty());
      assertThat(traceRule.getSpans(), everyItem(allOf(hasStatusWithCode(StatusCode.ERROR),
        hasTraceId(traceRule.getSpans().iterator().next().getTraceId()))));
    } finally {
      rpcServer.stop();
    }
  }

  protected abstract AbstractRpcClient<?> createBadAuthRpcClient(Configuration conf);

  private IOException doBadPreableHeaderCall(BlockingInterface stub) {
    ServiceException se = assertThrows(ServiceException.class,
      () -> stub.echo(null, EchoRequestProto.newBuilder().setMessage("hello").build()));
    return ProtobufUtil.handleRemoteException(se);
  }

  @Test
  public void testBadPreambleHeader() throws Exception {
    Configuration clientConf = new Configuration(CONF);
    RpcServer rpcServer = createRpcServer("testRpcServer", Collections.emptyList(),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try (AbstractRpcClient<?> client = createBadAuthRpcClient(clientConf)) {
      rpcServer.start();
      BlockingInterface stub = newBlockingStub(client, rpcServer.getListenerAddress());
      BadAuthException error = null;
      // for SimpleRpcServer, it is possible that we get a broken pipe before getting the
      // BadAuthException, so we add some retries here, see HBASE-28417
      for (int i = 0; i < 10; i++) {
        IOException ioe = doBadPreableHeaderCall(stub);
        if (ioe instanceof BadAuthException) {
          error = (BadAuthException) ioe;
          break;
        }
        Thread.sleep(100);
      }
      assertNotNull("Can not get expected BadAuthException", error);
      assertThat(error.getMessage(), containsString("authName=unknown"));
    } finally {
      rpcServer.stop();
    }
  }

  /**
   * Testcase for getting connection registry information through connection preamble header, see
   * HBASE-25051 for more details.
   */
  @Test
  public void testGetConnectionRegistry() throws IOException, ServiceException {
    Configuration clientConf = new Configuration(CONF);
    String clusterId = "test_cluster_id";
    HBaseServerBase<?> server = mock(HBaseServerBase.class);
    when(server.getClusterId()).thenReturn(clusterId);
    // do not need any services
    RpcServer rpcServer = createRpcServer(server, "testRpcServer", Collections.emptyList(),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try (AbstractRpcClient<?> client = createRpcClient(clientConf)) {
      rpcServer.start();
      InetSocketAddress addr = rpcServer.getListenerAddress();
      BlockingRpcChannel channel =
        client.createBlockingRpcChannel(ServerName.valueOf(addr.getHostName(), addr.getPort(),
          EnvironmentEdgeManager.currentTime()), User.getCurrent(), 0);
      ConnectionRegistryService.BlockingInterface stub =
        ConnectionRegistryService.newBlockingStub(channel);
      GetConnectionRegistryResponse resp =
        stub.getConnectionRegistry(null, GetConnectionRegistryRequest.getDefaultInstance());
      assertEquals(clusterId, resp.getClusterId());
    } finally {
      rpcServer.stop();
    }
  }

  /**
   * Test server does not support getting connection registry information through connection
   * preamble header, i.e, a new client connecting to an old server. We simulate this by using a
   * Server without implementing the ConnectionRegistryEndpoint interface.
   */
  @Test
  public void testGetConnectionRegistryError() throws IOException, ServiceException {
    Configuration clientConf = new Configuration(CONF);
    // do not need any services
    RpcServer rpcServer = createRpcServer("testRpcServer", Collections.emptyList(),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try (AbstractRpcClient<?> client = createRpcClient(clientConf)) {
      rpcServer.start();
      InetSocketAddress addr = rpcServer.getListenerAddress();
      RpcChannel channel = client.createRpcChannel(ServerName.valueOf(addr.getHostName(),
        addr.getPort(), EnvironmentEdgeManager.currentTime()), User.getCurrent(), 0);
      ConnectionRegistryService.Interface stub = ConnectionRegistryService.newStub(channel);
      HBaseRpcController pcrc = new HBaseRpcControllerImpl();
      BlockingRpcCallback<GetConnectionRegistryResponse> done = new BlockingRpcCallback<>();
      stub.getConnectionRegistry(pcrc, GetConnectionRegistryRequest.getDefaultInstance(), done);
      // should have failed so no response
      assertNull(done.get());
      assertTrue(pcrc.failed());
      // should be a FatalConnectionException
      assertThat(pcrc.getFailed(), instanceOf(RemoteException.class));
      assertEquals(FatalConnectionException.class.getName(),
        ((RemoteException) pcrc.getFailed()).getClassName());
      assertThat(pcrc.getFailed().getMessage(), startsWith("Expected HEADER="));
    } finally {
      rpcServer.stop();
    }
  }
}
