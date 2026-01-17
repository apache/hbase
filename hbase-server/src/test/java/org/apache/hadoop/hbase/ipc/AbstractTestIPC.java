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
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MatcherPredicate;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.StringUtils;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoResponseProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EmptyRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EmptyResponseProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.PauseRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.Interface;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * Some basic ipc tests.
 */
public abstract class AbstractTestIPC {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTestIPC.class);

  private static final byte[] CELL_BYTES = Bytes.toBytes("xyz");
  private static final KeyValue CELL = new KeyValue(CELL_BYTES, CELL_BYTES, CELL_BYTES, CELL_BYTES);

  protected static final Configuration CONF = HBaseConfiguration.create();

  private RpcServer createRpcServer(Server server, String name,
    List<BlockingServiceAndInterface> services, InetSocketAddress bindAddress, Configuration conf,
    RpcScheduler scheduler) throws IOException {
    return RpcServerFactory.createRpcServer(server, name, services, bindAddress, conf, scheduler);
  }

  protected abstract AbstractRpcClient<?> createRpcClientNoCodec(Configuration conf);

  @RegisterExtension
  private static final OpenTelemetryExtension OTEL_EXT = OpenTelemetryExtension.create();

  private Class<? extends RpcServer> rpcServerImpl;

  protected AbstractTestIPC(Class<? extends RpcServer> rpcServerImpl) {
    this.rpcServerImpl = rpcServerImpl;
  }

  @BeforeEach
  public void setUpBeforeTest() {
    CONF.setClass(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY, rpcServerImpl, RpcServer.class);
  }

  /**
   * Ensure we do not HAVE TO HAVE a codec.
   */
  @TestTemplate
  public void testNoCodec() throws IOException, ServiceException {
    Configuration conf = HBaseConfiguration.create();
    RpcServer rpcServer = createRpcServer(null, "testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try (AbstractRpcClient<?> client = createRpcClientNoCodec(conf)) {
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
  @TestTemplate
  public void testCompressCellBlock() throws IOException, ServiceException {
    Configuration conf = new Configuration(HBaseConfiguration.create());
    conf.set("hbase.client.rpc.compressor", GzipCodec.class.getCanonicalName());
    List<Cell> cells = new ArrayList<>();
    int count = 3;
    for (int i = 0; i < count; i++) {
      cells.add(CELL);
    }
    RpcServer rpcServer = createRpcServer(null, "testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));

    try (AbstractRpcClient<?> client = createRpcClient(conf)) {
      rpcServer.start();
      BlockingInterface stub = newBlockingStub(client, rpcServer.getListenerAddress());
      HBaseRpcController pcrc = new HBaseRpcControllerImpl(CellUtil.createCellScanner(cells));
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

  @TestTemplate
  public void testRTEDuringConnectionSetup() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    RpcServer rpcServer = createRpcServer(null, "testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try (AbstractRpcClient<?> client = createRpcClientRTEDuringConnectionSetup(conf)) {
      rpcServer.start();
      BlockingInterface stub = newBlockingStub(client, rpcServer.getListenerAddress());
      stub.ping(null, EmptyRequestProto.getDefaultInstance());
      fail("Expected an exception to have been thrown!");
    } catch (Exception e) {
      LOG.info("Caught expected exception: " + e.toString());
      assertTrue(StringUtils.stringifyException(e).contains("Injected fault"), e.toString());
    } finally {
      rpcServer.stop();
    }
  }

  /**
   * Tests that the rpc scheduler is called when requests arrive.
   */
  @TestTemplate
  public void testRpcScheduler() throws IOException, ServiceException, InterruptedException {
    RpcScheduler scheduler = spy(new FifoRpcScheduler(CONF, 1));
    RpcServer rpcServer = createRpcServer(null, "testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, scheduler);
    verify(scheduler).init(any(RpcScheduler.Context.class));
    try (AbstractRpcClient<?> client = createRpcClient(CONF)) {
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
  @TestTemplate
  public void testRpcMaxRequestSize() throws IOException, ServiceException {
    Configuration conf = new Configuration(CONF);
    conf.setInt(RpcServer.MAX_REQUEST_SIZE, 1000);
    RpcServer rpcServer = createRpcServer(null, "testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), conf, new FifoRpcScheduler(conf, 1));
    try (AbstractRpcClient<?> client = createRpcClient(conf)) {
      rpcServer.start();
      BlockingInterface stub = newBlockingStub(client, rpcServer.getListenerAddress());
      StringBuilder message = new StringBuilder(1200);
      for (int i = 0; i < 200; i++) {
        message.append("hello.");
      }
      // set total RPC size bigger than 100 bytes
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage(message.toString()).build();
      stub.echo(
        new HBaseRpcControllerImpl(CellUtil.createCellScanner(ImmutableList.<Cell> of(CELL))),
        param);
      fail("RPC should have failed because it exceeds max request size");
    } catch (ServiceException e) {
      LOG.info("Caught expected exception: " + e);
      assertTrue(StringUtils.stringifyException(e).contains("RequestTooBigException"),
        e.toString());
    } finally {
      rpcServer.stop();
    }
  }

  /**
   * Tests that the RpcServer creates & dispatches CallRunner object to scheduler with non-null
   * remoteAddress set to its Call Object
   */
  @TestTemplate
  public void testRpcServerForNotNullRemoteAddressInCallObject()
    throws IOException, ServiceException {
    RpcServer rpcServer = createRpcServer(null, "testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    InetSocketAddress localAddr = new InetSocketAddress("localhost", 0);
    try (AbstractRpcClient<?> client = createRpcClient(CONF)) {
      rpcServer.start();
      BlockingInterface stub = newBlockingStub(client, rpcServer.getListenerAddress());
      assertEquals(localAddr.getAddress().getHostAddress(),
        stub.addr(null, EmptyRequestProto.getDefaultInstance()).getAddr());
    } finally {
      rpcServer.stop();
    }
  }

  @TestTemplate
  public void testRemoteError() throws IOException, ServiceException {
    RpcServer rpcServer = createRpcServer(null, "testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try (AbstractRpcClient<?> client = createRpcClient(CONF)) {
      rpcServer.start();
      BlockingInterface stub = newBlockingStub(client, rpcServer.getListenerAddress());
      ServiceException se = assertThrows(ServiceException.class,
        () -> stub.error(null, EmptyRequestProto.getDefaultInstance()));
      LOG.info("Caught expected exception: " + se);
      IOException ioe = ProtobufUtil.handleRemoteException(se);
      assertThat(ioe, instanceOf(DoNotRetryIOException.class));
      assertThat(ioe.getMessage(), containsString("server error!"));
    } finally {
      rpcServer.stop();
    }
  }

  @TestTemplate
  public void testTimeout() throws IOException {
    RpcServer rpcServer = createRpcServer(null, "testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try (AbstractRpcClient<?> client = createRpcClient(CONF)) {
      rpcServer.start();
      BlockingInterface stub = newBlockingStub(client, rpcServer.getListenerAddress());
      HBaseRpcController pcrc = new HBaseRpcControllerImpl();
      int ms = 1000;
      int timeout = 100;
      for (int i = 0; i < 10; i++) {
        pcrc.reset();
        pcrc.setCallTimeout(timeout);
        long startTime = System.nanoTime();
        ServiceException se = assertThrows(ServiceException.class,
          () -> stub.pause(pcrc, PauseRequestProto.newBuilder().setMs(ms).build()));
        long waitTime = (System.nanoTime() - startTime) / 1000000;
        // expected
        LOG.info("Caught expected exception: " + se);
        IOException ioe = ProtobufUtil.handleRemoteException(se);
        assertThat(ioe.getCause(), instanceOf(CallTimeoutException.class));
        // confirm that we got exception before the actual pause.
        assertThat(waitTime, lessThan((long) ms));
      }
    } finally {
      // wait until all active calls quit, otherwise it may mess up the tracing spans
      await().atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertEquals(0, rpcServer.getScheduler().getActiveRpcHandlerCount()));
      rpcServer.stop();
    }
  }

  @SuppressWarnings("deprecation")
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

  private static class FailingNettyRpcServer extends NettyRpcServer {

    FailingNettyRpcServer(Server server, String name,
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
    protected NettyRpcServerPreambleHandler createNettyRpcServerPreambleHandler() {
      return new NettyRpcServerPreambleHandler(FailingNettyRpcServer.this) {
        @Override
        protected NettyServerRpcConnection createNettyServerRpcConnection(Channel channel) {
          return new FailingConnection(FailingNettyRpcServer.this, channel);
        }
      };
    }
  }

  private RpcServer createTestFailingRpcServer(final String name,
    final List<BlockingServiceAndInterface> services, final InetSocketAddress bindAddress,
    Configuration conf, RpcScheduler scheduler) throws IOException {
    if (rpcServerImpl.equals(NettyRpcServer.class)) {
      return new FailingNettyRpcServer(null, name, services, bindAddress, conf, scheduler);
    } else {
      return new FailingSimpleRpcServer(null, name, services, bindAddress, conf, scheduler);
    }
  }

  /** Tests that the connection closing is handled by the client with outstanding RPC calls */
  @TestTemplate
  public void testConnectionCloseWithOutstandingRPCs() throws InterruptedException, IOException {
    Configuration conf = new Configuration(CONF);
    RpcServer rpcServer = createTestFailingRpcServer("testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));

    try (AbstractRpcClient<?> client = createRpcClient(conf)) {
      rpcServer.start();
      BlockingInterface stub = newBlockingStub(client, rpcServer.getListenerAddress());
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage("hello").build();
      ServiceException se = assertThrows(ServiceException.class, () -> stub.echo(null, param),
        "RPC should have failed because connection closed");
      LOG.info("Caught expected exception: " + se);
    } finally {
      rpcServer.stop();
    }
  }

  @TestTemplate
  public void testAsyncEcho() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    RpcServer rpcServer = createRpcServer(null, "testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try (AbstractRpcClient<?> client = createRpcClient(conf)) {
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
        HBaseRpcController pcrc = pcrcList.get(i);
        assertFalse(pcrc.failed());
        assertNull(pcrc.cellScanner());
        assertEquals("hello-" + i, callbackList.get(i).get().getMessage());
      }
    } finally {
      rpcServer.stop();
    }
  }

  @TestTemplate
  public void testAsyncRemoteError() throws IOException {
    Configuration clientConf = new Configuration(CONF);
    RpcServer rpcServer = createRpcServer(null, "testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try (AbstractRpcClient<?> client = createRpcClient(clientConf)) {
      rpcServer.start();
      Interface stub = newStub(client, rpcServer.getListenerAddress());
      BlockingRpcCallback<EmptyResponseProto> callback = new BlockingRpcCallback<>();
      HBaseRpcController pcrc = new HBaseRpcControllerImpl();
      stub.error(pcrc, EmptyRequestProto.getDefaultInstance(), callback);
      assertNull(callback.get());
      assertTrue(pcrc.failed());
      LOG.info("Caught expected exception: " + pcrc.getFailed());
      IOException ioe = ProtobufUtil.handleRemoteException(pcrc.getFailed());
      assertThat(ioe, instanceOf(DoNotRetryIOException.class));
      assertThat(ioe.getMessage(), containsString("server error!"));
    } finally {
      rpcServer.stop();
    }
  }

  @TestTemplate
  public void testAsyncTimeout() throws IOException {
    RpcServer rpcServer = createRpcServer(null, "testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try (AbstractRpcClient<?> client = createRpcClient(CONF)) {
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
        assertThat(ioe.getCause(), instanceOf(CallTimeoutException.class));
      }
      // confirm that we got exception before the actual pause.
      assertThat(waitTime, lessThan((long) ms));
    } finally {
      // wait until all active calls quit, otherwise it may mess up the tracing spans
      await().atMost(Duration.ofSeconds(2))
        .untilAsserted(() -> assertEquals(0, rpcServer.getScheduler().getActiveRpcHandlerCount()));
      rpcServer.stop();
    }
  }

  private SpanData waitSpan(Matcher<SpanData> matcher) {
    Waiter.waitFor(CONF, 1000, new MatcherPredicate<>(() -> OTEL_EXT.getSpans(), hasItem(matcher)));
    return OTEL_EXT.getSpans().stream().filter(matcher::matches).findFirst()
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

  @TestTemplate
  public void testTracingSuccessIpc() throws IOException, ServiceException {
    RpcServer rpcServer = createRpcServer(null, "testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try (AbstractRpcClient<?> client = createRpcClient(CONF)) {
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
      assertFalse(OTEL_EXT.getSpans().isEmpty(), "no spans provided");
      assertThat(OTEL_EXT.getSpans(),
        everyItem(allOf(hasStatusWithCode(StatusCode.OK),
          hasTraceId(OTEL_EXT.getSpans().iterator().next().getTraceId()),
          hasDuration(greaterThanOrEqualTo(Duration.ofMillis(100L))))));
    } finally {
      rpcServer.stop();
    }
  }

  @TestTemplate
  public void testTracingErrorIpc() throws IOException {
    RpcServer rpcServer = createRpcServer(null, "testRpcServer",
      Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
      new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 1));
    try (AbstractRpcClient<?> client = createRpcClient(CONF)) {
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
      assertFalse(OTEL_EXT.getSpans().isEmpty(), "no spans provided");
      assertThat(OTEL_EXT.getSpans(), everyItem(allOf(hasStatusWithCode(StatusCode.ERROR),
        hasTraceId(OTEL_EXT.getSpans().iterator().next().getTraceId()))));
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

  @TestTemplate
  public void testBadPreambleHeader() throws Exception {
    Configuration clientConf = new Configuration(CONF);
    RpcServer rpcServer = createRpcServer(null, "testRpcServer", Collections.emptyList(),
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
      assertNotNull(error, "Can not get expected BadAuthException");
      assertThat(error.getMessage(), containsString("authName=unknown"));
    } finally {
      rpcServer.stop();
    }
  }
}
