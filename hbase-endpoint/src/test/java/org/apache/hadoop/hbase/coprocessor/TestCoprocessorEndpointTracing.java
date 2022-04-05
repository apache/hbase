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
package org.apache.hadoop.hbase.coprocessor;

import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasEnded;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasName;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasParentSpanId;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasStatusWithCode;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ConnectionRule;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MatcherPredicate;
import org.apache.hadoop.hbase.MiniClusterRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.ServiceCaller;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.trace.StringTraceRenderer;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.trace.OpenTelemetryClassRule;
import org.apache.hadoop.hbase.trace.OpenTelemetryTestRule;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.Matcher;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.MapUtils;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoResponseProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto;

/**
 * Test cases to verify tracing coprocessor Endpoint execution
 */
@Category({ CoprocessorTests.class, MediumTests.class})
public class TestCoprocessorEndpointTracing {
  private static final Logger logger =
    LoggerFactory.getLogger(TestCoprocessorEndpointTracing.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCoprocessorEndpointTracing.class);

  private static final OpenTelemetryClassRule otelClassRule = OpenTelemetryClassRule.create();
  private static final MiniClusterRule miniclusterRule = MiniClusterRule.newBuilder()
    .setConfiguration(() -> {
      final Configuration conf = HBaseConfiguration.create();
      conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 5000);
      conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        ProtobufCoprocessorService.class.getName());
      conf.setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        ProtobufCoprocessorService.class.getName());
      return conf;
    })
    .build();
  private static final ConnectionRule connectionRule =
    ConnectionRule.createAsyncConnectionRule(miniclusterRule::createAsyncConnection);

  private static final class Setup extends ExternalResource {
    @Override
    protected void before() throws Throwable {
      final HBaseTestingUtil util = miniclusterRule.getTestingUtility();
      final AsyncConnection connection = connectionRule.getAsyncConnection();
      final AsyncAdmin admin = connection.getAdmin();
      final TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TEST_TABLE)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(TEST_FAMILY)).build();
      admin.createTable(tableDescriptor).get();
      util.waitUntilAllRegionsAssigned(TEST_TABLE);
    }
  }

  @ClassRule
  public static final TestRule testRule = RuleChain.outerRule(otelClassRule)
    .around(miniclusterRule)
    .around(connectionRule)
    .around(new Setup());

  private static final TableName TEST_TABLE =
    TableName.valueOf(TestCoprocessorEndpointTracing.class.getSimpleName());
  private static final byte[] TEST_FAMILY = Bytes.toBytes("TestFamily");

  @Rule
  public OpenTelemetryTestRule otelTestRule = new OpenTelemetryTestRule(otelClassRule);

  @Rule
  public TestName testName = new TestName();

  @Test
  public void traceAsyncTableEndpoint() {
    final AsyncConnection connection = connectionRule.getAsyncConnection();
    final AsyncTable<?> table = connection.getTable(TEST_TABLE);
    final EchoRequestProto request = EchoRequestProto.newBuilder().setMessage("hello").build();
    final CompletableFuture<Map<byte[], String>> future = new CompletableFuture<>();
    final AsyncTable.CoprocessorCallback<EchoResponseProto> callback =
      new AsyncTable.CoprocessorCallback<EchoResponseProto>() {
        final ConcurrentMap<byte[], String> results = new ConcurrentHashMap<>();

        @Override
        public void onRegionComplete(RegionInfo region, EchoResponseProto resp) {
          if (!future.isDone()) {
            results.put(region.getRegionName(), resp.getMessage());
          }
        }

        @Override
        public void onRegionError(RegionInfo region, Throwable error) {
          if (!future.isDone()) {
            future.completeExceptionally(error);
          }
        }

        @Override
        public void onComplete() {
          if (!future.isDone()) {
            future.complete(results);
          }
        }

        @Override
        public void onError(Throwable error) {
          if (!future.isDone()) {
            future.completeExceptionally(error);
          }
        }
      };

    final Map<byte[], String> results = TraceUtil.trace(() -> {
      table.coprocessorService(TestProtobufRpcProto::newStub,
        (stub, controller, cb) -> stub.echo(controller, request, cb), callback)
        .execute();
      try {
        return future.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }, testName.getMethodName());
    assertNotNull(results);
    assertTrue("coprocessor call returned no results.", MapUtils.isNotEmpty(results));
    assertThat(results.values(), everyItem(allOf(
      notNullValue(),
      equalTo("hello"))));

    final Matcher<SpanData> parentMatcher = allOf(hasName(testName.getMethodName()), hasEnded());
    waitForAndLog(parentMatcher);
    final List<SpanData> spans = otelClassRule.getSpans();

    final SpanData testSpan = spans.stream()
      .filter(parentMatcher::matches)
      .findFirst()
      .orElseThrow(AssertionError::new);
    final Matcher<SpanData> tableOpMatcher = allOf(
      hasName(containsString("COPROC_EXEC")),
      hasParentSpanId(testSpan),
      hasStatusWithCode(StatusCode.OK));
    assertThat(spans, hasItem(tableOpMatcher));
    final SpanData tableOpSpan = spans.stream()
      .filter(tableOpMatcher::matches)
      .findFirst()
      .orElseThrow(AssertionError::new);
    final Matcher<SpanData> rpcMatcher = allOf(
      hasName("hbase.pb.ClientService/ExecService"),
      hasParentSpanId(tableOpSpan),
      hasStatusWithCode(StatusCode.OK));
    assertThat(spans, hasItem(rpcMatcher));
  }

  @Test
  public void traceSyncTableEndpointCall() throws Exception {
    final Connection connection = connectionRule.getConnection();
    try (final Table table = connection.getTable(TEST_TABLE)) {
      final RpcController controller = new ServerRpcController();
      final EchoRequestProto request = EchoRequestProto.newBuilder().setMessage("hello").build();
      final CoprocessorRpcUtils.BlockingRpcCallback<EchoResponseProto> callback =
        new CoprocessorRpcUtils.BlockingRpcCallback<>();
      final Map<byte[], EchoResponseProto> results = TraceUtil.trace(() -> {
        try {
          return table.coprocessorService(TestProtobufRpcProto.class, null, null,
            t -> {
              t.echo(controller, request, callback);
              return callback.get();
            });
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }, testName.getMethodName());
      assertNotNull(results);
      assertTrue("coprocessor call returned no results.", MapUtils.isNotEmpty(results));
      assertThat(results.values(), everyItem(allOf(
        notNullValue(),
        hasProperty("message", equalTo("hello")))));
    }

    final Matcher<SpanData> parentMatcher = allOf(
      hasName(testName.getMethodName()),
      hasEnded());
    waitForAndLog(parentMatcher);
    final List<SpanData> spans = otelClassRule.getSpans();

    final SpanData testSpan = spans.stream()
      .filter(parentMatcher::matches)
      .findFirst()
      .orElseThrow(AssertionError::new);
    final Matcher<SpanData> tableOpMatcher = allOf(
      hasName(containsString("COPROC_EXEC")),
      hasParentSpanId(testSpan),
      hasStatusWithCode(StatusCode.OK));
    assertThat(spans, hasItem(tableOpMatcher));
    final SpanData tableOpSpan = spans.stream()
      .filter(tableOpMatcher::matches)
      .findFirst()
      .orElseThrow(AssertionError::new);
    final Matcher<SpanData> rpcMatcher = allOf(
      hasName("hbase.pb.ClientService/ExecService"),
      hasParentSpanId(tableOpSpan),
      hasStatusWithCode(StatusCode.OK));
    assertThat(spans, hasItem(rpcMatcher));
  }

  @Test
  public void traceSyncTableEndpointCallAndCallback() throws Exception {
    final Connection connection = connectionRule.getConnection();
    try (final Table table = connection.getTable(TEST_TABLE)) {
      final RpcController controller = new ServerRpcController();
      final EchoRequestProto request = EchoRequestProto.newBuilder().setMessage("hello").build();
      final CoprocessorRpcUtils.BlockingRpcCallback<EchoResponseProto> callback =
        new CoprocessorRpcUtils.BlockingRpcCallback<>();
      final ConcurrentMap<byte[], EchoResponseProto> results = new ConcurrentHashMap<>();
      TraceUtil.trace(() -> {
        try {
          table.coprocessorService(TestProtobufRpcProto.class, null, null, t -> {
            t.echo(controller, request, callback);
            return callback.get();
          }, (region, row, result) -> results.put(region, result));
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }, testName.getMethodName());
      assertNotNull(results);
      assertTrue("coprocessor call returned no results.", MapUtils.isNotEmpty(results));
      assertThat(results.values(), everyItem(allOf(
        notNullValue(),
        hasProperty("message", equalTo("hello")))));
    }

    final Matcher<SpanData> parentMatcher = allOf(
      hasName(testName.getMethodName()),
      hasEnded());
    waitForAndLog(parentMatcher);
    final List<SpanData> spans = otelClassRule.getSpans();

    final SpanData testSpan = spans.stream()
      .filter(parentMatcher::matches)
      .findFirst()
      .orElseThrow(AssertionError::new);
    final Matcher<SpanData> tableOpMatcher = allOf(
      hasName(containsString("COPROC_EXEC")),
      hasParentSpanId(testSpan),
      hasStatusWithCode(StatusCode.OK));
    assertThat(spans, hasItem(tableOpMatcher));
    final SpanData tableOpSpan = spans.stream()
      .filter(tableOpMatcher::matches)
      .findFirst()
      .orElseThrow(AssertionError::new);
    final Matcher<SpanData> rpcMatcher = allOf(
      hasName("hbase.pb.ClientService/ExecService"),
      hasParentSpanId(tableOpSpan),
      hasStatusWithCode(StatusCode.OK));
    assertThat(spans, hasItem(rpcMatcher));
  }

  @Test
  public void traceSyncTableRegionCoprocessorRpcChannel() throws Exception {
    final Connection connection = connectionRule.getConnection();
    try (final Table table = connection.getTable(TEST_TABLE)) {
      final EchoRequestProto request = EchoRequestProto.newBuilder().setMessage("hello").build();
      final EchoResponseProto response = TraceUtil.trace(() -> {
        try {
          final CoprocessorRpcChannel channel = table.coprocessorService(new byte[] {});
          final TestProtobufRpcProto.BlockingInterface service =
            TestProtobufRpcProto.newBlockingStub(channel);
          return service.echo(null, request);
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }, testName.getMethodName());
      assertNotNull(response);
      assertEquals("hello", response.getMessage());
    }

    final Matcher<SpanData> parentMatcher = allOf(
      hasName(testName.getMethodName()),
      hasEnded());
    waitForAndLog(parentMatcher);
    final List<SpanData> spans = otelClassRule.getSpans();

    /*
     * This interface is really low level: it returns a Channel and expects the caller to invoke it.
     * The Table instance isn't issuing a command here, it's not a table operation, so don't expect
     * there to be a span like `COPROC_EXEC table`.
     */
    final SpanData testSpan = spans.stream()
      .filter(parentMatcher::matches)
      .findFirst()
      .orElseThrow(AssertionError::new);
    final Matcher<SpanData> tableOpMatcher = allOf(
      hasName(containsString("COPROC_EXEC")),
      hasParentSpanId(testSpan));
    assertThat(spans, not(hasItem(tableOpMatcher)));
  }

  @Test
  public void traceSyncTableBatchEndpoint() throws Exception {
    final Connection connection = connectionRule.getConnection();
    try (final Table table = connection.getTable(TEST_TABLE)) {
      final Descriptors.MethodDescriptor descriptor =
        TestProtobufRpcProto.getDescriptor().findMethodByName("echo");
      final EchoRequestProto request = EchoRequestProto.newBuilder().setMessage("hello").build();
      final Map<byte[], EchoResponseProto> response = TraceUtil.trace(() -> {
        try {
          return table.batchCoprocessorService(
            descriptor, request, null, null, EchoResponseProto.getDefaultInstance());
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }, testName.getMethodName());
      assertNotNull(response);
      assertThat(response.values(), everyItem(allOf(
        notNullValue(),
        hasProperty("message", equalTo("hello")))));
    }

    final Matcher<SpanData> parentMatcher = allOf(
      hasName(testName.getMethodName()),
      hasEnded());
    waitForAndLog(parentMatcher);
    final List<SpanData> spans = otelClassRule.getSpans();

    final SpanData testSpan = spans.stream()
      .filter(parentMatcher::matches)
      .findFirst()
      .orElseThrow(AssertionError::new);
    final Matcher<SpanData> tableOpMatcher = allOf(
      hasName(containsString("COPROC_EXEC")),
      hasParentSpanId(testSpan),
      hasStatusWithCode(StatusCode.OK));
    assertThat(spans, hasItem(tableOpMatcher));
    final SpanData tableOpSpan = spans.stream()
      .filter(tableOpMatcher::matches)
      .findFirst()
      .orElseThrow(AssertionError::new);
    final Matcher<SpanData> rpcMatcher = allOf(
      hasName("hbase.pb.ClientService/ExecService"),
      hasParentSpanId(tableOpSpan),
      hasStatusWithCode(StatusCode.OK));
    assertThat(spans, hasItem(rpcMatcher));
  }

  @Test
  public void traceSyncTableBatchEndpointCallback() throws Exception {
    final Connection connection = connectionRule.getConnection();
    try (final Table table = connection.getTable(TEST_TABLE)) {
      final Descriptors.MethodDescriptor descriptor =
        TestProtobufRpcProto.getDescriptor().findMethodByName("echo");
      final EchoRequestProto request = EchoRequestProto.newBuilder().setMessage("hello").build();
      final ConcurrentMap<byte[], EchoResponseProto> results = new ConcurrentHashMap<>();
      TraceUtil.trace(() -> {
        try {
          table.batchCoprocessorService(descriptor, request, null, null,
            EchoResponseProto.getDefaultInstance(), (region, row, res) -> results.put(region, res));
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }, testName.getMethodName());
      assertNotNull(results);
      assertTrue("coprocessor call returned no results.", MapUtils.isNotEmpty(results));
      assertThat(results.values(), everyItem(allOf(
        notNullValue(),
        hasProperty("message", equalTo("hello")))));
    }

    final Matcher<SpanData> parentMatcher = allOf(
      hasName(testName.getMethodName()),
      hasEnded());
    waitForAndLog(parentMatcher);
    final List<SpanData> spans = otelClassRule.getSpans();

    final SpanData testSpan = spans.stream()
      .filter(parentMatcher::matches)
      .findFirst()
      .orElseThrow(AssertionError::new);
    final Matcher<SpanData> tableOpMatcher = allOf(
      hasName(containsString("COPROC_EXEC")),
      hasParentSpanId(testSpan),
      hasStatusWithCode(StatusCode.OK));
    assertThat(spans, hasItem(tableOpMatcher));
    final SpanData tableOpSpan = spans.stream()
      .filter(tableOpMatcher::matches)
      .findFirst()
      .orElseThrow(AssertionError::new);
    final Matcher<SpanData> rpcMatcher = allOf(
      hasName("hbase.pb.ClientService/ExecService"),
      hasParentSpanId(tableOpSpan),
      hasStatusWithCode(StatusCode.OK));
    assertThat(spans, hasItem(rpcMatcher));
  }

  @Test
  public void traceAsyncAdminEndpoint() throws Exception {
    final AsyncConnection connection = connectionRule.getAsyncConnection();
    final AsyncAdmin admin = connection.getAdmin();
    final EchoRequestProto request = EchoRequestProto.newBuilder().setMessage("hello").build();
    final ServiceCaller<TestProtobufRpcProto, EchoResponseProto> callback =
      (stub, controller, cb) -> stub.echo(controller, request, cb);

    final String response = TraceUtil.tracedFuture(
      () -> admin.coprocessorService(TestProtobufRpcProto::newStub, callback),
      testName.getMethodName())
      .get()
      .getMessage();
    assertEquals("hello", response);

    final Matcher<SpanData> parentMatcher = allOf(
      hasName(testName.getMethodName()),
      hasEnded());
    waitForAndLog(parentMatcher);
    final List<SpanData> spans = otelClassRule.getSpans();

    final SpanData testSpan = spans.stream()
      .filter(parentMatcher::matches)
      .findFirst()
      .orElseThrow(AssertionError::new);
    final Matcher<SpanData> rpcMatcher = allOf(
      hasName("hbase.pb.MasterService/ExecMasterService"),
      hasParentSpanId(testSpan),
      hasStatusWithCode(StatusCode.OK));
    assertThat(spans, hasItem(rpcMatcher));
  }

  @Test
  public void traceSyncAdminEndpoint() throws Exception {
    final Connection connection = connectionRule.getConnection();
    try (final Admin admin = connection.getAdmin()) {
      final TestProtobufRpcProto.BlockingInterface service =
        TestProtobufRpcProto.newBlockingStub(admin.coprocessorService());
      final EchoRequestProto request = EchoRequestProto.newBuilder().setMessage("hello").build();
      final String response = TraceUtil.trace(() -> {
        try {
          return service.echo(null, request).getMessage();
        } catch (ServiceException e) {
          throw new RuntimeException(e);
        }
      }, testName.getMethodName());
      assertEquals("hello", response);
    }

    final Matcher<SpanData> parentMatcher = allOf(
      hasName(testName.getMethodName()),
      hasEnded());
    waitForAndLog(parentMatcher);
    final List<SpanData> spans = otelClassRule.getSpans();

    final SpanData testSpan = spans.stream()
      .filter(parentMatcher::matches)
      .findFirst()
      .orElseThrow(AssertionError::new);
    final Matcher<SpanData> rpcMatcher = allOf(
      hasName("hbase.pb.MasterService/ExecMasterService"),
      hasParentSpanId(testSpan),
      hasStatusWithCode(StatusCode.OK));
    assertThat(spans, hasItem(rpcMatcher));
  }

  private void waitForAndLog(Matcher<SpanData> spanMatcher) {
    final Configuration conf = connectionRule.getAsyncConnection().getConfiguration();
    Waiter.waitFor(conf, TimeUnit.SECONDS.toMillis(5), new MatcherPredicate<>(
      otelClassRule::getSpans, hasItem(spanMatcher)));
    final List<SpanData> spans = otelClassRule.getSpans();
    if (logger.isDebugEnabled()) {
      StringTraceRenderer renderer = new StringTraceRenderer(spans);
      renderer.render(logger::debug);
    }
  }
}
