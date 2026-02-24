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
package org.apache.hadoop.hbase.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Counter;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.RatioGauge.Ratio;
import com.codahale.metrics.Timer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MetricsTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;

@Tag(ClientTests.TAG)
@Tag(MetricsTests.TAG)
@Tag(SmallTests.TAG)
@HBaseParameterizedTestTemplate
public class TestMetricsConnection {

  private static final Configuration conf = new Configuration();
  private static MetricsConnection METRICS;
  private static final ThreadPoolExecutor BATCH_POOL =
    (ThreadPoolExecutor) Executors.newFixedThreadPool(2);

  private static final String MOCK_CONN_STR = "mocked-connection";

  public boolean tableMetricsEnabled;

  public static Stream<Arguments> parameters() {
    return Stream.of(Arguments.of(true), Arguments.of(false));
  }

  @BeforeEach
  public void before() {
    conf.setBoolean(MetricsConnection.CLIENT_SIDE_TABLE_METRICS_ENABLED_KEY, tableMetricsEnabled);
    METRICS =
      MetricsConnection.getMetricsConnection(conf, MOCK_CONN_STR, () -> BATCH_POOL, () -> null);
  }

  @AfterEach
  public void after() {
    MetricsConnection.deleteMetricsConnection(MOCK_CONN_STR);
  }

  @TestTemplate
  public void testMetricsConnectionScopeAsyncClient() throws IOException {
    Configuration conf = new Configuration();
    String clusterId = "foo";
    String scope = "testScope";
    conf.setBoolean(MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY, true);

    AsyncConnectionImpl impl = new AsyncConnectionImpl(conf, null, "foo", User.getCurrent());
    Optional<MetricsConnection> metrics = impl.getConnectionMetrics();
    assertTrue(metrics.isPresent(), "Metrics should be present");
    assertEquals(clusterId + "@" + Integer.toHexString(impl.hashCode()),
      metrics.get().getMetricScope());
    conf.set(MetricsConnection.METRICS_SCOPE_KEY, scope);
    impl = new AsyncConnectionImpl(conf, null, "foo", User.getCurrent());

    metrics = impl.getConnectionMetrics();
    assertTrue(metrics.isPresent(), "Metrics should be present");
    assertEquals(scope, metrics.get().getMetricScope());
  }

  @TestTemplate
  public void testMetricsWithMultiConnections() throws IOException {
    Configuration conf = new Configuration();
    conf.setBoolean(MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY, true);
    conf.set(MetricsConnection.METRICS_SCOPE_KEY, "unit-test");

    User user = User.getCurrent();

    /* create multiple connections */
    final int num = 3;
    AsyncConnectionImpl impl;
    List<AsyncConnectionImpl> connList = new ArrayList<AsyncConnectionImpl>();
    for (int i = 0; i < num; i++) {
      impl = new AsyncConnectionImpl(conf, null, null, user);
      connList.add(impl);
    }

    /* verify metrics presence */
    impl = connList.get(0);
    Optional<MetricsConnection> metrics = impl.getConnectionMetrics();
    assertTrue(metrics.isPresent(), "Metrics should be present");

    /* verify connection count in a shared metrics */
    long count = metrics.get().getConnectionCount();
    assertEquals(count, num, "Failed to verify connection count." + count);

    /* close some connections */
    for (int i = 0; i < num - 1; i++) {
      connList.get(i).close();
    }

    /* verify metrics presence again */
    impl = connList.get(num - 1);
    metrics = impl.getConnectionMetrics();
    assertTrue(metrics.isPresent(),
      "Metrics should be present after some of connections are closed.");

    /* verify count of remaining connections */
    count = metrics.get().getConnectionCount();
    assertEquals(count, 1, "Connection count suppose to be 1 but got: " + count);

    /* shutdown */
    impl.close();
  }

  @TestTemplate
  public void testMetricsConnectionScopeBlockingClient() throws IOException {
    Configuration conf = new Configuration();
    String clusterId = "foo";
    String scope = "testScope";
    conf.setBoolean(MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY, true);

    ConnectionRegistry mockRegistry = mock(ConnectionRegistry.class);
    when(mockRegistry.getClusterId()).thenReturn(CompletableFuture.completedFuture(clusterId));

    ConnectionImplementation impl =
      new ConnectionImplementation(conf, null, User.getCurrent(), mockRegistry);
    MetricsConnection metrics = impl.getConnectionMetrics();
    assertNotNull(metrics, "Metrics should be present");
    assertEquals(clusterId + "@" + Integer.toHexString(impl.hashCode()), metrics.getMetricScope());
    conf.set(MetricsConnection.METRICS_SCOPE_KEY, scope);
    impl = new ConnectionImplementation(conf, null, User.getCurrent(), mockRegistry);

    metrics = impl.getConnectionMetrics();
    assertNotNull(metrics, "Metrics should be present");
    assertEquals(scope, metrics.getMetricScope());
  }

  @TestTemplate
  public void testStaticMetrics() throws IOException {
    final byte[] foo = Bytes.toBytes("foo");
    String table = "TableX";
    final RegionSpecifier region = RegionSpecifier.newBuilder()
      .setValue(ByteString.copyFromUtf8(table)).setType(RegionSpecifierType.REGION_NAME).build();
    final int loop = 5;

    for (int i = 0; i < loop; i++) {
      METRICS.updateRpc(ClientService.getDescriptor().findMethodByName("Get"),
        TableName.valueOf(table),
        GetRequest.newBuilder().setRegion(region).setGet(ProtobufUtil.toGet(new Get(foo))).build(),
        MetricsConnection.newCallStats(), null);
      METRICS.updateRpc(ClientService.getDescriptor().findMethodByName("Scan"),
        TableName.valueOf(table),
        ScanRequest.newBuilder().setRegion(region)
          .setScan(ProtobufUtil.toScan(new Scan(new Get(foo)))).build(),
        MetricsConnection.newCallStats(),
        new RemoteWithExtrasException("java.io.IOException", null, false, false));
      METRICS.updateRpc(ClientService.getDescriptor().findMethodByName("Multi"),
        TableName.valueOf(table),
        MultiRequest.newBuilder()
          .addRegionAction(ClientProtos.RegionAction.newBuilder()
            .addAction(
              ClientProtos.Action.newBuilder().setGet(ProtobufUtil.toGet(new Get(foo))).build())
            .setRegion(region).build())
          .build(),
        MetricsConnection.newCallStats(),
        new CallTimeoutException("test with CallTimeoutException"));
      METRICS.updateRpc(ClientService.getDescriptor().findMethodByName("Mutate"),
        TableName.valueOf(table),
        MutateRequest.newBuilder()
          .setMutation(ProtobufUtil.toMutation(MutationType.APPEND, new Append(foo)))
          .setRegion(region).build(),
        MetricsConnection.newCallStats(), null);
      METRICS.updateRpc(ClientService.getDescriptor().findMethodByName("Mutate"),
        TableName.valueOf(table),
        MutateRequest.newBuilder()
          .setMutation(ProtobufUtil.toMutation(MutationType.DELETE, new Delete(foo)))
          .setRegion(region).build(),
        MetricsConnection.newCallStats(), null);
      METRICS.updateRpc(ClientService.getDescriptor().findMethodByName("Mutate"),
        TableName.valueOf(table),
        MutateRequest.newBuilder()
          .setMutation(ProtobufUtil.toMutation(MutationType.INCREMENT, new Increment(foo)))
          .setRegion(region).build(),
        MetricsConnection.newCallStats(), null);
      METRICS.updateRpc(ClientService.getDescriptor().findMethodByName("Mutate"),
        TableName.valueOf(table),
        MutateRequest.newBuilder()
          .setMutation(ProtobufUtil.toMutation(MutationType.PUT, new Put(foo))).setRegion(region)
          .build(),
        MetricsConnection.newCallStats(),
        new CallTimeoutException("test with CallTimeoutException"));
    }

    testRpcCallMetrics(table, loop);

    String metricKey;
    long metricVal;
    Counter counter;

    // remote exception
    metricKey = "rpcRemoteExceptions_IOException";
    counter = METRICS.getRpcCounters().get(metricKey);
    metricVal = (counter != null) ? counter.getCount() : 0;
    assertEquals(metricVal, loop, "metric: " + metricKey + " val: " + metricVal);

    // local exception
    metricKey = "rpcLocalExceptions_CallTimeoutException";
    counter = METRICS.getRpcCounters().get(metricKey);
    metricVal = (counter != null) ? counter.getCount() : 0;
    assertEquals(metricVal, loop * 2, "metric: " + metricKey + " val: " + metricVal);

    // total exception
    metricKey = "rpcTotalExceptions";
    counter = METRICS.getRpcCounters().get(metricKey);
    metricVal = (counter != null) ? counter.getCount() : 0;
    assertEquals(metricVal, loop * 3, "metric: " + metricKey + " val: " + metricVal);

    testRpcCallTableMetrics(table, loop);

    for (MetricsConnection.CallTracker t : new MetricsConnection.CallTracker[] {
      METRICS.getGetTracker(), METRICS.getScanTracker(), METRICS.getMultiTracker(),
      METRICS.getAppendTracker(), METRICS.getDeleteTracker(), METRICS.getIncrementTracker(),
      METRICS.getPutTracker() }) {
      assertEquals(loop, t.callTimer.getCount(), "Failed to invoke callTimer on " + t);
      assertEquals(loop, t.reqHist.getCount(), "Failed to invoke reqHist on " + t);
      assertEquals(loop, t.respHist.getCount(), "Failed to invoke respHist on " + t);
    }
    RatioGauge executorMetrics =
      (RatioGauge) METRICS.getMetricRegistry().getMetrics().get(METRICS.getExecutorPoolName());
    RatioGauge metaMetrics =
      (RatioGauge) METRICS.getMetricRegistry().getMetrics().get(METRICS.getMetaPoolName());
    assertEquals(Ratio.of(0, 3).getValue(), executorMetrics.getValue(), 0);
    assertEquals(Double.NaN, metaMetrics.getValue(), 0);
  }

  private void testRpcCallTableMetrics(String table, int expectedVal) {
    String metricKey;
    Timer timer;
    String numOpsSuffix = "_num_ops";
    String p95Suffix = "_95th_percentile";
    String p99Suffix = "_99th_percentile";
    String service = ClientService.getDescriptor().getName();
    for (String m : new String[] { "Get", "Scan", "Multi" }) {
      metricKey = "rpcCallDurationMs_" + service + "_" + m + "_" + table;
      timer = METRICS.getRpcTimers().get(metricKey);
      if (tableMetricsEnabled) {
        long numOps = timer.getCount();
        double p95 = timer.getSnapshot().get95thPercentile();
        double p99 = timer.getSnapshot().get99thPercentile();
        assertEquals(expectedVal, numOps,
          "metric: " + metricKey + numOpsSuffix + " val: " + numOps);
        assertTrue(p95 >= 0, "metric: " + metricKey + p95Suffix + " val: " + p95);
        assertTrue(p99 >= 0, "metric: " + metricKey + p99Suffix + " val: " + p99);
      } else {
        assertNull(timer);
      }
    }

    // Distinguish mutate types for mutate method.
    String mutateMethod = "Mutate";
    for (String mutationType : new String[] { "Append", "Delete", "Increment", "Put" }) {
      metricKey = "rpcCallDurationMs_" + service + "_" + mutateMethod + "(" + mutationType + ")"
        + "_" + table;
      timer = METRICS.getRpcTimers().get(metricKey);
      if (tableMetricsEnabled) {
        long numOps = timer.getCount();
        double p95 = timer.getSnapshot().get95thPercentile();
        double p99 = timer.getSnapshot().get99thPercentile();
        assertEquals(expectedVal, numOps,
          "metric: " + metricKey + numOpsSuffix + " val: " + numOps);
        assertTrue(p95 >= 0, "metric: " + metricKey + p95Suffix + " val: " + p95);
        assertTrue(p99 >= 0, "metric: " + metricKey + p99Suffix + " val: " + p99);
      } else {
        assertNull(timer);
      }
    }
  }

  private void testRpcCallMetrics(String table, int expectedVal) {
    final String rpcCountPrefix = "rpcCount_" + ClientService.getDescriptor().getName() + "_";
    final String rpcFailureCountPrefix =
      "rpcFailureCount_" + ClientService.getDescriptor().getName() + "_";
    String metricKey;
    long metricVal;
    Counter counter;

    for (String method : new String[] { "Get", "Scan", "Multi" }) {
      // rpc call count
      metricKey = rpcCountPrefix + method;
      metricVal = METRICS.getRpcCounters().get(metricKey).getCount();
      assertEquals(metricVal, expectedVal, "metric: " + metricKey + " val: " + metricVal);

      // rpc failure call
      metricKey = tableMetricsEnabled
        ? rpcFailureCountPrefix + method + "_" + table
        : rpcFailureCountPrefix + method;
      counter = METRICS.getRpcCounters().get(metricKey);
      metricVal = (counter != null) ? counter.getCount() : 0;
      if (method.equals("Get")) {
        // no failure
        assertEquals(0, metricVal, "metric: " + metricKey + " val: " + metricVal);
      } else {
        // has failure
        assertEquals(metricVal, expectedVal, "metric: " + metricKey + " val: " + metricVal);
      }
    }

    String method = "Mutate";
    for (String mutationType : new String[] { "Append", "Delete", "Increment", "Put" }) {
      // rpc call count
      metricKey = rpcCountPrefix + method + "(" + mutationType + ")";
      metricVal = METRICS.getRpcCounters().get(metricKey).getCount();
      assertEquals(metricVal, expectedVal, "metric: " + metricKey + " val: " + metricVal);

      // rpc failure call
      metricKey = tableMetricsEnabled
        ? rpcFailureCountPrefix + method + "(" + mutationType + ")" + "_" + table
        : rpcFailureCountPrefix + method + "(" + mutationType + ")";
      counter = METRICS.getRpcCounters().get(metricKey);
      metricVal = (counter != null) ? counter.getCount() : 0;
      if (mutationType.equals("Put")) {
        // has failure
        assertEquals(metricVal, expectedVal, "metric: " + metricKey + " val: " + metricVal);
      } else {
        // no failure
        assertEquals(0, metricVal, "metric: " + metricKey + " val: " + metricVal);
      }
    }
  }
}
