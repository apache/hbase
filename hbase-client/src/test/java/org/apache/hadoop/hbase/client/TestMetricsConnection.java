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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.codahale.metrics.Counter;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.RatioGauge.Ratio;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MetricsTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;

@Category({ ClientTests.class, MetricsTests.class, SmallTests.class })
public class TestMetricsConnection {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMetricsConnection.class);

  private static MetricsConnection METRICS;
  private static final ThreadPoolExecutor BATCH_POOL =
    (ThreadPoolExecutor) Executors.newFixedThreadPool(2);

  private static final String MOCK_CONN_STR = "mocked-connection";

  @BeforeClass
  public static void beforeClass() {
    METRICS = MetricsConnection.getMetricsConnection(MOCK_CONN_STR, () -> BATCH_POOL, () -> null);
  }

  @AfterClass
  public static void afterClass() {
    MetricsConnection.deleteMetricsConnection(MOCK_CONN_STR);
  }

  @Test
  public void testMetricsConnectionScopeAsyncClient() throws IOException {
    Configuration conf = new Configuration();
    String clusterId = "foo";
    String scope = "testScope";
    conf.setBoolean(MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY, true);

    AsyncConnectionImpl impl = new AsyncConnectionImpl(conf, null, "foo", User.getCurrent());
    Optional<MetricsConnection> metrics = impl.getConnectionMetrics();
    assertTrue("Metrics should be present", metrics.isPresent());
    assertEquals(clusterId + "@" + Integer.toHexString(impl.hashCode()),
      metrics.get().getMetricScope());
    conf.set(MetricsConnection.METRICS_SCOPE_KEY, scope);
    impl = new AsyncConnectionImpl(conf, null, "foo", User.getCurrent());

    metrics = impl.getConnectionMetrics();
    assertTrue("Metrics should be present", metrics.isPresent());
    assertEquals(scope, metrics.get().getMetricScope());
  }

  @Test
  public void testMetricsWithMutiConnections() throws IOException {
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
    assertTrue("Metrics should be present", metrics.isPresent());

    /* verify connection count in a shared metrics */
    long count = metrics.get().getConnectionCount();
    assertEquals("Failed to verify connection count." + count, count, num);

    /* close some connections */
    for (int i = 0; i < num - 1; i++) {
      connList.get(i).close();
    }

    /* verify metrics presence again */
    impl = connList.get(num - 1);
    metrics = impl.getConnectionMetrics();
    assertTrue("Metrics should be present after some of connections are closed.",
      metrics.isPresent());

    /* verify count of remaining connections */
    count = metrics.get().getConnectionCount();
    assertEquals("Connection count suppose to be 1 but got: " + count, count, 1);

    /* shutdown */
    impl.close();
  }

  @Test
  public void testMetricsConnectionScopeBlockingClient() throws IOException {
    Configuration conf = new Configuration();
    String clusterId = "foo";
    String scope = "testScope";
    conf.setBoolean(MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY, true);

    ConnectionRegistry mockRegistry = Mockito.mock(ConnectionRegistry.class);
    Mockito.when(mockRegistry.getClusterId())
      .thenReturn(CompletableFuture.completedFuture(clusterId));

    ConnectionImplementation impl =
      new ConnectionImplementation(conf, null, User.getCurrent(), mockRegistry);
    MetricsConnection metrics = impl.getConnectionMetrics();
    assertNotNull("Metrics should be present", metrics);
    assertEquals(clusterId + "@" + Integer.toHexString(impl.hashCode()), metrics.getMetricScope());
    conf.set(MetricsConnection.METRICS_SCOPE_KEY, scope);
    impl = new ConnectionImplementation(conf, null, User.getCurrent(), mockRegistry);

    metrics = impl.getConnectionMetrics();
    assertNotNull("Metrics should be present", metrics);
    assertEquals(scope, metrics.getMetricScope());
  }

  @Test
  public void testStaticMetrics() throws IOException {
    final byte[] foo = Bytes.toBytes("foo");
    final RegionSpecifier region = RegionSpecifier.newBuilder().setValue(ByteString.EMPTY)
      .setType(RegionSpecifierType.REGION_NAME).build();
    final int loop = 5;

    for (int i = 0; i < loop; i++) {
      METRICS.updateRpc(ClientService.getDescriptor().findMethodByName("Get"),
        GetRequest.getDefaultInstance(), MetricsConnection.newCallStats(), null);
      METRICS.updateRpc(ClientService.getDescriptor().findMethodByName("Scan"),
        ScanRequest.getDefaultInstance(), MetricsConnection.newCallStats(),
        new RemoteWithExtrasException("java.io.IOException", null, false, false));
      METRICS.updateRpc(ClientService.getDescriptor().findMethodByName("Multi"),
        MultiRequest.getDefaultInstance(), MetricsConnection.newCallStats(),
        new CallTimeoutException("test with CallTimeoutException"));
      METRICS.updateRpc(ClientService.getDescriptor().findMethodByName("Mutate"),
        MutateRequest.newBuilder()
          .setMutation(ProtobufUtil.toMutation(MutationType.APPEND, new Append(foo)))
          .setRegion(region).build(),
        MetricsConnection.newCallStats(), null);
      METRICS.updateRpc(ClientService.getDescriptor().findMethodByName("Mutate"),
        MutateRequest.newBuilder()
          .setMutation(ProtobufUtil.toMutation(MutationType.DELETE, new Delete(foo)))
          .setRegion(region).build(),
        MetricsConnection.newCallStats(), null);
      METRICS.updateRpc(ClientService.getDescriptor().findMethodByName("Mutate"),
        MutateRequest.newBuilder()
          .setMutation(ProtobufUtil.toMutation(MutationType.INCREMENT, new Increment(foo)))
          .setRegion(region).build(),
        MetricsConnection.newCallStats(), null);
      METRICS.updateRpc(ClientService.getDescriptor().findMethodByName("Mutate"),
        MutateRequest.newBuilder()
          .setMutation(ProtobufUtil.toMutation(MutationType.PUT, new Put(foo))).setRegion(region)
          .build(),
        MetricsConnection.newCallStats(), null);
    }

    final String rpcCountPrefix = "rpcCount_" + ClientService.getDescriptor().getName() + "_";
    final String rpcFailureCountPrefix =
      "rpcFailureCount_" + ClientService.getDescriptor().getName() + "_";
    String metricKey;
    long metricVal;
    Counter counter;

    for (String method : new String[] { "Get", "Scan", "Multi", "Mutate" }) {
      metricKey = rpcCountPrefix + method;
      metricVal = METRICS.getRpcCounters().get(metricKey).getCount();
      assertTrue("metric: " + metricKey + " val: " + metricVal, metricVal >= loop);

      metricKey = rpcFailureCountPrefix + method;
      counter = METRICS.getRpcCounters().get(metricKey);
      metricVal = (counter != null) ? counter.getCount() : 0;
      if (method.equals("Get") || method.equals("Mutate")) {
        // no failure
        assertTrue("metric: " + metricKey + " val: " + metricVal, metricVal == 0);
      } else {
        // has failure
        assertTrue("metric: " + metricKey + " val: " + metricVal, metricVal == loop);
      }
    }

    // remote exception
    metricKey = "rpcRemoteExceptions_IOException";
    counter = METRICS.getRpcCounters().get(metricKey);
    metricVal = (counter != null) ? counter.getCount() : 0;
    assertTrue("metric: " + metricKey + " val: " + metricVal, metricVal == loop);

    // local exception
    metricKey = "rpcLocalExceptions_CallTimeoutException";
    counter = METRICS.getRpcCounters().get(metricKey);
    metricVal = (counter != null) ? counter.getCount() : 0;
    assertTrue("metric: " + metricKey + " val: " + metricVal, metricVal == loop);

    // total exception
    metricKey = "rpcTotalExceptions";
    counter = METRICS.getRpcCounters().get(metricKey);
    metricVal = (counter != null) ? counter.getCount() : 0;
    assertTrue("metric: " + metricKey + " val: " + metricVal, metricVal == loop * 2);

    for (MetricsConnection.CallTracker t : new MetricsConnection.CallTracker[] {
      METRICS.getGetTracker(), METRICS.getScanTracker(), METRICS.getMultiTracker(),
      METRICS.getAppendTracker(), METRICS.getDeleteTracker(), METRICS.getIncrementTracker(),
      METRICS.getPutTracker() }) {
      assertEquals("Failed to invoke callTimer on " + t, loop, t.callTimer.getCount());
      assertEquals("Failed to invoke reqHist on " + t, loop, t.reqHist.getCount());
      assertEquals("Failed to invoke respHist on " + t, loop, t.respHist.getCount());
    }
    RatioGauge executorMetrics =
      (RatioGauge) METRICS.getMetricRegistry().getMetrics().get(METRICS.getExecutorPoolName());
    RatioGauge metaMetrics =
      (RatioGauge) METRICS.getMetricRegistry().getMetrics().get(METRICS.getMetaPoolName());
    assertEquals(Ratio.of(0, 3).getValue(), executorMetrics.getValue(), 0);
    assertEquals(Double.NaN, metaMetrics.getValue(), 0);
  }
}
