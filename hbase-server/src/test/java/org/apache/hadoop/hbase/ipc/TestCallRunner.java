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

import static org.apache.hadoop.hbase.client.trace.hamcrest.AttributesMatchers.containsEntry;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasEnded;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasEvents;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasName;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasStatusWithCode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasItem;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.junit4.OpenTelemetryRule;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CallDroppedException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.MatcherPredicate;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.trace.hamcrest.EventMatchers;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandlerImpl;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;

@Category({RPCTests.class, SmallTests.class})
public class TestCallRunner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCallRunner.class);

  @Rule
  public TestName testName = new TestName();

  @Rule
  public OpenTelemetryRule otelRule = OpenTelemetryRule.create();

  private Configuration conf = null;

  @Before
  public void before() {
    final HBaseTestingUtil util = new HBaseTestingUtil();
    conf = util.getConfiguration();
  }

  /**
   * Does nothing but exercise a {@link CallRunner} outside of {@link RpcServer} context.
   */
  @Test
  public void testSimpleCall() {
    RpcServerInterface mockRpcServer = Mockito.mock(RpcServerInterface.class);
    Mockito.when(mockRpcServer.isStarted()).thenReturn(true);
    ServerCall<?> mockCall = Mockito.mock(ServerCall.class);

    TraceUtil.trace(() -> {
      CallRunner cr = new CallRunner(mockRpcServer, mockCall);
      cr.setStatus(new MonitoredRPCHandlerImpl());
      cr.run();
    }, testName.getMethodName());

    Waiter.waitFor(conf, TimeUnit.SECONDS.toMillis(5), new MatcherPredicate<>(
      otelRule::getSpans, hasItem(allOf(
        hasName(testName.getMethodName()),
        hasEnded()))));

    assertThat(otelRule.getSpans(), hasItem(allOf(
      hasName(testName.getMethodName()),
      hasStatusWithCode(StatusCode.OK),
      hasEnded())));
  }

  @Test
  public void testCallCleanup() {
    RpcServerInterface mockRpcServer = Mockito.mock(RpcServerInterface.class);
    Mockito.when(mockRpcServer.isStarted()).thenReturn(true);
    ServerCall<?> mockCall = Mockito.mock(ServerCall.class);
    Mockito.when(mockCall.disconnectSince()).thenReturn(1L);

    TraceUtil.trace(() -> {
      CallRunner cr = new CallRunner(mockRpcServer, mockCall);
      cr.setStatus(new MonitoredRPCHandlerImpl());
      cr.run();
    }, testName.getMethodName());
    Mockito.verify(mockCall, Mockito.times(1)).cleanup();
  }

  @Test
  public void testCallRunnerDropDisconnected() {
    RpcServerInterface mockRpcServer = Mockito.mock(RpcServerInterface.class);
    Mockito.when(mockRpcServer.isStarted()).thenReturn(true);
    ServerCall<?> mockCall = Mockito.mock(ServerCall.class);
    Mockito.when(mockCall.disconnectSince()).thenReturn(1L);

    TraceUtil.trace(() -> {
      CallRunner cr = new CallRunner(mockRpcServer, mockCall);
      cr.setStatus(new MonitoredRPCHandlerImpl());
      cr.drop();
    }, testName.getMethodName());
    Mockito.verify(mockCall, Mockito.times(1)).cleanup();

    Waiter.waitFor(conf, TimeUnit.SECONDS.toMillis(5), new MatcherPredicate<>(
      otelRule::getSpans, hasItem(allOf(
      hasName(testName.getMethodName()),
      hasEnded()))));

    assertThat(otelRule.getSpans(), hasItem(allOf(
      hasName(testName.getMethodName()),
      hasStatusWithCode(StatusCode.OK),
      hasEvents(hasItem(EventMatchers.hasName("Client disconnect detected"))),
      hasEnded())));
  }

  @Test
  public void testCallRunnerDropConnected() {
    RpcServerInterface mockRpcServer = Mockito.mock(RpcServerInterface.class);
    MetricsHBaseServer mockMetrics = Mockito.mock(MetricsHBaseServer.class);
    Mockito.when(mockRpcServer.getMetrics()).thenReturn(mockMetrics);
    Mockito.when(mockRpcServer.isStarted()).thenReturn(true);
    Mockito.when(mockRpcServer.getListenerAddress())
      .thenReturn(InetSocketAddress.createUnresolved("foo", 60020));
    ServerCall<?> mockCall = Mockito.mock(ServerCall.class);
    Mockito.when(mockCall.disconnectSince()).thenReturn(-1L);

    TraceUtil.trace(() -> {
      CallRunner cr = new CallRunner(mockRpcServer, mockCall);
      cr.setStatus(new MonitoredRPCHandlerImpl());
      cr.drop();
    }, testName.getMethodName());
    Mockito.verify(mockCall, Mockito.times(1)).cleanup();
    Mockito.verify(mockMetrics).exception(Mockito.any(CallDroppedException.class));

    Waiter.waitFor(conf, TimeUnit.SECONDS.toMillis(5), new MatcherPredicate<>(
      otelRule::getSpans, hasItem(allOf(
      hasName(testName.getMethodName()),
      hasEnded()))));

    assertThat(otelRule.getSpans(), hasItem(allOf(
      hasName(testName.getMethodName()),
      hasStatusWithCode(StatusCode.ERROR),
      hasEvents(hasItem(allOf(
        EventMatchers.hasName("exception"),
        EventMatchers.hasAttributes(
          containsEntry("exception.type", CallDroppedException.class.getName()))))),
      hasEnded())));
  }
}
