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

import static org.apache.hadoop.hbase.client.RegionReplicaTestHelper.testLocator;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasEnded;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasKind;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasName;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasParentSpanId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasItem;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MatcherPredicate;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionReplicaTestHelper.Locator;
import org.apache.hadoop.hbase.client.trace.StringTraceRenderer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Tag(MediumTests.TAG)
@Tag(ClientTests.TAG)
@HBaseParameterizedTestTemplate(name = "[{index}]: registry = {0}")
public class TestAsyncMetaRegionLocator {

  private static final Logger LOG = LoggerFactory.getLogger(TestAsyncMetaRegionLocator.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @RegisterExtension
  private static final OpenTelemetryExtension OTEL_EXT = OpenTelemetryExtension.create();

  private Class<? extends ConnectionRegistry> registryClass;

  private ConnectionRegistry registry;

  private AsyncMetaRegionLocator locator;

  public TestAsyncMetaRegionLocator(Class<? extends ConnectionRegistry> registryClass) {
    this.registryClass = registryClass;
  }

  @SuppressWarnings("deprecation")
  public static Stream<Arguments> parameters() {
    return Stream.of(Arguments.of(RpcConnectionRegistry.class), Arguments.of(MasterRegistry.class),
      Arguments.of(ZKConnectionRegistry.class));
  }

  @BeforeAll
  public static void setUpBeforeAll() throws Exception {
    UTIL.startMiniCluster(3);
    HBaseTestingUtility.setReplicas(UTIL.getAdmin(), TableName.META_TABLE_NAME, 3);
    UTIL.waitUntilNoRegionsInTransition();
    try (ConnectionRegistry registry =
      ConnectionRegistryFactory.getRegistry(UTIL.getConfiguration())) {
      RegionReplicaTestHelper.waitUntilAllMetaReplicasAreReady(UTIL, registry);
    }
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterAll
  public static void tearDownAfterAll() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @BeforeEach
  public void setUp() throws IOException {
    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.setClass(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY, registryClass,
      ConnectionRegistry.class);
    registry = ConnectionRegistryFactory.getRegistry(conf);
    locator = new AsyncMetaRegionLocator(registry);
  }

  @AfterEach
  public void tearDown() throws IOException {
    Closeables.close(registry, true);
  }

  @TestTemplate
  public void test(TestInfo testInfo) {
    String methodName = testInfo.getTestMethod().get().getName();
    TraceUtil.trace(() -> {
      try {
        testLocator(UTIL, TableName.META_TABLE_NAME, new Locator() {
          @Override
          public void updateCachedLocationOnError(HRegionLocation loc, Throwable error) {
            locator.updateCachedLocationOnError(loc, error);
          }

          @Override
          public RegionLocations getRegionLocations(TableName tableName, int replicaId,
            boolean reload) throws Exception {
            return locator.getRegionLocations(replicaId, reload).get();
          }
        });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, methodName);

    Matcher<SpanData> parentSpanMatcher = allOf(hasName(methodName), hasEnded());

    UTIL.waitFor(TimeUnit.SECONDS.toMillis(5),
      new MatcherPredicate<>(OTEL_EXT::getSpans, hasItem(parentSpanMatcher)));
    List<SpanData> spans = OTEL_EXT.getSpans();
    if (LOG.isDebugEnabled()) {
      StringTraceRenderer renderer = new StringTraceRenderer(spans);
      renderer.render(LOG::debug);
    }
    assertThat(spans, hasItem(parentSpanMatcher));
    final SpanData parentSpan =
      spans.stream().filter(parentSpanMatcher::matches).findAny().orElseThrow(AssertionError::new);

    Matcher<SpanData> registryGetMetaRegionLocationsMatcher =
      allOf(hasName(endsWith(registryClass.getSimpleName() + ".getMetaRegionLocations")),
        hasParentSpanId(parentSpan), hasKind(SpanKind.INTERNAL), hasEnded());
    assertThat(spans, hasItem(registryGetMetaRegionLocationsMatcher));

    // RpcConnectionRegistry specific tracing assertions
    if (registry instanceof RpcConnectionRegistry) {
      SpanData registryGetMetaRegionLocationsSpan =
        spans.stream().filter(registryGetMetaRegionLocationsMatcher::matches).findFirst()
          .orElseThrow(AssertionError::new);
      Matcher<SpanData> clientGetMetaRegionLocationsMatcher = allOf(
        hasName(endsWith("ClientMetaService/GetMetaRegionLocations")),
        hasParentSpanId(registryGetMetaRegionLocationsSpan), hasKind(SpanKind.CLIENT), hasEnded());
      assertThat(spans, hasItem(clientGetMetaRegionLocationsMatcher));
    }
  }
}
