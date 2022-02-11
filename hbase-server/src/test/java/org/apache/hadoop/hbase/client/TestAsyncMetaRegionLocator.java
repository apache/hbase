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
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ConnectionRule;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MatcherPredicate;
import org.apache.hadoop.hbase.MiniClusterRule;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.RegionReplicaTestHelper.Locator;
import org.apache.hadoop.hbase.client.trace.StringTraceRenderer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.trace.OpenTelemetryClassRule;
import org.apache.hadoop.hbase.trace.OpenTelemetryTestRule;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.hamcrest.Matcher;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncMetaRegionLocator {
  private static final Logger logger = LoggerFactory.getLogger(TestAsyncMetaRegionLocator.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncMetaRegionLocator.class);

  private static final OpenTelemetryClassRule otelClassRule = OpenTelemetryClassRule.create();
  private static final MiniClusterRule miniClusterRule = MiniClusterRule.newBuilder()
    .setMiniClusterOption(StartTestingClusterOption.builder()
      .numWorkers(3)
      .build())
    .build();
  private static final ConnectionRule connectionRule =
    ConnectionRule.createAsyncConnectionRule(miniClusterRule::createAsyncConnection);

  private static final class Setup extends ExternalResource {
    private ConnectionRegistry registry;

    @Override
    protected void before() throws Throwable {
      final AsyncAdmin admin = connectionRule.getAsyncConnection().getAdmin();
      TEST_UTIL = miniClusterRule.getTestingUtility();
      HBaseTestingUtil.setReplicas(admin, TableName.META_TABLE_NAME, 3);
      TEST_UTIL.waitUntilNoRegionsInTransition();
      registry = ConnectionRegistryFactory.getRegistry(TEST_UTIL.getConfiguration());
      RegionReplicaTestHelper.waitUntilAllMetaReplicasAreReady(TEST_UTIL, registry);
      admin.balancerSwitch(false).get();
      LOCATOR = new AsyncMetaRegionLocator(registry);
    }

    @Override
    protected void after() {
      registry.close();
    }
  }

  @ClassRule
  public static final TestRule classRule = RuleChain.outerRule(otelClassRule)
    .around(miniClusterRule)
    .around(connectionRule)
    .around(new Setup());

  private static HBaseTestingUtil TEST_UTIL;
  private static AsyncMetaRegionLocator LOCATOR;

  @Rule
  public final OpenTelemetryTestRule otelTestRule = new OpenTelemetryTestRule(otelClassRule);

  @Test
  public void test() throws Exception {
    TraceUtil.trace(() -> {
      try {
        testLocator(miniClusterRule.getTestingUtility(), TableName.META_TABLE_NAME, new Locator() {
          @Override
          public void updateCachedLocationOnError(HRegionLocation loc, Throwable error) {
            LOCATOR.updateCachedLocationOnError(loc, error);
          }

          @Override
          public RegionLocations getRegionLocations(
            TableName tableName,
            int replicaId,
            boolean reload
          ) throws Exception {
            return LOCATOR.getRegionLocations(replicaId, reload).get();
          }
        });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, "test");

    final Configuration conf = TEST_UTIL.getConfiguration();
    final Matcher<SpanData> parentSpanMatcher = allOf(hasName("test"), hasEnded());
    Waiter.waitFor(conf, TimeUnit.SECONDS.toMillis(5), new MatcherPredicate<>(
      otelClassRule::getSpans, hasItem(parentSpanMatcher)));
    final List<SpanData> spans = otelClassRule.getSpans();
    if (logger.isDebugEnabled()) {
      StringTraceRenderer renderer = new StringTraceRenderer(spans);
      renderer.render(logger::debug);
    }

    assertThat(spans, hasItem(parentSpanMatcher));
    final SpanData parentSpan = spans.stream()
      .filter(parentSpanMatcher::matches)
      .findAny()
      .orElseThrow(AssertionError::new);

    final Matcher<SpanData> registryGetMetaRegionLocationsMatcher = allOf(
      hasName(endsWith("ConnectionRegistry.getMetaRegionLocations")),
      hasParentSpanId(parentSpan),
      hasKind(SpanKind.INTERNAL),
      hasEnded());
    assertThat(spans, hasItem(registryGetMetaRegionLocationsMatcher));
    final SpanData registry_getMetaRegionLocationsSpan = spans.stream()
      .filter(registryGetMetaRegionLocationsMatcher::matches)
      .findAny()
      .orElseThrow(AssertionError::new);

    final Matcher<SpanData> clientGetMetaRegionLocationsMatcher = allOf(
      hasName(endsWith("ClientMetaService/GetMetaRegionLocations")),
      hasParentSpanId(registry_getMetaRegionLocationsSpan),
      hasKind(SpanKind.CLIENT),
      hasEnded());
    assertThat(spans, hasItem(clientGetMetaRegionLocationsMatcher));
  }
}
