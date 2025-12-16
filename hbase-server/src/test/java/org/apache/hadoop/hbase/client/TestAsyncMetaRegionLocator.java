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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MatcherPredicate;
import org.apache.hadoop.hbase.MetaTableName;
import org.apache.hadoop.hbase.MiniClusterRule;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.RegionReplicaTestHelper.Locator;
import org.apache.hadoop.hbase.client.trace.StringTraceRenderer;
import org.apache.hadoop.hbase.security.User;
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
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Enclosed.class)
public class TestAsyncMetaRegionLocator {
  private static final Logger logger = LoggerFactory.getLogger(TestAsyncMetaRegionLocator.class);

  private static final class Setup extends ExternalResource {

    private final MiniClusterRule miniClusterRule;
    private final ConnectionRule connectionRule;

    private boolean initialized = false;
    private HBaseTestingUtil testUtil;
    private AsyncMetaRegionLocator locator;
    private ConnectionRegistry registry;

    public Setup(final ConnectionRule connectionRule, final MiniClusterRule miniClusterRule) {
      this.connectionRule = connectionRule;
      this.miniClusterRule = miniClusterRule;
    }

    public HBaseTestingUtil getTestingUtility() {
      assertInitialized();
      return testUtil;
    }

    public AsyncMetaRegionLocator getLocator() {
      assertInitialized();
      return locator;
    }

    private void assertInitialized() {
      if (!initialized) {
        throw new IllegalStateException("before method has not been called.");
      }
    }

    @Override
    protected void before() throws Throwable {
      final AsyncAdmin admin = connectionRule.getAsyncConnection().getAdmin();
      testUtil = miniClusterRule.getTestingUtility();
      HBaseTestingUtil.setReplicas(admin, MetaTableName.getInstance(), 3);
      testUtil.waitUntilNoRegionsInTransition();
      registry = ConnectionRegistryFactory.create(testUtil.getConfiguration(), User.getCurrent());
      RegionReplicaTestHelper.waitUntilAllMetaReplicasAreReady(testUtil, registry);
      admin.balancerSwitch(false).get();
      locator = new AsyncMetaRegionLocator(registry);
      initialized = true;
    }

    @Override
    protected void after() {
      registry.close();
    }
  }

  public static abstract class AbstractBase {
    private final OpenTelemetryClassRule otelClassRule = OpenTelemetryClassRule.create();
    private final MiniClusterRule miniClusterRule;
    private final Setup setup;

    protected Matcher<SpanData> parentSpanMatcher;
    protected List<SpanData> spans;
    protected Matcher<SpanData> registryGetMetaRegionLocationsMatcher;

    @Rule
    public final TestRule classRule;

    @Rule
    public final OpenTelemetryTestRule otelTestRule = new OpenTelemetryTestRule(otelClassRule);

    @Rule
    public TestName testName = new TestName();

    public AbstractBase() {
      miniClusterRule = MiniClusterRule.newBuilder()
        .setMiniClusterOption(StartTestingClusterOption.builder().numWorkers(3).build())
        .setConfiguration(() -> {
          final Configuration conf = HBaseConfiguration.create();
          conf.setClass(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
            getConnectionRegistryClass(), ConnectionRegistry.class);
          return conf;
        }).build();
      final ConnectionRule connectionRule =
        ConnectionRule.createAsyncConnectionRule(miniClusterRule::createAsyncConnection);
      setup = new Setup(connectionRule, miniClusterRule);
      classRule = RuleChain.outerRule(otelClassRule).around(miniClusterRule).around(connectionRule)
        .around(setup);
    }

    protected abstract Class<? extends ConnectionRegistry> getConnectionRegistryClass();

    @Test
    public void test() throws Exception {
      final AsyncMetaRegionLocator locator = setup.getLocator();
      final HBaseTestingUtil testUtil = setup.getTestingUtility();

      TraceUtil.trace(() -> {
        try {
          testLocator(miniClusterRule.getTestingUtility(), MetaTableName.getInstance(),
            new Locator() {
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
      }, testName.getMethodName());

      final Configuration conf = testUtil.getConfiguration();
      parentSpanMatcher = allOf(hasName(testName.getMethodName()), hasEnded());
      Waiter.waitFor(conf, TimeUnit.SECONDS.toMillis(5),
        new MatcherPredicate<>(otelClassRule::getSpans, hasItem(parentSpanMatcher)));
      spans = otelClassRule.getSpans();
      if (logger.isDebugEnabled()) {
        StringTraceRenderer renderer = new StringTraceRenderer(spans);
        renderer.render(logger::debug);
      }
      assertThat(spans, hasItem(parentSpanMatcher));
      final SpanData parentSpan = spans.stream().filter(parentSpanMatcher::matches).findAny()
        .orElseThrow(AssertionError::new);

      registryGetMetaRegionLocationsMatcher =
        allOf(hasName(endsWith("ConnectionRegistry.getMetaRegionLocations")),
          hasParentSpanId(parentSpan), hasKind(SpanKind.INTERNAL), hasEnded());
      assertThat(spans, hasItem(registryGetMetaRegionLocationsMatcher));
    }
  }

  /**
   * Test covers when client is configured with {@link ZKConnectionRegistry}.
   */
  @Category({ MediumTests.class, ClientTests.class })
  public static class TestZKConnectionRegistry extends AbstractBase {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestZKConnectionRegistry.class);

    @Override
    protected Class<? extends ConnectionRegistry> getConnectionRegistryClass() {
      return ZKConnectionRegistry.class;
    }
  }

  /**
   * Test covers when client is configured with {@link RpcConnectionRegistry}.
   */
  @Category({ MediumTests.class, ClientTests.class })
  public static class TestRpcConnectionRegistry extends AbstractBase {
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRpcConnectionRegistry.class);

    @Override
    protected Class<? extends ConnectionRegistry> getConnectionRegistryClass() {
      return RpcConnectionRegistry.class;
    }

    @Test
    @Override
    public void test() throws Exception {
      super.test();
      final SpanData registry_getMetaRegionLocationsSpan =
        spans.stream().filter(registryGetMetaRegionLocationsMatcher::matches).findAny()
          .orElseThrow(AssertionError::new);
      final Matcher<SpanData> clientGetMetaRegionLocationsMatcher = allOf(
        hasName(endsWith("ClientMetaService/GetMetaRegionLocations")),
        hasParentSpanId(registry_getMetaRegionLocationsSpan), hasKind(SpanKind.CLIENT), hasEnded());
      assertThat(spans, hasItem(clientGetMetaRegionLocationsMatcher));
    }
  }
}
