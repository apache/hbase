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
package org.apache.hadoop.hbase;

import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasName;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasParentSpanId;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasStatusWithCode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.startsWith;

import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.client.trace.StringTraceRenderer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.trace.OpenTelemetryClassRule;
import org.hamcrest.Matcher;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that sundry operations internal to the region server are traced as expected.
 */
@Category({ MediumTests.class, RegionServerTests.class })
public class TestServerInternalsTracing {
  private static final Logger LOG = LoggerFactory.getLogger(TestServerInternalsTracing.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestServerInternalsTracing.class);

  private static final String NO_PARENT_ID = "0000000000000000";
  private static List<SpanData> spans;

  /**
   * Wait for the underlying cluster to come up -- defined by meta being available.
   */
  private static class Setup extends ExternalResource {
    private final Supplier<HBaseTestingUtil> testingUtilSupplier;

    public Setup(final Supplier<HBaseTestingUtil> testingUtilSupplier) {
      this.testingUtilSupplier = testingUtilSupplier;
    }

    @Override
    protected void before() throws Throwable {
      final HBaseTestingUtil testingUtil = testingUtilSupplier.get();
      testingUtil.waitTableAvailable(connection.getMetaTableName());
    }
  }

  private static class Noop extends Statement {
    @Override
    public void evaluate() throws Throwable {
    }
  }

  @ClassRule
  public static TestRule classRule = (base, description) -> new Statement() {
    @Override
    public void evaluate() throws Throwable {
      // setup and tear down the cluster, collecting all the spans produced in the process.
      final OpenTelemetryClassRule otelClassRule = OpenTelemetryClassRule.create();
      final MiniClusterRule miniClusterRule = MiniClusterRule.newBuilder().build();
      final Setup setup = new Setup(miniClusterRule::getTestingUtility);
      final TestRule clusterRule =
        RuleChain.outerRule(otelClassRule).around(miniClusterRule).around(setup);
      clusterRule.apply(new Noop(), description).evaluate();
      spans = otelClassRule.getSpans();
      if (LOG.isDebugEnabled()) {
        StringTraceRenderer renderer = new StringTraceRenderer(spans);
        renderer.render(LOG::debug);
      }
      base.evaluate();
    }
  };

  @Test
  public void testHBaseServerBaseConstructor() {
    final Matcher<SpanData> hbaseServerBaseMatcher = allOf(hasName("HBaseServerBase.cxtor"),
      hasParentSpanId(NO_PARENT_ID), hasStatusWithCode(isOneOf(StatusCode.OK, StatusCode.ERROR)));
    assertThat("there should be a span from the HBaseServerBase constructor.", spans,
      hasItem(hbaseServerBaseMatcher));
    final List<SpanData> hbaseServerBaseSpans =
      spans.stream().filter(hbaseServerBaseMatcher::matches).collect(Collectors.toList());
    assertThat("there should be two spans from the HBaseServerBase constructor, one for the master"
      + " and one for the region server.", hbaseServerBaseSpans, hasSize(2));
    assertThat("both should show zookeeper interaction.", spans,
      hasItems(
        allOf(hasParentSpanId(hbaseServerBaseSpans.get(0)),
          hasName(containsString("RecoverableZookeeper."))),
        allOf(hasParentSpanId(hbaseServerBaseSpans.get(1)),
          hasName(containsString("RecoverableZookeeper.")))));
  }

  @Test
  public void testHMasterConstructor() {
    final Matcher<SpanData> masterConstructorMatcher = allOf(hasName("HMaster.cxtor"),
      hasParentSpanId(NO_PARENT_ID), hasStatusWithCode(isOneOf(StatusCode.OK, StatusCode.ERROR)));
    assertThat("there should be a span from the HMaster constructor.", spans,
      hasItem(masterConstructorMatcher));
    final SpanData masterConstructorSpan = spans.stream().filter(masterConstructorMatcher::matches)
      .findAny().orElseThrow(AssertionError::new);
    assertThat("the HMaster constructor span should show zookeeper interaction.", spans, hasItem(
      allOf(hasName(startsWith("RecoverableZookeeper.")), hasParentSpanId(masterConstructorSpan))));
  }

  @Test
  public void testHMasterBecomeActiveMaster() {
    final Matcher<SpanData> masterBecomeActiveMasterMatcher =
      allOf(hasName("HMaster.becomeActiveMaster"), hasParentSpanId(NO_PARENT_ID),
        hasStatusWithCode(isOneOf(StatusCode.OK, StatusCode.ERROR)));
    assertThat("there should be a span from the HMaster.becomeActiveMaster.", spans,
      hasItem(masterBecomeActiveMasterMatcher));
    final SpanData masterBecomeActiveMasterSpan = spans.stream()
      .filter(masterBecomeActiveMasterMatcher::matches).findAny().orElseThrow(AssertionError::new);
    assertThat("the HMaster.becomeActiveMaster span should show zookeeper interaction.", spans,
      hasItem(allOf(hasName(startsWith("RecoverableZookeeper.")),
        hasParentSpanId(masterBecomeActiveMasterSpan))));
    assertThat("the HMaster.becomeActiveMaster span should show Region interaction.", spans,
      hasItem(
        allOf(hasName(startsWith("Region.")), hasParentSpanId(masterBecomeActiveMasterSpan))));
    assertThat("the HMaster.becomeActiveMaster span should show RegionScanner interaction.", spans,
      hasItem(allOf(hasName(startsWith("RegionScanner.")),
        hasParentSpanId(masterBecomeActiveMasterSpan))));
    assertThat("the HMaster.becomeActiveMaster span should show hbase:meta interaction.", spans,
      hasItem(allOf(hasName(containsString("hbase:meta")),
        hasParentSpanId(masterBecomeActiveMasterSpan))));
    assertThat("the HMaster.becomeActiveMaster span should show WAL interaction.", spans,
      hasItem(allOf(hasName(startsWith("WAL.")), hasParentSpanId(masterBecomeActiveMasterSpan))));
  }

  @Test
  public void testZKWatcherHMaster() {
    final Matcher<SpanData> mZKWatcherMatcher = allOf(hasName(startsWith("ZKWatcher-master")),
      hasParentSpanId(NO_PARENT_ID), hasStatusWithCode(isOneOf(StatusCode.OK, StatusCode.ERROR)));
    assertThat("there should be a span from the ZKWatcher running in the HMaster.", spans,
      hasItem(mZKWatcherMatcher));
    final SpanData mZKWatcherSpan =
      spans.stream().filter(mZKWatcherMatcher::matches).findAny().orElseThrow(AssertionError::new);
    assertThat("the ZKWatcher running in the HMaster span should invoke processEvent.", spans,
      hasItem(allOf(hasName(containsString("processEvent")), hasParentSpanId(mZKWatcherSpan))));
  }

  @Test
  public void testHMasterShutdown() {
    final Matcher<SpanData> masterShutdownMatcher = allOf(hasName("HMaster.shutdown"),
      hasParentSpanId(NO_PARENT_ID), hasStatusWithCode(isOneOf(StatusCode.OK, StatusCode.ERROR)));
    assertThat("there should be a span from the HMaster.shutdown.", spans,
      hasItem(masterShutdownMatcher));
    final SpanData masterShutdownSpan = spans.stream().filter(masterShutdownMatcher::matches)
      .findAny().orElseThrow(AssertionError::new);
    assertThat("the HMaster.shutdown span should show zookeeper interaction.", spans, hasItem(
      allOf(hasName(startsWith("RecoverableZookeeper.")), hasParentSpanId(masterShutdownSpan))));
    assertThat("the HMaster.shutdown span should show AsyncConnection interaction.", spans,
      hasItem(allOf(hasName(startsWith("AsyncConnection.")), hasParentSpanId(masterShutdownSpan))));
  }

  @Test
  public void testHMasterExitingMainLoop() {
    final Matcher<SpanData> masterExitingMainLoopMatcher =
      allOf(hasName("HMaster exiting main loop"), hasParentSpanId(NO_PARENT_ID),
        hasStatusWithCode(isOneOf(StatusCode.OK, StatusCode.ERROR)));
    assertThat("there should be a span from the HMaster exiting main loop.", spans,
      hasItem(masterExitingMainLoopMatcher));
    final SpanData masterExitingMainLoopSpan = spans.stream()
      .filter(masterExitingMainLoopMatcher::matches).findAny().orElseThrow(AssertionError::new);
    assertThat("the HMaster exiting main loop span should show zookeeper interaction.", spans,
      hasItem(allOf(hasName(startsWith("RecoverableZookeeper.")),
        hasParentSpanId(masterExitingMainLoopSpan))));
    assertThat("the HMaster exiting main loop span should show WAL interaction.", spans,
      hasItem(allOf(hasName(startsWith("WAL.")), hasParentSpanId(masterExitingMainLoopSpan))));
    assertThat("the HMaster exiting main loop span should show AsyncConnection interaction.", spans,
      hasItem(allOf(hasName(startsWith("AsyncConnection.")),
        hasParentSpanId(masterExitingMainLoopSpan))));
  }

  @Test
  public void testTryRegionServerReport() {
    final Matcher<SpanData> tryRegionServerReportMatcher =
      allOf(hasName("HRegionServer.tryRegionServerReport"), hasParentSpanId(NO_PARENT_ID),
        hasStatusWithCode(isOneOf(StatusCode.OK, StatusCode.ERROR)));
    assertThat("there should be a span for the region server sending a report.", spans,
      hasItem(tryRegionServerReportMatcher));
    final SpanData tryRegionServerReportSpan = spans.stream()
      .filter(tryRegionServerReportMatcher::matches).findAny().orElseThrow(AssertionError::new);
    assertThat(
      "the region server report span should have an invocation of the RegionServerReport RPC.",
      spans, hasItem(allOf(hasName(endsWith("RegionServerStatusService/RegionServerReport")),
        hasParentSpanId(tryRegionServerReportSpan))));
  }

  @Test
  public void testHRegionServerStartup() {
    final Matcher<SpanData> regionServerStartupMatcher = allOf(hasName("HRegionServer.startup"),
      hasParentSpanId(NO_PARENT_ID), hasStatusWithCode(isOneOf(StatusCode.OK, StatusCode.ERROR)));
    assertThat("there should be a span from the HRegionServer startup procedure.", spans,
      hasItem(regionServerStartupMatcher));
    final SpanData regionServerStartupSpan = spans.stream()
      .filter(regionServerStartupMatcher::matches).findAny().orElseThrow(AssertionError::new);
    assertThat("the HRegionServer startup procedure span should show zookeeper interaction.", spans,
      hasItem(allOf(hasName(startsWith("RecoverableZookeeper.")),
        hasParentSpanId(regionServerStartupSpan))));
  }

  @Test
  public void testHRegionServerConstructor() {
    final Matcher<SpanData> rsConstructorMatcher = allOf(hasName("HRegionServer.cxtor"),
      hasParentSpanId(NO_PARENT_ID), hasStatusWithCode(isOneOf(StatusCode.OK, StatusCode.ERROR)));
    assertThat("there should be a span from the HRegionServer constructor.", spans,
      hasItem(rsConstructorMatcher));
    final SpanData rsConstructorSpan = spans.stream().filter(rsConstructorMatcher::matches)
      .findAny().orElseThrow(AssertionError::new);
    assertThat("the HRegionServer constructor span should show zookeeper interaction.", spans,
      hasItem(
        allOf(hasName(startsWith("RecoverableZookeeper.")), hasParentSpanId(rsConstructorSpan))));
    assertThat("the HRegionServer constructor span should invoke the MasterAddressTracker.", spans,
      hasItem(
        allOf(hasName(startsWith("MasterAddressTracker.")), hasParentSpanId(rsConstructorSpan))));
  }

  @Test
  public void testHRegionServerPreRegistrationInitialization() {
    final Matcher<SpanData> rsPreRegistrationInitializationMatcher =
      allOf(hasName("HRegionServer.preRegistrationInitialization"), hasParentSpanId(NO_PARENT_ID),
        hasStatusWithCode(isOneOf(StatusCode.OK, StatusCode.ERROR)));
    assertThat("there should be a span from the HRegionServer preRegistrationInitialization.",
      spans, hasItem(rsPreRegistrationInitializationMatcher));
    final SpanData rsPreRegistrationInitializationSpan =
      spans.stream().filter(rsPreRegistrationInitializationMatcher::matches).findAny()
        .orElseThrow(AssertionError::new);
    assertThat(
      "the HRegionServer preRegistrationInitialization span should show zookeeper interaction.",
      spans, hasItem(allOf(hasName(startsWith("RecoverableZookeeper.")),
        hasParentSpanId(rsPreRegistrationInitializationSpan))));
  }

  @Test
  public void testHRegionServerRegisterWithMaster() {
    final Matcher<SpanData> rsRegisterWithMasterMatcher =
      allOf(hasName("HRegionServer.registerWithMaster"), hasParentSpanId(NO_PARENT_ID),
        hasStatusWithCode(isOneOf(StatusCode.OK, StatusCode.ERROR)));
    assertThat("there should be a span from the HRegionServer registerWithMaster.", spans,
      hasItem(rsRegisterWithMasterMatcher));
    final SpanData rsRegisterWithMasterSpan = spans.stream()
      .filter(rsRegisterWithMasterMatcher::matches).findAny().orElseThrow(AssertionError::new);
    assertThat("the HRegionServer registerWithMaster span should show zookeeper interaction.",
      spans, hasItem(allOf(hasName(startsWith("RecoverableZookeeper.")),
        hasParentSpanId(rsRegisterWithMasterSpan))));
    assertThat(
      "the HRegionServer registerWithMaster span should have an invocation of the"
        + " RegionServerStartup RPC.",
      spans, hasItem(allOf(hasName(endsWith("RegionServerStatusService/RegionServerStartup")),
        hasParentSpanId(rsRegisterWithMasterSpan))));
  }

  @Test
  public void testZKWatcherRegionServer() {
    final Matcher<SpanData> rsZKWatcherMatcher =
      allOf(hasName(startsWith("ZKWatcher-regionserver")), hasParentSpanId(NO_PARENT_ID),
        hasStatusWithCode(isOneOf(StatusCode.OK, StatusCode.ERROR)));
    assertThat("there should be a span from the ZKWatcher running in the HRegionServer.", spans,
      hasItem(rsZKWatcherMatcher));
    final SpanData rsZKWatcherSpan =
      spans.stream().filter(rsZKWatcherMatcher::matches).findAny().orElseThrow(AssertionError::new);
    assertThat("the ZKWatcher running in the HRegionServer span should invoke processEvent.", spans,
      hasItem(allOf(hasName(containsString("processEvent")), hasParentSpanId(rsZKWatcherSpan))));
  }

  @Test
  public void testHRegionServerExitingMainLoop() {
    final Matcher<SpanData> rsExitingMainLoopMatcher =
      allOf(hasName("HRegionServer exiting main loop"), hasParentSpanId(NO_PARENT_ID),
        hasStatusWithCode(isOneOf(StatusCode.OK, StatusCode.ERROR)));
    assertThat("there should be a span from the HRegionServer exiting main loop.", spans,
      hasItem(rsExitingMainLoopMatcher));
    final SpanData rsExitingMainLoopSpan = spans.stream().filter(rsExitingMainLoopMatcher::matches)
      .findAny().orElseThrow(AssertionError::new);
    assertThat("the HRegionServer exiting main loop span should show zookeeper interaction.", spans,
      hasItem(allOf(hasName(startsWith("RecoverableZookeeper.")),
        hasParentSpanId(rsExitingMainLoopSpan))));
    assertThat("the HRegionServer exiting main loop span should show AsyncConnection interaction.",
      spans, hasItem(
        allOf(hasName(startsWith("AsyncConnection.")), hasParentSpanId(rsExitingMainLoopSpan))));
    assertThat("the HRegionServer exiting main loop span should invoke CloseMetaHandler.", spans,
      hasItem(allOf(hasName("CloseMetaHandler"), hasParentSpanId(rsExitingMainLoopSpan))));
  }
}
