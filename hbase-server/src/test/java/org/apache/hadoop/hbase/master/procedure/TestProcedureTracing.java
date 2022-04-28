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
package org.apache.hadoop.hbase.master.procedure;

import static io.opentelemetry.api.common.AttributeKey.longKey;
import static org.apache.hadoop.hbase.client.trace.hamcrest.AttributesMatchers.containsEntry;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasAttributes;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasEnded;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasName;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasStatusWithCode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;

import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.ConnectionRule;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MatcherPredicate;
import org.apache.hadoop.hbase.MiniClusterRule;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.trace.StringTraceRenderer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.trace.OpenTelemetryClassRule;
import org.apache.hadoop.hbase.trace.OpenTelemetryTestRule;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test of master ProcedureV2 tracing.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestProcedureTracing {
  private static final Logger LOG = LoggerFactory.getLogger(TestProcedureTracing.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestProcedureTracing.class);

  private static final OpenTelemetryClassRule otelClassRule = OpenTelemetryClassRule.create();
  private static final MiniClusterRule miniClusterRule = MiniClusterRule.newBuilder()
    .setMiniClusterOption(StartMiniClusterOption.builder().numWorkers(3).build()).build();
  protected static final ConnectionRule connectionRule =
    ConnectionRule.createAsyncConnectionRule(miniClusterRule::createAsyncConnection);

  @ClassRule
  public static final TestRule classRule =
    RuleChain.outerRule(otelClassRule).around(miniClusterRule).around(connectionRule);

  @Rule
  public final OpenTelemetryTestRule otelTestRule = new OpenTelemetryTestRule(otelClassRule);

  @Rule
  public TestName name = new TestName();

  @Test
  public void testCreateOpenDeleteTableSpans() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final AsyncConnection conn = connectionRule.getAsyncConnection();
    final AsyncAdmin admin = conn.getAdmin();

    final AtomicReference<List<String>> tableRegionsRef = new AtomicReference<>();
    TraceUtil.trace(() -> {
      try (final Table ignored = miniClusterRule.getTestingUtility()
        .createMultiRegionTable(tableName, Bytes.toBytes("fam"), 5)) {
        final List<String> tableRegions = conn.getRegionLocator(tableName).getAllRegionLocations()
          .get().stream().map(HRegionLocation::getRegion).map(RegionInfo::getEncodedName)
          .collect(Collectors.toList());
        tableRegionsRef.set(tableRegions);
      }
      if (admin.tableExists(tableName).get()) {
        if (admin.isTableEnabled(tableName).get()) {
          admin.disableTable(tableName).get();
        }
        admin.deleteTable(tableName).get();
      }
    }, name.getMethodName());

    final Matcher<SpanData> testSpanMatcher = allOf(hasName(name.getMethodName()), hasEnded());
    Waiter.waitFor(conn.getConfiguration(), TimeUnit.MINUTES.toMillis(3),
      new MatcherPredicate<>(otelClassRule::getSpans, hasItem(testSpanMatcher)));
    final List<SpanData> spans = otelClassRule.getSpans();
    final StringTraceRenderer renderer = new StringTraceRenderer(spans);
    renderer.render(LOG::debug);

    // Expect to find a span for a CreateTableProcedure for the test table
    final Matcher<SpanData> createTableProcedureSpanMatcher = allOf(
      hasName(allOf(containsString("CreateTableProcedure"),
        containsString("table=" + name.getMethodName()))),
      hasEnded(), hasStatusWithCode(StatusCode.OK),
      hasAttributes(allOf(containsEntry(longKey("procId"), any(Long.class)),
        containsEntry(longKey("parentProcId"), any(Long.class)))));
    assertThat("Expected to find a span for a CreateTableProcedure for the test table", spans,
      hasItem(createTableProcedureSpanMatcher));

    // Expect to find a span for a TransitRegionStateProcedure for the test table
    final Matcher<SpanData> transitRegionStateProcedureSpanMatcher = allOf(
      hasName(allOf(containsString("TransitRegionStateProcedure"),
        containsString("table=" + name.getMethodName()))),
      hasEnded(), hasStatusWithCode(StatusCode.OK),
      hasAttributes(allOf(containsEntry(longKey("procId"), any(Long.class)),
        containsEntry(longKey("parentProcId"), any(Long.class)))));
    assertThat("Expected to find a span for a TransitRegionStateProcedure for the test table",
      spans, hasItem(transitRegionStateProcedureSpanMatcher));

    // Expect to find a span for an OpenRegionProcedure for a region of the test table
    final List<Matcher<? super String>> tableRegionMatchers =
      tableRegionsRef.get().stream().map(Matchers::containsString).collect(Collectors.toList());
    final Matcher<SpanData> openRegionProcedureSpanMatcher =
      allOf(hasName(allOf(containsString("OpenRegionProcedure"), anyOf(tableRegionMatchers))),
        hasEnded(), hasStatusWithCode(StatusCode.OK),
        hasAttributes(allOf(containsEntry(longKey("procId"), any(Long.class)),
          containsEntry(longKey("parentProcId"), any(Long.class)))));
    assertThat("Expected to find a span for an OpenRegionProcedure for a region of the test table",
      spans, hasItem(openRegionProcedureSpanMatcher));

    // Expect to find a span for a CloseRegionProcedure for a region of the test table
    final Matcher<SpanData> closeRegionProcedureSpanMatcher =
      allOf(hasName(allOf(containsString("CloseRegionProcedure"), anyOf(tableRegionMatchers))),
        hasEnded(), hasStatusWithCode(StatusCode.OK),
        hasAttributes(allOf(containsEntry(longKey("procId"), any(Long.class)),
          containsEntry(longKey("parentProcId"), any(Long.class)))));
    assertThat("Expected to find a span for a CloseRegionProcedure for a region of the test table",
      spans, hasItem(closeRegionProcedureSpanMatcher));

    // Expect to find a span for a DisableTableProcedure for the test table
    final Matcher<SpanData> disableTableProcedureSpanMatcher = allOf(
      hasName(allOf(containsString("DisableTableProcedure"),
        containsString("table=" + name.getMethodName()))),
      hasEnded(), hasStatusWithCode(StatusCode.OK),
      hasAttributes(allOf(containsEntry(longKey("procId"), any(Long.class)),
        containsEntry(longKey("parentProcId"), any(Long.class)))));
    assertThat("Expected to find a span for a DisableTableProcedure for the test table", spans,
      hasItem(disableTableProcedureSpanMatcher));

    // Expect to find a span for a DeleteTableProcedure for the test table
    final Matcher<SpanData> deleteTableProcedureSpanMatcher = allOf(
      hasName(allOf(containsString("DeleteTableProcedure"),
        containsString("table=" + name.getMethodName()))),
      hasEnded(), hasStatusWithCode(StatusCode.OK),
      hasAttributes(allOf(containsEntry(longKey("procId"), any(Long.class)),
        containsEntry(longKey("parentProcId"), any(Long.class)))));
    assertThat("Expected to find a span for a DeleteTableProcedure for the test table", spans,
      hasItem(deleteTableProcedureSpanMatcher));
  }
}
