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

import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasEnded;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasName;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasParentSpanId;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasStatusWithCode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MatcherPredicate;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.trace.StringTraceRenderer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Tag(LargeTests.TAG)
@Tag(ClientTests.TAG)
public class TestResultScannerTracing {
  private static final Logger LOG = LoggerFactory.getLogger(TestResultScannerTracing.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final TableName TABLE_NAME =
    TableName.valueOf(TestResultScannerTracing.class.getSimpleName());
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] CQ = Bytes.toBytes("q");
  private static final int COUNT = 1000;

  @RegisterExtension
  private static final OpenTelemetryExtension OTEL_EXT = OpenTelemetryExtension.create();

  private static Connection CONN;

  private String methodName;

  @BeforeAll
  public static void setUpBeforeAll() throws Exception {
    UTIL.startMiniCluster();
    byte[][] splitKeys = new byte[8][];
    for (int i = 111; i < 999; i += 111) {
      splitKeys[i / 111 - 1] = Bytes.toBytes(String.format("%03d", i));
    }
    try (Table table = UTIL.createTable(TABLE_NAME, FAMILY, splitKeys)) {
      UTIL.waitTableAvailable(TABLE_NAME);
      table.put(
        IntStream.range(0, COUNT).mapToObj(i -> new Put(Bytes.toBytes(String.format("%03d", i)))
          .addColumn(FAMILY, CQ, Bytes.toBytes(i))).collect(Collectors.toList()));
    }
    CONN = ConnectionFactory.createConnection(UTIL.getConfiguration());
  }

  @AfterAll
  public static void tearDown() throws Exception {
    Closeables.close(CONN, true);
    UTIL.shutdownMiniCluster();
  }

  @BeforeEach
  public void before(TestInfo testInfo) throws Exception {
    methodName = testInfo.getTestMethod().get().getName();
    try (final RegionLocator locator = CONN.getRegionLocator(TABLE_NAME)) {
      locator.clearRegionLocationCache();
    }
  }

  private static void waitForSpan(final Matcher<SpanData> parentSpanMatcher) {
    UTIL.waitFor(TimeUnit.SECONDS.toMillis(5), new MatcherPredicate<>(
      "Span for test failed to complete.", OTEL_EXT::getSpans, hasItem(parentSpanMatcher)));
  }

  private Scan buildDefaultScan() {
    return new Scan().withStartRow(Bytes.toBytes(String.format("%03d", 1)))
      .withStopRow(Bytes.toBytes(String.format("%03d", 998)));
  }

  private void assertDefaultScan(final Scan scan) {
    assertThat(scan.isReversed(), is(false));
    assertThat(scan.isAsyncPrefetch(), nullValue());
  }

  private Scan buildAsyncPrefetchScan() {
    return new Scan().withStartRow(Bytes.toBytes(String.format("%03d", 1)))
      .withStopRow(Bytes.toBytes(String.format("%03d", 998))).setAsyncPrefetch(true);
  }

  private void assertAsyncPrefetchScan(final Scan scan) {
    assertThat(scan.isReversed(), is(false));
    assertThat(scan.isAsyncPrefetch(), is(true));
  }

  private Scan buildReversedScan() {
    return new Scan().withStartRow(Bytes.toBytes(String.format("%03d", 998)))
      .withStopRow(Bytes.toBytes(String.format("%03d", 1))).setReversed(true);
  }

  private void assertReversedScan(final Scan scan) {
    assertThat(scan.isReversed(), is(true));
    assertThat(scan.isAsyncPrefetch(), nullValue());
  }

  private void doScan(final Supplier<Scan> spanSupplier, final Consumer<Scan> scanAssertions)
    throws Exception {
    final Scan scan = spanSupplier.get();
    scanAssertions.accept(scan);
    try (final Table table = CONN.getTable(TABLE_NAME);
      final ResultScanner scanner = table.getScanner(scan)) {
      final List<Result> results = new ArrayList<>(COUNT);
      scanner.forEach(results::add);
      assertThat(results, not(emptyIterable()));
    }
  }

  @Test
  public void testNormalScan() throws Exception {
    TraceUtil.trace(() -> doScan(this::buildDefaultScan, this::assertDefaultScan), methodName);

    final String parentSpanName = methodName;
    final Matcher<SpanData> parentSpanMatcher =
      allOf(hasName(parentSpanName), hasStatusWithCode(StatusCode.OK), hasEnded());
    waitForSpan(parentSpanMatcher);

    final List<SpanData> spans =
      OTEL_EXT.getSpans().stream().filter(Objects::nonNull).collect(Collectors.toList());
    if (LOG.isDebugEnabled()) {
      StringTraceRenderer stringTraceRenderer = new StringTraceRenderer(spans);
      stringTraceRenderer.render(LOG::debug);
    }

    final String parentSpanId = spans.stream().filter(parentSpanMatcher::matches)
      .map(SpanData::getSpanId).findAny().orElseThrow(AssertionError::new);

    final Matcher<SpanData> scanOperationSpanMatcher =
      allOf(hasName(startsWith("SCAN " + TABLE_NAME.getNameWithNamespaceInclAsString())),
        hasParentSpanId(parentSpanId), hasStatusWithCode(StatusCode.OK), hasEnded());
    assertThat(spans, hasItem(scanOperationSpanMatcher));
    final String scanOperationSpanId = spans.stream().filter(scanOperationSpanMatcher::matches)
      .map(SpanData::getSpanId).findAny().orElseThrow(AssertionError::new);

    final Matcher<SpanData> childMetaScanSpanMatcher = allOf(hasName(startsWith("SCAN hbase:meta")),
      hasParentSpanId(scanOperationSpanId), hasStatusWithCode(StatusCode.OK), hasEnded());
    assertThat("expected a scan of hbase:meta", spans, hasItem(childMetaScanSpanMatcher));
  }

  @Test
  public void testAsyncPrefetchScan() throws Exception {
    TraceUtil.trace(() -> doScan(this::buildAsyncPrefetchScan, this::assertAsyncPrefetchScan),
      methodName);

    final String parentSpanName = methodName;
    final Matcher<SpanData> parentSpanMatcher =
      allOf(hasName(parentSpanName), hasStatusWithCode(StatusCode.OK), hasEnded());
    waitForSpan(parentSpanMatcher);

    final List<SpanData> spans =
      OTEL_EXT.getSpans().stream().filter(Objects::nonNull).collect(Collectors.toList());
    if (LOG.isDebugEnabled()) {
      StringTraceRenderer stringTraceRenderer = new StringTraceRenderer(spans);
      stringTraceRenderer.render(LOG::debug);
    }

    final String parentSpanId = spans.stream().filter(parentSpanMatcher::matches)
      .map(SpanData::getSpanId).findAny().orElseThrow(AssertionError::new);

    final Matcher<SpanData> scanOperationSpanMatcher =
      allOf(hasName(startsWith("SCAN " + TABLE_NAME.getNameWithNamespaceInclAsString())),
        hasParentSpanId(parentSpanId), hasStatusWithCode(StatusCode.OK), hasEnded());
    assertThat(spans, hasItem(scanOperationSpanMatcher));
    final String scanOperationSpanId = spans.stream().filter(scanOperationSpanMatcher::matches)
      .map(SpanData::getSpanId).findAny().orElseThrow(AssertionError::new);

    final Matcher<SpanData> childMetaScanSpanMatcher = allOf(hasName(startsWith("SCAN hbase:meta")),
      hasParentSpanId(scanOperationSpanId), hasStatusWithCode(StatusCode.OK), hasEnded());
    assertThat("expected a scan of hbase:meta", spans, hasItem(childMetaScanSpanMatcher));
  }

  @Test
  public void testReversedScan() throws Exception {
    TraceUtil.trace(() -> doScan(this::buildReversedScan, this::assertReversedScan), methodName);

    final String parentSpanName = methodName;
    final Matcher<SpanData> parentSpanMatcher =
      allOf(hasName(parentSpanName), hasStatusWithCode(StatusCode.OK), hasEnded());
    waitForSpan(parentSpanMatcher);

    final List<SpanData> spans =
      OTEL_EXT.getSpans().stream().filter(Objects::nonNull).collect(Collectors.toList());
    if (LOG.isDebugEnabled()) {
      StringTraceRenderer stringTraceRenderer = new StringTraceRenderer(spans);
      stringTraceRenderer.render(LOG::debug);
    }

    final String parentSpanId = spans.stream().filter(parentSpanMatcher::matches)
      .map(SpanData::getSpanId).findAny().orElseThrow(AssertionError::new);

    final Matcher<SpanData> scanOperationSpanMatcher =
      allOf(hasName(startsWith("SCAN " + TABLE_NAME.getNameWithNamespaceInclAsString())),
        hasParentSpanId(parentSpanId), hasStatusWithCode(StatusCode.OK), hasEnded());
    assertThat(spans, hasItem(scanOperationSpanMatcher));
    final String scanOperationSpanId = spans.stream().filter(scanOperationSpanMatcher::matches)
      .map(SpanData::getSpanId).findAny().orElseThrow(AssertionError::new);

    final Matcher<SpanData> childMetaScanSpanMatcher = allOf(hasName(startsWith("SCAN hbase:meta")),
      hasParentSpanId(scanOperationSpanId), hasStatusWithCode(StatusCode.OK), hasEnded());
    assertThat("expected a scan of hbase:meta", spans, hasItem(childMetaScanSpanMatcher));
  }
}
