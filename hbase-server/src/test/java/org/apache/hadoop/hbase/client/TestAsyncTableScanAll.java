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
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasExceptionWithType;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasName;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasParentSpanId;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasStatusWithCode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.startsWith;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.trace.StringTraceRenderer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.hamcrest.Matcher;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncTableScanAll extends AbstractTestAsyncTableScan {
  private static final Logger logger = LoggerFactory.getLogger(TestAsyncTableScanAll.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncTableScanAll.class);

  @Parameter(0)
  public String tableType;

  @Parameter(1)
  public Supplier<AsyncTable<?>> getTable;

  @Parameter(2)
  public String scanType;

  @Parameter(3)
  public Supplier<Scan> scanCreator;

  @Parameters(name = "{index}: table={0}, scan={2}")
  public static List<Object[]> params() {
    return getTableAndScanCreatorParams();
  }

  @Override
  protected Scan createScan() {
    return scanCreator.get();
  }

  @Override
  protected List<Result> doScan(Scan scan, int closeAfter) throws Exception {
    List<Result> results = getTable.get().scanAll(scan).get();
    if (scan.getBatch() > 0) {
      results = convertFromBatchResult(results);
    }
    // we can't really close the scan early for scanAll, but to keep the assertions
    // simple in AbstractTestAsyncTableScan we'll just sublist here instead.
    if (closeAfter > 0 && closeAfter < results.size()) {
      results = results.subList(0, closeAfter);
    }
    return results;
  }

  @Override
  protected void assertTraceContinuity() {
    final String parentSpanName = testName.getMethodName();
    final Matcher<SpanData> parentSpanMatcher = allOf(
      hasName(parentSpanName),
      hasStatusWithCode(StatusCode.OK),
      hasEnded());
    waitForSpan(parentSpanMatcher);

    final List<SpanData> spans = otelClassRule.getSpans()
      .stream()
      .filter(Objects::nonNull)
      .collect(Collectors.toList());
    if (logger.isDebugEnabled()) {
      StringTraceRenderer stringTraceRenderer = new StringTraceRenderer(spans);
      stringTraceRenderer.render(logger::debug);
    }

    final String parentSpanId = spans.stream()
      .filter(parentSpanMatcher::matches)
      .map(SpanData::getSpanId)
      .findAny()
      .orElseThrow(AssertionError::new);

    final Matcher<SpanData> scanOperationSpanMatcher = allOf(
      hasName(startsWith("SCAN " + TABLE_NAME.getNameWithNamespaceInclAsString())),
      hasParentSpanId(parentSpanId),
      hasStatusWithCode(StatusCode.OK),
      hasEnded());
    assertThat(spans, hasItem(scanOperationSpanMatcher));
  }

  @Override
  protected void assertTraceError(Matcher<String> exceptionTypeNameMatcher) {
    final String parentSpanName = testName.getMethodName();
    final Matcher<SpanData> parentSpanMatcher = allOf(hasName(parentSpanName), hasEnded());
    waitForSpan(parentSpanMatcher);

    final List<SpanData> spans = otelClassRule.getSpans()
      .stream()
      .filter(Objects::nonNull)
      .collect(Collectors.toList());
    if (logger.isDebugEnabled()) {
      StringTraceRenderer stringTraceRenderer = new StringTraceRenderer(spans);
      stringTraceRenderer.render(logger::debug);
    }

    final String parentSpanId = spans.stream()
      .filter(parentSpanMatcher::matches)
      .map(SpanData::getSpanId)
      .findAny()
      .orElseThrow(AssertionError::new);

    final Matcher<SpanData> scanOperationSpanMatcher = allOf(
      hasName(startsWith("SCAN " + TABLE_NAME.getNameWithNamespaceInclAsString())),
      hasParentSpanId(parentSpanId),
      hasStatusWithCode(StatusCode.ERROR),
      hasExceptionWithType(exceptionTypeNameMatcher),
      hasEnded());
    assertThat(spans, hasItem(scanOperationSpanMatcher));
  }
}
