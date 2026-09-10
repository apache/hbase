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
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasException;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasName;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasParentSpanId;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasStatusWithCode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.startsWith;

import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.client.trace.StringTraceRenderer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(LargeTests.TAG)
@Tag(ClientTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: scan={0}")
public class TestRawAsyncTableScan extends AbstractTestAsyncTableScan {
  private static final Logger logger = LoggerFactory.getLogger(TestRawAsyncTableScan.class);

  private Supplier<Scan> scanCreator;

  // scanType is only for displaying
  public TestRawAsyncTableScan(String scanType, Supplier<Scan> scanCreator) {
    this.scanCreator = scanCreator;
  }

  public static Stream<Arguments> parameters() {
    return getScanCreatorParams();
  }

  @Override
  protected Scan createScan() {
    return scanCreator.get();
  }

  @Override
  protected List<Result> doScan(Scan scan, int closeAfter) throws Exception {
    TracedAdvancedScanResultConsumer scanConsumer = new TracedAdvancedScanResultConsumer();
    CONN.getTable(TABLE_NAME).scan(scan, scanConsumer);
    List<Result> results = new ArrayList<>();
    // these tests batch settings with the sample data result in each result being
    // split in two. so we must allow twice the expected results in order to reach
    // our true limit. see convertFromBatchResult for details.
    if (closeAfter > 0 && scan.getBatch() > 0) {
      closeAfter = closeAfter * 2;
    }
    for (Result result; (result = scanConsumer.take()) != null;) {
      results.add(result);
      if (closeAfter > 0 && results.size() >= closeAfter) {
        break;
      }
    }
    if (scan.getBatch() > 0) {
      results = convertFromBatchResult(results);
    }
    return results;
  }

  @Override
  protected void assertTraceContinuity() {
    final String parentSpanName = methodName;
    final Matcher<SpanData> parentSpanMatcher =
      allOf(hasName(parentSpanName), hasStatusWithCode(StatusCode.OK), hasEnded());
    waitForSpan(parentSpanMatcher);

    if (logger.isDebugEnabled()) {
      StringTraceRenderer stringTraceRenderer =
        new StringTraceRenderer(spanStream().collect(Collectors.toList()));
      stringTraceRenderer.render(logger::debug);
    }

    final String parentSpanId = spanStream().filter(parentSpanMatcher::matches)
      .max((a, b) -> Long.compare(a.getEndEpochNanos(), b.getEndEpochNanos()))
      .map(SpanData::getSpanId).get();

    final Matcher<SpanData> scanOperationSpanMatcher =
      allOf(hasName(startsWith("SCAN " + TABLE_NAME.getNameWithNamespaceInclAsString())),
        hasParentSpanId(parentSpanId), hasStatusWithCode(StatusCode.OK), hasEnded());
    waitForSpan(scanOperationSpanMatcher);

    final String scanOperationSpanId = spanStream().filter(scanOperationSpanMatcher::matches)
      .max((a, b) -> Long.compare(a.getEndEpochNanos(), b.getEndEpochNanos()))
      .map(SpanData::getSpanId).get();
    final Matcher<SpanData> onNextMatcher = hasName("TracedAdvancedScanResultConsumer#onNext");
    waitForSpan(onNextMatcher);
    spanStream().filter(onNextMatcher::matches)
      .forEach(span -> assertThat(span, hasParentSpanId(scanOperationSpanId)));
    waitForSpan(allOf(onNextMatcher, hasParentSpanId(scanOperationSpanId),
      hasStatusWithCode(StatusCode.OK), hasEnded()));

    final Matcher<SpanData> onCompleteMatcher =
      hasName("TracedAdvancedScanResultConsumer#onComplete");
    waitForSpan(onCompleteMatcher);
    spanStream().filter(onCompleteMatcher::matches)
      .forEach(span -> assertThat(span, allOf(onCompleteMatcher,
        hasParentSpanId(scanOperationSpanId), hasStatusWithCode(StatusCode.OK), hasEnded())));
  }

  @Override
  protected void
    assertTraceError(Matcher<io.opentelemetry.api.common.Attributes> exceptionMatcher) {
    final String parentSpanName = methodName;
    final Matcher<SpanData> parentSpanMatcher = allOf(hasName(parentSpanName), hasEnded());
    waitForSpan(parentSpanMatcher);

    if (logger.isDebugEnabled()) {
      StringTraceRenderer stringTraceRenderer =
        new StringTraceRenderer(spanStream().collect(Collectors.toList()));
      stringTraceRenderer.render(logger::debug);
    }

    final String parentSpanId = spanStream().filter(parentSpanMatcher::matches)
      .max((a, b) -> Long.compare(a.getEndEpochNanos(), b.getEndEpochNanos()))
      .map(SpanData::getSpanId).get();

    final Matcher<SpanData> scanOperationSpanMatcher =
      allOf(hasName(startsWith("SCAN " + TABLE_NAME.getNameWithNamespaceInclAsString())),
        hasParentSpanId(parentSpanId), hasStatusWithCode(StatusCode.ERROR),
        hasException(exceptionMatcher), hasEnded());
    waitForSpan(scanOperationSpanMatcher);
    final String scanOperationSpanId = spanStream().filter(scanOperationSpanMatcher::matches)
      .max((a, b) -> Long.compare(a.getEndEpochNanos(), b.getEndEpochNanos()))
      .map(SpanData::getSpanId).get();

    final Matcher<SpanData> onCompleteMatcher = hasName("TracedAdvancedScanResultConsumer#onError");
    waitForSpan(onCompleteMatcher);
    spanStream().filter(onCompleteMatcher::matches)
      .forEach(span -> assertThat(span, allOf(onCompleteMatcher,
        hasParentSpanId(scanOperationSpanId), hasStatusWithCode(StatusCode.OK), hasEnded())));
  }
}
