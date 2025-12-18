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
import java.util.List;
import java.util.concurrent.ForkJoinPool;
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
public class TestAsyncTableScan extends AbstractTestAsyncTableScan {
  private static final Logger logger = LoggerFactory.getLogger(TestAsyncTableScan.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableScan.class);

  @Parameter(0)
  public String scanType;

  @Parameter(1)
  public Supplier<Scan> scanCreater;

  @Parameters(name = "{index}: scan={0}")
  public static List<Object[]> params() {
    return getScanCreatorParams();
  }

  @Override
  protected Scan createScan() {
    return scanCreater.get();
  }

  @Override
  protected List<Result> doScan(Scan scan, int closeAfter) throws Exception {
    AsyncTable<ScanResultConsumer> table =
      CONN_RULE.getAsyncConnection().getTable(TABLE_NAME, ForkJoinPool.commonPool());
    List<Result> results;
    if (closeAfter > 0) {
      // these tests batch settings with the sample data result in each result being
      // split in two. so we must allow twice the expected results in order to reach
      // our true limit. see convertFromBatchResult for details.
      if (scan.getBatch() > 0) {
        closeAfter = closeAfter * 2;
      }
      TracedScanResultConsumer consumer =
        new TracedScanResultConsumer(new LimitedScanResultConsumer(closeAfter));
      table.scan(scan, consumer);
      results = consumer.getAll();
    } else {
      TracedScanResultConsumer consumer =
        new TracedScanResultConsumer(new SimpleScanResultConsumerImpl());
      table.scan(scan, consumer);
      results = consumer.getAll();
    }
    if (scan.getBatch() > 0) {
      results = convertFromBatchResult(results);
    }
    return results;
  }

  @Override
  protected void assertTraceContinuity() {
    final String parentSpanName = testName.getMethodName();
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

    final Matcher<SpanData> onNextMatcher = hasName("TracedScanResultConsumer#onNext");
    waitForSpan(onNextMatcher);
    spanStream().filter(onNextMatcher::matches)
      .forEach(span -> assertThat(span, allOf(onNextMatcher, hasParentSpanId(scanOperationSpanId),
        hasStatusWithCode(StatusCode.OK), hasEnded())));

    final Matcher<SpanData> onCompleteMatcher = hasName("TracedScanResultConsumer#onComplete");
    waitForSpan(onCompleteMatcher);
    spanStream().filter(onCompleteMatcher::matches)
      .forEach(span -> assertThat(span, allOf(onCompleteMatcher,
        hasParentSpanId(scanOperationSpanId), hasStatusWithCode(StatusCode.OK), hasEnded())));
  }

  @Override
  protected void
    assertTraceError(Matcher<io.opentelemetry.api.common.Attributes> exceptionMatcher) {
    final String parentSpanName = testName.getMethodName();
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

    final Matcher<SpanData> onErrorMatcher = hasName("TracedScanResultConsumer#onError");
    waitForSpan(onErrorMatcher);
    spanStream().filter(onErrorMatcher::matches)
      .forEach(span -> assertThat(span, allOf(onErrorMatcher, hasParentSpanId(scanOperationSpanId),
        hasStatusWithCode(StatusCode.OK), hasEnded())));
  }
}
