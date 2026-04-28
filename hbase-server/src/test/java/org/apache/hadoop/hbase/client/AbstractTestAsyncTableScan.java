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

import static org.apache.hadoop.hbase.client.trace.hamcrest.AttributesMatchers.containsEntry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MatcherPredicate;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.trace.HBaseSemanticAttributes;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.provider.Arguments;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

public abstract class AbstractTestAsyncTableScan {

  @RegisterExtension
  protected static final OpenTelemetryExtension OTEL_EXT = OpenTelemetryExtension.create();

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  protected static AsyncConnection CONN;

  protected String methodName;

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster(3);
    byte[][] splitKeys = new byte[8][];
    for (int i = 111; i < 999; i += 111) {
      splitKeys[i / 111 - 1] = Bytes.toBytes(String.format("%03d", i));
    }
    UTIL.createTable(TABLE_NAME, FAMILY, splitKeys);
    UTIL.waitTableAvailable(TABLE_NAME);
    try (Table table = UTIL.getConnection().getTable(TABLE_NAME)) {
      table.put(IntStream.range(0, COUNT)
        .mapToObj(i -> new Put(Bytes.toBytes(String.format("%03d", i)))
          .addColumn(FAMILY, CQ1, Bytes.toBytes(i)).addColumn(FAMILY, CQ2, Bytes.toBytes(i * i)))
        .collect(Collectors.toList()));
    }
    CONN = ConnectionFactory.createAsyncConnection(UTIL.getConfiguration()).get();
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    Closeables.close(CONN, true);
    UTIL.shutdownMiniCluster();
  }

  @BeforeEach
  public void setUp(TestInfo testInfo) {
    methodName = testInfo.getTestMethod().get().getName();
  }

  protected static TableName TABLE_NAME = TableName.valueOf("async");

  protected static byte[] FAMILY = Bytes.toBytes("cf");

  protected static byte[] CQ1 = Bytes.toBytes("cq1");

  protected static byte[] CQ2 = Bytes.toBytes("cq2");

  protected static int COUNT = 1000;

  private static Scan createNormalScan() {
    return new Scan();
  }

  private static Scan createBatchScan() {
    return new Scan().setBatch(1);
  }

  // set a small result size for testing flow control
  private static Scan createSmallResultSizeScan() {
    return new Scan().setMaxResultSize(1);
  }

  private static Scan createBatchSmallResultSizeScan() {
    return new Scan().setBatch(1).setMaxResultSize(1);
  }

  private static AsyncTable<?> getRawTable() {
    return CONN.getTable(TABLE_NAME);
  }

  private static AsyncTable<?> getTable() {
    return CONN.getTable(TABLE_NAME, ForkJoinPool.commonPool());
  }

  private static List<Pair<String, Supplier<Scan>>> getScanCreator() {
    return Arrays.asList(Pair.newPair("normal", AbstractTestAsyncTableScan::createNormalScan),
      Pair.newPair("batch", AbstractTestAsyncTableScan::createBatchScan),
      Pair.newPair("smallResultSize", AbstractTestAsyncTableScan::createSmallResultSizeScan),
      Pair.newPair("batchSmallResultSize",
        AbstractTestAsyncTableScan::createBatchSmallResultSizeScan));
  }

  protected static Stream<Arguments> getScanCreatorParams() {
    return getScanCreator().stream().map(p -> Arguments.of(p.getFirst(), p.getSecond()));
  }

  private static List<Pair<String, Supplier<AsyncTable<?>>>> getTableCreator() {
    return Arrays.asList(Pair.newPair("raw", AbstractTestAsyncTableScan::getRawTable),
      Pair.newPair("normal", AbstractTestAsyncTableScan::getTable));
  }

  protected static Stream<Arguments> getTableAndScanCreatorParams() {
    List<Pair<String, Supplier<AsyncTable<?>>>> tableCreator = getTableCreator();
    List<Pair<String, Supplier<Scan>>> scanCreator = getScanCreator();
    return tableCreator.stream().flatMap(tp -> scanCreator.stream()
      .map(sp -> Arguments.of(tp.getFirst(), tp.getSecond(), sp.getFirst(), sp.getSecond())));
  }

  protected abstract Scan createScan();

  protected abstract List<Result> doScan(Scan scan, int closeAfter) throws Exception;

  /**
   * Used by implementation classes to assert the correctness of spans produced under test.
   */
  protected abstract void assertTraceContinuity();

  /**
   * Used by implementation classes to assert the correctness of spans having errors.
   */
  protected abstract void
    assertTraceError(final Matcher<io.opentelemetry.api.common.Attributes> exceptionMatcher);

  protected final List<Result> convertFromBatchResult(List<Result> results) {
    assertEquals(0, results.size() % 2);
    return IntStream.range(0, results.size() / 2).mapToObj(i -> {
      try {
        return Result
          .createCompleteResult(Arrays.asList(results.get(2 * i), results.get(2 * i + 1)));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }).collect(Collectors.toList());
  }

  protected static void waitForSpan(final Matcher<SpanData> parentSpanMatcher) {
    UTIL.waitFor(TimeUnit.SECONDS.toMillis(5), new MatcherPredicate<>(
      "Span for test failed to complete.", OTEL_EXT::getSpans, hasItem(parentSpanMatcher)));
  }

  protected static Stream<SpanData> spanStream() {
    return OTEL_EXT.getSpans().stream().filter(Objects::nonNull);
  }

  @TestTemplate
  public void testScanAll() throws Exception {
    List<Result> results = doScan(createScan(), -1);
    // make sure all scanners are closed at RS side
    UTIL.getHBaseCluster().getRegionServerThreads().stream()
      .map(JVMClusterUtil.RegionServerThread::getRegionServer).forEach(rs -> {
        int scannerCount = rs.getRSRpcServices().getScannersCount();
        assertEquals(0, scannerCount,
          "The scanner count of " + rs.getServerName() + " is " + scannerCount);
      });
    assertEquals(COUNT, results.size());
    IntStream.range(0, COUNT).forEach(i -> {
      Result result = results.get(i);
      assertEquals(String.format("%03d", i), Bytes.toString(result.getRow()));
      assertEquals(i, Bytes.toInt(result.getValue(FAMILY, CQ1)));
    });
  }

  private void assertResultEquals(Result result, int i) {
    assertEquals(String.format("%03d", i), Bytes.toString(result.getRow()));
    assertEquals(i, Bytes.toInt(result.getValue(FAMILY, CQ1)));
    assertEquals(i * i, Bytes.toInt(result.getValue(FAMILY, CQ2)));
  }

  @TestTemplate
  public void testReversedScanAll() throws Exception {
    List<Result> results =
      TraceUtil.trace(() -> doScan(createScan().setReversed(true), -1), methodName);
    assertEquals(COUNT, results.size());
    IntStream.range(0, COUNT).forEach(i -> assertResultEquals(results.get(i), COUNT - i - 1));
    assertTraceContinuity();
  }

  @TestTemplate
  public void testScanNoStopKey() throws Exception {
    int start = 345;
    List<Result> results = TraceUtil.trace(
      () -> doScan(createScan().withStartRow(Bytes.toBytes(String.format("%03d", start))), -1),
      methodName);
    assertEquals(COUNT - start, results.size());
    IntStream.range(0, COUNT - start).forEach(i -> assertResultEquals(results.get(i), start + i));
    assertTraceContinuity();
  }

  @TestTemplate
  public void testReverseScanNoStopKey() throws Exception {
    int start = 765;
    final Scan scan =
      createScan().withStartRow(Bytes.toBytes(String.format("%03d", start))).setReversed(true);
    List<Result> results = TraceUtil.trace(() -> doScan(scan, -1), methodName);
    assertEquals(start + 1, results.size());
    IntStream.range(0, start + 1).forEach(i -> assertResultEquals(results.get(i), start - i));
    assertTraceContinuity();
  }

  @TestTemplate
  public void testScanWrongColumnFamily() {
    final Exception e = assertThrows(Exception.class, () -> TraceUtil.trace(
      () -> doScan(createScan().addFamily(Bytes.toBytes("WrongColumnFamily")), -1), methodName));
    // hamcrest generic enforcement for `anyOf` is a pain; skip it
    // but -- don't we always unwrap ExecutionExceptions -- bug?
    if (e instanceof NoSuchColumnFamilyException) {
      final NoSuchColumnFamilyException ex = (NoSuchColumnFamilyException) e;
      assertThat(ex, isA(NoSuchColumnFamilyException.class));
    } else if (e instanceof ExecutionException) {
      final ExecutionException ex = (ExecutionException) e;
      assertThat(ex, allOf(isA(ExecutionException.class),
        hasProperty("cause", isA(NoSuchColumnFamilyException.class))));
    } else {
      fail("Found unexpected Exception " + e);
    }
    assertTraceError(anyOf(
      containsEntry(is(HBaseSemanticAttributes.EXCEPTION_TYPE),
        endsWith(NoSuchColumnFamilyException.class.getName())),
      allOf(
        containsEntry(is(HBaseSemanticAttributes.EXCEPTION_TYPE),
          endsWith(RemoteWithExtrasException.class.getName())),
        containsEntry(is(HBaseSemanticAttributes.EXCEPTION_MESSAGE),
          containsString(NoSuchColumnFamilyException.class.getName())))));
  }

  private void testScan(int start, boolean startInclusive, int stop, boolean stopInclusive,
    int limit) throws Exception {
    testScan(start, startInclusive, stop, stopInclusive, limit, -1);
  }

  private void testScan(int start, boolean startInclusive, int stop, boolean stopInclusive,
    int limit, int closeAfter) throws Exception {
    Scan scan =
      createScan().withStartRow(Bytes.toBytes(String.format("%03d", start)), startInclusive)
        .withStopRow(Bytes.toBytes(String.format("%03d", stop)), stopInclusive);
    if (limit > 0) {
      scan.setLimit(limit);
    }
    List<Result> results = doScan(scan, closeAfter);
    int actualStart = startInclusive ? start : start + 1;
    int actualStop = stopInclusive ? stop + 1 : stop;
    int count = actualStop - actualStart;
    if (limit > 0) {
      count = Math.min(count, limit);
    }
    if (closeAfter > 0) {
      count = Math.min(count, closeAfter);
    }
    assertEquals(count, results.size());
    IntStream.range(0, count).forEach(i -> assertResultEquals(results.get(i), actualStart + i));
  }

  private void testReversedScan(int start, boolean startInclusive, int stop, boolean stopInclusive,
    int limit) throws Exception {
    Scan scan =
      createScan().withStartRow(Bytes.toBytes(String.format("%03d", start)), startInclusive)
        .withStopRow(Bytes.toBytes(String.format("%03d", stop)), stopInclusive).setReversed(true);
    if (limit > 0) {
      scan.setLimit(limit);
    }
    List<Result> results = doScan(scan, -1);
    int actualStart = startInclusive ? start : start - 1;
    int actualStop = stopInclusive ? stop - 1 : stop;
    int count = actualStart - actualStop;
    if (limit > 0) {
      count = Math.min(count, limit);
    }
    assertEquals(count, results.size());
    IntStream.range(0, count).forEach(i -> assertResultEquals(results.get(i), actualStart - i));
  }

  @TestTemplate
  public void testScanWithStartKeyAndStopKey() throws Exception {
    testScan(1, true, 998, false, -1); // from first region to last region
    testScan(123, true, 345, true, -1);
    testScan(234, true, 456, false, -1);
    testScan(345, false, 567, true, -1);
    testScan(456, false, 678, false, -1);
  }

  @TestTemplate
  public void testReversedScanWithStartKeyAndStopKey() throws Exception {
    testReversedScan(998, true, 1, false, -1); // from last region to first region
    testReversedScan(543, true, 321, true, -1);
    testReversedScan(654, true, 432, false, -1);
    testReversedScan(765, false, 543, true, -1);
    testReversedScan(876, false, 654, false, -1);
  }

  @TestTemplate
  public void testScanAtRegionBoundary() throws Exception {
    testScan(222, true, 333, true, -1);
    testScan(333, true, 444, false, -1);
    testScan(444, false, 555, true, -1);
    testScan(555, false, 666, false, -1);
  }

  @TestTemplate
  public void testReversedScanAtRegionBoundary() throws Exception {
    testReversedScan(333, true, 222, true, -1);
    testReversedScan(444, true, 333, false, -1);
    testReversedScan(555, false, 444, true, -1);
    testReversedScan(666, false, 555, false, -1);
  }

  @TestTemplate
  public void testScanWithLimit() throws Exception {
    testScan(1, true, 998, false, 900); // from first region to last region
    testScan(123, true, 234, true, 100);
    testScan(234, true, 456, false, 100);
    testScan(345, false, 567, true, 100);
    testScan(456, false, 678, false, 100);
  }

  @TestTemplate
  public void testScanWithLimitGreaterThanActualCount() throws Exception {
    testScan(1, true, 998, false, 1000); // from first region to last region
    testScan(123, true, 345, true, 200);
    testScan(234, true, 456, false, 200);
    testScan(345, false, 567, true, 200);
    testScan(456, false, 678, false, 200);
  }

  @TestTemplate
  public void testReversedScanWithLimit() throws Exception {
    testReversedScan(998, true, 1, false, 900); // from last region to first region
    testReversedScan(543, true, 321, true, 100);
    testReversedScan(654, true, 432, false, 100);
    testReversedScan(765, false, 543, true, 100);
    testReversedScan(876, false, 654, false, 100);
  }

  @TestTemplate
  public void testReversedScanWithLimitGreaterThanActualCount() throws Exception {
    testReversedScan(998, true, 1, false, 1000); // from last region to first region
    testReversedScan(543, true, 321, true, 200);
    testReversedScan(654, true, 432, false, 200);
    testReversedScan(765, false, 543, true, 200);
    testReversedScan(876, false, 654, false, 200);
  }

  @TestTemplate
  public void testScanEndingEarly() throws Exception {
    testScan(1, true, 998, false, 0, 900); // from first region to last region
    testScan(123, true, 234, true, 0, 100);
    testScan(234, true, 456, false, 0, 100);
    testScan(345, false, 567, true, 0, 100);
    testScan(456, false, 678, false, 0, 100);
  }
}
