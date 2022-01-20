/**
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ConnectionRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.MatcherPredicate;
import org.apache.hadoop.hbase.MiniClusterRule;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.trace.OpenTelemetryClassRule;
import org.apache.hadoop.hbase.trace.OpenTelemetryTestRule;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.hamcrest.Matcher;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

public abstract class AbstractTestAsyncTableScan {

  protected static final OpenTelemetryClassRule otelClassRule = OpenTelemetryClassRule.create();
  protected static final MiniClusterRule miniClusterRule = MiniClusterRule.newBuilder()
    .setMiniClusterOption(StartTestingClusterOption.builder()
      .numWorkers(3)
      .build())
    .build();

  protected static final ConnectionRule connectionRule =
    ConnectionRule.createAsyncConnectionRule(miniClusterRule::createAsyncConnection);

  private static final class Setup extends ExternalResource {
    @Override
    protected void before() throws Throwable {
      final HBaseTestingUtil testingUtil = miniClusterRule.getTestingUtility();
      final AsyncConnection conn = connectionRule.getAsyncConnection();

      byte[][] splitKeys = new byte[8][];
      for (int i = 111; i < 999; i += 111) {
        splitKeys[i / 111 - 1] = Bytes.toBytes(String.format("%03d", i));
      }
      testingUtil.createTable(TABLE_NAME, FAMILY, splitKeys);
      testingUtil.waitTableAvailable(TABLE_NAME);
      conn.getTable(TABLE_NAME)
        .putAll(IntStream.range(0, COUNT)
          .mapToObj(i -> new Put(Bytes.toBytes(String.format("%03d", i)))
            .addColumn(FAMILY, CQ1, Bytes.toBytes(i))
            .addColumn(FAMILY, CQ2, Bytes.toBytes(i * i)))
          .collect(Collectors.toList()))
        .get();
    }
  }

  @ClassRule
  public static final TestRule classRule = RuleChain.outerRule(otelClassRule)
    .around(miniClusterRule)
    .around(connectionRule)
    .around(new Setup());

  @Rule
  public final OpenTelemetryTestRule otelTestRule = new OpenTelemetryTestRule(otelClassRule);

  @Rule
  public final TestName testName = new TestName();

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
    return connectionRule.getAsyncConnection().getTable(TABLE_NAME);
  }

  private static AsyncTable<?> getTable() {
    return connectionRule.getAsyncConnection().getTable(TABLE_NAME, ForkJoinPool.commonPool());
  }

  private static List<Pair<String, Supplier<Scan>>> getScanCreator() {
    return Arrays.asList(Pair.newPair("normal", AbstractTestAsyncTableScan::createNormalScan),
      Pair.newPair("batch", AbstractTestAsyncTableScan::createBatchScan),
      Pair.newPair("smallResultSize", AbstractTestAsyncTableScan::createSmallResultSizeScan),
      Pair.newPair("batchSmallResultSize",
        AbstractTestAsyncTableScan::createBatchSmallResultSizeScan));
  }

  protected static List<Object[]> getScanCreatorParams() {
    return getScanCreator().stream().map(p -> new Object[] { p.getFirst(), p.getSecond() })
        .collect(Collectors.toList());
  }

  private static List<Pair<String, Supplier<AsyncTable<?>>>> getTableCreator() {
    return Arrays.asList(Pair.newPair("raw", AbstractTestAsyncTableScan::getRawTable),
      Pair.newPair("normal", AbstractTestAsyncTableScan::getTable));
  }

  protected static List<Object[]> getTableAndScanCreatorParams() {
    List<Pair<String, Supplier<AsyncTable<?>>>> tableCreator = getTableCreator();
    List<Pair<String, Supplier<Scan>>> scanCreator = getScanCreator();
    return tableCreator.stream()
        .flatMap(tp -> scanCreator.stream().map(
          sp -> new Object[] { tp.getFirst(), tp.getSecond(), sp.getFirst(), sp.getSecond() }))
        .collect(Collectors.toList());
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
  protected abstract void assertTraceError(final Matcher<String> exceptionTypeNameMatcher);

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
    final Configuration conf = miniClusterRule.getTestingUtility().getConfiguration();
    Waiter.waitFor(conf, TimeUnit.SECONDS.toMillis(5), new MatcherPredicate<>(
      "Span for test failed to complete.", otelClassRule::getSpans, hasItem(parentSpanMatcher)));
  }

  @Test
  public void testScanAll() throws Exception {
    List<Result> results = doScan(createScan(), -1);
    // make sure all scanners are closed at RS side
    miniClusterRule.getTestingUtility()
      .getHBaseCluster()
      .getRegionServerThreads()
      .stream()
      .map(JVMClusterUtil.RegionServerThread::getRegionServer)
      .forEach(rs -> assertEquals(
        "The scanner count of " + rs.getServerName() + " is " +
          rs.getRSRpcServices().getScannersCount(),
        0, rs.getRSRpcServices().getScannersCount()));
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

  @Test
  public void testReversedScanAll() throws Exception {
    List<Result> results = TraceUtil.trace(
      () -> doScan(createScan().setReversed(true), -1), testName.getMethodName());
    assertEquals(COUNT, results.size());
    IntStream.range(0, COUNT).forEach(i -> assertResultEquals(results.get(i), COUNT - i - 1));
    assertTraceContinuity();
  }

  @Test
  public void testScanNoStopKey() throws Exception {
    int start = 345;
    List<Result> results = TraceUtil.trace(() ->
      doScan(createScan().withStartRow(Bytes.toBytes(String.format("%03d", start))), -1),
      testName.getMethodName());
    assertEquals(COUNT - start, results.size());
    IntStream.range(0, COUNT - start).forEach(i -> assertResultEquals(results.get(i), start + i));
    assertTraceContinuity();
  }

  @Test
  public void testReverseScanNoStopKey() throws Exception {
    int start = 765;
    final Scan scan = createScan()
      .withStartRow(Bytes.toBytes(String.format("%03d", start)))
      .setReversed(true);
    List<Result> results = TraceUtil.trace(() -> doScan(scan, -1), testName.getMethodName());
    assertEquals(start + 1, results.size());
    IntStream.range(0, start + 1).forEach(i -> assertResultEquals(results.get(i), start - i));
    assertTraceContinuity();
  }

  @Test
  public void testScanWrongColumnFamily() {
    final Exception e = assertThrows(Exception.class, () -> TraceUtil.trace(
      () -> doScan(createScan().addFamily(Bytes.toBytes("WrongColumnFamily")), -1),
      testName.getMethodName()));
    // hamcrest generic enforcement for `anyOf` is a pain; skip it
    // but -- don't we always unwrap ExecutionExceptions -- bug?
    if (e instanceof NoSuchColumnFamilyException) {
      final NoSuchColumnFamilyException ex = (NoSuchColumnFamilyException) e;
      assertThat(ex, isA(NoSuchColumnFamilyException.class));
    } else if (e instanceof ExecutionException) {
      final ExecutionException ex = (ExecutionException) e;
      assertThat(ex, allOf(
        isA(ExecutionException.class),
        hasProperty("cause", isA(NoSuchColumnFamilyException.class))));
    } else {
      fail("Found unexpected Exception " + e);
    }
    assertTraceError(endsWith(NoSuchColumnFamilyException.class.getName()));
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

  @Test
  public void testScanWithStartKeyAndStopKey() throws Exception {
    testScan(1, true, 998, false, -1); // from first region to last region
    testScan(123, true, 345, true, -1);
    testScan(234, true, 456, false, -1);
    testScan(345, false, 567, true, -1);
    testScan(456, false, 678, false, -1);
  }

  @Test
  public void testReversedScanWithStartKeyAndStopKey() throws Exception {
    testReversedScan(998, true, 1, false, -1); // from last region to first region
    testReversedScan(543, true, 321, true, -1);
    testReversedScan(654, true, 432, false, -1);
    testReversedScan(765, false, 543, true, -1);
    testReversedScan(876, false, 654, false, -1);
  }

  @Test
  public void testScanAtRegionBoundary() throws Exception {
    testScan(222, true, 333, true, -1);
    testScan(333, true, 444, false, -1);
    testScan(444, false, 555, true, -1);
    testScan(555, false, 666, false, -1);
  }

  @Test
  public void testReversedScanAtRegionBoundary() throws Exception {
    testReversedScan(333, true, 222, true, -1);
    testReversedScan(444, true, 333, false, -1);
    testReversedScan(555, false, 444, true, -1);
    testReversedScan(666, false, 555, false, -1);
  }

  @Test
  public void testScanWithLimit() throws Exception {
    testScan(1, true, 998, false, 900); // from first region to last region
    testScan(123, true, 234, true, 100);
    testScan(234, true, 456, false, 100);
    testScan(345, false, 567, true, 100);
    testScan(456, false, 678, false, 100);
  }

  @Test
  public void testScanWithLimitGreaterThanActualCount() throws Exception {
    testScan(1, true, 998, false, 1000); // from first region to last region
    testScan(123, true, 345, true, 200);
    testScan(234, true, 456, false, 200);
    testScan(345, false, 567, true, 200);
    testScan(456, false, 678, false, 200);
  }

  @Test
  public void testReversedScanWithLimit() throws Exception {
    testReversedScan(998, true, 1, false, 900); // from last region to first region
    testReversedScan(543, true, 321, true, 100);
    testReversedScan(654, true, 432, false, 100);
    testReversedScan(765, false, 543, true, 100);
    testReversedScan(876, false, 654, false, 100);
  }

  @Test
  public void testReversedScanWithLimitGreaterThanActualCount() throws Exception {
    testReversedScan(998, true, 1, false, 1000); // from last region to first region
    testReversedScan(543, true, 321, true, 200);
    testReversedScan(654, true, 432, false, 200);
    testReversedScan(765, false, 543, true, 200);
    testReversedScan(876, false, 654, false, 200);
  }

  @Test
  public void testScanEndingEarly() throws Exception {
    testScan(1, true, 998, false, 0, 900); // from first region to last region
    testScan(123, true, 234, true, 0, 100);
    testScan(234, true, 456, false, 0, 100);
    testScan(345, false, 567, true, 0, 100);
    testScan(456, false, 678, false, 0, 100);
  }
}
