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
package org.apache.hadoop.hbase.master.normalizer;

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A test over {@link RegionNormalizerWorker}. Being a background thread, the only points of
 * interaction we have to this class are its input source ({@link RegionNormalizerWorkQueue} and
 * its callbacks invoked against {@link RegionNormalizer} and {@link MasterServices}. The work
 * queue is simple enough to use directly; for {@link MasterServices}, use a mock because, as of
 * now, the worker only invokes 4 methods.
 */
@Category({ MasterTests.class, SmallTests.class})
public class TestRegionNormalizerWorker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionNormalizerWorker.class);

  @Rule
  public TestName testName = new TestName();
  @Rule
  public TableNameTestRule tableName = new TableNameTestRule();

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private MasterServices masterServices;
  @Mock
  private RegionNormalizer regionNormalizer;

  private HBaseCommonTestingUtility testingUtility;
  private RegionNormalizerWorkQueue<TableName> queue;
  private ExecutorService workerPool;

  private final AtomicReference<Throwable> workerThreadThrowable = new AtomicReference<>();

  @Before
  public void before() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(masterServices.skipRegionManagementAction(any())).thenReturn(false);
    testingUtility = new HBaseCommonTestingUtility();
    queue = new RegionNormalizerWorkQueue<>();
    workerThreadThrowable.set(null);

    final String threadNameFmt =
      TestRegionNormalizerWorker.class.getSimpleName() + "-" + testName.getMethodName() + "-%d";
    final ThreadFactory threadFactory = new ThreadFactoryBuilder()
      .setNameFormat(threadNameFmt)
      .setDaemon(true)
      .setUncaughtExceptionHandler((t, e) -> workerThreadThrowable.set(e))
      .build();
    workerPool = Executors.newSingleThreadExecutor(threadFactory);
  }

  @After
  public void after() throws Exception {
    workerPool.shutdownNow(); // shutdownNow to interrupt the worker thread sitting on `take()`
    assertTrue("timeout waiting for worker thread to terminate",
      workerPool.awaitTermination(30, TimeUnit.SECONDS));
    final Throwable workerThrowable = workerThreadThrowable.get();
    assertThat("worker thread threw unexpected exception", workerThrowable, nullValue());
  }

  @Test
  public void testMergeCounter() throws Exception {
    final TableName tn = tableName.getTableName();
    final TableDescriptor tnDescriptor = TableDescriptorBuilder.newBuilder(tn)
      .setNormalizationEnabled(true)
      .build();
    when(masterServices.getTableDescriptors().get(tn)).thenReturn(tnDescriptor);
    when(masterServices.mergeRegions(any(), anyBoolean(), anyLong(), anyLong()))
      .thenReturn(1L);
    when(regionNormalizer.computePlansForTable(tnDescriptor))
      .thenReturn(singletonList(new MergeNormalizationPlan.Builder()
        .addTarget(RegionInfoBuilder.newBuilder(tn).build(), 10)
        .addTarget(RegionInfoBuilder.newBuilder(tn).build(), 20)
        .build()));

    final RegionNormalizerWorker worker = new RegionNormalizerWorker(
      testingUtility.getConfiguration(), masterServices, regionNormalizer, queue);
    final long beforeMergePlanCount = worker.getMergePlanCount();
    workerPool.submit(worker);
    queue.put(tn);

    assertThatEventually("executing work should see plan count increase",
      worker::getMergePlanCount, greaterThan(beforeMergePlanCount));
  }

  @Test
  public void testSplitCounter() throws Exception {
    final TableName tn = tableName.getTableName();
    final TableDescriptor tnDescriptor = TableDescriptorBuilder.newBuilder(tn)
      .setNormalizationEnabled(true)
      .build();
    when(masterServices.getTableDescriptors().get(tn)).thenReturn(tnDescriptor);
    when(masterServices.splitRegion(any(), any(), anyLong(), anyLong()))
      .thenReturn(1L);
    when(regionNormalizer.computePlansForTable(tnDescriptor))
      .thenReturn(singletonList(
        new SplitNormalizationPlan(RegionInfoBuilder.newBuilder(tn).build(), 10)));

    final RegionNormalizerWorker worker = new RegionNormalizerWorker(
      testingUtility.getConfiguration(), masterServices, regionNormalizer, queue);
    final long beforeSplitPlanCount = worker.getSplitPlanCount();
    workerPool.submit(worker);
    queue.put(tn);

    assertThatEventually("executing work should see plan count increase",
      worker::getSplitPlanCount, greaterThan(beforeSplitPlanCount));
  }

  /**
   * Assert that a rate limit is honored, at least in a rough way. Maintainers should manually
   * inspect the log messages emitted by the worker thread to confirm that expected behavior.
   */
  @Test
  public void testRateLimit() throws Exception {
    final TableName tn = tableName.getTableName();
    final TableDescriptor tnDescriptor = TableDescriptorBuilder.newBuilder(tn)
      .setNormalizationEnabled(true)
      .build();
    final RegionInfo splitRegionInfo = RegionInfoBuilder.newBuilder(tn).build();
    final RegionInfo mergeRegionInfo1 = RegionInfoBuilder.newBuilder(tn).build();
    final RegionInfo mergeRegionInfo2 = RegionInfoBuilder.newBuilder(tn).build();
    when(masterServices.getTableDescriptors().get(tn)).thenReturn(tnDescriptor);
    when(masterServices.splitRegion(any(), any(), anyLong(), anyLong()))
      .thenReturn(1L);
    when(masterServices.mergeRegions(any(), anyBoolean(), anyLong(), anyLong()))
      .thenReturn(1L);
    when(regionNormalizer.computePlansForTable(tnDescriptor))
      .thenReturn(Arrays.asList(
        new SplitNormalizationPlan(splitRegionInfo, 2),
        new MergeNormalizationPlan.Builder()
          .addTarget(mergeRegionInfo1, 1)
          .addTarget(mergeRegionInfo2, 2)
          .build(),
        new SplitNormalizationPlan(splitRegionInfo, 1)));

    final Configuration conf = testingUtility.getConfiguration();
    conf.set("hbase.normalizer.throughput.max_bytes_per_sec", "1m");
    final RegionNormalizerWorker worker = new RegionNormalizerWorker(
      testingUtility.getConfiguration(), masterServices, regionNormalizer, queue);
    workerPool.submit(worker);
    final long startTime = System.nanoTime();
    queue.put(tn);

    assertThatEventually("executing work should see split plan count increase",
      worker::getSplitPlanCount, comparesEqualTo(2L));
    assertThatEventually("executing work should see merge plan count increase",
      worker::getMergePlanCount, comparesEqualTo(1L));

    final long endTime = System.nanoTime();
    assertThat("rate limited normalizer should have taken at least 5 seconds",
      Duration.ofNanos(endTime - startTime), greaterThanOrEqualTo(Duration.ofSeconds(5)));
  }

  /**
   * Repeatedly evaluates {@code matcher} against the result of calling {@code actualSupplier}
   * until the matcher succeeds or the timeout period of 30 seconds is exhausted.
   */
  private <T> void assertThatEventually(
    final String reason,
    final Supplier<? extends T> actualSupplier,
    final Matcher<? super T> matcher
  ) throws Exception {
    testingUtility.waitFor(TimeUnit.SECONDS.toMillis(30),
      new Waiter.ExplainingPredicate<Exception>() {
        private T lastValue = null;

        @Override
        public String explainFailure() {
          final Description description = new StringDescription()
            .appendText(reason)
            .appendText("\nExpected: ")
            .appendDescriptionOf(matcher)
            .appendText("\n     but: ");
          matcher.describeMismatch(lastValue, description);
          return description.toString();
        }

        @Override public boolean evaluate() {
          lastValue = actualSupplier.get();
          return matcher.matches(lastValue);
        }
      });
  }
}
