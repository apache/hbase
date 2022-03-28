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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({MiscTests.class, SmallTests.class})
public class TestMetricsWAL {
  @Rule
  public TestName name = new TestName();

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetricsWAL.class);

  @Test
  public void testLogRollRequested() throws Exception {
    MetricsWALSource source = mock(MetricsWALSourceImpl.class);
    MetricsWAL metricsWAL = new MetricsWAL(source);
    metricsWAL.logRollRequested(WALActionsListener.RollRequestReason.ERROR);
    metricsWAL.logRollRequested(WALActionsListener.RollRequestReason.LOW_REPLICATION);
    metricsWAL.logRollRequested(WALActionsListener.RollRequestReason.SLOW_SYNC);
    metricsWAL.logRollRequested(WALActionsListener.RollRequestReason.SIZE);

    // Log roll was requested four times
    verify(source, times(4)).incrementLogRollRequested();
    // One was because of an IO error.
    verify(source, times(1)).incrementErrorLogRoll();
    // One was because of low replication on the hlog.
    verify(source, times(1)).incrementLowReplicationLogRoll();
    // One was because of slow sync on the hlog.
    verify(source, times(1)).incrementSlowSyncLogRoll();
    // One was because of hlog file length limit.
    verify(source, times(1)).incrementSizeLogRoll();
  }

  @Test
  public void testPostSync() throws Exception {
    long nanos = TimeUnit.MILLISECONDS.toNanos(145);
    MetricsWALSource source = mock(MetricsWALSourceImpl.class);
    MetricsWAL metricsWAL = new MetricsWAL(source);
    metricsWAL.postSync(nanos, 1);
    verify(source, times(1)).incrementSyncTime(145);
  }

  @Test
  public void testSlowAppend() throws Exception {
    String testName = name.getMethodName();
    MetricsWALSource source = new MetricsWALSourceImpl(testName, testName, testName, testName);
    MetricsWAL metricsWAL = new MetricsWAL(source);
    TableName tableName = TableName.valueOf("foo");
    WALKey walKey = new WALKeyImpl(null, tableName, -1);
    // One not so slow append (< 1000)
    metricsWAL.postAppend(1, 900, walKey, null);
    // Two slow appends (> 1000)
    metricsWAL.postAppend(1, 1010, walKey, null);
    metricsWAL.postAppend(1, 2000, walKey, null);
    assertEquals(2, source.getSlowAppendCount());
  }

  @Test
  public void testWalWrittenInBytes() throws Exception {
    MetricsWALSource source = mock(MetricsWALSourceImpl.class);
    MetricsWAL metricsWAL = new MetricsWAL(source);
    TableName tableName = TableName.valueOf("foo");
    WALKey walKey = new WALKeyImpl(null, tableName, -1);
    metricsWAL.postAppend(100, 900, walKey, null);
    metricsWAL.postAppend(200, 2000, walKey, null);
    verify(source, times(1)).incrementWrittenBytes(100);
    verify(source, times(1)).incrementWrittenBytes(200);
  }

  @Test
  public void testPerTableWALMetrics() throws Exception {
    MetricsWALSourceImpl source = new MetricsWALSourceImpl("foo", "foo", "foo", "foo");
    final int numThreads = 10;
    final int numIters = 10;
    CountDownLatch latch = new CountDownLatch(numThreads);
    for (int i = 0; i < numThreads; i++) {
      final TableName tableName = TableName.valueOf("tab_" + i);
      long size = i;
      new Thread(() -> {
        for (int j = 0; j < numIters; j++) {
          source.incrementAppendCount(tableName);
          source.incrementAppendSize(tableName, size);
        }
        latch.countDown();
      }).start();
    }
    // Wait for threads to finish.
    latch.await();
    DynamicMetricsRegistry registry = source.getMetricsRegistry();
    // Validate the metrics
    for (int i = 0; i < numThreads; i++) {
      TableName tableName = TableName.valueOf("tab_" + i);
      long tableAppendCount =
          registry.getCounter(tableName + "." + MetricsWALSource.APPEND_COUNT, -1).value();
      assertEquals(numIters, tableAppendCount);
      long tableAppendSize =
          registry.getCounter(tableName + "." + MetricsWALSource.APPEND_SIZE, -1).value();
      assertEquals(i * numIters, tableAppendSize);
    }
  }

  @Test
  public void testLogRolls() {
    String testName = name.getMethodName();
    MetricsWALSource source = new MetricsWALSourceImpl(testName, testName, testName, testName);
    MetricsWAL metricsWAL = new MetricsWAL(source);
    Path path1 = new Path("path-1");
    int count = 1;
    // oldPath is null but newPath is not null;
    metricsWAL.postLogRoll(null, path1);
    assertEquals(count, source.getSuccessfulLogRolls());

    // Simulating a case where AbstractFSWAL#replaceWriter fails
    metricsWAL.postLogRoll(path1, path1);
    assertEquals(count, source.getSuccessfulLogRolls());

    count++;
    Path path2 = new Path("path-2");
    metricsWAL.postLogRoll(path1, path2);
    assertEquals(count, source.getSuccessfulLogRolls());
  }
}
