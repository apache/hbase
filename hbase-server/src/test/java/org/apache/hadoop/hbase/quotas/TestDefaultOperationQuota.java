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
package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestDefaultOperationQuota {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDefaultOperationQuota.class);

  private static final int DEFAULT_REQUESTS_PER_SECOND = 1000;
  private static ManualEnvironmentEdge envEdge = new ManualEnvironmentEdge();
  static {
    envEdge.setValue(EnvironmentEdgeManager.currentTime());
    // only active the envEdge for quotas package
    EnvironmentEdgeManagerTestHelper.injectEdgeForPackage(envEdge,
      ThrottleQuotaTestUtil.class.getPackage().getName());
  }

  @Test
  public void testScanEstimateNewScanner() {
    long blockSize = 64 * 1024;
    long nextCallSeq = 0;
    long maxScannerResultSize = 100 * 1024 * 1024;
    long maxBlockBytesScanned = 0;
    long prevBBSDifference = 0;
    long estimate = DefaultOperationQuota.getScanReadConsumeEstimate(blockSize, nextCallSeq,
      maxScannerResultSize, maxBlockBytesScanned, prevBBSDifference);

    // new scanner should estimate scan read as 1 block
    assertEquals(blockSize, estimate);
  }

  @Test
  public void testScanEstimateSecondNextCall() {
    long blockSize = 64 * 1024;
    long nextCallSeq = 1;
    long maxScannerResultSize = 100 * 1024 * 1024;
    long maxBlockBytesScanned = 10 * blockSize;
    long prevBBSDifference = 10 * blockSize;
    long estimate = DefaultOperationQuota.getScanReadConsumeEstimate(blockSize, nextCallSeq,
      maxScannerResultSize, maxBlockBytesScanned, prevBBSDifference);

    // 2nd next call should be estimated at maxBBS
    assertEquals(maxBlockBytesScanned, estimate);
  }

  @Test
  public void testScanEstimateFlatWorkload() {
    long blockSize = 64 * 1024;
    long nextCallSeq = 100;
    long maxScannerResultSize = 100 * 1024 * 1024;
    long maxBlockBytesScanned = 10 * blockSize;
    long prevBBSDifference = 0;
    long estimate = DefaultOperationQuota.getScanReadConsumeEstimate(blockSize, nextCallSeq,
      maxScannerResultSize, maxBlockBytesScanned, prevBBSDifference);

    // flat workload should not overestimate
    assertEquals(maxBlockBytesScanned, estimate);
  }

  @Test
  public void testScanEstimateVariableFlatWorkload() {
    long blockSize = 64 * 1024;
    long nextCallSeq = 1;
    long maxScannerResultSize = 100 * 1024 * 1024;
    long maxBlockBytesScanned = 10 * blockSize;
    long prevBBSDifference = 0;
    for (int i = 0; i < 100; i++) {
      long variation = Math.round(Math.random() * blockSize);
      if (variation % 2 == 0) {
        variation *= -1;
      }
      // despite +/- <1 block variation, we consider this workload flat
      prevBBSDifference = variation;

      long estimate = DefaultOperationQuota.getScanReadConsumeEstimate(blockSize, nextCallSeq + i,
        maxScannerResultSize, maxBlockBytesScanned, prevBBSDifference);

      // flat workload should not overestimate
      assertEquals(maxBlockBytesScanned, estimate);
    }
  }

  @Test
  public void testScanEstimateGrowingWorkload() {
    long blockSize = 64 * 1024;
    long nextCallSeq = 100;
    long maxScannerResultSize = 100 * 1024 * 1024;
    long maxBlockBytesScanned = 20 * blockSize;
    long prevBBSDifference = 10 * blockSize;
    long estimate = DefaultOperationQuota.getScanReadConsumeEstimate(blockSize, nextCallSeq,
      maxScannerResultSize, maxBlockBytesScanned, prevBBSDifference);

    // growing workload should overestimate
    assertTrue(nextCallSeq * maxBlockBytesScanned == estimate || maxScannerResultSize == estimate);
  }

  @Test
  public void testScanEstimateShrinkingWorkload() {
    long blockSize = 64 * 1024;
    long nextCallSeq = 100;
    long maxScannerResultSize = 100 * 1024 * 1024;
    long maxBlockBytesScanned = 20 * blockSize;
    long prevBBSDifference = -10 * blockSize;
    long estimate = DefaultOperationQuota.getScanReadConsumeEstimate(blockSize, nextCallSeq,
      maxScannerResultSize, maxBlockBytesScanned, prevBBSDifference);

    // shrinking workload should only shrink estimate to maxBBS
    assertEquals(maxBlockBytesScanned, estimate);
  }

  @Test
  public void testLargeBatchSaturatesReadNumLimit()
    throws RpcThrottlingException, InterruptedException {
    int limit = 10;
    QuotaProtos.Throttle throttle =
      QuotaProtos.Throttle.newBuilder().setReadNum(QuotaProtos.TimedQuota.newBuilder()
        .setSoftLimit(limit).setTimeUnit(HBaseProtos.TimeUnit.SECONDS).build()).build();
    QuotaLimiter limiter = TimeBasedLimiter.fromThrottle(throttle);
    DefaultOperationQuota quota =
      new DefaultOperationQuota(new Configuration(), 65536, DEFAULT_REQUESTS_PER_SECOND, limiter);

    // use the whole limit
    quota.checkBatchQuota(0, limit, false);

    // the next request should be rejected
    assertThrows(RpcThrottlingException.class, () -> quota.checkBatchQuota(0, 1, false));

    envEdge.incValue(1000);
    // after the TimeUnit, the limit should be refilled
    quota.checkBatchQuota(0, limit, false);
  }

  @Test
  public void testLargeBatchSaturatesReadWriteLimit()
    throws RpcThrottlingException, InterruptedException {
    int limit = 10;
    QuotaProtos.Throttle throttle =
      QuotaProtos.Throttle.newBuilder().setWriteNum(QuotaProtos.TimedQuota.newBuilder()
        .setSoftLimit(limit).setTimeUnit(HBaseProtos.TimeUnit.SECONDS).build()).build();
    QuotaLimiter limiter = TimeBasedLimiter.fromThrottle(throttle);
    DefaultOperationQuota quota =
      new DefaultOperationQuota(new Configuration(), 65536, DEFAULT_REQUESTS_PER_SECOND, limiter);

    // use the whole limit
    quota.checkBatchQuota(limit, 0, false);

    // the next request should be rejected
    assertThrows(RpcThrottlingException.class, () -> quota.checkBatchQuota(1, 0, false));

    envEdge.incValue(1000);
    // after the TimeUnit, the limit should be refilled
    quota.checkBatchQuota(limit, 0, false);
  }

  @Test
  public void testTooLargeReadBatchIsNotBlocked()
    throws RpcThrottlingException, InterruptedException {
    int limit = 10;
    QuotaProtos.Throttle throttle =
      QuotaProtos.Throttle.newBuilder().setReadNum(QuotaProtos.TimedQuota.newBuilder()
        .setSoftLimit(limit).setTimeUnit(HBaseProtos.TimeUnit.SECONDS).build()).build();
    QuotaLimiter limiter = TimeBasedLimiter.fromThrottle(throttle);
    DefaultOperationQuota quota =
      new DefaultOperationQuota(new Configuration(), 65536, DEFAULT_REQUESTS_PER_SECOND, limiter);

    // use more than the limit, which should succeed rather than being indefinitely blocked
    quota.checkBatchQuota(0, 10 + limit, false);

    // the next request should be blocked
    assertThrows(RpcThrottlingException.class, () -> quota.checkBatchQuota(0, 1, false));

    envEdge.incValue(1000);
    // even after the TimeUnit, the limit should not be refilled because we oversubscribed
    assertThrows(RpcThrottlingException.class, () -> quota.checkBatchQuota(0, limit, false));
  }

  @Test
  public void testTooLargeWriteBatchIsNotBlocked()
    throws RpcThrottlingException, InterruptedException {
    int limit = 10;
    QuotaProtos.Throttle throttle =
      QuotaProtos.Throttle.newBuilder().setWriteNum(QuotaProtos.TimedQuota.newBuilder()
        .setSoftLimit(limit).setTimeUnit(HBaseProtos.TimeUnit.SECONDS).build()).build();
    QuotaLimiter limiter = TimeBasedLimiter.fromThrottle(throttle);
    DefaultOperationQuota quota =
      new DefaultOperationQuota(new Configuration(), 65536, DEFAULT_REQUESTS_PER_SECOND, limiter);

    // use more than the limit, which should succeed rather than being indefinitely blocked
    quota.checkBatchQuota(10 + limit, 0, false);

    // the next request should be blocked
    assertThrows(RpcThrottlingException.class, () -> quota.checkBatchQuota(1, 0, false));

    envEdge.incValue(1000);
    // even after the TimeUnit, the limit should not be refilled because we oversubscribed
    assertThrows(RpcThrottlingException.class, () -> quota.checkBatchQuota(limit, 0, false));
  }

  @Test
  public void testTooLargeWriteSizeIsNotBlocked()
    throws RpcThrottlingException, InterruptedException {
    int limit = 50;
    QuotaProtos.Throttle throttle =
      QuotaProtos.Throttle.newBuilder().setWriteSize(QuotaProtos.TimedQuota.newBuilder()
        .setSoftLimit(limit).setTimeUnit(HBaseProtos.TimeUnit.SECONDS).build()).build();
    QuotaLimiter limiter = TimeBasedLimiter.fromThrottle(throttle);
    DefaultOperationQuota quota =
      new DefaultOperationQuota(new Configuration(), 65536, DEFAULT_REQUESTS_PER_SECOND, limiter);

    // writes are estimated a 100 bytes, so this will use 2x the limit but should not be blocked
    quota.checkBatchQuota(1, 0, false);

    // the next request should be blocked
    assertThrows(RpcThrottlingException.class, () -> quota.checkBatchQuota(1, 0, false));

    envEdge.incValue(1000);
    // even after the TimeUnit, the limit should not be refilled because we oversubscribed
    assertThrows(RpcThrottlingException.class, () -> quota.checkBatchQuota(limit, 0, false));
  }

  @Test
  public void testTooLargeReadSizeIsNotBlocked()
    throws RpcThrottlingException, InterruptedException {
    long blockSize = 65536;
    long limit = blockSize / 2;
    QuotaProtos.Throttle throttle =
      QuotaProtos.Throttle.newBuilder().setReadSize(QuotaProtos.TimedQuota.newBuilder()
        .setSoftLimit(limit).setTimeUnit(HBaseProtos.TimeUnit.SECONDS).build()).build();
    QuotaLimiter limiter = TimeBasedLimiter.fromThrottle(throttle);
    DefaultOperationQuota quota = new DefaultOperationQuota(new Configuration(), (int) blockSize,
      DEFAULT_REQUESTS_PER_SECOND, limiter);

    // reads are estimated at 1 block each, so this will use ~2x the limit but should not be blocked
    quota.checkBatchQuota(0, 1, false);

    // the next request should be blocked
    assertThrows(RpcThrottlingException.class, () -> quota.checkBatchQuota(0, 1, false));

    envEdge.incValue(1000);
    // even after the TimeUnit, the limit should not be refilled because we oversubscribed
    assertThrows(RpcThrottlingException.class, () -> quota.checkBatchQuota((int) limit, 1, false));
  }

  @Test
  public void testTooLargeRequestSizeIsNotBlocked()
    throws RpcThrottlingException, InterruptedException {
    long blockSize = 65536;
    long limit = blockSize / 2;
    QuotaProtos.Throttle throttle =
      QuotaProtos.Throttle.newBuilder().setReqSize(QuotaProtos.TimedQuota.newBuilder()
        .setSoftLimit(limit).setTimeUnit(HBaseProtos.TimeUnit.SECONDS).build()).build();
    QuotaLimiter limiter = TimeBasedLimiter.fromThrottle(throttle);
    DefaultOperationQuota quota = new DefaultOperationQuota(new Configuration(), (int) blockSize,
      DEFAULT_REQUESTS_PER_SECOND, limiter);

    // reads are estimated at 1 block each, so this will use ~2x the limit but should not be blocked
    quota.checkBatchQuota(0, 1, false);

    // the next request should be blocked
    assertThrows(RpcThrottlingException.class, () -> quota.checkBatchQuota(0, 1, false));

    envEdge.incValue(1000);
    // even after the TimeUnit, the limit should not be refilled because we oversubscribed
    assertThrows(RpcThrottlingException.class, () -> quota.checkBatchQuota((int) limit, 1, false));
  }

  // @Test
  // void testHandlerUsageTimeQuota() throws RpcThrottlingException {
  // long blockSize = 65536;
  // int handlerTimeLimitMs = 200; // 200ms per second
  // QuotaProtos.Throttle throttle = QuotaProtos.Throttle.newBuilder()
  // .setReqHandlerUsageMs(
  // QuotaProtos.TimedQuota.newBuilder()
  // .setSoftLimit(handlerTimeLimitMs)
  // .setTimeUnit(HBaseProtos.TimeUnit.SECONDS)
  // .build()
  // ).build();
  //
  // QuotaLimiter limiter = TimeBasedLimiter.fromThrottle(throttle);
  // DefaultOperationQuota quota = new DefaultOperationQuota(new Configuration(), blockSize,
  // DEFAULT_REQUESTS_PER_SECOND, limiter);
  //
  // // Simulate a batch that should be allowed (estimate is under the limit)
  // quota.checkBatchQuota(1, 0, false); // 1 op, estimate 100ms < 200ms limit
  //
  // // Simulate actual handler time usage: advance the clock by 100ms
  // long start = envEdge.currentTime();
  // envEdge.setValue(start + 100);
  // limiter.grabQuota(1, 0, false);
  //
  // // Next op: estimate will again be 100ms, but actual usage will push us over the limit
  // quota.checkBatchQuota(1, 0, false);
  // envEdge.setValue(start + 200); // another 100ms used, total 200ms
  // limiter.grabQuota(1, 0, false);
  //
  // // Now, any further operation should be blocked by the handler usage time quota
  // assertThrows(RpcThrottlingException.class, () -> {
  // quota.checkBatchQuota(1, 0, false);
  // });
  // }
}
