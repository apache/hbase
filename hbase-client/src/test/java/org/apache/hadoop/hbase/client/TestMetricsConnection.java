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

import com.google.protobuf.ByteString;
import com.yammer.metrics.util.RatioGauge;
import org.apache.hadoop.hbase.client.ConnectionManager.HConnectionImplementation;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.testclassification.MetricsTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.Assert.assertEquals;

@Category({MetricsTests.class, SmallTests.class})
public class TestMetricsConnection {
  private static final ExecutorService BATCH_POOL = Executors.newFixedThreadPool(2);
  private static MetricsConnection METRICS;
  private static final AtomicBoolean closed = new AtomicBoolean(false);
  private static final Runnable RUNNER = new Runnable() {
    @Override
    public void run() {
      try {
        while (!closed.get() && !Thread.interrupted()) {
          TimeUnit.MILLISECONDS.sleep(10);
        }
      } catch (InterruptedException e) {
      }
    }
  };
  @BeforeClass
  public static void beforeClass() {
    HConnectionImplementation mocked = Mockito.mock(HConnectionImplementation.class);
    Mockito.when(mocked.toString()).thenReturn("mocked-connection");
    Mockito.when(mocked.getCurrentBatchPool()).thenReturn(BATCH_POOL);
    BATCH_POOL.submit(RUNNER);
    METRICS = new MetricsConnection(mocked);
  }

  @AfterClass
  public static void afterClass() throws InterruptedException {
    METRICS.shutdown();
    BATCH_POOL.shutdownNow();
    BATCH_POOL.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
  }

  @Test
  public void testStaticMetrics() throws IOException {
    final byte[] foo = Bytes.toBytes("foo");
    final RegionSpecifier region = RegionSpecifier.newBuilder()
        .setValue(ByteString.EMPTY)
        .setType(RegionSpecifierType.REGION_NAME)
        .build();
    final int loop = 5;

    for (int i = 0; i < loop; i++) {
      METRICS.updateRpc(
          ClientService.getDescriptor().findMethodByName("Get"),
          GetRequest.getDefaultInstance(),
          MetricsConnection.newCallStats());
      METRICS.updateRpc(
          ClientService.getDescriptor().findMethodByName("Scan"),
          ScanRequest.getDefaultInstance(),
          MetricsConnection.newCallStats());
      METRICS.updateRpc(
          ClientService.getDescriptor().findMethodByName("Multi"),
          MultiRequest.getDefaultInstance(),
          MetricsConnection.newCallStats());
      METRICS.updateRpc(
          ClientService.getDescriptor().findMethodByName("Mutate"),
          MutateRequest.newBuilder()
              .setMutation(ProtobufUtil.toMutation(MutationType.APPEND, new Append(foo)))
              .setRegion(region)
              .build(),
          MetricsConnection.newCallStats());
      METRICS.updateRpc(
          ClientService.getDescriptor().findMethodByName("Mutate"),
          MutateRequest.newBuilder()
              .setMutation(ProtobufUtil.toMutation(MutationType.DELETE, new Delete(foo)))
              .setRegion(region)
              .build(),
          MetricsConnection.newCallStats());
      METRICS.updateRpc(
          ClientService.getDescriptor().findMethodByName("Mutate"),
          MutateRequest.newBuilder()
              .setMutation(ProtobufUtil.toMutation(MutationType.INCREMENT, new Increment(foo)))
              .setRegion(region)
              .build(),
          MetricsConnection.newCallStats());
      METRICS.updateRpc(
          ClientService.getDescriptor().findMethodByName("Mutate"),
          MutateRequest.newBuilder()
              .setMutation(ProtobufUtil.toMutation(MutationType.PUT, new Put(foo)))
              .setRegion(region)
              .build(),
          MetricsConnection.newCallStats());
    }
    for (MetricsConnection.CallTracker t : new MetricsConnection.CallTracker[] {
        METRICS.getTracker, METRICS.scanTracker, METRICS.multiTracker, METRICS.appendTracker,
        METRICS.deleteTracker, METRICS.incrementTracker, METRICS.putTracker
    }) {
      Assert.assertEquals("Failed to invoke callTimer on " + t, loop, t.callTimer.count());
      Assert.assertEquals("Failed to invoke reqHist on " + t, loop, t.reqHist.count());
      Assert.assertEquals("Failed to invoke respHist on " + t, loop, t.respHist.count());
    }
    RatioGauge executorMetrics = (RatioGauge) METRICS.getMetricsRegistry()
            .allMetrics().get(METRICS.getExecutorPoolName());
    RatioGauge metaMetrics = (RatioGauge) METRICS.getMetricsRegistry()
            .allMetrics().get(METRICS.getMetaPoolName());
    assertEquals((double) 0.5, executorMetrics.value(), 0);
    assertEquals(Double.NaN, metaMetrics.value(), 0);
  }
}
