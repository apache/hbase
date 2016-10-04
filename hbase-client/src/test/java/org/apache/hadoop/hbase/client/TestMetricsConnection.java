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

import org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.testclassification.ClientTests;
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

@Category({ClientTests.class, MetricsTests.class, SmallTests.class})
public class TestMetricsConnection {

  private static MetricsConnection METRICS;

  @BeforeClass
  public static void beforeClass() {
    ConnectionImplementation mocked = Mockito.mock(ConnectionImplementation.class);
    Mockito.when(mocked.toString()).thenReturn("mocked-connection");
    METRICS = new MetricsConnection(Mockito.mock(ConnectionImplementation.class));
  }

  @AfterClass
  public static void afterClass() {
    METRICS.shutdown();
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
      Assert.assertEquals("Failed to invoke callTimer on " + t, loop, t.callTimer.getCount());
      Assert.assertEquals("Failed to invoke reqHist on " + t, loop, t.reqHist.getCount());
      Assert.assertEquals("Failed to invoke respHist on " + t, loop, t.respHist.getCount());
    }
  }
}
