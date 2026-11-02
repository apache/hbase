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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hbase.exceptions.ScannerResetException;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.HBaseRpcControllerImpl;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;

@Tag(ClientTests.TAG)
@Tag(MediumTests.TAG)
@HBaseParameterizedTestTemplate(name = "[{index}] remote={0}")
public class TestAsyncScanSingleRegionRpcRetryingCaller {

  private final boolean remote;

  public TestAsyncScanSingleRegionRpcRetryingCaller(boolean remote) {
    this.remote = remote;
  }

  public static Stream<Arguments> parameters() {
    return Stream.of(Arguments.of(false), Arguments.of(true));
  }

  private void assertRetryMetrics(IOException error, boolean closeScanner) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    User user = User.getCurrent();
    try (AsyncConnectionImpl conn =
      new AsyncConnectionImpl(conf, new DoNothingConnectionRegistry(conf), "test", user)) {
      ClientService.Interface stub = mock(ClientService.Interface.class);
      doAnswer(invocation -> {
        HBaseRpcController controller = invocation.getArgument(0);
        ScanRequest request = invocation.getArgument(1);
        RpcCallback<ScanResponse> done = invocation.getArgument(2);
        if (request.getCloseScanner()) {
          done.run(ScanResponse.getDefaultInstance());
        } else {
          controller.setFailed(error);
          done.run(null);
        }
        return null;
      }).when(stub).scan(any(), any(), any());

      HRegionLocation location =
        new HRegionLocation(RegionInfoBuilder.newBuilder(TableName.valueOf("test")).build(),
          ServerName.valueOf("rs", 16020, 1));
      ScanMetrics metrics = new ScanMetrics();
      AsyncScanSingleRegionRpcRetryingCaller caller = conn.callerFactory.scanSingleRegion().id(1)
        .setScan(new Scan().setScanMetricsEnabled(true)).metrics(metrics).remote(remote)
        .resultCache(new CompleteScanResultCache()).consumer(mock(AdvancedScanResultConsumer.class))
        .stub(stub).location(location).maxAttempts(3).build();

      // A successful open is followed by a scan-next RPC that fails and requires reopening.
      assertTrue(
        caller
          .start(new HBaseRpcControllerImpl(),
            ScanResponse.newBuilder().setMoreResultsInRegion(true).build())
          .get(5, TimeUnit.SECONDS));
      verify(stub).scan(any(), argThat(request -> !request.getCloseScanner()), any());
      verify(stub, times(closeScanner ? 1 : 0)).scan(any(), argThat(ScanRequest::getCloseScanner),
        any());
      assertEquals(1, metrics.countOfRPCRetries.get());
      assertEquals(remote ? 1 : 0, metrics.countOfRemoteRPCRetries.get());
    }
  }

  @TestTemplate
  public void testRetryMetricsAfterScannerReset() throws Exception {
    assertRetryMetrics(new ScannerResetException("Injected scanner reset"), false);
  }

  @TestTemplate
  public void testRetryMetricsAfterOutOfOrderScannerNext() throws Exception {
    assertRetryMetrics(new OutOfOrderScannerNextException("Injected out-of-order scan"), true);
  }
}
