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
package org.apache.hadoop.hbase.coprocessor;

import static org.apache.hadoop.hbase.client.coprocessor.AggregationHelper.getParsedGenericInstance;
import static org.apache.hadoop.hbase.quotas.RpcThrottlingException.Type.ReadSizeExceeded;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateRequest;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateResponse;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.quotas.OperationQuota;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionScannerImpl;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

/**
 * Test AggregateImplementation with throttling and partial results
 */
@Category({ SmallTests.class, CoprocessorTests.class })
public class TestAggregateImplementation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAggregateImplementation.class);

  private static final byte[] CF = Bytes.toBytes("CF");
  private static final byte[] CQ = Bytes.toBytes("CQ");
  private static final int NUM_ROWS = 5;
  private static final int THROTTLE_AT_ROW = 2;
  private static final LongColumnInterpreter LONG_COLUMN_INTERPRETER = new LongColumnInterpreter();

  private AggregateImplementation<Long, Long, HBaseProtos.LongMsg, HBaseProtos.LongMsg,
    HBaseProtos.LongMsg> aggregate;
  private RegionCoprocessorEnvironment env;
  private HRegion region;
  private RegionScannerImpl scanner;
  private Scan scan;
  private AggregateRequest request;
  private RpcController controller;

  @Before
  public void setUp() throws Exception {
    env = mock(RegionCoprocessorEnvironment.class);
    region = mock(HRegion.class);
    RegionCoprocessorHost host = mock(RegionCoprocessorHost.class);
    when(env.getRegion()).thenReturn(region);
    when(region.getCoprocessorHost()).thenReturn(host);

    RegionInfo regionInfo = mock(RegionInfo.class);
    when(region.getRegionInfo()).thenReturn(regionInfo);
    when(regionInfo.getRegionNameAsString()).thenReturn("testRegion");

    scan = new Scan().addColumn(CF, CQ).setCaching(1);

    scanner = mock(RegionScannerImpl.class);
    doAnswer(createMockScanner()).when(scanner).next(any(List.class));
    when(region.getScanner(any())).thenReturn(scanner);

    doAnswer(createMockQuota()).when(env).checkScanQuota(any(), anyLong(), anyLong());

    request = AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
      .setInterpreterClassName(LongColumnInterpreter.class.getName())
      .setClientSupportsPartialResult(true).build();

    controller = mock(RpcController.class);

    aggregate = new AggregateImplementation<>();
    aggregate.start(env);
  }

  private Answer<Boolean> createMockScanner() throws IOException {
    AtomicInteger callCount = new AtomicInteger(0);
    return invocation -> {
      List<Cell> results = (List<Cell>) invocation.getArguments()[0];
      int call = callCount.getAndIncrement();
      if (call < NUM_ROWS) {
        Cell cell = mock(Cell.class);
        when(cell.getRowArray()).thenReturn(Bytes.toBytes("row" + (call + 1)));
        when(cell.getRowOffset()).thenReturn(0);
        when(cell.getRowLength()).thenReturn((short) 4);

        when(cell.getValueArray()).thenReturn(Bytes.toBytes((long) call + 1));
        when(cell.getValueOffset()).thenReturn(0);
        when(cell.getValueLength()).thenReturn(8);
        results.add(cell);
        return call < NUM_ROWS - 1;
      } else {
        // No more rows
        return false;
      }
    };
  }

  private Answer<OperationQuota> createMockQuota() throws IOException {
    OperationQuota mockQuota = mock(OperationQuota.class);

    final AtomicInteger rowCount = new AtomicInteger(0);

    return invocation -> {
      int count = rowCount.incrementAndGet();
      if (count == THROTTLE_AT_ROW) {
        RpcThrottlingException throttlingEx =
          new RpcThrottlingException(ReadSizeExceeded, 1000, "Throttled for testing");
        throw throttlingEx;
      }
      return mockQuota;
    };
  }

  private void reset() throws IOException {
    // Create a non-throttling quota for the second call, since throttling
    // should only happen on the first call
    OperationQuota nonThrottlingQuota = mock(OperationQuota.class);
    when(env.checkScanQuota(any(), anyLong(), anyLong())).thenReturn(nonThrottlingQuota);
  }

  @Test
  public void testMaxWithThrottling() throws Exception {
    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    aggregate.getMax(controller, request, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();
    assertTrue("Response should indicate there are more rows", response.hasNextChunkStartRow());
    assertEquals("Wait interval should be set", 1000, response.getWaitIntervalMs());
    ByteString b = response.getFirstPart(0);
    HBaseProtos.LongMsg q = getParsedGenericInstance(LONG_COLUMN_INTERPRETER.getClass(), 3, b);
    assertEquals(1L, (long) LONG_COLUMN_INTERPRETER.getCellValueFromProto(q));

    // Create a second request with the next chunk start row
    AggregateRequest request2 = AggregateRequest.newBuilder(request)
      .setScan(request.getScan().toBuilder().setStartRow(response.getNextChunkStartRow()).build())
      .build();

    // Reset throttles for second call
    reset();

    RpcCallback<AggregateResponse> callback2 = mock(RpcCallback.class);
    aggregate.getMax(controller, request2, callback2);

    verify(callback2).run(responseCaptor.capture());

    AggregateResponse response2 = responseCaptor.getValue();
    b = response2.getFirstPart(0);
    q = getParsedGenericInstance(LONG_COLUMN_INTERPRETER.getClass(), 3, b);
    assertEquals("Final max value should be correct", 5L,
      (long) LONG_COLUMN_INTERPRETER.getCellValueFromProto(q));

    assertFalse("Response should not indicate there are more rows",
      response2.hasNextChunkStartRow());
  }

  @Test
  public void testMaxThrottleWithNoResults() throws Exception {
    AggregateRequest request = AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
      .setInterpreterClassName(LongColumnInterpreter.class.getName())
      .setClientSupportsPartialResult(true).build();

    when(env.checkScanQuota(any(), anyLong(), anyLong()))
      .thenThrow(new RpcThrottlingException(ReadSizeExceeded, 1000, "Throttled for testing"));

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    // call gets no results, and response should contain the same start row as the request, because
    // no progress in the scan was made
    aggregate.getMax(controller, request, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();

    assertTrue("Response should indicate there are more rows", response.hasNextChunkStartRow());
    assertEquals("response should contain the same start row as the request",
      request.getScan().getStartRow(), response.getNextChunkStartRow());
  }

  @Test
  public void testMaxWithNoResults() throws Exception {
    AggregateRequest request = AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
      .setInterpreterClassName(LongColumnInterpreter.class.getName())
      .setClientSupportsPartialResult(false).build();

    doAnswer(invocation -> false).when(scanner).next(any(List.class));

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    aggregate.getMax(controller, request, callback);
    verify(callback).run(responseCaptor.capture());
    AggregateResponse response = responseCaptor.getValue();
    assertNull(response);
  }

  @Test
  public void testMaxDoesNotSupportPartialResults() throws Exception {
    AggregateRequest noPartialRequest =
      AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
        .setInterpreterClassName(LongColumnInterpreter.class.getName())
        .setClientSupportsPartialResult(false).build();

    OperationQuota nonThrottlingQuota = mock(OperationQuota.class);
    when(env.checkScanQuota(any(), anyLong(), anyLong())).thenReturn(nonThrottlingQuota);

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    // Call should complete without throttling
    aggregate.getMax(controller, noPartialRequest, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();

    assertFalse("Response should not indicate there are more rows",
      response.hasNextChunkStartRow());
    ByteString b = response.getFirstPart(0);
    HBaseProtos.LongMsg q = getParsedGenericInstance(LONG_COLUMN_INTERPRETER.getClass(), 3, b);
    assertEquals(5L, (long) LONG_COLUMN_INTERPRETER.getCellValueFromProto(q));
  }

  @Test
  public void testMinWithThrottling() throws Exception {
    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    // First call should get throttled
    aggregate.getMin(controller, request, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();
    assertTrue("Response should indicate there are more rows", response.hasNextChunkStartRow());
    assertEquals("Wait interval should be set", 1000, response.getWaitIntervalMs());
    ByteString b = response.getFirstPart(0);
    HBaseProtos.LongMsg q = getParsedGenericInstance(LONG_COLUMN_INTERPRETER.getClass(), 3, b);
    assertEquals(1L, (long) LONG_COLUMN_INTERPRETER.getCellValueFromProto(q));

    AggregateRequest request2 = AggregateRequest.newBuilder(request)
      .setScan(request.getScan().toBuilder().setStartRow(response.getNextChunkStartRow()).build())
      .build();

    // Reset throttles for second call
    reset();

    RpcCallback<AggregateResponse> callback2 = mock(RpcCallback.class);
    aggregate.getMin(controller, request2, callback2);

    verify(callback2).run(responseCaptor.capture());

    AggregateResponse response2 = responseCaptor.getValue();
    b = response.getFirstPart(0);
    q = getParsedGenericInstance(LONG_COLUMN_INTERPRETER.getClass(), 3, b);
    assertEquals(1L, (long) LONG_COLUMN_INTERPRETER.getCellValueFromProto(q));
    assertFalse("Response should not indicate there are more rows",
      response2.hasNextChunkStartRow());
  }

  @Test
  public void testMinThrottleWithNoResults() throws Exception {
    AggregateRequest request = AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
      .setInterpreterClassName(LongColumnInterpreter.class.getName())
      .setClientSupportsPartialResult(true).build();

    when(env.checkScanQuota(any(), anyLong(), anyLong()))
      .thenThrow(new RpcThrottlingException(ReadSizeExceeded, 1000, "Throttled for testing"));

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    // call gets no results, and response should contain the same start row as the request, because
    // no progress in the scan was made
    aggregate.getMin(controller, request, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();

    assertTrue("Response should indicate there are more rows", response.hasNextChunkStartRow());
    assertEquals("response should contain the same start row as the request",
      request.getScan().getStartRow(), response.getNextChunkStartRow());
  }

  @Test
  public void testMinWithNoResults() throws Exception {
    AggregateRequest request = AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
      .setInterpreterClassName(LongColumnInterpreter.class.getName())
      .setClientSupportsPartialResult(false).build();

    doAnswer(invocation -> false).when(scanner).next(any(List.class));

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    aggregate.getMin(controller, request, callback);
    verify(callback).run(responseCaptor.capture());
    AggregateResponse response = responseCaptor.getValue();
    assertNull(response);
  }

  @Test
  public void testMinDoesNotSupportPartialResults() throws Exception {
    AggregateRequest noPartialRequest =
      AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
        .setInterpreterClassName(LongColumnInterpreter.class.getName())
        .setClientSupportsPartialResult(false).build();

    OperationQuota nonThrottlingQuota = mock(OperationQuota.class);
    when(env.checkScanQuota(any(), anyLong(), anyLong())).thenReturn(nonThrottlingQuota);

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    // Call should complete without throttling
    aggregate.getMin(controller, noPartialRequest, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();

    assertFalse("Response should not indicate there are more rows",
      response.hasNextChunkStartRow());
    ByteString b = response.getFirstPart(0);
    HBaseProtos.LongMsg q = getParsedGenericInstance(LONG_COLUMN_INTERPRETER.getClass(), 3, b);
    assertEquals(1L, (long) LONG_COLUMN_INTERPRETER.getCellValueFromProto(q));
  }

  @Test
  public void testSumWithThrottling() throws Exception {
    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    aggregate.getSum(controller, request, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();
    assertTrue("Response should indicate there are more rows", response.hasNextChunkStartRow());
    assertEquals("Wait interval should be set", 1000, response.getWaitIntervalMs());
    ByteString b = response.getFirstPart(0);
    HBaseProtos.LongMsg q = getParsedGenericInstance(LONG_COLUMN_INTERPRETER.getClass(), 3, b);
    assertEquals(1L, (long) LONG_COLUMN_INTERPRETER.getCellValueFromProto(q));

    // Create a second request with the next chunk start row
    AggregateRequest request2 = AggregateRequest.newBuilder(request)
      .setScan(request.getScan().toBuilder().setStartRow(response.getNextChunkStartRow()).build())
      .build();

    // Reset throttles for second call
    reset();

    RpcCallback<AggregateResponse> callback2 = mock(RpcCallback.class);
    aggregate.getSum(controller, request2, callback2);

    verify(callback2).run(responseCaptor.capture());

    AggregateResponse response2 = responseCaptor.getValue();
    b = response2.getFirstPart(0);
    q = getParsedGenericInstance(LONG_COLUMN_INTERPRETER.getClass(), 3, b);
    assertEquals(14L, (long) LONG_COLUMN_INTERPRETER.getCellValueFromProto(q));
    assertFalse("Response should not indicate there are more rows",
      response2.hasNextChunkStartRow());
  }

  @Test
  public void testSumThrottleWithNoResults() throws Exception {
    AggregateRequest request = AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
      .setInterpreterClassName(LongColumnInterpreter.class.getName())
      .setClientSupportsPartialResult(true).build();

    when(env.checkScanQuota(any(), anyLong(), anyLong()))
      .thenThrow(new RpcThrottlingException(ReadSizeExceeded, 1000, "Throttled for testing"));

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    // call gets no results, and response should contain the same start row as the request, because
    // no progress in the scan was made
    aggregate.getSum(controller, request, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();

    assertTrue("Response should indicate there are more rows", response.hasNextChunkStartRow());
    assertEquals("response should contain the same start row as the request",
      request.getScan().getStartRow(), response.getNextChunkStartRow());
  }

  @Test
  public void testSumWithNoResults() throws Exception {
    AggregateRequest request = AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
      .setInterpreterClassName(LongColumnInterpreter.class.getName())
      .setClientSupportsPartialResult(false).build();

    doAnswer(invocation -> false).when(scanner).next(any(List.class));

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    aggregate.getSum(controller, request, callback);
    verify(callback).run(responseCaptor.capture());
    AggregateResponse response = responseCaptor.getValue();
    assertNull(response);
  }

  @Test
  public void testSumDoesNotSupportPartialResults() throws Exception {
    AggregateRequest noPartialRequest =
      AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
        .setInterpreterClassName(LongColumnInterpreter.class.getName())
        .setClientSupportsPartialResult(false).build();

    OperationQuota nonThrottlingQuota = mock(OperationQuota.class);
    when(env.checkScanQuota(any(), anyLong(), anyLong())).thenReturn(nonThrottlingQuota);

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    // Call should complete without throttling
    aggregate.getSum(controller, noPartialRequest, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();

    assertFalse("Response should not indicate there are more rows",
      response.hasNextChunkStartRow());
    ByteString b = response.getFirstPart(0);
    HBaseProtos.LongMsg q = getParsedGenericInstance(LONG_COLUMN_INTERPRETER.getClass(), 3, b);
    assertEquals(15L, (long) LONG_COLUMN_INTERPRETER.getCellValueFromProto(q));
  }

  @Test
  public void testRowNumWithThrottling() throws Exception {
    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    // First call should get throttled
    aggregate.getRowNum(controller, request, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();
    assertTrue("Response should indicate there are more rows", response.hasNextChunkStartRow());
    assertEquals("Wait interval should be set", 1000, response.getWaitIntervalMs());
    assertEquals(THROTTLE_AT_ROW - 1, response.getFirstPart(0).asReadOnlyByteBuffer().getLong());

    AggregateRequest request2 = AggregateRequest.newBuilder(request)
      .setScan(request.getScan().toBuilder().setStartRow(response.getNextChunkStartRow()).build())
      .build();

    // Reset throttle for second call
    reset();

    RpcCallback<AggregateResponse> callback2 = mock(RpcCallback.class);
    aggregate.getRowNum(controller, request2, callback2);

    verify(callback2).run(responseCaptor.capture());

    AggregateResponse response2 = responseCaptor.getValue();
    assertEquals("Final row count should be correct", NUM_ROWS - THROTTLE_AT_ROW + 1,
      response2.getFirstPart(0).asReadOnlyByteBuffer().getLong());
    assertFalse("Response should not indicate there are more rows",
      response2.hasNextChunkStartRow());
  }

  @Test
  public void testRowNumWithScannerCaching() throws Exception {
    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    // set caching such that throttles are not triggered
    scan = new Scan().addColumn(CF, CQ).setCaching(5);
    request = AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
      .setInterpreterClassName(LongColumnInterpreter.class.getName())
      .setClientSupportsPartialResult(true).build();
    aggregate.getRowNum(controller, request, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();
    assertFalse("Response should not indicate there are more rows",
      response.hasNextChunkStartRow());
    assertEquals(NUM_ROWS, response.getFirstPart(0).asReadOnlyByteBuffer().getLong());
  }

  @Test
  public void testRowNumThrottleWithNoResults() throws Exception {
    AggregateRequest request = AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
      .setInterpreterClassName(LongColumnInterpreter.class.getName())
      .setClientSupportsPartialResult(true).build();

    when(env.checkScanQuota(any(), anyLong(), anyLong()))
      .thenThrow(new RpcThrottlingException(ReadSizeExceeded, 1000, "Throttled for testing"));

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    // call gets no results, and response should contain the same start row as the request, because
    // no progress in the scan was made
    aggregate.getRowNum(controller, request, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();

    assertTrue("Response should indicate there are more rows", response.hasNextChunkStartRow());
    assertEquals("response should contain the same start row as the request",
      request.getScan().getStartRow(), response.getNextChunkStartRow());
  }

  @Test
  public void testRowNumWithNoResults() throws Exception {
    AggregateRequest request = AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
      .setInterpreterClassName(LongColumnInterpreter.class.getName())
      .setClientSupportsPartialResult(false).build();

    doAnswer(invocation -> false).when(scanner).next(any(List.class));

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    aggregate.getRowNum(controller, request, callback);
    verify(callback).run(responseCaptor.capture());
    AggregateResponse response = responseCaptor.getValue();

    assertFalse("Response should indicate there are no more rows", response.hasNextChunkStartRow());
    assertEquals(0, response.getFirstPart(0).asReadOnlyByteBuffer().getLong());
  }

  @Test
  public void testRowNumDoesNotSupportPartialResults() throws Exception {
    AggregateRequest noPartialRequest =
      AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
        .setInterpreterClassName(LongColumnInterpreter.class.getName())
        .setClientSupportsPartialResult(false).build();

    OperationQuota nonThrottlingQuota = mock(OperationQuota.class);
    when(env.checkScanQuota(any(), anyLong(), anyLong())).thenReturn(nonThrottlingQuota);

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    // Call should complete without throttling
    aggregate.getRowNum(controller, noPartialRequest, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();

    assertFalse("Response should not indicate there are more rows",
      response.hasNextChunkStartRow());
    assertEquals("Final row count should be correct", NUM_ROWS,
      response.getFirstPart(0).asReadOnlyByteBuffer().getLong());
  }

  @Test
  public void testAvgWithThrottling() throws Exception {
    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    // First call should get throttled
    aggregate.getAvg(controller, request, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();
    assertTrue("Response should indicate there are more rows", response.hasNextChunkStartRow());
    assertEquals("Wait interval should be set", 1000, response.getWaitIntervalMs());
    assertEquals("sum should be 1", 1L, (long) LONG_COLUMN_INTERPRETER.getPromotedValueFromProto(
      getParsedGenericInstance(LONG_COLUMN_INTERPRETER.getClass(), 3, response.getFirstPart(0))));
    assertEquals("count should be 1", THROTTLE_AT_ROW - 1,
      response.getSecondPart().asReadOnlyByteBuffer().getLong());

    AggregateRequest request2 = AggregateRequest.newBuilder(request)
      .setScan(request.getScan().toBuilder().setStartRow(response.getNextChunkStartRow()).build())
      .build();

    // Reset throttle for second call
    reset();

    RpcCallback<AggregateResponse> callback2 = mock(RpcCallback.class);
    aggregate.getAvg(controller, request2, callback2);

    verify(callback2).run(responseCaptor.capture());

    AggregateResponse response2 = responseCaptor.getValue();
    assertEquals("sum should be 14", 14L, (long) LONG_COLUMN_INTERPRETER.getPromotedValueFromProto(
      getParsedGenericInstance(LONG_COLUMN_INTERPRETER.getClass(), 3, response2.getFirstPart(0))));
    assertEquals("count should be 4", NUM_ROWS - THROTTLE_AT_ROW + 1,
      response2.getSecondPart().asReadOnlyByteBuffer().getLong());
    assertFalse("Response should not indicate there are more rows",
      response2.hasNextChunkStartRow());
  }

  @Test
  public void testAvgThrottleWithNoResults() throws Exception {
    AggregateRequest request = AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
      .setInterpreterClassName(LongColumnInterpreter.class.getName())
      .setClientSupportsPartialResult(true).build();

    when(env.checkScanQuota(any(), anyLong(), anyLong()))
      .thenThrow(new RpcThrottlingException(ReadSizeExceeded, 1000, "Throttled for testing"));

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    // call gets no results, and response should contain the same start row as the request, because
    // no progress in the scan was made
    aggregate.getAvg(controller, request, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();

    assertTrue("Response should indicate there are more rows", response.hasNextChunkStartRow());
    assertEquals("response should contain the same start row as the request",
      request.getScan().getStartRow(), response.getNextChunkStartRow());
  }

  @Test
  public void testAvgWithNoResults() throws Exception {
    AggregateRequest request = AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
      .setInterpreterClassName(LongColumnInterpreter.class.getName())
      .setClientSupportsPartialResult(false).build();

    doAnswer(invocation -> false).when(scanner).next(any(List.class));

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    aggregate.getAvg(controller, request, callback);
    verify(callback).run(responseCaptor.capture());
    AggregateResponse response = responseCaptor.getValue();
    assertNull(response);
  }

  @Test
  public void testAvgDoesNotSupportPartialResults() throws Exception {
    AggregateRequest noPartialRequest =
      AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
        .setInterpreterClassName(LongColumnInterpreter.class.getName())
        .setClientSupportsPartialResult(false).build();

    OperationQuota nonThrottlingQuota = mock(OperationQuota.class);
    when(env.checkScanQuota(any(), anyLong(), anyLong())).thenReturn(nonThrottlingQuota);

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    // Call should complete without throttling
    aggregate.getAvg(controller, noPartialRequest, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();

    assertFalse("Response should not indicate there are more rows",
      response.hasNextChunkStartRow());
    assertEquals("sum should be 15", 15L, (long) LONG_COLUMN_INTERPRETER.getPromotedValueFromProto(
      getParsedGenericInstance(LONG_COLUMN_INTERPRETER.getClass(), 3, response.getFirstPart(0))));
    assertEquals("count should be 5", NUM_ROWS,
      response.getSecondPart().asReadOnlyByteBuffer().getLong());
  }

  @Test
  public void testStdWithThrottling() throws Exception {
    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    // First call should get throttled
    aggregate.getStd(controller, request, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();
    assertTrue("Response should indicate there are more rows", response.hasNextChunkStartRow());
    assertEquals("Wait interval should be set", 1000, response.getWaitIntervalMs());
    assertEquals("sum should be 1", 1L, (long) LONG_COLUMN_INTERPRETER.getPromotedValueFromProto(
      getParsedGenericInstance(LONG_COLUMN_INTERPRETER.getClass(), 3, response.getFirstPart(0))));
    assertEquals("sumSq should be 1", 1L, (long) LONG_COLUMN_INTERPRETER.getPromotedValueFromProto(
      getParsedGenericInstance(LONG_COLUMN_INTERPRETER.getClass(), 3, response.getFirstPart(1))));
    assertEquals("count should be 1", THROTTLE_AT_ROW - 1,
      response.getSecondPart().asReadOnlyByteBuffer().getLong());

    AggregateRequest request2 = AggregateRequest.newBuilder(request)
      .setScan(request.getScan().toBuilder().setStartRow(response.getNextChunkStartRow()).build())
      .build();

    // Reset throttle for second call
    reset();

    RpcCallback<AggregateResponse> callback2 = mock(RpcCallback.class);
    aggregate.getStd(controller, request2, callback2);

    verify(callback2).run(responseCaptor.capture());

    AggregateResponse response2 = responseCaptor.getValue();
    assertEquals("sum should be 14", 14L, (long) LONG_COLUMN_INTERPRETER.getPromotedValueFromProto(
      getParsedGenericInstance(LONG_COLUMN_INTERPRETER.getClass(), 3, response2.getFirstPart(0))));
    assertEquals("sumSq should be 54", 54L,
      (long) LONG_COLUMN_INTERPRETER.getPromotedValueFromProto(getParsedGenericInstance(
        LONG_COLUMN_INTERPRETER.getClass(), 3, response2.getFirstPart(1))));
    assertEquals("count should be 4", NUM_ROWS - THROTTLE_AT_ROW + 1,
      response2.getSecondPart().asReadOnlyByteBuffer().getLong());
    assertFalse("Response should not indicate there are more rows",
      response2.hasNextChunkStartRow());
  }

  @Test
  public void testStdThrottleWithNoResults() throws Exception {
    AggregateRequest request = AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
      .setInterpreterClassName(LongColumnInterpreter.class.getName())
      .setClientSupportsPartialResult(true).build();

    when(env.checkScanQuota(any(), anyLong(), anyLong()))
      .thenThrow(new RpcThrottlingException(ReadSizeExceeded, 1000, "Throttled for testing"));

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    // call gets no results, and response should contain the same start row as the request, because
    // no progress in the scan was made
    aggregate.getStd(controller, request, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();

    assertTrue("Response should indicate there are more rows", response.hasNextChunkStartRow());
    assertEquals("response should contain the same start row as the request",
      request.getScan().getStartRow(), response.getNextChunkStartRow());
  }

  @Test
  public void testStdWithNoResults() throws Exception {
    AggregateRequest request = AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
      .setInterpreterClassName(LongColumnInterpreter.class.getName())
      .setClientSupportsPartialResult(false).build();

    doAnswer(invocation -> false).when(scanner).next(any(List.class));

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    aggregate.getStd(controller, request, callback);
    verify(callback).run(responseCaptor.capture());
    AggregateResponse response = responseCaptor.getValue();
    assertNull(response);
  }

  @Test
  public void testStdDoesNotSupportPartialResults() throws Exception {
    AggregateRequest noPartialRequest =
      AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
        .setInterpreterClassName(LongColumnInterpreter.class.getName())
        .setClientSupportsPartialResult(false).build();

    OperationQuota nonThrottlingQuota = mock(OperationQuota.class);
    when(env.checkScanQuota(any(), anyLong(), anyLong())).thenReturn(nonThrottlingQuota);

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    // Call should complete without throttling
    aggregate.getStd(controller, noPartialRequest, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();

    assertFalse("Response should not indicate there are more rows",
      response.hasNextChunkStartRow());
    assertEquals("sum should be 15", 15L, (long) LONG_COLUMN_INTERPRETER.getPromotedValueFromProto(
      getParsedGenericInstance(LONG_COLUMN_INTERPRETER.getClass(), 3, response.getFirstPart(0))));
    assertEquals("sumSq should be 55", 55L,
      (long) LONG_COLUMN_INTERPRETER.getPromotedValueFromProto(
        getParsedGenericInstance(LONG_COLUMN_INTERPRETER.getClass(), 3, response.getFirstPart(1))));
    assertEquals("count should be 5", NUM_ROWS,
      response.getSecondPart().asReadOnlyByteBuffer().getLong());
  }

  @Test
  public void testMedianWithThrottling() throws Exception {
    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    // First call should get throttled
    aggregate.getMedian(controller, request, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();
    assertTrue("Response should indicate there are more rows", response.hasNextChunkStartRow());
    assertEquals("Wait interval should be set", 1000, response.getWaitIntervalMs());
    assertEquals("sum should be 1", 1L, (long) LONG_COLUMN_INTERPRETER.getPromotedValueFromProto(
      getParsedGenericInstance(LONG_COLUMN_INTERPRETER.getClass(), 3, response.getFirstPart(0))));

    AggregateRequest request2 = AggregateRequest.newBuilder(request)
      .setScan(request.getScan().toBuilder().setStartRow(response.getNextChunkStartRow()).build())
      .build();

    // Reset throttle for second call
    reset();

    RpcCallback<AggregateResponse> callback2 = mock(RpcCallback.class);
    aggregate.getMedian(controller, request2, callback2);

    verify(callback2).run(responseCaptor.capture());

    AggregateResponse response2 = responseCaptor.getValue();
    assertEquals("sum should be 14", 14L, (long) LONG_COLUMN_INTERPRETER.getPromotedValueFromProto(
      getParsedGenericInstance(LONG_COLUMN_INTERPRETER.getClass(), 3, response2.getFirstPart(0))));
    assertFalse("Response should indicate there are more rows", response2.hasNextChunkStartRow());
  }

  @Test
  public void testMedianThrottleWithNoResults() throws Exception {
    AggregateRequest request = AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
      .setInterpreterClassName(LongColumnInterpreter.class.getName())
      .setClientSupportsPartialResult(true).build();

    when(env.checkScanQuota(any(), anyLong(), anyLong()))
      .thenThrow(new RpcThrottlingException(ReadSizeExceeded, 1000, "Throttled for testing"));

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    // call gets no results, and response should contain the same start row as the request, because
    // no progress in the scan was made
    aggregate.getMedian(controller, request, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();

    assertTrue("Response should indicate there are more rows", response.hasNextChunkStartRow());
    assertEquals("response should contain the same start row as the request",
      request.getScan().getStartRow(), response.getNextChunkStartRow());
  }

  @Test
  public void testMedianWithNoResults() throws Exception {
    AggregateRequest request = AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
      .setInterpreterClassName(LongColumnInterpreter.class.getName())
      .setClientSupportsPartialResult(false).build();

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    doAnswer(invocation -> false).when(scanner).next(any(List.class));

    aggregate.getMedian(controller, request, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();

    assertNull(response);
  }

  @Test
  public void testMedianDoesNotSupportPartialResults() throws Exception {
    AggregateRequest noPartialRequest =
      AggregateRequest.newBuilder().setScan(ProtobufUtil.toScan(scan))
        .setInterpreterClassName(LongColumnInterpreter.class.getName())
        .setClientSupportsPartialResult(false).build();

    OperationQuota nonThrottlingQuota = mock(OperationQuota.class);
    when(env.checkScanQuota(any(), anyLong(), anyLong())).thenReturn(nonThrottlingQuota);

    ArgumentCaptor<AggregateResponse> responseCaptor =
      ArgumentCaptor.forClass(AggregateResponse.class);
    RpcCallback<AggregateResponse> callback = mock(RpcCallback.class);

    // Call should complete without throttling
    aggregate.getMedian(controller, noPartialRequest, callback);

    verify(callback).run(responseCaptor.capture());

    AggregateResponse response = responseCaptor.getValue();

    assertEquals("sum should be 15", 15L, (long) LONG_COLUMN_INTERPRETER.getPromotedValueFromProto(
      getParsedGenericInstance(LONG_COLUMN_INTERPRETER.getClass(), 3, response.getFirstPart(0))));
    assertFalse("Response should indicate there are more rows", response.hasNextChunkStartRow());
  }

}
