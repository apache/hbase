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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import io.opentelemetry.api.trace.Span.Kind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.junit4.OpenTelemetryRule;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.ColumnValue;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.ColumnValue.QualifierValue;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.RegionActionResult;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ResultOrException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;

@Category({ ClientTests.class, MediumTests.class })
public class TestAsyncTableTracing {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableTracing.class);

  private static Configuration CONF = HBaseConfiguration.create();

  private ClientService.Interface stub;

  private AsyncConnection conn;

  private AsyncTable<?> table;

  @Rule
  public OpenTelemetryRule traceRule = OpenTelemetryRule.create();

  @Before
  public void setUp() throws IOException {
    stub = mock(ClientService.Interface.class);
    AtomicInteger scanNextCalled = new AtomicInteger(0);
    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        ScanRequest req = invocation.getArgument(1);
        RpcCallback<ScanResponse> done = invocation.getArgument(2);
        if (!req.hasScannerId()) {
          done.run(ScanResponse.newBuilder().setScannerId(1).setTtl(800)
            .setMoreResultsInRegion(true).setMoreResults(true).build());
        } else {
          if (req.hasCloseScanner() && req.getCloseScanner()) {
            done.run(ScanResponse.getDefaultInstance());
          } else {
            Cell cell = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setType(Type.Put)
              .setRow(Bytes.toBytes(scanNextCalled.incrementAndGet()))
              .setFamily(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("cq"))
              .setValue(Bytes.toBytes("v")).build();
            Result result = Result.create(Arrays.asList(cell));
            ScanResponse.Builder builder = ScanResponse.newBuilder().setScannerId(1).setTtl(800)
              .addResults(ProtobufUtil.toResult(result));
            if (req.getLimitOfRows() == 1) {
              builder.setMoreResultsInRegion(false).setMoreResults(false);
            } else {
              builder.setMoreResultsInRegion(true).setMoreResults(true);
            }
            ForkJoinPool.commonPool().execute(() -> done.run(builder.build()));
          }
        }
        return null;
      }
    }).when(stub).scan(any(HBaseRpcController.class), any(ScanRequest.class), any());
    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        ClientProtos.MultiResponse resp =
          ClientProtos.MultiResponse.newBuilder()
            .addRegionActionResult(RegionActionResult.newBuilder().addResultOrException(
              ResultOrException.newBuilder().setResult(ProtobufUtil.toResult(new Result()))))
            .build();
        RpcCallback<ClientProtos.MultiResponse> done = invocation.getArgument(2);
        ForkJoinPool.commonPool().execute(() -> done.run(resp));
        return null;
      }
    }).when(stub).multi(any(HBaseRpcController.class), any(ClientProtos.MultiRequest.class), any());
    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        MutationProto req = ((MutateRequest) invocation.getArgument(1)).getMutation();
        MutateResponse resp;
        switch (req.getMutateType()) {
          case INCREMENT:
            ColumnValue value = req.getColumnValue(0);
            QualifierValue qvalue = value.getQualifierValue(0);
            Cell cell = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setType(Type.Put)
              .setRow(req.getRow().toByteArray()).setFamily(value.getFamily().toByteArray())
              .setQualifier(qvalue.getQualifier().toByteArray())
              .setValue(qvalue.getValue().toByteArray()).build();
            resp = MutateResponse.newBuilder()
              .setResult(ProtobufUtil.toResult(Result.create(Arrays.asList(cell)))).build();
            break;
          default:
            resp = MutateResponse.getDefaultInstance();
            break;
        }
        RpcCallback<MutateResponse> done = invocation.getArgument(2);
        ForkJoinPool.commonPool().execute(() -> done.run(resp));
        return null;
      }
    }).when(stub).mutate(any(HBaseRpcController.class), any(MutateRequest.class), any());
    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        RpcCallback<GetResponse> done = invocation.getArgument(2);
        ForkJoinPool.commonPool().execute(() -> done.run(GetResponse.getDefaultInstance()));
        return null;
      }
    }).when(stub).get(any(HBaseRpcController.class), any(GetRequest.class), any());
    conn = new AsyncConnectionImpl(CONF, new DoNothingConnectionRegistry(CONF), "test", null,
      UserProvider.instantiate(CONF).getCurrent()) {

      @Override
      AsyncRegionLocator getLocator() {
        AsyncRegionLocator locator = mock(AsyncRegionLocator.class);
        Answer<CompletableFuture<HRegionLocation>> answer =
          new Answer<CompletableFuture<HRegionLocation>>() {

            @Override
            public CompletableFuture<HRegionLocation> answer(InvocationOnMock invocation)
              throws Throwable {
              TableName tableName = invocation.getArgument(0);
              RegionInfo info = RegionInfoBuilder.newBuilder(tableName).build();
              ServerName serverName = ServerName.valueOf("rs", 16010, 12345);
              HRegionLocation loc = new HRegionLocation(info, serverName);
              return CompletableFuture.completedFuture(loc);
            }
          };
        doAnswer(answer).when(locator).getRegionLocation(any(TableName.class), any(byte[].class),
          any(RegionLocateType.class), anyLong());
        doAnswer(answer).when(locator).getRegionLocation(any(TableName.class), any(byte[].class),
          anyInt(), any(RegionLocateType.class), anyLong());
        return locator;
      }

      @Override
      ClientService.Interface getRegionServerStub(ServerName serverName) throws IOException {
        return stub;
      }
    };
    table = conn.getTable(TableName.valueOf("table"), ForkJoinPool.commonPool());
  }

  @After
  public void tearDown() throws IOException {
    Closeables.close(conn, true);
  }

  private void assertTrace(String methodName) {
    Waiter.waitFor(CONF, 1000,
      () -> traceRule.getSpans().stream()
        .anyMatch(span -> span.getName().equals("AsyncTable." + methodName) &&
          span.getKind() == Kind.INTERNAL && span.hasEnded()));
    SpanData data = traceRule.getSpans().stream()
      .filter(s -> s.getName().equals("AsyncTable." + methodName)).findFirst().get();
    assertEquals(StatusCode.OK, data.getStatus().getStatusCode());
    TableName tableName = table.getName();
    assertEquals(tableName.getNamespaceAsString(),
      data.getAttributes().get(TraceUtil.NAMESPACE_KEY));
    assertEquals(tableName.getNameAsString(), data.getAttributes().get(TraceUtil.TABLE_KEY));
  }

  @Test
  public void testExists() {
    table.exists(new Get(Bytes.toBytes(0))).join();
    assertTrace("get");
  }

  @Test
  public void testGet() {
    table.get(new Get(Bytes.toBytes(0))).join();
    assertTrace("get");
  }

  @Test
  public void testPut() {
    table.put(new Put(Bytes.toBytes(0)).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"),
      Bytes.toBytes("v"))).join();
    assertTrace("put");
  }

  @Test
  public void testDelete() {
    table.delete(new Delete(Bytes.toBytes(0))).join();
    assertTrace("delete");
  }

  @Test
  public void testAppend() {
    table.append(new Append(Bytes.toBytes(0)).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"),
      Bytes.toBytes("v"))).join();
    assertTrace("append");
  }

  @Test
  public void testIncrement() {
    table
      .increment(
        new Increment(Bytes.toBytes(0)).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1))
      .join();
    assertTrace("increment");
  }

  @Test
  public void testIncrementColumnValue1() {
    table.incrementColumnValue(Bytes.toBytes(0), Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1)
      .join();
    assertTrace("increment");
  }

  @Test
  public void testIncrementColumnValue2() {
    table.incrementColumnValue(Bytes.toBytes(0), Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1,
      Durability.ASYNC_WAL).join();
    assertTrace("increment");
  }

  @Test
  public void testCheckAndMutate() {
    table.checkAndMutate(CheckAndMutate.newBuilder(Bytes.toBytes(0))
      .ifEquals(Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("v"))
      .build(new Delete(Bytes.toBytes(0)))).join();
    assertTrace("checkAndMutate");
  }

  @Test
  public void testCheckAndMutateList() {
    CompletableFuture
      .allOf(table.checkAndMutate(Arrays.asList(CheckAndMutate.newBuilder(Bytes.toBytes(0))
        .ifEquals(Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("v"))
        .build(new Delete(Bytes.toBytes(0))))).toArray(new CompletableFuture[0]))
      .join();
    assertTrace("checkAndMutateList");
  }

  @Test
  public void testCheckAndMutateAll() {
    table.checkAndMutateAll(Arrays.asList(CheckAndMutate.newBuilder(Bytes.toBytes(0))
      .ifEquals(Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("v"))
      .build(new Delete(Bytes.toBytes(0))))).join();
    assertTrace("checkAndMutateList");
  }

  @Test
  public void testMutateRow() throws IOException {
    table.mutateRow(new RowMutations(Bytes.toBytes(0)).add(new Delete(Bytes.toBytes(0))));
    assertTrace("mutateRow");
  }

  @Test
  public void testScanAll() throws IOException {
    table.scanAll(new Scan().setCaching(1).setMaxResultSize(1).setLimit(1)).join();
    assertTrace("scanAll");
  }

  @Test
  public void testExistsList() {
    CompletableFuture
      .allOf(
        table.exists(Arrays.asList(new Get(Bytes.toBytes(0)))).toArray(new CompletableFuture[0]))
      .join();
    assertTrace("getList");
  }

  @Test
  public void testExistsAll() {
    table.existsAll(Arrays.asList(new Get(Bytes.toBytes(0)))).join();
    assertTrace("getList");
  }

  @Test
  public void testGetList() {
    CompletableFuture
      .allOf(table.get(Arrays.asList(new Get(Bytes.toBytes(0)))).toArray(new CompletableFuture[0]))
      .join();
    assertTrace("getList");
  }

  @Test
  public void testGetAll() {
    table.getAll(Arrays.asList(new Get(Bytes.toBytes(0)))).join();
    assertTrace("getList");
  }

  @Test
  public void testPutList() {
    CompletableFuture
      .allOf(table.put(Arrays.asList(new Put(Bytes.toBytes(0)).addColumn(Bytes.toBytes("cf"),
        Bytes.toBytes("cq"), Bytes.toBytes("v")))).toArray(new CompletableFuture[0]))
      .join();
    assertTrace("putList");
  }

  @Test
  public void testPutAll() {
    table.putAll(Arrays.asList(new Put(Bytes.toBytes(0)).addColumn(Bytes.toBytes("cf"),
      Bytes.toBytes("cq"), Bytes.toBytes("v")))).join();
    assertTrace("putList");
  }

  @Test
  public void testDeleteList() {
    CompletableFuture
      .allOf(
        table.delete(Arrays.asList(new Delete(Bytes.toBytes(0)))).toArray(new CompletableFuture[0]))
      .join();
    assertTrace("deleteList");
  }

  @Test
  public void testDeleteAll() {
    table.deleteAll(Arrays.asList(new Delete(Bytes.toBytes(0)))).join();
    assertTrace("deleteList");
  }

  @Test
  public void testBatch() {
    CompletableFuture
      .allOf(
        table.batch(Arrays.asList(new Delete(Bytes.toBytes(0)))).toArray(new CompletableFuture[0]))
      .join();
    assertTrace("batch");
  }

  @Test
  public void testBatchAll() {
    table.batchAll(Arrays.asList(new Delete(Bytes.toBytes(0)))).join();
    assertTrace("batch");
  }

  @Test
  public void testConnClose() throws IOException {
    conn.close();
    Waiter.waitFor(CONF, 1000,
      () -> traceRule.getSpans().stream()
        .anyMatch(span -> span.getName().equals("AsyncConnection.close") &&
          span.getKind() == Kind.INTERNAL && span.hasEnded()));
    SpanData data = traceRule.getSpans().stream()
      .filter(s -> s.getName().equals("AsyncConnection.close")).findFirst().get();
    assertEquals(StatusCode.OK, data.getStatus().getStatusCode());
  }
}
