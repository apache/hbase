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

import static org.apache.hadoop.hbase.client.trace.hamcrest.AttributesMatchers.containsEntryWithStringValuesOf;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasAttributes;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasEnded;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasKind;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasName;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasStatusWithCode;
import static org.apache.hadoop.hbase.client.trace.hamcrest.TraceTestUtil.buildConnectionAttributesMatcher;
import static org.apache.hadoop.hbase.client.trace.hamcrest.TraceTestUtil.buildTableAttributesMatcher;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.junit4.OpenTelemetryRule;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MatcherPredicate;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.Matcher;
import org.hamcrest.core.IsAnything;
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

  private AsyncConnectionImpl conn;

  private AsyncTable<ScanResultConsumer> table;

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
        ClientProtos.MultiRequest req = invocation.getArgument(1);
        ClientProtos.MultiResponse.Builder builder = ClientProtos.MultiResponse.newBuilder();
        for (ClientProtos.RegionAction regionAction : req.getRegionActionList()) {
          RegionActionResult.Builder raBuilder = RegionActionResult.newBuilder();
          for (ClientProtos.Action ignored : regionAction.getActionList()) {
            raBuilder.addResultOrException(
              ResultOrException.newBuilder().setResult(ProtobufUtil.toResult(new Result())));
          }
          builder.addRegionActionResult(raBuilder);
        }
        ClientProtos.MultiResponse resp = builder.build();
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
    final User user = UserProvider.instantiate(CONF).getCurrent();
    conn = new AsyncConnectionImpl(CONF, new DoNothingConnectionRegistry(CONF), "test", null,
      user) {

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

  private void assertTrace(String tableOperation) {
    assertTrace(tableOperation, new IsAnything<>());
  }

  private void assertTrace(String tableOperation, Matcher<SpanData> matcher) {
    final TableName tableName = table.getName();
    final Matcher<SpanData> spanLocator = allOf(
      hasName(containsString(tableOperation)), hasEnded());
    final String expectedName = tableOperation + " " + tableName.getNameWithNamespaceInclAsString();

    Waiter.waitFor(CONF, 1000, new MatcherPredicate<>(
      "waiting for span to emit",
      () -> traceRule.getSpans(), hasItem(spanLocator)));
    List<SpanData> candidateSpans = traceRule.getSpans()
      .stream()
      .filter(spanLocator::matches)
      .collect(Collectors.toList());
    assertThat(candidateSpans, hasSize(1));
    SpanData data = candidateSpans.iterator().next();
    assertThat(data, allOf(
      hasName(expectedName),
      hasKind(SpanKind.CLIENT),
      hasStatusWithCode(StatusCode.OK),
      buildConnectionAttributesMatcher(conn),
      buildTableAttributesMatcher(tableName),
      matcher));
  }

  @Test
  public void testExists() {
    table.exists(new Get(Bytes.toBytes(0))).join();
    assertTrace("GET");
  }

  @Test
  public void testGet() {
    table.get(new Get(Bytes.toBytes(0))).join();
    assertTrace("GET");
  }

  @Test
  public void testPut() {
    table.put(new Put(Bytes.toBytes(0)).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"),
      Bytes.toBytes("v"))).join();
    assertTrace("PUT");
  }

  @Test
  public void testDelete() {
    table.delete(new Delete(Bytes.toBytes(0))).join();
    assertTrace("DELETE");
  }

  @Test
  public void testAppend() {
    table.append(new Append(Bytes.toBytes(0)).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"),
      Bytes.toBytes("v"))).join();
    assertTrace("APPEND");
  }

  @Test
  public void testIncrement() {
    table
      .increment(
        new Increment(Bytes.toBytes(0)).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1))
      .join();
    assertTrace("INCREMENT");
  }

  @Test
  public void testIncrementColumnValue1() {
    table.incrementColumnValue(Bytes.toBytes(0), Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1)
      .join();
    assertTrace("INCREMENT");
  }

  @Test
  public void testIncrementColumnValue2() {
    table.incrementColumnValue(Bytes.toBytes(0), Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1,
      Durability.ASYNC_WAL).join();
    assertTrace("INCREMENT");
  }

  @Test
  public void testCheckAndMutate() {
    table.checkAndMutate(CheckAndMutate.newBuilder(Bytes.toBytes(0))
      .ifEquals(Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("v"))
      .build(new Delete(Bytes.toBytes(0)))).join();
    assertTrace("CHECK_AND_MUTATE");
  }

  @Test
  public void testCheckAndMutateList() {
    CompletableFuture
      .allOf(table.checkAndMutate(Arrays.asList(CheckAndMutate.newBuilder(Bytes.toBytes(0))
        .ifEquals(Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("v"))
        .build(new Delete(Bytes.toBytes(0))))).toArray(new CompletableFuture[0]))
      .join();
    assertTrace("BATCH", hasAttributes(
      containsEntryWithStringValuesOf(
        "db.hbase.container_operations", "CHECK_AND_MUTATE", "DELETE")));
  }

  @Test
  public void testCheckAndMutateAll() {
    table.checkAndMutateAll(Arrays.asList(CheckAndMutate.newBuilder(Bytes.toBytes(0))
      .ifEquals(Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("v"))
      .build(new Delete(Bytes.toBytes(0))))).join();
    assertTrace("BATCH", hasAttributes(
      containsEntryWithStringValuesOf(
        "db.hbase.container_operations", "CHECK_AND_MUTATE", "DELETE")));
  }

  private void testCheckAndMutateBuilder(Row op) {
    AsyncTable.CheckAndMutateBuilder builder =
      table.checkAndMutate(Bytes.toBytes(0), Bytes.toBytes("cf"))
        .qualifier(Bytes.toBytes("cq"))
        .ifEquals(Bytes.toBytes("v"));
    if (op instanceof Put) {
      Put put = (Put) op;
      builder.thenPut(put).join();
    } else if (op instanceof Delete) {
      Delete delete = (Delete) op;
      builder.thenDelete(delete).join();
    } else if (op instanceof RowMutations) {
      RowMutations mutations = (RowMutations) op;
      builder.thenMutate(mutations).join();
    } else {
      fail("unsupported CheckAndPut operation " + op);
    }
    assertTrace("CHECK_AND_MUTATE");
  }

  @Test
  public void testCheckAndMutateBuilderThenPut() {
    Put put = new Put(Bytes.toBytes(0))
      .addColumn(Bytes.toBytes("f"), Bytes.toBytes("cq"), Bytes.toBytes("v"));
    testCheckAndMutateBuilder(put);
  }

  @Test
  public void testCheckAndMutateBuilderThenDelete() {
    testCheckAndMutateBuilder(new Delete(Bytes.toBytes(0)));
  }

  @Test
  public void testCheckAndMutateBuilderThenMutations() throws IOException {
    RowMutations mutations = new RowMutations(Bytes.toBytes(0))
      .add(new Put(Bytes.toBytes(0))
        .addColumn(Bytes.toBytes("f"), Bytes.toBytes("cq"), Bytes.toBytes("v")))
      .add(new Delete(Bytes.toBytes(0)));
    testCheckAndMutateBuilder(mutations);
  }

  private void testCheckAndMutateWithFilterBuilder(Row op) {
    // use of `PrefixFilter` is completely arbitrary here.
    AsyncTable.CheckAndMutateWithFilterBuilder builder =
      table.checkAndMutate(Bytes.toBytes(0), new PrefixFilter(Bytes.toBytes(0)));
    if (op instanceof Put) {
      Put put = (Put) op;
      builder.thenPut(put).join();
    } else if (op instanceof Delete) {
      Delete delete = (Delete) op;
      builder.thenDelete(delete).join();
    } else if (op instanceof RowMutations) {
      RowMutations mutations = (RowMutations) op;
      builder.thenMutate(mutations).join();
    } else {
      fail("unsupported CheckAndPut operation " + op);
    }
    assertTrace("CHECK_AND_MUTATE");
  }

  @Test
  public void testCheckAndMutateWithFilterBuilderThenPut() {
    Put put = new Put(Bytes.toBytes(0))
      .addColumn(Bytes.toBytes("f"), Bytes.toBytes("cq"), Bytes.toBytes("v"));
    testCheckAndMutateWithFilterBuilder(put);
  }

  @Test
  public void testCheckAndMutateWithFilterBuilderThenDelete() {
    testCheckAndMutateWithFilterBuilder(new Delete(Bytes.toBytes(0)));
  }

  @Test
  public void testCheckAndMutateWithFilterBuilderThenMutations() throws IOException {
    RowMutations mutations = new RowMutations(Bytes.toBytes(0))
      .add(new Put(Bytes.toBytes(0))
        .addColumn(Bytes.toBytes("f"), Bytes.toBytes("cq"), Bytes.toBytes("v")))
      .add(new Delete(Bytes.toBytes(0)));
    testCheckAndMutateWithFilterBuilder(mutations);
  }

  @Test
  public void testMutateRow() throws IOException {
    final RowMutations mutations = new RowMutations(Bytes.toBytes(0))
      .add(new Put(Bytes.toBytes(0))
        .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("v")))
      .add(new Delete(Bytes.toBytes(0)));
    table.mutateRow(mutations).join();
    assertTrace("BATCH", hasAttributes(
      containsEntryWithStringValuesOf("db.hbase.container_operations", "DELETE", "PUT")));
  }

  @Test
  public void testScanAll() {
    table.scanAll(new Scan().setCaching(1).setMaxResultSize(1).setLimit(1)).join();
    assertTrace("SCAN");
  }

  @Test
  public void testScan() throws Throwable {
    final CountDownLatch doneSignal = new CountDownLatch(1);
    final AtomicInteger count = new AtomicInteger();
    final AtomicReference<Throwable> throwable = new AtomicReference<>();
    final Scan scan = new Scan().setCaching(1).setMaxResultSize(1).setLimit(1);
    table.scan(scan, new ScanResultConsumer() {
      @Override public boolean onNext(Result result) {
        if (result.getRow() != null) {
          count.incrementAndGet();
        }
        return true;
      }

      @Override public void onError(Throwable error) {
        throwable.set(error);
        doneSignal.countDown();
      }

      @Override public void onComplete() {
        doneSignal.countDown();
      }
    });
    doneSignal.await();
    if (throwable.get() != null) {
      throw throwable.get();
    }
    assertThat("user code did not run. check test setup.", count.get(), greaterThan(0));
    assertTrace("SCAN");
  }

  @Test
  public void testGetScanner() {
    final Scan scan = new Scan().setCaching(1).setMaxResultSize(1).setLimit(1);
    try (ResultScanner scanner = table.getScanner(scan)) {
      int count = 0;
      for (Result result : scanner) {
        if (result.getRow() != null) {
          count++;
        }
      }
      // do something with it.
      assertThat(count, greaterThanOrEqualTo(0));
    }
    assertTrace("SCAN");
  }

  @Test
  public void testExistsList() {
    CompletableFuture
      .allOf(
        table.exists(Arrays.asList(new Get(Bytes.toBytes(0)))).toArray(new CompletableFuture[0]))
      .join();
    assertTrace("BATCH", hasAttributes(
      containsEntryWithStringValuesOf("db.hbase.container_operations", "GET")));
  }

  @Test
  public void testExistsAll() {
    table.existsAll(Arrays.asList(new Get(Bytes.toBytes(0)))).join();
    assertTrace("BATCH", hasAttributes(
      containsEntryWithStringValuesOf("db.hbase.container_operations", "GET")));
  }

  @Test
  public void testGetList() {
    CompletableFuture
      .allOf(table.get(Arrays.asList(new Get(Bytes.toBytes(0)))).toArray(new CompletableFuture[0]))
      .join();
    assertTrace("BATCH", hasAttributes(
      containsEntryWithStringValuesOf("db.hbase.container_operations", "GET")));
  }

  @Test
  public void testGetAll() {
    table.getAll(Arrays.asList(new Get(Bytes.toBytes(0)))).join();
    assertTrace("BATCH", hasAttributes(
      containsEntryWithStringValuesOf("db.hbase.container_operations", "GET")));
  }

  @Test
  public void testPutList() {
    CompletableFuture
      .allOf(table.put(Arrays.asList(new Put(Bytes.toBytes(0)).addColumn(Bytes.toBytes("cf"),
        Bytes.toBytes("cq"), Bytes.toBytes("v")))).toArray(new CompletableFuture[0]))
      .join();
    assertTrace("BATCH", hasAttributes(
      containsEntryWithStringValuesOf("db.hbase.container_operations", "PUT")));
  }

  @Test
  public void testPutAll() {
    table.putAll(Arrays.asList(new Put(Bytes.toBytes(0)).addColumn(Bytes.toBytes("cf"),
      Bytes.toBytes("cq"), Bytes.toBytes("v")))).join();
    assertTrace("BATCH", hasAttributes(
      containsEntryWithStringValuesOf("db.hbase.container_operations", "PUT")));
  }

  @Test
  public void testDeleteList() {
    CompletableFuture
      .allOf(
        table.delete(Arrays.asList(new Delete(Bytes.toBytes(0)))).toArray(new CompletableFuture[0]))
      .join();
    assertTrace("BATCH", hasAttributes(
      containsEntryWithStringValuesOf("db.hbase.container_operations", "DELETE")));
  }

  @Test
  public void testDeleteAll() {
    table.deleteAll(Arrays.asList(new Delete(Bytes.toBytes(0)))).join();
    assertTrace("BATCH", hasAttributes(
      containsEntryWithStringValuesOf("db.hbase.container_operations", "DELETE")));
  }

  @Test
  public void testBatch() {
    CompletableFuture
      .allOf(
        table.batch(Arrays.asList(new Delete(Bytes.toBytes(0)))).toArray(new CompletableFuture[0]))
      .join();
    assertTrace("BATCH", hasAttributes(
      containsEntryWithStringValuesOf("db.hbase.container_operations", "DELETE")));
  }

  @Test
  public void testBatchAll() {
    table.batchAll(Arrays.asList(new Delete(Bytes.toBytes(0)))).join();
    assertTrace("BATCH", hasAttributes(
      containsEntryWithStringValuesOf("db.hbase.container_operations", "DELETE")));
  }
}
