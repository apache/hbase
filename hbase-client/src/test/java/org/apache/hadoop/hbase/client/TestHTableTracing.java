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
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MatcherPredicate;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.Matcher;
import org.hamcrest.core.IsAnything;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiResponse;
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
public class TestHTableTracing extends TestTracingBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHTableTracing.class);

  private ClientProtos.ClientService.BlockingInterface stub;
  private ConnectionImplementation conn;
  private Table table;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    stub = mock(ClientService.BlockingInterface.class);

    AtomicInteger scanNextCalled = new AtomicInteger(0);

    doAnswer(new Answer<ScanResponse>() {
      @Override
      public ScanResponse answer(InvocationOnMock invocation) throws Throwable {
        ScanRequest req = invocation.getArgument(1);
        if (!req.hasScannerId()) {
          return ScanResponse.newBuilder().setScannerId(1).setTtl(800).setMoreResultsInRegion(true)
            .setMoreResults(true).build();
        } else {
          if (req.hasCloseScanner() && req.getCloseScanner()) {
            return ScanResponse.getDefaultInstance();
          } else {
            Cell cell = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
              .setType(Cell.Type.Put).setRow(Bytes.toBytes(scanNextCalled.incrementAndGet()))
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
            return builder.build();
          }
        }
      }
    }).when(stub).scan(any(HBaseRpcController.class), any(ScanRequest.class));

    doAnswer(new Answer<MultiResponse>() {
      @Override
      public MultiResponse answer(InvocationOnMock invocation) throws Throwable {
        MultiResponse resp =
          MultiResponse.newBuilder()
            .addRegionActionResult(RegionActionResult.newBuilder().addResultOrException(
              ResultOrException.newBuilder().setResult(ProtobufUtil.toResult(new Result()))))
            .build();
        return resp;
      }
    }).when(stub).multi(any(HBaseRpcController.class), any(ClientProtos.MultiRequest.class));

    doAnswer(new Answer<MutateResponse>() {
      @Override
      public MutateResponse answer(InvocationOnMock invocation) throws Throwable {
        MutationProto req = ((MutateRequest) invocation.getArgument(1)).getMutation();
        MutateResponse resp;
        switch (req.getMutateType()) {
          case INCREMENT:
            ColumnValue value = req.getColumnValue(0);
            QualifierValue qvalue = value.getQualifierValue(0);
            Cell cell =
              CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setType(Cell.Type.Put)
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
        return resp;
      }
    }).when(stub).mutate(any(HBaseRpcController.class), any(MutateRequest.class));

    doAnswer(new Answer<GetResponse>() {
      @Override
      public GetResponse answer(InvocationOnMock invocation) throws Throwable {
        ClientProtos.Get req = ((GetRequest) invocation.getArgument(1)).getGet();
        ColumnValue value = ColumnValue.getDefaultInstance();
        QualifierValue qvalue = QualifierValue.getDefaultInstance();
        Cell cell = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY).setType(Cell.Type.Put)
          .setRow(req.getRow().toByteArray()).setFamily(value.getFamily().toByteArray())
          .setQualifier(qvalue.getQualifier().toByteArray())
          .setValue(qvalue.getValue().toByteArray()).build();
        return GetResponse.newBuilder()
          .setResult(ProtobufUtil.toResult(Result.create(Arrays.asList(cell), true))).build();
      }
    }).when(stub).get(any(HBaseRpcController.class), any(GetRequest.class));

    conn = spy(new ConnectionImplementation(conf, null, UserProvider.instantiate(conf).getCurrent(),
      Collections.emptyMap()) {
      @Override
      public RegionLocator getRegionLocator(TableName tableName) throws IOException {
        RegionLocator locator = mock(HRegionLocator.class);
        Answer<HRegionLocation> answer = new Answer<HRegionLocation>() {

          @Override
          public HRegionLocation answer(InvocationOnMock invocation) throws Throwable {
            TableName tableName = TableName.META_TABLE_NAME;
            RegionInfo info = RegionInfoBuilder.newBuilder(tableName).build();
            ServerName serverName = MASTER_HOST;
            HRegionLocation loc = new HRegionLocation(info, serverName);
            return loc;
          }
        };
        doAnswer(answer).when(locator).getRegionLocation(any(byte[].class), anyInt(), anyBoolean());
        doAnswer(answer).when(locator).getRegionLocation(any(byte[].class));
        doAnswer(answer).when(locator).getRegionLocation(any(byte[].class), anyInt());
        doAnswer(answer).when(locator).getRegionLocation(any(byte[].class), anyBoolean());
        return locator;
      }

      @Override
      public ClientService.BlockingInterface getClient(ServerName serverName) throws IOException {
        return stub;
      }
    });
    // this setup of AsyncProcess is for MultiResponse
    AsyncProcess asyncProcess = mock(AsyncProcess.class);
    AsyncRequestFuture asyncRequestFuture = mock(AsyncRequestFuture.class);
    doNothing().when(asyncRequestFuture).waitUntilDone();
    doReturn(asyncRequestFuture).when(asyncProcess).submit(any());
    doReturn(asyncProcess).when(conn).getAsyncProcess();
    // setup the table instance
    table = conn.getTable(TableName.META_TABLE_NAME, ForkJoinPool.commonPool());
  }

  @After
  public void tearDown() throws IOException {
    Closeables.close(conn, true);
  }

  private void assertTrace(String tableOperation) {
    assertTrace(tableOperation, new IsAnything<>());
  }

  private void assertTrace(String tableOperation, Matcher<SpanData> matcher) {
    // n.b. this method implementation must match the one of the same name found in
    // TestAsyncTableTracing
    final TableName tableName = table.getName();
    final Matcher<SpanData> spanLocator =
      allOf(hasName(containsString(tableOperation)), hasEnded());
    final String expectedName = tableOperation + " " + tableName.getNameWithNamespaceInclAsString();

    Waiter.waitFor(conf, 1000, new MatcherPredicate<>("waiting for span to emit",
      () -> TRACE_RULE.getSpans(), hasItem(spanLocator)));
    List<SpanData> candidateSpans =
      TRACE_RULE.getSpans().stream().filter(spanLocator::matches).collect(Collectors.toList());
    assertThat(candidateSpans, hasSize(1));
    SpanData data = candidateSpans.iterator().next();
    assertThat(data,
      allOf(hasName(expectedName), hasKind(SpanKind.CLIENT), hasStatusWithCode(StatusCode.OK),
        buildConnectionAttributesMatcher(conn), buildTableAttributesMatcher(tableName), matcher));
  }

  @Test
  public void testPut() throws IOException {
    table.put(new Put(Bytes.toBytes(0)).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"),
      Bytes.toBytes("v")));
    assertTrace("PUT");
  }

  @Test
  public void testExists() throws IOException {
    table.exists(new Get(Bytes.toBytes(0)));
    assertTrace("GET");
  }

  @Test
  public void testGet() throws IOException {
    table.get(new Get(Bytes.toBytes(0)));
    assertTrace("GET");
  }

  @Test
  public void testDelete() throws IOException {
    table.delete(new Delete(Bytes.toBytes(0)));
    assertTrace("DELETE");
  }

  @Test
  public void testAppend() throws IOException {
    table.append(new Append(Bytes.toBytes(0)).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"),
      Bytes.toBytes("v")));
    assertTrace("APPEND");
  }

  @Test
  public void testIncrement() throws IOException {
    table.increment(
      new Increment(Bytes.toBytes(0)).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1));
    assertTrace("INCREMENT");
  }

  @Test
  public void testIncrementColumnValue1() throws IOException {
    table.incrementColumnValue(Bytes.toBytes(0), Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1);
    assertTrace("INCREMENT");
  }

  @Test
  public void testIncrementColumnValue2() throws IOException {
    table.incrementColumnValue(Bytes.toBytes(0), Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1,
      Durability.SYNC_WAL);
    assertTrace("INCREMENT");
  }

  @Test
  public void testCheckAndMutate() throws IOException {
    table.checkAndMutate(CheckAndMutate.newBuilder(Bytes.toBytes(0))
      .ifEquals(Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("v"))
      .build(new Delete(Bytes.toBytes(0))));
    assertTrace("CHECK_AND_MUTATE",
      hasAttributes(containsEntryWithStringValuesOf("db.hbase.container_operations",
        "CHECK_AND_MUTATE", "DELETE")));
  }

  @Test
  public void testCheckAndMutateList() throws IOException {
    table.checkAndMutate(Arrays.asList(CheckAndMutate.newBuilder(Bytes.toBytes(0))
      .ifEquals(Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("v"))
      .build(new Delete(Bytes.toBytes(0)))));
    assertTrace("BATCH",
      hasAttributes(containsEntryWithStringValuesOf("db.hbase.container_operations",
        "CHECK_AND_MUTATE", "DELETE")));
  }

  @Test
  public void testCheckAndMutateAll() throws IOException {
    table.checkAndMutate(Arrays.asList(CheckAndMutate.newBuilder(Bytes.toBytes(0))
      .ifEquals(Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("v"))
      .build(new Delete(Bytes.toBytes(0)))));
    assertTrace("BATCH",
      hasAttributes(containsEntryWithStringValuesOf("db.hbase.container_operations",
        "CHECK_AND_MUTATE", "DELETE")));
  }

  @Test
  public void testMutateRow() throws Exception {
    byte[] row = Bytes.toBytes(0);
    table.mutateRow(RowMutations.of(Arrays.asList(new Delete(row))));
    assertTrace("BATCH",
      hasAttributes(containsEntryWithStringValuesOf("db.hbase.container_operations", "DELETE")));
  }

  @Test
  public void testExistsList() throws IOException {
    table.exists(Arrays.asList(new Get(Bytes.toBytes(0))));
    assertTrace("BATCH",
      hasAttributes(containsEntryWithStringValuesOf("db.hbase.container_operations", "GET")));
  }

  @Test
  public void testExistsAll() throws IOException {
    table.existsAll(Arrays.asList(new Get(Bytes.toBytes(0))));
    assertTrace("BATCH",
      hasAttributes(containsEntryWithStringValuesOf("db.hbase.container_operations", "GET")));
  }

  @Test
  public void testGetList() throws IOException {
    table.get(Arrays.asList(new Get(Bytes.toBytes(0))));
    assertTrace("BATCH",
      hasAttributes(containsEntryWithStringValuesOf("db.hbase.container_operations", "GET")));
  }

  @Test
  public void testPutList() throws IOException {
    table.put(Arrays.asList(new Put(Bytes.toBytes(0)).addColumn(Bytes.toBytes("cf"),
      Bytes.toBytes("cq"), Bytes.toBytes("v"))));
    assertTrace("BATCH",
      hasAttributes(containsEntryWithStringValuesOf("db.hbase.container_operations", "PUT")));
  }

  @Test
  public void testDeleteList() throws IOException {
    table.delete(Lists.newArrayList(new Delete(Bytes.toBytes(0))));
    assertTrace("BATCH",
      hasAttributes(containsEntryWithStringValuesOf("db.hbase.container_operations", "DELETE")));
  }

  @Test
  public void testBatchList() throws IOException, InterruptedException {
    table.batch(Arrays.asList(new Delete(Bytes.toBytes(0))), null);
    assertTrace("BATCH",
      hasAttributes(containsEntryWithStringValuesOf("db.hbase.container_operations", "DELETE")));
  }

  @Test
  public void testTableClose() throws IOException {
    table.close();
    assertTrace(HTable.class.getSimpleName(), "close", null, TableName.META_TABLE_NAME);
  }
}
