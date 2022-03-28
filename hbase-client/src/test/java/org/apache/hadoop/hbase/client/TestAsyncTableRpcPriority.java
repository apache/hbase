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

import static org.apache.hadoop.hbase.HConstants.HIGH_QOS;
import static org.apache.hadoop.hbase.HConstants.NORMAL_QOS;
import static org.apache.hadoop.hbase.HConstants.SYSTEMTABLE_QOS;
import static org.apache.hadoop.hbase.NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
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

/**
 * Confirm that we will set the priority in {@link HBaseRpcController} for several table operations.
 */
@Category({ ClientTests.class, MediumTests.class })
public class TestAsyncTableRpcPriority {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableRpcPriority.class);

  private static Configuration CONF = HBaseConfiguration.create();

  private ClientService.Interface stub;

  private ExecutorService threadPool;

  private AsyncConnection conn;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws IOException {
    this.threadPool = Executors.newSingleThreadExecutor();
    stub = mock(ClientService.Interface.class);

    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        ClientProtos.MultiResponse resp =
          ClientProtos.MultiResponse.newBuilder()
            .addRegionActionResult(RegionActionResult.newBuilder().addResultOrException(
              ResultOrException.newBuilder().setResult(ProtobufUtil.toResult(new Result()))))
            .build();
        RpcCallback<ClientProtos.MultiResponse> done = invocation.getArgument(2);
        done.run(resp);
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
        done.run(resp);
        return null;
      }
    }).when(stub).mutate(any(HBaseRpcController.class), any(MutateRequest.class), any());
    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        RpcCallback<GetResponse> done = invocation.getArgument(2);
        done.run(GetResponse.getDefaultInstance());
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
  }

  private HBaseRpcController assertPriority(int priority) {
    return argThat(new ArgumentMatcher<HBaseRpcController>() {

      @Override
      public boolean matches(HBaseRpcController controller) {
        return controller.getPriority() == priority;
      }
    });
  }

  private ScanRequest assertScannerCloseRequest() {
    return argThat(new ArgumentMatcher<ScanRequest>() {

      @Override
      public boolean matches(ScanRequest request) {
        return request.hasCloseScanner() && request.getCloseScanner();
      }
    });
  }

  @Test
  public void testGet() {
    conn.getTable(TableName.valueOf(name.getMethodName()))
      .get(new Get(Bytes.toBytes(0)).setPriority(11)).join();
    verify(stub, times(1)).get(assertPriority(11), any(GetRequest.class), any());
  }

  @Test
  public void testGetNormalTable() {
    conn.getTable(TableName.valueOf(name.getMethodName())).get(new Get(Bytes.toBytes(0))).join();
    verify(stub, times(1)).get(assertPriority(NORMAL_QOS), any(GetRequest.class), any());
  }

  @Test
  public void testGetSystemTable() {
    conn.getTable(TableName.valueOf(SYSTEM_NAMESPACE_NAME_STR, name.getMethodName()))
      .get(new Get(Bytes.toBytes(0))).join();
    verify(stub, times(1)).get(assertPriority(SYSTEMTABLE_QOS), any(GetRequest.class), any());
  }

  @Test
  public void testGetMetaTable() {
    conn.getTable(TableName.META_TABLE_NAME).get(new Get(Bytes.toBytes(0))).join();
    verify(stub, times(1)).get(assertPriority(SYSTEMTABLE_QOS), any(GetRequest.class), any());
  }

  @Test
  public void testPut() {
    conn
      .getTable(TableName.valueOf(name.getMethodName())).put(new Put(Bytes.toBytes(0))
        .setPriority(12).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("v")))
      .join();
    verify(stub, times(1)).mutate(assertPriority(12), any(MutateRequest.class), any());
  }

  @Test
  public void testPutNormalTable() {
    conn.getTable(TableName.valueOf(name.getMethodName())).put(new Put(Bytes.toBytes(0))
      .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("v"))).join();
    verify(stub, times(1)).mutate(assertPriority(NORMAL_QOS), any(MutateRequest.class), any());
  }

  @Test
  public void testPutSystemTable() {
    conn.getTable(TableName.valueOf(SYSTEM_NAMESPACE_NAME_STR, name.getMethodName()))
      .put(new Put(Bytes.toBytes(0)).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"),
        Bytes.toBytes("v")))
      .join();
    verify(stub, times(1)).mutate(assertPriority(SYSTEMTABLE_QOS), any(MutateRequest.class), any());
  }

  @Test
  public void testPutMetaTable() {
    conn.getTable(TableName.META_TABLE_NAME).put(new Put(Bytes.toBytes(0))
      .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("v"))).join();
    verify(stub, times(1)).mutate(assertPriority(SYSTEMTABLE_QOS), any(MutateRequest.class), any());
  }

  @Test
  public void testDelete() {
    conn.getTable(TableName.valueOf(name.getMethodName()))
      .delete(new Delete(Bytes.toBytes(0)).setPriority(13)).join();
    verify(stub, times(1)).mutate(assertPriority(13), any(MutateRequest.class), any());
  }

  @Test
  public void testDeleteNormalTable() {
    conn.getTable(TableName.valueOf(name.getMethodName())).delete(new Delete(Bytes.toBytes(0)))
      .join();
    verify(stub, times(1)).mutate(assertPriority(NORMAL_QOS), any(MutateRequest.class), any());
  }

  @Test
  public void testDeleteSystemTable() {
    conn.getTable(TableName.valueOf(SYSTEM_NAMESPACE_NAME_STR, name.getMethodName()))
      .delete(new Delete(Bytes.toBytes(0))).join();
    verify(stub, times(1)).mutate(assertPriority(SYSTEMTABLE_QOS), any(MutateRequest.class), any());
  }

  @Test
  public void testDeleteMetaTable() {
    conn.getTable(TableName.META_TABLE_NAME).delete(new Delete(Bytes.toBytes(0))).join();
    verify(stub, times(1)).mutate(assertPriority(SYSTEMTABLE_QOS), any(MutateRequest.class), any());
  }

  @Test
  public void testAppend() {
    conn
      .getTable(TableName.valueOf(name.getMethodName())).append(new Append(Bytes.toBytes(0))
        .setPriority(14).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("v")))
      .join();
    verify(stub, times(1)).mutate(assertPriority(14), any(MutateRequest.class), any());
  }

  @Test
  public void testAppendNormalTable() {
    conn.getTable(TableName.valueOf(name.getMethodName())).append(new Append(Bytes.toBytes(0))
      .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("v"))).join();
    verify(stub, times(1)).mutate(assertPriority(NORMAL_QOS), any(MutateRequest.class), any());
  }

  @Test
  public void testAppendSystemTable() {
    conn.getTable(TableName.valueOf(SYSTEM_NAMESPACE_NAME_STR, name.getMethodName()))
      .append(new Append(Bytes.toBytes(0)).addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"),
        Bytes.toBytes("v")))
      .join();
    verify(stub, times(1)).mutate(assertPriority(SYSTEMTABLE_QOS), any(MutateRequest.class), any());
  }

  @Test
  public void testAppendMetaTable() {
    conn.getTable(TableName.META_TABLE_NAME).append(new Append(Bytes.toBytes(0))
      .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("v"))).join();
    verify(stub, times(1)).mutate(assertPriority(SYSTEMTABLE_QOS), any(MutateRequest.class), any());
  }

  @Test
  public void testIncrement() {
    conn.getTable(TableName.valueOf(name.getMethodName())).increment(new Increment(Bytes.toBytes(0))
      .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1).setPriority(15)).join();
    verify(stub, times(1)).mutate(assertPriority(15), any(MutateRequest.class), any());
  }

  @Test
  public void testIncrementNormalTable() {
    conn.getTable(TableName.valueOf(name.getMethodName()))
      .incrementColumnValue(Bytes.toBytes(0), Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1).join();
    verify(stub, times(1)).mutate(assertPriority(NORMAL_QOS), any(MutateRequest.class), any());
  }

  @Test
  public void testIncrementSystemTable() {
    conn.getTable(TableName.valueOf(SYSTEM_NAMESPACE_NAME_STR, name.getMethodName()))
      .incrementColumnValue(Bytes.toBytes(0), Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1).join();
    verify(stub, times(1)).mutate(assertPriority(SYSTEMTABLE_QOS), any(MutateRequest.class), any());
  }

  @Test
  public void testIncrementMetaTable() {
    conn.getTable(TableName.META_TABLE_NAME)
      .incrementColumnValue(Bytes.toBytes(0), Bytes.toBytes("cf"), Bytes.toBytes("cq"), 1).join();
    verify(stub, times(1)).mutate(assertPriority(SYSTEMTABLE_QOS), any(MutateRequest.class), any());
  }

  @Test
  public void testCheckAndPut() {
    conn.getTable(TableName.valueOf(name.getMethodName()))
      .checkAndMutate(Bytes.toBytes(0), Bytes.toBytes("cf")).qualifier(Bytes.toBytes("cq"))
      .ifNotExists()
      .thenPut(new Put(Bytes.toBytes(0))
        .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("v")).setPriority(16))
      .join();
    verify(stub, times(1)).mutate(assertPriority(16), any(MutateRequest.class), any());
  }

  @Test
  public void testCheckAndPutNormalTable() {
    conn.getTable(TableName.valueOf(name.getMethodName()))
      .checkAndMutate(Bytes.toBytes(0), Bytes.toBytes("cf")).qualifier(Bytes.toBytes("cq"))
      .ifNotExists().thenPut(new Put(Bytes.toBytes(0)).addColumn(Bytes.toBytes("cf"),
        Bytes.toBytes("cq"), Bytes.toBytes("v")))
      .join();
    verify(stub, times(1)).mutate(assertPriority(NORMAL_QOS), any(MutateRequest.class), any());
  }

  @Test
  public void testCheckAndPutSystemTable() {
    conn.getTable(TableName.valueOf(SYSTEM_NAMESPACE_NAME_STR, name.getMethodName()))
      .checkAndMutate(Bytes.toBytes(0), Bytes.toBytes("cf")).qualifier(Bytes.toBytes("cq"))
      .ifNotExists().thenPut(new Put(Bytes.toBytes(0)).addColumn(Bytes.toBytes("cf"),
        Bytes.toBytes("cq"), Bytes.toBytes("v")))
      .join();
    verify(stub, times(1)).mutate(assertPriority(SYSTEMTABLE_QOS), any(MutateRequest.class), any());
  }

  @Test
  public void testCheckAndPutMetaTable() {
    conn.getTable(TableName.META_TABLE_NAME).checkAndMutate(Bytes.toBytes(0), Bytes.toBytes("cf"))
      .qualifier(Bytes.toBytes("cq")).ifNotExists().thenPut(new Put(Bytes.toBytes(0))
        .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("v")))
      .join();
    verify(stub, times(1)).mutate(assertPriority(SYSTEMTABLE_QOS), any(MutateRequest.class), any());
  }

  @Test
  public void testCheckAndDelete() {
    conn.getTable(TableName.valueOf(name.getMethodName()))
      .checkAndMutate(Bytes.toBytes(0), Bytes.toBytes("cf")).qualifier(Bytes.toBytes("cq"))
      .ifEquals(Bytes.toBytes("v")).thenDelete(new Delete(Bytes.toBytes(0)).setPriority(17)).join();
    verify(stub, times(1)).mutate(assertPriority(17), any(MutateRequest.class), any());
  }

  @Test
  public void testCheckAndDeleteNormalTable() {
    conn.getTable(TableName.valueOf(name.getMethodName()))
      .checkAndMutate(Bytes.toBytes(0), Bytes.toBytes("cf")).qualifier(Bytes.toBytes("cq"))
      .ifEquals(Bytes.toBytes("v")).thenDelete(new Delete(Bytes.toBytes(0))).join();
    verify(stub, times(1)).mutate(assertPriority(NORMAL_QOS), any(MutateRequest.class), any());
  }

  @Test
  public void testCheckAndDeleteSystemTable() {
    conn.getTable(TableName.valueOf(SYSTEM_NAMESPACE_NAME_STR, name.getMethodName()))
      .checkAndMutate(Bytes.toBytes(0), Bytes.toBytes("cf")).qualifier(Bytes.toBytes("cq"))
      .ifEquals(Bytes.toBytes("v")).thenDelete(new Delete(Bytes.toBytes(0))).join();
    verify(stub, times(1)).mutate(assertPriority(SYSTEMTABLE_QOS), any(MutateRequest.class), any());
  }

  @Test
  public void testCheckAndDeleteMetaTable() {
    conn.getTable(TableName.META_TABLE_NAME).checkAndMutate(Bytes.toBytes(0), Bytes.toBytes("cf"))
      .qualifier(Bytes.toBytes("cq")).ifNotExists().thenPut(new Put(Bytes.toBytes(0))
        .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cq"), Bytes.toBytes("v")))
      .join();
    verify(stub, times(1)).mutate(assertPriority(SYSTEMTABLE_QOS), any(MutateRequest.class), any());
  }

  @Test
  public void testCheckAndMutate() throws IOException {
    conn.getTable(TableName.valueOf(name.getMethodName()))
      .checkAndMutate(Bytes.toBytes(0), Bytes.toBytes("cf")).qualifier(Bytes.toBytes("cq"))
      .ifEquals(Bytes.toBytes("v")).thenMutate(new RowMutations(Bytes.toBytes(0))
        .add((Mutation) new Delete(Bytes.toBytes(0)).setPriority(18)))
      .join();
    verify(stub, times(1)).multi(assertPriority(18), any(ClientProtos.MultiRequest.class), any());
  }

  @Test
  public void testCheckAndMutateNormalTable() throws IOException {
    conn.getTable(TableName.valueOf(name.getMethodName()))
      .checkAndMutate(Bytes.toBytes(0), Bytes.toBytes("cf")).qualifier(Bytes.toBytes("cq"))
      .ifEquals(Bytes.toBytes("v"))
      .thenMutate(new RowMutations(Bytes.toBytes(0)).add((Mutation) new Delete(Bytes.toBytes(0))))
      .join();
    verify(stub, times(1)).multi(assertPriority(NORMAL_QOS), any(ClientProtos.MultiRequest.class),
      any());
  }

  @Test
  public void testCheckAndMutateSystemTable() throws IOException {
    conn.getTable(TableName.valueOf(SYSTEM_NAMESPACE_NAME_STR, name.getMethodName()))
      .checkAndMutate(Bytes.toBytes(0), Bytes.toBytes("cf")).qualifier(Bytes.toBytes("cq"))
      .ifEquals(Bytes.toBytes("v"))
      .thenMutate(new RowMutations(Bytes.toBytes(0)).add((Mutation) new Delete(Bytes.toBytes(0))))
      .join();
    verify(stub, times(1)).multi(assertPriority(SYSTEMTABLE_QOS),
      any(ClientProtos.MultiRequest.class), any());
  }

  @Test
  public void testCheckAndMutateMetaTable() throws IOException {
    conn.getTable(TableName.META_TABLE_NAME).checkAndMutate(Bytes.toBytes(0), Bytes.toBytes("cf"))
      .qualifier(Bytes.toBytes("cq")).ifEquals(Bytes.toBytes("v"))
      .thenMutate(new RowMutations(Bytes.toBytes(0)).add((Mutation) new Delete(Bytes.toBytes(0))))
      .join();
    verify(stub, times(1)).multi(assertPriority(SYSTEMTABLE_QOS),
      any(ClientProtos.MultiRequest.class), any());
  }

  private CompletableFuture<Void> mockScanReturnRenewFuture(int scanPriority) {
    int scannerId = 1;
    CompletableFuture<Void> future = new CompletableFuture<>();
    AtomicInteger scanNextCalled = new AtomicInteger(0);
    doAnswer(new Answer<Void>() {

      @SuppressWarnings("FutureReturnValueIgnored")
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        threadPool.submit(() -> {
          ScanRequest req = invocation.getArgument(1);
          RpcCallback<ScanResponse> done = invocation.getArgument(2);
          if (!req.hasScannerId()) {
            done.run(ScanResponse.newBuilder()
                .setScannerId(scannerId).setTtl(800)
                .setMoreResultsInRegion(true).setMoreResults(true)
                .build());
          } else {
            if (req.hasRenew() && req.getRenew()) {
              future.complete(null);
            }

            assertFalse("close scanner should not come in with scan priority " + scanPriority,
                req.hasCloseScanner() && req.getCloseScanner());

            Cell cell = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
                .setType(Type.Put).setRow(Bytes.toBytes(scanNextCalled.incrementAndGet()))
                .setFamily(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("cq"))
                .setValue(Bytes.toBytes("v")).build();
            Result result = Result.create(Arrays.asList(cell));
            done.run(
                ScanResponse.newBuilder()
                    .setScannerId(scannerId).setTtl(800).setMoreResultsInRegion(true)
                    .setMoreResults(true).addResults(ProtobufUtil.toResult(result))
                    .build());
          }
        });
        return null;
      }
    }).when(stub).scan(assertPriority(scanPriority), any(ScanRequest.class), any());

    doAnswer(new Answer<Void>() {

      @SuppressWarnings("FutureReturnValueIgnored")
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        threadPool.submit(() ->{
          ScanRequest req = invocation.getArgument(1);
          RpcCallback<ScanResponse> done = invocation.getArgument(2);
          assertTrue("close request should have scannerId", req.hasScannerId());
          assertEquals("close request's scannerId should match", scannerId, req.getScannerId());
          assertTrue("close request should have closerScanner set",
              req.hasCloseScanner() && req.getCloseScanner());

          done.run(ScanResponse.getDefaultInstance());
        });
        return null;
      }
    }).when(stub).scan(assertPriority(HIGH_QOS), assertScannerCloseRequest(), any());
    return future;
  }

  @Test
  public void testScan() throws Exception {
    CompletableFuture<Void> renewFuture = mockScanReturnRenewFuture(19);
    testForTable(TableName.valueOf(name.getMethodName()), renewFuture, Optional.of(19));
  }

  @Test
  public void testScanNormalTable() throws Exception {
    CompletableFuture<Void> renewFuture = mockScanReturnRenewFuture(NORMAL_QOS);
    testForTable(TableName.valueOf(name.getMethodName()), renewFuture, Optional.of(NORMAL_QOS));
  }

  @Test
  public void testScanSystemTable() throws Exception {
    CompletableFuture<Void> renewFuture = mockScanReturnRenewFuture(SYSTEMTABLE_QOS);
    testForTable(TableName.valueOf(SYSTEM_NAMESPACE_NAME_STR, name.getMethodName()),
        renewFuture, Optional.empty());
  }

  @Test
  public void testScanMetaTable() throws Exception {
    CompletableFuture<Void> renewFuture = mockScanReturnRenewFuture(SYSTEMTABLE_QOS);
    testForTable(TableName.META_TABLE_NAME, renewFuture, Optional.empty());
  }

  private void testForTable(TableName tableName, CompletableFuture<Void> renewFuture,
                            Optional<Integer> priority) throws Exception {
    Scan scan = new Scan().setCaching(1).setMaxResultSize(1);
    priority.ifPresent(scan::setPriority);

    try (ResultScanner scanner = conn.getTable(tableName).getScanner(scan)) {
      assertNotNull(scanner.next());
      // wait for at least one renew to come in before closing
      renewFuture.join();
    }

    // ensures the close thread has time to finish before asserting
    threadPool.shutdown();
    threadPool.awaitTermination(5, TimeUnit.SECONDS);

    // just verify that the calls happened. verification of priority occurred in the mocking
    // open, next, then one or more lease renewals, then close
    verify(stub, atLeast(4)).scan(any(), any(ScanRequest.class), any());
    // additionally, explicitly check for a close request
    verify(stub, times(1)).scan(any(), assertScannerCloseRequest(), any());
  }

  @Test
  public void testBatchNormalTable() {
    conn.getTable(TableName.valueOf(name.getMethodName()))
      .batchAll(Arrays.asList(new Delete(Bytes.toBytes(0)))).join();
    verify(stub, times(1)).multi(assertPriority(NORMAL_QOS), any(ClientProtos.MultiRequest.class),
      any());
  }

  @Test
  public void testBatchSystemTable() {
    conn.getTable(TableName.valueOf(SYSTEM_NAMESPACE_NAME_STR, name.getMethodName()))
      .batchAll(Arrays.asList(new Delete(Bytes.toBytes(0)))).join();
    verify(stub, times(1)).multi(assertPriority(SYSTEMTABLE_QOS),
      any(ClientProtos.MultiRequest.class), any());
  }

  @Test
  public void testBatchMetaTable() {
    conn.getTable(TableName.META_TABLE_NAME).batchAll(Arrays.asList(new Delete(Bytes.toBytes(0))))
      .join();
    verify(stub, times(1)).multi(assertPriority(SYSTEMTABLE_QOS),
      any(ClientProtos.MultiRequest.class), any());
  }
}
