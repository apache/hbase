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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

/**
 * The purpose of this test is to ensure whether rs deals with the malformed cells correctly.
 */
@Category({ MediumTests.class, ClientTests.class })
public class TestMalformedCellFromClient {
  private static final Logger LOG = LoggerFactory.getLogger(TestMalformedCellFromClient.class);
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMalformedCellFromClient.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] FAMILY = Bytes.toBytes("testFamily");
  private static final int CELL_SIZE = 100;
  private static final TableName TABLE_NAME = TableName.valueOf("TestMalformedCellFromClient");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // disable the retry
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 0);
    TEST_UTIL.startMiniCluster(1);
  }

  @Before
  public void before() throws Exception {
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
      .setValue(HRegion.HBASE_MAX_CELL_SIZE_KEY, String.valueOf(CELL_SIZE)).build();
    TEST_UTIL.getConnection().getAdmin().createTable(desc);
  }

  @After
  public void tearDown() throws Exception {
    for (TableDescriptor htd : TEST_UTIL.getAdmin().listTableDescriptors()) {
      TEST_UTIL.deleteTable(htd.getTableName());
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * The purpose of this ut is to check the consistency between the exception and results.
   * If the RetriesExhaustedWithDetailsException contains the whole batch,
   * each result should be of IOE. Otherwise, the row operation which is not in the exception
   * should have a true result.
   */
  @Test
  public void testRegionException() throws InterruptedException, IOException {
    List<Row> batches = new ArrayList<>();
    batches.add(new Put(Bytes.toBytes("good")).addColumn(FAMILY, null, new byte[10]));
    // the rm is used to prompt the region exception.
    // see RSRpcServices#multi
    RowMutations rm = new RowMutations(Bytes.toBytes("fail"));
    rm.add(new Put(rm.getRow()).addColumn(FAMILY, null, new byte[CELL_SIZE]));
    batches.add(rm);
    Object[] results = new Object[batches.size()];

    try (Table table = TEST_UTIL.getConnection().getTable(TABLE_NAME)) {
      Throwable exceptionByCaught = null;
      try {
        table.batch(batches, results);
        fail("Where is the exception? We put the malformed cells!!!");
      } catch (RetriesExhaustedWithDetailsException e) {
        for (Throwable throwable : e.getCauses()) {
          assertNotNull(throwable);
        }
        assertEquals(1, e.getNumExceptions());
        exceptionByCaught = e.getCause(0);
      }
      for (Object obj : results) {
        assertNotNull(obj);
      }
      assertEquals(Result.class, results[0].getClass());
      assertEquals(exceptionByCaught.getClass(), results[1].getClass());
      Result result = table.get(new Get(Bytes.toBytes("good")));
      assertEquals(1, result.size());
      Cell cell = result.getColumnLatestCell(FAMILY, null);
      assertTrue(Bytes.equals(CellUtil.cloneValue(cell), new byte[10]));
    }
  }

  /**
   * This test verifies region exception doesn't corrupt the results of batch. The prescription is
   * shown below. 1) honor the action result rather than region exception. If the action have both
   * of true result and region exception, the action is fine as the exception is caused by other
   * actions which are in the same region. 2) honor the action exception rather than region
   * exception. If the action have both of action exception and region exception, we deal with the
   * action exception only. If we also handle the region exception for the same action, it will
   * introduce the negative count of actions in progress. The AsyncRequestFuture#waitUntilDone will
   * block forever. If the RetriesExhaustedWithDetailsException contains the whole batch, each
   * result should be of IOE. Otherwise, the row operation which is not in the exception should have
   * a true result. The no-cluster test is in TestAsyncProcessWithRegionException.
   */
  @Test
  public void testRegionExceptionByAsync() throws Exception {
    List<Row> batches = new ArrayList<>();
    batches.add(new Put(Bytes.toBytes("good")).addColumn(FAMILY, null, new byte[10]));
    // the rm is used to prompt the region exception.
    // see RSRpcServices#multi
    RowMutations rm = new RowMutations(Bytes.toBytes("fail"));
    rm.add(new Put(rm.getRow()).addColumn(FAMILY, null, new byte[CELL_SIZE]));
    batches.add(rm);
    try (AsyncConnection asyncConnection = ConnectionFactory
      .createAsyncConnection(TEST_UTIL.getConfiguration()).get()) {
      AsyncTable<AdvancedScanResultConsumer> table = asyncConnection.getTable(TABLE_NAME);
      List<CompletableFuture<AdvancedScanResultConsumer>> results = table.batch(batches);
      assertEquals(2, results.size());
      try {
        results.get(1).get();
        fail("Where is the exception? We put the malformed cells!!!");
      } catch (ExecutionException e) {
        // pass
      }
      Result result = table.get(new Get(Bytes.toBytes("good"))).get();
      assertEquals(1, result.size());
      Cell cell = result.getColumnLatestCell(FAMILY, null);
      assertTrue(Bytes.equals(CellUtil.cloneValue(cell), new byte[10]));
    }
  }

  /**
   * The invalid cells is in rm. The rm should fail but the subsequent mutations should succeed.
   * Currently, we have no client api to submit the request consisting of condition-rm and mutation.
   * Hence, this test build the request manually.
   */
  @Test
  public void testAtomicOperations() throws Exception {
    RowMutations rm = new RowMutations(Bytes.toBytes("fail"));
    rm.add(new Put(rm.getRow()).addColumn(FAMILY, null, new byte[CELL_SIZE]));
    rm.add(new Put(rm.getRow()).addColumn(FAMILY, null, new byte[10]));
    Put put = new Put(Bytes.toBytes("good")).addColumn(FAMILY, null, new byte[10]);

    // build the request
    HRegion r = TEST_UTIL.getMiniHBaseCluster().getRegions(TABLE_NAME).get(0);
    ClientProtos.MultiRequest request =
      ClientProtos.MultiRequest.newBuilder(createRequest(rm, r.getRegionInfo().getRegionName()))
        .addRegionAction(ClientProtos.RegionAction.newBuilder().setRegion(RequestConverter
          .buildRegionSpecifier(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME,
            r.getRegionInfo().getRegionName())).addAction(ClientProtos.Action.newBuilder()
          .setMutation(
            ProtobufUtil.toMutationNoData(ClientProtos.MutationProto.MutationType.PUT, put))))
        .build();

    List<Cell> cells = new ArrayList<>();
    for (Mutation m : rm.getMutations()) {
      cells.addAll(m.getCellList(FAMILY));
    }
    cells.addAll(put.getCellList(FAMILY));
    assertEquals(3, cells.size());
    HBaseRpcController controller = Mockito.mock(HBaseRpcController.class);
    Mockito.when(controller.cellScanner()).thenReturn(CellUtil.createCellScanner(cells));
    HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(
      TEST_UTIL.getMiniHBaseCluster()
        .getServerHoldingRegion(TABLE_NAME, r.getRegionInfo().getRegionName()));

    ClientProtos.MultiResponse response = rs.getRSRpcServices().multi(controller, request);
    assertEquals(2, response.getRegionActionResultCount());
    assertTrue(response.getRegionActionResultList().get(0).hasException());
    assertFalse(response.getRegionActionResultList().get(1).hasException());
    assertEquals(1, response.getRegionActionResultList().get(1).getResultOrExceptionCount());
    assertTrue(
      response.getRegionActionResultList().get(1).getResultOrExceptionList().get(0).hasResult());
    try (Table table = TEST_UTIL.getConnection().getTable(TABLE_NAME)) {
      Result result = table.get(new Get(Bytes.toBytes("good")));
      assertEquals(1, result.size());
      Cell cell = result.getColumnLatestCell(FAMILY, null);
      assertTrue(Bytes.equals(CellUtil.cloneValue(cell), new byte[10]));
    }
  }

  private static ClientProtos.MultiRequest createRequest(RowMutations rm, byte[] regionName)
    throws IOException {
    ClientProtos.RegionAction.Builder builder = RequestConverter
      .getRegionActionBuilderWithRegion(ClientProtos.RegionAction.newBuilder(), regionName);
    builder.setAtomic(true);
    ClientProtos.Action.Builder actionBuilder = ClientProtos.Action.newBuilder();
    ClientProtos.MutationProto.Builder mutationBuilder = ClientProtos.MutationProto.newBuilder();
    ClientProtos.Condition condition = ProtobufUtil.toCondition(rm.getRow(), FAMILY, null,
      CompareOperator.EQUAL, new byte[10], null, null);
    for (Mutation mutation : rm.getMutations()) {
      ClientProtos.MutationProto.MutationType mutateType = null;
      if (mutation instanceof Put) {
        mutateType = ClientProtos.MutationProto.MutationType.PUT;
      } else if (mutation instanceof Delete) {
        mutateType = ClientProtos.MutationProto.MutationType.DELETE;
      } else {
        throw new DoNotRetryIOException(
          "RowMutations supports only put and delete, not " + mutation.getClass().getName());
      }
      mutationBuilder.clear();
      ClientProtos.MutationProto mp =
        ProtobufUtil.toMutationNoData(mutateType, mutation, mutationBuilder);
      actionBuilder.clear();
      actionBuilder.setMutation(mp);
      builder.addAction(actionBuilder.build());
    }
    ClientProtos.MultiRequest request = ClientProtos.MultiRequest.newBuilder()
        .addRegionAction(builder.setCondition(condition).build()).build();
    return request;
  }

  /**
   * This test depends on how regionserver process the batch ops.
   * 1) group the put/delete until meeting the increment
   * 2) process the batch of put/delete
   * 3) process the increment
   * see RSRpcServices#doNonAtomicRegionMutation
   */
  @Test
  public void testNonAtomicOperations() throws InterruptedException, IOException {
    Increment inc = new Increment(Bytes.toBytes("good")).addColumn(FAMILY, null, 100);
    List<Row> batches = new ArrayList<>();
    // the first and second puts will be group by regionserver
    batches.add(new Put(Bytes.toBytes("fail")).addColumn(FAMILY, null, new byte[CELL_SIZE]));
    batches.add(new Put(Bytes.toBytes("fail")).addColumn(FAMILY, null, new byte[CELL_SIZE]));
    // this Increment should succeed
    batches.add(inc);
    // this put should succeed
    batches.add(new Put(Bytes.toBytes("good")).addColumn(FAMILY, null, new byte[1]));
    Object[] objs = new Object[batches.size()];
    try (Table table = TEST_UTIL.getConnection().getTable(TABLE_NAME)) {
      table.batch(batches, objs);
      fail("Where is the exception? We put the malformed cells!!!");
    } catch (RetriesExhaustedWithDetailsException e) {
      assertEquals(2, e.getNumExceptions());
      for (int i = 0; i != e.getNumExceptions(); ++i) {
        assertNotNull(e.getCause(i));
        assertEquals(DoNotRetryIOException.class, e.getCause(i).getClass());
        assertEquals("fail", Bytes.toString(e.getRow(i).getRow()));
      }
    } finally {
      assertObjects(objs, batches.size());
      assertTrue(objs[0] instanceof IOException);
      assertTrue(objs[1] instanceof IOException);
      assertEquals(Result.class, objs[2].getClass());
      assertEquals(Result.class, objs[3].getClass());
    }
  }

  @Test
  public void testRowMutations() throws InterruptedException, IOException {
    Put put = new Put(Bytes.toBytes("good")).addColumn(FAMILY, null, new byte[1]);
    List<Row> batches = new ArrayList<>();
    RowMutations mutations = new RowMutations(Bytes.toBytes("fail"));
    // the first and second puts will be group by regionserver
    mutations.add(new Put(Bytes.toBytes("fail")).addColumn(FAMILY, null, new byte[CELL_SIZE]));
    mutations.add(new Put(Bytes.toBytes("fail")).addColumn(FAMILY, null, new byte[CELL_SIZE]));
    batches.add(mutations);
    // this bm should succeed
    mutations = new RowMutations(Bytes.toBytes("good"));
    mutations.add(put);
    mutations.add(put);
    batches.add(mutations);
    Object[] objs = new Object[batches.size()];
    try (Table table = TEST_UTIL.getConnection().getTable(TABLE_NAME)) {
      table.batch(batches, objs);
      fail("Where is the exception? We put the malformed cells!!!");
    } catch (RetriesExhaustedWithDetailsException e) {
      assertEquals(1, e.getNumExceptions());
      for (int i = 0; i != e.getNumExceptions(); ++i) {
        assertNotNull(e.getCause(i));
        assertTrue(e.getCause(i) instanceof IOException);
        assertEquals("fail", Bytes.toString(e.getRow(i).getRow()));
      }
    } finally {
      assertObjects(objs, batches.size());
      assertTrue(objs[0] instanceof IOException);
      assertEquals(Result.class, objs[1].getClass());
    }
  }

  private static void assertObjects(Object[] objs, int expectedSize) {
    int count = 0;
    for (Object obj : objs) {
      assertNotNull(obj);
      ++count;
    }
    assertEquals(expectedSize, count);
  }
}
