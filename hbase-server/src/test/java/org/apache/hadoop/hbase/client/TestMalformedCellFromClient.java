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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * The purpose of this test is to ensure whether rs deals with the malformed cells correctly.
 */
@Category({ MediumTests.class, ClientTests.class })
public class TestMalformedCellFromClient {
  private static final Log LOG = LogFactory.getLog(TestMalformedCellFromClient.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] FAMILY = Bytes.toBytes("testFamily");
  private static final TableName TABLE_NAME = TableName.valueOf("TestMalformedCellFromClient");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // disable the retry
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    TEST_UTIL.startMiniCluster(1);
  }

  @Before
  public void before() throws Exception {
    HTableDescriptor desc =
      new HTableDescriptor(TABLE_NAME).addFamily(new HColumnDescriptor(FAMILY));
    TEST_UTIL.getConnection().getAdmin().createTable(desc);
  }

  @After
  public void tearDown() throws Exception {
    for (HTableDescriptor htd : TEST_UTIL.getHBaseAdmin().listTables()) {
      TEST_UTIL.deleteTable(htd.getTableName());
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * The invalid cells is in rm. The rm should fail but the subsequent mutations should succeed.
   * Currently, we have no client api to submit the request consisting of condition-rm and mutation.
   * Hence, this test build the request manually.
   */
  @Test
  public void testAtomicOperations() throws Exception {
    RowMutations rm = new RowMutations(Bytes.toBytes("fail"));
    rm.add(new Put(rm.getRow()).addColumn(FAMILY, null, new byte[10]));
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
    PayloadCarryingRpcController controller = Mockito.mock(PayloadCarryingRpcController.class);
    Mockito.when(controller.cellScanner()).thenReturn(CellUtil.createCellScanner(cells));
    HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
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
    ClientProtos.Condition condition = RequestConverter
      .buildCondition(rm.getRow(), FAMILY, FAMILY, new BinaryComparator(new byte[10]),
        HBaseProtos.CompareType.EQUAL);
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
      // add a get to fail the rm
      actionBuilder.setGet(ProtobufUtil.toGet(new Get(rm.getRow())));
      builder.addAction(actionBuilder.build());
    }
    ClientProtos.MultiRequest request =
      ClientProtos.MultiRequest.newBuilder().addRegionAction(builder.build())
        .setCondition(condition).build();
    return request;
  }
}