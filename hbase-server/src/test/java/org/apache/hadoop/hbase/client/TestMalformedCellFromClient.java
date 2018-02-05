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
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegion;
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

/**
 * The purpose of this test is to make sure the region exception won't corrupt the results
 * of batch. The prescription is shown below.
 * 1) honor the action result rather than region exception. If the action have both of true result
 * and region exception, the action is fine as the exception is caused by other actions
 * which are in the same region.
 * 2) honor the action exception rather than region exception. If the action have both of action
 * exception and region exception, we deal with the action exception only. If we also
 * handle the region exception for the same action, it will introduce the negative count of
 * actions in progress. The AsyncRequestFuture#waitUntilDone will block forever.
 *
 * The no-cluster test is in TestAsyncProcessWithRegionException.
 */
@Category({ MediumTests.class, ClientTests.class })
public class TestMalformedCellFromClient {

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
      .addColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
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
   * The purpose of this ut is to check the consistency between the exception and results.
   * If the RetriesExhaustedWithDetailsException contains the whole batch,
   * each result should be of IOE. Otherwise, the row operation which is not in the exception
   * should have a true result.
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
}
