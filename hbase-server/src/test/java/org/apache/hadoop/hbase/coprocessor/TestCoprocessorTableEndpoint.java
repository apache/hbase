/*
 *
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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ColumnAggregationProtos;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.ServiceException;

@Category({CoprocessorTests.class, MediumTests.class})
public class TestCoprocessorTableEndpoint {

  private static final byte[] TEST_FAMILY = Bytes.toBytes("TestFamily");
  private static final byte[] TEST_QUALIFIER = Bytes.toBytes("TestQualifier");
  private static final byte[] ROW = Bytes.toBytes("testRow");
  private static final int ROWSIZE = 20;
  private static final int rowSeperator1 = 5;
  private static final int rowSeperator2 = 12;
  private static final byte[][] ROWS = makeN(ROW, ROWSIZE);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCoprocessorTableEndpoint() throws Throwable {    
    final TableName tableName = TableName.valueOf("testCoprocessorTableEndpoint");

    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(TEST_FAMILY));
    desc.addCoprocessor(org.apache.hadoop.hbase.coprocessor.ColumnAggregationEndpoint.class.getName());

    createTable(desc);
    verifyTable(tableName);
  }

  @Test
  public void testDynamicCoprocessorTableEndpoint() throws Throwable {    
    final TableName tableName = TableName.valueOf("testDynamicCoprocessorTableEndpoint");

    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(TEST_FAMILY));

    createTable(desc);

    desc.addCoprocessor(org.apache.hadoop.hbase.coprocessor.ColumnAggregationEndpoint.class.getName());
    updateTable(desc);

    verifyTable(tableName);
  }

  private static byte[][] makeN(byte[] base, int n) {
    byte[][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      ret[i] = Bytes.add(base, Bytes.toBytes(String.format("%02d", i)));
    }
    return ret;
  }

  private static Map<byte [], Long> sum(final Table table, final byte [] family,
    final byte [] qualifier, final byte [] start, final byte [] end)
      throws ServiceException, Throwable {
  return table.coprocessorService(ColumnAggregationProtos.ColumnAggregationService.class,
      start, end,
    new Batch.Call<ColumnAggregationProtos.ColumnAggregationService, Long>() {
      @Override
      public Long call(ColumnAggregationProtos.ColumnAggregationService instance)
      throws IOException {
        BlockingRpcCallback<ColumnAggregationProtos.SumResponse> rpcCallback =
            new BlockingRpcCallback<ColumnAggregationProtos.SumResponse>();
        ColumnAggregationProtos.SumRequest.Builder builder =
          ColumnAggregationProtos.SumRequest.newBuilder();
        builder.setFamily(ByteStringer.wrap(family));
        if (qualifier != null && qualifier.length > 0) {
          builder.setQualifier(ByteStringer.wrap(qualifier));
        }
        instance.sum(null, builder.build(), rpcCallback);
        return rpcCallback.get().getSum();
      }
    });
  }

  private static final void createTable(HTableDescriptor desc) throws Exception {
    Admin admin = TEST_UTIL.getHBaseAdmin();
    admin.createTable(desc, new byte[][]{ROWS[rowSeperator1], ROWS[rowSeperator2]});
    TEST_UTIL.waitUntilAllRegionsAssigned(desc.getTableName());
    Table table = TEST_UTIL.getConnection().getTable(desc.getTableName());
    try {
      for (int i = 0; i < ROWSIZE; i++) {
        Put put = new Put(ROWS[i]);
        put.addColumn(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(i));
        table.put(put);
      }
    } finally {
      table.close();    
    }
  }

  private static void updateTable(HTableDescriptor desc) throws Exception {
    Admin admin = TEST_UTIL.getHBaseAdmin();
    admin.disableTable(desc.getTableName());
    admin.modifyTable(desc.getTableName(), desc);
    admin.enableTable(desc.getTableName());
  }

  private static final void verifyTable(TableName tableName) throws Throwable {
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    try {
      Map<byte[], Long> results = sum(table, TEST_FAMILY, TEST_QUALIFIER, ROWS[0],
        ROWS[ROWS.length-1]);
      int sumResult = 0;
      int expectedResult = 0;
      for (Map.Entry<byte[], Long> e : results.entrySet()) {
        sumResult += e.getValue();
      }
      for (int i = 0; i < ROWSIZE; i++) {
        expectedResult += i;
      }
      assertEquals("Invalid result", expectedResult, sumResult);

      // scan: for region 2 and region 3
      results.clear();
      results = sum(table, TEST_FAMILY, TEST_QUALIFIER, ROWS[rowSeperator1], ROWS[ROWS.length-1]);
      sumResult = 0;
      expectedResult = 0;
      for (Map.Entry<byte[], Long> e : results.entrySet()) {
        sumResult += e.getValue();
      }
      for (int i = rowSeperator1; i < ROWSIZE; i++) {
        expectedResult += i;
      }
      assertEquals("Invalid result", expectedResult, sumResult);
    } finally {
      table.close();
    }
  }
}
