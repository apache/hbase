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
package org.apache.hadoop.hbase.coprocessor.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.example.generated.BulkDeleteProtos.BulkDeleteRequest;
import org.apache.hadoop.hbase.coprocessor.example.generated.BulkDeleteProtos.BulkDeleteRequest.Builder;
import org.apache.hadoop.hbase.coprocessor.example.generated.BulkDeleteProtos.BulkDeleteRequest.DeleteType;
import org.apache.hadoop.hbase.coprocessor.example.generated.BulkDeleteProtos.BulkDeleteResponse;
import org.apache.hadoop.hbase.coprocessor.example.generated.BulkDeleteProtos.BulkDeleteService;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;

@Category({CoprocessorTests.class, MediumTests.class})
public class TestBulkDeleteProtocol {
  private static final byte[] FAMILY1 = Bytes.toBytes("cf1");
  private static final byte[] FAMILY2 = Bytes.toBytes("cf2");
  private static final byte[] QUALIFIER1 = Bytes.toBytes("c1");
  private static final byte[] QUALIFIER2 = Bytes.toBytes("c2");
  private static final byte[] QUALIFIER3 = Bytes.toBytes("c3");
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  // @Ignore @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().set(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        BulkDeleteEndpoint.class.getName());
    TEST_UTIL.startMiniCluster(2);
  }

  // @Ignore @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  // @Ignore @Test
  public void testBulkDeleteEndpoint() throws Throwable {
    TableName tableName = TableName.valueOf("testBulkDeleteEndpoint");
    Table ht = createTable(tableName);
    List<Put> puts = new ArrayList<Put>(100);
    for (int j = 0; j < 100; j++) {
      byte[] rowkey = Bytes.toBytes(j);
      puts.add(createPut(rowkey, "v1"));
    }
    ht.put(puts);
    // Deleting all the rows.
    long noOfRowsDeleted = invokeBulkDeleteProtocol(tableName, new Scan(), 5, DeleteType.ROW, null);
    assertEquals(100, noOfRowsDeleted);

    int rows = 0;
    for (Result result : ht.getScanner(new Scan())) {
      rows++;
    }
    assertEquals(0, rows);
    ht.close();
  }

  // @Ignore @Test
  public void testBulkDeleteEndpointWhenRowBatchSizeLessThanRowsToDeleteFromARegion()
      throws Throwable {
    TableName tableName = TableName
        .valueOf("testBulkDeleteEndpointWhenRowBatchSizeLessThanRowsToDeleteFromARegion");
    Table ht = createTable(tableName);
    List<Put> puts = new ArrayList<Put>(100);
    for (int j = 0; j < 100; j++) {
      byte[] rowkey = Bytes.toBytes(j);
      puts.add(createPut(rowkey, "v1"));
    }
    ht.put(puts);
    // Deleting all the rows.
    long noOfRowsDeleted = invokeBulkDeleteProtocol(tableName, new Scan(), 10, DeleteType.ROW, null);
    assertEquals(100, noOfRowsDeleted);

    int rows = 0;
    for (Result result : ht.getScanner(new Scan())) {
      rows++;
    }
    assertEquals(0, rows);
    ht.close();
  }

  private long invokeBulkDeleteProtocol(TableName tableName, final Scan scan, final int rowBatchSize,
      final DeleteType deleteType, final Long timeStamp) throws Throwable {
    Table ht = TEST_UTIL.getConnection().getTable(tableName);
    long noOfDeletedRows = 0L;
    Batch.Call<BulkDeleteService, BulkDeleteResponse> callable =
      new Batch.Call<BulkDeleteService, BulkDeleteResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<BulkDeleteResponse> rpcCallback =
        new BlockingRpcCallback<BulkDeleteResponse>();

      public BulkDeleteResponse call(BulkDeleteService service) throws IOException {
        Builder builder = BulkDeleteRequest.newBuilder();
        builder.setScan(ProtobufUtil.toScan(scan));
        builder.setDeleteType(deleteType);
        builder.setRowBatchSize(rowBatchSize);
        if (timeStamp != null) {
          builder.setTimestamp(timeStamp);
        }
        service.delete(controller, builder.build(), rpcCallback);
        return rpcCallback.get();
      }
    };
    Map<byte[], BulkDeleteResponse> result = ht.coprocessorService(BulkDeleteService.class, scan
        .getStartRow(), scan.getStopRow(), callable);
    for (BulkDeleteResponse response : result.values()) {
      noOfDeletedRows += response.getRowsDeleted();
    }
    ht.close();
    return noOfDeletedRows;
  }

  // @Ignore @Test
  public void testBulkDeleteWithConditionBasedDelete() throws Throwable {
    TableName tableName = TableName.valueOf("testBulkDeleteWithConditionBasedDelete");
    Table ht = createTable(tableName);
    List<Put> puts = new ArrayList<Put>(100);
    for (int j = 0; j < 100; j++) {
      byte[] rowkey = Bytes.toBytes(j);
      String value = (j % 10 == 0) ? "v1" : "v2";
      puts.add(createPut(rowkey, value));
    }
    ht.put(puts);
    Scan scan = new Scan();
    FilterList fl = new FilterList(Operator.MUST_PASS_ALL);
    SingleColumnValueFilter scvf = new SingleColumnValueFilter(FAMILY1, QUALIFIER3,
        CompareOp.EQUAL, Bytes.toBytes("v1"));
    // fl.addFilter(new FirstKeyOnlyFilter());
    fl.addFilter(scvf);
    scan.setFilter(fl);
    // Deleting all the rows where cf1:c1=v1
    long noOfRowsDeleted = invokeBulkDeleteProtocol(tableName, scan, 500, DeleteType.ROW, null);
    assertEquals(10, noOfRowsDeleted);

    int rows = 0;
    for (Result result : ht.getScanner(new Scan())) {
      rows++;
    }
    assertEquals(90, rows);
    ht.close();
  }

  // @Ignore @Test
  public void testBulkDeleteColumn() throws Throwable {
    TableName tableName = TableName.valueOf("testBulkDeleteColumn");
    Table ht = createTable(tableName);
    List<Put> puts = new ArrayList<Put>(100);
    for (int j = 0; j < 100; j++) {
      byte[] rowkey = Bytes.toBytes(j);
      String value = (j % 10 == 0) ? "v1" : "v2";
      puts.add(createPut(rowkey, value));
    }
    ht.put(puts);
    Scan scan = new Scan();
    scan.addColumn(FAMILY1, QUALIFIER2);
    // Delete the column cf1:col2
    long noOfRowsDeleted = invokeBulkDeleteProtocol(tableName, scan, 500, DeleteType.COLUMN, null);
    assertEquals(100, noOfRowsDeleted);

    int rows = 0;
    for (Result result : ht.getScanner(new Scan())) {
      assertEquals(2, result.getFamilyMap(FAMILY1).size());
      assertTrue(result.getColumnCells(FAMILY1, QUALIFIER2).isEmpty());
      assertEquals(1, result.getColumnCells(FAMILY1, QUALIFIER1).size());
      assertEquals(1, result.getColumnCells(FAMILY1, QUALIFIER3).size());
      rows++;
    }
    assertEquals(100, rows);
    ht.close();
  }

  // @Ignore @Test
  public void testBulkDeleteFamily() throws Throwable {
    TableName tableName = TableName.valueOf("testBulkDeleteFamily");
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(FAMILY1));
    htd.addFamily(new HColumnDescriptor(FAMILY2));
    TEST_UTIL.getHBaseAdmin().createTable(htd, Bytes.toBytes(0), Bytes.toBytes(120), 5);
    Table ht = TEST_UTIL.getConnection().getTable(tableName);
    List<Put> puts = new ArrayList<Put>(100);
    for (int j = 0; j < 100; j++) {
      Put put = new Put(Bytes.toBytes(j));
      put.add(FAMILY1, QUALIFIER1, "v1".getBytes());
      put.add(FAMILY2, QUALIFIER2, "v2".getBytes());
      puts.add(put);
    }
    ht.put(puts);
    Scan scan = new Scan();
    scan.addFamily(FAMILY1);
    // Delete the column family cf1
    long noOfRowsDeleted = invokeBulkDeleteProtocol(tableName, scan, 500, DeleteType.FAMILY, null);
    assertEquals(100, noOfRowsDeleted);
    int rows = 0;
    for (Result result : ht.getScanner(new Scan())) {
      assertTrue(result.getFamilyMap(FAMILY1).isEmpty());
      assertEquals(1, result.getColumnCells(FAMILY2, QUALIFIER2).size());
      rows++;
    }
    assertEquals(100, rows);
    ht.close();
  }

  // @Ignore @Test
  public void testBulkDeleteColumnVersion() throws Throwable {
    TableName tableName = TableName.valueOf("testBulkDeleteColumnVersion");
    Table ht = createTable(tableName);
    List<Put> puts = new ArrayList<Put>(100);
    for (int j = 0; j < 100; j++) {
      Put put = new Put(Bytes.toBytes(j));
      byte[] value = "v1".getBytes();
      put.add(FAMILY1, QUALIFIER1, 1234L, value);
      put.add(FAMILY1, QUALIFIER2, 1234L, value);
      put.add(FAMILY1, QUALIFIER3, 1234L, value);
      // Latest version values
      value = "v2".getBytes();
      put.add(FAMILY1, QUALIFIER1, value);
      put.add(FAMILY1, QUALIFIER2, value);
      put.add(FAMILY1, QUALIFIER3, value);
      put.add(FAMILY1, null, value);
      puts.add(put);
    }
    ht.put(puts);
    Scan scan = new Scan();
    scan.addFamily(FAMILY1);
    // Delete the latest version values of all the columns in family cf1.
    long noOfRowsDeleted = invokeBulkDeleteProtocol(tableName, scan, 500, DeleteType.VERSION,
        HConstants.LATEST_TIMESTAMP);
    assertEquals(100, noOfRowsDeleted);
    int rows = 0;
    scan = new Scan();
    scan.setMaxVersions();
    for (Result result : ht.getScanner(scan)) {
      assertEquals(3, result.getFamilyMap(FAMILY1).size());
      List<Cell> column = result.getColumnCells(FAMILY1, QUALIFIER1);
      assertEquals(1, column.size());
      assertTrue(CellUtil.matchingValue(column.get(0), "v1".getBytes()));

      column = result.getColumnCells(FAMILY1, QUALIFIER2);
      assertEquals(1, column.size());
      assertTrue(CellUtil.matchingValue(column.get(0), "v1".getBytes()));

      column = result.getColumnCells(FAMILY1, QUALIFIER3);
      assertEquals(1, column.size());
      assertTrue(CellUtil.matchingValue(column.get(0), "v1".getBytes()));
      rows++;
    }
    assertEquals(100, rows);
    ht.close();
  }

  // @Ignore @Test
  public void testBulkDeleteColumnVersionBasedOnTS() throws Throwable {
    TableName tableName = TableName.valueOf("testBulkDeleteColumnVersionBasedOnTS");
    Table ht = createTable(tableName);
    List<Put> puts = new ArrayList<Put>(100);
    for (int j = 0; j < 100; j++) {
      Put put = new Put(Bytes.toBytes(j));
      // TS = 1000L
      byte[] value = "v1".getBytes();
      put.add(FAMILY1, QUALIFIER1, 1000L, value);
      put.add(FAMILY1, QUALIFIER2, 1000L, value);
      put.add(FAMILY1, QUALIFIER3, 1000L, value);
      // TS = 1234L
      value = "v2".getBytes();
      put.add(FAMILY1, QUALIFIER1, 1234L, value);
      put.add(FAMILY1, QUALIFIER2, 1234L, value);
      put.add(FAMILY1, QUALIFIER3, 1234L, value);
      // Latest version values
      value = "v3".getBytes();
      put.add(FAMILY1, QUALIFIER1, value);
      put.add(FAMILY1, QUALIFIER2, value);
      put.add(FAMILY1, QUALIFIER3, value);
      puts.add(put);
    }
    ht.put(puts);
    Scan scan = new Scan();
    scan.addColumn(FAMILY1, QUALIFIER3);
    // Delete the column cf1:c3's one version at TS=1234
    long noOfRowsDeleted = invokeBulkDeleteProtocol(tableName, scan, 500, DeleteType.VERSION, 1234L);
    assertEquals(100, noOfRowsDeleted);
    int rows = 0;
    scan = new Scan();
    scan.setMaxVersions();
    for (Result result : ht.getScanner(scan)) {
      assertEquals(3, result.getFamilyMap(FAMILY1).size());
      assertEquals(3, result.getColumnCells(FAMILY1, QUALIFIER1).size());
      assertEquals(3, result.getColumnCells(FAMILY1, QUALIFIER2).size());
      List<Cell> column = result.getColumnCells(FAMILY1, QUALIFIER3);
      assertEquals(2, column.size());
      assertTrue(CellUtil.matchingValue(column.get(0), "v3".getBytes()));
      assertTrue(CellUtil.matchingValue(column.get(1), "v1".getBytes()));
      rows++;
    }
    assertEquals(100, rows);
    ht.close();
  }

  // @Ignore @Test
  public void testBulkDeleteWithNumberOfVersions() throws Throwable {
    TableName tableName = TableName.valueOf("testBulkDeleteWithNumberOfVersions");
    Table ht = createTable(tableName);
    List<Put> puts = new ArrayList<Put>(100);
    for (int j = 0; j < 100; j++) {
      Put put = new Put(Bytes.toBytes(j));
      // TS = 1000L
      byte[] value = "v1".getBytes();
      put.add(FAMILY1, QUALIFIER1, 1000L, value);
      put.add(FAMILY1, QUALIFIER2, 1000L, value);
      put.add(FAMILY1, QUALIFIER3, 1000L, value);
      // TS = 1234L
      value = "v2".getBytes();
      put.add(FAMILY1, QUALIFIER1, 1234L, value);
      put.add(FAMILY1, QUALIFIER2, 1234L, value);
      put.add(FAMILY1, QUALIFIER3, 1234L, value);
      // TS = 2000L
      value = "v3".getBytes();
      put.add(FAMILY1, QUALIFIER1, 2000L, value);
      put.add(FAMILY1, QUALIFIER2, 2000L, value);
      put.add(FAMILY1, QUALIFIER3, 2000L, value);
      // Latest version values
      value = "v4".getBytes();
      put.add(FAMILY1, QUALIFIER1, value);
      put.add(FAMILY1, QUALIFIER2, value);
      put.add(FAMILY1, QUALIFIER3, value);
      puts.add(put);
    }
    ht.put(puts);

    // Delete all the versions of columns cf1:c1 and cf1:c2 falling with the time range
    // [1000,2000)
    final Scan scan = new Scan();
    scan.addColumn(FAMILY1, QUALIFIER1);
    scan.addColumn(FAMILY1, QUALIFIER2);
    scan.setTimeRange(1000L, 2000L);
    scan.setMaxVersions();

    long noOfDeletedRows = 0L;
    long noOfVersionsDeleted = 0L;
    Batch.Call<BulkDeleteService, BulkDeleteResponse> callable =
      new Batch.Call<BulkDeleteService, BulkDeleteResponse>() {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<BulkDeleteResponse> rpcCallback =
        new BlockingRpcCallback<BulkDeleteResponse>();

      public BulkDeleteResponse call(BulkDeleteService service) throws IOException {
        Builder builder = BulkDeleteRequest.newBuilder();
        builder.setScan(ProtobufUtil.toScan(scan));
        builder.setDeleteType(DeleteType.VERSION);
        builder.setRowBatchSize(500);
        service.delete(controller, builder.build(), rpcCallback);
        return rpcCallback.get();
      }
    };
    Map<byte[], BulkDeleteResponse> result = ht.coprocessorService(BulkDeleteService.class, scan
        .getStartRow(), scan.getStopRow(), callable);
    for (BulkDeleteResponse response : result.values()) {
      noOfDeletedRows += response.getRowsDeleted();
      noOfVersionsDeleted += response.getVersionsDeleted();
    }
    assertEquals(100, noOfDeletedRows);
    assertEquals(400, noOfVersionsDeleted);

    int rows = 0;
    Scan scan1 = new Scan();
    scan1.setMaxVersions();
    for (Result res : ht.getScanner(scan1)) {
      assertEquals(3, res.getFamilyMap(FAMILY1).size());
      List<Cell> column = res.getColumnCells(FAMILY1, QUALIFIER1);
      assertEquals(2, column.size());
      assertTrue(CellUtil.matchingValue(column.get(0), "v4".getBytes()));
      assertTrue(CellUtil.matchingValue(column.get(1), "v3".getBytes()));
      column = res.getColumnCells(FAMILY1, QUALIFIER2);
      assertEquals(2, column.size());
      assertTrue(CellUtil.matchingValue(column.get(0), "v4".getBytes()));
      assertTrue(CellUtil.matchingValue(column.get(1), "v3".getBytes()));
      assertEquals(4, res.getColumnCells(FAMILY1, QUALIFIER3).size());
      rows++;
    }
    assertEquals(100, rows);
    ht.close();
  }

  private Table createTable(TableName tableName) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY1);
    hcd.setMaxVersions(10);// Just setting 10 as I am not testing with more than 10 versions here
    htd.addFamily(hcd);
    TEST_UTIL.getHBaseAdmin().createTable(htd, Bytes.toBytes(0), Bytes.toBytes(120), 5);
    Table ht = TEST_UTIL.getConnection().getTable(tableName);
    return ht;
  }

  private Put createPut(byte[] rowkey, String value) throws IOException {
    Put put = new Put(rowkey);
    put.add(FAMILY1, QUALIFIER1, value.getBytes());
    put.add(FAMILY1, QUALIFIER2, value.getBytes());
    put.add(FAMILY1, QUALIFIER3, value.getBytes());
    return put;
  }
}
