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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MultiRowMutationService;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsResponse;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run tests that use the HBase clients; {@link Table}.
 * Sets up the HBase mini cluster once at start and runs through all client tests.
 * Each creates a table named for the method and does its stuff against that.
 *
 * Parameterized to run with different registry implementations.
 */
@Category({LargeTests.class, ClientTests.class})
@SuppressWarnings ("deprecation")
@RunWith(Parameterized.class)
public class TestFromClientSide5 extends FromClientSideBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestFromClientSide5.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFromClientSide5.class);
  @Rule
  public TableNameTestRule name = new TableNameTestRule();

  // To keep the child classes happy.
  TestFromClientSide5() {}

  public TestFromClientSide5(Class registry, int numHedgedReqs) throws Exception {
    initialize(registry, numHedgedReqs, MultiRowMutationEndpoint.class);
  }

  @Parameterized.Parameters
  public static Collection parameters() {
    return Arrays.asList(new Object[][] {
        { MasterRegistry.class, 1},
        { MasterRegistry.class, 2},
        { ZKConnectionRegistry.class, 1}
    });
  }

  @AfterClass public static void tearDownAfterClass() throws Exception {
    afterClass();
  }

  @Test
  public void testGetClosestRowBefore() throws IOException, InterruptedException {
    final TableName tableName = name.getTableName();
    final byte[] firstRow = Bytes.toBytes("row111");
    final byte[] secondRow = Bytes.toBytes("row222");
    final byte[] thirdRow = Bytes.toBytes("row333");
    final byte[] forthRow = Bytes.toBytes("row444");
    final byte[] beforeFirstRow = Bytes.toBytes("row");
    final byte[] beforeSecondRow = Bytes.toBytes("row22");
    final byte[] beforeThirdRow = Bytes.toBytes("row33");
    final byte[] beforeForthRow = Bytes.toBytes("row44");

    try (Table table =
        TEST_UTIL.createTable(tableName,
          new byte[][] { HConstants.CATALOG_FAMILY, Bytes.toBytes("info2") }, 1, 1024);
      RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {

      // set block size to 64 to making 2 kvs into one block, bypassing the walkForwardInSingleRow
      // in Store.rowAtOrBeforeFromStoreFile
      String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();
      HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegion(regionName);
      Put put1 = new Put(firstRow);
      Put put2 = new Put(secondRow);
      Put put3 = new Put(thirdRow);
      Put put4 = new Put(forthRow);
      byte[] one = new byte[] { 1 };
      byte[] two = new byte[] { 2 };
      byte[] three = new byte[] { 3 };
      byte[] four = new byte[] { 4 };

      put1.addColumn(HConstants.CATALOG_FAMILY, null, one);
      put2.addColumn(HConstants.CATALOG_FAMILY, null, two);
      put3.addColumn(HConstants.CATALOG_FAMILY, null, three);
      put4.addColumn(HConstants.CATALOG_FAMILY, null, four);
      table.put(put1);
      table.put(put2);
      table.put(put3);
      table.put(put4);
      region.flush(true);

      Result result;

      // Test before first that null is returned
      result = getReverseScanResult(table, beforeFirstRow);
      assertNull(result);

      // Test at first that first is returned
      result = getReverseScanResult(table, firstRow);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), firstRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), one));

      // Test in between first and second that first is returned
      result = getReverseScanResult(table, beforeSecondRow);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), firstRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), one));

      // Test at second make sure second is returned
      result = getReverseScanResult(table, secondRow);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), secondRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), two));

      // Test in second and third, make sure second is returned
      result = getReverseScanResult(table, beforeThirdRow);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), secondRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), two));

      // Test at third make sure third is returned
      result = getReverseScanResult(table, thirdRow);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), thirdRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), three));

      // Test in third and forth, make sure third is returned
      result = getReverseScanResult(table, beforeForthRow);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), thirdRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), three));

      // Test at forth make sure forth is returned
      result = getReverseScanResult(table, forthRow);
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), forthRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), four));

      // Test after forth make sure forth is returned
      result = getReverseScanResult(table, Bytes.add(forthRow, one));
      assertTrue(result.containsColumn(HConstants.CATALOG_FAMILY, null));
      assertTrue(Bytes.equals(result.getRow(), forthRow));
      assertTrue(Bytes.equals(result.getValue(HConstants.CATALOG_FAMILY, null), four));
    }
  }

  private Result getReverseScanResult(Table table, byte[] row) throws IOException {
    Scan scan = new Scan(row);
    scan.setSmall(true);
    scan.setReversed(true);
    scan.setCaching(1);
    scan.addFamily(HConstants.CATALOG_FAMILY);
    try (ResultScanner scanner = table.getScanner(scan)) {
      return scanner.next();
    }
  }

  /**
   * For HBASE-2156
   */
  @Test
  public void testScanVariableReuse() {
    Scan scan = new Scan();
    scan.addFamily(FAMILY);
    scan.addColumn(FAMILY, ROW);

    assertEquals(1, scan.getFamilyMap().get(FAMILY).size());

    scan = new Scan();
    scan.addFamily(FAMILY);

    assertNull(scan.getFamilyMap().get(FAMILY));
    assertTrue(scan.getFamilyMap().containsKey(FAMILY));
  }

  @Test
  public void testMultiRowMutation() throws Exception {
    LOG.info("Starting testMultiRowMutation");
    final TableName tableName = name.getTableName();
    final byte [] ROW1 = Bytes.toBytes("testRow1");
    final byte [] ROW2 = Bytes.toBytes("testRow2");
    final byte [] ROW3 = Bytes.toBytes("testRow3");

    try (Table t = TEST_UTIL.createTable(tableName, FAMILY)) {
      // Add initial data
      t.batch(Arrays.asList(
        new Put(ROW1).addColumn(FAMILY, QUALIFIER, VALUE),
        new Put(ROW2).addColumn(FAMILY, QUALIFIER, Bytes.toBytes(1L)),
        new Put(ROW3).addColumn(FAMILY, QUALIFIER, VALUE)
      ), new Object[3]);

      // Execute MultiRowMutation
      Put put = new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE);
      MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put);

      Delete delete = new Delete(ROW1);
      MutationProto m2 = ProtobufUtil.toMutation(MutationType.DELETE, delete);

      Increment increment = new Increment(ROW2).addColumn(FAMILY, QUALIFIER, 1L);
      MutationProto m3 = ProtobufUtil.toMutation(MutationType.INCREMENT, increment);

      Append append = new Append(ROW3).addColumn(FAMILY, QUALIFIER, VALUE);
      MutationProto m4 = ProtobufUtil.toMutation(MutationType.APPEND, append);

      MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
      mrmBuilder.addMutationRequest(m1);
      mrmBuilder.addMutationRequest(m2);
      mrmBuilder.addMutationRequest(m3);
      mrmBuilder.addMutationRequest(m4);

      CoprocessorRpcChannel channel = t.coprocessorService(ROW);
      MultiRowMutationService.BlockingInterface service =
              MultiRowMutationService.newBlockingStub(channel);
      MutateRowsResponse response = service.mutateRows(null, mrmBuilder.build());

      // Assert
      assertTrue(response.getProcessed());

      Result r = t.get(new Get(ROW));
      assertEquals(Bytes.toString(VALUE), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));

      r = t.get(new Get(ROW1));
      assertTrue(r.isEmpty());

      r = t.get(new Get(ROW2));
      assertEquals(2L, Bytes.toLong(r.getValue(FAMILY, QUALIFIER)));

      r = t.get(new Get(ROW3));
      assertEquals(Bytes.toString(VALUE) + Bytes.toString(VALUE),
        Bytes.toString(r.getValue(FAMILY, QUALIFIER)));
    }
  }

  @Test
  public void testMultiRowMutationWithSingleConditionWhenConditionMatches() throws Exception {
    final TableName tableName = name.getTableName();
    final byte [] ROW1 = Bytes.toBytes("testRow1");
    final byte [] ROW2 = Bytes.toBytes("testRow2");
    final byte [] VALUE1 = Bytes.toBytes("testValue1");
    final byte [] VALUE2 = Bytes.toBytes("testValue2");

    try (Table t = TEST_UTIL.createTable(tableName, FAMILY)) {
      // Add initial data
      t.put(new Put(ROW2).addColumn(FAMILY, QUALIFIER, VALUE2));

      // Execute MultiRowMutation with conditions
      Put put1 = new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE);
      MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put1);
      Put put2 = new Put(ROW1).addColumn(FAMILY, QUALIFIER, VALUE1);
      MutationProto m2 = ProtobufUtil.toMutation(MutationType.PUT, put2);
      Delete delete = new Delete(ROW2);
      MutationProto m3 = ProtobufUtil.toMutation(MutationType.DELETE, delete);

      MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
      mrmBuilder.addMutationRequest(m1);
      mrmBuilder.addMutationRequest(m2);
      mrmBuilder.addMutationRequest(m3);
      mrmBuilder.addCondition(ProtobufUtil.toCondition(ROW2, FAMILY, QUALIFIER,
        CompareOperator.EQUAL, VALUE2, null));

      CoprocessorRpcChannel channel = t.coprocessorService(ROW);
      MultiRowMutationService.BlockingInterface service =
        MultiRowMutationService.newBlockingStub(channel);
      MutateRowsResponse response = service.mutateRows(null, mrmBuilder.build());

      // Assert
      assertTrue(response.getProcessed());

      Result r = t.get(new Get(ROW));
      assertEquals(Bytes.toString(VALUE), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));

      r = t.get(new Get(ROW1));
      assertEquals(Bytes.toString(VALUE1), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));

      r = t.get(new Get(ROW2));
      assertTrue(r.isEmpty());
    }
  }

  @Test
  public void testMultiRowMutationWithSingleConditionWhenConditionNotMatch() throws Exception {
    final TableName tableName = name.getTableName();
    final byte [] ROW1 = Bytes.toBytes("testRow1");
    final byte [] ROW2 = Bytes.toBytes("testRow2");
    final byte [] VALUE1 = Bytes.toBytes("testValue1");
    final byte [] VALUE2 = Bytes.toBytes("testValue2");

    try (Table t = TEST_UTIL.createTable(tableName, FAMILY)) {
      // Add initial data
      t.put(new Put(ROW2).addColumn(FAMILY, QUALIFIER, VALUE2));

      // Execute MultiRowMutation with conditions
      Put put1 = new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE);
      MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put1);
      Put put2 = new Put(ROW1).addColumn(FAMILY, QUALIFIER, VALUE1);
      MutationProto m2 = ProtobufUtil.toMutation(MutationType.PUT, put2);
      Delete delete = new Delete(ROW2);
      MutationProto m3 = ProtobufUtil.toMutation(MutationType.DELETE, delete);

      MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
      mrmBuilder.addMutationRequest(m1);
      mrmBuilder.addMutationRequest(m2);
      mrmBuilder.addMutationRequest(m3);
      mrmBuilder.addCondition(ProtobufUtil.toCondition(ROW2, FAMILY, QUALIFIER,
        CompareOperator.EQUAL, VALUE1, null));

      CoprocessorRpcChannel channel = t.coprocessorService(ROW);
      MultiRowMutationService.BlockingInterface service =
        MultiRowMutationService.newBlockingStub(channel);
      MutateRowsResponse response = service.mutateRows(null, mrmBuilder.build());

      // Assert
      assertFalse(response.getProcessed());

      Result r = t.get(new Get(ROW));
      assertTrue(r.isEmpty());

      r = t.get(new Get(ROW1));
      assertTrue(r.isEmpty());

      r = t.get(new Get(ROW2));
      assertEquals(Bytes.toString(VALUE2), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));
    }
  }

  @Test
  public void testMultiRowMutationWithMultipleConditionsWhenConditionsMatch() throws Exception {
    final TableName tableName = name.getTableName();
    final byte [] ROW1 = Bytes.toBytes("testRow1");
    final byte [] ROW2 = Bytes.toBytes("testRow2");
    final byte [] VALUE1 = Bytes.toBytes("testValue1");
    final byte [] VALUE2 = Bytes.toBytes("testValue2");

    try (Table t = TEST_UTIL.createTable(tableName, FAMILY)) {
      // Add initial data
      t.put(new Put(ROW2).addColumn(FAMILY, QUALIFIER, VALUE2));

      // Execute MultiRowMutation with conditions
      Put put1 = new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE);
      MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put1);
      Put put2 = new Put(ROW1).addColumn(FAMILY, QUALIFIER, VALUE1);
      MutationProto m2 = ProtobufUtil.toMutation(MutationType.PUT, put2);
      Delete delete = new Delete(ROW2);
      MutationProto m3 = ProtobufUtil.toMutation(MutationType.DELETE, delete);

      MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
      mrmBuilder.addMutationRequest(m1);
      mrmBuilder.addMutationRequest(m2);
      mrmBuilder.addMutationRequest(m3);
      mrmBuilder.addCondition(ProtobufUtil.toCondition(ROW, FAMILY, QUALIFIER,
        CompareOperator.EQUAL, null, null));
      mrmBuilder.addCondition(ProtobufUtil.toCondition(ROW2, FAMILY, QUALIFIER,
        CompareOperator.EQUAL, VALUE2, null));

      CoprocessorRpcChannel channel = t.coprocessorService(ROW);
      MultiRowMutationService.BlockingInterface service =
        MultiRowMutationService.newBlockingStub(channel);
      MutateRowsResponse response = service.mutateRows(null, mrmBuilder.build());

      // Assert
      assertTrue(response.getProcessed());

      Result r = t.get(new Get(ROW));
      assertEquals(Bytes.toString(VALUE), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));

      r = t.get(new Get(ROW1));
      assertEquals(Bytes.toString(VALUE1), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));

      r = t.get(new Get(ROW2));
      assertTrue(r.isEmpty());
    }
  }

  @Test
  public void testMultiRowMutationWithMultipleConditionsWhenConditionsNotMatch() throws Exception {
    final TableName tableName = name.getTableName();
    final byte [] ROW1 = Bytes.toBytes("testRow1");
    final byte [] ROW2 = Bytes.toBytes("testRow2");
    final byte [] VALUE1 = Bytes.toBytes("testValue1");
    final byte [] VALUE2 = Bytes.toBytes("testValue2");

    try (Table t = TEST_UTIL.createTable(tableName, FAMILY)) {
      // Add initial data
      t.put(new Put(ROW2).addColumn(FAMILY, QUALIFIER, VALUE2));

      // Execute MultiRowMutation with conditions
      Put put1 = new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE);
      MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put1);
      Put put2 = new Put(ROW1).addColumn(FAMILY, QUALIFIER, VALUE1);
      MutationProto m2 = ProtobufUtil.toMutation(MutationType.PUT, put2);
      Delete delete = new Delete(ROW2);
      MutationProto m3 = ProtobufUtil.toMutation(MutationType.DELETE, delete);

      MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
      mrmBuilder.addMutationRequest(m1);
      mrmBuilder.addMutationRequest(m2);
      mrmBuilder.addMutationRequest(m3);
      mrmBuilder.addCondition(ProtobufUtil.toCondition(ROW1, FAMILY, QUALIFIER,
        CompareOperator.EQUAL, null, null));
      mrmBuilder.addCondition(ProtobufUtil.toCondition(ROW2, FAMILY, QUALIFIER,
        CompareOperator.EQUAL, VALUE1, null));

      CoprocessorRpcChannel channel = t.coprocessorService(ROW);
      MultiRowMutationService.BlockingInterface service =
        MultiRowMutationService.newBlockingStub(channel);
      MutateRowsResponse response = service.mutateRows(null, mrmBuilder.build());

      // Assert
      assertFalse(response.getProcessed());

      Result r = t.get(new Get(ROW));
      assertTrue(r.isEmpty());

      r = t.get(new Get(ROW1));
      assertTrue(r.isEmpty());

      r = t.get(new Get(ROW2));
      assertEquals(Bytes.toString(VALUE2), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));
    }
  }

  @Test
  public void testMultiRowMutationWithFilterConditionWhenConditionMatches() throws Exception {
    final TableName tableName = name.getTableName();
    final byte [] ROW1 = Bytes.toBytes("testRow1");
    final byte [] ROW2 = Bytes.toBytes("testRow2");
    final byte [] QUALIFIER2 = Bytes.toBytes("testQualifier2");
    final byte [] VALUE1 = Bytes.toBytes("testValue1");
    final byte [] VALUE2 = Bytes.toBytes("testValue2");
    final byte [] VALUE3 = Bytes.toBytes("testValue3");

    try (Table t = TEST_UTIL.createTable(tableName, FAMILY)) {
      // Add initial data
      t.put(new Put(ROW2).addColumn(FAMILY, QUALIFIER, VALUE2)
        .addColumn(FAMILY, QUALIFIER2, VALUE3));

      // Execute MultiRowMutation with conditions
      Put put1 = new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE);
      MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put1);
      Put put2 = new Put(ROW1).addColumn(FAMILY, QUALIFIER, VALUE1);
      MutationProto m2 = ProtobufUtil.toMutation(MutationType.PUT, put2);
      Delete delete = new Delete(ROW2);
      MutationProto m3 = ProtobufUtil.toMutation(MutationType.DELETE, delete);

      MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
      mrmBuilder.addMutationRequest(m1);
      mrmBuilder.addMutationRequest(m2);
      mrmBuilder.addMutationRequest(m3);
      mrmBuilder.addCondition(ProtobufUtil.toCondition(ROW2, new FilterList(
        new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOperator.EQUAL, VALUE2),
        new SingleColumnValueFilter(FAMILY, QUALIFIER2, CompareOperator.EQUAL, VALUE3)), null));

      CoprocessorRpcChannel channel = t.coprocessorService(ROW);
      MultiRowMutationService.BlockingInterface service =
        MultiRowMutationService.newBlockingStub(channel);
      MutateRowsResponse response = service.mutateRows(null, mrmBuilder.build());

      // Assert
      assertTrue(response.getProcessed());

      Result r = t.get(new Get(ROW));
      assertEquals(Bytes.toString(VALUE), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));

      r = t.get(new Get(ROW1));
      assertEquals(Bytes.toString(VALUE1), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));

      r = t.get(new Get(ROW2));
      assertTrue(r.isEmpty());
    }
  }

  @Test
  public void testMultiRowMutationWithFilterConditionWhenConditionNotMatch() throws Exception {
    final TableName tableName = name.getTableName();
    final byte [] ROW1 = Bytes.toBytes("testRow1");
    final byte [] ROW2 = Bytes.toBytes("testRow2");
    final byte [] QUALIFIER2 = Bytes.toBytes("testQualifier2");
    final byte [] VALUE1 = Bytes.toBytes("testValue1");
    final byte [] VALUE2 = Bytes.toBytes("testValue2");
    final byte [] VALUE3 = Bytes.toBytes("testValue3");

    try (Table t = TEST_UTIL.createTable(tableName, FAMILY)) {
      // Add initial data
      t.put(new Put(ROW2).addColumn(FAMILY, QUALIFIER, VALUE2)
        .addColumn(FAMILY, QUALIFIER2, VALUE3));

      // Execute MultiRowMutation with conditions
      Put put1 = new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE);
      MutationProto m1 = ProtobufUtil.toMutation(MutationType.PUT, put1);
      Put put2 = new Put(ROW1).addColumn(FAMILY, QUALIFIER, VALUE1);
      MutationProto m2 = ProtobufUtil.toMutation(MutationType.PUT, put2);
      Delete delete = new Delete(ROW2);
      MutationProto m3 = ProtobufUtil.toMutation(MutationType.DELETE, delete);

      MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();
      mrmBuilder.addMutationRequest(m1);
      mrmBuilder.addMutationRequest(m2);
      mrmBuilder.addMutationRequest(m3);
      mrmBuilder.addCondition(ProtobufUtil.toCondition(ROW2, new FilterList(
        new SingleColumnValueFilter(FAMILY, QUALIFIER, CompareOperator.EQUAL, VALUE2),
        new SingleColumnValueFilter(FAMILY, QUALIFIER2, CompareOperator.EQUAL, VALUE2)), null));

      CoprocessorRpcChannel channel = t.coprocessorService(ROW);
      MultiRowMutationService.BlockingInterface service =
        MultiRowMutationService.newBlockingStub(channel);
      MutateRowsResponse response = service.mutateRows(null, mrmBuilder.build());

      // Assert
      assertFalse(response.getProcessed());

      Result r = t.get(new Get(ROW));
      assertTrue(r.isEmpty());

      r = t.get(new Get(ROW1));
      assertTrue(r.isEmpty());

      r = t.get(new Get(ROW2));
      assertEquals(Bytes.toString(VALUE2), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));
    }
  }

  @Test
  public void testRowMutations() throws Exception {
    LOG.info("Starting testRowMutations");
    final TableName tableName = name.getTableName();
    try (Table t = TEST_UTIL.createTable(tableName, FAMILY)) {
      byte[][] QUALIFIERS = new byte[][] { Bytes.toBytes("a"), Bytes.toBytes("b"),
        Bytes.toBytes("c"), Bytes.toBytes("d") };

      // Test for Put operations
      RowMutations arm = new RowMutations(ROW);
      Put p = new Put(ROW);
      p.addColumn(FAMILY, QUALIFIERS[0], VALUE);
      arm.add(p);
      Result r = t.mutateRow(arm);
      assertTrue(r.getExists());
      assertTrue(r.isEmpty());

      Get g = new Get(ROW);
      r = t.get(g);
      assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[0])));

      // Test for Put and Delete operations
      arm = new RowMutations(ROW);
      p = new Put(ROW);
      p.addColumn(FAMILY, QUALIFIERS[1], VALUE);
      arm.add(p);
      Delete d = new Delete(ROW);
      d.addColumns(FAMILY, QUALIFIERS[0]);
      arm.add(d);
      // TODO: Trying mutateRow again. The batch was failing with a one try only.
      r = t.mutateRow(arm);
      assertTrue(r.getExists());
      assertTrue(r.isEmpty());

      r = t.get(g);
      assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[1])));
      assertNull(r.getValue(FAMILY, QUALIFIERS[0]));

      // Test for Increment and Append operations
      arm = new RowMutations(ROW);
      arm.add(Arrays.asList(
        new Put(ROW).addColumn(FAMILY, QUALIFIERS[0], VALUE),
        new Delete(ROW).addColumns(FAMILY, QUALIFIERS[1]),
        new Increment(ROW).addColumn(FAMILY, QUALIFIERS[2], 5L),
        new Append(ROW).addColumn(FAMILY, QUALIFIERS[3], Bytes.toBytes("abc"))
      ));
      r = t.mutateRow(arm);
      assertTrue(r.getExists());
      assertEquals(5L, Bytes.toLong(r.getValue(FAMILY, QUALIFIERS[2])));
      assertEquals("abc", Bytes.toString(r.getValue(FAMILY, QUALIFIERS[3])));

      g = new Get(ROW);
      r = t.get(g);
      assertEquals(0, Bytes.compareTo(VALUE, r.getValue(FAMILY, QUALIFIERS[0])));
      assertNull(r.getValue(FAMILY, QUALIFIERS[1]));
      assertEquals(5L, Bytes.toLong(r.getValue(FAMILY, QUALIFIERS[2])));
      assertEquals("abc", Bytes.toString(r.getValue(FAMILY, QUALIFIERS[3])));

      // Test that we get a region level exception
      try {
        arm = new RowMutations(ROW);
        p = new Put(ROW);
        p.addColumn(new byte[] { 'b', 'o', 'g', 'u', 's' }, QUALIFIERS[0], VALUE);
        arm.add(p);
        t.mutateRow(arm);
        fail("Expected NoSuchColumnFamilyException");
      } catch (NoSuchColumnFamilyException e) {
        return;
      } catch (RetriesExhaustedWithDetailsException e) {
        for (Throwable rootCause : e.getCauses()) {
          if (rootCause instanceof NoSuchColumnFamilyException) {
            return;
          }
        }
        throw e;
      }
    }
  }

  @Test
  public void testBatchAppendWithReturnResultFalse() throws Exception {
    LOG.info("Starting testBatchAppendWithReturnResultFalse");
    final TableName tableName = name.getTableName();
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY)) {
      Append append1 = new Append(Bytes.toBytes("row1"));
      append1.setReturnResults(false);
      append1.addColumn(FAMILY, Bytes.toBytes("f1"), Bytes.toBytes("value1"));
      Append append2 = new Append(Bytes.toBytes("row1"));
      append2.setReturnResults(false);
      append2.addColumn(FAMILY, Bytes.toBytes("f1"), Bytes.toBytes("value2"));
      List<Append> appends = new ArrayList<>();
      appends.add(append1);
      appends.add(append2);
      Object[] results = new Object[2];
      table.batch(appends, results);
      assertEquals(2, results.length);
      for (Object r : results) {
        Result result = (Result) r;
        assertTrue(result.isEmpty());
      }
    }
  }

  @Test
  public void testAppend() throws Exception {
    LOG.info("Starting testAppend");
    final TableName tableName = name.getTableName();
    try (Table t = TEST_UTIL.createTable(tableName, FAMILY)) {
      byte[] v1 = Bytes.toBytes("42");
      byte[] v2 = Bytes.toBytes("23");
      byte[][] QUALIFIERS = new byte[][]{
              Bytes.toBytes("b"), Bytes.toBytes("a"), Bytes.toBytes("c")
      };
      Append a = new Append(ROW);
      a.addColumn(FAMILY, QUALIFIERS[0], v1);
      a.addColumn(FAMILY, QUALIFIERS[1], v2);
      a.setReturnResults(false);
      assertEmptyResult(t.append(a));

      a = new Append(ROW);
      a.addColumn(FAMILY, QUALIFIERS[0], v2);
      a.addColumn(FAMILY, QUALIFIERS[1], v1);
      a.addColumn(FAMILY, QUALIFIERS[2], v2);
      Result r = t.append(a);
      assertEquals(0, Bytes.compareTo(Bytes.add(v1, v2), r.getValue(FAMILY, QUALIFIERS[0])));
      assertEquals(0, Bytes.compareTo(Bytes.add(v2, v1), r.getValue(FAMILY, QUALIFIERS[1])));
      // QUALIFIERS[2] previously not exist, verify both value and timestamp are correct
      assertEquals(0, Bytes.compareTo(v2, r.getValue(FAMILY, QUALIFIERS[2])));
      assertEquals(r.getColumnLatestCell(FAMILY, QUALIFIERS[0]).getTimestamp(),
              r.getColumnLatestCell(FAMILY, QUALIFIERS[2]).getTimestamp());
    }
  }
  private List<Result> doAppend(final boolean walUsed) throws IOException {
    LOG.info("Starting testAppend, walUsed is " + walUsed);
    final TableName TABLENAME =
            TableName.valueOf(walUsed ? "testAppendWithWAL" : "testAppendWithoutWAL");
    try (Table t = TEST_UTIL.createTable(TABLENAME, FAMILY)) {
      final byte[] row1 = Bytes.toBytes("c");
      final byte[] row2 = Bytes.toBytes("b");
      final byte[] row3 = Bytes.toBytes("a");
      final byte[] qual = Bytes.toBytes("qual");
      Put put_0 = new Put(row2);
      put_0.addColumn(FAMILY, qual, Bytes.toBytes("put"));
      Put put_1 = new Put(row3);
      put_1.addColumn(FAMILY, qual, Bytes.toBytes("put"));
      Append append_0 = new Append(row1);
      append_0.addColumn(FAMILY, qual, Bytes.toBytes("i"));
      Append append_1 = new Append(row1);
      append_1.addColumn(FAMILY, qual, Bytes.toBytes("k"));
      Append append_2 = new Append(row1);
      append_2.addColumn(FAMILY, qual, Bytes.toBytes("e"));
      if (!walUsed) {
        append_2.setDurability(Durability.SKIP_WAL);
      }
      Append append_3 = new Append(row1);
      append_3.addColumn(FAMILY, qual, Bytes.toBytes("a"));
      Scan s = new Scan();
      s.setCaching(1);
      t.append(append_0);
      t.put(put_0);
      t.put(put_1);
      List<Result> results = new LinkedList<>();
      try (ResultScanner scanner = t.getScanner(s)) {
        t.append(append_1);
        t.append(append_2);
        t.append(append_3);
        for (Result r : scanner) {
          results.add(r);
        }
      }
      TEST_UTIL.deleteTable(TABLENAME);
      return results;
    }
  }

  @Test
  public void testAppendWithoutWAL() throws Exception {
    List<Result> resultsWithWal = doAppend(true);
    List<Result> resultsWithoutWal = doAppend(false);
    assertEquals(resultsWithWal.size(), resultsWithoutWal.size());
    for (int i = 0; i != resultsWithWal.size(); ++i) {
      Result resultWithWal = resultsWithWal.get(i);
      Result resultWithoutWal = resultsWithoutWal.get(i);
      assertEquals(resultWithWal.rawCells().length, resultWithoutWal.rawCells().length);
      for (int j = 0; j != resultWithWal.rawCells().length; ++j) {
        Cell cellWithWal = resultWithWal.rawCells()[j];
        Cell cellWithoutWal = resultWithoutWal.rawCells()[j];
        assertArrayEquals(CellUtil.cloneRow(cellWithWal), CellUtil.cloneRow(cellWithoutWal));
        assertArrayEquals(CellUtil.cloneFamily(cellWithWal), CellUtil.cloneFamily(cellWithoutWal));
        assertArrayEquals(CellUtil.cloneQualifier(cellWithWal),
          CellUtil.cloneQualifier(cellWithoutWal));
        assertArrayEquals(CellUtil.cloneValue(cellWithWal), CellUtil.cloneValue(cellWithoutWal));
      }
    }
  }

  @Test
  public void testClientPoolRoundRobin() throws IOException {
    final TableName tableName = name.getTableName();

    int poolSize = 3;
    int numVersions = poolSize * 2;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_CLIENT_IPC_POOL_TYPE, "round-robin");
    conf.setInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, poolSize);

    try (Table table =
                 TEST_UTIL.createTable(tableName, new byte[][] { FAMILY }, Integer.MAX_VALUE)) {

      final long ts = EnvironmentEdgeManager.currentTime();
      Get get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readAllVersions();

      for (int versions = 1; versions <= numVersions; versions++) {
        Put put = new Put(ROW);
        put.addColumn(FAMILY, QUALIFIER, ts + versions, VALUE);
        table.put(put);

        Result result = table.get(get);
        NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY)
                .get(QUALIFIER);

        assertEquals("The number of versions of '" + Bytes.toString(FAMILY) + ":"
                + Bytes.toString(QUALIFIER) + " did not match", versions, navigableMap.size());
        for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
          assertTrue("The value at time " + entry.getKey()
                          + " did not match what was put",
                  Bytes.equals(VALUE, entry.getValue()));
        }
      }
    }
  }

  @Ignore ("Flakey: HBASE-8989") @Test
  public void testClientPoolThreadLocal() throws IOException {
    final TableName tableName = name.getTableName();

    int poolSize = Integer.MAX_VALUE;
    int numVersions = 3;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(HConstants.HBASE_CLIENT_IPC_POOL_TYPE, "thread-local");
    conf.setInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, poolSize);

    try (final Table table = TEST_UTIL.createTable(tableName, new byte[][] { FAMILY },  3)) {

      final long ts = EnvironmentEdgeManager.currentTime();
      final Get get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readAllVersions();

      for (int versions = 1; versions <= numVersions; versions++) {
        Put put = new Put(ROW);
        put.addColumn(FAMILY, QUALIFIER, ts + versions, VALUE);
        table.put(put);

        Result result = table.get(get);
        NavigableMap<Long, byte[]> navigableMap = result.getMap().get(FAMILY)
                .get(QUALIFIER);

        assertEquals("The number of versions of '" + Bytes.toString(FAMILY) + ":"
                + Bytes.toString(QUALIFIER) + " did not match", versions, navigableMap.size());
        for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
          assertTrue("The value at time " + entry.getKey()
                          + " did not match what was put",
                  Bytes.equals(VALUE, entry.getValue()));
        }
      }

      final Object waitLock = new Object();
      ExecutorService executorService = Executors.newFixedThreadPool(numVersions);
      final AtomicReference<AssertionError> error = new AtomicReference<>(null);
      for (int versions = numVersions; versions < numVersions * 2; versions++) {
        final int versionsCopy = versions;
        executorService.submit((Callable<Void>) () -> {
          try {
            Put put = new Put(ROW);
            put.addColumn(FAMILY, QUALIFIER, ts + versionsCopy, VALUE);
            table.put(put);

            Result result = table.get(get);
            NavigableMap<Long, byte[]> navigableMap = result.getMap()
                    .get(FAMILY).get(QUALIFIER);

            assertEquals("The number of versions of '" + Bytes.toString(FAMILY) + ":"
                    + Bytes.toString(QUALIFIER) + " did not match " + versionsCopy, versionsCopy,
                    navigableMap.size());
            for (Map.Entry<Long, byte[]> entry : navigableMap.entrySet()) {
              assertTrue("The value at time " + entry.getKey()
                              + " did not match what was put",
                      Bytes.equals(VALUE, entry.getValue()));
            }
            synchronized (waitLock) {
              waitLock.wait();
            }
          } catch (Exception ignored) {
          } catch (AssertionError e) {
            // the error happens in a thread, it won't fail the test,
            // need to pass it to the caller for proper handling.
            error.set(e);
            LOG.error(e.toString(), e);
          }

          return null;
        });
      }
      synchronized (waitLock) {
        waitLock.notifyAll();
      }
      executorService.shutdownNow();
      assertNull(error.get());
    }
  }

  @Test
  public void testCheckAndPut() throws IOException {
    final byte [] anotherrow = Bytes.toBytes("anotherrow");
    final byte [] value2 = Bytes.toBytes("abcd");

    try (Table table = TEST_UTIL.createTable(name.getTableName(), FAMILY)) {
      Put put1 = new Put(ROW);
      put1.addColumn(FAMILY, QUALIFIER, VALUE);

      // row doesn't exist, so using non-null value should be considered "not match".
      boolean ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifEquals(VALUE).thenPut(put1);
      assertFalse(ok);

      // row doesn't exist, so using "ifNotExists" should be considered "match".
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).ifNotExists().thenPut(put1);
      assertTrue(ok);

      // row now exists, so using "ifNotExists" should be considered "not match".
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).ifNotExists().thenPut(put1);
      assertFalse(ok);

      Put put2 = new Put(ROW);
      put2.addColumn(FAMILY, QUALIFIER, value2);

      // row now exists, use the matching value to check
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).ifEquals(VALUE).thenPut(put2);
      assertTrue(ok);

      Put put3 = new Put(anotherrow);
      put3.addColumn(FAMILY, QUALIFIER, VALUE);

      // try to do CheckAndPut on different rows
      try {
        table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).ifEquals(value2).thenPut(put3);
        fail("trying to check and modify different rows should have failed.");
      } catch (Exception ignored) {
      }
    }
  }

  @Test
  public void testCheckAndMutateWithTimeRange() throws IOException {
    try (Table table = TEST_UTIL.createTable(name.getTableName(), FAMILY)) {
      final long ts = System.currentTimeMillis() / 2;
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, ts, VALUE);

      boolean ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifNotExists()
              .thenPut(put);
      assertTrue(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.at(ts + 10000))
              .ifEquals(VALUE)
              .thenPut(put);
      assertFalse(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.from(ts + 10000))
              .ifEquals(VALUE)
              .thenPut(put);
      assertFalse(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.between(ts + 10000, ts + 20000))
              .ifEquals(VALUE)
              .thenPut(put);
      assertFalse(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.until(ts))
              .ifEquals(VALUE)
              .thenPut(put);
      assertFalse(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.at(ts))
              .ifEquals(VALUE)
              .thenPut(put);
      assertTrue(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.from(ts))
              .ifEquals(VALUE)
              .thenPut(put);
      assertTrue(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.between(ts, ts + 20000))
              .ifEquals(VALUE)
              .thenPut(put);
      assertTrue(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.until(ts + 10000))
              .ifEquals(VALUE)
              .thenPut(put);
      assertTrue(ok);

      RowMutations rm = new RowMutations(ROW)
              .add((Mutation) put);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.at(ts + 10000))
              .ifEquals(VALUE)
              .thenMutate(rm);
      assertFalse(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.at(ts))
              .ifEquals(VALUE)
              .thenMutate(rm);
      assertTrue(ok);

      Delete delete = new Delete(ROW)
              .addColumn(FAMILY, QUALIFIER);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.at(ts + 10000))
              .ifEquals(VALUE)
              .thenDelete(delete);
      assertFalse(ok);

      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .timeRange(TimeRange.at(ts))
              .ifEquals(VALUE)
              .thenDelete(delete);
      assertTrue(ok);
    }
  }

  @Test
  public void testCheckAndPutWithCompareOp() throws IOException {
    final byte [] value1 = Bytes.toBytes("aaaa");
    final byte [] value2 = Bytes.toBytes("bbbb");
    final byte [] value3 = Bytes.toBytes("cccc");
    final byte [] value4 = Bytes.toBytes("dddd");

    try (Table table = TEST_UTIL.createTable(name.getTableName(), FAMILY)) {

      Put put2 = new Put(ROW);
      put2.addColumn(FAMILY, QUALIFIER, value2);

      Put put3 = new Put(ROW);
      put3.addColumn(FAMILY, QUALIFIER, value3);

      // row doesn't exist, so using "ifNotExists" should be considered "match".
      boolean ok =
              table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER).ifNotExists().thenPut(put2);
      assertTrue(ok);

      // cell = "bbbb", using "aaaa" to compare only LESS/LESS_OR_EQUAL/NOT_EQUAL
      // turns out "match"
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER, value1).thenPut(put2);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.EQUAL, value1).thenPut(put2);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER_OR_EQUAL, value1).thenPut(put2);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS, value1).thenPut(put2);
      assertTrue(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS_OR_EQUAL, value1).thenPut(put2);
      assertTrue(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.NOT_EQUAL, value1).thenPut(put3);
      assertTrue(ok);

      // cell = "cccc", using "dddd" to compare only LARGER/LARGER_OR_EQUAL/NOT_EQUAL
      // turns out "match"
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS, value4).thenPut(put3);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS_OR_EQUAL, value4).thenPut(put3);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.EQUAL, value4).thenPut(put3);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER, value4).thenPut(put3);
      assertTrue(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER_OR_EQUAL, value4).thenPut(put3);
      assertTrue(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.NOT_EQUAL, value4).thenPut(put2);
      assertTrue(ok);

      // cell = "bbbb", using "bbbb" to compare only GREATER_OR_EQUAL/LESS_OR_EQUAL/EQUAL
      // turns out "match"
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER, value2).thenPut(put2);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.NOT_EQUAL, value2).thenPut(put2);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS, value2).thenPut(put2);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER_OR_EQUAL, value2).thenPut(put2);
      assertTrue(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS_OR_EQUAL, value2).thenPut(put2);
      assertTrue(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.EQUAL, value2).thenPut(put3);
      assertTrue(ok);
    }
  }

  @Test
  public void testCheckAndDelete() throws IOException {
    final byte [] value1 = Bytes.toBytes("aaaa");

    try (Table table = TEST_UTIL.createTable(name.getTableName(),
        FAMILY)) {

      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, value1);
      table.put(put);

      Delete delete = new Delete(ROW);
      delete.addColumns(FAMILY, QUALIFIER);

      boolean ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifEquals(value1).thenDelete(delete);
      assertTrue(ok);
    }
  }

  @Test
  public void testCheckAndDeleteWithCompareOp() throws IOException {
    final byte [] value1 = Bytes.toBytes("aaaa");
    final byte [] value2 = Bytes.toBytes("bbbb");
    final byte [] value3 = Bytes.toBytes("cccc");
    final byte [] value4 = Bytes.toBytes("dddd");

    try (Table table = TEST_UTIL.createTable(name.getTableName(),
        FAMILY)) {

      Put put2 = new Put(ROW);
      put2.addColumn(FAMILY, QUALIFIER, value2);
      table.put(put2);

      Put put3 = new Put(ROW);
      put3.addColumn(FAMILY, QUALIFIER, value3);

      Delete delete = new Delete(ROW);
      delete.addColumns(FAMILY, QUALIFIER);

      // cell = "bbbb", using "aaaa" to compare only LESS/LESS_OR_EQUAL/NOT_EQUAL
      // turns out "match"
      boolean ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER, value1).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.EQUAL, value1).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER_OR_EQUAL, value1).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS, value1).thenDelete(delete);
      assertTrue(ok);
      table.put(put2);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS_OR_EQUAL, value1).thenDelete(delete);
      assertTrue(ok);
      table.put(put2);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.NOT_EQUAL, value1).thenDelete(delete);
      assertTrue(ok);

      // cell = "cccc", using "dddd" to compare only LARGER/LARGER_OR_EQUAL/NOT_EQUAL
      // turns out "match"
      table.put(put3);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS, value4).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS_OR_EQUAL, value4).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.EQUAL, value4).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER, value4).thenDelete(delete);
      assertTrue(ok);
      table.put(put3);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER_OR_EQUAL, value4).thenDelete(delete);
      assertTrue(ok);
      table.put(put3);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.NOT_EQUAL, value4).thenDelete(delete);
      assertTrue(ok);

      // cell = "bbbb", using "bbbb" to compare only GREATER_OR_EQUAL/LESS_OR_EQUAL/EQUAL
      // turns out "match"
      table.put(put2);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER, value2).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.NOT_EQUAL, value2).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS, value2).thenDelete(delete);
      assertFalse(ok);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.GREATER_OR_EQUAL, value2).thenDelete(delete);
      assertTrue(ok);
      table.put(put2);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.LESS_OR_EQUAL, value2).thenDelete(delete);
      assertTrue(ok);
      table.put(put2);
      ok = table.checkAndMutate(ROW, FAMILY).qualifier(QUALIFIER)
              .ifMatches(CompareOperator.EQUAL, value2).thenDelete(delete);
      assertTrue(ok);
    }
  }

  /**
  * Test ScanMetrics
  */
  @Test
  @SuppressWarnings({"unused", "checkstyle:EmptyBlock"})
  public void testScanMetrics() throws Exception {
    final TableName tableName = name.getTableName();

    // Set up test table:
    // Create table:
    try (Table ht = TEST_UTIL.createMultiRegionTable(tableName, FAMILY)) {
      int numOfRegions;
      try (RegionLocator r = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
        numOfRegions = r.getStartKeys().length;
      }
      // Create 3 rows in the table, with rowkeys starting with "zzz*" so that
      // scan are forced to hit all the regions.
      Put put1 = new Put(Bytes.toBytes("zzz1"));
      put1.addColumn(FAMILY, QUALIFIER, VALUE);
      Put put2 = new Put(Bytes.toBytes("zzz2"));
      put2.addColumn(FAMILY, QUALIFIER, VALUE);
      Put put3 = new Put(Bytes.toBytes("zzz3"));
      put3.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(Arrays.asList(put1, put2, put3));

      Scan scan1 = new Scan();
      int numRecords = 0;
      try (ResultScanner scanner = ht.getScanner(scan1)) {
        for (Result result : scanner) {
          numRecords++;
        }

        LOG.info("test data has {} records.", numRecords);

        // by default, scan metrics collection is turned off
        assertNull(scanner.getScanMetrics());
      }

      // turn on scan metrics
      Scan scan2 = new Scan();
      scan2.setScanMetricsEnabled(true);
      scan2.setCaching(numRecords + 1);
      try (ResultScanner scanner = ht.getScanner(scan2)) {
        for (Result result : scanner.next(numRecords - 1)) {
        }
        assertNotNull(scanner.getScanMetrics());
      }

      // set caching to 1, because metrics are collected in each roundtrip only
      scan2 = new Scan();
      scan2.setScanMetricsEnabled(true);
      scan2.setCaching(1);
      try (ResultScanner scanner = ht.getScanner(scan2)) {
        // per HBASE-5717, this should still collect even if you don't run all the way to
        // the end of the scanner. So this is asking for 2 of the 3 rows we inserted.
        for (Result result : scanner.next(numRecords - 1)) {
        }
        ScanMetrics scanMetrics = scanner.getScanMetrics();
        assertEquals("Did not access all the regions in the table", numOfRegions,
          scanMetrics.countOfRegions.get());
      }

      // check byte counters
      scan2 = new Scan();
      scan2.setScanMetricsEnabled(true);
      scan2.setCaching(1);
      try (ResultScanner scanner = ht.getScanner(scan2)) {
        int numBytes = 0;
        for (Result result : scanner) {
          for (Cell cell : result.listCells()) {
            numBytes += PrivateCellUtil.estimatedSerializedSizeOf(cell);
          }
        }
        ScanMetrics scanMetrics = scanner.getScanMetrics();
        assertEquals("Did not count the result bytes", numBytes,
          scanMetrics.countOfBytesInResults.get());
      }

      // check byte counters on a small scan
      scan2 = new Scan();
      scan2.setScanMetricsEnabled(true);
      scan2.setCaching(1);
      scan2.setSmall(true);
      try (ResultScanner scanner = ht.getScanner(scan2)) {
        int numBytes = 0;
        for (Result result : scanner) {
          for (Cell cell : result.listCells()) {
            numBytes += PrivateCellUtil.estimatedSerializedSizeOf(cell);
          }
        }
        ScanMetrics scanMetrics = scanner.getScanMetrics();
        assertEquals("Did not count the result bytes", numBytes,
          scanMetrics.countOfBytesInResults.get());
      }

      // now, test that the metrics are still collected even if you don't call close, but do
      // run past the end of all the records
      /** There seems to be a timing issue here.  Comment out for now. Fix when time.
       Scan scanWithoutClose = new Scan();
       scanWithoutClose.setCaching(1);
       scanWithoutClose.setScanMetricsEnabled(true);
       ResultScanner scannerWithoutClose = ht.getScanner(scanWithoutClose);
       for (Result result : scannerWithoutClose.next(numRecords + 1)) {
       }
       ScanMetrics scanMetricsWithoutClose = getScanMetrics(scanWithoutClose);
       assertEquals("Did not access all the regions in the table", numOfRegions,
       scanMetricsWithoutClose.countOfRegions.get());
       */

      // finally,
      // test that the metrics are collected correctly if you both run past all the records,
      // AND close the scanner
      Scan scanWithClose = new Scan();
      // make sure we can set caching up to the number of a scanned values
      scanWithClose.setCaching(numRecords);
      scanWithClose.setScanMetricsEnabled(true);
      try (ResultScanner scannerWithClose = ht.getScanner(scanWithClose)) {
        for (Result result : scannerWithClose.next(numRecords + 1)) {
        }
        scannerWithClose.close();
        ScanMetrics scanMetricsWithClose = scannerWithClose.getScanMetrics();
        assertEquals("Did not access all the regions in the table", numOfRegions,
          scanMetricsWithClose.countOfRegions.get());
      }
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  /**
   * Tests that cache on write works all the way up from the client-side.
   *
   * Performs inserts, flushes, and compactions, verifying changes in the block
   * cache along the way.
   */
  @Test
  public void testCacheOnWriteEvictOnClose() throws Exception {
    final TableName tableName = name.getTableName();
    byte [] data = Bytes.toBytes("data");
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY)) {
      try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
        // get the block cache and region
        String regionName = locator.getAllRegionLocations().get(0).getRegion().getEncodedName();

        HRegion region = TEST_UTIL.getRSForFirstRegionInTable(tableName)
                .getRegion(regionName);
        HStore store = region.getStores().iterator().next();
        CacheConfig cacheConf = store.getCacheConfig();
        cacheConf.setCacheDataOnWrite(true);
        cacheConf.setEvictOnClose(true);
        BlockCache cache = cacheConf.getBlockCache().get();

        // establish baseline stats
        long startBlockCount = cache.getBlockCount();
        long startBlockHits = cache.getStats().getHitCount();
        long startBlockMiss = cache.getStats().getMissCount();

        // wait till baseline is stable, (minimal 500 ms)
        for (int i = 0; i < 5; i++) {
          Thread.sleep(100);
          if (startBlockCount != cache.getBlockCount()
                  || startBlockHits != cache.getStats().getHitCount()
                  || startBlockMiss != cache.getStats().getMissCount()) {
            startBlockCount = cache.getBlockCount();
            startBlockHits = cache.getStats().getHitCount();
            startBlockMiss = cache.getStats().getMissCount();
            i = -1;
          }
        }

        // insert data
        Put put = new Put(ROW);
        put.addColumn(FAMILY, QUALIFIER, data);
        table.put(put);
        assertTrue(Bytes.equals(table.get(new Get(ROW)).value(), data));

        // data was in memstore so don't expect any changes
        assertEquals(startBlockCount, cache.getBlockCount());
        assertEquals(startBlockHits, cache.getStats().getHitCount());
        assertEquals(startBlockMiss, cache.getStats().getMissCount());

        // flush the data
        LOG.debug("Flushing cache");
        region.flush(true);

        // expect two more blocks in cache - DATA and ROOT_INDEX
        // , no change in hits/misses
        long expectedBlockCount = startBlockCount + 2;
        long expectedBlockHits = startBlockHits;
        long expectedBlockMiss = startBlockMiss;
        assertEquals(expectedBlockCount, cache.getBlockCount());
        assertEquals(expectedBlockHits, cache.getStats().getHitCount());
        assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
        // read the data and expect same blocks, one new hit, no misses
        assertTrue(Bytes.equals(table.get(new Get(ROW)).value(), data));
        assertEquals(expectedBlockCount, cache.getBlockCount());
        assertEquals(++expectedBlockHits, cache.getStats().getHitCount());
        assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
        // insert a second column, read the row, no new blocks, one new hit
        byte[] QUALIFIER2 = Bytes.add(QUALIFIER, QUALIFIER);
        byte[] data2 = Bytes.add(data, data);
        put = new Put(ROW);
        put.addColumn(FAMILY, QUALIFIER2, data2);
        table.put(put);
        Result r = table.get(new Get(ROW));
        assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), data));
        assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER2), data2));
        assertEquals(expectedBlockCount, cache.getBlockCount());
        assertEquals(++expectedBlockHits, cache.getStats().getHitCount());
        assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
        // flush, one new block
        System.out.println("Flushing cache");
        region.flush(true);

        // + 1 for Index Block, +1 for data block
        expectedBlockCount += 2;
        assertEquals(expectedBlockCount, cache.getBlockCount());
        assertEquals(expectedBlockHits, cache.getStats().getHitCount());
        assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
        // compact, net minus two blocks, two hits, no misses
        System.out.println("Compacting");
        assertEquals(2, store.getStorefilesCount());
        store.triggerMajorCompaction();
        region.compact(true);
        store.closeAndArchiveCompactedFiles();
        waitForStoreFileCount(store, 1, 10000); // wait 10 seconds max
        assertEquals(1, store.getStorefilesCount());
        // evicted two data blocks and two index blocks and compaction does not cache new blocks
        expectedBlockCount = 0;
        assertEquals(expectedBlockCount, cache.getBlockCount());
        expectedBlockHits += 2;
        assertEquals(expectedBlockMiss, cache.getStats().getMissCount());
        assertEquals(expectedBlockHits, cache.getStats().getHitCount());
        // read the row, this should be a cache miss because we don't cache data
        // blocks on compaction
        r = table.get(new Get(ROW));
        assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), data));
        assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER2), data2));
        expectedBlockCount += 1; // cached one data block
        assertEquals(expectedBlockCount, cache.getBlockCount());
        assertEquals(expectedBlockHits, cache.getStats().getHitCount());
        assertEquals(++expectedBlockMiss, cache.getStats().getMissCount());
      }
    }
  }

  private void waitForStoreFileCount(HStore store, int count, int timeout)
      throws InterruptedException {
    long start = System.currentTimeMillis();
    while (start + timeout > System.currentTimeMillis() && store.getStorefilesCount() != count) {
      Thread.sleep(100);
    }
    System.out.println("start=" + start + ", now=" + System.currentTimeMillis() + ", cur=" +
        store.getStorefilesCount());
    assertEquals(count, store.getStorefilesCount());
  }

  /**
   * Tests the non cached version of getRegionLocator by moving a region.
   */
  @Test
  public void testNonCachedGetRegionLocation() throws Exception {
    // Test Initialization.
    final TableName tableName = name.getTableName();
    byte [] family1 = Bytes.toBytes("f1");
    byte [] family2 = Bytes.toBytes("f2");
    try (Table ignored = TEST_UTIL.createTable(tableName, new byte[][] {family1, family2}, 10);
        Admin admin = TEST_UTIL.getAdmin();
        RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      List<HRegionLocation> allRegionLocations = locator.getAllRegionLocations();
      assertEquals(1, allRegionLocations.size());
      RegionInfo regionInfo = allRegionLocations.get(0).getRegion();
      ServerName addrBefore = allRegionLocations.get(0).getServerName();
      // Verify region location before move.
      HRegionLocation addrCache = locator.getRegionLocation(regionInfo.getStartKey(), false);
      HRegionLocation addrNoCache = locator.getRegionLocation(regionInfo.getStartKey(),  true);

      assertEquals(addrBefore.getPort(), addrCache.getPort());
      assertEquals(addrBefore.getPort(), addrNoCache.getPort());

      // Make sure more than one server.
      if (TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size() <= 1) {
        TEST_UTIL.getMiniHBaseCluster().startRegionServer();
        Waiter.waitFor(TEST_UTIL.getConfiguration(), 30000, new Waiter.Predicate<Exception>() {
          @Override public boolean evaluate() throws Exception {
            return TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size() > 1;
          }
        });
      }

      ServerName addrAfter = null;
      // Now move the region to a different server.
      for (int i = 0; i < TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size();
           i++) {
        HRegionServer regionServer = TEST_UTIL.getHBaseCluster().getRegionServer(i);
        ServerName addr = regionServer.getServerName();
        if (addr.getPort() != addrBefore.getPort()) {
          admin.move(regionInfo.getEncodedNameAsBytes(), addr);
          // Wait for the region to move.
          Thread.sleep(5000);
          addrAfter = addr;
          break;
        }
      }

      // Verify the region was moved.
      addrCache = locator.getRegionLocation(regionInfo.getStartKey(), false);
      addrNoCache = locator.getRegionLocation(regionInfo.getStartKey(), true);
      assertNotNull(addrAfter);
      assertTrue(addrAfter.getPort() != addrCache.getPort());
      assertEquals(addrAfter.getPort(), addrNoCache.getPort());
    }
  }

  /**
   * Tests getRegionsInRange by creating some regions over which a range of
   * keys spans; then changing the key range.
   */
  @Test
  public void testGetRegionsInRange() throws Exception {
    // Test Initialization.
    byte [] startKey = Bytes.toBytes("ddc");
    byte [] endKey = Bytes.toBytes("mmm");
    TableName tableName = name.getTableName();
    TEST_UTIL.createMultiRegionTable(tableName, new byte[][] { FAMILY }, 10);

    int numOfRegions;
    try (RegionLocator r = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      numOfRegions = r.getStartKeys().length;
    }
    assertEquals(26, numOfRegions);

    // Get the regions in this range
    List<HRegionLocation> regionsList = getRegionsInRange(tableName, startKey, endKey);
    assertEquals(10, regionsList.size());

    // Change the start key
    startKey = Bytes.toBytes("fff");
    regionsList = getRegionsInRange(tableName, startKey, endKey);
    assertEquals(7, regionsList.size());

    // Change the end key
    endKey = Bytes.toBytes("nnn");
    regionsList = getRegionsInRange(tableName, startKey, endKey);
    assertEquals(8, regionsList.size());

    // Empty start key
    regionsList = getRegionsInRange(tableName, HConstants.EMPTY_START_ROW, endKey);
    assertEquals(13, regionsList.size());

    // Empty end key
    regionsList = getRegionsInRange(tableName, startKey, HConstants.EMPTY_END_ROW);
    assertEquals(21, regionsList.size());

    // Both start and end keys empty
    regionsList = getRegionsInRange(tableName, HConstants.EMPTY_START_ROW,
        HConstants.EMPTY_END_ROW);
    assertEquals(26, regionsList.size());

    // Change the end key to somewhere in the last block
    endKey = Bytes.toBytes("zzz1");
    regionsList = getRegionsInRange(tableName, startKey, endKey);
    assertEquals(21, regionsList.size());

    // Change the start key to somewhere in the first block
    startKey = Bytes.toBytes("aac");
    regionsList = getRegionsInRange(tableName, startKey, endKey);
    assertEquals(26, regionsList.size());

    // Make start and end key the same
    startKey = Bytes.toBytes("ccc");
    endKey = Bytes.toBytes("ccc");
    regionsList = getRegionsInRange(tableName, startKey, endKey);
    assertEquals(1, regionsList.size());
  }

  private List<HRegionLocation> getRegionsInRange(TableName tableName, byte[] startKey,
      byte[] endKey) throws IOException {
    List<HRegionLocation> regionsInRange = new ArrayList<>();
    byte[] currentKey = startKey;
    final boolean endKeyIsEndOfTable = Bytes.equals(endKey, HConstants.EMPTY_END_ROW);
    try (RegionLocator r = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      do {
        HRegionLocation regionLocation = r.getRegionLocation(currentKey);
        regionsInRange.add(regionLocation);
        currentKey = regionLocation.getRegion().getEndKey();
      } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW)
          && (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0));
      return regionsInRange;
    }
  }

  @Test
  public void testJira6912() throws Exception {
    final TableName tableName = name.getTableName();
    try (Table foo = TEST_UTIL.createTable(tableName, new byte[][] {FAMILY}, 10)) {

      List<Put> puts = new ArrayList<>();
      for (int i = 0; i != 100; i++) {
        Put put = new Put(Bytes.toBytes(i));
        put.addColumn(FAMILY, FAMILY, Bytes.toBytes(i));
        puts.add(put);
      }
      foo.put(puts);
      // If i comment this out it works
      TEST_UTIL.flush();

      Scan scan = new Scan();
      scan.setStartRow(Bytes.toBytes(1));
      scan.setStopRow(Bytes.toBytes(3));
      scan.addColumn(FAMILY, FAMILY);
      scan.setFilter(new RowFilter(CompareOperator.NOT_EQUAL,
              new BinaryComparator(Bytes.toBytes(1))));

      try (ResultScanner scanner = foo.getScanner(scan)) {
        Result[] bar = scanner.next(100);
        assertEquals(1, bar.length);
      }
    }
  }

  @Test
  public void testScan_NullQualifier() throws IOException {
    try (Table table = TEST_UTIL.createTable(name.getTableName(), FAMILY)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);

      put = new Put(ROW);
      put.addColumn(FAMILY, null, VALUE);
      table.put(put);
      LOG.info("Row put");

      Scan scan = new Scan();
      scan.addColumn(FAMILY, null);

      ResultScanner scanner = table.getScanner(scan);
      Result[] bar = scanner.next(100);
      assertEquals(1, bar.length);
      assertEquals(1, bar[0].size());

      scan = new Scan();
      scan.addFamily(FAMILY);

      scanner = table.getScanner(scan);
      bar = scanner.next(100);
      assertEquals(1, bar.length);
      assertEquals(2, bar[0].size());
    }
  }

  @Test
  public void testNegativeTimestamp() throws IOException {
    try (Table table = TEST_UTIL.createTable(name.getTableName(), FAMILY)) {

      try {
        Put put = new Put(ROW, -1);
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        table.put(put);
        fail("Negative timestamps should not have been allowed");
      } catch (IllegalArgumentException ex) {
        assertTrue(ex.getMessage().contains("negative"));
      }

      try {
        Put put = new Put(ROW);
        long ts = -1;
        put.addColumn(FAMILY, QUALIFIER, ts, VALUE);
        table.put(put);
        fail("Negative timestamps should not have been allowed");
      } catch (IllegalArgumentException ex) {
        assertTrue(ex.getMessage().contains("negative"));
      }

      try {
        Delete delete = new Delete(ROW, -1);
        table.delete(delete);
        fail("Negative timestamps should not have been allowed");
      } catch (IllegalArgumentException ex) {
        assertTrue(ex.getMessage().contains("negative"));
      }

      try {
        Delete delete = new Delete(ROW);
        delete.addFamily(FAMILY, -1);
        table.delete(delete);
        fail("Negative timestamps should not have been allowed");
      } catch (IllegalArgumentException ex) {
        assertTrue(ex.getMessage().contains("negative"));
      }

      try {
        Scan scan = new Scan();
        scan.setTimeRange(-1, 1);
        table.getScanner(scan);
        fail("Negative timestamps should not have been allowed");
      } catch (IllegalArgumentException ex) {
        assertTrue(ex.getMessage().contains("negative"));
      }

      // KeyValue should allow negative timestamps for backwards compat. Otherwise, if the user
      // already has negative timestamps in cluster data, HBase won't be able to handle that
      try {
        new KeyValue(Bytes.toBytes(42), Bytes.toBytes(42), Bytes.toBytes(42), -1,
                Bytes.toBytes(42));
      } catch (IllegalArgumentException ex) {
        fail("KeyValue SHOULD allow negative timestamps");
      }

    }
  }

  @Test
  public void testRawScanRespectsVersions() throws Exception {
    final TableName tableName = name.getTableName();
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY)) {
      byte[] row = Bytes.toBytes("row");

      // put the same row 4 times, with different values
      Put p = new Put(row);
      p.addColumn(FAMILY, QUALIFIER, 10, VALUE);
      table.put(p);
      p = new Put(row);
      p.addColumn(FAMILY, QUALIFIER, 11, ArrayUtils.add(VALUE, (byte) 2));
      table.put(p);

      p = new Put(row);
      p.addColumn(FAMILY, QUALIFIER, 12, ArrayUtils.add(VALUE, (byte) 3));
      table.put(p);

      p = new Put(row);
      p.addColumn(FAMILY, QUALIFIER, 13, ArrayUtils.add(VALUE, (byte) 4));
      table.put(p);

      int versions = 4;
      Scan s = new Scan(row);
      // get all the possible versions
      s.setMaxVersions();
      s.setRaw(true);

      try (ResultScanner scanner = table.getScanner(s)) {
        int count = 0;
        for (Result r : scanner) {
          assertEquals("Found an unexpected number of results for the row!", versions,
                  r.listCells().size());
          count++;
        }
        assertEquals("Found more than a single row when raw scanning the table with a single row!",
                1, count);
      }

      // then if we decrease the number of versions, but keep the scan raw, we should see exactly
      // that number of versions
      versions = 2;
      s.setMaxVersions(versions);
      try (ResultScanner scanner = table.getScanner(s)) {
        int count = 0;
        for (Result r : scanner) {
          assertEquals("Found an unexpected number of results for the row!", versions,
                  r.listCells().size());
          count++;
        }
        assertEquals("Found more than a single row when raw scanning the table with a single row!",
                1, count);
      }

      // finally, if we turn off raw scanning, but max out the number of versions, we should go back
      // to seeing just three
      versions = 3;
      s.setMaxVersions(versions);
      try (ResultScanner scanner = table.getScanner(s)) {
        int count = 0;
        for (Result r : scanner) {
          assertEquals("Found an unexpected number of results for the row!", versions,
                  r.listCells().size());
          count++;
        }
        assertEquals("Found more than a single row when raw scanning the table with a single row!",
                1, count);
      }

    }
    TEST_UTIL.deleteTable(tableName);
  }

  @Test
  public void testEmptyFilterList() throws Exception {
    // Test Initialization.
    final TableName tableName = name.getTableName();
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY)) {

      // Insert one row each region
      Put put = new Put(Bytes.toBytes("row"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);

      List<Result> scanResults = new LinkedList<>();
      Scan scan = new Scan();
      scan.setFilter(new FilterList());
      try (ResultScanner scanner = table.getScanner(scan)) {
        for (Result r : scanner) {
          scanResults.add(r);
        }
      }
      assertEquals(1, scanResults.size());
      Get g = new Get(Bytes.toBytes("row"));
      g.setFilter(new FilterList());
      Result getResult = table.get(g);
      Result scanResult = scanResults.get(0);
      assertEquals(scanResult.rawCells().length, getResult.rawCells().length);
      for (int i = 0; i != scanResult.rawCells().length; ++i) {
        Cell scanCell = scanResult.rawCells()[i];
        Cell getCell = getResult.rawCells()[i];
        assertEquals(0, Bytes.compareTo(CellUtil.cloneRow(scanCell),
                CellUtil.cloneRow(getCell)));
        assertEquals(0, Bytes.compareTo(CellUtil.cloneFamily(scanCell),
                CellUtil.cloneFamily(getCell)));
        assertEquals(0, Bytes.compareTo(CellUtil.cloneQualifier(scanCell),
                CellUtil.cloneQualifier(getCell)));
        assertEquals(0, Bytes.compareTo(CellUtil.cloneValue(scanCell),
                CellUtil.cloneValue(getCell)));
      }
    }
  }

  @Test
  public void testSmallScan() throws Exception {
    // Test Initialization.
    final TableName tableName = name.getTableName();
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY)) {

      // Insert one row each region
      int insertNum = 10;
      for (int i = 0; i < 10; i++) {
        Put put = new Put(Bytes.toBytes("row" + String.format("%03d", i)));
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        table.put(put);
      }

      // normal scan
      try (ResultScanner scanner = table.getScanner(new Scan())) {
        int count = 0;
        for (Result r : scanner) {
          assertFalse(r.isEmpty());
          count++;
        }
        assertEquals(insertNum, count);
      }

      // small scan
      Scan scan = new Scan(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      scan.setSmall(true);
      scan.setCaching(2);
      try (ResultScanner scanner = table.getScanner(scan)) {
        int count = 0;
        for (Result r : scanner) {
          assertFalse(r.isEmpty());
          count++;
        }
        assertEquals(insertNum, count);
      }
    }
  }

  @Test
  public void testSuperSimpleWithReverseScan() throws Exception {
    final TableName tableName = name.getTableName();
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      Put put = new Put(Bytes.toBytes("0-b11111-0000000000000000000"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b11111-0000000000000000002"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b11111-0000000000000000004"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b11111-0000000000000000006"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b11111-0000000000000000008"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b22222-0000000000000000001"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b22222-0000000000000000003"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b22222-0000000000000000005"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b22222-0000000000000000007"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b22222-0000000000000000009"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      Scan scan = new Scan(Bytes.toBytes("0-b11111-9223372036854775807"),
              Bytes.toBytes("0-b11111-0000000000000000000"));
      scan.setReversed(true);
      try (ResultScanner scanner = ht.getScanner(scan)) {
        Result result = scanner.next();
        assertTrue(Bytes.equals(result.getRow(),
                Bytes.toBytes("0-b11111-0000000000000000008")));
      }
    }
  }

  @Test
  public void testFiltersWithReverseScan() throws Exception {
    final TableName tableName = name.getTableName();
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      byte[][] ROWS = makeN(ROW, 10);
      byte[][] QUALIFIERS = {Bytes.toBytes("col0-<d2v1>-<d3v2>"),
              Bytes.toBytes("col1-<d2v1>-<d3v2>"),
              Bytes.toBytes("col2-<d2v1>-<d3v2>"),
              Bytes.toBytes("col3-<d2v1>-<d3v2>"),
              Bytes.toBytes("col4-<d2v1>-<d3v2>"),
              Bytes.toBytes("col5-<d2v1>-<d3v2>"),
              Bytes.toBytes("col6-<d2v1>-<d3v2>"),
              Bytes.toBytes("col7-<d2v1>-<d3v2>"),
              Bytes.toBytes("col8-<d2v1>-<d3v2>"),
              Bytes.toBytes("col9-<d2v1>-<d3v2>")};
      for (int i = 0; i < 10; i++) {
        Put put = new Put(ROWS[i]);
        put.addColumn(FAMILY, QUALIFIERS[i], VALUE);
        ht.put(put);
      }
      Scan scan = new Scan();
      scan.setReversed(true);
      scan.addFamily(FAMILY);
      Filter filter = new QualifierFilter(CompareOperator.EQUAL,
              new RegexStringComparator("col[1-5]"));
      scan.setFilter(filter);
      try (ResultScanner scanner = ht.getScanner(scan)) {
        int expectedIndex = 5;
        for (Result result : scanner) {
          assertEquals(1, result.size());
          Cell c = result.rawCells()[0];
          assertTrue(Bytes.equals(c.getRowArray(), c.getRowOffset(), c.getRowLength(),
                  ROWS[expectedIndex], 0, ROWS[expectedIndex].length));
          assertTrue(Bytes.equals(c.getQualifierArray(), c.getQualifierOffset(),
                  c.getQualifierLength(), QUALIFIERS[expectedIndex], 0,
                  QUALIFIERS[expectedIndex].length));
          expectedIndex--;
        }
        assertEquals(0, expectedIndex);
      }
    }
  }

  @Test
  public void testKeyOnlyFilterWithReverseScan() throws Exception {
    final TableName tableName = name.getTableName();
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      byte[][] ROWS = makeN(ROW, 10);
      byte[][] QUALIFIERS = {Bytes.toBytes("col0-<d2v1>-<d3v2>"),
              Bytes.toBytes("col1-<d2v1>-<d3v2>"),
              Bytes.toBytes("col2-<d2v1>-<d3v2>"),
              Bytes.toBytes("col3-<d2v1>-<d3v2>"),
              Bytes.toBytes("col4-<d2v1>-<d3v2>"),
              Bytes.toBytes("col5-<d2v1>-<d3v2>"),
              Bytes.toBytes("col6-<d2v1>-<d3v2>"),
              Bytes.toBytes("col7-<d2v1>-<d3v2>"),
              Bytes.toBytes("col8-<d2v1>-<d3v2>"),
              Bytes.toBytes("col9-<d2v1>-<d3v2>")};
      for (int i = 0; i < 10; i++) {
        Put put = new Put(ROWS[i]);
        put.addColumn(FAMILY, QUALIFIERS[i], VALUE);
        ht.put(put);
      }
      Scan scan = new Scan();
      scan.setReversed(true);
      scan.addFamily(FAMILY);
      Filter filter = new KeyOnlyFilter(true);
      scan.setFilter(filter);
      try (ResultScanner ignored = ht.getScanner(scan)) {
        int count = 0;
        for (Result result : ht.getScanner(scan)) {
          assertEquals(1, result.size());
          assertEquals(Bytes.SIZEOF_INT, result.rawCells()[0].getValueLength());
          assertEquals(VALUE.length, Bytes.toInt(CellUtil.cloneValue(result.rawCells()[0])));
          count++;
        }
        assertEquals(10, count);
      }
    }
  }

  /**
   * Test simple table and non-existent row cases.
   */
  @Test
  public void testSimpleMissingWithReverseScan() throws Exception {
    final TableName tableName = name.getTableName();
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      byte[][] ROWS = makeN(ROW, 4);

      // Try to get a row on an empty table
      Scan scan = new Scan();
      scan.setReversed(true);
      Result result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      scan = new Scan(ROWS[0]);
      scan.setReversed(true);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      scan = new Scan(ROWS[0], ROWS[1]);
      scan.setReversed(true);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      scan = new Scan();
      scan.setReversed(true);
      scan.addFamily(FAMILY);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      scan = new Scan();
      scan.setReversed(true);
      scan.addColumn(FAMILY, QUALIFIER);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      // Insert a row

      Put put = new Put(ROWS[2]);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);

      // Make sure we can scan the row
      scan = new Scan();
      scan.setReversed(true);
      result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);

      scan = new Scan(ROWS[3], ROWS[0]);
      scan.setReversed(true);
      result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);

      scan = new Scan(ROWS[2], ROWS[1]);
      scan.setReversed(true);
      result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);

      // Try to scan empty rows around it
      // Introduced MemStore#shouldSeekForReverseScan to fix the following
      scan = new Scan(ROWS[1]);
      scan.setReversed(true);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);
    }
  }

  @Test
  public void testNullWithReverseScan() throws Exception {
    final TableName tableName = name.getTableName();
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY)) {
      // Null qualifier (should work)
      Put put = new Put(ROW);
      put.addColumn(FAMILY, null, VALUE);
      ht.put(put);
      scanTestNull(ht, ROW, FAMILY, VALUE, true);
      Delete delete = new Delete(ROW);
      delete.addColumns(FAMILY, null);
      ht.delete(delete);
    }

    // Use a new table
    try (Table ht =
       TEST_UTIL.createTable(TableName.valueOf(name.getTableName().toString() + "2"), FAMILY)) {
      // Empty qualifier, byte[0] instead of null (should work)
      Put put = new Put(ROW);
      put.addColumn(FAMILY, HConstants.EMPTY_BYTE_ARRAY, VALUE);
      ht.put(put);
      scanTestNull(ht, ROW, FAMILY, VALUE, true);
      TEST_UTIL.flush();
      scanTestNull(ht, ROW, FAMILY, VALUE, true);
      Delete delete = new Delete(ROW);
      delete.addColumns(FAMILY, HConstants.EMPTY_BYTE_ARRAY);
      ht.delete(delete);
      // Null value
      put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, null);
      ht.put(put);
      Scan scan = new Scan();
      scan.setReversed(true);
      scan.addColumn(FAMILY, QUALIFIER);
      Result result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROW, FAMILY, QUALIFIER, null);
    }
  }

  @Test
  @SuppressWarnings("checkstyle:MethodLength")
  public void testDeletesWithReverseScan() throws Exception {
    final TableName tableName = name.getTableName();
    byte[][] ROWS = makeNAscii(ROW, 6);
    byte[][] FAMILIES = makeNAscii(FAMILY, 3);
    byte[][] VALUES = makeN(VALUE, 5);
    long[] ts = { 1000, 2000, 3000, 4000, 5000 };
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILIES, 3)) {

      Put put = new Put(ROW);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[1], VALUES[1]);
      ht.put(put);

      Delete delete = new Delete(ROW);
      delete.addFamily(FAMILIES[0], ts[0]);
      ht.delete(delete);

      Scan scan = new Scan(ROW);
      scan.setReversed(true);
      scan.addFamily(FAMILIES[0]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      Result result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[]{ts[1]},
              new byte[][]{VALUES[1]}, 0, 0);

      // Test delete latest version
      put = new Put(ROW);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[2], VALUES[2]);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[3], VALUES[3]);
      put.addColumn(FAMILIES[0], null, ts[4], VALUES[4]);
      put.addColumn(FAMILIES[0], null, ts[2], VALUES[2]);
      put.addColumn(FAMILIES[0], null, ts[3], VALUES[3]);
      ht.put(put);

      delete = new Delete(ROW);
      delete.addColumn(FAMILIES[0], QUALIFIER); // ts[4]
      ht.delete(delete);

      scan = new Scan(ROW);
      scan.setReversed(true);
      scan.addColumn(FAMILIES[0], QUALIFIER);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[]{ts[1],
              ts[2], ts[3]}, new byte[][]{VALUES[1], VALUES[2], VALUES[3]}, 0, 2);

      // Test for HBASE-1847
      delete = new Delete(ROW);
      delete.addColumn(FAMILIES[0], null);
      ht.delete(delete);

      // Cleanup null qualifier
      delete = new Delete(ROW);
      delete.addColumns(FAMILIES[0], null);
      ht.delete(delete);

      // Expected client behavior might be that you can re-put deleted values
      // But alas, this is not to be. We can't put them back in either case.

      put = new Put(ROW);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]);
      ht.put(put);

      // The Scanner returns the previous values, the expected-naive-unexpected
      // behavior

      scan = new Scan(ROW);
      scan.setReversed(true);
      scan.addFamily(FAMILIES[0]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[]{ts[1],
              ts[2], ts[3]}, new byte[][]{VALUES[1], VALUES[2], VALUES[3]}, 0, 2);

      // Test deleting an entire family from one row but not the other various
      // ways

      put = new Put(ROWS[0]);
      put.addColumn(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
      put.addColumn(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
      put.addColumn(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
      put.addColumn(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
      ht.put(put);

      put = new Put(ROWS[1]);
      put.addColumn(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
      put.addColumn(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
      put.addColumn(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
      put.addColumn(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
      ht.put(put);

      put = new Put(ROWS[2]);
      put.addColumn(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
      put.addColumn(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
      put.addColumn(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
      put.addColumn(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
      ht.put(put);

      delete = new Delete(ROWS[0]);
      delete.addFamily(FAMILIES[2]);
      ht.delete(delete);

      delete = new Delete(ROWS[1]);
      delete.addColumns(FAMILIES[1], QUALIFIER);
      ht.delete(delete);

      delete = new Delete(ROWS[2]);
      delete.addColumn(FAMILIES[1], QUALIFIER);
      delete.addColumn(FAMILIES[1], QUALIFIER);
      delete.addColumn(FAMILIES[2], QUALIFIER);
      ht.delete(delete);

      scan = new Scan(ROWS[0]);
      scan.setReversed(true);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertEquals("Expected 2 keys but received " + result.size(), 2, result.size());
      assertNResult(result, ROWS[0], FAMILIES[1], QUALIFIER, new long[]{ts[0],
              ts[1]}, new byte[][]{VALUES[0], VALUES[1]}, 0, 1);

      scan = new Scan(ROWS[1]);
      scan.setReversed(true);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertEquals("Expected 2 keys but received " + result.size(), 2, result.size());

      scan = new Scan(ROWS[2]);
      scan.setReversed(true);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertEquals(1, result.size());
      assertNResult(result, ROWS[2], FAMILIES[2], QUALIFIER,
              new long[]{ts[2]}, new byte[][]{VALUES[2]}, 0, 0);

      // Test if we delete the family first in one row (HBASE-1541)

      delete = new Delete(ROWS[3]);
      delete.addFamily(FAMILIES[1]);
      ht.delete(delete);

      put = new Put(ROWS[3]);
      put.addColumn(FAMILIES[2], QUALIFIER, VALUES[0]);
      ht.put(put);

      put = new Put(ROWS[4]);
      put.addColumn(FAMILIES[1], QUALIFIER, VALUES[1]);
      put.addColumn(FAMILIES[2], QUALIFIER, VALUES[2]);
      ht.put(put);

      scan = new Scan(ROWS[4]);
      scan.setReversed(true);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      scan.setMaxVersions(Integer.MAX_VALUE);
      ResultScanner scanner = ht.getScanner(scan);
      result = scanner.next();
      assertEquals("Expected 2 keys but received " + result.size(), 2, result.size());
      assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[0]), ROWS[4]));
      assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[1]), ROWS[4]));
      assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[0]), VALUES[1]));
      assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[1]), VALUES[2]));
      result = scanner.next();
      assertEquals("Expected 1 key but received " + result.size(), 1, result.size());
      assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[0]), ROWS[3]));
      assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[0]), VALUES[0]));
      scanner.close();
    }
  }

  /**
   * Tests reversed scan under multi regions
   */
  @Test
  public void testReversedScanUnderMultiRegions() throws Exception {
    // Test Initialization.
    final TableName tableName = name.getTableName();
    byte[] maxByteArray = ConnectionUtils.MAX_BYTE_ARRAY;
    byte[][] splitRows = new byte[][] { Bytes.toBytes("005"),
        Bytes.add(Bytes.toBytes("005"), Bytes.multiple(maxByteArray, 16)),
        Bytes.toBytes("006"),
        Bytes.add(Bytes.toBytes("006"), Bytes.multiple(maxByteArray, 8)),
        Bytes.toBytes("007"),
        Bytes.add(Bytes.toBytes("007"), Bytes.multiple(maxByteArray, 4)),
        Bytes.toBytes("008"), Bytes.multiple(maxByteArray, 2) };
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY, splitRows)) {
      TEST_UTIL.waitUntilAllRegionsAssigned(table.getName());

      try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
        assertEquals(splitRows.length + 1, l.getAllRegionLocations().size());
      }
      // Insert one row each region
      int insertNum = splitRows.length;
      for (byte[] splitRow : splitRows) {
        Put put = new Put(splitRow);
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        table.put(put);
      }

      // scan forward
      try (ResultScanner scanner = table.getScanner(new Scan())) {
        int count = 0;
        for (Result r : scanner) {
          assertFalse(r.isEmpty());
          count++;
        }
        assertEquals(insertNum, count);
      }

      // scan backward
      Scan scan = new Scan();
      scan.setReversed(true);
      try (ResultScanner scanner = table.getScanner(scan)) {
        int count = 0;
        byte[] lastRow = null;
        for (Result r : scanner) {
          assertFalse(r.isEmpty());
          count++;
          byte[] thisRow = r.getRow();
          if (lastRow != null) {
            assertTrue("Error scan order, last row= " + Bytes.toString(lastRow)
                            + ",this row=" + Bytes.toString(thisRow),
                    Bytes.compareTo(thisRow, lastRow) < 0);
          }
          lastRow = thisRow;
        }
        assertEquals(insertNum, count);
      }
    }
  }

  /**
   * Tests reversed scan under multi regions
   */
  @Test
  public void testSmallReversedScanUnderMultiRegions() throws Exception {
    // Test Initialization.
    final TableName tableName = name.getTableName();
    byte[][] splitRows = new byte[][]{
        Bytes.toBytes("000"), Bytes.toBytes("002"), Bytes.toBytes("004"),
        Bytes.toBytes("006"), Bytes.toBytes("008"), Bytes.toBytes("010")};
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY, splitRows)) {
      TEST_UTIL.waitUntilAllRegionsAssigned(table.getName());

      try (RegionLocator l = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
        assertEquals(splitRows.length + 1, l.getAllRegionLocations().size());
      }
      for (byte[] splitRow : splitRows) {
        Put put = new Put(splitRow);
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        table.put(put);

        byte[] nextRow = Bytes.copy(splitRow);
        nextRow[nextRow.length - 1]++;

        put = new Put(nextRow);
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        table.put(put);
      }

      // scan forward
      try (ResultScanner scanner = table.getScanner(new Scan())) {
        int count = 0;
        for (Result r : scanner) {
          assertTrue(!r.isEmpty());
          count++;
        }
        assertEquals(12, count);
      }

      reverseScanTest(table, false);
      reverseScanTest(table, true);
    }
  }

  private void reverseScanTest(Table table, boolean small) throws IOException {
    // scan backward
    Scan scan = new Scan();
    scan.setReversed(true);
    try (ResultScanner scanner = table.getScanner(scan)) {
      int count = 0;
      byte[] lastRow = null;
      for (Result r : scanner) {
        assertTrue(!r.isEmpty());
        count++;
        byte[] thisRow = r.getRow();
        if (lastRow != null) {
          assertTrue("Error scan order, last row= " + Bytes.toString(lastRow)
                          + ",this row=" + Bytes.toString(thisRow),
                  Bytes.compareTo(thisRow, lastRow) < 0);
        }
        lastRow = thisRow;
      }
      assertEquals(12, count);
    }

    scan = new Scan();
    scan.setSmall(small);
    scan.setReversed(true);
    scan.setStartRow(Bytes.toBytes("002"));
    try (ResultScanner scanner = table.getScanner(scan)) {
      int count = 0;
      byte[] lastRow = null;
      for (Result r : scanner) {
        assertTrue(!r.isEmpty());
        count++;
        byte[] thisRow = r.getRow();
        if (lastRow != null) {
          assertTrue("Error scan order, last row= " + Bytes.toString(lastRow)
                          + ",this row=" + Bytes.toString(thisRow),
                  Bytes.compareTo(thisRow, lastRow) < 0);
        }
        lastRow = thisRow;
      }
      assertEquals(3, count); // 000 001 002
    }

    scan = new Scan();
    scan.setSmall(small);
    scan.setReversed(true);
    scan.setStartRow(Bytes.toBytes("002"));
    scan.setStopRow(Bytes.toBytes("000"));
    try (ResultScanner scanner = table.getScanner(scan)) {
      int count = 0;
      byte[] lastRow = null;
      for (Result r : scanner) {
        assertFalse(r.isEmpty());
        count++;
        byte[] thisRow = r.getRow();
        if (lastRow != null) {
          assertTrue("Error scan order, last row= " + Bytes.toString(lastRow)
                          + ",this row=" + Bytes.toString(thisRow),
                  Bytes.compareTo(thisRow, lastRow) < 0);
        }
        lastRow = thisRow;
      }
      assertEquals(2, count); // 001 002
    }

    scan = new Scan();
    scan.setSmall(small);
    scan.setReversed(true);
    scan.setStartRow(Bytes.toBytes("001"));
    try (ResultScanner scanner = table.getScanner(scan)) {
      int count = 0;
      byte[] lastRow = null;
      for (Result r : scanner) {
        assertFalse(r.isEmpty());
        count++;
        byte[] thisRow = r.getRow();
        if (lastRow != null) {
          assertTrue("Error scan order, last row= " + Bytes.toString(lastRow)
                          + ",this row=" + Bytes.toString(thisRow),
                  Bytes.compareTo(thisRow, lastRow) < 0);
        }
        lastRow = thisRow;
      }
      assertEquals(2, count); // 000 001
    }

    scan = new Scan();
    scan.setSmall(small);
    scan.setReversed(true);
    scan.setStartRow(Bytes.toBytes("000"));
    try (ResultScanner scanner = table.getScanner(scan)) {
      int count = 0;
      byte[] lastRow = null;
      for (Result r : scanner) {
        assertFalse(r.isEmpty());
        count++;
        byte[] thisRow = r.getRow();
        if (lastRow != null) {
          assertTrue("Error scan order, last row= " + Bytes.toString(lastRow)
                          + ",this row=" + Bytes.toString(thisRow),
                  Bytes.compareTo(thisRow, lastRow) < 0);
        }
        lastRow = thisRow;
      }
      assertEquals(1, count); // 000
    }

    scan = new Scan();
    scan.setSmall(small);
    scan.setReversed(true);
    scan.setStartRow(Bytes.toBytes("006"));
    scan.setStopRow(Bytes.toBytes("002"));
    try (ResultScanner scanner = table.getScanner(scan)) {
      int count = 0;
      byte[] lastRow = null;
      for (Result r : scanner) {
        assertFalse(r.isEmpty());
        count++;
        byte[] thisRow = r.getRow();
        if (lastRow != null) {
          assertTrue("Error scan order, last row= " + Bytes.toString(lastRow)
                          + ",this row=" + Bytes.toString(thisRow),
                  Bytes.compareTo(thisRow, lastRow) < 0);
        }
        lastRow = thisRow;
      }
      assertEquals(4, count); // 003 004 005 006
    }
  }

  @Test
  public void testFilterAllRecords() throws IOException {
    Scan scan = new Scan();
    scan.setBatch(1);
    scan.setCaching(1);
    // Filter out any records
    scan.setFilter(new FilterList(new FirstKeyOnlyFilter(), new InclusiveStopFilter(new byte[0])));
    try (Table table = TEST_UTIL.getConnection().getTable(TableName.META_TABLE_NAME)) {
      try (ResultScanner s = table.getScanner(scan)) {
        assertNull(s.next());
      }
    }
  }

  @Test
  public void testCellSizeLimit() throws IOException {
    final TableName tableName = name.getTableName();
    TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor =
      new TableDescriptorBuilder.ModifyableTableDescriptor(tableName)
        .setValue(HRegion.HBASE_MAX_CELL_SIZE_KEY, Integer.toString(10 * 1024));
    ColumnFamilyDescriptor familyDescriptor =
      new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(FAMILY);

    tableDescriptor.setColumnFamily(familyDescriptor);
    try (Admin admin = TEST_UTIL.getAdmin()) {
      admin.createTable(tableDescriptor);
    }
    // Will succeed
    try (Table t = TEST_UTIL.getConnection().getTable(tableName)) {
      t.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, Bytes.toBytes(0L)));
      t.increment(new Increment(ROW).addColumn(FAMILY, QUALIFIER, 1L));
    }
    // Will succeed
    try (Table t = TEST_UTIL.getConnection().getTable(tableName)) {
      t.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, new byte[9*1024]));
    }
    // Will fail
    try (Table t = TEST_UTIL.getConnection().getTable(tableName)) {
      try {
        t.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, new byte[10 * 1024]));
        fail("Oversize cell failed to trigger exception");
      } catch (IOException e) {
        // expected
      }
      try {
        t.append(new Append(ROW).addColumn(FAMILY, QUALIFIER, new byte[2 * 1024]));
        fail("Oversize cell failed to trigger exception");
      } catch (IOException e) {
        // expected
      }
    }
  }

  @Test
  public void testCellSizeNoLimit() throws IOException {
    final TableName tableName = name.getTableName();
    ColumnFamilyDescriptor familyDescriptor =
      new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(FAMILY);
    TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor =
      new TableDescriptorBuilder.ModifyableTableDescriptor(tableName)
        .setValue(HRegion.HBASE_MAX_CELL_SIZE_KEY, Integer.toString(0));
    tableDescriptor.setColumnFamily(familyDescriptor);

    try (Admin admin = TEST_UTIL.getAdmin()) {
      admin.createTable(tableDescriptor);
    }

    // Will succeed
    try (Table ht = TEST_UTIL.getConnection().getTable(tableName)) {
      ht.put(new Put(ROW).addColumn(FAMILY, QUALIFIER,  new byte[HRegion.DEFAULT_MAX_CELL_SIZE -
        1024]));
      ht.append(new Append(ROW).addColumn(FAMILY, QUALIFIER, new byte[1024 + 1]));
    }
  }

  @Test
  public void testDeleteSpecifiedVersionOfSpecifiedColumn() throws Exception {
    final TableName tableName = name.getTableName();

    byte[][] VALUES = makeN(VALUE, 5);
    long[] ts = {1000, 2000, 3000, 4000, 5000};

    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY, 5)) {

      Put put = new Put(ROW);
      // Put version 1000,2000,3000,4000 of column FAMILY:QUALIFIER
      for (int t = 0; t < 4; t++) {
        put.addColumn(FAMILY, QUALIFIER, ts[t], VALUES[t]);
      }
      ht.put(put);

      Delete delete = new Delete(ROW);
      // Delete version 3000 of column FAMILY:QUALIFIER
      delete.addColumn(FAMILY, QUALIFIER, ts[2]);
      ht.delete(delete);

      Get get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readVersions(Integer.MAX_VALUE);
      Result result = ht.get(get);
      // verify version 1000,2000,4000 remains for column FAMILY:QUALIFIER
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[0], ts[1], ts[3]}, new byte[][]{
              VALUES[0], VALUES[1], VALUES[3]}, 0, 2);

      delete = new Delete(ROW);
      // Delete a version 5000 of column FAMILY:QUALIFIER which didn't exist
      delete.addColumn(FAMILY, QUALIFIER, ts[4]);
      ht.delete(delete);

      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      // verify version 1000,2000,4000 remains for column FAMILY:QUALIFIER
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[0], ts[1], ts[3]}, new byte[][]{
              VALUES[0], VALUES[1], VALUES[3]}, 0, 2);
    }
  }

  @Test
  public void testDeleteLatestVersionOfSpecifiedColumn() throws Exception {
    final TableName tableName = name.getTableName();
    byte[][] VALUES = makeN(VALUE, 5);
    long[] ts = {1000, 2000, 3000, 4000, 5000};
    try (Table ht = TEST_UTIL.createTable(tableName, FAMILY, 5)) {
      Put put = new Put(ROW);
      // Put version 1000,2000,3000,4000 of column FAMILY:QUALIFIER
      for (int t = 0; t < 4; t++) {
        put.addColumn(FAMILY, QUALIFIER, ts[t], VALUES[t]);
      }
      ht.put(put);

      Delete delete = new Delete(ROW);
      // Delete latest version of column FAMILY:QUALIFIER
      delete.addColumn(FAMILY, QUALIFIER);
      ht.delete(delete);

      Get get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readVersions(Integer.MAX_VALUE);
      Result result = ht.get(get);
      // verify version 1000,2000,3000 remains for column FAMILY:QUALIFIER
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[0], ts[1], ts[2]}, new byte[][]{
              VALUES[0], VALUES[1], VALUES[2]}, 0, 2);

      delete = new Delete(ROW);
      // Delete two latest version of column FAMILY:QUALIFIER
      delete.addColumn(FAMILY, QUALIFIER);
      delete.addColumn(FAMILY, QUALIFIER);
      ht.delete(delete);

      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      // verify version 1000 remains for column FAMILY:QUALIFIER
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[0]}, new byte[][]{VALUES[0]},
              0, 0);

      put = new Put(ROW);
      // Put a version 5000 of column FAMILY:QUALIFIER
      put.addColumn(FAMILY, QUALIFIER, ts[4], VALUES[4]);
      ht.put(put);

      get = new Get(ROW);
      get.addColumn(FAMILY, QUALIFIER);
      get.readVersions(Integer.MAX_VALUE);
      result = ht.get(get);
      // verify version 1000,5000 remains for column FAMILY:QUALIFIER
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[0], ts[4]}, new byte[][]{
              VALUES[0], VALUES[4]}, 0, 1);
    }
  }

  /**
   * Test for HBASE-17125
   */
  @Test
  public void testReadWithFilter() throws Exception {
    final TableName tableName = name.getTableName();
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY, 3)) {

      byte[] VALUEA = Bytes.toBytes("value-a");
      byte[] VALUEB = Bytes.toBytes("value-b");
      long[] ts = {1000, 2000, 3000, 4000};

      Put put = new Put(ROW);
      // Put version 1000,2000,3000,4000 of column FAMILY:QUALIFIER
      for (int t = 0; t <= 3; t++) {
        if (t <= 1) {
          put.addColumn(FAMILY, QUALIFIER, ts[t], VALUEA);
        } else {
          put.addColumn(FAMILY, QUALIFIER, ts[t], VALUEB);
        }
      }
      table.put(put);

      Scan scan =
              new Scan().setFilter(new ValueFilter(CompareOperator.EQUAL,
                      new SubstringComparator("value-a")))
                      .setMaxVersions(3);
      ResultScanner scanner = table.getScanner(scan);
      Result result = scanner.next();
      // ts[0] has gone from user view. Only read ts[2] which value is less or equal to 3
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[1]}, new byte[][]{VALUEA}, 0,
              0);

      Get get =
              new Get(ROW)
                      .setFilter(new ValueFilter(CompareOperator.EQUAL,
                              new SubstringComparator("value-a")))
                      .readVersions(3);
      result = table.get(get);
      // ts[0] has gone from user view. Only read ts[2] which value is less or equal to 3
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[1]}, new byte[][]{VALUEA}, 0,
              0);

      // Test with max versions 1, it should still read ts[1]
      scan =
              new Scan().setFilter(new ValueFilter(CompareOperator.EQUAL,
                      new SubstringComparator("value-a")))
                      .setMaxVersions(1);
      scanner = table.getScanner(scan);
      result = scanner.next();
      // ts[0] has gone from user view. Only read ts[2] which value is less or equal to 3
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[1]}, new byte[][]{VALUEA}, 0,
              0);

      // Test with max versions 1, it should still read ts[1]
      get =
              new Get(ROW)
                      .setFilter(new ValueFilter(CompareOperator.EQUAL,
                              new SubstringComparator("value-a")))
                      .readVersions(1);
      result = table.get(get);
      // ts[0] has gone from user view. Only read ts[2] which value is less or equal to 3
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[1]}, new byte[][]{VALUEA}, 0,
              0);

      // Test with max versions 5, it should still read ts[1]
      scan =
              new Scan().setFilter(new ValueFilter(CompareOperator.EQUAL,
                      new SubstringComparator("value-a")))
                      .setMaxVersions(5);
      scanner = table.getScanner(scan);
      result = scanner.next();
      // ts[0] has gone from user view. Only read ts[2] which value is less or equal to 3
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[1]}, new byte[][]{VALUEA}, 0,
              0);

      // Test with max versions 5, it should still read ts[1]
      get =
              new Get(ROW)
                      .setFilter(new ValueFilter(CompareOperator.EQUAL,
                              new SubstringComparator("value-a")))
                      .readVersions(5);
      result = table.get(get);
      // ts[0] has gone from user view. Only read ts[2] which value is less or equal to 3
      assertNResult(result, ROW, FAMILY, QUALIFIER, new long[]{ts[1]}, new byte[][]{VALUEA}, 0,
              0);
    }
  }

  @Test
  public void testCellUtilTypeMethods() throws IOException {
    final TableName tableName = name.getTableName();
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY)) {

      final byte[] row = Bytes.toBytes("p");
      Put p = new Put(row);
      p.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(p);

      try (ResultScanner scanner = table.getScanner(new Scan())) {
        Result result = scanner.next();
        assertNotNull(result);
        CellScanner cs = result.cellScanner();
        assertTrue(cs.advance());
        Cell c = cs.current();
        assertTrue(CellUtil.isPut(c));
        assertFalse(CellUtil.isDelete(c));
        assertFalse(cs.advance());
        assertNull(scanner.next());
      }

      Delete d = new Delete(row);
      d.addColumn(FAMILY, QUALIFIER);
      table.delete(d);

      Scan scan = new Scan();
      scan.setRaw(true);
      try (ResultScanner scanner = table.getScanner(scan)) {
        Result result = scanner.next();
        assertNotNull(result);
        CellScanner cs = result.cellScanner();
        assertTrue(cs.advance());

        // First cell should be the delete (masking the Put)
        Cell c = cs.current();
        assertTrue("Cell should be a Delete: " + c, CellUtil.isDelete(c));
        assertFalse("Cell should not be a Put: " + c, CellUtil.isPut(c));

        // Second cell should be the original Put
        assertTrue(cs.advance());
        c = cs.current();
        assertFalse("Cell should not be a Delete: " + c, CellUtil.isDelete(c));
        assertTrue("Cell should be a Put: " + c, CellUtil.isPut(c));

        // No more cells in this row
        assertFalse(cs.advance());

        // No more results in this scan
        assertNull(scanner.next());
      }
    }
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testCreateTableWithZeroRegionReplicas() throws Exception {
    TableName tableName = name.getTableName();
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes("cf")))
        .setRegionReplication(0)
        .build();

    TEST_UTIL.getAdmin().createTable(desc);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testModifyTableWithZeroRegionReplicas() throws Exception {
    TableName tableName = name.getTableName();
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(Bytes.toBytes("cf")))
        .build();

    TEST_UTIL.getAdmin().createTable(desc);
    TableDescriptor newDesc = TableDescriptorBuilder.newBuilder(desc)
        .setRegionReplication(0)
        .build();

    TEST_UTIL.getAdmin().modifyTable(newDesc);
  }

  @Test(timeout = 60000)
  public void testModifyTableWithMemstoreData() throws Exception {
    TableName tableName = name.getTableName();
    createTableAndValidateTableSchemaModification(tableName, true);
  }

  @Test(timeout = 60000)
  public void testDeleteCFWithMemstoreData() throws Exception {
    TableName tableName = name.getTableName();
    createTableAndValidateTableSchemaModification(tableName, false);
  }

  /**
   * Create table and validate online schema modification
   * @param tableName Table name
   * @param modifyTable Modify table if true otherwise delete column family
   * @throws IOException in case of failures
   */
  private void createTableAndValidateTableSchemaModification(TableName tableName,
      boolean modifyTable) throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    // Create table with two Cfs
    byte[] cf1 = Bytes.toBytes("cf1");
    byte[] cf2 = Bytes.toBytes("cf2");
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf1))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf2)).build();
    admin.createTable(tableDesc);

    Table t = TEST_UTIL.getConnection().getTable(tableName);
    // Insert few records and flush the table
    t.put(new Put(ROW).addColumn(cf1, QUALIFIER, Bytes.toBytes("val1")));
    t.put(new Put(ROW).addColumn(cf2, QUALIFIER, Bytes.toBytes("val2")));
    admin.flush(tableName);
    Path tableDir = CommonFSUtils.getTableDir(TEST_UTIL.getDefaultRootDirPath(), tableName);
    List<Path> regionDirs = FSUtils.getRegionDirs(TEST_UTIL.getTestFileSystem(), tableDir);
    assertEquals(1, regionDirs.size());
    List<Path> familyDirs = FSUtils.getFamilyDirs(TEST_UTIL.getTestFileSystem(), regionDirs.get(0));
    assertEquals(2, familyDirs.size());

    // Insert record but dont flush the table
    t.put(new Put(ROW).addColumn(cf1, QUALIFIER, Bytes.toBytes("val2")));
    t.put(new Put(ROW).addColumn(cf2, QUALIFIER, Bytes.toBytes("val2")));

    if (modifyTable) {
      tableDesc = TableDescriptorBuilder.newBuilder(tableDesc).removeColumnFamily(cf2).build();
      admin.modifyTable(tableDesc);
    } else {
      admin.deleteColumnFamily(tableName, cf2);
    }
    // After table modification or delete family there should be only one CF in FS
    familyDirs = FSUtils.getFamilyDirs(TEST_UTIL.getTestFileSystem(), regionDirs.get(0));
    assertEquals("CF dir count should be 1, but was " + familyDirs.size(), 1, familyDirs.size());
  }
}
