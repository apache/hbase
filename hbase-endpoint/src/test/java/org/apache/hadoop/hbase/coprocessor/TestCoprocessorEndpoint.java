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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.coprocessor.protobuf.generated.ColumnAggregationProtos;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos;

/**
 * TestEndpoint: test cases to verify coprocessor Endpoint
 */
@Category({CoprocessorTests.class, MediumTests.class})
public class TestCoprocessorEndpoint {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCoprocessorEndpoint.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCoprocessorEndpoint.class);

  private static final TableName TEST_TABLE =
      TableName.valueOf("TestCoprocessorEndpoint");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("TestFamily");
  private static final byte[] TEST_QUALIFIER = Bytes.toBytes("TestQualifier");
  private static byte[] ROW = Bytes.toBytes("testRow");

  private static final int ROWSIZE = 20;
  private static final int rowSeperator1 = 5;
  private static final int rowSeperator2 = 12;
  private static byte[][] ROWS = makeN(ROW, ROWSIZE);

  private static HBaseTestingUtility util = new HBaseTestingUtility();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // set configure to indicate which cp should be loaded
    Configuration conf = util.getConfiguration();
    conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 5000);
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        org.apache.hadoop.hbase.coprocessor.ColumnAggregationEndpoint.class.getName(),
        ProtobufCoprocessorService.class.getName());
    conf.setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        ProtobufCoprocessorService.class.getName());
    util.startMiniCluster(2);
    Admin admin = util.getAdmin();

    TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor =
      new TableDescriptorBuilder.ModifyableTableDescriptor(TEST_TABLE);
    tableDescriptor.setColumnFamily(
      new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(TEST_FAMILY));
    admin.createTable(tableDescriptor, new byte[][]{ROWS[rowSeperator1], ROWS[rowSeperator2]});
    util.waitUntilAllRegionsAssigned(TEST_TABLE);

    Table table = util.getConnection().getTable(TEST_TABLE);
    for (int i = 0; i < ROWSIZE; i++) {
      Put put = new Put(ROWS[i]);
      put.addColumn(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(i));
      table.put(put);
    }
    table.close();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  private Map<byte [], Long> sum(final Table table, final byte [] family,
          final byte [] qualifier, final byte [] start, final byte [] end)
          throws ServiceException, Throwable {
    return table.coprocessorService(ColumnAggregationProtos.ColumnAggregationService.class,
        start, end,
      new Batch.Call<ColumnAggregationProtos.ColumnAggregationService, Long>() {
        @Override
        public Long call(ColumnAggregationProtos.ColumnAggregationService instance)
          throws IOException {
          CoprocessorRpcUtils.BlockingRpcCallback<ColumnAggregationProtos.SumResponse> rpcCallback =
              new CoprocessorRpcUtils.BlockingRpcCallback<>();
          ColumnAggregationProtos.SumRequest.Builder builder =
            ColumnAggregationProtos.SumRequest.newBuilder();
          builder.setFamily(UnsafeByteOperations.unsafeWrap(family));
          if (qualifier != null && qualifier.length > 0) {
            builder.setQualifier(UnsafeByteOperations.unsafeWrap(qualifier));
          }
          instance.sum(null, builder.build(), rpcCallback);
          return rpcCallback.get().getSum();
        }
      });
  }

  @Test
  public void testAggregation() throws Throwable {
    Table table = util.getConnection().getTable(TEST_TABLE);
    Map<byte[], Long> results = sum(table, TEST_FAMILY, TEST_QUALIFIER,
      ROWS[0], ROWS[ROWS.length-1]);
    int sumResult = 0;
    int expectedResult = 0;
    for (Map.Entry<byte[], Long> e : results.entrySet()) {
      LOG.info("Got value "+e.getValue()+" for region "+Bytes.toStringBinary(e.getKey()));
      sumResult += e.getValue();
    }
    for (int i = 0; i < ROWSIZE; i++) {
      expectedResult += i;
    }
    assertEquals("Invalid result", expectedResult, sumResult);

    results.clear();

    // scan: for region 2 and region 3
    results = sum(table, TEST_FAMILY, TEST_QUALIFIER,
      ROWS[rowSeperator1], ROWS[ROWS.length-1]);
    sumResult = 0;
    expectedResult = 0;
    for (Map.Entry<byte[], Long> e : results.entrySet()) {
      LOG.info("Got value "+e.getValue()+" for region "+Bytes.toStringBinary(e.getKey()));
      sumResult += e.getValue();
    }
    for (int i = rowSeperator1; i < ROWSIZE; i++) {
      expectedResult += i;
    }
    assertEquals("Invalid result", expectedResult, sumResult);
    table.close();
  }

  @Test
  public void testCoprocessorService() throws Throwable {
    Table table = util.getConnection().getTable(TEST_TABLE);

    List<HRegionLocation> regions;
    try(RegionLocator rl = util.getConnection().getRegionLocator(TEST_TABLE)) {
      regions = rl.getAllRegionLocations();
    }
    final TestProtos.EchoRequestProto request =
        TestProtos.EchoRequestProto.newBuilder().setMessage("hello").build();
    final Map<byte[], String> results = Collections.synchronizedMap(
        new TreeMap<byte[], String>(Bytes.BYTES_COMPARATOR));
    try {
      // scan: for all regions
      final RpcController controller = new ServerRpcController();
      table.coprocessorService(TestRpcServiceProtos.TestProtobufRpcProto.class,
        ROWS[0], ROWS[ROWS.length - 1],
        new Batch.Call<TestRpcServiceProtos.TestProtobufRpcProto, TestProtos.EchoResponseProto>() {
          @Override
          public TestProtos.EchoResponseProto call(
              TestRpcServiceProtos.TestProtobufRpcProto instance) throws IOException {
            LOG.debug("Default response is " + TestProtos.EchoRequestProto.getDefaultInstance());
            CoprocessorRpcUtils.BlockingRpcCallback<TestProtos.EchoResponseProto> callback =
                new CoprocessorRpcUtils.BlockingRpcCallback<>();
            instance.echo(controller, request, callback);
            TestProtos.EchoResponseProto response = callback.get();
            LOG.debug("Batch.Call returning result " + response);
            return response;
          }
        },
        new Batch.Callback<TestProtos.EchoResponseProto>() {
          @Override
          public void update(byte[] region, byte[] row, TestProtos.EchoResponseProto result) {
            assertNotNull(result);
            assertEquals("hello", result.getMessage());
            results.put(region, result.getMessage());
          }
        }
      );
      for (Map.Entry<byte[], String> e : results.entrySet()) {
        LOG.info("Got value "+e.getValue()+" for region "+Bytes.toStringBinary(e.getKey()));
      }
      assertEquals(3, results.size());
      for (HRegionLocation info : regions) {
        LOG.info("Region info is "+info.getRegion().getRegionNameAsString());
        assertTrue(results.containsKey(info.getRegion().getRegionName()));
      }
      results.clear();

      // scan: for region 2 and region 3
      table.coprocessorService(TestRpcServiceProtos.TestProtobufRpcProto.class,
        ROWS[rowSeperator1], ROWS[ROWS.length - 1],
        new Batch.Call<TestRpcServiceProtos.TestProtobufRpcProto, TestProtos.EchoResponseProto>() {
          @Override
          public TestProtos.EchoResponseProto call(
              TestRpcServiceProtos.TestProtobufRpcProto instance) throws IOException {
            LOG.debug("Default response is " + TestProtos.EchoRequestProto.getDefaultInstance());
            CoprocessorRpcUtils.BlockingRpcCallback<TestProtos.EchoResponseProto> callback =
                new CoprocessorRpcUtils.BlockingRpcCallback<>();
            instance.echo(controller, request, callback);
            TestProtos.EchoResponseProto response = callback.get();
            LOG.debug("Batch.Call returning result " + response);
            return response;
          }
        },
        new Batch.Callback<TestProtos.EchoResponseProto>() {
          @Override
          public void update(byte[] region, byte[] row, TestProtos.EchoResponseProto result) {
            assertNotNull(result);
            assertEquals("hello", result.getMessage());
            results.put(region, result.getMessage());
          }
        }
      );
      for (Map.Entry<byte[], String> e : results.entrySet()) {
        LOG.info("Got value "+e.getValue()+" for region "+Bytes.toStringBinary(e.getKey()));
      }
      assertEquals(2, results.size());
    } finally {
      table.close();
    }
  }

  @Test
  public void testCoprocessorServiceNullResponse() throws Throwable {
    Table table = util.getConnection().getTable(TEST_TABLE);
    List<HRegionLocation> regions;
    try(RegionLocator rl = util.getConnection().getRegionLocator(TEST_TABLE)) {
      regions = rl.getAllRegionLocations();
    }

    final TestProtos.EchoRequestProto request =
        TestProtos.EchoRequestProto.newBuilder().setMessage("hello").build();
    try {
      // scan: for all regions
      final RpcController controller = new ServerRpcController();
      // test that null results are supported
      Map<byte[], String> results =
            table.coprocessorService(TestRpcServiceProtos.TestProtobufRpcProto.class,
          ROWS[0], ROWS[ROWS.length - 1],
          new Batch.Call<TestRpcServiceProtos.TestProtobufRpcProto, String>() {
            public String call(TestRpcServiceProtos.TestProtobufRpcProto instance)
                throws IOException {
              CoprocessorRpcUtils.BlockingRpcCallback<TestProtos.EchoResponseProto> callback =
                  new CoprocessorRpcUtils.BlockingRpcCallback<>();
              instance.echo(controller, request, callback);
              TestProtos.EchoResponseProto response = callback.get();
              LOG.debug("Batch.Call got result " + response);
              return null;
            }
          }
      );
      for (Map.Entry<byte[], String> e : results.entrySet()) {
        LOG.info("Got value "+e.getValue()+" for region "+Bytes.toStringBinary(e.getKey()));
      }
      assertEquals(3, results.size());
      for (HRegionLocation region : regions) {
        RegionInfo info = region.getRegion();
        LOG.info("Region info is "+info.getRegionNameAsString());
        assertTrue(results.containsKey(info.getRegionName()));
        assertNull(results.get(info.getRegionName()));
      }
    } finally {
      table.close();
    }
  }

  @Test
  public void testMasterCoprocessorService() throws Throwable {
    Admin admin = util.getAdmin();
    final TestProtos.EchoRequestProto request =
        TestProtos.EchoRequestProto.newBuilder().setMessage("hello").build();
    TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface service =
        TestRpcServiceProtos.TestProtobufRpcProto.newBlockingStub(admin.coprocessorService());
    assertEquals("hello", service.echo(null, request).getMessage());
  }

  @Test
  public void testCoprocessorError() throws Exception {
    Configuration configuration = new Configuration(util.getConfiguration());
    // Make it not retry forever
    configuration.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    Table table = util.getConnection().getTable(TEST_TABLE);

    try {
      CoprocessorRpcChannel protocol = table.coprocessorService(ROWS[0]);

      TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface service =
          TestRpcServiceProtos.TestProtobufRpcProto.newBlockingStub(protocol);

      service.error(null, TestProtos.EmptyRequestProto.getDefaultInstance());
      fail("Should have thrown an exception");
    } catch (ServiceException e) {
    } finally {
      table.close();
    }
  }

  @Test
  public void testMasterCoprocessorError() throws Throwable {
    Admin admin = util.getAdmin();
    TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface service =
        TestRpcServiceProtos.TestProtobufRpcProto.newBlockingStub(admin.coprocessorService());
    try {
      service.error(null, TestProtos.EmptyRequestProto.getDefaultInstance());
      fail("Should have thrown an exception");
    } catch (ServiceException e) {
    }
  }

  private static byte[][] makeN(byte[] base, int n) {
    byte[][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      ret[i] = Bytes.add(base, Bytes.toBytes(String.format("%02d", i)));
    }
    return ret;
  }
}
