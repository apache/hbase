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
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ColumnAggregationProtos;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ColumnAggregationProtos.SumResponse;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ColumnAggregationWithErrorsProtos;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ColumnAggregationWithNullResponseProtos;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ColumnAggregationWithNullResponseProtos.ColumnAggregationServiceNullResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.ServiceException;

/**
 * TestEndpoint: test cases to verify the batch execution of coprocessor Endpoint
 */
@Category(MediumTests.class)
public class TestBatchCoprocessorEndpoint {
  private static final Log LOG = LogFactory.getLog(TestBatchCoprocessorEndpoint.class);

  private static final TableName TEST_TABLE =
      TableName.valueOf("TestTable");
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
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        org.apache.hadoop.hbase.coprocessor.ColumnAggregationEndpoint.class.getName(),
        ProtobufCoprocessorService.class.getName(),
        ColumnAggregationEndpointWithErrors.class.getName(),
        ColumnAggregationEndpointNullResponse.class.getName());
    conf.setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        ProtobufCoprocessorService.class.getName());
    util.startMiniCluster(2);
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor desc = new HTableDescriptor(TEST_TABLE);
    desc.addFamily(new HColumnDescriptor(TEST_FAMILY));
    admin.createTable(desc, new byte[][]{ROWS[rowSeperator1], ROWS[rowSeperator2]});
    util.waitUntilAllRegionsAssigned(TEST_TABLE);
    admin.close();

    HTable table = new HTable(conf, TEST_TABLE);
    for (int i = 0; i < ROWSIZE; i++) {
      Put put = new Put(ROWS[i]);
      put.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(i));
      table.put(put);
    }
    table.close();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testAggregationNullResponse() throws Throwable {
    HTable table = new HTable(util.getConfiguration(), TEST_TABLE);
    ColumnAggregationWithNullResponseProtos.SumRequest.Builder builder =
        ColumnAggregationWithNullResponseProtos.SumRequest
        .newBuilder();
    builder.setFamily(ByteStringer.wrap(TEST_FAMILY));
    if (TEST_QUALIFIER != null && TEST_QUALIFIER.length > 0) {
      builder.setQualifier(ByteStringer.wrap(TEST_QUALIFIER));
    }
    Map<byte[], ColumnAggregationWithNullResponseProtos.SumResponse> results =
        table.batchCoprocessorService(
            ColumnAggregationServiceNullResponse.getDescriptor().findMethodByName("sum"),
            builder.build(), ROWS[0], ROWS[ROWS.length - 1],
            ColumnAggregationWithNullResponseProtos.SumResponse.getDefaultInstance());

    int sumResult = 0;
    int expectedResult = 0;
    for (Map.Entry<byte[], ColumnAggregationWithNullResponseProtos.SumResponse> e :
        results.entrySet()) {
      LOG.info("Got value " + e.getValue().getSum() + " for region "
          + Bytes.toStringBinary(e.getKey()));
      sumResult += e.getValue().getSum();
    }
    for (int i = 0; i < rowSeperator2; i++) {
      expectedResult += i;
    }
    assertEquals("Invalid result", expectedResult, sumResult);
    table.close();
  }

  private static byte[][] makeN(byte[] base, int n) {
    byte[][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      ret[i] = Bytes.add(base, Bytes.toBytes(String.format("%02d", i)));
    }
    return ret;
  }

  private Map<byte[], SumResponse> sum(final HTable table, final byte[] family,
      final byte[] qualifier, final byte[] start, final byte[] end) throws ServiceException,
      Throwable {
    ColumnAggregationProtos.SumRequest.Builder builder = ColumnAggregationProtos.SumRequest
        .newBuilder();
    builder.setFamily(ByteStringer.wrap(family));
    if (qualifier != null && qualifier.length > 0) {
      builder.setQualifier(ByteStringer.wrap(qualifier));
    }
    return table.batchCoprocessorService(
        ColumnAggregationProtos.ColumnAggregationService.getDescriptor().findMethodByName("sum"),
        builder.build(), start, end, ColumnAggregationProtos.SumResponse.getDefaultInstance());
  }

  @Test
  public void testAggregationWithReturnValue() throws Throwable {
    HTable table = new HTable(util.getConfiguration(), TEST_TABLE);
    Map<byte[], SumResponse> results = sum(table, TEST_FAMILY, TEST_QUALIFIER, ROWS[0],
        ROWS[ROWS.length - 1]);
    int sumResult = 0;
    int expectedResult = 0;
    for (Map.Entry<byte[], SumResponse> e : results.entrySet()) {
      LOG.info("Got value " + e.getValue().getSum() + " for region "
          + Bytes.toStringBinary(e.getKey()));
      sumResult += e.getValue().getSum();
    }
    for (int i = 0; i < ROWSIZE; i++) {
      expectedResult += i;
    }
    assertEquals("Invalid result", expectedResult, sumResult);

    results.clear();

    // scan: for region 2 and region 3
    results = sum(table, TEST_FAMILY, TEST_QUALIFIER, ROWS[rowSeperator1],
        ROWS[ROWS.length - 1]);
    sumResult = 0;
    expectedResult = 0;
    for (Map.Entry<byte[], SumResponse> e : results.entrySet()) {
      LOG.info("Got value " + e.getValue().getSum() + " for region "
          + Bytes.toStringBinary(e.getKey()));
      sumResult += e.getValue().getSum();
    }
    for (int i = rowSeperator1; i < ROWSIZE; i++) {
      expectedResult += i;
    }
    assertEquals("Invalid result", expectedResult, sumResult);
    table.close();
  }

  @Test
  public void testAggregation() throws Throwable {
    HTable table = new HTable(util.getConfiguration(), TEST_TABLE);
    Map<byte[], SumResponse> results = sum(table, TEST_FAMILY, TEST_QUALIFIER,
        ROWS[0], ROWS[ROWS.length - 1]);
    int sumResult = 0;
    int expectedResult = 0;
    for (Map.Entry<byte[], SumResponse> e : results.entrySet()) {
      LOG.info("Got value " + e.getValue().getSum() + " for region "
          + Bytes.toStringBinary(e.getKey()));
      sumResult += e.getValue().getSum();
    }
    for (int i = 0; i < ROWSIZE; i++) {
      expectedResult += i;
    }
    assertEquals("Invalid result", expectedResult, sumResult);

    // scan: for region 2 and region 3
    results = sum(table, TEST_FAMILY, TEST_QUALIFIER, ROWS[rowSeperator1], ROWS[ROWS.length - 1]);
    sumResult = 0;
    expectedResult = 0;
    for (Map.Entry<byte[], SumResponse> e : results.entrySet()) {
      LOG.info("Got value " + e.getValue().getSum() + " for region "
          + Bytes.toStringBinary(e.getKey()));
      sumResult += e.getValue().getSum();
    }
    for (int i = rowSeperator1; i < ROWSIZE; i++) {
      expectedResult += i;
    }
    assertEquals("Invalid result", expectedResult, sumResult);
    table.close();
  }

  @Test
  public void testAggregationWithErrors() throws Throwable {
    HTable table = new HTable(util.getConfiguration(), TEST_TABLE);
    final Map<byte[], ColumnAggregationWithErrorsProtos.SumResponse> results =
        Collections.synchronizedMap(
            new TreeMap<byte[], ColumnAggregationWithErrorsProtos.SumResponse>(
                Bytes.BYTES_COMPARATOR
            ));
    ColumnAggregationWithErrorsProtos.SumRequest.Builder builder =
        ColumnAggregationWithErrorsProtos.SumRequest
        .newBuilder();
    builder.setFamily(ByteStringer.wrap(TEST_FAMILY));
    if (TEST_QUALIFIER != null && TEST_QUALIFIER.length > 0) {
      builder.setQualifier(ByteStringer.wrap(TEST_QUALIFIER));
    }

    boolean hasError = false;
    try {
      table.batchCoprocessorService(
          ColumnAggregationWithErrorsProtos.ColumnAggregationServiceWithErrors.getDescriptor()
              .findMethodByName("sum"),
          builder.build(), ROWS[0], ROWS[ROWS.length - 1],
          ColumnAggregationWithErrorsProtos.SumResponse.getDefaultInstance(),
          new Batch.Callback<ColumnAggregationWithErrorsProtos.SumResponse>() {

            @Override
            public void update(byte[] region, byte[] row,
                ColumnAggregationWithErrorsProtos.SumResponse result) {
              results.put(region, result);
            }
          });
    } catch (Throwable t) {
      LOG.info("Exceptions in coprocessor service", t);
      hasError = true;
    }

    int sumResult = 0;
    int expectedResult = 0;
    for (Map.Entry<byte[], ColumnAggregationWithErrorsProtos.SumResponse> e : results.entrySet()) {
      LOG.info("Got value " + e.getValue().getSum() + " for region "
          + Bytes.toStringBinary(e.getKey()));
      sumResult += e.getValue().getSum();
    }
    for (int i = 0; i < rowSeperator2; i++) {
      expectedResult += i;
    }
    assertEquals("Invalid result", expectedResult, sumResult);
    assertTrue(hasError);
    table.close();
  }
}