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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.google.protobuf.RpcController;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

/**
 * TestEndpoint: test cases to verify coprocessor Endpoint
 */
@Category(MediumTests.class)
public class TestCoprocessorEndpoint {
  private static final Log LOG = LogFactory.getLog(TestCoprocessorEndpoint.class);

  private static final byte[] TEST_TABLE = Bytes.toBytes("TestTable");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("TestFamily");
  private static final byte[] TEST_QUALIFIER = Bytes.toBytes("TestQualifier");
  private static byte[] ROW = Bytes.toBytes("testRow");
  
  private static final String protocolName =  "org.apache.hadoop.hbase.CustomProtocol";
  private static final String methodName = "myFunc";

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
        org.apache.hadoop.hbase.coprocessor.GenericEndpoint.class.getName(),
        ProtobufCoprocessorService.class.getName());
    util.startMiniCluster(2);
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor desc = new HTableDescriptor(TEST_TABLE);
    desc.addFamily(new HColumnDescriptor(TEST_FAMILY));
    admin.createTable(desc, new byte[][]{ROWS[rowSeperator1], ROWS[rowSeperator2]});
    util.waitUntilAllRegionsAssigned(3);
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
  public void testGeneric() throws Throwable {
    HTable table = new HTable(util.getConfiguration(), TEST_TABLE);
    GenericProtocol protocol = table.coprocessorProxy(GenericProtocol.class,
                                                      Bytes.toBytes("testRow"));
    String workResult1 = protocol.doWork("foo");
    assertEquals("foo", workResult1);
    byte[] workResult2 = protocol.doWork(new byte[]{1});
    assertArrayEquals(new byte[]{1}, workResult2);
    byte workResult3 = protocol.doWork((byte)1);
    assertEquals((byte)1, workResult3);
    char workResult4 = protocol.doWork('c');
    assertEquals('c', workResult4);
    boolean workResult5 = protocol.doWork(true);
    assertEquals(true, workResult5);
    short workResult6 = protocol.doWork((short)1);
    assertEquals((short)1, workResult6);
    int workResult7 = protocol.doWork(5);
    assertEquals(5, workResult7);
    long workResult8 = protocol.doWork(5l);
    assertEquals(5l, workResult8);
    double workResult9 = protocol.doWork(6d);
    assertEquals(6d, workResult9, 0.01);
    float workResult10 = protocol.doWork(6f);
    assertEquals(6f, workResult10, 0.01);
    Text workResult11 = protocol.doWork(new Text("foo"));
    assertEquals(new Text("foo"), workResult11);
    table.close();
  }

  @Test
  public void testAggregation() throws Throwable {
    HTable table = new HTable(util.getConfiguration(), TEST_TABLE);
    Map<byte[], Long> results;

    // scan: for all regions
    results = table
        .coprocessorExec(ColumnAggregationProtocol.class,
                         ROWS[0], ROWS[ROWS.length-1],
                         new Batch.Call<ColumnAggregationProtocol, Long>() {
                           public Long call(ColumnAggregationProtocol instance)
                               throws IOException {
                             return instance.sum(TEST_FAMILY, TEST_QUALIFIER);
                           }
                         });
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
    results = table
        .coprocessorExec(ColumnAggregationProtocol.class,
                         ROWS[rowSeperator1], ROWS[ROWS.length-1],
                         new Batch.Call<ColumnAggregationProtocol, Long>() {
                           public Long call(ColumnAggregationProtocol instance)
                               throws IOException {
                             return instance.sum(TEST_FAMILY, TEST_QUALIFIER);
                           }
                         });
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
    HTable table = new HTable(util.getConfiguration(), TEST_TABLE);
    NavigableMap<HRegionInfo,ServerName> regions = table.getRegionLocations();

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
            public TestProtos.EchoResponseProto call(TestRpcServiceProtos.TestProtobufRpcProto instance)
                throws IOException {
              LOG.debug("Default response is " + TestProtos.EchoRequestProto.getDefaultInstance());
              BlockingRpcCallback<TestProtos.EchoResponseProto> callback = new BlockingRpcCallback<TestProtos.EchoResponseProto>();
              instance.echo(controller, request, callback);
              TestProtos.EchoResponseProto response = callback.get();
              LOG.debug("Batch.Call returning result " + response);
              return response;
            }
          },
          new Batch.Callback<TestProtos.EchoResponseProto>() {
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
      for (HRegionInfo info : regions.navigableKeySet()) {
        LOG.info("Region info is "+info.getRegionNameAsString());
        assertTrue(results.containsKey(info.getRegionName()));
      }
      results.clear();

      // scan: for region 2 and region 3
      table.coprocessorService(TestRpcServiceProtos.TestProtobufRpcProto.class,
          ROWS[rowSeperator1], ROWS[ROWS.length - 1],
          new Batch.Call<TestRpcServiceProtos.TestProtobufRpcProto, TestProtos.EchoResponseProto>() {
            public TestProtos.EchoResponseProto call(TestRpcServiceProtos.TestProtobufRpcProto instance)
                throws IOException {
              LOG.debug("Default response is " + TestProtos.EchoRequestProto.getDefaultInstance());
              BlockingRpcCallback<TestProtos.EchoResponseProto> callback = new BlockingRpcCallback<TestProtos.EchoResponseProto>();
              instance.echo(controller, request, callback);
              TestProtos.EchoResponseProto response = callback.get();
              LOG.debug("Batch.Call returning result " + response);
              return response;
            }
          },
          new Batch.Callback<TestProtos.EchoResponseProto>() {
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

  private static byte[][] makeN(byte[] base, int n) {
    byte[][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      ret[i] = Bytes.add(base, Bytes.toBytes(String.format("%02d", i)));
    }
    return ret;
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

