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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster.MiniHBaseClusterRegionServer;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.ServerCall;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestShortCircuitGet {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestShortCircuitGet.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] FAMILY = Bytes.toBytes("testFamily");
  private static final byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static final byte[] VALUE = Bytes.toBytes("testValue");

  static final byte[] r0 = Bytes.toBytes("row-0");
  static final byte[] r1 = Bytes.toBytes("row-1");
  static final byte[] r2 = Bytes.toBytes("row-2");
  static final byte[] r3 = Bytes.toBytes("row-3");
  static final byte[] r4 = Bytes.toBytes("row-4");
  static final byte[] r5 = Bytes.toBytes("row-5");
  static final byte[] r6 = Bytes.toBytes("row-6");
  static final TableName tableName = TableName.valueOf("TestShortCircuitGet");

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 30 * 60 * 1000);
    conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30 * 60 * 1000);

    conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 60 * 60 * 1000);
    conf.setStrings(HConstants.REGION_SERVER_IMPL, MyRegionServer.class.getName());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    conf.setInt(HConstants.HBASE_CLIENT_PAUSE, 10000);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * This test is for HBASE-26821,when we initiate get or scan in cp, the {@link RegionScanner} for
   * get and scan is close when get or scan is completed.
   */
  @Test
  public void testScannerCloseWhenScanAndGetInCP() throws Exception {
    Table table = null;
    try {
      table = TEST_UTIL.createTable(tableName, new byte[][] { FAMILY }, 1,
        HConstants.DEFAULT_BLOCKSIZE, MyScanObserver.class.getName());
      putToTable(table, r0);
      putToTable(table, r1);
      putToTable(table, r2);
      putToTable(table, r3);
      putToTable(table, r4);
      putToTable(table, r5);
      putToTable(table, r6);
    } finally {
      if (table != null) {
        table.close();
      }
    }

    final Configuration conf = TEST_UTIL.getConfiguration();
    ResultScanner resultScanner = null;
    Connection conn = null;
    Table clientTable = null;
    try {
      conn = ConnectionFactory.createConnection(conf);
      clientTable = conn.getTable(tableName);
      Scan scan = new Scan();
      scan.setCaching(1);
      scan.withStartRow(r0, true).withStopRow(r1, true);
      resultScanner = table.getScanner(scan);
      Result result = resultScanner.next();
      assertTrue("Expected row: row-0", Bytes.equals(r0, result.getRow()));
      result = resultScanner.next();
      assertTrue("Expected row: row-1", Bytes.equals(r1, result.getRow()));
      assertNull(resultScanner.next());
    } finally {
      if (resultScanner != null) {
        resultScanner.close();
      }
      if (clientTable != null) {
        clientTable.close();
      }
      if (conn != null) {
        conn.close();
      }
    }

    assertTrue(MyRSRpcServices.exceptionRef.get() == null);
    assertTrue(MyScanObserver.exceptionRef.get() == null);
  }

  private void putToTable(Table ht, byte[] rowkey) throws IOException {
    Put put = new Put(rowkey);
    put.addColumn(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
  }

  private static class MyRegionServer extends MiniHBaseClusterRegionServer {
    public MyRegionServer(Configuration conf)
        throws IOException, InterruptedException {
      super(conf);
    }

    @Override
    protected RSRpcServices createRpcServices() throws IOException {
      return new MyRSRpcServices(this);
    }
  }

  private static class MyRSRpcServices extends RSRpcServices {
    private static AtomicReference<Throwable> exceptionRef = new AtomicReference<Throwable>(null);
    public MyRSRpcServices(HRegionServer rs) throws IOException {
      super(rs);
    }

    @Override
    public MultiResponse multi(RpcController rpcc, MultiRequest request) throws ServiceException {
      try {
        if (!MyScanObserver.inCP) {
          return super.multi(rpcc, request);
        }

        assertTrue(!RpcServer.getCurrentCall().isPresent());
        return super.multi(rpcc, request);
      } catch (Throwable e) {
        exceptionRef.set(e);
        throw new ServiceException(e);
      }
    }

    @Override
    public ScanResponse scan(RpcController controller, ScanRequest request)
        throws ServiceException {
      try {
        if (!MyScanObserver.inCP) {
          return super.scan(controller, request);
        }

        HRegion region = null;
        if (request.hasRegion()) {
          region = this.getRegion(request.getRegion());
        }

        if (region != null
            && TableName.isMetaTableName(region.getTableDescriptor().getTableName())) {
          return super.scan(controller, request);
        }

        assertTrue(!RpcServer.getCurrentCall().isPresent());
        return super.scan(controller, request);
      } catch (Throwable e) {
        exceptionRef.set(e);
        throw new ServiceException(e);
      }
    }

    @Override
    public GetResponse get(RpcController controller, GetRequest request) throws ServiceException {
      try {
        if (!MyScanObserver.inCP) {
          return super.get(controller, request);
        }

        HRegion region = null;
        if (request.hasRegion()) {
          region = this.getRegion(request.getRegion());
        }
        if (region != null
            && TableName.isMetaTableName(region.getTableDescriptor().getTableName())) {
          return super.get(controller, request);
        }

        assertTrue(!RpcServer.getCurrentCall().isPresent());
        return super.get(controller, request);
      } catch (Throwable e) {
        exceptionRef.set(e);
        throw new ServiceException(e);
      }
    }
  }

  public static class MyScanObserver implements RegionCoprocessor, RegionObserver {

    private static volatile boolean inCP = false;
    private static AtomicReference<Throwable> exceptionRef = new AtomicReference<Throwable>(null);
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public RegionScanner postScannerOpen(
        final ObserverContext<RegionCoprocessorEnvironment> observerContext, final Scan scan,
        final RegionScanner regionScanner) throws IOException {

      if (inCP) {
        return regionScanner;
      }

      HRegion region = (HRegion) observerContext.getEnvironment().getRegion();
      int prevScannerCount = region.scannerReadPoints.size();
      Table table1 = null;
      Get get2 = new Get(r2);
      inCP = true;
      try (Connection connection = observerContext.getEnvironment()
          .createConnection(observerContext.getEnvironment().getConfiguration())) {
        try {
          table1 = connection.getTable(tableName);
          Result result = table1.get(get2);
          assertTrue("Expected row: row-2", Bytes.equals(r2, result.getRow()));

        } finally {
          if (table1 != null) {
            table1.close();
          }
          inCP = false;
        }

        // RegionScanner is closed and there is no rpcCallBack set
        assertTrue(prevScannerCount == region.scannerReadPoints.size());
        ServerCall serverCall = (ServerCall) RpcServer.getCurrentCall().get();
        assertTrue(serverCall.getCallBack() == null);

        Get get3 = new Get(r3);
        Get get4 = new Get(r4);
        Table table2 = null;
        inCP = true;
        try {
          table2 = connection.getTable(tableName);
          Result[] results = table2.get(Arrays.asList(get3, get4));
          assertTrue("Expected row: row-3", Bytes.equals(r3, results[0].getRow()));
          assertTrue("Expected row: row-4", Bytes.equals(r4, results[1].getRow()));
        } finally {
          if (table2 != null) {
            table2.close();
          }
          inCP = false;
        }

        // RegionScanner is closed and there is no rpcCallBack set
        assertTrue(prevScannerCount == region.scannerReadPoints.size());
        serverCall = (ServerCall) RpcServer.getCurrentCall().get();
        assertTrue(serverCall.getCallBack() == null);

        Scan newScan = new Scan();
        newScan.setCaching(1);
        newScan.withStartRow(r5, true).withStopRow(r6, true);
        Table table3 = null;
        ResultScanner resultScanner = null;
        inCP = true;
        try {
          table3 = connection.getTable(tableName);
          resultScanner = table3.getScanner(newScan);
          Result result = resultScanner.next();
          assertTrue("Expected row: row-5", Bytes.equals(r5, result.getRow()));
          result = resultScanner.next();
          assertTrue("Expected row: row-6", Bytes.equals(r6, result.getRow()));
          result = resultScanner.next();
          assertNull(result);
        } finally {
          if (resultScanner != null) {
            resultScanner.close();
          }
          if (table3 != null) {
            table3.close();
          }
          inCP = false;
        }

        // RegionScanner is closed and there is no rpcCallBack set
        assertTrue(prevScannerCount == region.scannerReadPoints.size());
        serverCall = (ServerCall) RpcServer.getCurrentCall().get();
        assertTrue(serverCall.getCallBack() == null);
        return regionScanner;
      } catch (Throwable e) {
        exceptionRef.set(e);
        throw e;
      }
    }
  }

}
