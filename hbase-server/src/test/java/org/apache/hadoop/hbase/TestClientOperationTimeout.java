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
package org.apache.hadoop.hbase;

import java.io.IOException;

import java.net.SocketTimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

/**
 * These tests verify that the RPC timeouts ('hbase.client.operation.timeout' and
 * 'hbase.client.scanner.timeout.period') work correctly using a modified Region Server which
 * injects delays to get, scan and mutate operations.
 *
 * When 'hbase.client.operation.timeout' is set and client operation is not completed in time the
 * client will retry the operation 'hbase.client.retries.number' times. After that
 * {@link SocketTimeoutException} will be thrown.
 *
 * Using 'hbase.client.scanner.timeout.period' configuration property similar behavior can be
 * specified for scan related operations such as openScanner(), next(). If that times out
 * {@link RetriesExhaustedException} will be thrown.
 */
@Category({ClientTests.class, MediumTests.class})
public class TestClientOperationTimeout {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestClientOperationTimeout.class);

  private static final HBaseTestingUtility TESTING_UTIL = new HBaseTestingUtility();

  // Activate the delays after table creation to test get/scan/put
  private static int DELAY_GET;
  private static int DELAY_SCAN;
  private static int DELAY_MUTATE;

  private final byte[] FAMILY = Bytes.toBytes("family");
  private final byte[] ROW = Bytes.toBytes("row");
  private final byte[] QUALIFIER = Bytes.toBytes("qualifier");
  private final byte[] VALUE = Bytes.toBytes("value");

  @Rule
  public TestName name = new TestName();
  private Table table;

  @BeforeClass
  public static void setUpClass() throws Exception {
    TESTING_UTIL.getConfiguration().setLong(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 500);
    TESTING_UTIL.getConfiguration().setLong(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, 500);
    TESTING_UTIL.getConfiguration().setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 500);
    TESTING_UTIL.getConfiguration().setLong(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);

    TESTING_UTIL.startMiniCluster(1, 1, null, null, DelayedRegionServer.class);
  }

  @Before
  public void setUp() throws Exception {
    DELAY_GET = 0;
    DELAY_SCAN = 0;
    DELAY_MUTATE = 0;

    table = TESTING_UTIL.createTable(TableName.valueOf(name.getMethodName()), FAMILY);
    Put put = new Put(ROW);
    put.addColumn(FAMILY, QUALIFIER, VALUE);
    table.put(put);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TESTING_UTIL.shutdownMiniCluster();
  }

  /**
   * Tests that a get on a table throws {@link SocketTimeoutException} when the operation takes
   * longer than 'hbase.client.operation.timeout'.
   */
  @Test(expected = SocketTimeoutException.class)
  public void testGetTimeout() throws Exception {
    DELAY_GET = 600;
    table.get(new Get(ROW));
  }

  /**
   * Tests that a put on a table throws {@link SocketTimeoutException} when the operation takes
   * longer than 'hbase.client.operation.timeout'.
   */
  @Test(expected = SocketTimeoutException.class)
  public void testPutTimeout() throws Exception {
    DELAY_MUTATE = 600;

    Put put = new Put(ROW);
    put.addColumn(FAMILY, QUALIFIER, VALUE);
    table.put(put);
  }

  /**
   * Tests that scan on a table throws {@link RetriesExhaustedException} when the operation takes
   * longer than 'hbase.client.scanner.timeout.period'.
   */
  @Test(expected = RetriesExhaustedException.class)
  public void testScanTimeout() throws Exception {
    DELAY_SCAN = 600;
    ResultScanner scanner = table.getScanner(new Scan());
    scanner.next();
  }

  private static class DelayedRegionServer extends MiniHBaseCluster.MiniHBaseClusterRegionServer {
    public DelayedRegionServer(Configuration conf) throws IOException, InterruptedException {
      super(conf);
    }

    @Override
    protected RSRpcServices createRpcServices() throws IOException {
      return new DelayedRSRpcServices(this);
    }
  }

  /**
   * This {@link RSRpcServices} class injects delay for Rpc calls and after executes super methods.
   */
  public static class DelayedRSRpcServices extends RSRpcServices {
    DelayedRSRpcServices(HRegionServer rs) throws IOException {
      super(rs);
    }

    @Override
    public ClientProtos.GetResponse get(RpcController controller, ClientProtos.GetRequest request)
        throws ServiceException {
      try {
        Thread.sleep(DELAY_GET);
      } catch (InterruptedException e) {
        LOG.error("Sleep interrupted during get operation", e);
      }
      return super.get(controller, request);
    }

    @Override
    public ClientProtos.MutateResponse mutate(RpcController rpcc,
        ClientProtos.MutateRequest request) throws ServiceException {
      try {
        Thread.sleep(DELAY_MUTATE);
      } catch (InterruptedException e) {
        LOG.error("Sleep interrupted during mutate operation", e);
      }
      return super.mutate(rpcc, request);
    }

    @Override
    public ClientProtos.ScanResponse scan(RpcController controller,
        ClientProtos.ScanRequest request) throws ServiceException {
      try {
        Thread.sleep(DELAY_SCAN);
      } catch (InterruptedException e) {
        LOG.error("Sleep interrupted during scan operation", e);
      }
      return super.scan(controller, request);
    }
  }
}
