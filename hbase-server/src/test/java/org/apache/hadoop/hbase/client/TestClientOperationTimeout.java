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

import static org.apache.hadoop.hbase.client.MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

/**
 * These tests verify that the RPC timeouts ('hbase.client.operation.timeout' and
 * 'hbase.client.scanner.timeout.period') work correctly using a modified Region Server which
 * injects delays to get, scan and mutate operations.
 * <p/>
 * When 'hbase.client.operation.timeout' is set and client operation is not completed in time the
 * client will retry the operation 'hbase.client.retries.number' times. After that
 * {@link SocketTimeoutException} will be thrown.
 * <p/>
 * Using 'hbase.client.scanner.timeout.period' configuration property similar behavior can be
 * specified for scan related operations such as openScanner(), next(). If that times out
 * {@link RetriesExhaustedException} will be thrown.
 */
@Category({ ClientTests.class, MediumTests.class })
public class TestClientOperationTimeout {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestClientOperationTimeout.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  // Activate the delays after table creation to test get/scan/put
  private static int DELAY_GET;
  private static int DELAY_SCAN;
  private static int DELAY_MUTATE;
  private static int DELAY_BATCH;
  private static int DELAY_META_SCAN;

  private static final TableName TABLE_NAME = TableName.valueOf("Timeout");
  private static final byte[] FAMILY = Bytes.toBytes("family");
  private static final byte[] ROW = Bytes.toBytes("row");
  private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
  private static final byte[] VALUE = Bytes.toBytes("value");

  private static Connection CONN;
  private static Table TABLE;

  @BeforeClass
  public static void setUpClass() throws Exception {
    // Set RegionServer class and use default values for other options.
    StartMiniClusterOption option =
      StartMiniClusterOption.builder().rsClass(DelayedRegionServer.class).build();
    UTIL.startMiniCluster(option);
    UTIL.getAdmin().createTable(TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build());

    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.setLong(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 500);
    conf.setLong(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, 500);
    conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 500);
    conf.setLong(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    CONN = ConnectionFactory.createConnection(conf);
    TABLE = CONN.getTable(TABLE_NAME);
  }

  @Before
  public void setUp() throws Exception {
    DELAY_GET = 0;
    DELAY_SCAN = 0;
    DELAY_MUTATE = 0;
    DELAY_BATCH = 0;
    DELAY_META_SCAN = 0;
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Closeables.close(TABLE, true);
    Closeables.close(CONN, true);
    UTIL.shutdownMiniCluster();
  }

  /**
   * Tests that a get on a table throws {@link RetriesExhaustedException} when the operation takes
   * longer than 'hbase.client.operation.timeout'.
   */
  @Test
  public void testGetTimeout() {
    DELAY_GET = 600;
    try {
      TABLE.get(new Get(ROW));
      Assert.fail("should not reach here");
    } catch (Exception e) {
      Assert.assertTrue(
        e instanceof SocketTimeoutException && e.getCause() instanceof CallTimeoutException);
    }
  }

  /**
   * Tests that a put on a table throws {@link RetriesExhaustedException} when the operation takes
   * longer than 'hbase.client.operation.timeout'.
   */
  @Test
  public void testPutTimeout() {
    DELAY_MUTATE = 600;
    Put put = new Put(ROW);
    put.addColumn(FAMILY, QUALIFIER, VALUE);
    try {
      TABLE.put(put);
      Assert.fail("should not reach here");
    } catch (Exception e) {
      Assert.assertTrue(
        e instanceof SocketTimeoutException && e.getCause() instanceof CallTimeoutException);
    }
  }

  /**
   * Tests that a batch mutate and batch get on a table throws {@link SocketTimeoutException} when
   * the operation takes longer than 'hbase.client.operation.timeout'.
   */
  @Test
  public void testMultiTimeout() {
    DELAY_BATCH = 600;
    Put put1 = new Put(ROW);
    put1.addColumn(FAMILY, QUALIFIER, VALUE);
    Put put2 = new Put(ROW);
    put2.addColumn(FAMILY, QUALIFIER, VALUE);
    List<Put> puts = new ArrayList<>();
    puts.add(put1);
    puts.add(put2);
    try {
      TABLE.batch(puts, new Object[2]);
      Assert.fail("should not reach here");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SocketTimeoutException);
    }

    Get get1 = new Get(ROW);
    get1.addColumn(FAMILY, QUALIFIER);
    Get get2 = new Get(ROW);
    get2.addColumn(FAMILY, QUALIFIER);

    List<Get> gets = new ArrayList<>();
    gets.add(get1);
    gets.add(get2);
    try {
      TABLE.batch(gets, new Object[2]);
      Assert.fail("should not reach here");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof SocketTimeoutException);
    }
  }

  /**
   * Tests that a batch get on a table throws
   * {@link org.apache.hadoop.hbase.client.OperationTimeoutExceededException} when the region lookup
   * takes longer than the 'hbase.client.operation.timeout'
   */
  @Test
  public void testMultiGetMetaTimeout() throws IOException {

    Configuration conf = new Configuration(UTIL.getConfiguration());

    // the operation timeout must be lower than the delay from a meta scan to etch region locations
    // of the get requests. Simply increasing the meta scan timeout to greater than the
    // HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD will result in SocketTimeoutException on the scans thus
    // avoiding the simulation of load on meta. See: HBASE-27487
    conf.setLong(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 400);
    conf.setBoolean(CLIENT_SIDE_METRICS_ENABLED_KEY, true);
    try (Connection specialConnection = ConnectionFactory.createConnection(conf);
      Table specialTable = specialConnection.getTable(TABLE_NAME)) {

      MetricsConnection metrics =
        ((ConnectionImplementation) specialConnection).getConnectionMetrics();
      long metaCacheNumClearServerPreFailure = metrics.metaCacheNumClearServer.getCount();

      DELAY_META_SCAN = 400;
      List<Get> gets = new ArrayList<>();
      // we need to ensure the region look-ups eat up more time than the operation timeout without
      // exceeding the scan timeout.
      for (int i = 0; i < 100; i++) {
        gets.add(new Get(Bytes.toBytes(i)).addColumn(FAMILY, QUALIFIER));
      }
      try {
        specialTable.get(gets);
        Assert.fail("should not reach here");
      } catch (Exception e) {
        RetriesExhaustedWithDetailsException expected = (RetriesExhaustedWithDetailsException) e;
        Assert.assertEquals(100, expected.getNumExceptions());

        // verify we do not clear the cache in this situation otherwise we will create pathological
        // feedback loop with multigets See: HBASE-27487
        long metaCacheNumClearServerPostFailure = metrics.metaCacheNumClearServer.getCount();
        Assert.assertEquals(metaCacheNumClearServerPreFailure, metaCacheNumClearServerPostFailure);

        for (Throwable cause : expected.getCauses()) {
          Assert.assertTrue(cause instanceof OperationTimeoutExceededException);
        }

      }
    }

  }

  /**
   * Tests that scan on a table throws {@link RetriesExhaustedException} when the operation takes
   * longer than 'hbase.client.scanner.timeout.period'.
   */
  @Test
  public void testScanTimeout() {
    DELAY_SCAN = 600;
    try {
      ResultScanner scanner = TABLE.getScanner(new Scan());
      scanner.next();
      Assert.fail("should not reach here");
    } catch (Exception e) {
      Assert.assertTrue(
        e instanceof RetriesExhaustedException && e.getCause() instanceof SocketTimeoutException);
    }
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
        String regionName = Bytes.toString(request.getRegion().getValue().toByteArray());
        if (regionName.contains(TableName.META_TABLE_NAME.getNameAsString())) {
          Thread.sleep(DELAY_META_SCAN);
        } else {
          Thread.sleep(DELAY_SCAN);
        }
      } catch (InterruptedException e) {
        LOG.error("Sleep interrupted during scan operation", e);
      }
      return super.scan(controller, request);
    }

    @Override
    public ClientProtos.MultiResponse multi(RpcController rpcc, ClientProtos.MultiRequest request)
      throws ServiceException {
      try {
        Thread.sleep(DELAY_BATCH);
      } catch (InterruptedException e) {
        LOG.error("Sleep interrupted during multi operation", e);
      }
      return super.multi(rpcc, request);
    }
  }
}
