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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(TestClientOperationTimeout.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestClientOperationTimeout.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  // Activate the delays after table creation to test get/scan/put
  private static int DELAY_GET;
  private static int DELAY_SCAN;
  private static int DELAY_MUTATE;
  private static int DELAY_BATCH_MUTATE;

  private static final TableName TABLE_NAME = TableName.valueOf("Timeout");
  private static final byte[] FAMILY = Bytes.toBytes("family");
  private static final byte[] ROW = Bytes.toBytes("row");
  private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
  private static final byte[] VALUE = Bytes.toBytes("value");

  private static Connection CONN;
  private static Table TABLE;

  @BeforeClass
  public static void setUp() throws Exception {
    // Set RegionServer class and use default values for other options.
    StartTestingClusterOption option =
      StartTestingClusterOption.builder().rsClass(DelayedRegionServer.class).build();
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

  @AfterClass
  public static void tearDown() throws Exception {
    Closeables.close(TABLE, true);
    Closeables.close(CONN, true);
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUpBeforeTest() throws Exception {
    DELAY_GET = 0;
    DELAY_SCAN = 0;
    DELAY_MUTATE = 0;
    DELAY_BATCH_MUTATE = 0;
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
      fail("should not reach here");
    } catch (Exception e) {
      LOG.info("Got exception for get", e);
      assertThat(e, instanceOf(RetriesExhaustedException.class));
      assertThat(e.getCause(), instanceOf(CallTimeoutException.class));
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
      fail("should not reach here");
    } catch (Exception e) {
      LOG.info("Got exception for put", e);
      assertThat(e, instanceOf(RetriesExhaustedException.class));
      assertThat(e.getCause(), instanceOf(CallTimeoutException.class));
    }
  }

  /**
   * Tests that a batch mutate on a table throws {@link RetriesExhaustedException} when the
   * operation takes longer than 'hbase.client.operation.timeout'.
   */
  @Test
  public void testMultiPutsTimeout() {
    DELAY_BATCH_MUTATE = 600;
    Put put1 = new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE);
    Put put2 = new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE);
    List<Put> puts = Arrays.asList(put1, put2);
    try {
      TABLE.batch(puts, new Object[2]);
      fail("should not reach here");
    } catch (Exception e) {
      LOG.info("Got exception for batch", e);
      assertThat(e, instanceOf(RetriesExhaustedException.class));
      assertThat(e.getCause(), instanceOf(RetriesExhaustedException.class));
      assertThat(e.getCause().getCause(), instanceOf(CallTimeoutException.class));
    }
  }

  /**
   * Tests that scan on a table throws {@link RetriesExhaustedException} when the operation takes
   * longer than 'hbase.client.scanner.timeout.period'.
   */
  @Test
  public void testScanTimeout() throws IOException, InterruptedException {
    // cache the region location.
    try (RegionLocator locator = TABLE.getRegionLocator()) {
      locator.getRegionLocation(HConstants.EMPTY_BYTE_ARRAY);
    }
    // sleep a bit to make sure the location has been cached as it is an async operation.
    Thread.sleep(100);
    DELAY_SCAN = 600;
    try (ResultScanner scanner = TABLE.getScanner(new Scan())) {
      scanner.next();
      fail("should not reach here");
    } catch (Exception e) {
      LOG.info("Got exception for scan", e);
      assertThat(e, instanceOf(RetriesExhaustedException.class));
      assertThat(e.getCause(), instanceOf(CallTimeoutException.class));
    }
  }

  public static final class DelayedRegionServer
    extends SingleProcessHBaseCluster.MiniHBaseClusterRegionServer {
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
  private static final class DelayedRSRpcServices extends RSRpcServices {
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

    @Override
    public ClientProtos.MultiResponse multi(RpcController rpcc, ClientProtos.MultiRequest request)
      throws ServiceException {
      try {
        Thread.sleep(DELAY_BATCH_MUTATE);
      } catch (InterruptedException e) {
        LOG.error("Sleep interrupted during multi operation", e);
      }
      return super.multi(rpcc, request);
    }
  }
}
