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
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NotServingRegionException;
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
import org.junit.function.ThrowingRunnable;

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

  private static boolean FAIL_BATCH = false;

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
    FAIL_BATCH = false;
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
   * Tests that a batch mutate and batch get on a table throws {@link SocketTimeoutException} or
   * {@link OperationTimeoutExceededException} when the operation takes longer than
   * 'hbase.client.operation.timeout'.
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
    assertMultiException(() -> TABLE.batch(puts, new Object[2]));

    Get get1 = new Get(ROW);
    get1.addColumn(FAMILY, QUALIFIER);
    Get get2 = new Get(ROW);
    get2.addColumn(FAMILY, QUALIFIER);

    List<Get> gets = new ArrayList<>();
    gets.add(get1);
    gets.add(get2);
    assertMultiException(() -> TABLE.batch(gets, new Object[2]));
  }

  /**
   * AsyncProcess has an overall waitUntilDone with a timeout, and if all callables dont finish by
   * then it throws a SocketTimeoutException. The callables themselves also try to honor the
   * operation timeout and result in OperationTimeoutExceededException (wrapped in
   * RetriesExhausted). The latter is the more user-friendly exception because it contains details
   * about which server has issues, etc. For now we need to account for both because it's sort of a
   * race to see which timeout exceeds first. Maybe we can replace the waitUntilDone behavior with
   * an interrupt in the future so we can further unify.
   */
  private void assertMultiException(ThrowingRunnable runnable) {
    IOException e = Assert.assertThrows(IOException.class, runnable);
    if (e instanceof SocketTimeoutException) {
      return;
    }
    Assert.assertTrue("Expected SocketTimeoutException or RetriesExhaustedWithDetailsException"
      + " but was " + e.getClass(), e instanceof RetriesExhaustedWithDetailsException);
    for (Throwable cause : ((RetriesExhaustedWithDetailsException) e).getCauses()) {
      Assert.assertEquals(OperationTimeoutExceededException.class, cause.getClass());
    }
  }

  /**
   * Tests that a batch get on a table throws
   * {@link org.apache.hadoop.hbase.client.OperationTimeoutExceededException} when the region lookup
   * takes longer than the 'hbase.client.operation.timeout'. This specifically tests that when meta
   * is slow, the fetching of region locations for a batch is not allowed to itself exceed the
   * operation timeout. In a batch size of 100, it's possible to need to make 100 meta calls in
   * sequence. If meta is slow, we should abort the request once the operation timeout is exceeded,
   * even if we haven't finished locating all regions. See HBASE-27490
   */
  @Test
  public void testMultiGetMetaTimeout() throws IOException {
    Configuration conf = new Configuration(UTIL.getConfiguration());

    conf.setLong(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 400);
    conf.setBoolean(CLIENT_SIDE_METRICS_ENABLED_KEY, true);
    try (Connection specialConnection = ConnectionFactory.createConnection(conf);
      Table specialTable = specialConnection.getTable(TABLE_NAME)) {

      MetricsConnection metrics =
        ((ConnectionImplementation) specialConnection).getConnectionMetrics();
      long metaCacheNumClearServerPreFailure = metrics.getMetaCacheNumClearServer().getCount();

      // delay and timeout are the same, so we should see a timeout after the first region lookup
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
        long metaCacheNumClearServerPostFailure = metrics.getMetaCacheNumClearServer().getCount();
        Assert.assertEquals(metaCacheNumClearServerPreFailure, metaCacheNumClearServerPostFailure);

        for (Throwable cause : expected.getCauses()) {
          Assert.assertTrue(cause instanceof OperationTimeoutExceededException);
          // Check that this is the timeout thrown by AsyncRequestFutureImpl during region lookup
          Assert.assertTrue(cause.getMessage().contains("Operation timeout exceeded during"));
        }
      }
    }
  }

  /**
   * Tests that a batch get on a table throws
   * {@link org.apache.hadoop.hbase.client.OperationTimeoutExceededException} when retries are tuned
   * too high to be able to be processed within the operation timeout. In this case, the final
   * OperationTimeoutExceededException should not trigger a cache clear (but the individual failures
   * may, if appropriate). This test skirts around the timeout checks during meta lookups from
   * HBASE-27490, because we want to test for the case where meta lookups were able to succeed in
   * time but did not leave enough time for the actual calls to occur. See HBASE-27487
   */
  @Test
  public void testMultiGetRetryTimeout() {
    Configuration conf = new Configuration(UTIL.getConfiguration());

    // allow 1 retry, and 0 backoff
    conf.setLong(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 500);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    conf.setLong(HConstants.HBASE_CLIENT_PAUSE, 0);
    conf.setBoolean(CLIENT_SIDE_METRICS_ENABLED_KEY, true);

    try (Connection specialConnection = ConnectionFactory.createConnection(conf);
      Table specialTable = specialConnection.getTable(TABLE_NAME)) {

      MetricsConnection metrics =
        ((ConnectionImplementation) specialConnection).getConnectionMetrics();
      long metaCacheNumClearServerPreFailure = metrics.getMetaCacheNumClearRegion().getCount();

      // meta scan should take up most of the timeout but not all
      DELAY_META_SCAN = 300;
      // fail the batch call, causing a retry
      FAIL_BATCH = true;

      // Use a batch size of 1 so that we only make 1 meta call per attempt
      List<Get> gets = new ArrayList<>();
      gets.add(new Get(Bytes.toBytes(0)).addColumn(FAMILY, QUALIFIER));

      try {
        specialTable.batch(gets, new Object[1]);
        Assert.fail("should not reach here");
      } catch (Exception e) {
        RetriesExhaustedWithDetailsException expected = (RetriesExhaustedWithDetailsException) e;
        Assert.assertEquals(1, expected.getNumExceptions());

        // We expect that the error caused by FAIL_BATCH would clear the meta cache but
        // the OperationTimeoutExceededException should not. So only allow new cache clear here
        long metaCacheNumClearServerPostFailure = metrics.getMetaCacheNumClearRegion().getCount();
        Assert.assertEquals(metaCacheNumClearServerPreFailure + 1,
          metaCacheNumClearServerPostFailure);

        for (Throwable cause : expected.getCauses()) {
          Assert.assertTrue(cause instanceof OperationTimeoutExceededException);
          // Check that this is the timeout thrown by CancellableRegionServerCallable
          Assert.assertTrue(cause.getMessage().contains("Timeout exceeded before call began"));
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Test that for a batch operation where region location resolution fails for the first action in
   * the batch and consumes the entire operation timeout, that the location error is preserved for
   * the first action and that the rest of the batch is failed fast with
   * OperationTimeoutExceededException , this also (indirectly) tests that the action counter is
   * decremented properly for all actions, see last catch block
   */
  @Test
  public void testMultiOperationTimeoutWithLocationError() throws IOException, InterruptedException {
    // Need meta delay > meta scan timeout > operation timeout (with no retries) so that the
    // meta scan for resolving region location for the first action times out after the operation
    // timeout has been exceeded leaving no time to attempt region location resolution for any
    // other actions remaining in the batch
    int operationTimeout = 100;
    int metaScanTimeout = 150;
    DELAY_META_SCAN = 200;

    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.setLong(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, operationTimeout);
    conf.setLong(ConnectionConfiguration.HBASE_CLIENT_META_SCANNER_TIMEOUT, metaScanTimeout);
    conf.setLong(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 0);

    try (Connection specialConnection = ConnectionFactory.createConnection(conf);
      Table specialTable = specialConnection.getTable(TABLE_NAME)) {

      // Region location resolution for first action should fail due to meta scan timeout and cause
      // the batch to exceed the operation timeout, second and third action should then be failed
      // fast with OperationTimeoutExceededException
      Get firstAction = new Get(Bytes.toBytes(0)).addColumn(FAMILY, QUALIFIER);
      Get secondAction = firstAction;
      Get thirdAction = new Get(Bytes.toBytes(1)).addColumn(FAMILY, QUALIFIER);
      List<Get> gets = Arrays.asList(firstAction, secondAction, thirdAction);
      try {
        specialTable.batch(gets, new Object[3]);
        Assert.fail("Should not reach here");
      } catch (RetriesExhaustedWithDetailsException exception) {
        byte[] firstExceptionRow = exception.getRow(0).getRow();
        Assert.assertEquals(firstAction.getRow(), firstExceptionRow);

        // CallTimeout comes from the scan timeout to meta table in locateRegionInMeta
        Throwable firstActionCause = exception.getCause(0);
        Assert.assertTrue(firstActionCause instanceof RetriesExhaustedException);
        Assert.assertTrue(firstActionCause.getCause() instanceof CallTimeoutException);

        byte[] secondExceptionRow = exception.getRow(1).getRow();
        Assert.assertEquals(secondAction.getRow(), secondExceptionRow);

        Throwable secondActionCause = exception.getCause(1);
        Assert.assertTrue(secondActionCause instanceof OperationTimeoutExceededException);

        byte[] thirdExceptionRow = exception.getRow(2).getRow();
        Assert.assertEquals(thirdAction.getRow(), thirdExceptionRow);

        Throwable thirdActionCause = exception.getCause(2);
        Assert.assertTrue(thirdActionCause instanceof OperationTimeoutExceededException);
      }
    } catch (SocketTimeoutException ste) {
      if (ste.getMessage().contains("time out before the actionsInProgress changed to zero")) {
        Assert.fail("Not all actions had action counter decremented: " + ste);
      }
      throw ste;
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
        if (FAIL_BATCH) {
          throw new ServiceException(new NotServingRegionException("simulated failure"));
        }
        Thread.sleep(DELAY_BATCH);
      } catch (InterruptedException e) {
        LOG.error("Sleep interrupted during multi operation", e);
      }
      return super.multi(rpcc, request);
    }
  }
}
