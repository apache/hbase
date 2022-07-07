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

import static org.apache.hadoop.hbase.client.ConnectionConfiguration.HBASE_CLIENT_META_READ_RPC_TIMEOUT_KEY;
import static org.apache.hadoop.hbase.client.ConnectionConfiguration.HBASE_CLIENT_META_SCANNER_TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;

@Category({ MediumTests.class, ClientTests.class })
public class TestClientScannerTimeouts {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestClientScannerTimeouts.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestClientScannerTimeouts.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static AsyncConnection ASYNC_CONN;
  private static Connection CONN;
  private static final byte[] FAMILY = Bytes.toBytes("testFamily");
  private static final byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static final byte[] VALUE = Bytes.toBytes("testValue");

  private static final byte[] ROW0 = Bytes.toBytes("row-0");
  private static final byte[] ROW1 = Bytes.toBytes("row-1");
  private static final byte[] ROW2 = Bytes.toBytes("row-2");
  private static final byte[] ROW3 = Bytes.toBytes("row-3");
  private static final int rpcTimeout = 1000;
  private static final int scanTimeout = 3 * rpcTimeout;
  private static final int metaScanTimeout = 6 * rpcTimeout;
  private static final int CLIENT_RETRIES_NUMBER = 3;

  private static TableName tableName;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Don't report so often so easier to see other rpcs
    conf.setInt("hbase.regionserver.msginterval", 3 * 10000);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, rpcTimeout);
    conf.setStrings(HConstants.REGION_SERVER_IMPL, RegionServerWithScanTimeout.class.getName());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, CLIENT_RETRIES_NUMBER);
    conf.setInt(HConstants.HBASE_CLIENT_PAUSE, 1000);
    TEST_UTIL.startMiniCluster(1);

    conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, scanTimeout);
    conf.setInt(HBASE_CLIENT_META_READ_RPC_TIMEOUT_KEY, metaScanTimeout);
    conf.setInt(HBASE_CLIENT_META_SCANNER_TIMEOUT, metaScanTimeout);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(conf).get();
    CONN = ConnectionFactory.createConnection(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    CONN.close();
    ASYNC_CONN.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  public void setup(boolean isSystemTable) throws IOException {
    RSRpcServicesWithScanTimeout.reset();

    String nameAsString = name.getMethodName();
    if (isSystemTable) {
      nameAsString = NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR + ":" + nameAsString;
    }
    tableName = TableName.valueOf(nameAsString);
    TEST_UTIL.createTable(tableName, FAMILY);

    Table table = CONN.getTable(tableName);
    putToTable(table, ROW0);
    putToTable(table, ROW1);
    putToTable(table, ROW2);
    putToTable(table, ROW3);
    LOG.info("Wrote our four values");

    table.getRegionLocator().getAllRegionLocations();

    // reset again incase the creation/population caused anything to trigger
    RSRpcServicesWithScanTimeout.reset();
  }

  private void expectRow(byte[] expected, Result result) {
    assertTrue("Expected row: " + Bytes.toString(expected),
      Bytes.equals(expected, result.getRow()));
  }

  private void expectNumTries(int expected) {
    assertEquals(
      "Expected tryNumber=" + expected + ", actual=" + RSRpcServicesWithScanTimeout.tryNumber,
      expected, RSRpcServicesWithScanTimeout.tryNumber);
    // reset for next
    RSRpcServicesWithScanTimeout.tryNumber = 0;
  }

  /**
   * verify that we don't miss any data when encountering an OutOfOrderScannerNextException.
   * Typically, the only way to naturally trigger this is if a client-side timeout causes an
   * erroneous next() call. This is relatively hard to do these days because the server attempts to
   * always return before the timeout. In this test we force the server to throw this exception, so
   * that we can test the retry logic appropriately.
   */
  @Test
  public void testRetryOutOfOrderScannerNextException() throws IOException {
    expectRetryOutOfOrderScannerNext(() -> getScanner(CONN));
  }

  /**
   * AsyncTable version of above
   */
  @Test
  public void testRetryOutOfOrderScannerNextExceptionAsync() throws IOException {
    expectRetryOutOfOrderScannerNext(this::getAsyncScanner);
  }

  /**
   * verify that we honor the {@link HConstants#HBASE_RPC_READ_TIMEOUT_KEY} for normal scans. Use a
   * special connection which has retries disabled, because otherwise the scanner will retry the
   * timed out next() call and mess up the test.
   */
  @Test
  public void testNormalScanTimeoutOnNext() throws IOException {
    setup(false);
    // Unlike AsyncTable, Table's ResultScanner.next() call uses rpcTimeout and
    // will retry until scannerTimeout. This makes it more difficult to test the timeouts
    // of normal next() calls. So we use a separate connection here which has retries disabled.
    Configuration confNoRetries = new Configuration(CONN.getConfiguration());
    confNoRetries.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 0);
    try (Connection conn = ConnectionFactory.createConnection(confNoRetries)) {
      expectTimeoutOnNext(rpcTimeout, () -> getScanner(conn));
    }
  }

  /**
   * AsyncTable version of above
   */
  @Test
  public void testNormalScanTimeoutOnNextAsync() throws IOException {
    setup(false);
    expectTimeoutOnNext(scanTimeout, this::getAsyncScanner);
  }

  /**
   * verify that we honor {@link HConstants#HBASE_RPC_READ_TIMEOUT_KEY} for openScanner() calls for
   * meta scans
   */
  @Test
  public void testNormalScanTimeoutOnOpenScanner() throws IOException {
    setup(false);
    expectTimeoutOnOpenScanner(rpcTimeout, this::getScanner);
  }

  /**
   * AsyncTable version of above
   */
  @Test
  public void testNormalScanTimeoutOnOpenScannerAsync() throws IOException {
    setup(false);
    expectTimeoutOnOpenScanner(rpcTimeout, this::getAsyncScanner);
  }

  /**
   * verify that we honor {@link ConnectionConfiguration#HBASE_CLIENT_META_SCANNER_TIMEOUT} for
   * next() calls in meta scans
   */
  @Test
  public void testMetaScanTimeoutOnNext() throws IOException {
    setup(true);
    expectTimeoutOnNext(metaScanTimeout, this::getScanner);
  }

  /**
   * AsyncTable version of above
   */
  @Test
  public void testMetaScanTimeoutOnNextAsync() throws IOException {
    setup(true);
    expectTimeoutOnNext(metaScanTimeout, this::getAsyncScanner);
  }

  /**
   * verify that we honor {@link ConnectionConfiguration#HBASE_CLIENT_META_READ_RPC_TIMEOUT_KEY} for
   * openScanner() calls for meta scans
   */
  @Test
  public void testMetaScanTimeoutOnOpenScanner() throws IOException {
    setup(true);
    expectTimeoutOnOpenScanner(metaScanTimeout, this::getScanner);
  }

  /**
   * AsyncTable version of above
   */
  @Test
  public void testMetaScanTimeoutOnOpenScannerAsync() throws IOException {
    setup(true);
    expectTimeoutOnOpenScanner(metaScanTimeout, this::getAsyncScanner);
  }

  private void expectRetryOutOfOrderScannerNext(Supplier<ResultScanner> scannerSupplier)
    throws IOException {
    setup(false);
    RSRpcServicesWithScanTimeout.seqNoToThrowOn = 1;

    LOG.info(
      "Opening scanner, expecting no errors from first next() call from openScanner response");
    ResultScanner scanner = scannerSupplier.get();
    Result result = scanner.next();
    expectRow(ROW0, result);
    expectNumTries(0);

    LOG.info("Making first next() RPC, expecting no errors for seqNo 0");
    result = scanner.next();
    expectRow(ROW1, result);
    expectNumTries(0);

    LOG.info(
      "Making second next() RPC, expecting OutOfOrderScannerNextException and appropriate retry");
    result = scanner.next();
    expectRow(ROW2, result);
    expectNumTries(1);

    // reset so no errors. since last call restarted the scan and following
    // call would otherwise fail
    RSRpcServicesWithScanTimeout.seqNoToThrowOn = -1;

    LOG.info("Finishing scan, expecting no errors");
    result = scanner.next();
    expectRow(ROW3, result);
    scanner.close();

    LOG.info("Testing always throw exception");
    byte[][] expectedResults = new byte[][] { ROW0, ROW1, ROW2, ROW3 };
    int i = 0;

    // test the case that RPC always throws
    scanner = scannerSupplier.get();
    RSRpcServicesWithScanTimeout.throwAlways = true;

    while (true) {
      LOG.info("Calling scanner.next()");
      result = scanner.next();
      if (result == null) {
        break;
      } else {
        byte[] expectedResult = expectedResults[i++];
        expectRow(expectedResult, result);
      }
    }

    // ensure we verified all rows. this along with the expectRow check above
    // proves that we didn't miss any rows.
    assertEquals("Expected to exhaust expectedResults array length=" + expectedResults.length
      + ", actual index=" + i, expectedResults.length, i);

    // expect all but the first row (which came from initial openScanner) to have thrown an error
    expectNumTries(expectedResults.length - 1);

  }

  private void expectTimeoutOnNext(int timeout, Supplier<ResultScanner> scannerSupplier)
    throws IOException {
    RSRpcServicesWithScanTimeout.seqNoToSleepOn = 1;
    RSRpcServicesWithScanTimeout.setSleepForTimeout(timeout);

    LOG.info(
      "Opening scanner, expecting no timeouts from first next() call from openScanner response");
    ResultScanner scanner = scannerSupplier.get();
    Result result = scanner.next();
    expectRow(ROW0, result);

    LOG.info("Making first next() RPC, expecting no timeout for seqNo 0");
    result = scanner.next();
    expectRow(ROW1, result);

    LOG.info("Making second next() RPC, expecting timeout");
    long start = System.nanoTime();
    try {
      scanner.next();
      fail("Expected CallTimeoutException");
    } catch (RetriesExhaustedException e) {
      assertTrue("Expected CallTimeoutException", e.getCause() instanceof CallTimeoutException
        || e.getCause() instanceof SocketTimeoutException);
    }
    expectTimeout(start, timeout);
  }

  private void expectTimeoutOnOpenScanner(int timeout, Supplier<ResultScanner> scannerSupplier)
    throws IOException {
    RSRpcServicesWithScanTimeout.setSleepForTimeout(timeout);
    RSRpcServicesWithScanTimeout.sleepOnOpen = true;
    LOG.info("Opening scanner, expecting timeout from first next() call from openScanner response");
    long start = System.nanoTime();
    try {
      scannerSupplier.get().next();
      fail("Expected SocketTimeoutException or CallTimeoutException");
    } catch (RetriesExhaustedException e) {
      LOG.info("Got error", e);
      assertTrue("Expected SocketTimeoutException or CallTimeoutException, but was " + e.getCause(),
        e.getCause() instanceof CallTimeoutException
          || e.getCause() instanceof SocketTimeoutException);
    }
    expectTimeout(start, timeout);
  }

  private void expectTimeout(long start, int timeout) {
    long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    LOG.info("Expected duration >= {}, and got {}", timeout, duration);
    assertTrue("Expected duration >= " + timeout + ", but was " + duration, duration >= timeout);
  }

  private ResultScanner getScanner() {
    return getScanner(CONN);
  }

  private ResultScanner getScanner(Connection conn) {
    Scan scan = new Scan();
    scan.setCaching(1);
    try {
      return conn.getTable(tableName).getScanner(scan);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private ResultScanner getAsyncScanner() {
    Scan scan = new Scan();
    scan.setCaching(1);
    return ASYNC_CONN.getTable(tableName).getScanner(scan);
  }

  private void putToTable(Table ht, byte[] rowkey) throws IOException {
    Put put = new Put(rowkey);
    put.addColumn(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
  }

  private static class RegionServerWithScanTimeout
    extends MiniHBaseCluster.MiniHBaseClusterRegionServer {
    public RegionServerWithScanTimeout(Configuration conf)
      throws IOException, InterruptedException {
      super(conf);
    }

    @Override
    protected RSRpcServices createRpcServices() throws IOException {
      return new RSRpcServicesWithScanTimeout(this);
    }
  }

  private static class RSRpcServicesWithScanTimeout extends RSRpcServices {
    private long tableScannerId;

    private static long seqNoToThrowOn = -1;
    private static boolean throwAlways = false;
    private static boolean threw;

    private static long seqNoToSleepOn = -1;
    private static boolean sleepOnOpen = false;
    private static volatile boolean slept;
    private static int tryNumber = 0;

    private static int sleepTime = rpcTimeout + 500;

    public static void setSleepForTimeout(int timeout) {
      sleepTime = timeout + 500;
    }

    public static void reset() {
      setSleepForTimeout(scanTimeout);

      seqNoToSleepOn = -1;
      seqNoToThrowOn = -1;
      throwAlways = false;
      threw = false;
      sleepOnOpen = false;
      slept = false;
      tryNumber = 0;
    }

    public RSRpcServicesWithScanTimeout(HRegionServer rs) throws IOException {
      super(rs);
    }

    @Override
    public ScanResponse scan(final RpcController controller, final ScanRequest request)
      throws ServiceException {
      if (request.hasScannerId()) {
        LOG.info("Got request {}", request);
        ScanResponse scanResponse = super.scan(controller, request);
        if (tableScannerId != request.getScannerId() || request.getCloseScanner()) {
          return scanResponse;
        }

        if (
          throwAlways
            || (!threw && request.hasNextCallSeq() && seqNoToThrowOn == request.getNextCallSeq())
        ) {
          threw = true;
          tryNumber++;
          LOG.info("THROWING exception, tryNumber={}, tableScannerId={}", tryNumber,
            tableScannerId);
          throw new ServiceException(new OutOfOrderScannerNextException());
        }

        if (!slept && request.hasNextCallSeq() && seqNoToSleepOn == request.getNextCallSeq()) {
          try {
            LOG.info("SLEEPING " + sleepTime);
            Thread.sleep(sleepTime);
          } catch (InterruptedException e) {
          }
          slept = true;
          tryNumber++;
        }
        return scanResponse;
      } else {
        ScanResponse scanRes = super.scan(controller, request);
        String regionName = Bytes.toString(request.getRegion().getValue().toByteArray());
        if (!regionName.contains(TableName.META_TABLE_NAME.getNameAsString())) {
          tableScannerId = scanRes.getScannerId();
          if (sleepOnOpen) {
            try {
              LOG.info("openScanner SLEEPING " + sleepTime);
              Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
            }
          }
        }
        return scanRes;
      }
    }
  }
}
