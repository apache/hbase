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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.exceptions.ScannerResetException;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestScannerTimeoutHandling {

  private static final Logger LOG = LoggerFactory.getLogger(TestScannerTimeoutHandling.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestScannerTimeoutHandling.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final MetricsAssertHelper METRICS_ASSERT =
    CompatibilityFactory.getInstance(MetricsAssertHelper.class);
  private static final int TIMEOUT = 3000;
  private static final TableName TABLE_NAME = TableName.valueOf("foo");
  private static Connection CONN;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Don't report so often so easier to see other rpcs
    conf.setInt("hbase.regionserver.msginterval", 3 * 10000);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, TIMEOUT);
    // conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, TIMEOUT);
    conf.setStrings(HConstants.REGION_SERVER_IMPL, RegionServerWithScanTimeout.class.getName());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 0);
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE_NAME, "0");

    CONN = ConnectionFactory.createConnection(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    CONN.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * If a client's timeout would be exceeded before scan results are ready, there is no point
   * returning results to the client. Worse, for openScanner calls, the client cannot close the
   * timed out scanner so this leaks scanners on the server. This test verifies that we properly
   * track and cleanup scanners when a client timeout is exceeded. This should be more rare when
   * heartbeats are enabled, since we try to return before timeout there. But it's still possible if
   * queueing eats up most of the timeout or the inner workings of the scan were slowed down enough
   * to still exceed the timeout despite the calculated heartbeat deadline.
   */
  @Test
  public void testExceededClientDeadline() throws Exception {
    Table table = CONN.getTable(TABLE_NAME);

    // put some rows so that our scanner doesn't complete on the first rpc.
    // this would prematurely close the scanner before the timeout handling has a chance to
    for (int i = 0; i < 10; i++) {
      table.put(new Put(Bytes.toBytes(i)).addColumn(new byte[] { '0' }, new byte[] { '0' },
        new byte[] { '0' }));
    }

    try {
      ResultScanner scanner = table.getScanner(new Scan().setCaching(1).setMaxResultSize(1));
      scanner.next();
    } catch (RetriesExhaustedException e) {
      assertTrue(e.getCause() instanceof CallTimeoutException);
    } finally {
      // ensure the scan has finished on the server side
      RSRpcServicesWithScanTimeout.lock.tryLock(60, TimeUnit.SECONDS);
      // there should be 0 running scanners, meaning the scanner was properly closed on the server
      assertEquals(0, RSRpcServicesWithScanTimeout.scannerCount);
      // we should have caught the expected exception
      assertTrue(RSRpcServicesWithScanTimeout.caughtTimeoutException);
      // we should have incremented the callTimedOut metric
      METRICS_ASSERT.assertCounterGt("exceptions.callTimedOut", 0, TEST_UTIL.getHBaseCluster()
        .getRegionServer(0).getRpcServer().getMetrics().getMetricsSource());
    }
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

    private static boolean caughtTimeoutException = false;
    private static int scannerCount = -1;
    private static final Lock lock = new ReentrantLock();

    public RSRpcServicesWithScanTimeout(HRegionServer rs) throws IOException {
      super(rs);
    }

    @Override
    public ClientProtos.ScanResponse scan(final RpcController controller,
      final ClientProtos.ScanRequest request) throws ServiceException {

      String regionName = Bytes.toString(request.getRegion().getValue().toByteArray());
      if (regionName.contains(TABLE_NAME.getNameAsString())) {

        // if the client's timeout is exceeded, it may either retry or attempt to close
        // the scanner. we don't want to allow either until we've verified the server handling.
        // so only allow 1 request at a time to our test table
        try {
          if (!lock.tryLock(60, TimeUnit.SECONDS)) {
            throw new ServiceException("Failed to get lock");
          }
        } catch (InterruptedException e) {
          throw new ServiceException(e);
        }

        try {
          LOG.info("SLEEPING");
          Thread.sleep(TIMEOUT * 2);
        } catch (Exception e) {
        }

        try {
          return super.scan(controller, request);
        } catch (ServiceException e) {
          if (
            e.getCause() instanceof ScannerResetException
              && e.getCause().getCause() instanceof TimeoutIOException
          ) {
            LOG.info("caught EXPECTED exception in scan after sleep", e);
            caughtTimeoutException = true;
          } else {
            LOG.warn("caught UNEXPECTED exception in scan after sleep", e);
          }
        } finally {
          scannerCount = getScannersCount();
          lock.unlock();
        }
      }

      return super.scan(controller, request);

    }
  }
}
