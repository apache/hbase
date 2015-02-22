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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster.MiniHBaseClusterRegionServer;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.ipc.AbstractRpcClient;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Test the scenario where a HRegionServer#scan() call, while scanning, timeout at client side and
 * getting retried. This scenario should not result in some data being skipped at RS side.
 */
@Category({MediumTests.class, ClientTests.class})
public class TestClientScannerRPCTimeout {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] FAMILY = Bytes.toBytes("testFamily");
  private static final byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static final byte[] VALUE = Bytes.toBytes("testValue");
  private static final int rpcTimeout = 2 * 1000;
  private static final int CLIENT_RETRIES_NUMBER = 3;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    ((Log4JLogger)RpcServer.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)AbstractRpcClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)ScannerCallable.LOG).getLogger().setLevel(Level.ALL);
    Configuration conf = TEST_UTIL.getConfiguration();
    // Don't report so often so easier to see other rpcs
    conf.setInt("hbase.regionserver.msginterval", 3 * 10000);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, rpcTimeout);
    conf.setStrings(HConstants.REGION_SERVER_IMPL, RegionServerWithScanTimeout.class.getName());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, CLIENT_RETRIES_NUMBER);
    conf.setInt(HConstants.HBASE_CLIENT_PAUSE, 1000);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testScannerNextRPCTimesout() throws Exception {
    final TableName TABLE_NAME = TableName.valueOf("testScannerNextRPCTimesout");
    Table ht = TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    byte[] r1 = Bytes.toBytes("row-1");
    byte[] r2 = Bytes.toBytes("row-2");
    byte[] r3 = Bytes.toBytes("row-3");
    putToTable(ht, r1);
    putToTable(ht, r2);
    putToTable(ht, r3);
    LOG.info("Wrote our three values");
    RSRpcServicesWithScanTimeout.seqNoToSleepOn = 1;
    Scan scan = new Scan();
    scan.setCaching(1);
    ResultScanner scanner = ht.getScanner(scan);
    Result result = scanner.next();
    assertTrue("Expected row: row-1", Bytes.equals(r1, result.getRow()));
    LOG.info("Got expected first row");
    long t1 = System.currentTimeMillis();
    result = scanner.next();
    assertTrue((System.currentTimeMillis() - t1) > rpcTimeout);
    assertTrue("Expected row: row-2", Bytes.equals(r2, result.getRow()));
    RSRpcServicesWithScanTimeout.seqNoToSleepOn = -1;// No need of sleep
    result = scanner.next();
    assertTrue("Expected row: row-3", Bytes.equals(r3, result.getRow()));
    scanner.close();

    // test the case that RPC is always timesout
    scanner = ht.getScanner(scan);
    RSRpcServicesWithScanTimeout.sleepAlways = true;
    RSRpcServicesWithScanTimeout.tryNumber = 0;
    try {
      result = scanner.next();
    } catch (IOException ioe) {
      // catch the exception after max retry number
      LOG.info("Failed after maximal attempts=" + CLIENT_RETRIES_NUMBER, ioe);
    }
    assertTrue("Expected maximal try number=" + CLIENT_RETRIES_NUMBER
        + ", actual =" + RSRpcServicesWithScanTimeout.tryNumber,
        RSRpcServicesWithScanTimeout.tryNumber <= CLIENT_RETRIES_NUMBER);
  }

  private void putToTable(Table ht, byte[] rowkey) throws IOException {
    Put put = new Put(rowkey);
    put.add(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
  }

  private static class RegionServerWithScanTimeout extends MiniHBaseClusterRegionServer {
    public RegionServerWithScanTimeout(Configuration conf, CoordinatedStateManager cp)
        throws IOException, InterruptedException {
      super(conf, cp);
    }

    protected RSRpcServices createRpcServices() throws IOException {
      return new RSRpcServicesWithScanTimeout(this);
    }
  }

  private static class RSRpcServicesWithScanTimeout extends RSRpcServices {
    private long tableScannerId;
    private boolean slept;
    private static long seqNoToSleepOn = -1;
    private static boolean sleepAlways = false;
    private static int tryNumber = 0;

    public RSRpcServicesWithScanTimeout(HRegionServer rs)
        throws IOException {
      super(rs);
    }

    @Override
    public ScanResponse scan(final RpcController controller, final ScanRequest request)
        throws ServiceException {
      if (request.hasScannerId()) {
        ScanResponse scanResponse = super.scan(controller, request);
        if (this.tableScannerId == request.getScannerId() && 
            (sleepAlways || (!slept && seqNoToSleepOn == request.getNextCallSeq()))) {
          try {
            LOG.info("SLEEPING " + (rpcTimeout + 500));
            Thread.sleep(rpcTimeout + 500);
          } catch (InterruptedException e) {
          }
          slept = true;
          tryNumber++;
          if (tryNumber > 2 * CLIENT_RETRIES_NUMBER) {
            sleepAlways = false;
          }
        }
        return scanResponse;
      } else {
        ScanResponse scanRes = super.scan(controller, request);
        String regionName = Bytes.toString(request.getRegion().getValue().toByteArray());
        if (!regionName.contains(TableName.META_TABLE_NAME.getNameAsString())) {
          tableScannerId = scanRes.getScannerId();
        }
        return scanRes;
      }
    }
  }
}