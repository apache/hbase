/**
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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test various scanner timeout issues.
 */
@Category(LargeTests.class)
public class TestScannerTimeout {

  private final static HBaseTestingUtility
      TEST_UTIL = new HBaseTestingUtility();

  final Log LOG = LogFactory.getLog(getClass());
  private final static byte[] SOME_BYTES = Bytes.toBytes("f");
  private final static TableName TABLE_NAME = TableName.valueOf("t");
  private final static int NB_ROWS = 10;
  // Be careful w/ what you set this timer to... it can get in the way of
  // the mini cluster coming up -- the verification in particular.
  private final static int THREAD_WAKE_FREQUENCY = 1000;
  private final static int SCANNER_TIMEOUT = 15000;
  private final static int SCANNER_CACHING = 5;

   /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration c = TEST_UTIL.getConfiguration();
    c.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, SCANNER_TIMEOUT);
    c.setInt(HConstants.THREAD_WAKE_FREQUENCY, THREAD_WAKE_FREQUENCY);
    // Put meta on master to avoid meta server shutdown handling
    c.set("hbase.balancer.tablesOnMaster", "hbase:meta");
    // We need more than one region server for this test
    TEST_UTIL.startMiniCluster(2);
    Table table = TEST_UTIL.createTable(TABLE_NAME, SOME_BYTES);
     for (int i = 0; i < NB_ROWS; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.add(SOME_BYTES, SOME_BYTES, SOME_BYTES);
      table.put(put);
    }
    table.close();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    TEST_UTIL.ensureSomeNonStoppedRegionServersAvailable(2);
  }

  /**
   * Test that scanner can continue even if the region server it was reading
   * from failed. Before 2772, it reused the same scanner id.
   * @throws Exception
   */
  @Test(timeout=300000)
  public void test2772() throws Exception {
    LOG.info("START************ test2772");
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    Scan scan = new Scan();
    // Set a very high timeout, we want to test what happens when a RS
    // fails but the region is recovered before the lease times out.
    // Since the RS is already created, this conf is client-side only for
    // this new table
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, SCANNER_TIMEOUT * 100);
    Table higherScanTimeoutTable = new HTable(conf, TABLE_NAME);
    ResultScanner r = higherScanTimeoutTable.getScanner(scan);
    // This takes way less than SCANNER_TIMEOUT*100
    rs.abort("die!");
    Result[] results = r.next(NB_ROWS);
    assertEquals(NB_ROWS, results.length);
    r.close();
    higherScanTimeoutTable.close();
    LOG.info("END ************ test2772");

  }

  /**
   * Test that scanner won't miss any rows if the region server it was reading
   * from failed. Before 3686, it would skip rows in the scan.
   * @throws Exception
   */
  @Test(timeout=300000)
  public void test3686a() throws Exception {
    LOG.info("START ************ TEST3686A---1");
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    LOG.info("START ************ TEST3686A---1111");

    Scan scan = new Scan();
    scan.setCaching(SCANNER_CACHING);
    LOG.info("************ TEST3686A");
    MetaTableAccessor.fullScanMetaAndPrint(TEST_UTIL.getHBaseAdmin().getConnection());
    // Set a very high timeout, we want to test what happens when a RS
    // fails but the region is recovered before the lease times out.
    // Since the RS is already created, this conf is client-side only for
    // this new table
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setInt(
        HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, SCANNER_TIMEOUT*100);
    Table table = new HTable(conf, TABLE_NAME);
    LOG.info("START ************ TEST3686A---22");

    ResultScanner r = table.getScanner(scan);
    LOG.info("START ************ TEST3686A---33");

    int count = 1;
    r.next();
    LOG.info("START ************ TEST3686A---44");

    // Kill after one call to next(), which got 5 rows.
    rs.abort("die!");
    while(r.next() != null) {
      count ++;
    }
    assertEquals(NB_ROWS, count);
    r.close();
    table.close();
    LOG.info("************ END TEST3686A");
  }

  /**
   * Make sure that no rows are lost if the scanner timeout is longer on the
   * client than the server, and the scan times out on the server but not the
   * client.
   * @throws Exception
   */
  @Test(timeout=300000)
  public void test3686b() throws Exception {
    LOG.info("START ************ test3686b");
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    Scan scan = new Scan();
    scan.setCaching(SCANNER_CACHING);
    // Set a very high timeout, we want to test what happens when a RS
    // fails but the region is recovered before the lease times out.
    // Since the RS is already created, this conf is client-side only for
    // this new table
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, SCANNER_TIMEOUT * 100);
    Table higherScanTimeoutTable = new HTable(conf, TABLE_NAME);
    ResultScanner r = higherScanTimeoutTable.getScanner(scan);
    int count = 1;
    r.next();
    // Sleep, allowing the scan to timeout on the server but not on the client.
    Thread.sleep(SCANNER_TIMEOUT+2000);
    while(r.next() != null) {
      count ++;
    }
    assertEquals(NB_ROWS, count);
    r.close();
    higherScanTimeoutTable.close();
    LOG.info("END ************ END test3686b");

  }

}

