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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.MetricsHBaseServerSource;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestLeaseRenewal {
  public MetricsAssertHelper HELPER = CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static final byte[] ANOTHERROW = Bytes.toBytes("anotherrow");
  private final static byte[] COL_QUAL = Bytes.toBytes("f1");
  private final static byte[] VAL_BYTES = Bytes.toBytes("v1");
  private final static byte[] ROW_BYTES = Bytes.toBytes("r1");
  private final static int leaseTimeout =
      HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD / 4;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
      leaseTimeout);
    TEST_UTIL.startMiniCluster();
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
    // Nothing to do.
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    for (HTableDescriptor htd : TEST_UTIL.getHBaseAdmin().listTables()) {
      LOG.info("Tear down, remove table=" + htd.getTableName());
      TEST_UTIL.deleteTable(htd.getTableName());
    }
  }

  @Test
  public void testLeaseRenewal() throws Exception {
    HTable table = TEST_UTIL.createTable(
      TableName.valueOf("testLeaseRenewal"), FAMILY);
    Put p = new Put(ROW_BYTES);
    p.addColumn(FAMILY, COL_QUAL, VAL_BYTES);
    table.put(p);
    p = new Put(ANOTHERROW);
    p.addColumn(FAMILY, COL_QUAL, VAL_BYTES);
    table.put(p);
    Scan s = new Scan();
    s.setCaching(1);
    ResultScanner rs = table.getScanner(s);
    // make sure that calling renewLease does not impact the scan results
    assertTrue(rs.renewLease());
    assertTrue(Arrays.equals(rs.next().getRow(), ANOTHERROW));
    // renew the lease a few times, long enough to be sure
    // the lease would have expired otherwise
    Thread.sleep(leaseTimeout/2);
    assertTrue(rs.renewLease());
    Thread.sleep(leaseTimeout/2);
    assertTrue(rs.renewLease());
    Thread.sleep(leaseTimeout/2);
    assertTrue(rs.renewLease());
    // make sure we haven't advanced the scanner
    assertTrue(Arrays.equals(rs.next().getRow(), ROW_BYTES));
    assertTrue(rs.renewLease());
    // make sure scanner is exhausted now
    assertNull(rs.next());
    // renewLease should return false now
    assertFalse(rs.renewLease());
    rs.close();
    table.close();
    MetricsHBaseServerSource serverSource = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0)
        .getRpcServer().getMetrics().getMetricsSource();
    HELPER.assertCounter("exceptions.OutOfOrderScannerNextException", 0, serverSource);
  }
}
