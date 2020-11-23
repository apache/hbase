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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster.MiniHBaseClusterRegionServer;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TestClientScannerRPCTimeout;
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
import java.io.IOException;

@Category({ RegionServerTests.class, MediumTests.class})
public class TestScannerRPCScanMetrics {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestScannerRPCScanMetrics.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestScannerRPCScanMetrics.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] FAMILY = Bytes.toBytes("testFamily");
  private static final byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static final byte[] VALUE = Bytes.toBytes("testValue");


  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setStrings(HConstants.REGION_SERVER_IMPL, RegionServerWithScanMetrics.class.getName());
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testScannerRPCScanMetrics() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte[][] splits = new byte[1][];
    splits[0] = Bytes.toBytes("row-4");
    Table ht = TEST_UTIL.createTable(tableName, FAMILY,splits);
    byte[] r0 = Bytes.toBytes("row-0");
    byte[] r1 = Bytes.toBytes("row-1");
    byte[] r2 = Bytes.toBytes("row-2");
    byte[] r3 = Bytes.toBytes("row-3");
    putToTable(ht, r0);
    putToTable(ht, r1);
    putToTable(ht, r2);
    putToTable(ht, r3);
    LOG.info("Wrote our four table entries");
    Scan scan1 = new Scan();
    scan1.withStartRow(r0);
    // This scan should not increment rpc full scan count (start row specified)
    scan1.withStopRow(Bytes.toBytes("row-4"));
    scanNextIterate(ht, scan1);
    Scan scan2 = new Scan();
    scan2.withStartRow(r1);
    // This scan should increment rpc full scan count by 1 (for second region only)
    scanNextIterate(ht, scan2);
    Scan scan3 = new Scan();
    scan3.withStopRow(Bytes.toBytes("row-5"));
    // This scan should increment rpc full scan count by 1 (for firts region only)
    scanNextIterate(ht, scan3);
    Scan scan4 = new Scan();
    scan4.withStartRow(r1);
    scan4.withStopRow(r2);
    // This scan should not increment rpc full scan count (both start and stop row)
    scanNextIterate(ht, scan4);
    Scan dummyScan = new Scan();
    // This scan should increment rpc full scan count by 2 (both regions - no stop/start row)
    scanNextIterate(ht, dummyScan);

    RSRpcServices testClusterRSRPCServices = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0)
      .rpcServices;
    assertEquals(4, testClusterRSRPCServices.rpcFullScanRequestCount.intValue());
  }

  private void putToTable(Table ht, byte[] rowkey) throws IOException {
    Put put = new Put(rowkey);
    put.addColumn(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
  }

  private void scanNextIterate(Table ht, Scan scan) throws Exception{
    ResultScanner scanner = ht.getScanner(scan);
    for (Result result = scanner.next(); result != null; result = scanner.next())
    {
      // Use the result object
    }
    scanner.close();
  }

  private static class RegionServerWithScanMetrics extends MiniHBaseClusterRegionServer {
    public RegionServerWithScanMetrics(Configuration conf)
      throws IOException, InterruptedException {
      super(conf);
    }

    protected RSRpcServices createRPCServices() throws IOException {
      return new RSRPCServicesWithScanMetrics(this);
    }
  }
  private static class RSRPCServicesWithScanMetrics extends RSRpcServices {
    public long getScanRequestCount() {
      return super.rpcScanRequestCount.longValue();
    }
    public long getFullScanRequestCount() {
      return super.rpcFullScanRequestCount.longValue();
    }
    public RSRPCServicesWithScanMetrics(HRegionServer rs)
      throws IOException {
      super(rs);
    }
  }

}
