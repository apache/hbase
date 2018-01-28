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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.HBaseRpcControllerImpl;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;

/**
 * Testcase to make sure that we do not close scanners if ScanRequest.numberOfRows is zero. See
 * HBASE-18042 for more details.
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestScanWithoutFetchingData {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestScanWithoutFetchingData.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final TableName TABLE_NAME = TableName.valueOf("test");

  private static final byte[] CF = Bytes.toBytes("cf");

  private static final byte[] CQ = Bytes.toBytes("cq");

  private static final int COUNT = 10;

  private static HRegionInfo HRI;

  private static ClientProtos.ClientService.BlockingInterface STUB;

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
    try (Table table = UTIL.createTable(TABLE_NAME, CF)) {
      for (int i = 0; i < COUNT; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    HRI = UTIL.getAdmin().getTableRegions(TABLE_NAME).get(0);
    STUB = ((ConnectionImplementation) UTIL.getConnection())
        .getClient(UTIL.getHBaseCluster().getRegionServer(0).getServerName());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private void assertResult(int row, Result result) {
    assertEquals(row, Bytes.toInt(result.getRow()));
    assertEquals(row, Bytes.toInt(result.getValue(CF, CQ)));
  }

  @Test
  public void test() throws ServiceException, IOException {
    Scan scan = new Scan();
    ScanRequest req = RequestConverter.buildScanRequest(HRI.getRegionName(), scan, 0, false);
    HBaseRpcController hrc = new HBaseRpcControllerImpl();
    ScanResponse resp = STUB.scan(hrc, req);
    assertTrue(resp.getMoreResults());
    assertTrue(resp.getMoreResultsInRegion());
    assertEquals(0, ResponseConverter.getResults(hrc.cellScanner(), resp).length);
    long scannerId = resp.getScannerId();
    int nextCallSeq = 0;
    // test normal next
    for (int i = 0; i < COUNT / 2; i++) {
      req = RequestConverter.buildScanRequest(scannerId, 1, false, nextCallSeq++, false, false, -1);
      hrc.reset();
      resp = STUB.scan(hrc, req);
      assertTrue(resp.getMoreResults());
      assertTrue(resp.getMoreResultsInRegion());
      Result[] results = ResponseConverter.getResults(hrc.cellScanner(), resp);
      assertEquals(1, results.length);
      assertResult(i, results[0]);
    }
    // test zero next
    req = RequestConverter.buildScanRequest(scannerId, 0, false, nextCallSeq++, false, false, -1);
    hrc.reset();
    resp = STUB.scan(hrc, req);
    assertTrue(resp.getMoreResults());
    assertTrue(resp.getMoreResultsInRegion());
    assertEquals(0, ResponseConverter.getResults(hrc.cellScanner(), resp).length);
    for (int i = COUNT / 2; i < COUNT; i++) {
      req = RequestConverter.buildScanRequest(scannerId, 1, false, nextCallSeq++, false, false, -1);
      hrc.reset();
      resp = STUB.scan(hrc, req);
      assertTrue(resp.getMoreResults());
      assertEquals(i != COUNT - 1, resp.getMoreResultsInRegion());
      Result[] results = ResponseConverter.getResults(hrc.cellScanner(), resp);
      assertEquals(1, results.length);
      assertResult(i, results[0]);
    }
    // close
    req = RequestConverter.buildScanRequest(scannerId, 0, true, false);
    resp = STUB.scan(null, req);
  }
}
