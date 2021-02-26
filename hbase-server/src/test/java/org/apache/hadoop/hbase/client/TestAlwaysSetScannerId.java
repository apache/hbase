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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;

/**
 * Testcase to make sure that we always set scanner id in ScanResponse. See HBASE-18000.
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestAlwaysSetScannerId {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAlwaysSetScannerId.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final TableName TABLE_NAME = TableName.valueOf("test");

  private static final byte[] CF = Bytes.toBytes("cf");

  private static final byte[] CQ = Bytes.toBytes("cq");

  private static final int COUNT = 10;

  private static RegionInfo HRI;

  private static ClientProtos.ClientService.BlockingInterface STUB;

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
    try (Table table = UTIL.createTable(TABLE_NAME, CF)) {
      for (int i = 0; i < COUNT; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
      HRI = table.getRegionLocator().getAllRegionLocations().get(0).getRegion();
    }
    STUB = ((ConnectionImplementation) UTIL.getConnection())
        .getClient(UTIL.getHBaseCluster().getRegionServer(0).getServerName());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws ServiceException, IOException {
    Scan scan = new Scan();
    ScanRequest req = RequestConverter.buildScanRequest(HRI.getRegionName(), scan, 1, false);
    ScanResponse resp = STUB.scan(null, req);
    assertTrue(resp.hasScannerId());
    long scannerId = resp.getScannerId();
    int nextCallSeq = 0;
    // test next
    for (int i = 0; i < COUNT / 2; i++) {
      req = RequestConverter.buildScanRequest(scannerId, 1, false, nextCallSeq++, false, false, -1);
      resp = STUB.scan(null, req);
      assertTrue(resp.hasScannerId());
      assertEquals(scannerId, resp.getScannerId());
    }
    // test renew
    req = RequestConverter.buildScanRequest(scannerId, 0, false, nextCallSeq++, false, true, -1);
    resp = STUB.scan(null, req);
    assertTrue(resp.hasScannerId());
    assertEquals(scannerId, resp.getScannerId());
    // test close
    req = RequestConverter.buildScanRequest(scannerId, 0, true, false);
    resp = STUB.scan(null, req);
    assertTrue(resp.hasScannerId());
    assertEquals(scannerId, resp.getScannerId());
  }
}
